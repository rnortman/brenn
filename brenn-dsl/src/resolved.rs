//! The resolved model: what a document says once the language is gone.
//!
//! Every type here is the far side of resolution. A [`ResolvedConfig`] carries
//! no references, no f-strings, no undecoded escapes, no classes and no
//! assemblies: references have resolved to the values or the identities they
//! named, strings are concrete, and every class and assembly instantiation has
//! been expanded into the entities it stamps. What survives is what lowering
//! and derivation read.
//!
//! Two things are deliberately unchanged from the parse form:
//!
//! - **The attr vocabularies.** A resolved body is the same `vocabulary!`
//!   struct over a different value type — `SurfaceAttrs<Spanned<RValue>>` where
//!   the parse form is `SurfaceAttrs`. One vocabulary, two phases, so a key
//!   added to an entity cannot be forgotten on this side.
//! - **The spans.** Every value position stays `Spanned`, because the checks
//!   that run after this one — the uuid precedence rules, the ACL family table,
//!   lowering's type checks — all have to cite where something was written, and
//!   by then the parse tree is gone.
//!
//! What this model does *not* assert is as deliberate: pins, ACL statements and
//! grants ride through with their values resolved and their semantics
//! unchecked. Resolution answers "what does this text mean"; whether what it
//! means is legal is the next pass's question.

use fltk_cst_core::Span;
use fltk_serde_core::Spanned;

use crate::model::{
    AgentAttrs, ChannelAttrs, DocComment, IntOrWord, McpServerAttrs, MqttClientAttrs, RemoteAttrs,
    RepoAttrs, SurfaceAttrs, WebhookAttrs, Word, WordList,
};

/// A whole configuration, resolved.
///
/// Flat vectors rather than maps: an entity's identity is its handle, two
/// entities may not share one, and the checks that enforce that cite both
/// sites — which wants source order, not a map's.
#[derive(Debug, Default, PartialEq)]
pub struct ResolvedConfig {
    /// Declared channels, post-expansion. A [`ChanId`] indexes this vector.
    pub channels: Vec<RChannel>,
    /// `channel at prefix "…"` blocks: tuning for a system-minted family, with
    /// no handle and nothing to reference.
    pub tunings: Vec<RTuning>,
    /// Every `uuid_pins` entry, concatenated across files. Duplicate and unused
    /// pins are derivation's to refuse: the precedence rules it needs are not
    /// here.
    pub uuid_pins: Vec<RPin>,
    pub surfaces: Vec<RSurface>,
    /// Top-level component instances.
    pub consumers: Vec<RConsumer>,
    pub agents: Vec<RAgent>,
    pub remotes: Vec<RRemote>,
    pub webhooks: Vec<RWebhook>,
    pub repos: Vec<RNamed<RepoAttrs<RVal>>>,
    pub mqtt_clients: Vec<RNamed<MqttClientAttrs<RVal>>>,
    /// Top-level `mcp_server` definitions only; an agent's inline ones ride on
    /// the agent.
    pub mcp_servers: Vec<RNamed<McpServerAttrs<RVal>>>,
    pub grants: Vec<RGrant>,
    /// The server's own configuration sections, typed by their kindword.
    pub sections: Vec<RSection>,
}

/// A resolved value in a position that carries one.
///
/// The whole reason the vocabularies are generic: this is what `V` is on this
/// side of resolution.
pub type RVal = Spanned<RValue>;

/// Converts a vocabulary field to an [`RVal`] for key/value listings.
///
/// A value field is already an [`RVal`] and crosses as itself; a
/// token-context field was projected at parse time and never resolved, so it
/// renders as the text that was written.
pub trait IntoRVal {
    fn into_rval(self) -> RVal;
}

impl IntoRVal for RVal {
    fn into_rval(self) -> RVal {
        self
    }
}

impl IntoRVal for Word {
    fn into_rval(self) -> RVal {
        let span = self.name.span().clone();
        Spanned::new(RValue::Str(self.name.into_value()), span)
    }
}

impl IntoRVal for WordList {
    fn into_rval(self) -> RVal {
        // A list carries no span of its own; the first word's is the nearest
        // true position, and an empty list has none to offer.
        let span = self
            .words
            .first()
            .map_or_else(Span::unknown, |word| word.name.span().clone());
        let words = self.words.into_iter().map(Word::into_rval).collect();
        Spanned::new(RValue::List(words), span)
    }
}

impl IntoRVal for IntOrWord {
    fn into_rval(self) -> RVal {
        match self {
            IntOrWord::Int(count) => {
                let span = count.span().clone();
                Spanned::new(RValue::Int(*count.value()), span)
            }
            IntOrWord::Word(word) => word.into_rval(),
        }
    }
}

/// A value with the language removed.
///
/// Gone relative to `model::Value`: a reference (resolved to its referent's
/// value, or to the identity of the channel or principal it named), an f-string
/// (interpolated), a raw string and an escaped one (both decoded). What is left
/// is data.
#[derive(Debug, Clone, PartialEq)]
pub enum RValue {
    Str(String),
    Int(i64),
    Flt(f64),
    Bool(bool),
    List(Vec<RVal>),
    /// Source order preserved: a diagnostic citing two entries cites them in
    /// the order they were written.
    Table(Vec<(String, RVal)>),
    Matcher(RMatcher),
}

impl RValue {
    /// What this is, for a diagnostic that has to say what it found.
    pub fn kind(&self) -> &'static str {
        match self {
            RValue::Str(_) => "a string",
            RValue::Int(_) => "an integer",
            RValue::Flt(_) => "a float",
            RValue::Bool(_) => "a boolean",
            RValue::List(_) => "a list",
            RValue::Table(_) => "a table",
            RValue::Matcher(_) => "a matcher",
        }
    }
}

/// `prefix "brenn:alice-desk."`, resolved.
#[derive(Debug, Clone, PartialEq)]
pub struct RMatcher {
    pub kind: Spanned<MatcherKind>,
    pub val: Spanned<RMatcherVal>,
    pub tail: Vec<(String, RVal)>,
}

/// What a matcher matches: one address, every address under a prefix, or one of
/// the shapes a transport-specific family keys on.
///
/// Closed vocabulary; the grammar admits any word here and the resolver refuses
/// the ones that spell no kind. The three transport kinds are spelled exactly as
/// the fields they land in are named, so a matcher and the entry it becomes read
/// the same. Which kinds a scheme admits is derivation's table, not this one's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatcherKind {
    /// Names exactly one address.
    Exact,
    /// Names the family under an address prefix.
    Prefix,
    /// An MQTT topic filter, scoped to one client: `mqtt:<client>:<filter>`.
    TopicFilter,
    /// A webhook endpoint: `webhook:<slug>`.
    Endpoint,
    /// An MQTT client, with no topic dimension: `mqtt:<client>`.
    Client,
}

impl MatcherKind {
    /// Every kind, in the order a diagnostic lists them.
    pub const ALL: [MatcherKind; 5] = [
        Self::Exact,
        Self::Prefix,
        Self::TopicFilter,
        Self::Endpoint,
        Self::Client,
    ];

    /// The kind a word spells, or `None` when it spells none.
    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == word)
    }

    /// The word this kind is written with.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Prefix => "prefix",
            Self::TopicFilter => "topic_filter",
            Self::Endpoint => "endpoint",
            Self::Client => "client",
        }
    }
}

/// The schemes an address may lead with.
///
/// The crate's one scheme vocabulary: resolution refuses an address that names
/// none of these, and derivation reads the family and the durability of the
/// channel a scheme names off it. Transcribed from the runtime's
/// `ChannelScheme`.
/// TODO(dsl-vocabulary-config-parity): held equal to `ChannelScheme` and its
/// capabilities by review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Brenn,
    Ephemeral,
    Local,
    Webhook,
    Mqtt,
}

impl Scheme {
    /// Every scheme, in the order a diagnostic lists them.
    pub const ALL: [Scheme; 5] = [
        Self::Brenn,
        Self::Ephemeral,
        Self::Local,
        Self::Webhook,
        Self::Mqtt,
    ];

    /// The prefix this scheme is written as, colon included.
    pub fn prefix(self) -> &'static str {
        match self {
            Self::Brenn => "brenn:",
            Self::Ephemeral => "ephemeral:",
            Self::Local => "local:",
            Self::Webhook => "webhook:",
            Self::Mqtt => "mqtt:",
        }
    }

    /// The scheme an address leads with and what follows it.
    ///
    /// `None` where the address names no scheme at all: `brenn:` is never
    /// implied, in this language or in a `.brenn` address.
    pub fn split(address: &str) -> Option<(Scheme, &str)> {
        Self::ALL.into_iter().find_map(|scheme| {
            address
                .strip_prefix(scheme.prefix())
                .map(|rest| (scheme, rest))
        })
    }

    /// Every prefix, as a diagnostic lists them: `brenn:, ephemeral:, …`.
    pub fn list() -> String {
        Self::ALL.map(Self::prefix).join(", ")
    }

    /// Every prefix quoted, as a diagnostic that expects one of them lists them:
    /// ``​`brenn:`, `ephemeral:`, … or `mqtt:` ``.
    pub fn quoted_list() -> String {
        crate::diag::or_list(Self::ALL.map(Self::prefix))
    }

    /// Is a channel under this scheme disk-backed — and so carrying an identity
    /// the configuration states rather than one derived at runtime?
    pub fn durable(self) -> bool {
        self == Self::Brenn
    }
}

/// A matcher's payload: a concrete address, or the channel it named.
#[derive(Debug, Clone, PartialEq)]
pub enum RMatcherVal {
    Lit(String),
    /// An `exact` matcher written against a declared channel.
    Chan(ChanId),
}

/// A declared channel, by position in [`ResolvedConfig::channels`].
///
/// An index rather than an address string, so that nothing downstream
/// re-matches a channel by re-parsing text someone else already resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChanId(pub usize);

/// An entity's identity after expansion: `alice_desk.messages_p1`.
///
/// The full path, not the leaf, because two instantiations of one assembly
/// stamp the same leaf names and are not the same entities.
#[derive(Debug, Clone, PartialEq)]
pub struct HandlePath(pub Vec<Spanned<String>>);

impl HandlePath {
    /// The path as it is written and as it defaults into a wire spelling.
    pub fn dotted(&self) -> String {
        self.0
            .iter()
            .map(|segment| segment.value().as_str())
            .collect::<Vec<_>>()
            .join(".")
    }

    /// This path with one more segment under it.
    pub fn child(&self, segment: Spanned<String>) -> HandlePath {
        let mut segments = self.0.clone();
        segments.push(segment);
        HandlePath(segments)
    }

    /// The handle a name written under `prefix` is stamped with: the leaf on
    /// its own where nothing encloses it. The single answer to "what handle
    /// does a name written here get".
    pub fn stamp(prefix: Option<&HandlePath>, name: Spanned<String>) -> HandlePath {
        match prefix {
            Some(prefix) => prefix.child(name),
            None => HandlePath(vec![name]),
        }
    }
}

/// `channel messages_p1 at "brenn:alice-desk.in.p1.messages" { … }`.
#[derive(Debug, PartialEq)]
pub struct RChannel {
    pub handle: HandlePath,
    /// Concrete and scheme-qualified. What follows the scheme is the runtime's
    /// to validate.
    pub address: Spanned<String>,
    pub attrs: ChannelAttrs<RVal>,
    pub doc: Option<DocComment>,
}

/// `channel at prefix "mqtt:broker:alice/" { … }` — depth tuning for a family
/// nothing declares. Not an identity, so not part of the address uniqueness set.
#[derive(Debug, PartialEq)]
pub struct RTuning {
    pub address: Spanned<String>,
    /// Whether `prefix` was written: a whole address tunes one channel, a
    /// prefix tunes the family under it.
    pub is_prefix: bool,
    pub attrs: ChannelAttrs<RVal>,
    pub doc: Option<DocComment>,
}

/// One hand-minted uuid, keyed by the address it belongs to.
#[derive(Debug, PartialEq)]
pub struct RPin {
    pub address: Spanned<String>,
    pub uuid: Spanned<String>,
}

/// A surface and everything written inside it.
#[derive(Debug, PartialEq)]
pub struct RSurface {
    pub handle: HandlePath,
    /// The wire spelling: the `slug` attr where one was written, else the
    /// handle's dotted path.
    pub slug: Spanned<String>,
    pub attrs: SurfaceAttrs<RVal>,
    pub acls: Vec<RAcl>,
    pub components: Vec<RComponentInst>,
    pub doc: Option<DocComment>,
}

/// A component instance inside a surface.
#[derive(Debug, PartialEq)]
pub struct RComponentInst {
    /// The `new` handle, which is the runtime's instance name.
    pub instance: Spanned<String>,
    pub class: ClassRef,
    pub attrs: Vec<(String, RVal)>,
    pub bindings: Vec<RBinding>,
}

/// A top-level component instance: a consumer, with an identity and authority
/// of its own.
#[derive(Debug, PartialEq)]
pub struct RConsumer {
    pub handle: HandlePath,
    pub slug: Spanned<String>,
    pub class: ClassRef,
    /// The transport rights the operator stated. A token context, so it is
    /// projected rather than resolved, and it rides beside the other keys
    /// rather than among them.
    pub grants: Option<RWordList>,
    pub attrs: Vec<(String, RVal)>,
    pub acls: Vec<RAcl>,
    pub bindings: Vec<RBinding>,
    pub doc: Option<DocComment>,
}

/// The artifact shapes a component class may declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Abi {
    /// Served to the browser and rendered inside a surface.
    Dom,
    /// Loaded from an artifact and run outside any surface.
    Processor,
}

impl Abi {
    /// The word this abi is written with.
    pub fn as_str(self) -> &'static str {
        match self {
            Abi::Dom => "dom",
            Abi::Processor => "processor",
        }
    }
}

/// What an instance's class says about it, carried so the class graph does not
/// have to outlive expansion.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassRef {
    pub name: Spanned<String>,
    /// Which artifact shape, and with it where an instance may be placed.
    pub abi: Spanned<Abi>,
    pub component_path: Option<RVal>,
    pub ports: Vec<RPort>,
}

/// One port a class declares.
#[derive(Debug, Clone, PartialEq)]
pub struct RPort {
    pub name: Spanned<String>,
    pub dir: PortDir,
    /// Reserved syntax: parsed, resolved, and inert until channels can declare
    /// a doctype to check it against.
    pub doctype: Option<Spanned<String>>,
}

/// Which way a port faces. The resolved counterpart of the model's, with the
/// three directions spelled the way a binding spells them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortDir {
    In,
    Out,
    Io,
}

impl PortDir {
    /// The word a document writes for this direction.
    pub fn as_str(self) -> &'static str {
        match self {
            PortDir::In => "in",
            PortDir::Out => "out",
            PortDir::Io => "io",
        }
    }
}

/// A port connected to a channel, or a free io port tuned in place.
#[derive(Debug, PartialEq)]
pub struct RBinding {
    pub dir: PortDir,
    pub port: Spanned<String>,
    /// `None` on a free io port: the port is tuned and connects nothing.
    pub chan: Option<RChanRef>,
    pub tail: Vec<(String, RVal)>,
}

/// What a binding, subscription or matcher names.
#[derive(Debug, Clone, PartialEq)]
pub enum RChanRef {
    /// A declared channel.
    Decl(ChanId),
    /// A literal address where no declaration exists — a `local:` plane, a
    /// system-minted `webhook:` or `mqtt:` channel.
    Addr(Spanned<String>),
}

/// An expanded agent instantiation.
#[derive(Debug, PartialEq)]
pub struct RAgent {
    pub handle: HandlePath,
    pub slug: Spanned<String>,
    /// The class it came from. Classes are gone after expansion; this is what a
    /// later diagnostic cites when it has to say where an agent came from.
    pub class: Spanned<String>,
    pub attrs: AgentAttrs<RVal>,
    pub mounts: Vec<RMount>,
    pub mcps: Vec<RMcp>,
    pub subs: Vec<RSubscribe>,
    pub acls: Vec<RAcl>,
    /// `start_hooks` and its siblings, typed by their kindword.
    pub hooks: Vec<RHooks>,
    pub doc: Option<DocComment>,
}

/// `mount ws { working_dir = true; }` — the repo resolved to its handle.
#[derive(Debug, PartialEq)]
pub struct RMount {
    pub repo: HandlePath,
    pub repo_span: Spanned<String>,
    pub tail: Vec<(String, RVal)>,
}

/// What an agent says about an mcp server: a top-level definition it names, or
/// one defined in its own body.
#[derive(Debug, PartialEq)]
pub enum RMcp {
    /// `mcp_server graf;`, resolved to the definition's handle.
    Ref(Spanned<String>),
    /// `mcp_server pfin { … }` — scoped to this agent, which is what lets its
    /// body name the class's parameters.
    Inline(Box<RNamed<McpServerAttrs<RVal>>>),
}

/// `subscribe alice_cmd { push_depth = 1000; }`.
#[derive(Debug, PartialEq)]
pub struct RSubscribe {
    pub chan: RChanRef,
    /// Where the channel was named. A [`RChanRef::Decl`] holds an index and no
    /// position, and a statement is the only thing a later refusal about this
    /// subscription can point at.
    pub span: Span,
    pub tail: Vec<(String, RVal)>,
}

/// A `remote`, with its authority carried and unchecked.
#[derive(Debug, PartialEq)]
pub struct RRemote {
    pub handle: HandlePath,
    pub slug: Spanned<String>,
    pub attrs: RemoteAttrs<RVal>,
    pub acls: Vec<RAcl>,
    pub doc: Option<DocComment>,
}

/// A `webhook` and its typed sub-blocks.
#[derive(Debug, PartialEq)]
pub struct RWebhook {
    pub handle: HandlePath,
    pub slug: Spanned<String>,
    pub attrs: WebhookAttrs<RVal>,
    pub blocks: Vec<RWebhookBlock>,
    pub doc: Option<DocComment>,
}

/// A `keyword name { … }` declaration: a repo, an mqtt client, an mcp server.
#[derive(Debug, PartialEq)]
pub struct RNamed<A> {
    pub handle: HandlePath,
    pub attrs: A,
    pub doc: Option<DocComment>,
}

/// `acl subscribe [prefix "brenn:alice-desk."];`, resolved.
///
/// Which plane words are legal where, and which matcher kinds each scheme
/// admits, is derivation's table — not applied here.
#[derive(Debug, PartialEq)]
pub struct RAcl {
    pub plane: Spanned<String>,
    pub matchers: Vec<RMatcher>,
}

/// `grant alice_pa subscribe prefix "brenn:alice-desk.";` — authority written
/// about another principal, with that principal resolved.
#[derive(Debug, PartialEq)]
pub struct RGrant {
    pub principal: HandlePath,
    pub principal_span: Spanned<String>,
    pub plane: Spanned<String>,
    pub m: RMatcher,
}

/// A resolved hook block, with the point in an agent's life it runs at.
#[derive(Debug, PartialEq)]
pub struct RHooks {
    pub kindword: Spanned<String>,
    pub host: Option<RVal>,
    pub container: Option<RVal>,
}

/// A resolved sub-block of a webhook body.
///
/// The kindword rides along: which credential a `key primary { … }` is, and
/// which scheme a signature block describes, is read from it downstream. The
/// shape is a section's — a kindworded, optionally named body — so it is that
/// type rather than a second declaration of the same fields; no webhook
/// sub-block nests, so `subs` is empty.
pub type RWebhookBlock = RSection;

/// A top-level configuration section, resolved.
///
/// The kindword and the optional name are what say which section this is; the
/// attrs are that section's vocabulary with its values resolved.
#[derive(Debug, PartialEq)]
pub struct RSection {
    pub kindword: Spanned<String>,
    pub name: Option<Spanned<String>>,
    pub attrs: Vec<(String, RVal)>,
    pub subs: Vec<RSection>,
    pub doc: Option<DocComment>,
}

/// A resolved list of bare words: `grants = [subscribe, publish];`.
///
/// Token contexts do not resolve — they were projected at parse time and carry
/// no reference — so the parse form crosses unchanged and this is an alias, not
/// a second type.
pub type RWordList = WordList;

#[cfg(test)]
mod tests {
    use super::*;

    fn word(name: &str) -> Word {
        Word {
            name: Spanned::new(name.to_string(), Span::unknown()),
        }
    }

    #[test]
    fn a_word_list_carries_every_word_as_a_string() {
        let list = WordList {
            words: vec![word("subscribe"), word("publish")],
        };
        assert_eq!(
            list.into_rval().into_value(),
            RValue::List(vec![
                Spanned::new(RValue::Str("subscribe".into()), Span::unknown()),
                Spanned::new(RValue::Str("publish".into()), Span::unknown()),
            ])
        );
    }

    #[test]
    fn an_empty_word_list_is_an_empty_list() {
        let list = WordList { words: Vec::new() };
        assert_eq!(list.into_rval().into_value(), RValue::List(Vec::new()));
    }

    #[test]
    fn either_arm_of_an_int_or_word_carries_as_what_it_is() {
        let count = IntOrWord::Int(Spanned::new(7, Span::unknown()));
        assert_eq!(count.into_rval().into_value(), RValue::Int(7));
        let unbounded = IntOrWord::Word(word("all"));
        assert_eq!(
            unbounded.into_rval().into_value(),
            RValue::Str("all".into())
        );
    }
}

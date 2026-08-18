//! The semantic model: the typed value a `.brenn` document deserializes into.
//!
//! Every type here is hand-written and served by the generated `de.rs`. The
//! grammar's labels and alternative names are the whole contract between the
//! two halves, and nothing checks that contract at generation time — a
//! disagreement surfaces as a positioned deserialize error at runtime, which is
//! why the corpus exercises every rule.
//!
//! Conventions, binding on everything added here:
//!
//! - `#[serde(deny_unknown_fields)]` on every closed-vocabulary struct, so an
//!   unknown key is a positioned error naming the legal set. An attr vocabulary
//!   is declared through `vocabulary!`, which emits it rather than trusting the
//!   author to.
//! - `Spanned<T>` on every field a later diagnostic must cite. Spans never take
//!   part in equality, so expected-value comparisons in tests stay literal.
//! - No tuples, no `#[serde(flatten)]`, no untagged enums. Those route through
//!   serde's buffering representation, which strips the newtype protocol
//!   `Spanned` and `Raw` ride on.
//! - Never a plain `String` field over a sum-shaped rule: it would silently
//!   receive the node's whole source lexeme, and over an enum-shaped rule the
//!   variant name. Value positions are `Value` or a specific literal shape.
//! - A `semi: bool` field is the trace of an optional `;`. Statement forms that
//!   may end either in a braced block or in `;` label the terminator instead of
//!   suppressing it, because the formatter reproduces the document and a
//!   terminator with no trace in the tree is a terminator it would drop.
//!
//! An entity body's `key = value;` entries deserialize into that entity's
//! vocabulary struct, which is where its closed key set — and with it the
//! unknown-key diagnostic naming the legal set — is written down. Bodies whose
//! vocabulary depends on something only resolution knows keep an `AttrMap`
//! instead: instance bodies, and the tails of bindings, mounts and
//! subscriptions.
//!
//! Generic sections cannot be typed that way, because their kindword is data
//! rather than a keyword, so they are typed in two phases: a section is held as
//! its CST subtree, then re-entered with the target its kindword selects
//! (`webhook_block`, `agent_block`, `config_block`). The server's own settings —
//! `server`, `database`, `logging`, … — are sections for that reason too: they
//! are configuration, not entities, and no keyword leads them.

use std::fmt;

use fltk_cst_core::Span;
use fltk_serde_core::{Raw, Spanned};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::cst;
use crate::diag::Diagnostic;

/// One parsed file. Imports first, then declarations, both in source order.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct File {
    pub uses: Vec<UseStmt>,
    pub items: Vec<Spanned<Item>>,
}

/// A top-level declaration.
///
/// Every payload but the node handle is boxed: an `Item` otherwise costs what
/// the largest declaration costs, everywhere one is held.
#[derive(Debug, Deserialize, PartialEq)]
pub enum Item {
    ConstDef(Box<ConstDef>),
    UuidPins(Box<UuidPins>),
    Component(Box<ComponentClass>),
    Agent(Box<AgentClass>),
    Assembly(Box<AssemblyDef>),
    Channel(Box<ChannelDef>),
    Surface(Box<SurfaceDef>),
    Inst(Box<NewStmt>),
    Remote(Box<RemoteDef>),
    Webhook(Box<WebhookDef>),
    Repo(Box<NamedAttrDef<RepoAttrs>>),
    MqttClient(Box<NamedAttrDef<MqttClientAttrs>>),
    McpServer(Box<NamedAttrDef<McpServerAttrs>>),
    Acl(Box<AclStmt>),
    Grant(Box<GrantStmt>),
    Section(SectionNode),
}

/// Typed views over a run of declarations, one per `Item` variant.
macro_rules! item_accessors {
    ($($method:ident => $variant:ident($target:ty),)+) => {
        $(
            #[doc = concat!("Every `", stringify!($variant), "` declaration, in source order.")]
            pub fn $method(&self) -> impl Iterator<Item = &$target> {
                self.items.iter().filter_map(|item| match item.value() {
                    Item::$variant(declaration) => Some(&**declaration),
                    _ => None,
                })
            }
        )+
    };
}

impl File {
    item_accessors! {
        consts => ConstDef(ConstDef),
        uuid_pins => UuidPins(UuidPins),
        components => Component(ComponentClass),
        agents => Agent(AgentClass),
        assemblies => Assembly(AssemblyDef),
        channels => Channel(ChannelDef),
        surfaces => Surface(SurfaceDef),
        instantiations => Inst(NewStmt),
        remotes => Remote(RemoteDef),
        webhooks => Webhook(WebhookDef),
        repos => Repo(NamedAttrDef<RepoAttrs>),
        mqtt_clients => MqttClient(NamedAttrDef<MqttClientAttrs>),
        mcp_servers => McpServer(NamedAttrDef<McpServerAttrs>),
        acls => Acl(AclStmt),
        grants => Grant(GrantStmt),
    }

    /// Every generic section, in source order. Held un-walked: what one is comes
    /// from its kindword, which `webhook_block` / `agent_block` read.
    pub fn sections(&self) -> impl Iterator<Item = &SectionNode> {
        self.items.iter().filter_map(|item| match item.value() {
            Item::Section(node) => Some(node),
            _ => None,
        })
    }
}

impl AssemblyDef {
    // TODO(dsl-assembly-item-vocabulary): the grammar gives an assembly body the
    // whole top-level `item` vocabulary, and these accessors cover eight of the
    // sixteen variants. An `acl` or a `repo` written directly in an assembly body
    // parses, lands in `items`, and is invisible to every reader — silent
    // authority loss in the `acl` case. Either the accessors complete or the
    // grammar restricts the body; which one is a question about assembly
    // semantics.
    item_accessors! {
        consts => ConstDef(ConstDef),
        components => Component(ComponentClass),
        agents => Agent(AgentClass),
        channels => Channel(ChannelDef),
        surfaces => Surface(SurfaceDef),
        instantiations => Inst(NewStmt),
        remotes => Remote(RemoteDef),
        grants => Grant(GrantStmt),
    }
}

/// `use wiring::deskbar::Deskbar;` or `use wiring::deskbar::*;`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UseStmt {
    pub path: PathRef,
    /// Whether the import ended in `::*`.
    pub glob: bool,
}

/// `const components_dir = "/home/alice/lib";`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConstDef {
    pub doc: Option<DocComment>,
    pub name: Spanned<String>,
    pub value: Spanned<Value>,
}

/// A channel declaration, in one of its two roles.
#[derive(Debug, Deserialize, PartialEq)]
pub enum ChannelDef {
    /// `channel utterance at "brenn:alice.out.utterance" { ... }` — a handle to
    /// bind against.
    Decl(ChanDecl),
    /// `channel at prefix "mqtt:broker:alice/" { ... }` — depth tuning for a
    /// system-minted family, with nothing to bind.
    Tuning(ChanTuning),
}

/// The handled channel role.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ChanDecl {
    pub doc: Option<DocComment>,
    pub handle: Spanned<String>,
    pub addr: ChanAddr,
    pub body: Option<AttrBlock<ChannelAttrs>>,
    /// Whether a `;` terminated the statement.
    pub semi: bool,
}

/// The handle-less tuning role.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ChanTuning {
    pub doc: Option<DocComment>,
    pub addr: ChanAddr,
    pub body: Option<AttrBlock<ChannelAttrs>>,
    /// Whether a `;` terminated the statement.
    pub semi: bool,
}

/// A whole address, or the prefix a family shares.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ChanAddr {
    /// Whether `prefix` was written.
    pub is_prefix: bool,
    pub addr: StrLike,
}

/// `uuid_pins { "brenn:alice-desk.in.p1.messages" = "…"; }`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UuidPins {
    pub doc: Option<DocComment>,
    pub pins: Vec<UuidPin>,
}

/// One pin. A duplicated address is the resolver's to refuse: cross-file merge
/// happens there, so refusing here would only catch half the cases.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UuidPin {
    pub addr: StrLit,
    pub uuid: StrLit,
}

/// `component Protobar { abi = dom; in messages; }`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ComponentClass {
    pub doc: Option<DocComment>,
    pub name: Spanned<String>,
    pub attrs: AttrMap,
    pub ports: Vec<PortDecl>,
}

/// One port of a component class, with its reserved doctype annotation.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PortDecl {
    pub dir: Spanned<PortDir>,
    pub name: Spanned<String>,
    pub doctype: Option<StrLike>,
}

/// Which way a port faces. The direction is the alternative, never a string:
/// a `String` here would receive the generated variant spelling.
#[derive(Debug, Deserialize, PartialEq)]
pub enum PortDir {
    Into,
    Outof,
    Both,
}

/// `agent PersonalAssistant(slug: String) { ... }`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AgentClass {
    pub doc: Option<DocComment>,
    pub name: Spanned<String>,
    pub params: Option<ParamList>,
    pub attrs: AgentAttrs,
    pub mounts: Vec<MountStmt>,
    pub mcps: Vec<McpServerStmt>,
    pub subs: Vec<SubscribeStmt>,
    pub acls: Vec<AclStmt>,
    /// The named sub-blocks — `start_hooks` — held until their kindword is read.
    pub blocks: Vec<SectionNode>,
}

/// `assembly Deskbar(slug: String, driver: Agent) { ... }`. The body is the
/// top-level declaration vocabulary again.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AssemblyDef {
    pub doc: Option<DocComment>,
    pub name: Spanned<String>,
    pub params: ParamList,
    pub items: Vec<Spanned<Item>>,
}

/// A class's parameter list.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ParamList {
    pub params: Vec<Param>,
}

/// `skin: String = "bench"`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Param {
    pub name: Spanned<String>,
    /// The CamelCase type name. Which names are legal is the resolver's.
    pub ty: Spanned<String>,
    pub default: Option<Spanned<Value>>,
}

/// `surface alice_desk { ... }`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SurfaceDef {
    pub doc: Option<DocComment>,
    pub name: Spanned<String>,
    pub attrs: SurfaceAttrs,
    pub acls: Vec<AclStmt>,
    pub insts: Vec<NewStmt>,
}

/// `new p1: Protobar { in messages <- messages_p1; }`, or with an argument
/// list instead of a body. One form for components, agents and assemblies.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NewStmt {
    pub doc: Option<DocComment>,
    pub handle: Spanned<String>,
    pub cls: PathRef,
    pub args: Option<ArgList>,
    pub body: Option<InstBody>,
    /// Whether a `;` terminated the statement.
    pub semi: bool,
}

/// An instantiation's arguments.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ArgList {
    pub args: Vec<Arg>,
}

/// `slug = "alice-desk"` at an instantiation site.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Arg {
    pub name: Spanned<String>,
    pub value: Spanned<Value>,
}

/// An instance body: identity, authority and wiring.
///
/// `attrs` stays a map rather than a typed struct because which vocabulary
/// applies — surface component or top-level consumer — depends on the class,
/// which only resolution knows.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InstBody {
    pub attrs: AttrMap,
    pub bindings: Vec<Binding>,
    pub acls: Vec<AclStmt>,
}

/// A port connected to a channel, or a free io port tuned in place.
#[derive(Debug, Deserialize, PartialEq)]
pub enum Binding {
    /// `in messages <- messages_p1;`
    Into(DirBinding),
    /// `out takeover -> "local:brenn/takeover";`
    Outof(DirBinding),
    /// `io acks <-> acks;` or the free form `io tick { push_depth = 1; }`
    Both(IoBinding),
}

/// A directional binding: a port connected to a channel. One struct for `in`
/// and `out` — the direction is the `Binding` variant, which is the only thing
/// that ever distinguished the two.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DirBinding {
    pub port: Spanned<String>,
    pub chan: ChanRef,
    pub tail: Option<AttrBlock>,
    /// Whether a `;` terminated the statement.
    pub semi: bool,
}

/// A bidirectional binding, or a free io port when `target` is absent.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IoBinding {
    pub port: Spanned<String>,
    pub target: Option<ChanRef>,
    pub tail: Option<AttrBlock>,
    /// Whether a `;` terminated the statement.
    pub semi: bool,
}

/// What a binding or subscription names: a declared channel, or a literal
/// address where no declaration exists.
#[derive(Debug, Deserialize, PartialEq)]
pub enum ChanRef {
    Handle(PathRef),
    Addr(StrLike),
}

/// `acl subscribe [prefix "brenn:alice-desk."];`.
///
/// `plane` is the word as written. It reaches the model as text because it is
/// an identifier of the statement, not a value — the token-context projection
/// types apply in value positions.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AclStmt {
    pub plane: Spanned<String>,
    pub matchers: MatcherList,
}

/// The bracketed matcher list of an `acl` statement.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MatcherList {
    pub items: Vec<Matcher>,
}

/// `grant alice_pa subscribe prefix "brenn:alice-desk.";` — authority written
/// about another principal.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GrantStmt {
    pub principal: PathRef,
    pub plane: Spanned<String>,
    pub m: Matcher,
}

/// `remote reachy00 { ... }`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RemoteDef {
    pub doc: Option<DocComment>,
    pub name: Spanned<String>,
    pub attrs: RemoteAttrs,
    pub acls: Vec<AclStmt>,
}

/// `webhook push_alice { ... }`. The `signature`, `key` and
/// `replay_protection` sub-blocks are generic sections; their kindword decides
/// what each one is.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WebhookDef {
    pub doc: Option<DocComment>,
    pub name: Spanned<String>,
    pub attrs: WebhookAttrs,
    pub blocks: Vec<SectionNode>,
}

/// A `keyword name { key = value; … }` declaration: `repo life { remote = "…"; }`,
/// `mqtt_client broker { url = "…"; }`, `mcp_server graf { command = "graf"; }`.
///
/// One struct for all three, parameterized by the vocabulary its body admits.
/// The keyword is the only thing that distinguishes their *shape*, and the
/// `Item` variant already carries it; giving each a struct of its own would only
/// give them somewhere to drift apart.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamedAttrDef<A> {
    pub doc: Option<DocComment>,
    pub name: Spanned<String>,
    pub body: AttrBlock<A>,
}

/// What an agent body says about an mcp server: which form was written is what
/// decides reference from definition.
#[derive(Debug, Deserialize, PartialEq)]
pub enum McpServerStmt {
    /// `mcp_server graf;` — a reference to a top-level definition.
    Ref(Spanned<String>),
    /// `mcp_server pfin { … }` — a definition scoped to this agent, which is
    /// what lets its body name the class's parameters. Boxed: a definition is an
    /// order of magnitude larger than the reference it sits beside.
    Inline(Box<NamedAttrDef<McpServerAttrs>>),
}

/// `mount ws { working_dir = true; }` — always a reference, with a per-use
/// tail. Repos are defined only by `repo` declarations and `Repo` parameters.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MountStmt {
    pub repo: PathRef,
    pub tail: Option<AttrBlock>,
    /// Whether a `;` terminated the statement.
    pub semi: bool,
}

/// `subscribe alice_cmd { push_depth = 1000; }`. One form; the address scheme
/// decides which subscription family it lowers to.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SubscribeStmt {
    pub chan: ChanRef,
    pub tail: Option<AttrBlock>,
    /// Whether a `;` terminated the statement.
    pub semi: bool,
}

/// A braced run of `key = value;` entries: an entity body, or the tail of a
/// binding, mount or subscription.
///
/// `A` is the vocabulary the entries deserialize into. It defaults to `AttrMap`
/// — an untyped body — which is what the tails carry: which keys a binding or
/// mount tail admits depends on the channel family or the repo it attaches to,
/// and only resolution knows that.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AttrBlock<A = AttrMap> {
    pub attrs: A,
}

// ── entity attr vocabularies ─────────────────────────────────────────────────
//
// One struct per entity, each a transcription of the config struct the entity
// lowers to: the DSL key is that struct's field name, never renamed, so the
// vocabulary is legible by reading the two side by side. What a statement of the
// entity's body carries is not an attr — a surface's components are `new`
// statements, its ACLs are `acl` statements — and the per-struct docs name what
// was left out for that reason.
//
// Every field is an `Attr<T>`: the key selected the field, so what the bridge
// hands over is the entry minus its key. `Option` means the key may be omitted;
// a bare `Attr` means it is required, and omitting it is a positioned error.
// Values are `Spanned<Value>` except in token contexts, which take `Word`,
// `WordList` or `IntOrWord` so that a bare identifier there is recorded as a
// word rather than resolved as a name.
//
// A config field with no entry here is inexpressible from the DSL until the
// vocabulary catches up, and says so as an unknown-key error at the offending
// key. That is the priced cost of transcription; the fields left out are the
// ones whose config shape is a nested table, which needs a sub-block form the
// grammar gives generic sections and these bodies do not have yet.
//
// TODO(dsl-vocabulary-config-parity): nothing mechanically ties a vocabulary to
// the config struct it transcribes. `brenn-dsl` does not depend on `brenn-lib`,
// so a field added to a config struct cannot break a build here; it surfaces as
// "not a key" at whoever writes the key months later. What is wanted is a gate
// on the config side — reflected field names against a table of (config struct,
// vocabulary, deliberately omitted fields and why) — and where it lives is a
// question this crate cannot answer alone.

/// Declare attr vocabularies: the closed key sets a typed body admits.
///
/// One entry per key: `opt` where the key may be omitted, `req` where omitting
/// it is a positioned error, then the type the value projects to.
///
/// The macro is what makes the conventions hold rather than merely stating
/// them. It emits `deny_unknown_fields` on every struct — forget it on one
/// vocabulary and unknown keys are silently accepted, which is the failure the
/// whole typed layer exists to prevent — and wraps every field in `Attr<…>`,
/// which is the shape the bridge hands a keyed-region element over in.
macro_rules! vocabulary {
    ($(
        $(#[$struct_meta:meta])*
        struct $name:ident {
            $($(#[$field_meta:meta])* $mode:ident $key:ident: $ty:ty,)+
        }
    )+) => {
        $(
            $(#[$struct_meta])*
            #[derive(Debug, Deserialize, PartialEq)]
            #[serde(deny_unknown_fields)]
            pub struct $name {
                $($(#[$field_meta])* pub $key: vocabulary_field!($mode $ty),)+
            }
        )+
    };
}

/// The declared type of one vocabulary field: optional or required.
macro_rules! vocabulary_field {
    (opt $ty:ty) => { Option<Attr<$ty>> };
    (req $ty:ty) => { Attr<$ty> };
}

vocabulary! {
    /// A `channel` body: the tuning that rides either channel role.
    ///
    /// `uuid`, `address` and `address_prefix` are carried by the statement — the
    /// address by the declaration itself, the uuid by a `uuid_pins` block.
    struct ChannelAttrs {
        opt description: Spanned<Value>,
        /// Depth fields take a count or the word `unbounded`.
        opt push_depth: IntOrWord,
        opt retain_depth: IntOrWord,
        opt standing_retain_depth: IntOrWord,
        opt noise: Word,
        opt sink: Word,
        opt wake_min: Word,
        opt send_rate: Spanned<Value>,
    }

    /// A `surface` body's attrs.
    ///
    /// Components are `new` statements, subscriptions and outputs and io ports
    /// are bindings, and the four ACL lists are `acl` statements.
    struct SurfaceAttrs {
        /// Required, like its config counterpart: transport rights are
        /// deny-by-default and the operator states the intent, so an absent
        /// `grants` is a positioned refusal here rather than a posture nothing
        /// wrote down.
        req grants: WordList,
        opt skin: Spanned<Value>,
        opt allowed_users: Spanned<Value>,
        opt publish_burst: Spanned<Value>,
        opt publish_per_sec: Spanned<Value>,
    }

    /// An `agent` body's attrs.
    ///
    /// Mounts, mcp servers, subscriptions and ACLs are statements; the hook
    /// lists are named sub-blocks. The config's nested tables — approval rules,
    /// attachment targets, tool grants, per-integration config, frontmatter
    /// rendering, pwa push — have no attr spelling and are unknown keys here.
    struct AgentAttrs {
        opt name: Spanned<Value>,
        opt description: Spanned<Value>,
        opt icon: Spanned<Value>,
        opt working_dir: Spanned<Value>,
        opt model: Spanned<Value>,
        opt single_instance: Spanned<Value>,
        opt singleton: Spanned<Value>,
        opt persistent: Spanned<Value>,
        opt multiuser: Spanned<Value>,
        opt idle_timeout_secs: Spanned<Value>,
        opt idle_hook_secs: Spanned<Value>,
        opt compact_reminder_pct: Spanned<Value>,
        opt compact_soft_pct: Spanned<Value>,
        opt compact_red_pct: Spanned<Value>,
        opt compact_hard_pct: Spanned<Value>,
        opt compact_reminder_tokens: Spanned<Value>,
        opt compact_soft_tokens: Spanned<Value>,
        opt compact_red_tokens: Spanned<Value>,
        opt compact_hard_tokens: Spanned<Value>,
        opt compact_idle_secs: Spanned<Value>,
        opt history_replay_limit: Spanned<Value>,
        opt allowed_users: Spanned<Value>,
        opt disabled_tools: Spanned<Value>,
        opt cc_extra_args: Spanned<Value>,
        opt integrations: Spanned<Value>,
        opt extra_mounts: Spanned<Value>,
        opt prefix_username: Spanned<Value>,
        opt prefix_timestamp: Spanned<Value>,
        opt prefix_device: Spanned<Value>,
        opt container: Spanned<Value>,
        opt container_working_dir: Spanned<Value>,
        /// Optional, unlike a surface's or a remote's: an agent's config
        /// counterpart defaults, and absent there means no grants.
        opt grants: WordList,
    }

    /// A `remote` body's attrs. The four ACL lists are `acl` statements.
    struct RemoteAttrs {
        /// Required: a remote authenticates with a bearer token and is unusable
        /// without one.
        req token_file: Spanned<Value>,
        /// Required, for the reason `SurfaceAttrs::grants` is.
        req grants: WordList,
        opt publish_burst: Spanned<Value>,
        opt publish_per_sec: Spanned<Value>,
        opt max_sessions: Spanned<Value>,
        opt max_subscriptions: Spanned<Value>,
    }

    /// A `webhook` body's attrs. The `signature`, `key`, `token` and
    /// `replay_protection` blocks are sub-blocks, typed by their kindword.
    struct WebhookAttrs {
        opt mount: Spanned<Value>,
        opt description: Spanned<Value>,
        opt transport_ceiling_bytes: Spanned<Value>,
        opt content_type: Spanned<Value>,
        opt urgency: Word,
    }

    /// A `repo` body's attrs.
    struct RepoAttrs {
        req remote: Spanned<Value>,
        opt auto_pull: Spanned<Value>,
    }

    /// An `mqtt_client` body's attrs. The last-will table has no attr spelling.
    struct MqttClientAttrs {
        req url: Spanned<Value>,
        opt username: Spanned<Value>,
        opt password_file: Spanned<Value>,
        opt ca_file: Spanned<Value>,
        opt tls_version_min: Spanned<Value>,
        opt keepalive_secs: Spanned<Value>,
        opt inbound_payload_cap_bytes: Spanned<Value>,
        opt reconnect_backoff_initial_secs: Spanned<Value>,
        opt reconnect_backoff_max_secs: Spanned<Value>,
        opt session_expiry_secs: Spanned<Value>,
        opt qos: Spanned<Value>,
        opt urgency: Word,
    }

    /// An `mcp_server` body's attrs, top-level or inline in an agent.
    struct McpServerAttrs {
        req command: Spanned<Value>,
        opt args: Spanned<Value>,
        opt env: Spanned<Value>,
    }
}

/// A generic section, held as its CST subtree until its kindword says what it
/// is.
///
/// The kindword is data, not a keyword, so the sum dispatch cannot type a
/// section. `Raw` captures the node as a cheap handle without walking it; the
/// dispatch functions below re-enter the bridge with a per-context target type.
pub type SectionNode = Raw<cst::Section>;

// ── typed sections ───────────────────────────────────────────────────────────
//
// Phase two of the two-phase pattern. A held section is re-entered with the
// target its kindword selects, so everything a keyword-led form gets —
// `deny_unknown_fields`, positioned unknown-key and duplicate-key errors — a
// generic section gets too, inside its own body.
//
// The dispatch tables are fixed in code, one match per context. A new legal
// kindword is a table entry and a target type; the grammar never changes.

/// A section whose kindword selected `A` as its body vocabulary.
///
/// The kindword and the optional name are carried rather than dropped: a `key
/// primary { … }` block's identity is its name, and a diagnostic about the
/// block cites its kindword.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TypedBlock<A> {
    pub doc: Option<DocComment>,
    pub kindword: Spanned<String>,
    pub name: Option<Spanned<String>>,
    pub attrs: A,
    /// Sub-blocks, still held: no typed section nests a second level yet.
    pub subs: Vec<SectionNode>,
}

vocabulary! {
    /// The webhook `signature` block: the union of every scheme's fields.
    ///
    /// The raw config's shape is an internally tagged enum, which the bridge
    /// cannot mirror, so the model takes the union with every variant-specific
    /// field optional and `scheme` the one required key. Which fields the named
    /// scheme actually requires — and which are errors for it — is checked at
    /// lowering, against the raw enum as the source of truth.
    struct SignatureAttrs {
        req scheme: Word,
        opt algorithm: Spanned<Value>,
        opt header: Spanned<Value>,
        opt format: Spanned<Value>,
        opt key_id_header: Spanned<Value>,
        opt sig_header: Spanned<Value>,
        opt sig_format: Spanned<Value>,
        opt timestamp_header: Spanned<Value>,
        opt template: Spanned<Value>,
        opt max_skew_secs: Spanned<Value>,
        opt token_id_header: Spanned<Value>,
    }

    /// `key primary { secret_file = "…"; }`, and its bearer-scheme `token`
    /// counterpart.
    ///
    /// One struct for both, because both are one secret named by the block: the
    /// id is the name, and which credential kind it is is the kindword.
    struct SecretFileAttrs {
        req secret_file: Spanned<Value>,
    }

    /// `replay_protection { component_path = …; store_path = …; }`.
    ///
    /// `component_path` is a plain attr, not a class reference: the replay guard
    /// is webhook plumbing the system instantiates itself — no ports, no
    /// bindings.
    struct ReplayProtectionAttrs {
        req component_path: Spanned<Value>,
        req store_path: Spanned<Value>,
        opt store_size_limit: Spanned<Value>,
        opt config: Spanned<Value>,
    }

    /// A hook block: `start_hooks { host = [...]; container = [...]; }`, and its
    /// `post_pull_hooks` and `startup_hooks` siblings.
    ///
    /// One struct for all three, because the config's three hook tables are the
    /// same two lists; which point in an agent's life the hooks run at is the
    /// kindword.
    struct HooksAttrs {
        opt host: Spanned<Value>,
        opt container: Spanned<Value>,
    }
}

/// One context's kindword vocabulary: the sum it deserializes into, the legal
/// set the diagnostic names, the name arity of each kindword, and the dispatch
/// that selects between them, all from one list.
///
/// The parts have to agree and nothing else makes them: a kindword missing from
/// the legal set would give a correct parse and a wrong error message, and a
/// block whose identity is its name — `container cc`, `key primary` — written
/// without one would otherwise deserialize into something nothing can reference.
macro_rules! kindword_dispatch {
    (
        $(#[$enum_doc:meta])* enum $sum:ident;
        $(#[$const_doc:meta])* const $kindwords:ident;
        $(#[$fn_doc:meta])* fn $dispatch:ident;
        context $context:literal;
        $($word:literal $arity:ident => $variant:ident($attrs:ty)),+ $(,)?
    ) => {
        $(#[$enum_doc])*
        #[derive(Debug, PartialEq)]
        pub enum $sum {
            $($variant(Box<TypedBlock<$attrs>>),)+
        }

        $(#[$const_doc])*
        pub const $kindwords: &[&str] = &[$($word),+];

        $(#[$fn_doc])*
        pub fn $dispatch(node: &SectionNode) -> Result<$sum, Diagnostic> {
            let (kindword, span) = section_kindword(node);
            match kindword.as_str() {
                $($word => typed_block(node)
                    .and_then(|block| check_name(block, $word, &span, kindword_named!($arity)))
                    .map($sum::$variant),)+
                _ => Err(unknown_kindword(&kindword, span, $context, $kindwords)),
            }
        }
    };
}

/// Whether a kindword's block is identified by a name.
macro_rules! kindword_named {
    (named) => {
        true
    };
    (unnamed) => {
        false
    };
}

kindword_dispatch! {
    /// A sub-block of a `webhook` body, typed by its kindword.
    enum WebhookBlock;
    /// The kindwords a `webhook` body admits.
    const WEBHOOK_BLOCK_KINDWORDS;
    /// Type a `webhook` body's held sub-block.
    fn webhook_block;
    context "a webhook body";
    "signature" unnamed => Signature(SignatureAttrs),
    "key" named => Key(SecretFileAttrs),
    "token" named => Token(SecretFileAttrs),
    "replay_protection" unnamed => ReplayProtection(ReplayProtectionAttrs),
}

kindword_dispatch! {
    /// A sub-block of an `agent` body, typed by its kindword.
    enum AgentBlock;
    /// The kindwords an `agent` body admits.
    const AGENT_BLOCK_KINDWORDS;
    /// Type an `agent` body's held sub-block.
    fn agent_block;
    context "an agent body";
    "start_hooks" unnamed => StartHooks(HooksAttrs),
    "post_pull_hooks" unnamed => PostPullHooks(HooksAttrs),
    "startup_hooks" unnamed => StartupHooks(HooksAttrs),
}

// ── top-level configuration sections ─────────────────────────────────────────
//
// The declaration forms cover what a `.brenn` document says about entities:
// agents, channels, surfaces, remotes, webhooks, repos, mqtt clients, mcp
// servers. What is left of the configuration is the server's own settings, and
// those are written as generic sections — one per top-level config table, typed
// here by the same two-phase dispatch the sub-blocks use.
//
// A key is optional when its config counterpart has a default; required when it
// does not. `server`'s `public_url` is the one exception, and it is not a
// contradiction: its config counterpart is an `Option` whose absence is a
// load-time panic, and a positioned refusal at the block is what this layer can
// do better.
//
// What has no spelling at all, and is not an oversight: `integrations` and a
// container's per-integration overrides are maps of arbitrary nested tables with
// no key vocabulary to transcribe, and `repo_dir` is a bare top-level scalar,
// which the grammar has no form for — a document says `repo_dir` nowhere,
// because there is no top-level attr.
//
// Everything else in `BrennConfig` that is not a section here has a declaration
// form instead: channels, surfaces, agents, remotes, mqtt clients, webhooks,
// repos, mcp servers, connections and wasm consumers are written as
// declarations, not as configuration tables.

vocabulary! {
    /// The `server` section: the socket, the asset directories, and what the
    /// server believes about what is in front of it.
    struct ServerAttrs {
        opt bind_address: Spanned<Value>,
        opt static_dir: Spanned<Value>,
        opt surface_dist_dir: Spanned<Value>,
        opt secure_cookies: Spanned<Value>,
        opt trusted_proxy_hops: Spanned<Value>,
        opt pid_file: Spanned<Value>,
        /// Required: a server without it does not start, and refusing the block
        /// here says so at the block instead of at startup with no position.
        req public_url: Spanned<Value>,
    }

    /// The `database` section.
    struct DatabaseAttrs {
        opt path: Spanned<Value>,
    }

    /// The `logging` section. The two levels are token contexts: a level is a
    /// bare word here, and which words name a level is lowering's table.
    struct LoggingAttrs {
        opt log_dir: Spanned<Value>,
        opt console_level: Word,
        opt file_level: Word,
    }

    /// The `security` section: the rate-limit buckets and the body-size caps.
    struct SecurityAttrs {
        opt auth_rate_interval_secs: Spanned<Value>,
        opt auth_rate_burst: Spanned<Value>,
        opt global_rate_interval_secs: Spanned<Value>,
        opt global_rate_burst: Spanned<Value>,
        opt asset_rate_interval_secs: Spanned<Value>,
        opt asset_rate_burst: Spanned<Value>,
        opt auth_body_limit: Spanned<Value>,
        opt global_body_limit: Spanned<Value>,
        opt upload_body_limit: Spanned<Value>,
        opt max_image_long_edge: Spanned<Value>,
    }

    /// The `alerting` section: the shared rate limit. Which backend delivers the
    /// alerts is the `ntfy` or `mail` sub-block.
    ///
    /// Both keys are required: an alerting section that names no limit says
    /// nothing about the only thing it is for.
    struct AlertingAttrs {
        req max_alerts: Spanned<Value>,
        req window_secs: Spanned<Value>,
    }

    /// `ntfy { url = "…"; }`.
    struct NtfyAttrs {
        req url: Spanned<Value>,
    }

    /// `mail { to = "…"; }`.
    struct MailAttrs {
        req to: Spanned<Value>,
        opt subject_label: Spanned<Value>,
    }

    /// The `claude_defaults` section: what every agent starts from.
    struct ClaudeDefaultsAttrs {
        opt mcp_script_path: Spanned<Value>,
        opt model: Spanned<Value>,
    }

    /// The `repo_sync` section: how often repos are polled, and when a pending
    /// event is too old to inject.
    struct RepoSyncAttrs {
        opt poll_interval_secs: Spanned<Value>,
        opt stale_conversation_days: Spanned<Value>,
    }

    /// The `messaging` section: the bus-wide defaults a `channel` body
    /// overrides.
    ///
    /// The three defaults that name a policy are token contexts, exactly as
    /// their per-channel counterparts in [`ChannelAttrs`] are.
    struct MessagingAttrs {
        opt default_send_budget: Spanned<Value>,
        opt max_body_bytes: Spanned<Value>,
        opt default_noise: Word,
        opt default_sink: Word,
        opt default_wake_min: Word,
        opt archive_path: Spanned<Value>,
        opt default_send_rate: Spanned<Value>,
    }

    /// The `observability` section. The usage sub-section is a `usage`
    /// sub-block.
    struct ObservabilityAttrs {
        opt surface_error_channel: Spanned<Value>,
        opt surface_error_publish_floor: Word,
    }

    /// `usage { session_gap_minutes = 30; }`.
    struct UsageAttrs {
        opt session_gap_minutes: Spanned<Value>,
    }

    /// The `surface_description` section: the namespace the derived help and
    /// geometry channels hang under, and the heartbeat cadence.
    struct SurfaceDescriptionAttrs {
        opt prefix: Spanned<Value>,
        opt status_interval_secs: Spanned<Value>,
    }

    /// The `llm_chat` section: chat-over-pubsub.
    struct LlmChatAttrs {
        opt prefix: Spanned<Value>,
        opt retained_window: Spanned<Value>,
        opt wake_min: Word,
        opt idle_timeout_secs: Spanned<Value>,
    }

    /// The `pwa_push` section: the VAPID identity and the endpoint allowlist.
    struct PwaPushAttrs {
        opt keypair_file: Spanned<Value>,
        opt subject: Spanned<Value>,
        opt endpoint_host_allowlist: Spanned<Value>,
        opt endpoint_host_allowlist_enforce: Spanned<Value>,
    }

    /// The `automation` section: the per-job caps.
    struct AutomationAttrs {
        opt max_fires_per_hour_per_job: Spanned<Value>,
        opt max_error_reports_per_hour_per_job: Spanned<Value>,
        opt consecutive_failures_to_disable: Spanned<Value>,
        opt max_jobs_per_app: Spanned<Value>,
    }

    /// The `events` section: how long a delivered event is kept.
    struct EventsAttrs {
        opt delivered_retention_days: Spanned<Value>,
    }

    /// The `wasm` section: the host-wide default store cap.
    struct WasmAttrs {
        opt store_size_limit: Spanned<Value>,
    }

    /// The `watchdog` section: the bridge-wedge sweep.
    struct WatchdogAttrs {
        opt sweep_interval_secs: Spanned<Value>,
        opt wedge_grace_secs: Spanned<Value>,
    }

    /// `container cc { image = "…"; home_dir = "…"; }` — the block's name is the
    /// name an agent references.
    ///
    /// `image` and `home_dir` are required; the home directory is the
    /// container's persistent state root.
    struct ContainerAttrs {
        req image: Spanned<Value>,
        req home_dir: Spanned<Value>,
        opt container_home: Spanned<Value>,
        opt extra_mounts: Spanned<Value>,
        opt extra_args: Spanned<Value>,
    }
}

kindword_dispatch! {
    /// A top-level configuration section, typed by its kindword.
    enum ConfigBlock;
    /// The kindwords a document admits as a top-level section.
    const CONFIG_BLOCK_KINDWORDS;
    /// Type a top-level held section.
    fn config_block;
    context "a document";
    "server" unnamed => Server(ServerAttrs),
    "database" unnamed => Database(DatabaseAttrs),
    "logging" unnamed => Logging(LoggingAttrs),
    "security" unnamed => Security(SecurityAttrs),
    "alerting" unnamed => Alerting(AlertingAttrs),
    "claude_defaults" unnamed => ClaudeDefaults(ClaudeDefaultsAttrs),
    "repo_sync" unnamed => RepoSync(RepoSyncAttrs),
    "messaging" unnamed => Messaging(MessagingAttrs),
    "observability" unnamed => Observability(ObservabilityAttrs),
    "surface_description" unnamed => SurfaceDescription(SurfaceDescriptionAttrs),
    "llm_chat" unnamed => LlmChat(LlmChatAttrs),
    "pwa_push" unnamed => PwaPush(PwaPushAttrs),
    "automation" unnamed => Automation(AutomationAttrs),
    "events" unnamed => Events(EventsAttrs),
    "wasm" unnamed => Wasm(WasmAttrs),
    "watchdog" unnamed => Watchdog(WatchdogAttrs),
    "container" named => Container(ContainerAttrs),
}

kindword_dispatch! {
    /// The backend an `alerting` section delivers through.
    enum AlertingBlock;
    /// The kindwords an `alerting` body admits.
    const ALERTING_BLOCK_KINDWORDS;
    /// Type an `alerting` body's held sub-block.
    fn alerting_block;
    context "an alerting body";
    "ntfy" unnamed => Ntfy(NtfyAttrs),
    "mail" unnamed => Mail(MailAttrs),
}

kindword_dispatch! {
    /// A sub-block of an `observability` section.
    enum ObservabilityBlock;
    /// The kindwords an `observability` body admits.
    const OBSERVABILITY_BLOCK_KINDWORDS;
    /// Type an `observability` body's held sub-block.
    fn observability_block;
    context "an observability body";
    "usage" unnamed => Usage(UsageAttrs),
}

/// The kindword a held section leads with, and where it was written.
///
/// A section node always carries exactly one kindword — the grammar admits no
/// other shape — so a node that does not is a broken tree, not bad input.
pub fn section_kindword(node: &SectionNode) -> (String, Span) {
    let section = node.node().read();
    let kindword = section
        .child_kindword()
        .expect("a section node carries exactly one kindword");
    let span = kindword.read().span().clone();
    let text = span
        .text_str()
        .expect("a parsed node's span carries its source")
        .to_owned();
    (text, span)
}

/// Re-enter the bridge with the target the kindword selected.
fn typed_block<A>(node: &SectionNode) -> Result<Box<TypedBlock<A>>, Diagnostic>
where
    TypedBlock<A>: serde::de::DeserializeOwned,
{
    crate::de::from_section_cst::<TypedBlock<A>>(node.node())
        .map(Box::new)
        .map_err(Diagnostic::from_deserialize_error)
}

/// Refuse a block written with the wrong name arity for its kindword.
///
/// A `container` or a `key` is identified by its name, so one written without
/// one defines something nothing can reference; a `server` has no name to carry,
/// so one written with a name says something the document then drops.
fn check_name<A>(
    block: Box<TypedBlock<A>>,
    kindword: &str,
    span: &Span,
    named: bool,
) -> Result<Box<TypedBlock<A>>, Diagnostic> {
    match (named, block.name.as_ref()) {
        (true, None) => Err(Diagnostic::at(
            format!("a `{kindword}` block is named: `{kindword} <name> {{ … }}`"),
            span.clone(),
        )),
        (false, Some(name)) => Err(Diagnostic::at(
            format!("a `{kindword}` block takes no name"),
            name.span().clone(),
        )),
        _ => Ok(block),
    }
}

fn unknown_kindword(kindword: &str, span: Span, context: &str, legal: &[&str]) -> Diagnostic {
    Diagnostic::at(
        format!(
            "`{kindword}` is not a block {context} admits; expected one of {}",
            legal.join(", ")
        ),
        span,
    )
}

/// The doc comment attached to a declaration, one entry per `///` line.
///
/// Attachment is positional and always to the following declaration; blank
/// lines and `//` comments in between do not detach it.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DocComment {
    pub lines: Vec<Spanned<String>>,
}

/// A value in any value position.
#[derive(Debug, Deserialize, PartialEq)]
pub enum Value {
    Str(StrLit),
    Fstr(FStr),
    Raw(Spanned<String>),
    Int(Spanned<i64>),
    Flt(Spanned<f64>),
    Bool(Spanned<bool>),
    Ref(PathRef),
    List(ValueList),
    Table(InlineTable),
    M(Matcher),
}

/// `[subscribe, publish, takeover]`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ValueList {
    pub items: Vec<Spanned<Value>>,
}

/// `{ retain_depth = 64, push_depth = 8 }`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InlineTable {
    pub entries: AttrMap,
}

/// `prefix "brenn:alice-desk."`, with an optional attribute tail.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Matcher {
    pub kind: Spanned<String>,
    pub val: MatcherVal,
    pub tail: Option<InlineTable>,
}

/// A matcher's payload: a literal address, or a path naming a channel.
#[derive(Debug, Deserialize, PartialEq)]
pub enum MatcherVal {
    Lit(StrLike),
    Chan(PathRef),
}

/// What a value is, for a diagnostic that has to say what it found.
fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Str(_) => "a string",
        Value::Fstr(_) => "an f-string",
        Value::Raw(_) => "a raw string",
        Value::Int(_) => "an integer",
        Value::Flt(_) => "a float",
        Value::Bool(_) => "a boolean",
        Value::Ref(_) => "a reference",
        Value::List(_) => "a list",
        Value::Table(_) => "an inline table",
        Value::M(_) => "a matcher",
    }
}

/// A bare identifier written where a value goes: `abi = dom;`, `access =
/// container;`, a plane word, a matcher kind.
///
/// A token context is what makes sigil-free references safe — in these
/// positions an identifier is a word, not a name to resolve. Which words are
/// legal is not checked here: the spelling tables live in the config enums and
/// are consulted at lowering. What is recorded is that this position is a
/// token.
#[derive(Debug, Clone, PartialEq)]
pub struct Word {
    pub name: Spanned<String>,
}

impl Word {
    /// Project a value written in a token context.
    ///
    /// A dotted or `::`-qualified reference is refused: a word has exactly one
    /// segment, and anything longer is a name someone expected to resolve.
    pub fn from_value(value: &Spanned<Value>) -> Result<Word, Diagnostic> {
        match value.value() {
            Value::Ref(path) if path.segs.is_empty() => Ok(Word {
                name: path.head.clone(),
            }),
            Value::Ref(_) => Err(Diagnostic::at(
                "expected a bare word, found a qualified reference",
                value.span().clone(),
            )),
            other => Err(Diagnostic::at(
                format!("expected a bare word, found {}", value_kind(other)),
                value.span().clone(),
            )),
        }
    }

    /// The word as written.
    pub fn as_str(&self) -> &str {
        self.name.value()
    }
}

/// A list of bare identifiers: `grants = [subscribe, publish, takeover];`.
#[derive(Debug, Clone, PartialEq)]
pub struct WordList {
    pub words: Vec<Word>,
}

impl WordList {
    /// Project a value written where a list of tokens goes.
    pub fn from_value(value: &Spanned<Value>) -> Result<WordList, Diagnostic> {
        let Value::List(list) = value.value() else {
            return Err(Diagnostic::at(
                format!(
                    "expected a list of bare words, found {}",
                    value_kind(value.value())
                ),
                value.span().clone(),
            ));
        };
        // The element's own position survives a direct call and is lost on the
        // bridge path, where the span is re-attached at the whole list. Naming
        // the element in the message is what keeps the refusal findable there.
        let words = list
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                Word::from_value(item).map_err(|mut error| {
                    error.message = format!("element {}: {}", index + 1, error.message);
                    error
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(WordList { words })
    }
}

/// A field that takes an integer or one token standing in for it — the depth
/// fields, where `unbounded` is a word and every other value is a count.
#[derive(Debug, Clone, PartialEq)]
pub enum IntOrWord {
    Int(Spanned<i64>),
    Word(Word),
}

impl IntOrWord {
    /// Project a value written where either arm is legal.
    pub fn from_value(value: &Spanned<Value>) -> Result<IntOrWord, Diagnostic> {
        match value.value() {
            Value::Int(count) => Ok(IntOrWord::Int(count.clone())),
            Value::Ref(_) => Word::from_value(value).map(IntOrWord::Word),
            other => Err(Diagnostic::at(
                format!(
                    "expected an integer or a bare word, found {}",
                    value_kind(other)
                ),
                value.span().clone(),
            )),
        }
    }
}

/// The three projections deserialize by reading the value and projecting it.
///
/// Reading `Spanned<Value>` first is what carries the span into the message.
/// The error goes out through `serde::de::Error::custom`, which drops it — the
/// bridge positions it again at the frame that knows where the value was — so
/// the span here only serves the direct `from_value` callers.
macro_rules! projection_deserialize {
    ($target:ty) => {
        impl<'de> Deserialize<'de> for $target {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = Spanned::<Value>::deserialize(deserializer)?;
                <$target>::from_value(&value)
                    .map_err(|error| serde::de::Error::custom(error.message))
            }
        }
    };
}

projection_deserialize!(Word);
projection_deserialize!(WordList);
projection_deserialize!(IntOrWord);

/// Anywhere a string literal or an f-string is equally legal.
#[derive(Debug, Deserialize, PartialEq)]
pub enum StrLike {
    Str(StrLit),
    Fstr(FStr),
}

/// A plain string: escapes, never interpolation.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StrLit {
    pub parts: Vec<StrPart>,
}

/// A piece of a plain string. Escape decoding is resolver work; the model
/// carries the pieces so a diagnostic can cite the offending one.
#[derive(Debug, Deserialize, PartialEq)]
pub enum StrPart {
    Esc(Spanned<String>),
    Frag(Spanned<String>),
}

/// An f-string: escapes, `{{`/`}}` for literal braces, and `{path}`
/// interpolation.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FStr {
    pub parts: Vec<FStrPart>,
}

/// A piece of an f-string.
#[derive(Debug, Deserialize, PartialEq)]
pub enum FStrPart {
    Esc(Spanned<String>),
    Brace(Spanned<BraceEscape>),
    Interp(PathRef),
    Frag(Spanned<String>),
}

/// Which literal brace `{{` or `}}` stood for.
#[derive(Debug, Deserialize, PartialEq)]
pub enum BraceEscape {
    Open,
    Close,
}

/// A dotted and/or `::`-qualified reference. Whether the segments name modules,
/// instances, or a mix of both is resolution's problem.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PathRef {
    pub head: Spanned<String>,
    pub segs: Vec<PathSeg>,
}

/// One segment of a path.
///
/// The separator is what the variant says; the payload is the name it
/// introduced, which is why both variants carry the same shape.
#[derive(Debug, Deserialize, PartialEq)]
pub enum PathSeg {
    /// `::name` — a module qualification.
    Module(Seg),
    /// `.name` — an entity inside an instantiated assembly.
    Inst(Seg),
}

/// The name a path segment introduced.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Seg {
    pub name: Spanned<String>,
}

/// A body's `key = value;` entries, in source order.
///
/// A keyed region, so a key repeated in one body is refused before this sees
/// either entry. Order is preserved because a later diagnostic that cites two
/// entries should cite them in the order they were written.
#[derive(Debug, Default, PartialEq)]
pub struct AttrMap {
    entries: Vec<(String, Spanned<Value>)>,
}

impl AttrMap {
    /// The entries, in source order.
    pub fn entries(&self) -> &[(String, Spanned<Value>)] {
        &self.entries
    }

    /// The value written for `key`, if any.
    pub fn get(&self, key: &str) -> Option<&Spanned<Value>> {
        self.entries
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }

    /// How many entries were written.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the body wrote no entries at all.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One `key = value;` entry as its target sees it.
///
/// The key is what selected the field, so the element the bridge hands over is
/// the entry minus its key. Every field of a typed attr vocabulary is therefore
/// an `Attr<T>`, never a bare `T`.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Attr<T> {
    pub value: T,
}

impl<'de> Deserialize<'de> for AttrMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(AttrMapVisitor)
    }
}

struct AttrMapVisitor;

impl<'de> Visitor<'de> for AttrMapVisitor {
    type Value = AttrMap;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a body of `key = value;` entries")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<AttrMap, A::Error> {
        let mut entries = Vec::new();
        while let Some((key, entry)) = access.next_entry::<String, Attr<Spanned<Value>>>()? {
            entries.push((key, entry.value));
        }
        Ok(AttrMap { entries })
    }
}

#[cfg(test)]
mod tests {
    //! What only a crate-internal caller can reach: re-entering a held section
    //! with a target this crate does not export a dispatch for.

    use super::*;
    use crate::parse_str;

    /// Type a held section with a caller-chosen vocabulary.
    fn typed<A>(node: &SectionNode) -> Result<TypedBlock<A>, Diagnostic>
    where
        TypedBlock<A>: serde::de::DeserializeOwned,
    {
        crate::de::from_section_cst::<TypedBlock<A>>(node.node())
            .map_err(Diagnostic::from_deserialize_error)
    }

    /// A section's `subs` are the sections written inside it, and they stay out
    /// of the enclosing body's attrs: the recursion is structural, not flattened.
    #[test]
    fn a_nested_section_lands_in_its_parents_subs() {
        let file = parse_str(
            "server {\n    bind = \"127.0.0.1:3000\";\n    tls alpha {\n        cert = \"/etc/brenn/alice.pem\";\n    }\n}\n",
            "t.brenn",
        )
        .expect("a parse");
        let held = file.sections().next().expect("one top-level section");

        let block: TypedBlock<AttrMap> = typed(held).expect("a generic body");
        assert_eq!(block.kindword.value(), "server");
        assert!(block.attrs.get("bind").is_some());
        assert!(
            block.attrs.get("tls").is_none(),
            "the sub-block is not an attr of its parent"
        );

        assert_eq!(block.subs.len(), 1);
        let inner: TypedBlock<AttrMap> = typed(&block.subs[0]).expect("the nested body");
        assert_eq!(inner.kindword.value(), "tls");
        assert_eq!(inner.name.expect("the block is named").value(), "alpha");
        assert!(inner.attrs.get("cert").is_some());
        assert!(inner.subs.is_empty());
    }

    /// The vocabulary the list and integer projections are exercised through.
    ///
    /// Its own, rather than a shipped one: what is under test is the bridge path
    /// — which is not the path a direct `from_value` call takes — and it should
    /// stay under test however the shipped vocabularies come to spell their
    /// fields.
    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct ProjectionAttrs {
        planes: Attr<WordList>,
        depth: Attr<IntOrWord>,
    }

    #[test]
    fn the_list_and_integer_projections_carry_their_values_through_the_bridge() {
        let file = parse_str(
            "probe {\n    planes = [subscribe, publish];\n    depth = unbounded;\n}\n",
            "t.brenn",
        )
        .expect("a parse");
        let held = file.sections().next().expect("one section");
        let block: TypedBlock<ProjectionAttrs> = typed(held).expect("both projections");

        let spellings: Vec<&str> = block
            .attrs
            .planes
            .value
            .words
            .iter()
            .map(Word::as_str)
            .collect();
        assert_eq!(spellings, ["subscribe", "publish"]);

        let IntOrWord::Word(word) = &block.attrs.depth.value else {
            panic!("the word arm");
        };
        assert_eq!(word.as_str(), "unbounded");
    }

    /// On the bridge path a bad list element is positioned at the whole list —
    /// hence the element ordinal in the message — while a bad scalar value is
    /// positioned at the value.
    #[test]
    fn a_projection_refusal_is_positioned_at_the_list_or_at_the_value() {
        let file = parse_str(
            "probe {\n    planes = [subscribe, \"publish\"];\n    depth = 8;\n}\n",
            "t.brenn",
        )
        .expect("a parse");
        let held = file.sections().next().expect("one section");
        let error = typed::<ProjectionAttrs>(held).expect_err("the second element is a string");
        assert_eq!(
            error.message,
            "element 2: expected a bare word, found a string"
        );
        // The span lands on the whole list, not on the element — hence the
        // element number in the message.
        // TODO(dsl-list-element-span): narrow this to the offending element.
        assert_eq!(error.line_col(), Some((2, 14)));

        let file = parse_str(
            "probe {\n    planes = [subscribe];\n    depth = \"8\";\n}\n",
            "t.brenn",
        )
        .expect("a parse");
        let held = file.sections().next().expect("one section");
        let error = typed::<ProjectionAttrs>(held).expect_err("a quoted count is not a count");
        assert!(
            error.message.contains("integer or a bare word"),
            "{}",
            error.message
        );
        assert_eq!(error.line_col(), Some((3, 13)));
    }
}

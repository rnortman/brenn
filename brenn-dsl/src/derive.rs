//! Derivation: from a resolved document to a derived one.
//!
//! The fourth pass, and the one where authority, identity and the channel model
//! stop being text and become facts. Resolution answered "what does this say";
//! this pass answers "is what it says legal, and what does it come to". It is
//! pure — a [`crate::resolved::ResolvedConfig`] in, a
//! [`crate::derived::DerivedConfig`] out — and it accumulates diagnostics the way
//! the resolver does: independent errors in one document are all reported.
//!
//! What runs here, in order:
//!
//! 1. **Roles** — which `channel` blocks declare and which tune. The rule is a
//!    total function of the address, so the two roles can never target one
//!    channel and there is no merge to define.
//! 2. **The channel model** — which depth attrs each role and durability class
//!    requires and which it refuses.
//! 3. **Identity** — the durable channel uuids, pinned or derived, and their
//!    distinctness against each other and against the runtime's derived
//!    non-durable identities.
//! 4. **Authority** — which family every `acl` matcher and cross-entity
//!    `grant` lands in, whether the entity that holds it has that family at all,
//!    and what the matcher comes to once its scheme is stripped; then what every
//!    binding and subscription derives where nothing explicit holds its plane,
//!    and that each of them is covered by the authority the entity ends up
//!    with.
//! 5. **Principals** — every declared principal's authority, and that each
//!    holds no more than the one it is `under`.
//! 6. **The wire-kind fold** — the kebab kind each surface-placed component
//!    instance is served under, and its collision check.
//!
//! Several tables below are transcriptions of runtime behavior — the schemes and
//! their durability, the tool namespaces, the presence rules, the uuid namespace
//! seeds. `brenn-dsl` depends on no brenn domain crate, so they are stated here
//! and carry the same drift exposure as the attr vocabularies do.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::LazyLock;

use fltk_cst_core::Span;
use fltk_serde_core::Spanned;
use uuid::Uuid;

use brenn_envelope::ChannelScheme;
use brenn_envelope::addressing::{
    MATCHER_BOUNDARIES, TUNING_BOUNDARIES, ends_at_matcher_boundary, ends_at_tuning_boundary,
    in_a_tool_namespace, is_auto_channel_name, nondurable_channel_uuid,
};
use brenn_envelope::channel_model::{
    ChannelBlockRole, ChannelDepthKey, TUNING_DURABILITY_IGNORED, depth_required, sink_admitted,
    standing_admitted,
};
use brenn_envelope::grants::{
    AppCapability, AttachGrant, ComponentGrant, ComponentHost, EntityKind, Plane, bindable_schemes,
};

use crate::derived::{
    DAclSet, DAuthorities, DAuthority, DMatcher, DMqttClient, DMqttSub, DRemoteAuthority,
    DRemoteSubEntry, DWebhook, DerivedConfig,
};
use crate::diag::{Diagnostic, check_unique, or_list, two_site};
use crate::model::Word;
use crate::resolved::scheme::{config_identified, spellable_quoted_list, split_spellable};
use crate::resolved::{
    ChanId, ClassRef, HandlePath, LinkId, MatcherKind, PortDir, RAcl, RAgent, RBinding, RChanRef,
    RChannel, RMatcher, RMatcherVal, RPort, RStamp, RSurface, RTail, RToolGrant, RTuning, RVal,
    RValue, RWordList, ResolvedConfig, StampId, str_value,
};

/// Derive a resolved document.
///
/// Errors accumulate; a failure reports every independent refusal rather than the
/// first one found.
pub fn derive(config: ResolvedConfig) -> Result<DerivedConfig, Vec<Diagnostic>> {
    let mut errors = Vec::new();
    check_channel_roles(&config, &mut errors);
    check_channel_model(&config, &mut errors);
    let channel_uuids = derive_channel_identity(&config, &mut errors);
    let refs = Refs::of(&config);
    let (authorities, conferrals) = derive_authorities(&config, &refs, &mut errors);
    check_ceilings(&config, &conferrals, &refs, &mut errors);
    let surface_component_kinds = fold_component_kinds(&config, &mut errors);
    check_links(&config, &mut errors);
    check_doctypes(&config, &refs, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(DerivedConfig::new(
        config,
        channel_uuids,
        authorities,
        surface_component_kinds,
    ))
}

/// Is this address one the system mints for itself?
///
/// The ingress families (`mqtt:`, `webhook:`) and the tool substrate's
/// (`brenn:tools/`, `brenn:tool-results/`). Everything else is an
/// operator-declared channel's address. A total function of the address, which
/// is what makes the declaring and tuning roles disjoint.
fn is_system_minted(address: &str) -> bool {
    match split_spellable(address) {
        Some((ChannelScheme::Mqtt | ChannelScheme::Webhook, _)) => true,
        Some((ChannelScheme::Brenn, name)) => in_a_tool_namespace(name),
        _ => false,
    }
}

/// One port bound to a link, as the link's own rules see it.
struct LinkBinding<'a> {
    dir: PortDir,
    /// Where the port was named, which is what a refusal about this endpoint
    /// points at.
    span: &'a Span,
    /// Whether the binding's tail states both halves of a window. Only a
    /// subscribing port has one, and the window rule asks only those.
    windowed: bool,
}

/// Every link is a channel some set of ports brings into existence.
///
/// The endpoint set is the whole of what a link is, so the rules are about it:
/// a link nobody binds is dead configuration, a link one port binds connects
/// nothing, and a link with no publisher or no subscriber is a ring one side
/// talks past. Boot asserts the same three as a backstop for hand-built
/// configurations; here they are refused with the spans the document wrote.
fn check_links(config: &ResolvedConfig, errors: &mut Vec<Diagnostic>) {
    let mut bound: BTreeMap<LinkId, Vec<LinkBinding<'_>>> = BTreeMap::new();
    fn collect<'a>(bindings: &'a [RBinding], bound: &mut BTreeMap<LinkId, Vec<LinkBinding<'a>>>) {
        for binding in bindings {
            let Some(RChanRef::Link(id)) = &binding.chan else {
                continue;
            };
            let windowed = match &binding.tail {
                RTail::In(tail) => tail.push_depth.is_some() && tail.retain_depth.is_some(),
                RTail::Io(tail) => tail.push_depth.is_some() && tail.retain_depth.is_some(),
                RTail::Out(_) => false,
            };
            bound.entry(*id).or_default().push(LinkBinding {
                dir: binding.dir(),
                span: binding.port.span(),
                windowed,
            });
        }
    }
    for surface in &config.surfaces {
        for instance in &surface.components {
            collect(&instance.bindings, &mut bound);
        }
    }
    for consumer in &config.consumers {
        collect(&consumer.bindings, &mut bound);
    }
    for (index, link) in config.links.iter().enumerate() {
        let handle = link.handle.dotted();
        let endpoints = bound.get(&LinkId(index)).map(Vec::as_slice).unwrap_or(&[]);
        match endpoints {
            [] => {
                errors.push(Diagnostic::at(
                    format!(
                        "link `{handle}` is bound by no port: a link is the channel its \
                         endpoints bring into existence, so one nobody binds is nothing"
                    ),
                    link.span.clone(),
                ));
                continue;
            }
            [lone] => {
                let advice = match lone.dir {
                    PortDir::Io => {
                        "an `io` port that connects to nothing is written `io <port>;`, \
                         with no link"
                    }
                    _ => "a link needs a port at each end",
                };
                errors.push(two_site(
                    format!(
                        "link `{handle}` is bound by one port, which connects it to \
                         nothing: {advice}"
                    ),
                    link.span.clone(),
                    "the one port bound to it",
                    lone.span.clone(),
                ));
                continue;
            }
            _ => {}
        }
        let publishes = endpoints
            .iter()
            .any(|end| matches!(end.dir, PortDir::Out | PortDir::Io));
        let subscribes = endpoints
            .iter()
            .any(|end| matches!(end.dir, PortDir::In | PortDir::Io));
        for (present, missing, role) in [
            (publishes, "publisher", "out"),
            (subscribes, "subscriber", "in"),
        ] {
            if present {
                continue;
            }
            errors.push(Diagnostic::at(
                format!(
                    "link `{handle}` has no {missing}: every port bound to it faces the \
                     other way, so nothing would ever reach the ring. Bind an `{role}` or an \
                     `io` port to it"
                ),
                link.span.clone(),
            ));
        }
        for endpoint in endpoints {
            // Direction decides whether the question applies at all: an `out`
            // port folds nothing into the ring's retention and states no
            // window.
            if !matches!(endpoint.dir, PortDir::In | PortDir::Io) || endpoint.windowed {
                continue;
            }
            errors.push(Diagnostic::at(
                format!(
                    "this port subscribes to link `{handle}` and states no window: a link's \
                     address appears nowhere in the document, so there is no `channel` block \
                     to carry the depths — write `push_depth` and `retain_depth` in the \
                     binding's tail"
                ),
                endpoint.span.clone(),
            ));
        }
    }
}

// ── pass 1: roles ────────────────────────────────────────────────────────────

/// Which `channel` blocks declare a channel and which tune a system-minted
/// family — and that no block is written in the role its address cannot play.
///
/// One spelling per role: a declarable address always carries a handle, and a
/// system-minted one never does. So a tuning and a declaration can never name
/// one channel, and there is no attribute merge to define.
fn check_channel_roles(config: &ResolvedConfig, errors: &mut Vec<Diagnostic>) {
    for channel in &config.channels {
        if is_system_minted(channel.address.value()) {
            errors.push(Diagnostic::at(
                format!(
                    "`{}` is an address the system mints, so there is nothing to declare; \
                     write `channel at \"…\"` without a handle to tune its depths",
                    channel.address.value()
                ),
                channel.address.span().clone(),
            ));
        }
    }

    for tuning in &config.tunings {
        check_tuning_address(tuning, errors);
    }

    // Exact and prefix keys live in separate spaces, so the key carries which one
    // it is: a prefix is a standing rule over a family, an exact key tunes one
    // channel, and one of each over the same text is not a duplicate.
    check_unique(
        config.tunings.iter().map(|tuning| {
            (
                (tuning.is_prefix, tuning.address.value().as_str()),
                (),
                tuning.address.span(),
            )
        }),
        |(is_prefix, address), (), span, (), first| {
            two_site(
                match is_prefix {
                    true => format!("two blocks tune the prefix `{address}`"),
                    false => format!("two blocks tune the address `{address}`"),
                },
                span.clone(),
                "the other one is here",
                first.clone(),
            )
        },
        errors,
    );
}

/// A handle-less block's address must name a system-minted family, and a prefix
/// must stop at the boundary of the family it names.
fn check_tuning_address(tuning: &RTuning, errors: &mut Vec<Diagnostic>) {
    let address = tuning.address.value();
    let span = tuning.address.span();
    // A malformed prefix is refused, and the family question is not asked of it:
    // what it names depends on where it was meant to stop. The description rules
    // below hold either way, so they run regardless.
    let mut classify = true;
    if tuning.is_prefix && !ends_at_tuning_boundary(address) {
        classify = false;
        errors.push(Diagnostic::at(
            format!(
                "the tuning prefix `{address}` does not end at a segment boundary \
                 ({}, the last of which closes an mqtt client) — a bare byte prefix \
                 reaches past the family it names",
                or_list(TUNING_BOUNDARIES)
            ),
            span.clone(),
        ));
    }
    if classify && !is_system_minted(address) {
        errors.push(Diagnostic::at(
            format!(
                "`{address}` names no system-minted family, so a handle-less block tunes \
                 nothing; tuning blocks name `mqtt:`, `webhook:`, `brenn:tools/` and \
                 `brenn:tool-results/` channels, and a declarable channel is written \
                 `channel <handle> at \"{address}\"`"
            ),
            span.clone(),
        ));
    }
    // The endpoint or tool that mints the channel owns its prose; a tuning block
    // supplies depths and nothing else. The doc comment lowers to `description`,
    // so both spellings are refused and `//` is what a note is written with.
    if let Some(doc) = &tuning.doc
        && let Some(first) = doc.lines.first()
    {
        errors.push(Diagnostic::at(
            "a tuning block carries no description: the endpoint or tool that mints the \
             channel owns it — write the note as a `//` comment"
                .to_string(),
            first.content.span().clone(),
        ));
    }
    if let Some(description) = &tuning.attrs.description {
        errors.push(Diagnostic::at(
            "a tuning block carries no description: the endpoint or tool that mints the \
             channel owns it"
                .to_string(),
            description.value.span().clone(),
        ));
    }
}

// ── pass 2: the channel model ────────────────────────────────────────────────

/// Which depth attrs each block must state and which it must leave alone.
///
/// Presence and absence only — the predicates are
/// [`brenn_envelope::channel_model`]'s, so this side and the boot builders
/// cannot disagree about the shape of a block. What a depth's *value* may be —
/// a count, the word `unbounded`, the noise and sink vocabularies — is
/// lowering's, which is where the runtime's own types live.
fn check_channel_model(config: &ResolvedConfig, errors: &mut Vec<Diagnostic>) {
    for channel in &config.channels {
        let address = channel.address.value();
        let span = channel.address.span();
        let durable = split_spellable(address).is_some_and(|(scheme, _)| config_identified(scheme));
        for key in ChannelDepthKey::ALL {
            let present = match key {
                ChannelDepthKey::PushDepth => channel.attrs.push_depth.is_some(),
                ChannelDepthKey::RetainDepth => channel.attrs.retain_depth.is_some(),
                ChannelDepthKey::StandingRetainDepth => {
                    channel.attrs.standing_retain_depth.is_some()
                }
            };
            if depth_required(key, ChannelBlockRole::Declaring, durable) {
                require_depth(present, key.word(), address, span, errors);
            }
        }
        if let Some(attr) = &channel.attrs.standing_retain_depth
            && !standing_admitted(durable)
        {
            errors.push(Diagnostic::at(
                format!(
                    "`{address}` is not disk-backed, so it states no \
                     standing_retain_depth: the standing buffer is the durable reaper's \
                     frontier, and this channel's retention is retain_depth alone"
                ),
                int_or_word_span(&attr.value).clone(),
            ));
        }
        if let Some(attr) = &channel.attrs.sink
            && !sink_admitted(durable)
        {
            errors.push(Diagnostic::at(
                format!(
                    "`{address}` is not disk-backed, so it states no sink: it evicts \
                     from memory and has nothing to archive"
                ),
                attr.value.name.span().clone(),
            ));
        }
    }

    // A system-minted channel has a bounded in-code default for every depth, so a
    // block that tunes one states all three rather than inheriting some.
    for tuning in &config.tunings {
        let label = match tuning.is_prefix {
            true => format!("prefix {}", tuning.address.value()),
            false => tuning.address.value().clone(),
        };
        for key in ChannelDepthKey::ALL {
            let present = match key {
                ChannelDepthKey::PushDepth => tuning.attrs.push_depth.is_some(),
                ChannelDepthKey::RetainDepth => tuning.attrs.retain_depth.is_some(),
                ChannelDepthKey::StandingRetainDepth => {
                    tuning.attrs.standing_retain_depth.is_some()
                }
            };
            if !present && depth_required(key, ChannelBlockRole::Tuning, TUNING_DURABILITY_IGNORED)
            {
                errors.push(Diagnostic::at(
                    format!(
                        "the block tuning `{label}` requires {}: a system-minted channel \
                         has a bounded in-code default, and a block that tunes it states \
                         every depth",
                        key.word(),
                    ),
                    tuning.address.span().clone(),
                ));
            }
        }
        if let Some(attr) = &tuning.attrs.doctype {
            errors.push(Diagnostic::at(
                format!(
                    "the block tuning `{label}` states no doctype: a tuning matches a \
                     family the system mints, so it names no one document contract — a \
                     doctype belongs on a `channel` declaration, which is one channel"
                ),
                attr.value.span().clone(),
            ));
        }
    }
}

/// One missing-depth refusal on a declaring block.
fn require_depth(
    present: bool,
    key: &str,
    address: &str,
    span: &Span,
    errors: &mut Vec<Diagnostic>,
) {
    if present {
        return;
    }
    errors.push(Diagnostic::at(
        format!(
            "`{address}` requires {key}: how deep a channel's window is sized is the \
             decision this declaration exists to record, not a default"
        ),
        span.clone(),
    ));
}

/// The span an `IntOrWord` was written at, whichever arm it took.
fn int_or_word_span(value: &crate::model::IntOrWord) -> &Span {
    match value {
        crate::model::IntOrWord::Int(count) => count.span(),
        crate::model::IntOrWord::Name { span, .. } => span,
    }
}

// ── pass 3: identity ─────────────────────────────────────────────────────────

/// The namespace seed a declared durable channel's uuid derives under.
///
/// Pinned forever: this seed and the address it hashes are what a persisted
/// channel row is named by, so changing either orphans every row a configuration
/// lowered from this pass created. Two-level derivation with a seed of its own,
/// the pattern every other channel address space uses — the derivation cannot
/// collide with the ephemeral, local, webhook or mqtt spaces, and hashing the
/// scheme-qualified address keeps it disjoint from the bare-name spaces by
/// construction.
const DSL_CHANNEL_SEED: &[u8] = b"brenn.dsl-channel";

/// The uuid a durable address derives to under [`DSL_CHANNEL_SEED`].
fn dsl_channel_uuid(address: &str) -> Uuid {
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_DNS, DSL_CHANNEL_SEED);
    Uuid::new_v5(&namespace, address.as_bytes())
}

/// The identity of every declared channel: pinned where a pin names it, derived
/// otherwise, and absent for the non-durable ones the runtime derives at boot.
///
/// Returns a vector parallel to `config.channels` whatever it refused, so the
/// parallelism assertion in [`DerivedConfig::new`] holds on the error path too.
fn derive_channel_identity(
    config: &ResolvedConfig,
    errors: &mut Vec<Diagnostic>,
) -> Vec<Option<Uuid>> {
    let pins = collect_pins(config, errors);
    let mut uuids = Vec::with_capacity(config.channels.len());
    // Every identity in play, in source order, so the collision check reads it
    // the way every other whole-document rule reads its keys.
    let mut identities: Vec<(Uuid, &str, &Span)> = Vec::new();
    for channel in &config.channels {
        let address = channel.address.value().as_str();
        let Some((scheme, name)) = split_spellable(address) else {
            // Resolution refuses a schemeless address, so this config never
            // reached derivation.
            unreachable!("a resolved channel address names a scheme");
        };
        let (uuid, span) = match (config_identified(scheme), pins.get(address)) {
            (true, Some(pin)) => (pin.0, pin.1),
            (true, None) => (dsl_channel_uuid(address), channel.address.span()),
            (false, _) => {
                match scheme {
                    // The runtime's own derivation, computed only so a pin or a
                    // durable derivation colliding with it is refused here.
                    ChannelScheme::Ephemeral | ChannelScheme::Local => (
                        nondurable_channel_uuid(scheme, name),
                        channel.address.span(),
                    ),
                    // Roles refused a declared channel on a system-minted
                    // scheme, and this config is already failing.
                    ChannelScheme::Webhook | ChannelScheme::Mqtt => {
                        uuids.push(None);
                        continue;
                    }
                    ChannelScheme::Brenn => unreachable!("brenn: states its identity"),
                    // `split_spellable` refuses the prefix, so no resolved
                    // address reaches here on it.
                    ChannelScheme::PwaPush => unreachable!("pwa_push: is not spellable"),
                }
            }
        };
        identities.push((uuid, address, span));
        uuids.push(config_identified(scheme).then_some(uuid));
    }
    check_unique(
        identities.into_iter(),
        |uuid, address, span, other, other_span| {
            two_site(
                format!("`{address}` and `{other}` have the same channel uuid {uuid}"),
                span.clone(),
                format!("`{other}` has it here"),
                other_span.clone(),
            )
        },
        errors,
    );
    uuids
}

/// Every pin, keyed by the address it belongs to.
///
/// Refuses a uuid that does not parse, two pins for one address, and a pin no
/// durable declared channel answers to — a pin is the carrier for a hand-minted
/// identity, so a pin naming nothing is a migration that silently did not happen.
fn collect_pins<'a>(
    config: &'a ResolvedConfig,
    errors: &mut Vec<Diagnostic>,
) -> HashMap<&'a str, (Uuid, &'a Span)> {
    let durable: HashSet<&str> = config
        .channels
        .iter()
        .filter(|channel| {
            split_spellable(channel.address.value())
                .is_some_and(|(scheme, _)| config_identified(scheme))
        })
        .map(|channel| channel.address.value().as_str())
        .collect();

    // A pin whose uuid does not parse states no identity, so it is refused and
    // does not go on to hold its address against a second pin.
    let mut stated: Vec<(&str, (Uuid, &Span), &Span)> = Vec::new();
    for pin in &config.uuid_pins {
        match Uuid::parse_str(pin.uuid.value()) {
            Ok(uuid) => stated.push((
                pin.address.value().as_str(),
                (uuid, pin.uuid.span()),
                pin.address.span(),
            )),
            Err(error) => errors.push(Diagnostic::at(
                // The cause distinguishes a truncated uuid from a pasted wrong
                // field, which is the difference between the two fixes.
                format!("`{}` is not a uuid: {error}", pin.uuid.value()),
                pin.uuid.span().clone(),
            )),
        }
    }
    check_unique(
        stated.iter().copied(),
        |address, _, span, _, first| {
            two_site(
                format!("two pins name the address `{address}`"),
                span.clone(),
                "the other one is here",
                first.clone(),
            )
        },
        errors,
    );

    // Source order, so a pin that names nothing is refused where it was written
    // and the diagnostics stay a function of the document. The first pin on an
    // address is the one that holds it; the repeats were refused above and are
    // not asked the next question.
    let mut pins: HashMap<&str, (Uuid, &Span)> = HashMap::new();
    let mut held: HashSet<&str> = HashSet::new();
    for (address, pin, address_span) in stated {
        if !held.insert(address) {
            continue;
        }
        if !durable.contains(address) {
            errors.push(Diagnostic::at(
                format!(
                    "no disk-backed channel declares the address `{address}`, so this pin \
                     names nothing; only a `brenn:` declaration carries a configured uuid"
                ),
                address_span.clone(),
            ));
            continue;
        }
        pins.insert(address, pin);
    }
    pins
}

// ── pass 4: authority ────────────────────────────────────────────────────────

/// The reason this entity type cannot hold this right, where there is one.
///
/// Only a component's rights have a host to be illegal on: every other entity
/// type's vocabulary is already exactly what it may state.
fn host_refusal(kind: EntityKind, right: Capability) -> Option<&'static str> {
    match (kind, right) {
        (EntityKind::Component(host), Capability::Grant(grant)) => grant.illegal_on(host),
        _ => None,
    }
}

/// Every capability a component states, whichever host it runs on: the shared
/// vocabulary whole, with [`host_refusal`] refusing what the host cannot
/// implement. One list rather than one per host, so a word is legal-here or
/// illegal-there and never unknown.
///
/// The leading segment of [`Capability::ALL`], which builds the shared words
/// first and appends the agent-only rights after them — so the relationship
/// between the two lists is structural rather than two builders held equal by
/// review.
const COMPONENT_CAPABILITIES: &[Capability] = Capability::ALL.split_at(ComponentGrant::ALL.len()).0;

/// The running entity a statement is about: what it is, and what to call it.
#[derive(Debug, Clone, Copy)]
struct Holder<'a> {
    kind: EntityKind,
    label: &'a str,
}

/// Where an entry came from.
///
/// Two readers, one fact. Entry resolution asks it because a `grant` reaches
/// into another entity's authority and one thing is legal in an entity's own
/// body and not from outside it. The ceiling fold asks it because what a stamp
/// confers is what the arrangement wrote — its own statements and what its
/// bindings derive — and never what someone else's `grant` handed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    /// An `acl` statement in the holder's own body.
    Statement,
    /// Derived from a binding the holder makes.
    Binding,
    /// A `grant` aimed at the holder, by position in `ResolvedConfig::grants`.
    Grant(usize),
}

/// One ACL list the runtime keeps, named as the field that holds it.
///
/// The plane and the scheme together select the list; which entity types have it
/// is the second half of the table.
///
/// The runtime structs that hold these lists must carry exactly the fields this
/// table names; a parity test in `brenn-lib` asserts the equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    BrennSubscribe,
    BrennPublish,
    EphemeralSubscribe,
    EphemeralPublish,
    LocalSubscribe,
    LocalPublish,
    MqttSubscribe,
    MqttPublish,
    Webhook,
}

/// How a struct spells the ACL list a [`Family`] names.
///
/// The same nine lists are fielded three ways across the runtime's structs;
/// keeping the transforms here avoids ad-hoc mapping in each parity test.
// TODO(acl-field-spelling-home): these transforms are brenn-lib's field-naming
// conventions, held in the crate that cannot see brenn-lib's structs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclShape {
    /// An LLM app's ACL block, which spells the `brenn:` families in full.
    App,
    /// A borrowed view of a component's or an attacher's lists, where the
    /// unqualified word means `brenn:`.
    View,
    /// A consumer's or a remote's own config struct, where the unqualified word
    /// means `brenn:` and every list carries an `_acl` suffix.
    ConsumerConfig,
}

impl Family {
    /// Every list the runtime keeps, in the order the families are declared.
    pub const ALL: [Family; 9] = [
        Family::BrennSubscribe,
        Family::BrennPublish,
        Family::EphemeralSubscribe,
        Family::EphemeralPublish,
        Family::LocalSubscribe,
        Family::LocalPublish,
        Family::MqttSubscribe,
        Family::MqttPublish,
        Family::Webhook,
    ];

    /// The list a plane and a scheme together name.
    ///
    /// `None` only for a webhook on the publish plane: webhooks are inbound, so
    /// there is no list for a right to send to one.
    pub fn of(scheme: ChannelScheme, plane: Plane) -> Option<Family> {
        Some(match (scheme, plane) {
            (ChannelScheme::Brenn, Plane::Subscribe) => Self::BrennSubscribe,
            (ChannelScheme::Brenn, Plane::Publish) => Self::BrennPublish,
            (ChannelScheme::Ephemeral, Plane::Subscribe) => Self::EphemeralSubscribe,
            (ChannelScheme::Ephemeral, Plane::Publish) => Self::EphemeralPublish,
            (ChannelScheme::Local, Plane::Subscribe) => Self::LocalSubscribe,
            (ChannelScheme::Local, Plane::Publish) => Self::LocalPublish,
            (ChannelScheme::Mqtt, Plane::Subscribe) => Self::MqttSubscribe,
            (ChannelScheme::Mqtt, Plane::Publish) => Self::MqttPublish,
            (ChannelScheme::Webhook, Plane::Subscribe) => Self::Webhook,
            (ChannelScheme::Webhook, Plane::Publish) => return None,
            // `split_spellable` refuses the prefix, so no resolved address
            // reaches a family question on it.
            (ChannelScheme::PwaPush, _) => unreachable!("pwa_push: is not spellable"),
        })
    }

    /// Does this entity type have this list?
    pub fn held_by(self, kind: EntityKind) -> bool {
        match self {
            Self::BrennSubscribe
            | Self::BrennPublish
            | Self::EphemeralSubscribe
            | Self::EphemeralPublish => true,
            // A confined channel reaches a component and nothing else: a surface
            // is authorized out of band by the page it is served to, and an agent
            // has no confined delivery path at all.
            Self::LocalSubscribe => matches!(kind, EntityKind::Component(_)),
            Self::LocalPublish => {
                matches!(kind, EntityKind::Agent | EntityKind::Component(_))
            }
            // A broker and an endpoint are the backend host's: the page
            // implements neither, which is the same fact the capability
            // legality table states as `store`/`mqtt` being backend-only.
            Self::MqttSubscribe | Self::MqttPublish | Self::Webhook => matches!(
                kind,
                EntityKind::Agent | EntityKind::Component(ComponentHost::TopLevel)
            ),
        }
    }

    /// Is this family confined — reach a ceiling neither caps nor is asked for?
    ///
    /// One home for the fact, because three readers depend on agreeing about it:
    /// a ceiling line in a confined family is refused, a confined channel is
    /// absent from a stamp's default reach, and a confined entry an arrangement
    /// derives is not reach the stamp confers. A confined channel reaches the
    /// one component that binds it, authorized by the host that serves the page
    /// rather than by anything a deployment writes.
    fn confined(self) -> bool {
        matches!(self, Self::LocalSubscribe | Self::LocalPublish)
    }

    /// Which matcher kind writes an entry in this list.
    fn admits(self, kind: MatcherKind) -> bool {
        match self {
            Self::MqttSubscribe => kind == MatcherKind::TopicFilter,
            Self::MqttPublish => kind == MatcherKind::Client,
            Self::Webhook => kind == MatcherKind::Endpoint,
            _ => matches!(kind, MatcherKind::Exact | MatcherKind::Prefix),
        }
    }

    /// The kinds this list admits, for a diagnostic that has to say which.
    fn kinds(self) -> &'static str {
        match self {
            Self::MqttSubscribe => "`topic_filter`",
            Self::MqttPublish => "`client`",
            Self::Webhook => "`endpoint`",
            _ => "`exact` and `prefix`",
        }
    }

    /// The plane its entries are on.
    ///
    /// A webhook is inbound only, so it is the subscribe plane.
    fn plane(self) -> Plane {
        match self {
            Self::BrennSubscribe
            | Self::EphemeralSubscribe
            | Self::LocalSubscribe
            | Self::MqttSubscribe
            | Self::Webhook => Plane::Subscribe,
            Self::BrennPublish
            | Self::EphemeralPublish
            | Self::LocalPublish
            | Self::MqttPublish => Plane::Publish,
        }
    }

    /// The scheme its entries are addresses under.
    fn scheme(self) -> ChannelScheme {
        match self {
            Self::BrennSubscribe | Self::BrennPublish => ChannelScheme::Brenn,
            Self::EphemeralSubscribe | Self::EphemeralPublish => ChannelScheme::Ephemeral,
            Self::LocalSubscribe | Self::LocalPublish => ChannelScheme::Local,
            Self::MqttSubscribe | Self::MqttPublish => ChannelScheme::Mqtt,
            Self::Webhook => ChannelScheme::Webhook,
        }
    }

    /// Is this a list whose entries a remote states depth ceilings on?
    pub fn carries_ceilings(self) -> bool {
        matches!(self, Self::BrennSubscribe | Self::EphemeralSubscribe)
    }

    /// Why this entity type keeps no list of this family.
    ///
    /// The one home for the fact both refusal paths rest on: a matcher naming a
    /// family the holder lacks, and a position on a scheme that family would
    /// have authorized.
    fn absent_reason(self, kind: EntityKind) -> String {
        match (self, kind) {
            (Self::LocalSubscribe, EntityKind::Agent) => "a confined channel has no delivery \
                 path to an agent, so the runtime keeps no such list — deliberately, not by \
                 omission"
                .to_string(),
            _ => format!("the runtime keeps no such list for a {}", kind.label()),
        }
    }

    /// The field this list is held in, in a struct of this shape.
    ///
    /// [`Family::name`] is the `App` spelling; the other two drop the `brenn_`
    /// qualifier, because a struct that is already about one entity's
    /// `brenn:` lists says it once in the type rather than nine times in the
    /// fields, and a config struct appends `_acl` because its ACL lists sit
    /// beside its ports and its budgets.
    pub fn field_name(self, shape: AclShape) -> String {
        let name = self.name();
        match shape {
            AclShape::App => name.to_string(),
            AclShape::View => name.strip_prefix("brenn_").unwrap_or(name).to_string(),
            AclShape::ConsumerConfig => {
                format!("{}_acl", name.strip_prefix("brenn_").unwrap_or(name))
            }
        }
    }
    /// The field this list is held in.
    pub fn name(self) -> &'static str {
        match self {
            Self::BrennSubscribe => "brenn_subscribe",
            Self::BrennPublish => "brenn_publish",
            Self::EphemeralSubscribe => "ephemeral_subscribe",
            Self::EphemeralPublish => "ephemeral_publish",
            Self::LocalSubscribe => "local_subscribe",
            Self::LocalPublish => "local_publish",
            Self::MqttSubscribe => "mqtt_subscribe",
            Self::MqttPublish => "mqtt_publish",
            Self::Webhook => "webhook",
        }
    }
}

/// One entry, before it is filed under the family it belongs to.
#[derive(Clone, Debug)]
enum DEntry {
    Chan(DMatcher),
    Ceiling(DRemoteSubEntry),
    MqttSub(DMqttSub),
    MqttPub(DMqttClient),
    Webhook(DWebhook),
}

/// The declarations a matcher may point at from outside the handle space.
///
/// A channel is reached by [`ChanId`]; an mqtt client and a webhook endpoint are
/// named as text inside an address, so what a slug answers to is a lookup. Both
/// sides live in the resolved model, which is what makes these
/// cross-references rather than a mirror of the runtime's address validation.
struct Refs<'a> {
    channels: &'a [RChannel],
    clients: HashSet<String>,
    endpoints: HashSet<String>,
}

impl<'a> Refs<'a> {
    fn of(config: &'a ResolvedConfig) -> Refs<'a> {
        Refs {
            channels: &config.channels,
            clients: config
                .mqtt_clients
                .iter()
                .map(|client| client.handle.dotted())
                .collect(),
            endpoints: config
                .webhooks
                .iter()
                .map(|webhook| webhook.slug.value().clone())
                .collect(),
        }
    }

    /// The address of the channel a matcher named.
    fn address(&self, id: ChanId) -> &str {
        self.channels[id.0].address.value()
    }

    /// That an `mqtt_client` block answers to this slug.
    fn client(&self, slug: &str, span: &Span, errors: &mut Vec<Diagnostic>) -> bool {
        if self.clients.contains(slug) {
            return true;
        }
        errors.push(Diagnostic::at(
            format!(
                "no `mqtt_client` is named `{slug}`, so nothing connects on this entry's \
                 behalf; an mqtt address names a client this configuration declares"
            ),
            span.clone(),
        ));
        false
    }

    /// That a `webhook` block answers to this endpoint slug.
    fn endpoint(&self, slug: &str, span: &Span, errors: &mut Vec<Diagnostic>) -> bool {
        if self.endpoints.contains(slug) {
            return true;
        }
        errors.push(Diagnostic::at(
            format!(
                "no `webhook` is named `{slug}`, so no endpoint mints that channel; an \
                 endpoint names a webhook this configuration declares"
            ),
            span.clone(),
        ));
        false
    }
}

/// One running entity, as every phase of the authority pass sees it.
///
/// Statements, grants, bindings and the grants walk all run over one list of
/// these, so a rule that must hold for every entity is written once and a
/// per-kind exemption is a value on the row rather than a missing loop.
struct Subject<'a> {
    kind: EntityKind,
    /// The dotted handle, spelled once per entity.
    label: String,
    /// The recorded stamp this entity was expanded inside, or `None` for one
    /// written in deployer text under no recorded stamp.
    stamp: Option<StampId>,
    /// Where the identity is written: what a refusal about the whole entity cites.
    span: Span,
    acls: &'a [RAcl],
    /// Every position this entity attaches through. Empty for a remote, which
    /// holds no ports and states no subscriptions, so its authority is the
    /// entries it writes and the ones granted to it.
    bounds: Vec<Bound<'a>>,
    /// Whether a refusal about one of its positions is this entity's to
    /// report.
    ///
    /// False for a surface-placed instance: its bindings are also its surface's
    /// positions, so the surface's pass reports what is wrong with an address
    /// and the instance's pass only records that its own authority is
    /// incomplete. One mistake, one message.
    owns_bindings: bool,
    /// Where its first free `io` port is, if it holds one. A free port connects
    /// nothing, so it is no position — but the ring it is served mints a page
    /// where the component publishes, so it is a send the `ports` rule asks
    /// about.
    free_send: Option<Span>,
    /// Its `grants` words, or `None` where the entity states no list at all —
    /// which an agent may do (no rights is a posture, as its config counterpart
    /// allows) and a component may not, at either placement (the field is
    /// required with no default).
    words: Option<&'a [Word]>,
    /// What its class declares it needs, for a component instance; `None` for
    /// every other entity, none of which instantiates a spec.
    spec: Option<&'a ClassRef>,
    /// The `tool` statements it holds. A component's are coupled to its `tools`
    /// word; an agent's are its whole tool authority, coupled to nothing.
    tools: &'a [RToolGrant],
}

impl Subject<'_> {
    /// This entity, as a diagnostic names it.
    fn holder(&self) -> Holder<'_> {
        Holder {
            kind: self.kind,
            label: &self.label,
        }
    }

    /// The words its `grants` list holds, refusing the one entity type that may
    /// not leave the list out.
    ///
    /// `refused` is set when the list itself was refused: the words are then not
    /// what the document states, so agreement stops asking about them.
    fn words(&self, refused: &mut bool, errors: &mut Vec<Diagnostic>) -> &[Word] {
        match self.words {
            Some(words) => words,
            None => {
                if matches!(self.kind, EntityKind::Component(_)) {
                    *refused = true;
                    errors.push(Diagnostic::at(
                        format!(
                            "{} `{}` states no `grants`: what a component is given is \
                             deny-by-default, so an empty list is written `grants = [];` \
                             rather than left out",
                            self.kind.label(),
                            self.label
                        ),
                        self.span.clone(),
                    ));
                }
                &[]
            }
        }
    }
}

/// Every running entity in the document, in the order the derived model holds
/// them: surfaces, consumers, agents, remotes.
fn subjects(config: &ResolvedConfig) -> Vec<Subject<'_>> {
    let surfaces = config.surfaces.iter().map(|entity| Subject {
        kind: EntityKind::Surface,
        label: entity.handle.dotted(),
        stamp: entity.stamp,
        span: handle_span(&entity.handle),
        acls: &entity.acls,
        bounds: surface_bounds(entity),
        owns_bindings: true,
        // A surface states no `grants` word about sending; the `ports` rule is
        // its components'.
        free_send: None,
        words: Some(&entity.attrs.grants.value.words),
        spec: None,
        tools: &[],
    });
    let components = config.surfaces.iter().flat_map(|surface| {
        let prefix = surface.handle.dotted();
        surface.components.iter().map(move |instance| Subject {
            kind: EntityKind::Component(ComponentHost::Surface),
            label: format!("{prefix}.{}", instance.instance.value()),
            stamp: instance.stamp,
            span: instance.instance.span().clone(),
            acls: &instance.acls,
            bounds: binding_bounds(&instance.bindings),
            owns_bindings: false,
            free_send: free_send(&instance.bindings),
            words: instance.grants.as_ref().map(|list| list.words.as_slice()),
            spec: Some(&instance.class),
            tools: &instance.tools,
        })
    });
    let consumers = config.consumers.iter().map(|entity| Subject {
        kind: EntityKind::Component(ComponentHost::TopLevel),
        label: entity.handle.dotted(),
        stamp: entity.stamp,
        span: handle_span(&entity.handle),
        acls: &entity.acls,
        bounds: binding_bounds(&entity.bindings),
        owns_bindings: true,
        free_send: free_send(&entity.bindings),
        words: entity.grants.as_ref().map(|list| list.words.as_slice()),
        spec: Some(&entity.class),
        tools: &entity.tools,
    });
    let agents = config.agents.iter().map(|entity| Subject {
        kind: EntityKind::Agent,
        label: entity.handle.dotted(),
        stamp: entity.stamp,
        span: handle_span(&entity.handle),
        acls: &entity.acls,
        bounds: agent_bounds(entity),
        owns_bindings: true,
        free_send: None,
        words: entity
            .attrs
            .grants
            .as_ref()
            .map(|attr| attr.value.words.as_slice()),
        spec: None,
        tools: &entity.tools,
    });
    let remotes = config.remotes.iter().map(|entity| Subject {
        kind: EntityKind::Remote,
        label: entity.handle.dotted(),
        // A remote is a network peer named in deployer text; no assembly
        // declares one, so nothing stamps one.
        stamp: None,
        span: handle_span(&entity.handle),
        acls: &entity.acls,
        bounds: Vec::new(),
        owns_bindings: true,
        free_send: None,
        words: Some(&entity.attrs.grants.value.words),
        spec: None,
        tools: &[],
    });
    surfaces
        .chain(components)
        .chain(consumers)
        .chain(agents)
        .chain(remotes)
        .collect()
}

/// One entity's entries as they accumulate, and where the explicit `acl`
/// statements it holds are.
///
/// The spans are what a coverage refusal cites: an explicit statement is the
/// whole authority for its plane, so a binding that derives nothing is answered
/// by the statement that stopped it from deriving.
#[derive(Default)]
struct Stated {
    entries: Vec<(Family, DEntry, Origin)>,
    explicit: Vec<(Plane, Span)>,
    /// Where the first send this entity makes is, if it makes one: a position
    /// on the publish plane, or the ring a free `io` port mints. What the `ports`
    /// rule is about at either placement — a send is a right to send whether or
    /// not an entry was ever filed for it.
    output: Option<Span>,
    /// Whether anything this entity wrote was refused.
    ///
    /// Agreement asks whether the words and the lists say the same thing, and a
    /// refused statement means the lists are not what the document says — so the
    /// question is not asked, and the refusal that caused it is not followed by a
    /// second one about its consequence.
    ///
    /// Set by the code that refuses, at each site: a refusal about something else
    /// filed from the same loop must not silently switch agreement off.
    refused: bool,
}

impl Stated {
    /// Does an explicit statement hold this plane?
    ///
    /// Across every scheme: a statement about a plane is a statement that the
    /// plane's authority is written out, and deriving beside it would widen what
    /// was written.
    fn holds(&self, plane: Plane) -> bool {
        self.explicit.iter().any(|(stated, _)| *stated == plane)
    }

    /// Where the statements holding a plane are, as a refusal cites them.
    fn sites(&self, plane: Plane) -> Vec<(String, Span)> {
        self.explicit
            .iter()
            .filter(|(stated, _)| *stated == plane)
            .map(|(stated, span)| {
                (
                    format!("`acl {}` is written here", stated.word()),
                    span.clone(),
                )
            })
            .collect()
    }

    /// Add an entry unless one exactly like it is already held.
    ///
    /// Two positions on one channel derive one entry: the second would grant
    /// nothing the first does not, and the derived model is about what is
    /// authorized rather than how many times it was asked for.
    fn add_derived(&mut self, family: Family, entry: DEntry) {
        let key = entry.key();
        if self
            .entries
            .iter()
            .any(|(held_family, held, _)| (*held_family, held.key()) == (family, key))
        {
            return;
        }
        self.entries.push((family, entry, Origin::Binding));
    }

    /// Is anything filed under this family?
    ///
    /// What agreement reads: a right with an empty list authorizes nothing, and a
    /// list no right admits is never consulted.
    fn first(&self, family: Family) -> Option<&DEntry> {
        self.entries
            .iter()
            .find(|(held, _, _)| *held == family)
            .map(|(_, entry, _)| entry)
    }

    /// Is anything here enough for a position on this family to be authorized?
    fn covers(&self, family: Family, name: &str) -> bool {
        self.entries
            .iter()
            .any(|(held_family, entry, _)| *held_family == family && entry.covers(name))
    }
}

/// Every entity's effective authority: what its own body states, what other
/// statements grant it, and what its bindings derive.
///
/// Four phases over one entity list, so every entity type takes every phase:
/// its own statements, the grants aimed at it, its bindings, then its `grants`
/// words against the lists the first three came to.
///
/// Order is the statement set's, not a hash's: explicit entries in source order,
/// then `grant` entries in source order, then derived entries in binding order,
/// so the derived model is a pure function of the document.
fn derive_authorities(
    config: &ResolvedConfig,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> (DAuthorities, Conferred) {
    let subjects = subjects(config);
    // Surface-placed components are deliberately absent: they hold no handle in
    // the handle space, so nothing can name one as a `grant` target, and a
    // label that happened to read like a real handle must not shadow it.
    let slots: HashMap<&str, usize> = subjects
        .iter()
        .enumerate()
        .filter(|(_, subject)| subject.kind != EntityKind::Component(ComponentHost::Surface))
        .map(|(index, subject)| (subject.label.as_str(), index))
        .collect();
    let mut stated: Vec<Stated> = subjects
        .iter()
        .map(|subject| collect_statements(subject, refs, errors))
        .collect();

    // Parallel to `config.grants`: what each grant filed. Not reachable
    // through the target's own row — a stamped arrangement may widen an entity
    // outside its subtree, and that entity has no row of its own.
    let mut granted: Vec<Option<(Family, DEntry)>> = Vec::new();
    for (position, grant) in config.grants.iter().enumerate() {
        let label = grant.target.dotted();
        let Some(&index) = slots.get(label.as_str()) else {
            unreachable!("resolution refuses a grant that names no running entity");
        };
        let Some(plane) = Plane::parse(grant.plane.value()) else {
            unreachable!("resolution refuses a grant on a plane that is not a plane");
        };
        let held = &mut stated[index];
        let entry = resolve_entry(
            &grant.m,
            plane,
            subjects[index].holder(),
            Origin::Grant(position),
            refs,
            errors,
            &mut held.refused,
        );
        // A refused grant is a refused part of that entity's authority, so
        // agreement stops asking about the entity it was aimed at.
        match entry {
            Some((family, entry)) => {
                granted.push(Some((family, entry.clone())));
                held.entries.push((family, entry, Origin::Grant(position)));
            }
            None => {
                granted.push(None);
                held.refused = true;
            }
        }
    }

    // Bindings after grants: a derived entry is a duplicate of one already held
    // if anything states it, and what states it may be a grant.
    for (subject, held) in subjects.iter().zip(stated.iter_mut()) {
        derive_bounds(held, subject, refs, errors);
        check_mqtt_sinks(subject, held, errors);
    }

    let mut authorities = DAuthorities::default();
    // Flat, in `subjects` walk order; re-nested per surface below.
    let mut placed: Vec<DAuthority> = Vec::new();
    // What every stamped entity holds. Collected here because this is the one
    // pass that has the whole of an entity's authority in hand: the lowered
    // `DAuthority` has filed its entries by family and dropped where each came
    // from, and the ceiling rules turn on exactly that.
    let mut entities = Vec::new();
    for (subject, held) in subjects.iter().zip(stated) {
        let grants = derive_grants(subject, &held, errors);
        if subject.stamp.is_some() {
            entities.push(Conferral {
                stamp: subject.stamp,
                label: subject.label.clone(),
                words: grants.words,
                entries: held.entries.clone(),
            });
        }
        let tokens = grants.tokens;
        match subject.kind {
            EntityKind::Surface => authorities.surfaces.push(authority(held, tokens)),
            EntityKind::Component(ComponentHost::Surface) => placed.push(authority(held, tokens)),
            EntityKind::Component(ComponentHost::TopLevel) => {
                authorities.consumers.push(authority(held, tokens))
            }
            EntityKind::Agent => authorities.agents.push(authority(held, tokens)),
            EntityKind::Remote => authorities.remotes.push(remote_authority(held, tokens)),
        }
    }
    let mut rest = placed.into_iter();
    authorities.surface_components = config
        .surfaces
        .iter()
        .map(|surface| rest.by_ref().take(surface.components.len()).collect())
        .collect();
    assert!(
        rest.next().is_none(),
        "one authority per placed instance, in surface order"
    );
    (
        authorities,
        Conferred {
            entities,
            grants: granted,
        },
    )
}

/// The authority-side inputs the ceiling fold needs.
struct Conferred {
    /// One row per authority-bearing entity with a recorded stamp.
    entities: Vec<Conferral>,
    /// Parallel to [`ResolvedConfig::grants`]: the entry each grant filed, or
    /// `None` where it was refused.
    grants: Vec<Option<(Family, DEntry)>>,
}

/// What one stamped entity confers on the stamp it came out of.
///
/// One row per authority-bearing entity with a recorded stamp; an entity under
/// none confers on nothing and gets no row.
struct Conferral {
    /// The stamp it was expanded inside. Every recorded stamp on that stamp's
    /// ancestor chain counts it, because an outer ceiling is a statement about
    /// the whole subtree under it.
    stamp: Option<StampId>,
    /// The dotted handle, as a refusal names the holder.
    label: String,
    /// Its grant words, in the ceiling's spelling.
    words: Vec<Spanned<String>>,
    /// Its reach, with where each entry came from.
    entries: Vec<(Family, DEntry, Origin)>,
}

/// One entity's entries, filed into the families an app-side entity holds.
fn authority(stated: Stated, grants: Vec<Spanned<String>>) -> DAuthority {
    let mut acl = DAclSet::default();
    for (family, entry, _) in stated.entries {
        entry.file(family, &mut Lists::Acl(&mut acl));
    }
    DAuthority { grants, acl }
}

/// One remote's entries, filed into the four lists a remote holds.
fn remote_authority(stated: Stated, grants: Vec<Spanned<String>>) -> DRemoteAuthority {
    let mut remote = DRemoteAuthority {
        grants,
        ..DRemoteAuthority::default()
    };
    for (family, entry, _) in stated.entries {
        entry.file(family, &mut Lists::Remote(&mut remote));
    }
    remote
}

/// Where an entity's identity is written.
fn handle_span(handle: &HandlePath) -> Span {
    handle
        .0
        .last()
        .expect("a handle has at least one segment")
        .span()
        .clone()
}

/// The plane an `acl` statement names.
///
/// One home for the refusal: a running entity's statement and a ceiling's line
/// are both read here, and a reword that reached only one of them would explain
/// the same mistake in two voices.
fn acl_plane(acl: &RAcl, errors: &mut Vec<Diagnostic>) -> Option<Plane> {
    let plane = Plane::parse(acl.plane.value());
    if plane.is_none() {
        errors.push(Diagnostic::at(
            format!(
                "`{}` is not a plane; an acl statement names `subscribe` or `publish`, \
                 and which scheme it is about comes from its matchers",
                acl.plane.value()
            ),
            acl.plane.span().clone(),
        ));
    }
    plane
}

/// One entity's own `acl` statements, resolved into the entries they name.
fn collect_statements(
    subject: &Subject<'_>,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Stated {
    let holder = subject.holder();
    let mut stated = Stated::default();
    for acl in subject.acls {
        let Some(plane) = acl_plane(acl, errors) else {
            stated.refused = true;
            continue;
        };
        stated.explicit.push((plane, acl.plane.span().clone()));
        for matcher in &acl.matchers {
            let entry = resolve_entry(
                matcher,
                plane,
                holder,
                Origin::Statement,
                refs,
                errors,
                &mut stated.refused,
            );
            match entry {
                Some((family, entry)) => {
                    stated.entries.push((family, entry, Origin::Statement));
                }
                None => stated.refused = true,
            }
        }
    }
    stated
}

/// One matcher, as the family entry it becomes.
///
/// The scheme in the pattern is what says which family this is about; it is
/// stripped on the way in, because the list an entry lands in already carries the
/// scheme and the runtime's own config spells entries bare.
///
/// `refused` is set when the matcher named an entry and something about it was
/// still refused — an illegal tail key. A matcher refused outright says so by
/// returning `None`, and the caller records that where it handles it.
fn resolve_entry(
    matcher: &RMatcher,
    plane: Plane,
    holder: Holder<'_>,
    origin: Origin,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
    refused: &mut bool,
) -> Option<(Family, DEntry)> {
    let kind = *matcher.kind.value();
    let span = matcher.val.span().clone();
    let (family, bare) = matcher_family(matcher, plane, kind, refs, errors)?;
    if !family.held_by(holder.kind) {
        errors.push(Diagnostic::at(no_such_family(family, holder), span.clone()));
        return None;
    }
    if !family_admits(matcher, kind, family, errors) {
        return None;
    }
    let ceilings = holder.kind == EntityKind::Remote && family.carries_ceilings();
    if ceilings && matches!(origin, Origin::Grant(_)) {
        errors.push(Diagnostic::at(
            format!(
                "a grant cannot reach the subscribe plane of remote `{}`: its entries cap \
                 how deep a subscription may be held, and one ceiling per remote is what \
                 makes that a bound — write the entry in the remote's own `acl subscribe`",
                holder.label
            ),
            span,
        ));
        return None;
    }
    // Two families are built here rather than by `plain_entry`, because what a
    // statement adds to them is inside the entry: an mqtt sink's budgets, and a
    // remote's subscribe depths. Every other family is the shared construction.
    let entry = match (family, ceilings) {
        (Family::MqttPublish, _) => {
            let budget = mqtt_sink_budget(matcher, holder, errors);
            *refused |= budget.is_none();
            let (publish_per_activation, publish_capacity) = budget.unwrap_or((None, None));
            DEntry::MqttPub(DMqttClient {
                client: mqtt_client(&bare, &span, refs, errors)?,
                publish_per_activation,
                publish_capacity,
            })
        }
        (_, true) => {
            let pattern = channel_matcher(kind, bare, &span, errors)?;
            let (push_depth, retain_depth) = remote_ceilings(matcher, &span, errors)?;
            DEntry::Ceiling(DRemoteSubEntry {
                m: pattern,
                push_depth,
                retain_depth,
            })
        }
        _ => {
            *refused |= refuse_tail(matcher, family, errors);
            plain_entry(family, kind, bare, &span, refs, errors)?
        }
    };
    Some((family, entry))
}

/// One matcher's family entry, undecorated.
///
/// The one construction both readers of a matcher end at — a running entity's
/// `acl` statement and a ceiling's `acl` line — so an entry a ceiling is
/// compared against is built the way the entry it caps was. A family whose
/// entry carries something the writer decorates it with is the caller's, and
/// there are two: an mqtt sink's budgets and a remote's subscribe depths.
fn plain_entry(
    family: Family,
    kind: MatcherKind,
    bare: String,
    span: &Span,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<DEntry> {
    Some(match family {
        Family::MqttSubscribe => DEntry::MqttSub(mqtt_sub_entry(&bare, span, refs, errors)?),
        Family::MqttPublish => DEntry::MqttPub(DMqttClient {
            client: mqtt_client(&bare, span, refs, errors)?,
            publish_per_activation: None,
            publish_capacity: None,
        }),
        Family::Webhook => {
            if !refs.endpoint(&bare, span, errors) {
                return None;
            }
            DEntry::Webhook(DWebhook {
                endpoint: Spanned::new(bare, span.clone()),
            })
        }
        _ => DEntry::Chan(channel_matcher(kind, bare, span, errors)?),
    })
}

/// The family a matcher on a plane is about, and what follows its scheme.
///
/// The half of entry resolution that is the same question whoever asks it: a
/// running entity's own statement, a grant aimed at it, and a ceiling line all
/// read one matcher as one family. What differs after this is who may hold that
/// family, which is the caller's.
fn matcher_family(
    matcher: &RMatcher,
    plane: Plane,
    kind: MatcherKind,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<(Family, String)> {
    let (scheme, bare) = matcher_address(matcher, kind, refs, errors)?;
    match Family::of(scheme, plane) {
        Some(family) => Some((family, bare)),
        None => {
            errors.push(Diagnostic::at(
                "a webhook is inbound only, so there is no publishing to one: an endpoint \
                 belongs on the subscribe plane",
                matcher.val.span().clone(),
            ));
            None
        }
    }
}

/// That this family's entries are written the way this matcher writes them.
fn family_admits(
    matcher: &RMatcher,
    kind: MatcherKind,
    family: Family,
    errors: &mut Vec<Diagnostic>,
) -> bool {
    if family.admits(kind) {
        return true;
    }
    errors.push(Diagnostic::at(
        format!(
            "`{}` is not how an entry in `{}` is written; that family takes {}",
            kind.as_str(),
            family.name(),
            family.kinds()
        ),
        matcher.kind.span().clone(),
    ));
    false
}

/// What a matcher is about: the scheme it names, and what follows it.
///
/// A matcher written against a declared channel takes the channel's scheme —
/// which is the whole reason the one-spelling rule pushes authors to that form.
fn matcher_address(
    matcher: &RMatcher,
    kind: MatcherKind,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<(ChannelScheme, String)> {
    match matcher.val.value() {
        RMatcherVal::Lit(text) => match split_spellable(text) {
            Some((scheme, bare)) => Some((scheme, bare.to_string())),
            None => {
                errors.push(Diagnostic::at(
                    format!(
                        "`{text}` names no scheme, so there is no family it is about; a \
                         matcher pattern leads with {}",
                        spellable_quoted_list()
                    ),
                    matcher.val.span().clone(),
                ));
                None
            }
        },
        RMatcherVal::Chan(id) => {
            if kind != MatcherKind::Exact {
                errors.push(Diagnostic::at(
                    format!(
                        "`{}` names a declared channel, and a declared channel is one \
                         address: write `exact`, or write the pattern `{}` names as a \
                         string",
                        kind.as_str(),
                        kind.as_str()
                    ),
                    matcher.kind.span().clone(),
                ));
                return None;
            }
            let address = refs.address(*id);
            let Some((scheme, bare)) = split_spellable(address) else {
                unreachable!("a resolved channel address names a scheme");
            };
            Some((scheme, bare.to_string()))
        }
    }
}

/// One channel-family entry's pattern.
///
/// The one place this pass mirrors the runtime's own matcher validation: an empty
/// pattern, a prefix that stops mid-segment, and any reach into the anonymous
/// namespace are all silent over-grants on a path an attacker can influence, and
/// a refusal here beats the same refusal at boot. The rules it mirrors are the
/// shared predicates the runtime's own validation calls, so the mirroring is a
/// second call site rather than a second statement of the rule.
fn channel_matcher(
    kind: MatcherKind,
    bare: String,
    span: &Span,
    errors: &mut Vec<Diagnostic>,
) -> Option<DMatcher> {
    if bare.is_empty() {
        errors.push(Diagnostic::at(
            "this matcher names a scheme and nothing under it: an empty pattern matches \
             every channel on the plane"
                .to_string(),
            span.clone(),
        ));
        return None;
    }
    if is_auto_channel_name(&bare) {
        errors.push(Diagnostic::at(
            format!(
                "`{bare}` reaches into the reserved anonymous namespace: an anonymous \
                 channel's endpoints are its authority, and letting another participant \
                 in is done by giving the channel a name"
            ),
            span.clone(),
        ));
        return None;
    }
    match kind {
        MatcherKind::Exact => Some(DMatcher::Exact(Spanned::new(bare, span.clone()))),
        MatcherKind::Prefix => {
            if !ends_at_matcher_boundary(&bare) {
                errors.push(Diagnostic::at(
                    format!(
                        "the prefix `{bare}` does not end at a segment boundary ({}), so \
                         it over-matches every sibling name it is the start of",
                        or_list(MATCHER_BOUNDARIES)
                    ),
                    span.clone(),
                ));
                return None;
            }
            Some(DMatcher::Prefix(Spanned::new(bare, span.clone())))
        }
        _ => unreachable!("a channel family admits `exact` and `prefix` only"),
    }
}

/// `mqtt:<client>:<filter>`, dissected.
///
/// The client and the filter are separate dimensions of one entry, so both are
/// required; what a legal topic filter looks like stays the runtime's.
fn mqtt_sub_entry(
    bare: &str,
    span: &Span,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<DMqttSub> {
    let Some((client, filter)) = bare.split_once(':') else {
        errors.push(Diagnostic::at(
            format!(
                "`mqtt:{bare}` names a client and no filter; a topic_filter entry is \
                 written `mqtt:<client>:<filter>`"
            ),
            span.clone(),
        ));
        return None;
    };
    if client.is_empty() || filter.is_empty() {
        errors.push(Diagnostic::at(
            format!(
                "`mqtt:{bare}` leaves half of the entry empty; a topic_filter entry names \
                 both the client that connects and the filter it subscribes"
            ),
            span.clone(),
        ));
        return None;
    }
    if !refs.client(client, span, errors) {
        return None;
    }
    Some(DMqttSub {
        client: Spanned::new(client.to_string(), span.clone()),
        topic_filter: Spanned::new(filter.to_string(), span.clone()),
    })
}

/// The client slug an outbound mqtt entry names.
fn mqtt_client(
    bare: &str,
    span: &Span,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<Spanned<String>> {
    if bare.is_empty() {
        errors.push(Diagnostic::at(
            "`mqtt:` names no client; an outbound mqtt entry is written `mqtt:<client>`"
                .to_string(),
            span.clone(),
        ));
        return None;
    }
    if bare.contains(':') {
        errors.push(Diagnostic::at(
            format!(
                "`mqtt:{bare}` names more than a client; publishing is scoped to the \
                 client and has no topic dimension to narrow"
            ),
            span.clone(),
        ));
        return None;
    }
    if !refs.client(bare, span, errors) {
        return None;
    }
    Some(Spanned::new(bare.to_string(), span.clone()))
}

const PUSH_DEPTH_KEY: &str = "push_depth";
const RETAIN_DEPTH_KEY: &str = "retain_depth";

/// The keys a remote's subscribe entry states, in the order it must state them.
///
/// Exported so that a parity test can gate the runtime struct's fields against
/// this list.
pub const REMOTE_CEILING_KEYS: [&str; 2] = [PUSH_DEPTH_KEY, RETAIN_DEPTH_KEY];

/// The depths a remote's subscribe entry caps a matching subscription at.
///
/// Both required, and plain counts: a remote has no `channel` block of its own to
/// inherit a depth from, and an unbounded window is not an answer a network
/// peer may be given.
/// The admitted keys are [`REMOTE_CEILING_KEYS`], pinned by a parity test in
/// `brenn-lib` against the runtime struct's fields.
fn remote_ceilings(
    matcher: &RMatcher,
    span: &Span,
    errors: &mut Vec<Diagnostic>,
) -> Option<(u64, u64)> {
    let mut push = None;
    let mut retain = None;
    let mut refused = false;
    for (key, value) in &matcher.tail {
        let slot = match key.as_str() {
            PUSH_DEPTH_KEY => &mut push,
            RETAIN_DEPTH_KEY => &mut retain,
            other => {
                errors.push(Diagnostic::at(
                    format!(
                        "`{other}` is not part of a remote's subscribe entry: it states \
                         push_depth and retain_depth and nothing else"
                    ),
                    value.span().clone(),
                ));
                refused = true;
                continue;
            }
        };
        match count(value, key, errors) {
            Some(count) => *slot = Some(count),
            None => refused = true,
        }
    }
    for (key, present) in [
        (PUSH_DEPTH_KEY, push.is_some()),
        (RETAIN_DEPTH_KEY, retain.is_some()),
    ] {
        if !present && !refused {
            errors.push(Diagnostic::at(
                format!(
                    "a remote's subscribe entry states {key}: a network peer holds no \
                     channel declaration to inherit a depth from"
                ),
                span.clone(),
            ));
            refused = true;
        }
    }
    if refused {
        return None;
    }
    let (push, retain) = (push?, retain?);
    if retain < 1 {
        errors.push(Diagnostic::at(
            "a remote's retain_depth is at least 1: a subscription that retains nothing \
             has nothing for a cursor to resume against"
                .to_string(),
            span.clone(),
        ));
        return None;
    }
    Some((push, retain))
}

/// One depth ceiling: a count, and nothing an unbounded window is spelled with.
fn count(value: &RVal, key: &str, errors: &mut Vec<Diagnostic>) -> Option<u64> {
    match value.value() {
        RValue::Int(written) => match u64::try_from(*written) {
            Ok(count) => Some(count),
            Err(_) => {
                errors.push(Diagnostic::at(
                    format!("{key} is {written}, and a depth is a count"),
                    value.span().clone(),
                ));
                None
            }
        },
        other => {
            errors.push(Diagnostic::at(
                format!(
                    "{key} is {}, and a remote's ceiling is a plain count: an unbounded \
                     window is not an answer a network peer may be given",
                    other.kind()
                ),
                value.span().clone(),
            ));
            None
        }
    }
}

const PUBLISH_PER_ACTIVATION_KEY: &str = "publish_per_activation";
const PUBLISH_CAPACITY_KEY: &str = "publish_capacity";

/// The keys an outbound MQTT entry may carry, in the order it states them.
///
/// Exported so that a parity test in `brenn-lib` can gate the runtime struct's
/// non-client fields against this list.
pub const MQTT_SINK_KEYS: [&str; 2] = [PUBLISH_PER_ACTIVATION_KEY, PUBLISH_CAPACITY_KEY];

/// The egress budget an `mqtt_publish` entry overrides for the sink it mints.
///
/// Both keys are optional — an entry that states neither takes the runtime's
/// default budget — and both are token counts, so an integer is the same number
/// as the float it widens to. Only a top-level component holds an MQTT sink of
/// its own; every other entity publishes through a host that budgets its
/// egress elsewhere, and a tail there is refused.
///
/// `None` says something in the tail was refused: the entry is still the client
/// it names, but not the budget that was written.
fn mqtt_sink_budget(
    matcher: &RMatcher,
    holder: Holder<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<(Option<f64>, Option<f64>)> {
    if matcher.tail.is_empty() {
        return Some((None, None));
    }
    if holder.kind != EntityKind::Component(ComponentHost::TopLevel) {
        for (key, value) in &matcher.tail {
            errors.push(Diagnostic::at(
                format!(
                    "`{key}` is not part of {} `{}`'s `mqtt_publish` entry: an egress budget \
                     tunes the sink a component holds, and this entity publishes through a \
                     host that budgets its own",
                    holder.kind.label(),
                    holder.label
                ),
                value.span().clone(),
            ));
        }
        return None;
    }
    let mut fill = None;
    let mut capacity = None;
    let mut refused = false;
    for (key, value) in &matcher.tail {
        let slot = match key.as_str() {
            PUBLISH_PER_ACTIVATION_KEY => &mut fill,
            PUBLISH_CAPACITY_KEY => &mut capacity,
            other => {
                errors.push(Diagnostic::at(
                    format!(
                        "`{other}` is not part of an `mqtt_publish` entry: the sink it mints \
                         takes publish_per_activation and publish_capacity and nothing else"
                    ),
                    value.span().clone(),
                ));
                refused = true;
                continue;
            }
        };
        match tokens(value, key, errors) {
            Some(count) => *slot = Some(count),
            None => refused = true,
        }
    }
    match refused {
        true => None,
        false => Some((fill, capacity)),
    }
}

/// A budget knob's value: a number of tokens.
///
/// An integer widens, because a budget written `2` is the same number as `2.0`
/// and the language has no float literal for a whole number.
#[expect(
    clippy::cast_precision_loss,
    reason = "a budget knob is a small count; the wide end of i64 is not a rate"
)]
fn tokens(value: &RVal, key: &str, errors: &mut Vec<Diagnostic>) -> Option<f64> {
    match value.value() {
        RValue::Flt(number) => Some(*number),
        RValue::Int(count) => Some(*count as f64),
        other => {
            errors.push(Diagnostic::at(
                format!(
                    "{key} is {}, and a budget is a number of tokens",
                    other.kind()
                ),
                value.span().clone(),
            ));
            None
        }
    }
}

/// Every attribute on a matcher that carries none. Returns whether any was
/// refused, which is what tells the caller the entry is not what was written.
fn refuse_tail(matcher: &RMatcher, family: Family, errors: &mut Vec<Diagnostic>) -> bool {
    for (key, value) in &matcher.tail {
        errors.push(Diagnostic::at(
            format!(
                "`{key}` is not part of an entry in `{}`: that family's entries are a \
                 pattern and nothing else",
                family.name()
            ),
            value.span().clone(),
        ));
    }
    !matcher.tail.is_empty()
}

/// The refusal for a family the holder's entity type does not have.
fn no_such_family(family: Family, holder: Holder<'_>) -> String {
    format!(
        "{} `{}` can hold no `{}` authority: {}",
        holder.kind.label(),
        holder.label,
        family.name(),
        family.absent_reason(holder.kind)
    )
}

/// The lists an entry is filed into: an app-side entity's families, or the
/// four a remote has.
enum Lists<'a> {
    Acl(&'a mut DAclSet),
    Remote(&'a mut DRemoteAuthority),
}

/// What two entries being the same entry means: the shape of the thing
/// authorized, borrowed from the entry that holds it.
///
/// A variant of its own per entry shape, so a new entry kind cannot deduplicate
/// against an existing one by sharing a spelling.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum EntryKey<'a> {
    Exact(&'a str),
    Prefix(&'a str),
    Ceiling(&'a str),
    MqttSub(&'a str, &'a str),
    MqttPub(&'a str),
    Webhook(&'a str),
}

impl DEntry {
    /// Where this entry was written, or the position that derived it.
    fn span(&self) -> &Span {
        match self {
            DEntry::Chan(matcher) => matcher.span(),
            DEntry::Ceiling(entry) => entry.m.span(),
            DEntry::MqttSub(entry) => entry.client.span(),
            DEntry::MqttPub(entry) => entry.client.span(),
            DEntry::Webhook(entry) => entry.endpoint.span(),
        }
    }

    /// This entry's identity within its family.
    fn key(&self) -> EntryKey<'_> {
        match self {
            DEntry::Chan(DMatcher::Exact(pattern)) => EntryKey::Exact(pattern.value()),
            DEntry::Chan(DMatcher::Prefix(pattern)) => EntryKey::Prefix(pattern.value()),
            DEntry::Ceiling(entry) => EntryKey::Ceiling(entry.m.pattern()),
            DEntry::MqttSub(entry) => {
                EntryKey::MqttSub(entry.client.value(), entry.topic_filter.value())
            }
            DEntry::MqttPub(entry) => EntryKey::MqttPub(entry.client.value()),
            DEntry::Webhook(entry) => EntryKey::Webhook(entry.endpoint.value()),
        }
    }

    /// What this entry is about: the bare channel name, the endpoint slug, the
    /// topic filter or the client — what coverage compares by.
    fn name(&self) -> &str {
        match self {
            DEntry::Chan(matcher) => matcher.pattern(),
            DEntry::Ceiling(entry) => entry.m.pattern(),
            DEntry::MqttSub(entry) => entry.topic_filter.value(),
            DEntry::MqttPub(entry) => entry.client.value(),
            DEntry::Webhook(entry) => entry.endpoint.value(),
        }
    }

    /// Does this entry authorize a position on the channel, endpoint or topic
    /// named?
    ///
    /// Prefix coverage is a byte prefix, which is the runtime's own matcher
    /// semantics; a webhook entry is an endpoint and matches its own slug. The
    /// three that never cover are answers, not gaps: a remote holds no ports and
    /// states no subscriptions, and mqtt coverage is the runtime's gate rather
    /// than this pass's.
    fn covers(&self, name: &str) -> bool {
        match self {
            DEntry::Chan(DMatcher::Exact(pattern)) => pattern.value() == name,
            DEntry::Chan(DMatcher::Prefix(pattern)) => name.starts_with(pattern.value().as_str()),
            DEntry::Webhook(entry) => entry.endpoint.value() == name,
            DEntry::Ceiling(_) | DEntry::MqttSub(_) | DEntry::MqttPub(_) => false,
        }
    }

    /// Does this entry hold no more than `parent`, within one family?
    ///
    /// The subsumption half of attenuation. Exact under exact is equality;
    /// exact under prefix is the prefix reaching it; prefix under prefix is one
    /// prefix reaching the other; prefix under exact never holds, because a
    /// family reaches addresses one address does not. The transport shapes
    /// compare by equality: there is no arithmetic over a topic filter or an
    /// endpoint that says one covers another, so a wildcard ceiling is written
    /// as the same wildcard.
    ///
    /// Every caller compares two ceiling-shaped authorities, and a remote entry
    /// is in neither: a ceiling line in a family that carries depths is refused
    /// its tail, and a remote is never stamped, so nothing a stamp confers is
    /// one either.
    fn subsumed_by(&self, parent: &DEntry) -> bool {
        match (self, parent) {
            (DEntry::Ceiling(_), _) | (_, DEntry::Ceiling(_)) => unreachable!(
                "a remote's entry is in no ceiling and in nothing a stamp confers, so \
                 attenuation never compares one"
            ),
            (DEntry::Chan(mine), DEntry::Chan(theirs)) => match (mine, theirs) {
                (DMatcher::Exact(mine), DMatcher::Exact(theirs)) => mine.value() == theirs.value(),
                (DMatcher::Exact(mine), DMatcher::Prefix(theirs))
                | (DMatcher::Prefix(mine), DMatcher::Prefix(theirs)) => {
                    mine.value().starts_with(theirs.value().as_str())
                }
                (DMatcher::Prefix(_), DMatcher::Exact(_)) => false,
            },
            (DEntry::MqttSub(mine), DEntry::MqttSub(theirs)) => {
                (mine.client.value(), mine.topic_filter.value())
                    == (theirs.client.value(), theirs.topic_filter.value())
            }
            (DEntry::MqttPub(mine), DEntry::MqttPub(theirs)) => {
                mine.client.value() == theirs.client.value()
            }
            (DEntry::Webhook(mine), DEntry::Webhook(theirs)) => {
                mine.endpoint.value() == theirs.endpoint.value()
            }
            // Two entries of one family are one shape, and every caller
            // compares within a family; a pair that is not is this pass
            // contradicting itself, which is a panic rather than an answer.
            _ => unreachable!("attenuation compares two entries of one family"),
        }
    }

    /// File this entry under the family it was resolved for.
    ///
    /// The one table pairing a family with the entry a matcher in it becomes and
    /// the list it lands in. `resolve_entry` decides that pairing, so a mismatch
    /// here is this pass contradicting itself, not a document doing anything.
    fn file(self, family: Family, lists: &mut Lists<'_>) {
        match (lists, family, self) {
            (Lists::Acl(acl), Family::BrennSubscribe, DEntry::Chan(m)) => {
                acl.brenn_subscribe.push(m);
            }
            (Lists::Acl(acl), Family::BrennPublish, DEntry::Chan(m)) => acl.brenn_publish.push(m),
            (Lists::Acl(acl), Family::EphemeralSubscribe, DEntry::Chan(m)) => {
                acl.ephemeral_subscribe.push(m);
            }
            (Lists::Acl(acl), Family::EphemeralPublish, DEntry::Chan(m)) => {
                acl.ephemeral_publish.push(m);
            }
            (Lists::Acl(acl), Family::LocalSubscribe, DEntry::Chan(m)) => {
                acl.local_subscribe.push(m);
            }
            (Lists::Acl(acl), Family::LocalPublish, DEntry::Chan(m)) => acl.local_publish.push(m),
            (Lists::Acl(acl), Family::MqttSubscribe, DEntry::MqttSub(entry)) => {
                acl.mqtt_subscribe.push(entry);
            }
            (Lists::Acl(acl), Family::MqttPublish, DEntry::MqttPub(entry)) => {
                acl.mqtt_publish.push(entry);
            }
            (Lists::Acl(acl), Family::Webhook, DEntry::Webhook(entry)) => acl.webhook.push(entry),
            (Lists::Remote(remote), Family::BrennSubscribe, DEntry::Ceiling(entry)) => {
                remote.subscribe.push(entry);
            }
            (Lists::Remote(remote), Family::EphemeralSubscribe, DEntry::Ceiling(entry)) => {
                remote.ephemeral_subscribe.push(entry);
            }
            (Lists::Remote(remote), Family::BrennPublish, DEntry::Chan(m)) => {
                remote.publish.push(m);
            }
            (Lists::Remote(remote), Family::EphemeralPublish, DEntry::Chan(m)) => {
                remote.ephemeral_publish.push(m);
            }
            (_, family, entry) => {
                unreachable!("{entry:?} is not an entry in `{}`", family.name())
            }
        }
    }
}

// ── what a binding derives, and that every binding is authorized ─────────────
//
// A binding is the ordinary way authority is written: an operator wires a port
// to a channel and means for it to be reachable, so the entry is derived rather
// than restated. An explicit `acl` statement says the opposite — that the plane's
// authority is written out — so it suppresses derivation, and every binding on
// the plane then has to be covered by what was written.

/// One position that attaches an entity to a channel.
struct Bound<'a> {
    /// Which planes it covers: `in` subscribes, `out` publishes, `io` does both.
    dir: PortDir,
    chan: &'a RChanRef,
    span: Span,
    /// What the position is called in a diagnostic.
    what: &'static str,
}

/// One position, resolved: which family authorizes it and what has to be found
/// there.
struct Position {
    plane: Plane,
    family: Family,
    /// What coverage compares: the bare channel name, or the endpoint slug.
    name: String,
    /// The address as written, for a refusal that has to name it.
    address: String,
    span: Span,
    what: &'static str,
}

/// The planes a direction covers.
fn planes(dir: PortDir) -> &'static [Plane] {
    match dir {
        PortDir::In => &[Plane::Subscribe],
        PortDir::Out => &[Plane::Publish],
        PortDir::Io => &[Plane::Subscribe, Plane::Publish],
    }
}

/// Every position one surface attaches through: the bound ports of every
/// instance placed on it.
fn surface_bounds(surface: &RSurface) -> Vec<Bound<'_>> {
    surface
        .components
        .iter()
        .flat_map(|instance| binding_bounds(&instance.bindings))
        .collect()
}

/// The positions a set of bindings holds.
///
/// A free `io` port is not one: it connects nothing, the ring it reads is minted
/// for the page it is served to, and there is no channel for an entry to be
/// about. Nor is a link-bound binding: the link's channel is minted at boot from
/// the endpoint set, and the transport capability and channel matcher its
/// endpoints need are injected there — binding the port *is* the authorization,
/// so there is nothing here to derive or to cover.
fn binding_bounds(bindings: &[RBinding]) -> Vec<Bound<'_>> {
    bindings
        .iter()
        .filter_map(|binding| {
            let chan = binding.chan.as_ref()?;
            if matches!(chan, RChanRef::Link(_)) {
                return None;
            }
            Some(Bound {
                dir: binding.dir(),
                chan,
                span: binding.port.span().clone(),
                what: "binding",
            })
        })
        .collect()
}

/// Where the first send this entity makes outside the position walk is.
///
/// Two shapes hold none: a free `io` port, whose page-local ring is minted for
/// the page it is served to, and any link-bound binding with a publishing role,
/// whose channel is minted at boot. Neither derives an entry — but both are
/// something the component publishes into, which is what the `ports` rule asks
/// about. Boot must count both halves of every `io` port for the same reason.
fn free_send(bindings: &[RBinding]) -> Option<Span> {
    bindings
        .iter()
        .find(|binding| match &binding.chan {
            None => true,
            Some(RChanRef::Link(_)) => {
                matches!(binding.dir(), PortDir::Out | PortDir::Io)
            }
            Some(_) => false,
        })
        .map(|binding| binding.port.span().clone())
}

/// An agent's positions: its `subscribe` statements, which are inbound.
fn agent_bounds(agent: &RAgent) -> Vec<Bound<'_>> {
    agent
        .subs
        .iter()
        .map(|sub| Bound {
            dir: PortDir::In,
            chan: &sub.chan,
            span: sub.span.clone(),
            what: "subscription",
        })
        .collect()
}

/// One egress budget per MQTT sink.
///
/// A client is one sink however many entries name it, so two entries carrying a
/// budget for one client state two answers to one question. This must be caught
/// here; by boot time the conflict would panic.
fn check_mqtt_sinks(subject: &Subject<'_>, stated: &mut Stated, errors: &mut Vec<Diagnostic>) {
    let mut budgeted: Vec<&str> = Vec::new();
    let mut duplicates: Vec<(String, Span)> = Vec::new();
    for (family, entry, _) in &stated.entries {
        let (Family::MqttPublish, DEntry::MqttPub(sink)) = (family, entry) else {
            continue;
        };
        if sink.publish_per_activation.is_none() && sink.publish_capacity.is_none() {
            continue;
        }
        let client = sink.client.value().as_str();
        match budgeted.contains(&client) {
            true => duplicates.push((client.to_string(), sink.client.span().clone())),
            false => budgeted.push(client),
        }
    }
    for (client, span) in duplicates {
        errors.push(Diagnostic::at(
            format!(
                "{} `{}` states an egress budget for client `{client}` twice: one client is one \
                 sink, and one sink holds one budget",
                subject.kind.label(),
                subject.label
            ),
            span,
        ));
        stated.refused = true;
    }
}

/// What one entity's positions come to: the entries they derive where nothing
/// explicit holds the plane, and that every one of them is authorized by the
/// entries the entity ends up with.
fn derive_bounds(
    stated: &mut Stated,
    subject: &Subject<'_>,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) {
    let kind = subject.kind;
    let label = &subject.label;
    let mut positions = Vec::new();
    // A free `io` port is a send before any position is walked: the ring is
    // minted whether or not the document names a channel.
    stated.output = subject.free_send.clone();
    // What is wrong with a position rather than with this entity's lists:
    // reported only by the entity that owns the position, so a binding a
    // surface and its instance both attach through earns one message.
    //
    // Dropping the non-owner's copy is sound only while the owner reaches the
    // same refusal, which rests on two tables: `bindable` admits the same
    // schemes to a surface and to a component placed on it, and `bound_entry`
    // is infallible for every family those schemes reach (its fallible arms are
    // mqtt and webhook, which `bindable` keeps off a surface). Widening either
    // table — lifting `mqtt` onto a page, say — must come with a pass that
    // reports here, or a refused document would carry no message at all.
    let mut shared = Vec::new();
    for bound in &subject.bounds {
        let Some((scheme, bare)) = bound_address(bound, kind, label, refs, &mut shared) else {
            stated.refused = true;
            continue;
        };
        for &plane in planes(bound.dir) {
            if plane == Plane::Publish && stated.output.is_none() {
                stated.output = Some(bound.span.clone());
            }
            // One lookup answers both questions: a scheme with no family on this
            // plane, and a scheme this position may not name, are refused the
            // same way. No invariant spans the two tables.
            let family = Family::of(scheme, plane)
                .filter(|_| bindable_schemes(kind, plane).contains(&scheme));
            let Some(family) = family else {
                shared.push(Diagnostic::at(
                    unbindable(scheme, plane, kind, label, bound),
                    bound.span.clone(),
                ));
                stated.refused = true;
                continue;
            };
            // A family the entity type does not hold derives nothing and is not
            // covered: a surface's `local:` frames are authorized by the page
            // they are served to, out of band from any list.
            if !family.held_by(kind) {
                continue;
            }
            let Some(entry) = bound_entry(family, &bare, &bound.span, refs, &mut shared) else {
                stated.refused = true;
                continue;
            };
            let name = entry.name().to_string();
            if !stated.holds(plane) {
                stated.add_derived(family, entry);
            }
            positions.push(Position {
                plane,
                family,
                name,
                address: format!("{}{bare}", scheme.prefix()),
                span: bound.span.clone(),
                what: bound.what,
            });
        }
    }

    if subject.owns_bindings {
        errors.append(&mut shared);
    }

    for position in positions {
        // MQTT coverage would take filter-subset logic, which is the runtime's
        // and stays there: it gates every delivery anyway, and no boot check
        // asks this question.
        if position.family == Family::MqttSubscribe {
            continue;
        }
        if stated.covers(position.family, &position.name) {
            continue;
        }
        let mut error = Diagnostic::at(
            format!(
                "this {} reaches `{}`, which nothing in {}'s `{}` authority covers: an \
                 explicit `acl {}` is the whole authority for the plane, so a {} beside it \
                 derives nothing",
                position.what,
                position.address,
                label,
                position.family.name(),
                position.plane.word(),
                position.what,
            ),
            position.span,
        );
        error.related = stated.sites(position.plane);
        errors.push(error);
        stated.refused = true;
    }
}

/// The refusal for a position on a scheme the entity cannot attach to there.
fn unbindable(
    scheme: ChannelScheme,
    plane: Plane,
    kind: EntityKind,
    label: &str,
    bound: &Bound<'_>,
) -> String {
    if let Some(family) = Family::of(scheme, plane).filter(|family| !family.held_by(kind)) {
        return format!(
            "{} `{label}` can hold no `{}` authority, so this {} cannot name `{}`: {}",
            kind.label(),
            family.name(),
            bound.what,
            scheme.prefix(),
            family.absent_reason(kind),
        );
    }
    let schemes = bindable_schemes(kind, plane);
    if schemes.is_empty() {
        // The two empty rows are an agent's publish plane and a remote's every
        // plane, and neither reaches here: an agent's positions are its
        // subscriptions, which are inbound, and a remote holds no position at all.
        unreachable!(
            "a {} attaches through no {}-plane position, so there is none to refuse",
            kind.label(),
            plane.word()
        );
    }
    format!(
        "a {} {} on the {} plane cannot name `{}`: it names {}",
        kind.label(),
        bound.what,
        plane.word(),
        scheme.prefix(),
        or_list(schemes.iter().map(|scheme| scheme.prefix())),
    )
}

/// What a position names: the scheme, and what follows it.
///
/// A transportable address exists because a `channel` block declares it, so a
/// literal one that names no declaration attaches to nothing — except in the tool
/// namespaces, which the substrate mints and no document declares.
fn bound_address(
    bound: &Bound<'_>,
    kind: EntityKind,
    label: &str,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<(ChannelScheme, String)> {
    let address = match bound.chan {
        RChanRef::Decl(id) => refs.address(*id),
        RChanRef::Addr(address) => address.value().as_str(),
        // `binding_bounds` and `agent_bounds` are the only producers, and
        // neither makes a position of a link.
        RChanRef::Link(_) => unreachable!("a link holds no position"),
    };
    let Some((scheme, bare)) = split_spellable(address) else {
        unreachable!("a resolved address names a scheme");
    };
    if matches!(bound.chan, RChanRef::Addr(_))
        && matches!(scheme, ChannelScheme::Brenn | ChannelScheme::Ephemeral)
        && !(scheme == ChannelScheme::Brenn && in_a_tool_namespace(bare))
    {
        errors.push(Diagnostic::at(
            format!(
                "`{address}` names no declared channel, so this {} of {} `{label}` \
                 attaches to nothing: a transportable channel exists because a `channel` \
                 block declares it",
                bound.what,
                kind.label(),
            ),
            bound.span.clone(),
        ));
        return None;
    }
    Some((scheme, bare.to_string()))
}

/// The entry one position derives.
///
/// An exact channel name on the channel families; on the ingress families, the
/// slug the address names, checked against the block that answers to it — a
/// position naming an endpoint or a client nothing declares reaches a channel
/// nothing mints.
fn bound_entry(
    family: Family,
    bare: &str,
    span: &Span,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<DEntry> {
    Some(match family {
        Family::MqttSubscribe => DEntry::MqttSub(mqtt_sub_entry(bare, span, refs, errors)?),
        Family::Webhook => {
            if !refs.endpoint(bare, span, errors) {
                return None;
            }
            DEntry::Webhook(DWebhook {
                endpoint: Spanned::new(bare.to_string(), span.clone()),
            })
        }
        _ => DEntry::Chan(DMatcher::Exact(Spanned::new(
            bare.to_string(),
            span.clone(),
        ))),
    })
}

// ── principals: authority declared to be delegated from ─────────────────────
//
// A bare principal is an authority and nothing else: no runtime body, nothing
// lowered, declared so that arrangements can be delegated authority under it.
// One relation holds a whole chain together — attenuation, `a ⊑ b`, "a holds no
// more than b" — and every rule here is that relation applied at a different
// pair.
//
// The operator is the unnamed root of every chain, and its authority cannot be
// inherited: a principal written directly under the operator holds exactly what
// it writes. So everything a chain will ever delegate is written at its root,
// and a child can only narrow.

/// An authority: the words it holds and the reach it holds.
///
/// Words are grant words compared as spelled, across every grant vocabulary the
/// language has, so `alert` covers both a component's alert capability and a
/// surface's alert attach right. That is deliberate: both words consent to the
/// same consequence, and keying on the vocabulary as well would make one consent
/// spelled twice.
///
/// A word carries the position it was written at, because a refusal about a word
/// points at the word. Sorted, so a diagnostic that lists words lists them the
/// same way twice.
#[derive(Clone, Default)]
struct Authority {
    words: BTreeMap<String, Span>,
    reach: Vec<(Family, DEntry)>,
}

impl Authority {
    /// Whether these two authorities hold the same words.
    ///
    /// Keys only: the map's values are where each word was written, and two
    /// documents' spans are never equal.
    fn same_words(&self, other: &Authority) -> bool {
        self.words.keys().eq(other.words.keys())
    }

    /// Every entry of one family this authority holds.
    fn family(&self, family: Family) -> impl Iterator<Item = &DEntry> {
        self.reach
            .iter()
            .filter(move |(held, _)| *held == family)
            .map(|(_, entry)| entry)
    }

    /// Every word and entry of this authority that `parent` does not cover.
    ///
    /// The attenuation relation itself: `self ⊑ parent` exactly when this is
    /// empty. Reach is compared within a family — an entry says nothing about a
    /// plane or a scheme it is not about.
    fn attenuated_from(&self, parent: &Authority) -> Vec<Excess> {
        let mut excess = Vec::new();
        for (word, span) in &self.words {
            if !parent.words.contains_key(word) {
                excess.push(Excess::Word(word.clone(), span.clone()));
            }
        }
        for (family, entry) in &self.reach {
            if !parent.family(*family).any(|held| entry.subsumed_by(held)) {
                excess.push(Excess::Reach(
                    entry.name().to_string(),
                    entry.span().clone(),
                ));
            }
        }
        excess
    }
}

/// One thing an authority holds that the authority it was compared against does
/// not, and where it was written.
enum Excess {
    Word(String, Span),
    Reach(String, Span),
}

impl Excess {
    /// What this excess is, as the refusal about it names it.
    fn describe(&self) -> String {
        match self {
            Excess::Word(word, _) => format!("`{word}` is not a word"),
            Excess::Reach(name, _) => format!("`{name}` is not reach"),
        }
    }

    /// The excess named as a noun: "`ui` holds the word `tools`, which …".
    fn what(&self) -> String {
        match self {
            Excess::Word(word, _) => format!("the word `{word}`"),
            Excess::Reach(name, _) => format!("reach over `{name}`"),
        }
    }

    /// Where it was written.
    fn span(&self) -> &Span {
        match self {
            Excess::Word(_, span) | Excess::Reach(_, span) => span,
        }
    }
}

/// The words a ceiling may name: every grant vocabulary's spellings, in one
/// namespace.
///
/// Derived, never authored — a word added to either vocabulary is nameable in a
/// ceiling with no edit here.
static CEILING_WORDS: LazyLock<BTreeSet<&'static str>> = LazyLock::new(|| {
    Capability::ALL
        .into_iter()
        .map(Capability::word)
        .chain(AttachGrant::ALL.into_iter().map(AttachGrant::word))
        .collect()
});

/// A ceiling body: the `grants` line it writes and the `acl` lines it writes.
struct Body<'a> {
    grants: Option<&'a RWordList>,
    acls: &'a [RAcl],
}

/// What a ceiling body writes, resolved.
struct Written {
    /// The two axes as an authority of their own: only what the body wrote.
    axes: Authority,
    /// Whether a `grants` line was written at all — which is what says the words
    /// axis is replaced rather than inherited. An empty list is a statement.
    wrote_words: bool,
    /// The families the `acl` lines resolved to, each with the plane word of the
    /// line that reached it. Replacement and the dead-config rules are keyed on
    /// these: a line replaces the inherited entries of the family it resolves
    /// to, and no other.
    families: Vec<(Family, Spanned<String>)>,
    /// Whether anything the body wrote was refused. What it comes to is then not
    /// what the document says, so no rule about that is asked and nothing
    /// downstream inherits it.
    refused: bool,
}

impl Written {
    /// Whether the body wrote no axis at all.
    fn empty(&self) -> bool {
        !self.wrote_words && self.families.is_empty()
    }

    /// The entries this body wrote in one family.
    fn family(&self, family: Family) -> impl Iterator<Item = &DEntry> {
        self.axes.family(family)
    }
}

/// One ceiling body's two axes, resolved, with what is illegal in a ceiling
/// refused.
fn written_authority(body: Body<'_>, refs: &Refs<'_>, errors: &mut Vec<Diagnostic>) -> Written {
    let mut written = Written {
        axes: Authority::default(),
        wrote_words: false,
        families: Vec::new(),
        refused: false,
    };
    if let Some(list) = body.grants {
        written.wrote_words = true;
        for word in &list.words {
            if !CEILING_WORDS.contains(word.name.value().as_str()) {
                errors.push(Diagnostic::at(
                    format!(
                        "`{}` is not a grant word, so it caps nothing; a ceiling names {}",
                        word.name.value(),
                        or_list(CEILING_WORDS.iter().copied())
                    ),
                    word.name.span().clone(),
                ));
                written.refused = true;
                continue;
            }
            written
                .axes
                .words
                .insert(word.name.value().clone(), word.name.span().clone());
        }
    }
    for acl in body.acls {
        let Some(plane) = acl_plane(acl, errors) else {
            written.refused = true;
            continue;
        };
        for matcher in &acl.matchers {
            match ceiling_entry(matcher, plane, refs, errors) {
                Some((family, entry)) => {
                    if !written.families.iter().any(|(held, _)| *held == family) {
                        written.families.push((family, acl.plane.clone()));
                    }
                    written.axes.reach.push((family, entry));
                }
                None => written.refused = true,
            }
        }
    }
    written
}

/// One `acl` matcher of a ceiling, as the family entry it becomes.
///
/// The same matcher→family core a running entity's statement goes through, with
/// two questions answered differently. Which families are cappable is one: every
/// family but the confined ones, because a `local:` channel reaches the one
/// component that binds it and is authorized by the host that serves it rather
/// than by anything a ceiling holds. Depth tails are the other: a ceiling caps
/// reach, and a window belongs on the position that holds it.
fn ceiling_entry(
    matcher: &RMatcher,
    plane: Plane,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Option<(Family, DEntry)> {
    let kind = *matcher.kind.value();
    let span = matcher.val.span().clone();
    let (family, bare) = matcher_family(matcher, plane, kind, refs, errors)?;
    if family.confined() {
        errors.push(Diagnostic::at(
            format!(
                "a ceiling caps no `{}` authority: a confined channel reaches the one \
                 component that binds it, authorized by the host that serves it",
                family.name()
            ),
            span,
        ));
        return None;
    }
    if !family_admits(matcher, kind, family, errors) {
        return None;
    }
    let mut refused = false;
    for (key, value) in &matcher.tail {
        errors.push(Diagnostic::at(
            format!("`{key}` is a depth, and a ceiling caps reach rather than depth"),
            value.span().clone(),
        ));
        refused = true;
    }
    if refused {
        return None;
    }
    Some((
        family,
        plain_entry(family, kind, bare, &span, refs, errors)?,
    ))
}

/// Every ceiling in the document, and that each holds no more than the one it
/// is under.
///
/// Two halves of one relation. First every declared principal's authority,
/// parents first — which the cycle refusal in resolution is what makes
/// possible: a chain bottoms out at the operator, so a walk that defers a
/// principal until its parent is built terminates. Then every recorded stamp's
/// ceiling, and that what its arrangement confers fits under it.
fn check_ceilings(
    config: &ResolvedConfig,
    conferred: &Conferred,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) {
    // Asserted rather than tolerated: every rule below is an authority gate, and
    // a pass that answered a chain it could not order by checking nothing would
    // compile a document with no ceiling enforced anywhere and no message saying
    // so. Resolution refuses a chain that does not bottom out, and a document
    // that does not resolve is never derived.
    let order = principal_order(config)
        .unwrap_or_else(|| unreachable!("resolution refuses a principal chain that cycles"));
    let root = Authority::default();
    // TODO(ceiling-principal-ids): a principal is addressed by its dotted handle
    // across three structures that have to be kept in step — the walk's own
    // name→index map, these authorities, and the chains below — over one `Vec`
    // that an id would index directly, as `StampId` indexes the stamps.
    let mut authorities: HashMap<String, Authority> = HashMap::new();
    // Which principals hold an authority that is not what their text says,
    // because something in their body was refused. Nothing under one is judged:
    // see `Delegated`.
    let mut unstated: HashSet<String> = HashSet::new();
    // Root-first, so a suggestion that has to name every principal a word must
    // be added to can read the chain off one map.
    let mut chains: HashMap<String, Vec<String>> = HashMap::new();
    // What each principal's body wrote, by declaration index, and whether it is
    // text the dead-config rules below are asked about at all. A body that was
    // refused, or one under a body that was, has already been reported on; a
    // second message about what it delegates would be a second message about one
    // mistake.
    let mut bodies: Vec<Option<Written>> = (0..config.principals.len()).map(|_| None).collect();
    for index in order {
        let principal = &config.principals[index];
        let label = principal.handle.dotted();
        let parent = principal.parent.as_ref().map(HandlePath::dotted);
        // Asserted, not tolerated: an `under` that named no declared principal
        // is refused in resolution and a document that does not resolve is never
        // derived, so a miss here is this pass looking a principal up under a
        // name it does not carry — a compiler bug, and one that would otherwise
        // read as the deployer's consent being too narrow.
        let inherited = parent.as_ref().map_or(&root, |parent| {
            authorities.get(parent).unwrap_or_else(|| {
                unreachable!("resolution refuses an `under` that names no principal")
            })
        });
        let written = written_authority(
            Body {
                grants: principal.grants.as_ref(),
                acls: &principal.acls,
            },
            refs,
            errors,
        );
        // A parent whose own text was refused holds an authority the document
        // does not state, so no rule is asked of what is under it.
        let above_unstated = parent
            .as_ref()
            .is_some_and(|parent| unstated.contains(parent));
        let delegation = parent
            .as_ref()
            .filter(|_| !above_unstated)
            .map(|parent| Delegation {
                holder: format!("`{label}`"),
                above: format!("`{parent}`"),
                noun: "a principal's body",
                rule: "a principal holds no more than the one it is under",
                span: principal.span.clone(),
            });
        if written.refused || above_unstated {
            unstated.insert(label.clone());
        }
        let (authority, body_refused) = attenuate(delegation.as_ref(), inherited, &written, errors);
        if !body_refused && !above_unstated {
            bodies[index] = Some(written);
        }
        let chain = match &parent {
            Some(parent) => {
                let mut chain = chains.get(parent).cloned().unwrap_or_else(|| {
                    unreachable!("`principal_order` lists a parent before its children")
                });
                chain.push(parent.clone());
                chain
            }
            None => Vec::new(),
        };
        chains.insert(label.clone(), chain);
        authorities.insert(label, authority);
    }
    let delegated = Delegated {
        authorities,
        unstated,
        chains,
    };
    // Computed here rather than inside the stamp pass because both halves read
    // them: a stamp's ceiling is judged against its own subtree, and a
    // principal's text against the union of every subtree under it.
    let confers = confers(config, conferred);
    let defaults = default_reach(config);
    check_stamps(config, &confers, &defaults, &delegated, refs, errors);
    check_dead_principals(config, &confers, &defaults, &delegated, &bodies, errors);
}

/// That every `principal` delegates its authority to something, and that every
/// word and line it writes is authority some arrangement under it holds.
///
/// The inverse direction for a declaration rather than for a stamp, and the
/// reason it is not a copy of [`check_dead_ceiling`]: a principal is reusable,
/// so it is judged by the union of everything under it. A principal that roots
/// several stamps holds a word legitimately when any one of them needs it, and
/// the word is dead only when none does.
fn check_dead_principals(
    config: &ResolvedConfig,
    confers: &HashMap<StampId, Confers>,
    defaults: &HashMap<StampId, Vec<(Family, DEntry)>>,
    delegated: &Delegated,
    bodies: &[Option<Written>],
    errors: &mut Vec<Diagnostic>,
) {
    // An arrangement that confers nothing, for a stamp that holds no entry at
    // all: the union below reads it as "holds no word, reaches nothing".
    let nothing = Confers::default();
    for (index, principal) in config.principals.iter().enumerate() {
        let label = principal.handle.dotted();
        let stamps = delegated_stamps(config, delegated, &label);
        if stamps.is_empty() {
            // A principal with children delegates through them, so the leaf of
            // the chain is where the whole chain's failure to reach an
            // arrangement is reported: refusing every ancestor as well, and then
            // every word each of them wrote, is one mistake fanned out over a
            // document.
            let rooted = config.principals.iter().any(|child| {
                child
                    .parent
                    .as_ref()
                    .is_some_and(|parent| parent.dotted() == label)
            });
            // A principal handed to a class as a `Principal` argument was
            // delegated, and the message below would be false at the
            // declaration. What is dead in that arrangement is the parameter
            // the class never wrote `under` with, in a file that may be the
            // author's; the deployer's `principal` is not the place to say so.
            let handed = config
                .handed_principals
                .iter()
                .any(|argument| argument.dotted() == label);
            if !rooted && !handed {
                errors.push(Diagnostic::at(
                    format!(
                        "`{label}` delegates to nothing: no stamp is under it and no \
                         principal is declared under it, so the authority it writes reaches \
                         no arrangement"
                    ),
                    principal.span.clone(),
                ));
            }
            continue;
        }
        let Some(written) = bodies[index].as_ref() else {
            continue;
        };
        let held: Vec<(&Confers, &[(Family, DEntry)])> = stamps
            .iter()
            .map(|stamp| {
                (
                    confers.get(stamp).unwrap_or(&nothing),
                    defaults.get(stamp).map_or(&[][..], Vec::as_slice),
                )
            })
            .collect();
        if written.wrote_words
            && written.axes.words.is_empty()
            && held.iter().all(|(confers, _)| confers.words.is_empty())
        {
            errors.push(Diagnostic::at(
                format!(
                    "no arrangement under `{label}` holds a capability, so this `grants` \
                     line caps nothing; a principal that delegates no capability writes no \
                     `grants` line"
                ),
                principal.span.clone(),
            ));
        }
        for (word, span) in &written.axes.words {
            if held
                .iter()
                .any(|(confers, _)| confers.words.contains_key(word))
            {
                continue;
            }
            errors.push(Diagnostic::at(
                format!(
                    "`{word}` caps nothing — no arrangement under `{label}` holds it; a \
                     ceiling word nothing reaches is dead config"
                ),
                span.clone(),
            ));
        }
        for (family, plane) in &written.families {
            if held
                .iter()
                .any(|(confers, defaults)| reaches_beyond(*family, confers, defaults))
            {
                continue;
            }
            errors.push(Diagnostic::at(
                format!(
                    "this `acl {}` line caps nothing any arrangement under `{label}` \
                     reaches in `{}` beyond its own channels and what it was handed; a \
                     ceiling line nothing needs is dead config",
                    plane.value(),
                    family.name()
                ),
                plane.span().clone(),
            ));
        }
    }
}

/// Every stamp one principal delegates to: those `under` it, and those under a
/// principal it is under.
///
/// Transitive through child principals, which is what makes a chain's root the
/// place every word is written and still leaves each word judged by real use.
/// A `Principal` argument the class wrote `under` with arrives here as that
/// stamp's own `under`; one it dropped arrives nowhere, which is why the
/// caller reads [`ResolvedConfig::handed_principals`] before it says a
/// principal delegates to nothing.
fn delegated_stamps(config: &ResolvedConfig, delegated: &Delegated, label: &str) -> Vec<StampId> {
    config
        .stamps
        .iter()
        .enumerate()
        .filter_map(|(index, stamp)| {
            let under = stamp.under.as_ref().map(HandlePath::dotted)?;
            let reaches =
                under == label || delegated.chain(&under).iter().any(|above| above == label);
            reaches.then_some(StampId(index))
        })
        .collect()
}

/// Whether an arrangement asks for reach in one family beyond what stamping it
/// and handing it its arguments already consented to.
///
/// The one test both dead-config directions turn on: a ceiling line in a family
/// the arrangement reaches nowhere beyond its own and its handed channels caps
/// nothing, whether it is written on the stamp or on a principal above it.
fn reaches_beyond(family: Family, confers: &Confers, defaults: &[(Family, DEntry)]) -> bool {
    confers
        .reach
        .iter()
        .filter(|(held, _, _)| *held == family)
        .any(|(_, entry, _)| {
            !defaults
                .iter()
                .any(|(held, default)| *held == family && entry.subsumed_by(default))
        })
}

/// What a refusal about one body calls the body, what it is under, and the rule
/// it breaks.
///
/// One struct because the two things that write a ceiling — a `principal`
/// declaration and a stamp — break the same rules in the same order and differ
/// only in what a reader should be told they are. A body under the operator has
/// no delegation at all: there is nothing above it to narrow.
struct Delegation {
    /// How a refusal names the body's holder: ``​`ui`​`` or ``the stamp `demo`​``.
    holder: String,
    /// How it names what the body narrows: ``​`site`​`` or ``the ceiling on the
    /// stamp `deployment`​``.
    above: String,
    /// The noun for the body itself.
    noun: &'static str,
    /// The rule an excess breaks, as its refusal states it.
    rule: &'static str,
    /// Where the whole thing is written: what the narrows-nothing refusal cites.
    span: Span,
}

/// One body's authority, built from what it inherits and what it writes.
///
/// Replacement, not intersection: on the words axis a `grants` line replaces the
/// inherited words, and on the reach axis an `acl` line replaces the inherited
/// entries of the family it resolves to. What the reader sees is what the
/// holder holds, and the compiler proves it fits rather than computing
/// something the text does not show.
///
/// The second half of the answer is whether this body's own text was refused —
/// malformed, or a widening, or a narrowing of nothing. A caller reads it to
/// keep the dead-config rules off text that already earned a message: two
/// refusals about one line is one mistake reported twice.
fn attenuate(
    delegation: Option<&Delegation>,
    inherited: &Authority,
    written: &Written,
    errors: &mut Vec<Diagnostic>,
) -> (Authority, bool) {
    // A body with a refusal in it is not the authority the document states, so
    // nothing is asked of it and nothing inherits it: the parent's authority
    // stands in, and a child is never refused for a consequence of a refusal
    // already reported.
    if written.refused {
        return (inherited.clone(), true);
    }
    let mut refused = false;
    if let Some(delegation) = delegation {
        refused = check_narrowing(delegation, inherited, written, errors);
    }
    let words = match written.wrote_words {
        true => written.axes.words.clone(),
        false => inherited.words.clone(),
    };
    let mut reach = written.axes.reach.clone();
    for (family, entry) in &inherited.reach {
        if written.families.iter().any(|(held, _)| held == family) {
            continue;
        }
        reach.push((*family, entry.clone()));
    }
    (Authority { words, reach }, refused)
}

/// That a written axis holds no more than the axis it replaces, and that it
/// narrows it.
///
/// Both directions of one rule: consent text that consents to more than it was
/// given is a widening, and consent text identical to what it was given is dead
/// config. A body under the operator writes its whole authority, so there is
/// nothing there to narrow and neither half applies — which is why the caller
/// has no delegation to pass in that case.
///
/// Answers whether it refused anything, which is what tells a caller that this
/// body's own text has already been reported on: the dead-config rules about
/// what a `principal` delegates are not asked of a body that broke this one.
fn check_narrowing(
    delegation: &Delegation,
    inherited: &Authority,
    written: &Written,
    errors: &mut Vec<Diagnostic>,
) -> bool {
    let Delegation {
        holder,
        above,
        noun,
        rule,
        span,
    } = delegation;
    if written.empty() {
        errors.push(Diagnostic::at(
            format!(
                "{holder} is under {above} and narrows nothing: {noun} writes the axis it \
                 narrows, and {above} is already a name for what this holds"
            ),
            span.clone(),
        ));
        return true;
    }
    let mut refused = false;
    for excess in written.axes.attenuated_from(inherited) {
        refused = true;
        errors.push(Diagnostic::at(
            format!(
                "{} {above} holds, which {holder} is under: {rule}, and everything a chain \
                 delegates is written at its root",
                excess.describe()
            ),
            excess.span().clone(),
        ));
    }
    if written.wrote_words && written.axes.same_words(inherited) {
        refused = true;
        errors.push(Diagnostic::at(
            format!(
                "this `grants` list is what {above} holds, so it narrows nothing; \
                 a ceiling axis that caps nothing is dead config"
            ),
            written
                .axes
                .words
                .values()
                .next()
                .cloned()
                .unwrap_or_else(|| span.clone()),
        ));
    }
    for (family, plane) in &written.families {
        let held: Vec<&DEntry> = inherited.family(*family).collect();
        let mine: Vec<&DEntry> = written.family(*family).collect();
        // Narrows nothing when the two axes reach each other in both
        // directions, which is what "a second spelling of what it replaced"
        // means. Coverage rather than a count, because a repeated matcher is one
        // reach and a matcher subsumed by another next to it adds none — and
        // both directions, because a line that reaches *further* than the axis
        // it replaces is a widening, refused by the excess rule above, and
        // telling the reader in the same breath that it is a second spelling of
        // what it replaced would be false.
        let same = !held.is_empty()
            && held
                .iter()
                .all(|other| mine.iter().any(|entry| other.subsumed_by(entry)))
            && mine
                .iter()
                .all(|entry| held.iter().any(|other| entry.subsumed_by(other)));
        if same {
            refused = true;
            errors.push(Diagnostic::at(
                format!(
                    "this `acl {}` line is what {above} holds in `{}`, so it narrows \
                     nothing; a ceiling line that caps nothing is dead config",
                    plane.value(),
                    family.name()
                ),
                plane.span().clone(),
            ));
        }
    }
    refused
}

/// The principals in an order that visits a parent before its children.
///
/// `None` where a chain does not bottom out at the operator — a cycle, which
/// resolution has already refused at the declaration that closed it.
fn principal_order(config: &ResolvedConfig) -> Option<Vec<usize>> {
    let slots: HashMap<String, usize> = config
        .principals
        .iter()
        .enumerate()
        .map(|(index, principal)| (principal.handle.dotted(), index))
        .collect();
    let mut order = Vec::new();
    let mut placed: HashSet<usize> = HashSet::new();
    while order.len() < config.principals.len() {
        let mut progressed = false;
        for (index, principal) in config.principals.iter().enumerate() {
            if placed.contains(&index) {
                continue;
            }
            let ready = match &principal.parent {
                // Asserted, not tolerated: resolution refuses an `under` that
                // named no declared principal, and treating a miss as a root
                // would order a chain this pass cannot see and check every
                // ceiling under it against the wrong authority.
                Some(parent) => placed.contains(slots.get(&parent.dotted()).unwrap_or_else(|| {
                    unreachable!("resolution refuses an `under` that names no principal")
                })),
                None => true,
            };
            if ready {
                order.push(index);
                placed.insert(index);
                progressed = true;
            }
        }
        if !progressed {
            return None;
        }
    }
    Some(order)
}

// ── stamp ceilings: what an arrangement confers against what consents to it ──
//
// A stamp of a packaged assembly imports config text an adversarial author
// wrote into the deployment's trust anchor. The ceiling is what the deployment
// says that text may come to: the stamp states what it accepts, and the
// compiler refuses the difference in both directions — nothing conferred beyond
// the ceiling, and nothing in the ceiling that confers nothing.

/// Every declared principal as a stamp reads it: what it holds, whether that is
/// what its text says, and the chain it is under.
struct Delegated {
    /// Each principal's authority, by dotted handle.
    authorities: HashMap<String, Authority>,
    /// The principals whose own body was refused, or that are under one whose
    /// was. Their authority is not what the document states, so nothing under
    /// one is judged against it: the refusal already reported is the answer, and
    /// one mistyped word in a principal that roots several stamps would
    /// otherwise fan out into a refusal per conferred word under every one of
    /// them, each suggesting a fix that changes nothing.
    unstated: HashSet<String>,
    /// Each principal's chain, root-first: the principals it is under.
    chains: HashMap<String, Vec<String>>,
}

impl Delegated {
    /// The principals one principal is under, root-first.
    ///
    /// Empty for a principal directly under the operator; every declared
    /// principal has an entry, which the walk that built the map inserted.
    fn chain(&self, label: &str) -> Vec<String> {
        self.chains
            .get(label)
            .cloned()
            .unwrap_or_else(|| unreachable!("every declared principal has a chain"))
    }

    /// What one principal holds.
    ///
    /// Asserted, not tolerated, for the reason the principal walk asserts it:
    /// resolution refuses an `under` that names no principal.
    fn authority(&self, label: &str) -> &Authority {
        self.authorities.get(label).unwrap_or_else(|| {
            unreachable!("resolution refuses an `under` that names no principal")
        })
    }
}

/// Every recorded stamp, its ceiling, and that what it stamps fits under it.
///
/// Index order, which is walk order, which is parents before children: a stamp
/// is recorded when its `new` is read and its arrangement is expanded after
/// that, so an enclosing stamp always holds the lower index.
fn check_stamps(
    config: &ResolvedConfig,
    confers: &HashMap<StampId, Confers>,
    defaults: &HashMap<StampId, Vec<(Family, DEntry)>>,
    delegated: &Delegated,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) {
    let root = Authority::default();
    let mut ceilings: Vec<Authority> = Vec::new();
    // What each stamp's body actually wrote, kept past the ceiling it produced:
    // the dead-config rules are about the text, not about the authority the text
    // and its inheritance came to.
    let mut bodies: Vec<Written> = Vec::new();
    // A stamp whose own ceiling text was refused has no stated ceiling to hold
    // its subtree against; the refusal already reported is the answer, and a
    // second one about its consequence is noise.
    let mut refused: Vec<bool> = Vec::new();
    // A stamp whose ceiling text was refused for what it says — a widening, or a
    // narrowing of nothing — is still held to the fit rule against the ceiling
    // it wrote, but nothing more is said about that text: the dead-config
    // message would be a second message about one line.
    let mut reported: Vec<bool> = Vec::new();
    for (index, stamp) in config.stamps.iter().enumerate() {
        let enclosing = stamp.parent.map(|StampId(parent)| {
            assert!(parent < index, "a stamp is recorded before its arrangement");
            parent
        });
        let handed = stamp
            .under
            .as_ref()
            .map(HandlePath::dotted)
            .map(|label| (delegated.authority(&label), label));
        // Whether what the ceiling is judged against is what the document
        // states. A handed principal whose own text was refused, or an enclosing
        // ceiling that was, is an authority nobody wrote; asking anything of
        // this stamp against it reports the same mistake again once per word
        // and entry its arrangement holds.
        let unstated = match (&handed, enclosing) {
            (Some((_, label)), enclosing) => {
                delegated.unstated.contains(label)
                    || enclosing.is_some_and(|parent| refused[parent])
            }
            (None, Some(parent)) => refused[parent],
            (None, None) => false,
        };
        // A handed principal is a bound of its own, and it does not widen the
        // ceiling it arrives inside: what the text says is what the ceiling is.
        if let (false, Some((handed, label)), Some(parent)) = (unstated, &handed, enclosing) {
            for excess in handed.attenuated_from(&ceilings[parent]) {
                let mut refusal = Diagnostic::at(
                    format!(
                        "`{label}` holds {}, which the ceiling on the stamp `{}` does not: a \
                         principal handed into a stamped arrangement is a bound of its own, \
                         and the ceiling it arrives inside is not widened by one",
                        excess.what(),
                        config.stamps[parent].handle.dotted()
                    ),
                    under_span(stamp),
                );
                refusal
                    .related
                    .push(("held here".to_string(), excess.span().clone()));
                // Where the principal reached this clause, when that is not the
                // clause: a `Principal` parameter is named at the enclosing
                // `new`, and the argument is the other place the arrangement
                // can be handed something narrower.
                let named = handle_span(stamp.under.as_ref().unwrap_or_else(|| {
                    unreachable!("a handed principal is a stamp that wrote `under`")
                }));
                if named != under_span(stamp) {
                    refusal.related.push(("handed in here".to_string(), named));
                }
                refusal.related.push((
                    "the enclosing ceiling is written here".to_string(),
                    config.stamps[parent].span.clone(),
                ));
                errors.push(refusal);
            }
        }
        let (inherited, above) = match (&handed, enclosing) {
            (Some((handed, label)), _) => (*handed, Some(format!("`{label}`"))),
            (None, Some(parent)) => (
                &ceilings[parent],
                Some(format!(
                    "the ceiling on the stamp `{}`",
                    config.stamps[parent].handle.dotted()
                )),
            ),
            // The operator: what the body writes is the whole ceiling, and an
            // axis it does not write is empty.
            (None, None) => (&root, None),
        };
        let written = written_authority(
            Body {
                grants: stamp.grants.as_ref(),
                acls: &stamp.acls,
            },
            refs,
            errors,
        );
        // `under p;` with no body is how a stamp says "exactly `p`", so there
        // is no narrowing to ask about; an empty body is text that narrows
        // nothing, which the narrowing rule refuses.
        let delegation = above
            .filter(|_| stamp.wrote_body && !unstated)
            .map(|above| Delegation {
                holder: format!("the stamp `{}`", stamp.handle.dotted()),
                above,
                noun: "a stamp's ceiling",
                rule: "a stamp's ceiling holds no more than what it is under",
                span: stamp.span.clone(),
            });
        refused.push(written.refused || unstated);
        let (ceiling, body_refused) = attenuate(delegation.as_ref(), inherited, &written, errors);
        ceilings.push(ceiling);
        reported.push(body_refused);
        bodies.push(written);
    }
    let nothing = Confers::default();
    for (index, stamp) in config.stamps.iter().enumerate() {
        if refused[index] {
            continue;
        }
        let default = defaults.get(&StampId(index)).cloned().unwrap_or_default();
        let mut effective = ceilings[index].clone();
        effective.reach.extend(default.iter().cloned());
        let held = confers.get(&StampId(index)).unwrap_or(&nothing);
        check_fit(config, stamp, held, &effective, delegated, errors);
        if !reported[index] {
            check_dead_ceiling(stamp, &bodies[index], held, &default, errors);
        }
    }
}

/// That a stamp's ceiling text caps something, so what a reader sees is true.
///
/// The inverse of the fit rule, and the half that makes a ceiling *consent*
/// rather than a bound: a word or a line that caps nothing is authority the
/// deployment stated and the arrangement never asked for, and it survives in
/// the deployer's file after the bundle that needed it stopped needing it —
/// so the next pin bump that re-introduces it needs no new consent.
///
/// Judged against this stamp's own subtree, because a stamp's body is one
/// deployment's statement about one arrangement. A `principal` is judged
/// against the union of everything under it instead, which is what makes it
/// reusable.
fn check_dead_ceiling(
    stamp: &RStamp,
    written: &Written,
    confers: &Confers,
    defaults: &[(Family, DEntry)],
    errors: &mut Vec<Diagnostic>,
) {
    // A stamp with no body of its own states nothing, so there is nothing here
    // that could be dead: `under p;` is how a stamp says "exactly `p`", and a
    // stamp with neither is the boundary recorded with nothing written.
    if !stamp.wrote_body {
        return;
    }
    let stamped = match &stamp.package {
        Some(package) => format!("`{}` from `@{package}`", stamp.assembly.value()),
        None => format!("`{}`", stamp.assembly.value()),
    };
    // Whose file the dead text is in, the same fact the fit rule reports. A
    // stamp written in packaged text is an author narrowing a nested
    // arrangement, so the line is in a module every deployment that stamps the
    // bundle imports and none of them can edit: without this the refusal reads
    // as consent the deployer left behind and sends them looking in their own
    // file for a line that is not there.
    let owner = match stamp.packaged_site {
        true => " — and that line is the author's: this stamp is the arrangement's own text",
        false => "",
    };
    // An arrangement that holds no capability is stamped with no `grants` line
    // at all; the empty list is text that caps nothing, and it reads as though
    // the deployment had something in mind.
    if written.wrote_words && written.axes.words.is_empty() && confers.words.is_empty() {
        errors.push(Diagnostic::at(
            format!(
                "no instance stamped by {stamped} holds a capability, so this `grants` line \
                 caps nothing; the stamp of an arrangement that holds no capability \
                 writes no `grants` line{owner}"
            ),
            stamp.span.clone(),
        ));
    }
    for (word, span) in &written.axes.words {
        if confers.words.contains_key(word) {
            continue;
        }
        errors.push(Diagnostic::at(
            format!(
                "`{word}` caps nothing — no instance stamped by {stamped} holds it; a ceiling \
                 word nothing reaches is dead config{owner}"
            ),
            span.clone(),
        ));
    }
    for (family, plane) in &written.families {
        // What the line has to cap: reach the arrangement asked for in this
        // family that the stamp's default reach does not already answer. A line
        // that only re-states the arrangement's own channels, or a channel the
        // deployer handed in, consents to something already consented to.
        //
        // Whether *this* line is what answers it is deliberately not asked. A
        // family the arrangement reaches and the line does not cover is the fit
        // rule's refusal, which names the line to write instead; saying in the
        // same breath that the line caps nothing would be a second message about
        // one mistake.
        if reaches_beyond(*family, confers, defaults) {
            continue;
        }
        errors.push(Diagnostic::at(
            format!(
                "this `acl {}` line caps nothing {stamped} reaches in `{}` beyond its own \
                 channels and what it was handed; a ceiling line nothing needs is dead \
                 config{owner}",
                plane.value(),
                family.name()
            ),
            plane.span().clone(),
        ));
    }
}

/// Where a stamp's `under` clause is written, which is where the choice of
/// principal was made.
///
/// The clause itself, not the handle it resolved to: a clause naming a
/// `Principal` parameter resolves to a handle whose position is the argument at
/// the enclosing `new`, in another file, and the clause is what a reader has to
/// change. A stamp with no `under` clause is cited at the `new` handle instead.
fn under_span(stamp: &RStamp) -> Span {
    stamp
        .under_span
        .clone()
        .unwrap_or_else(|| stamp.span.clone())
}

/// What one stamp's arrangement confers.
#[derive(Default)]
struct Confers {
    /// Each grant word, with every entity that holds it and where the word is
    /// written. One refusal per missing word rather than one per instance, with
    /// every holder as a related site.
    words: BTreeMap<String, Vec<(String, Span)>>,
    /// Each reach entry the arrangement itself wrote — its own `acl` statements,
    /// what its bindings derive, and what a `grant` inside it hands out — with
    /// the entity the entry is about.
    reach: Vec<(Family, DEntry, String)>,
}

/// What every recorded stamp confers, keyed by stamp.
///
/// An entity counts toward every stamp on its own stamp's ancestor chain, not
/// only the nearest: a nested stamp's own check bounds it by its own ceiling
/// alone, and the outer ceiling is a statement about the whole subtree under it.
///
/// A `grant` aimed *into* a subtree from outside it is deliberately absent. It
/// is deployer text, or reach another stamp confers and is counted there;
/// counting it here would make the deployer spell one consent twice and would
/// point the refusal at the author's file for a line the deployer wrote.
///
/// Confined reach is absent too, and for the reason a ceiling refuses a line in
/// a confined family: it is the serving host's to authorize, so demanding it of
/// a ceiling that cannot hold it would refuse every arrangement whose chrome
/// binds a `local:` address, with no line that answers the refusal.
fn confers(config: &ResolvedConfig, conferred: &Conferred) -> HashMap<StampId, Confers> {
    let mut confers: HashMap<StampId, Confers> = HashMap::new();
    for entity in &conferred.entities {
        for stamp in ancestry(config, entity.stamp) {
            let held = confers.entry(stamp).or_default();
            for word in &entity.words {
                held.words
                    .entry(word.value().clone())
                    .or_default()
                    .push((entity.label.clone(), word.span().clone()));
            }
            for (family, entry, origin) in &entity.entries {
                if matches!(origin, Origin::Grant(_)) || family.confined() {
                    continue;
                }
                held.reach
                    .push((*family, entry.clone(), entity.label.clone()));
            }
        }
    }
    for (grant, entry) in config.grants.iter().zip(&conferred.grants) {
        let Some((family, entry)) = entry.as_ref().filter(|(family, _)| !family.confined()) else {
            continue;
        };
        for stamp in ancestry(config, grant.stamp) {
            confers.entry(stamp).or_default().reach.push((
                *family,
                entry.clone(),
                grant.target.dotted(),
            ));
        }
    }
    confers
}

/// The reach every recorded stamp holds without writing a line for it.
///
/// Two kinds of consent already given: a channel the arrangement declares,
/// which stamping it is the consent to mint, and a channel handed in as an
/// argument, which naming it in the argument list is the consent to reach.
/// Both are exact entries on both planes of the address's own scheme.
fn default_reach(config: &ResolvedConfig) -> HashMap<StampId, Vec<(Family, DEntry)>> {
    let mut reach: HashMap<StampId, Vec<(Family, DEntry)>> = HashMap::new();
    let mut add = |stamp: Option<StampId>, channel: &RChannel| {
        for id in ancestry(config, stamp) {
            reach.entry(id).or_default().extend(exact_entries(channel));
        }
    };
    for channel in &config.channels {
        add(channel.stamp, channel);
    }
    for (index, stamp) in config.stamps.iter().enumerate() {
        for &ChanId(handed) in &stamp.handed {
            add(Some(StampId(index)), &config.channels[handed]);
        }
    }
    reach
}

/// One declared channel as the exact entries that reach it, on both planes.
///
/// A confined channel is absent: it reaches the one component that binds it,
/// authorized by the host that serves it, and no ceiling caps it.
fn exact_entries(channel: &RChannel) -> Vec<(Family, DEntry)> {
    let Some((scheme, bare)) = split_spellable(channel.address.value()) else {
        return Vec::new();
    };
    Plane::ALL
        .into_iter()
        .filter_map(|plane| Family::of(scheme, plane))
        .filter(|family| !family.confined())
        .map(|family| {
            (
                family,
                DEntry::Chan(DMatcher::Exact(Spanned::new(
                    bare.to_string(),
                    channel.address.span().clone(),
                ))),
            )
        })
        .collect()
}

/// Every recorded stamp an entity under `stamp` confers on: the stamp itself
/// and every one it was expanded inside.
fn ancestry(config: &ResolvedConfig, stamp: Option<StampId>) -> Vec<StampId> {
    let mut chain = Vec::new();
    let mut next = stamp;
    while let Some(id) = next {
        chain.push(id);
        next = config.stamps[id.0].parent;
    }
    chain
}

/// That what a stamp's arrangement confers fits under its effective ceiling.
///
/// One refusal per excess. A word is reported once with every instance holding
/// it as a related site, because the suggested line is the same for all of them
/// and fixing one fixes all; a reach entry is reported at the statement in the
/// author's file that produced it, which is the only place a reader can see
/// what the arrangement asked for.
fn check_fit(
    config: &ResolvedConfig,
    stamp: &RStamp,
    confers: &Confers,
    effective: &Authority,
    delegated: &Delegated,
    errors: &mut Vec<Diagnostic>,
) {
    let stamped = match &stamp.package {
        Some(package) => format!("`{}` from `@{package}`", stamp.assembly.value()),
        None => format!("`{}`", stamp.assembly.value()),
    };
    // Whose file a refusal here belongs to. A stamp written in packaged text is
    // an author narrowing a nested arrangement, so an arrangement that exceeds
    // it is the author's to fix and no line the deployer writes can answer it.
    let owner = match stamp.packaged_site {
        true => " — and that line is the author's: this stamp is the arrangement's own text",
        false => "",
    };
    let all: Vec<&str> = confers.words.keys().map(String::as_str).collect();
    for (word, holders) in &confers.words {
        if effective.words.contains_key(word) {
            continue;
        }
        let (first, _) = &holders[0];
        let mut refusal = Diagnostic::at(
            format!(
                "stamping {stamped} confers `{word}` on `{first}`, which this stamp's \
                 ceiling does not cover: a packaged arrangement holds what the deployment \
                 stamps it with, so {}{owner}",
                word_suggestion(word, &all, stamp, delegated)
            ),
            stamp.span.clone(),
        );
        for (holder, span) in holders {
            refusal
                .related
                .push((format!("`{holder}` holds it here"), span.clone()));
        }
        errors.push(refusal);
    }
    for (family, entry, holder) in &confers.reach {
        if effective
            .family(*family)
            .any(|held| entry.subsumed_by(held))
        {
            continue;
        }
        let line = entry_line(*family, entry);
        let mut refusal = Diagnostic::at(
            format!(
                "`{line}` on `{holder}` reaches beyond what {stamped} declares or was \
                 handed, and the stamp `{}` consents to none of it — {}{owner}",
                stamp.handle.dotted(),
                reach_suggestion(&line, *family, entry, stamp, config, delegated)
            ),
            entry.span().clone(),
        );
        refusal
            .related
            .push(("stamped here".to_string(), stamp.span.clone()));
        errors.push(refusal);
    }
}

/// One reach entry written back out as the `acl` line that would hold it.
///
/// The refusal writes the deployer's line for them, so the address carries its
/// scheme and the matcher its kind — which is exactly how the entry was
/// resolved, read backwards.
fn entry_line(family: Family, entry: &DEntry) -> String {
    let prefix = family.scheme().prefix();
    let (kind, address) = match entry {
        DEntry::Chan(DMatcher::Exact(pattern)) => {
            (MatcherKind::Exact, format!("{prefix}{}", pattern.value()))
        }
        DEntry::Chan(DMatcher::Prefix(pattern)) => {
            (MatcherKind::Prefix, format!("{prefix}{}", pattern.value()))
        }
        // A remote's entry reaches no refusal here: a remote is never stamped,
        // so nothing a stamp confers is one.
        DEntry::Ceiling(_) => {
            unreachable!("a remote's entry is in nothing a stamp confers")
        }
        DEntry::MqttSub(entry) => (
            MatcherKind::TopicFilter,
            format!(
                "{prefix}{}:{}",
                entry.client.value(),
                entry.topic_filter.value()
            ),
        ),
        DEntry::MqttPub(entry) => (
            MatcherKind::Client,
            format!("{prefix}{}", entry.client.value()),
        ),
        DEntry::Webhook(entry) => (
            MatcherKind::Endpoint,
            format!("{prefix}{}", entry.endpoint.value()),
        ),
    };
    format!(
        "acl {} [{} \"{address}\"]",
        family.plane().word(),
        kind.as_str()
    )
}

/// Where a missing word can be written, which is wherever the ceiling that
/// excludes it was written.
///
/// A stamp under a principal narrows what that principal holds, so the word is
/// the principal's to gain only when the principal lacks it too; when the
/// principal holds it, the ceiling that dropped it is the stamp's own body.
/// A word enters a chain at its root and a child can only narrow, so a word
/// missing from the principal a stamp is under is missing from every principal
/// that one is under too.
fn word_suggestion(word: &str, all: &[&str], stamp: &RStamp, delegated: &Delegated) -> String {
    match (stamp.under.as_ref().map(HandlePath::dotted), stamp.parent) {
        (Some(under), _) if delegated.authority(&under).words.contains_key(word) => format!(
            "add `{word}` to this stamp's ceiling — `{under}` holds it, and this stamp's \
             body hands down less"
        ),
        (Some(under), _) => {
            let missing = missing_in_chain(word, &under, delegated);
            match missing.is_empty() {
                true => format!(
                    "add `{word}` to `{under}`, or stamp under a principal that \
                                 holds it"
                ),
                false => format!(
                    "add `{word}` to `{under}`, and to the principals it is under: {}",
                    quoted_list(&missing)
                ),
            }
        }
        (None, Some(_)) => format!(
            "add `{word}` to this ceiling, and to the enclosing stamp's if that one does \
             not hold it either"
        ),
        (None, None) => format!("write it — `grants = [{}];`", all.join(", ")),
    }
}

/// Where a reach entry can be written. The same cases as a word's, and the
/// same reason.
fn reach_suggestion(
    line: &str,
    family: Family,
    entry: &DEntry,
    stamp: &RStamp,
    config: &ResolvedConfig,
    delegated: &Delegated,
) -> String {
    match (stamp.under.as_ref().map(HandlePath::dotted), stamp.parent) {
        (Some(under), _)
            if delegated
                .authority(&under)
                .family(family)
                .any(|held| entry.subsumed_by(held)) =>
        {
            format!(
                "add `{line};` to this stamp's ceiling if that reach is wanted — `{under}` \
                 holds it, and this stamp's body hands down less"
            )
        }
        (Some(under), _) => {
            let chain = delegated.chain(&under);
            match chain.is_empty() {
                true => format!("add `{line};` to `{under}` if that reach is wanted"),
                false => format!(
                    "add `{line};` to `{under}`, and to the principals it is under ({}), if \
                     that reach is wanted",
                    quoted_list(&chain)
                ),
            }
        }
        (None, Some(StampId(parent))) => format!(
            "add `{line};` to this ceiling, and to the ceiling on the stamp `{}` if that one \
             does not hold that reach either",
            config.stamps[parent].handle.dotted()
        ),
        (None, None) => format!("add `{line};` to the stamp if that reach is wanted"),
    }
}

/// Every principal in this one's chain that does not hold the word either,
/// root-first.
fn missing_in_chain(word: &str, under: &str, delegated: &Delegated) -> Vec<String> {
    delegated
        .chain(under)
        .into_iter()
        .filter(|label| !delegated.authority(label).words.contains_key(word))
        .collect()
}

/// ``​`a`, `b`​`` — a list of names as a suggestion spells them.
fn quoted_list(names: &[String]) -> String {
    names
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

// ── grants: the rights an entity states, and that they match its lists ───────
//
// A `grants` list names rights; the ACL lists say what each right reaches. The
// two are written separately and mean nothing apart: a right over an empty list
// authorizes nothing, and a list no right admits is never consulted. Both are
// refused, so a document says once what it means and the runtime is never handed
// configuration nothing reads.

/// The words one entity type's `grants` list may hold.
///
/// Every row is derived, never authored: the component words from
/// `ComponentGrant`, the attach words from `AttachGrant`, the agent words from
/// the LLM-authorable subset of `AppCapability`.
struct Vocabulary {
    /// Whether a plane word is a right here at all.
    ///
    /// False for a wasm consumer: its list names capability interfaces, and every
    /// transport right it has the runtime reads off its ACLs.
    planes: bool,
    /// The rights that name no plane.
    capabilities: &'static [Capability],
    /// The raw scheme-compound tokens, each with the plane word that replaces it.
    ///
    /// Refused by name rather than as unknown words: they are what the config
    /// this lowers to spells, so an operator who knows that side writes them, and
    /// the pointed message is the whole reason to keep the list.
    compound: &'static [(&'static str, &'static str)],
}

/// The words this entity type states rights with.
fn vocabulary(kind: EntityKind) -> Vocabulary {
    match kind {
        // A surface and a remote hold one vocabulary between them: both are
        // attach-route entities, and the rights a wire carries do not depend
        // on which kind of client is at the far end. Nothing page-shaped is
        // here — `takeover` is a capability a component holds within the page,
        // stated on the component.
        EntityKind::Surface | EntityKind::Remote => Vocabulary {
            planes: true,
            capabilities: &[Capability::Grant(ComponentGrant::Alert)],
            compound: &ATTACH_COMPOUND,
        },
        EntityKind::Agent => Vocabulary {
            planes: true,
            capabilities: &AGENT_CAPABILITIES,
            compound: &AGENT_COMPOUND,
        },
        // The whole shared vocabulary at either placement; what the host cannot
        // implement is refused by name rather than left out of the list.
        EntityKind::Component(_) => Vocabulary {
            planes: false,
            capabilities: COMPONENT_CAPABILITIES,
            compound: &[],
        },
    }
}

impl Vocabulary {
    /// Every word this list may hold, as a diagnostic lists them.
    fn list(&self) -> String {
        let planes = match self.planes {
            true => &Plane::ALL[..],
            false => &[],
        };
        or_list(
            planes
                .iter()
                .map(|plane| plane.word())
                .chain(self.capabilities.iter().map(|right| right.word())),
        )
    }
}

/// A right that names no plane: an interface, a device-facing capability, or a
/// tool the runtime gates on its own.
///
/// A variant rather than a word, so a rule keyed on one of these cannot go on
/// compiling once the word is renamed or misspelled.
///
/// [`Capability::Grant`] carries the shared component vocabulary rather than
/// restating it: the word list, its spellings, and what a word parses to all come
/// from the one enum the host and the surface kernel also read. `alert` is one
/// word across every entity type that states it — a component's and an
/// attacher's — so it is one variant here too; a right no component vocabulary
/// holds gets a variant of its own. The two that do are the agent-only
/// capabilities, and they take their spelling from `AppCapability` as well.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Capability {
    /// A right in the shared component grant vocabulary.
    Grant(ComponentGrant),
    /// The runtime's dynamic-subscribe tool. Agent-only; no component states it.
    DynamicSubscribe,
    /// PWA push egress. Agent-only; no component states it.
    PwaPush,
}

impl Capability {
    /// Every capability any entity type states: the whole shared component
    /// vocabulary, then the agent-only rights. Built from `ComponentGrant::ALL`
    /// so a word added there is parseable here without an edit.
    const ALL: [Capability; ComponentGrant::ALL.len() + 2] = {
        let mut all = [Capability::DynamicSubscribe; ComponentGrant::ALL.len() + 2];
        let mut next = 0;
        while next < ComponentGrant::ALL.len() {
            all[next] = Capability::Grant(ComponentGrant::ALL[next]);
            next += 1;
        }
        all[next] = Capability::DynamicSubscribe;
        all[next + 1] = Capability::PwaPush;
        all
    };

    /// The word this right is written as.
    fn word(self) -> &'static str {
        match self {
            Self::Grant(grant) => grant.word(),
            Self::DynamicSubscribe => AppCapability::DynamicSubscribe.word(),
            Self::PwaPush => AppCapability::PwaPush.word(),
        }
    }

    /// The right a word spells, or `None` when it spells none.
    fn parse(word: &str) -> Option<Capability> {
        Self::ALL.into_iter().find(|right| right.word() == word)
    }
}

/// The rights an agent may state that name a plane, as the compound tokens the
/// config this lowers to spells them, each with the plane word that replaces it.
///
/// Derived: an agent's transport rights are exactly the LLM-authorable
/// capabilities that have a transport shape. Order is lookup order only —
/// nothing reads this list front to back.
static AGENT_COMPOUND: LazyLock<Vec<(&'static str, &'static str)>> = LazyLock::new(|| {
    AppCapability::ALL
        .into_iter()
        .filter(|cap| cap.llm_authorable().is_ok())
        .filter_map(|cap| cap.transport().map(|(plane, _)| (cap.word(), plane.word())))
        .collect()
});

/// The same, for an attacher: the transport rights whose word is not already
/// the plane word.
static ATTACH_COMPOUND: LazyLock<Vec<(&'static str, &'static str)>> = LazyLock::new(|| {
    AttachGrant::ALL
        .into_iter()
        .filter_map(|grant| grant.transport().map(|(plane, _)| (grant, plane)))
        .filter(|(grant, plane)| grant.word() != plane.word())
        .map(|(grant, plane)| (grant.word(), plane.word()))
        .collect()
});

/// The rights an agent states that name no plane: the LLM-authorable
/// capabilities with no transport shape.
static AGENT_CAPABILITIES: LazyLock<Vec<Capability>> = LazyLock::new(|| {
    AppCapability::ALL
        .into_iter()
        .filter(|cap| cap.transport().is_none() && cap.llm_authorable().is_ok())
        .map(|cap| match Capability::parse(cap.word()) {
            Some(right) => right,
            None => panic!(
                "`{}` is authorable on an agent and names no plane, so it needs a \
                 `Capability` variant to be stated as",
                cap.word()
            ),
        })
        .collect()
});

/// What a plane word expands to: one raw token per scheme the plane reaches,
/// in the order they are written out.
///
/// A plane word is one right in the DSL and one token per scheme in the config
/// it lowers to, so the expansion is the whole translation. Only the schemes the
/// entity has entries on are written: a token over an empty list is dead
/// configuration, which is what agreement refuses.
///
/// Derived from the same two vocabularies the grant words come from, so a token
/// cannot be spelled one way here and another where it is granted. The rows are
/// generated plane-major — every subscribe row, then every publish row — and
/// within a plane in variant declaration order.
fn expansions(kind: EntityKind) -> &'static [(Plane, ChannelScheme, &'static str)] {
    match kind {
        EntityKind::Surface | EntityKind::Remote => &ATTACH_EXPANSIONS,
        EntityKind::Agent => &AGENT_EXPANSIONS,
        // A component's words are capabilities, which name no scheme and expand
        // to nothing: they cross into the config as written.
        EntityKind::Component(_) => &[],
    }
}

/// The agent expansion rows: every LLM-authorable transport capability.
static AGENT_EXPANSIONS: LazyLock<Vec<(Plane, ChannelScheme, &'static str)>> =
    LazyLock::new(|| {
        plane_major(AppCapability::ALL.into_iter().filter_map(|cap| {
            cap.llm_authorable().ok()?;
            let (plane, scheme) = cap.transport()?;
            Some((plane, scheme, cap.word()))
        }))
    });

/// The attacher expansion rows: every attach right with a transport shape.
static ATTACH_EXPANSIONS: LazyLock<Vec<(Plane, ChannelScheme, &'static str)>> =
    LazyLock::new(|| {
        plane_major(AttachGrant::ALL.into_iter().filter_map(|grant| {
            let (plane, scheme) = grant.transport()?;
            Some((plane, scheme, grant.word()))
        }))
    });

/// Rows sorted plane-major, each plane's rows keeping the order they arrived in.
///
/// The order is behavior: it is the order tokens are written into the lowered
/// config, and the expansion tests pin it.
fn plane_major(
    rows: impl Iterator<Item = (Plane, ChannelScheme, &'static str)>,
) -> Vec<(Plane, ChannelScheme, &'static str)> {
    let rows: Vec<_> = rows.collect();
    Plane::ALL
        .into_iter()
        .flat_map(|plane| {
            rows.iter()
                .copied()
                .filter(move |(row_plane, _, _)| *row_plane == plane)
        })
        .collect()
}

/// One word, once it is known what kind of word it is.
enum Right<'a> {
    Plane(Plane, &'a Spanned<String>),
    Capability(Capability, &'a Spanned<String>),
}

/// What one entity's `grants` list comes to, in the two spellings that read it.
struct DerivedGrants {
    /// The runtime's spelling: a plane word expanded into one token per scheme
    /// it has entries on, then the capability words as written. What lowering
    /// carries.
    tokens: Vec<Spanned<String>>,
    /// The ceiling's spelling: one word per right the entity holds, as the
    /// document wrote it, plane words unexpanded. What a stamp's ceiling caps —
    /// a ceiling names grant words as spelled, and the scheme-compound tokens
    /// are the lowered config's spelling rather than any vocabulary's.
    ///
    /// Built from the classified rights, so a word the pass refused is absent:
    /// a plane word a component may not state never became a right, and a
    /// ceiling is never asked to cap one.
    words: Vec<Spanned<String>>,
}

/// One entity's `grants`, classified, checked against its lists, and expanded.
fn derive_grants(
    subject: &Subject<'_>,
    stated: &Stated,
    errors: &mut Vec<Diagnostic>,
) -> DerivedGrants {
    let kind = subject.kind;
    let label = &subject.label;
    // A word this pass refuses means the classified list is not what the document
    // states, so agreement is not asked about it either — the refusal stands on
    // its own and is not followed by a second one about its consequence.
    let mut refused = false;
    let words = subject.words(&mut refused, errors);
    let vocabulary = vocabulary(kind);
    let mut rights = Vec::new();
    for word in words {
        let name = &word.name;
        let refusal = match Plane::parse(name.value()) {
            Some(plane) if vocabulary.planes => {
                rights.push(Right::Plane(plane, name));
                None
            }
            Some(_) => Some(format!(
                "{} `{label}` states no `{}` right: a component's grants name the \
                 capability interfaces it is given, and its transport rights are read off \
                 its bindings and acl statements",
                kind.label(),
                name.value()
            )),
            None => match Capability::parse(name.value())
                .filter(|right| vocabulary.capabilities.contains(right))
            {
                Some(right) => match host_refusal(kind, right) {
                    Some(message) => Some(message.to_string()),
                    None => {
                        rights.push(Right::Capability(right, name));
                        None
                    }
                },
                // A word that spells a capability another entity type has is not
                // one here, so it goes on to the same refusals any other word does.
                None => match vocabulary
                    .compound
                    .iter()
                    .find(|(token, _)| token == name.value())
                {
                    Some((token, plane)) => Some(format!(
                        "`{token}` is how the config this lowers to spells one scheme of \
                         one plane; {} grants name the plane and take their schemes from \
                         the entries — write `{plane}`",
                        kind.label()
                    )),
                    None => Some(format!(
                        "`{}` is not a right {} grants hold; they name {}",
                        name.value(),
                        kind.label(),
                        vocabulary.list()
                    )),
                },
            },
        };
        if let Some(message) = refusal {
            refused = true;
            errors.push(Diagnostic::at(message, name.span().clone()));
        }
    }
    check_unique(
        words
            .iter()
            .map(|word| (word.name.value().as_str(), (), word.name.span())),
        |word, (), span, (), prior| {
            two_site(
                format!("`{word}` is granted twice; one statement of a right is what holds it"),
                span.clone(),
                "it is granted here".to_string(),
                prior.clone(),
            )
        },
        errors,
    );
    let held = |plane: Plane| {
        rights
            .iter()
            .any(|right| matches!(right, Right::Plane(stated, _) if *stated == plane))
    };
    // Not an agreement rule: `dynamic_subscribe` gates the runtime's subscribe
    // tool, which decides with the plane's right as well, so the word without the
    // plane is refused whatever the lists hold.
    if let Some(word) = rights.iter().find_map(|right| match right {
        Right::Capability(Capability::DynamicSubscribe, word) => Some(word),
        _ => None,
    }) && !held(Plane::Subscribe)
    {
        errors.push(Diagnostic::at(
            format!(
                "agent `{label}` grants `dynamic_subscribe` and not `subscribe`: the subscribe \
                 tool decides with the transport right as well, so on its own it reaches \
                 nothing"
            ),
            word.span().clone(),
        ));
    }
    // Not an agreement rule either, and the placement-grain half of the pair the
    // class-grain rule refuses in a specification's lists: page-DOM authority
    // arranges other instances' elements and mutates them through the scoped
    // word, and only the scoped word makes an instance mountable. A class may
    // list both optional, so the lists agreeing does not make every grant set
    // drawn from them coherent — this is where the set itself is checked.
    if let Some(word) = rights.iter().find_map(|right| match right {
        Right::Capability(Capability::Grant(ComponentGrant::PageDom), word) => Some(word),
        _ => None,
    }) && !rights.iter().any(|right| {
        matches!(
            right,
            Right::Capability(Capability::Grant(ComponentGrant::Dom), _)
        )
    }) {
        errors.push(Diagnostic::at(
            format!(
                "{} `{label}` grants `{}` and not `{}`: the page-wide capability arranges \
                 other instances' elements and mutates them through the scoped one, and only \
                 the scoped one makes an instance mountable, so the pair is granted together \
                 or not at all",
                kind.label(),
                ComponentGrant::PageDom.word(),
                ComponentGrant::Dom.word(),
            ),
            word.span().clone(),
        ));
    }
    if !stated.refused && !refused {
        check_agreement(kind, label, &rights, stated, errors);
    }
    // Not behind `refused`: a word the host cannot implement is still a word
    // the spec does not see granted, and both refusals are true of the same
    // contradiction. A garbage word never became a right, so it cannot reach
    // the second direction at all. What the flag would guard against is the
    // list nobody wrote, which `words` has already refused whole.
    if let Some(spec) = subject.spec
        && subject.words.is_some()
    {
        check_spec_fit(subject, spec, &rights, errors);
    }
    if matches!(kind, EntityKind::Component(_)) && subject.words.is_some() {
        check_tool_coupling(subject, &rights, errors);
    }
    let words = rights
        .iter()
        .map(|right| match right {
            Right::Plane(plane, word) => {
                Spanned::new(plane.word().to_string(), word.span().clone())
            }
            Right::Capability(right, word) => {
                Spanned::new(right.word().to_string(), word.span().clone())
            }
        })
        .collect();
    DerivedGrants {
        tokens: expand(kind, &rights, stated),
        words,
    }
}

/// A component's `tools` word against the `tool` statements beside it, both
/// directions.
///
/// The same split-kill rule the rest of this pass applies to a right and the
/// list it reaches: the word is the deployer's consent to the registry, the
/// statements are what that consent covers, and neither means anything alone. A
/// word with no statement authorizes no invocation of anything; a statement
/// with no word is authority nobody granted.
///
/// A component only. An agent's tool authority is the statements themselves —
/// there is no agent-side word to couple them to.
fn check_tool_coupling(subject: &Subject<'_>, rights: &[Right<'_>], errors: &mut Vec<Diagnostic>) {
    let what = subject.kind.label();
    let label = &subject.label;
    let word = rights.iter().find_map(|right| match right {
        Right::Capability(Capability::Grant(ComponentGrant::Tools), word) => Some(word),
        _ => None,
    });
    match (word, subject.tools.first()) {
        (Some(word), None) => errors.push(Diagnostic::at(
            format!(
                "{what} `{label}` grants `tools` and states no `tool`: a grant that names \
                 no tool reaches nothing"
            ),
            word.span().clone(),
        )),
        (None, Some(first)) => errors.push(two_site(
            format!(
                "{what} `{label}` states a `tool` grant and does not grant `tools`: the \
                 word is what consents to reaching the registry at all"
            ),
            first.tool.span().clone(),
            "granted here",
            subject.span.clone(),
        )),
        _ => {}
    }
}

/// One instance's grants against the needs its class declares, both directions.
///
/// The class is the author's statement of what the component cannot run without
/// and what it can use; the instance's list is the deployer's act of consent.
/// Neither is derived from the other, so the check is what makes a spec worth
/// writing: a deployment that under-grants is refused before it boots, and one
/// that over-grants is refused because a capability the spec never asked for is
/// authority nothing reads.
fn check_spec_fit(
    subject: &Subject<'_>,
    spec: &ClassRef,
    rights: &[Right<'_>],
    errors: &mut Vec<Diagnostic>,
) {
    let what = subject.kind.label();
    let label = &subject.label;
    let class = spec.name.value();
    let held = |needed: ComponentGrant| {
        rights.iter().any(
            |right| matches!(right, Right::Capability(Capability::Grant(grant), _) if *grant == needed),
        )
    };
    for word in &spec.requires {
        if held(*word.value()) {
            continue;
        }
        errors.push(two_site(
            format!(
                "{what} `{label}` does not grant `{}`, which `{class}` requires: a \
                 component runs with what it was given, and this one was not \
                 given what it needs",
                word.value().word()
            ),
            subject.span.clone(),
            "required here",
            word.span().clone(),
        ));
    }
    for right in rights {
        let Right::Capability(Capability::Grant(grant), word) = right else {
            continue;
        };
        if spec
            .requires
            .iter()
            .chain(&spec.optional)
            .any(|declared| declared.value() == grant)
        {
            continue;
        }
        errors.push(two_site(
            format!(
                "{what} `{label}` grants `{}`, which `{class}` neither requires nor \
                 lists optional: the spec is the vocabulary",
                word.value()
            ),
            word.span().clone(),
            "declared here",
            spec.name.span().clone(),
        ));
    }
}

/// That every right reaches a list and every list is reached by a right.
fn check_agreement(
    kind: EntityKind,
    label: &str,
    rights: &[Right<'_>],
    stated: &Stated,
    errors: &mut Vec<Diagnostic>,
) {
    let table = expansions(kind);
    let reached = |plane: Plane| {
        table
            .iter()
            .filter(|(stated_plane, _, _)| *stated_plane == plane)
            .filter_map(|&(plane, scheme, _)| Family::of(scheme, plane))
            .any(|family| stated.first(family).is_some())
    };
    for right in rights {
        let Right::Plane(plane, word) = right else {
            continue;
        };
        if !reached(*plane) {
            errors.push(Diagnostic::at(
                format!(
                    "{} `{label}` grants `{}` and states no {} entry on any scheme, so the \
                     right reaches nothing: an acl statement or a bound port is what gives \
                     it something to authorize",
                    kind.label(),
                    word.value(),
                    plane.word(),
                ),
                word.span().clone(),
            ));
        }
    }
    for &(plane, scheme, _) in table {
        let Some(family) = Family::of(scheme, plane) else {
            unreachable!("an expansion names a family on the plane it is written for");
        };
        let Some(entry) = stated.first(family) else {
            continue;
        };
        if rights
            .iter()
            .any(|right| matches!(right, Right::Plane(held, _) if *held == plane))
        {
            continue;
        }
        errors.push(Diagnostic::at(
            format!(
                "{} `{label}` holds a `{}` entry and grants no `{}`, so nothing consults it: \
                 the plane's right is what admits the transport",
                kind.label(),
                family.name(),
                plane.word(),
            ),
            entry.span().clone(),
        ));
    }
    if let EntityKind::Component(host) = kind {
        check_component_agreement(host, kind.label(), label, rights, stated, errors);
    }
}

/// The rights a component's lists demand, and the lists its rights demand.
///
/// Its transport rights are read off its bindings and ACLs, so only two words
/// pair with a list at all: `ports`, which is the right to send anywhere, and
/// `mqtt`, which is the right to reach a broker.
///
/// Both directions are asked wherever the instance runs. A free `io` port and a
/// link-bound binding with a publishing role both count as a send at either
/// placement — neither has a channel for an entry to be about, but the ring
/// each mints is somewhere the component publishes, and boot must count the
/// same ports when it asks the forward direction.
fn check_component_agreement(
    host: ComponentHost,
    what: &str,
    label: &str,
    rights: &[Right<'_>],
    stated: &Stated,
    errors: &mut Vec<Diagnostic>,
) {
    let word = |granted: Capability| {
        rights.iter().find_map(|right| match right {
            Right::Capability(held, word) if *held == granted => Some(*word),
            _ => None,
        })
    };
    let sends = [
        Family::BrennPublish,
        Family::EphemeralPublish,
        Family::LocalPublish,
    ]
    .into_iter()
    .find_map(|family| stated.first(family))
    .map(|entry| entry.span().clone())
    .or_else(|| stated.output.clone());
    match (word(Capability::Grant(ComponentGrant::Ports)), sends) {
        (None, Some(span)) => errors.push(Diagnostic::at(
            format!(
                "{what} `{label}` sends and grants no `ports`: the messaging interface it \
                 publishes through is what `ports` gives it"
            ),
            span,
        )),
        (Some(word), None) => errors.push(Diagnostic::at(
            format!(
                "{what} `{label}` grants `ports` and neither binds an output nor states a \
                 publish entry, so the interface reaches nothing"
            ),
            word.span().clone(),
        )),
        _ => {}
    }
    // `mqtt` is refused outright on a surface, so the pair below is a top-level
    // question only; the legality table is what says so.
    if host == ComponentHost::Surface {
        return;
    }
    match (
        word(Capability::Grant(ComponentGrant::Mqtt)),
        stated.first(Family::MqttPublish),
    ) {
        (None, Some(entry)) => errors.push(Diagnostic::at(
            format!(
                "consumer `{label}` holds an `mqtt_publish` entry and grants no `mqtt`: the \
                 broker interface is what `mqtt` gives it"
            ),
            entry.span().clone(),
        )),
        (Some(word), None) => errors.push(Diagnostic::at(
            format!(
                "consumer `{label}` grants `mqtt` and states no `mqtt_publish` entry, so the \
                 broker interface reaches nothing"
            ),
            word.span().clone(),
        )),
        _ => {}
    }
}

/// The tokens a classified list comes to.
///
/// Expansion order is the table's, then the capability words as written, so the
/// list is a function of what was granted and not of how it was ordered.
fn expand(kind: EntityKind, rights: &[Right<'_>], stated: &Stated) -> Vec<Spanned<String>> {
    let mut tokens = Vec::new();
    for &(plane, scheme, token) in expansions(kind) {
        let Some(family) = Family::of(scheme, plane) else {
            unreachable!("an expansion names a family on the plane it is written for");
        };
        if stated.first(family).is_none() {
            continue;
        }
        for right in rights {
            if let Right::Plane(held, word) = right
                && *held == plane
            {
                tokens.push(Spanned::new(token.to_string(), word.span().clone()));
            }
        }
    }
    for right in rights {
        if let Right::Capability(_, word) = right {
            tokens.push(Spanned::new(word.value().clone(), word.span().clone()));
        }
    }
    tokens
}

// ── pass 5: the wire-kind fold ───────────────────────────────────────────────

/// The wire kind a component class name is served under: `ModeClock` →
/// `mode-clock`.
///
/// A `-` before every uppercase letter but the first, then lowercase throughout.
/// Over the class-name charset the result always satisfies the runtime's kind
/// rule — segments start with a letter, so there is no leading digit and no
/// doubled hyphen.
pub fn wire_kind(class_name: &str) -> String {
    let mut kind = String::with_capacity(class_name.len() + 4);
    for (index, character) in class_name.chars().enumerate() {
        if index > 0 && character.is_ascii_uppercase() {
            kind.push('-');
        }
        kind.extend(character.to_lowercase());
    }
    kind
}

/// The kind each surface-placed instance is served under.
///
/// Only surface-placed instances: a top-level instance is loaded from its
/// artifact and has no wire kind at all, so it takes no fold and joins no
/// collision check.
fn fold_component_kinds(config: &ResolvedConfig, errors: &mut Vec<Diagnostic>) -> Vec<Vec<String>> {
    // kind to the first class seen under it. Two classes folding to one kind are
    // two different things served under one name — unless they state the same
    // facts, in which case the browser cannot tell them apart either.
    let mut claimed: HashMap<String, (&ClassRef, &RSurface)> = HashMap::new();
    let mut kinds = Vec::with_capacity(config.surfaces.len());
    for surface in &config.surfaces {
        let mut surface_kinds = Vec::with_capacity(surface.components.len());
        for instance in &surface.components {
            let kind = wire_kind(instance.class.name.value());
            match claimed.get(kind.as_str()) {
                Some((first, first_surface)) if !same_class_facts(first, &instance.class) => {
                    errors.push(two_site(
                        format!(
                            "two component classes are served as `{kind}`: `{}` on surface \
                             `{}` states different facts than the one already claiming it",
                            instance.class.name.value(),
                            surface.handle.dotted()
                        ),
                        instance.class.name.span().clone(),
                        format!("`{}` claims it here", first_surface.handle.dotted()),
                        first.name.span().clone(),
                    ));
                }
                Some(_) => {}
                None => {
                    claimed.insert(kind.clone(), (&instance.class, surface));
                }
            }
            surface_kinds.push(kind);
        }
        kinds.push(surface_kinds);
    }
    kinds
}

/// Do two class references state the same wire facts?
///
/// Compared by value, span excluded: the same class written in two modules is two
/// positions in the source and one contract on the wire. `spec_sha256` is
/// excluded for the same reason: two copies differing only in comments state
/// the same contract, and whether their bytes bind to an artifact is a
/// deployment question this fold does not ask. `package` is excluded on the
/// same grounds — which packaged module declared a class is where it was
/// found, not what it states.
///
/// Both sides are destructured so a field added to [`ClassRef`] does not join
/// the comparison, or stay out of it, by nobody's decision: it is a compile
/// error here until someone makes one.
fn same_class_facts(left: &ClassRef, right: &ClassRef) -> bool {
    let ClassRef {
        name: left_name,
        abi: left_abi,
        requires: left_requires,
        optional: left_optional,
        ports: left_ports,
        spec_sha256: _,
        package: _,
    } = left;
    let ClassRef {
        name: right_name,
        abi: right_abi,
        requires: right_requires,
        optional: right_optional,
        ports: right_ports,
        spec_sha256: _,
        package: _,
    } = right;
    left_name.value() == right_name.value()
        && left_abi.value() == right_abi.value()
        && same_words(left_requires, right_requires)
        && same_words(left_optional, right_optional)
        && left_ports.len() == right_ports.len()
        && left_ports
            .iter()
            .zip(right_ports)
            .all(|(left, right)| same_port(left, right))
}

/// One grant list's facts, span excluded and order included: a list is
/// written, and two spellings of one class write it the same way.
fn same_words(left: &[Spanned<ComponentGrant>], right: &[Spanned<ComponentGrant>]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.value() == right.value())
}

/// One port's facts, span excluded.
fn same_port(left: &RPort, right: &RPort) -> bool {
    left.name.value() == right.name.value()
        && left.dir == right.dir
        && left.optional == right.optional
        && left.doctype.as_ref().map(Spanned::value) == right.doctype.as_ref().map(Spanned::value)
}

// ── pass 6: document types ───────────────────────────────────────────────────

/// One doctype tag, as one port declares it.
///
/// The site is the port declaration's, not the binding's: the tag is a fact of
/// the class contract, so a disagreement is between two authors and the
/// diagnostic points at what each of them wrote.
struct DoctypeClaim<'a> {
    tag: &'a str,
    site: &'a Span,
    class: &'a str,
    port: &'a str,
}

/// The ring a claim rides on.
///
/// A `local:` namespace is private to one ring: the server ring and each
/// surface's page ring cannot exchange a message, so one `local:` name appearing
/// in two of them is two channels spelled alike and nothing about the first
/// constrains the second. Every other scheme is transportable, so one address is
/// one channel wherever it is bound.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Realm {
    /// A transportable address: one channel, whoever binds it.
    Wire,
    /// The server ring's private namespace, which top-level components share.
    Backend,
    /// One surface's page ring, by that surface's index in the document.
    Page(usize),
}

/// What makes one group of claims one channel.
///
/// A link is grouped by its identity, not by an address: it has none until boot
/// places it, and every port bound to it will read and write the one ring that
/// placement mints.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DoctypeKey<'a> {
    Address(Realm, &'a str),
    Link(LinkId),
}

/// A `BTreeMap` so disagreements on two channels report in a stable order.
type DoctypeClaims<'a> = BTreeMap<DoctypeKey<'a>, Vec<DoctypeClaim<'a>>>;

/// Every doctype reaching one channel names the same document.
///
/// Claims are keyed by realm and resolved address, which is what makes one key
/// one channel: transportable addresses are disjoint by spelling, and a `local:`
/// name is disjoint per ring.
///
/// A port with no doctype binds to anything, and a channel with no doctyped port
/// bound to it is inert. Nothing here forces a declaration; what it refuses is
/// two declarations that disagree.
fn check_doctypes(config: &ResolvedConfig, refs: &Refs<'_>, errors: &mut Vec<Diagnostic>) {
    let mut claims: DoctypeClaims<'_> = BTreeMap::new();
    for (index, surface) in config.surfaces.iter().enumerate() {
        for instance in &surface.components {
            collect_doctypes(
                &instance.class,
                &instance.bindings,
                refs,
                Realm::Page(index),
                &mut claims,
            );
        }
    }
    for consumer in &config.consumers {
        collect_doctypes(
            &consumer.class,
            &consumer.bindings,
            refs,
            Realm::Backend,
            &mut claims,
        );
    }
    // The handle a declared channel is known by, so a diagnostic names the
    // channel the way the document does. Looked up where a conflict is being
    // reported rather than tabulated up front: the common load has no conflict
    // and needs no table. A literal address has no handle and is named by the
    // address itself.
    let handle_of = |address: &str| {
        config
            .channels
            .iter()
            .find(|channel| channel.address.value() == address)
            .map(|channel| &channel.handle)
    };
    for (key, claims) in &claims {
        if claims.len() < 2 {
            continue;
        }
        let label = match key {
            DoctypeKey::Address(_, address) => match handle_of(address) {
                Some(handle) => format!("`{}` (`{address}`)", handle.dotted()),
                None => format!("`{address}`"),
            },
            DoctypeKey::Link(id) => {
                format!("link `{}`", config.links[id.0].handle.dotted())
            }
        };
        let mut diagnostic = Diagnostic::at(
            format!(
                "the ports bound to {label} declare {} different document types, and one \
                 channel carries one document: a tag is compared whole, so `x@2` is not \
                 `x@1`",
                claims.len()
            ),
            claims[0].site.clone(),
        );
        for claim in claims {
            diagnostic.related.push((
                format!(
                    "port `{}` of `{}` declares `{}` here",
                    claim.port, claim.class, claim.tag
                ),
                claim.site.clone(),
            ));
        }
        errors.push(diagnostic);
    }
    // A channel's own doctype is the operator's expectation: never required,
    // inert where no doctyped port arrived, and the arbiter where one did.
    for channel in &config.channels {
        let Some(attr) = &channel.attrs.doctype else {
            continue;
        };
        let Some(expected) = doctype_tag(&attr.value, errors) else {
            continue;
        };
        // Every realm that bound this address, not one: a declaration is one row
        // in the document and the ring each binding lands on is where the row is
        // realized, so the expectation the row states holds in all of them.
        for claim in claims
            .iter()
            .filter(|(key, _)| {
                matches!(key, DoctypeKey::Address(_, address)
                    if *address == channel.address.value().as_str())
            })
            .flat_map(|(_, held)| held)
        {
            if claim.tag == expected {
                continue;
            }
            errors.push(two_site(
                format!(
                    "port `{}` of `{}` declares `{}`, and channel `{}` expects `{expected}`",
                    claim.port,
                    claim.class,
                    claim.tag,
                    channel.handle.dotted()
                ),
                claim.site.clone(),
                "the channel states its document type here",
                attr.value.span().clone(),
            ));
        }
    }
}

/// The claims one instance's bindings contribute.
///
/// Deduped by tag: classes are copied onto every instance, so N instances of one
/// class on one channel are N identical records and one claim. A binding naming
/// no channel — a free `io` port tuned in place — connects nothing and claims
/// nothing.
///
/// `realm` is the ring this instance runs on, and it keys a `local:` address; a
/// transportable address is keyed globally whatever the placement.
fn collect_doctypes<'a>(
    class: &'a ClassRef,
    bindings: &'a [RBinding],
    refs: &'a Refs<'a>,
    realm: Realm,
    claims: &mut DoctypeClaims<'a>,
) {
    for binding in bindings {
        let Some(chan) = &binding.chan else {
            continue;
        };
        let Some(port) = class
            .ports
            .iter()
            .find(|port| port.name.value() == binding.port.value())
        else {
            continue;
        };
        let Some(doctype) = &port.doctype else {
            continue;
        };
        let key = match chan {
            RChanRef::Link(id) => DoctypeKey::Link(*id),
            _ => {
                let address = match chan {
                    RChanRef::Decl(id) => refs.address(*id),
                    RChanRef::Addr(address) => address.value().as_str(),
                    RChanRef::Link(_) => unreachable!("handled above"),
                };
                let ring = match ChannelScheme::of(address) {
                    Some(ChannelScheme::Local) => realm,
                    _ => Realm::Wire,
                };
                DoctypeKey::Address(ring, address)
            }
        };
        let held = claims.entry(key).or_default();
        if held.iter().any(|claim| claim.tag == doctype.value()) {
            continue;
        }
        held.push(DoctypeClaim {
            tag: doctype.value(),
            site: doctype.span(),
            class: class.name.value(),
            port: port.name.value(),
        });
    }
}

/// A channel's `doctype`, which is a tag and nothing else: the name of a
/// document shape and its version, as a string.
fn doctype_tag<'a>(value: &'a RVal, errors: &mut Vec<Diagnostic>) -> Option<&'a str> {
    match str_value(value, "a document type") {
        Ok(tag) => Some(tag),
        Err(error) => {
            errors.push(error);
            None
        }
    }
}

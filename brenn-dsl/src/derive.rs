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
//! 4. **Authority** — which family every `acl` matcher and cross-principal
//!    `grant` lands in, whether the entity that holds it has that family at all,
//!    and what the matcher comes to once its scheme is stripped; then what every
//!    binding and subscription derives where nothing explicit holds its plane,
//!    and that each of them is covered by the authority the principal ends up
//!    with.
//! 5. **The wire-kind fold** — the kebab kind each surface-placed component
//!    instance is served under, and its collision check.
//!
//! Several tables below are transcriptions of runtime behavior — the schemes and
//! their durability, the tool namespaces, the presence rules, the uuid namespace
//! seeds. `brenn-dsl` depends on no brenn domain crate, so they are stated here
//! and carry the same drift exposure as the attr vocabularies do.

use std::collections::{HashMap, HashSet};

use fltk_cst_core::Span;
use fltk_serde_core::Spanned;
use uuid::Uuid;

use crate::derived::{
    DAclSet, DAuthorities, DAuthority, DMatcher, DMqttClient, DMqttSub, DRemoteAuthority,
    DRemoteSubEntry, DWebhook, DerivedConfig,
};
use crate::diag::{Diagnostic, check_unique, or_list, two_site};
use crate::model::Word;
use crate::resolved::{
    ChanId, ClassRef, HandlePath, MatcherKind, PortDir, RAcl, RAgent, RBinding, RChanRef, RChannel,
    RMatcher, RMatcherVal, RPort, RSurface, RTuning, RVal, RValue, ResolvedConfig, Scheme,
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
    let authorities = derive_authorities(&config, &mut errors);
    let surface_component_kinds = fold_component_kinds(&config, &mut errors);
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

/// The channel-name segments the tool substrate mints into.
///
/// Narrower than the runtime's reserved-name rule, which also reserves the `.`
/// boundary form against squatting: only the `/` form names a channel that
/// exists, so only it can be tuned.
/// TODO(dsl-vocabulary-config-parity): held equal to
/// `brenn_lib::tools::RESERVED_CHANNEL_SEGMENTS` by review.
const TOOL_NAMESPACES: [&str; 2] = ["tools", "tool-results"];

/// Does `name` — a scheme-stripped `brenn:` channel name — sit in a tool
/// namespace the substrate mints into?
fn in_a_tool_namespace(name: &str) -> bool {
    TOOL_NAMESPACES
        .iter()
        .any(|segment| name.starts_with(&format!("{segment}/")))
}

/// Is this address one the system mints for itself?
///
/// The ingress families (`mqtt:`, `webhook:`) and the tool substrate's
/// (`brenn:tools/`, `brenn:tool-results/`). Everything else is an
/// operator-declared channel's address. A total function of the address, which
/// is what makes the declaring and tuning roles disjoint.
fn is_system_minted(address: &str) -> bool {
    match Scheme::split(address) {
        Some((Scheme::Mqtt | Scheme::Webhook, _)) => true,
        Some((Scheme::Brenn, name)) => in_a_tool_namespace(name),
        _ => false,
    }
}

/// The characters a channel-name segment ends at.
const CHANNEL_BOUNDARIES: [char; 2] = ['/', '.'];

/// The characters a tuning prefix ends at: a channel name's, plus the colon that
/// closes an mqtt client (`mqtt:broker:`).
const TUNING_BOUNDARIES: [char; 3] = ['/', '.', ':'];

/// Does `prefix` end where a segment in `boundaries` ends?
///
/// Without this, `webhook:git` would reach `webhook:github`. One rule for both
/// the prefixes a tuning block names and the ones a matcher does, so a change to
/// what closes a segment cannot be made to half of them.
fn ends_at_segment_boundary(prefix: &str, boundaries: &[char]) -> bool {
    boundaries
        .iter()
        .any(|boundary| prefix.ends_with(*boundary))
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
    if tuning.is_prefix && !ends_at_segment_boundary(address, &TUNING_BOUNDARIES) {
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
            first.span().clone(),
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
/// Presence and absence only. What a depth's *value* may be — a count, the word
/// `unbounded`, the noise and sink vocabularies — is lowering's, which is where
/// the runtime's own types live.
/// TODO(dsl-vocabulary-config-parity): held equal to `build_channel_entries` and
/// `build_system_channel_tuning` by review.
fn check_channel_model(config: &ResolvedConfig, errors: &mut Vec<Diagnostic>) {
    for channel in &config.channels {
        let address = channel.address.value();
        let span = channel.address.span();
        let durable = Scheme::split(address).is_some_and(|(scheme, _)| scheme.durable());
        require_depth(
            channel.attrs.push_depth.is_some(),
            "push_depth",
            address,
            span,
            errors,
        );
        require_depth(
            channel.attrs.retain_depth.is_some(),
            "retain_depth",
            address,
            span,
            errors,
        );
        if durable {
            require_depth(
                channel.attrs.standing_retain_depth.is_some(),
                "standing_retain_depth",
                address,
                span,
                errors,
            );
            continue;
        }
        if let Some(attr) = &channel.attrs.standing_retain_depth {
            errors.push(Diagnostic::at(
                format!(
                    "`{address}` is not disk-backed, so it states no \
                     standing_retain_depth: the standing buffer is the durable reaper's \
                     frontier, and this channel's retention is retain_depth alone"
                ),
                int_or_word_span(&attr.value).clone(),
            ));
        }
        if let Some(attr) = &channel.attrs.sink {
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
        for (present, key) in [
            (tuning.attrs.push_depth.is_some(), "push_depth"),
            (tuning.attrs.retain_depth.is_some(), "retain_depth"),
            (
                tuning.attrs.standing_retain_depth.is_some(),
                "standing_retain_depth",
            ),
        ] {
            if !present {
                errors.push(Diagnostic::at(
                    format!(
                        "the block tuning `{label}` requires {key}: a system-minted channel \
                         has a bounded in-code default, and a block that tunes it states \
                         every depth"
                    ),
                    tuning.address.span().clone(),
                ));
            }
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
        crate::model::IntOrWord::Word(word) => word.name.span(),
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

/// The seeds the runtime derives a non-durable channel's identity under.
///
/// Read here only to check that no pinned or derived durable uuid collides with
/// one; these values never become a channel's configured identity.
/// TODO(dsl-vocabulary-config-parity): held equal to the seeds in
/// `brenn_lib::messaging::addressing` by review.
const EPHEMERAL_CHANNEL_SEED: &[u8] = b"brenn.ephemeral-channel";
const LOCAL_CHANNEL_SEED: &[u8] = b"brenn.local-channel";

/// A channel uuid under one of the pinned namespace seeds.
fn channel_uuid(seed: &[u8], name: &str) -> Uuid {
    let namespace = Uuid::new_v5(&Uuid::NAMESPACE_DNS, seed);
    Uuid::new_v5(&namespace, name.as_bytes())
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
        let Some((scheme, name)) = Scheme::split(address) else {
            // Resolution refuses a schemeless address, so this config never
            // reached derivation.
            unreachable!("a resolved channel address names a scheme");
        };
        let (uuid, span) = match (scheme.durable(), pins.get(address)) {
            (true, Some(pin)) => (pin.0, pin.1),
            (true, None) => (
                channel_uuid(DSL_CHANNEL_SEED, address),
                channel.address.span(),
            ),
            (false, _) => {
                // The runtime's own derivation, computed only so a pin or a
                // durable derivation colliding with it is refused here.
                let seed = match scheme {
                    Scheme::Ephemeral => EPHEMERAL_CHANNEL_SEED,
                    Scheme::Local => LOCAL_CHANNEL_SEED,
                    // Roles refused a declared channel on a system-minted
                    // scheme, and this config is already failing.
                    Scheme::Webhook | Scheme::Mqtt => {
                        uuids.push(None);
                        continue;
                    }
                    Scheme::Brenn => unreachable!("brenn: is durable"),
                };
                (channel_uuid(seed, name), channel.address.span())
            }
        };
        identities.push((uuid, address, span));
        uuids.push(scheme.durable().then_some(uuid));
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
            Scheme::split(channel.address.value()).is_some_and(|(scheme, _)| scheme.durable())
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

/// Which entity holds an authority, and with it which families it has at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityKind {
    Surface,
    Agent,
    Consumer,
    Remote,
}

impl EntityKind {
    /// What this kind is called in a diagnostic.
    fn label(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Agent => "agent",
            Self::Consumer => "consumer",
            Self::Remote => "remote",
        }
    }
}

/// The principal a statement is about: what it is, and what to call it.
#[derive(Debug, Clone, Copy)]
struct Principal<'a> {
    kind: EntityKind,
    label: &'a str,
}

/// The two planes an authority statement names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plane {
    Subscribe,
    Publish,
}

impl Plane {
    /// Both planes, in the order a diagnostic lists them.
    const ALL: [Plane; 2] = [Plane::Subscribe, Plane::Publish];

    /// The word this plane is written as.
    fn word(self) -> &'static str {
        match self {
            Plane::Subscribe => "subscribe",
            Plane::Publish => "publish",
        }
    }

    /// The plane a word spells, or `None` when it spells none.
    fn parse(word: &str) -> Option<Plane> {
        Self::ALL.into_iter().find(|plane| plane.word() == word)
    }
}

/// Where an entry was written.
///
/// A `grant` reaches into another principal's authority, and one thing is legal
/// in an entity's own body and not from outside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Statement,
    Grant,
}

/// One ACL list the runtime keeps, named as the field that holds it.
///
/// The plane and the scheme together select the list; which entity types have it
/// is the second half of the table.
/// TODO(dsl-vocabulary-config-parity): held equal to `AppAclRaw`,
/// `WasmConsumerConfigRaw`, `SurfaceConfigRaw` and `RemoteConfigRaw` by review.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Family {
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

impl Family {
    /// The list a plane and a scheme together name.
    ///
    /// `None` only for a webhook on the publish plane: webhooks are inbound, so
    /// there is no list for a right to send to one.
    fn of(scheme: Scheme, plane: Plane) -> Option<Family> {
        Some(match (scheme, plane) {
            (Scheme::Brenn, Plane::Subscribe) => Self::BrennSubscribe,
            (Scheme::Brenn, Plane::Publish) => Self::BrennPublish,
            (Scheme::Ephemeral, Plane::Subscribe) => Self::EphemeralSubscribe,
            (Scheme::Ephemeral, Plane::Publish) => Self::EphemeralPublish,
            (Scheme::Local, Plane::Subscribe) => Self::LocalSubscribe,
            (Scheme::Local, Plane::Publish) => Self::LocalPublish,
            (Scheme::Mqtt, Plane::Subscribe) => Self::MqttSubscribe,
            (Scheme::Mqtt, Plane::Publish) => Self::MqttPublish,
            (Scheme::Webhook, Plane::Subscribe) => Self::Webhook,
            (Scheme::Webhook, Plane::Publish) => return None,
        })
    }

    /// Does this entity type have this list?
    fn held_by(self, kind: EntityKind) -> bool {
        match self {
            Self::BrennSubscribe
            | Self::BrennPublish
            | Self::EphemeralSubscribe
            | Self::EphemeralPublish => true,
            // A confined channel reaches a component and nothing else: a surface
            // is authorized out of band by the page it is served to, and an agent
            // has no confined delivery path at all.
            Self::LocalSubscribe => kind == EntityKind::Consumer,
            Self::LocalPublish => matches!(kind, EntityKind::Agent | EntityKind::Consumer),
            Self::MqttSubscribe | Self::MqttPublish | Self::Webhook => {
                matches!(kind, EntityKind::Agent | EntityKind::Consumer)
            }
        }
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

    /// Is this a list whose entries a remote states depth ceilings on?
    fn carries_ceilings(self) -> bool {
        matches!(self, Self::BrennSubscribe | Self::EphemeralSubscribe)
    }

    /// Why this entity type keeps no list of this family.
    ///
    /// The one home for the fact both refusal paths rest on: a matcher naming a
    /// family the principal lacks, and a position on a scheme that family would
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

    /// The field this list is held in.
    fn name(self) -> &'static str {
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
#[derive(Debug)]
enum DEntry {
    Chan(DMatcher),
    Ceiling(DRemoteSubEntry),
    MqttSub(DMqttSub),
    MqttPub(DMqttClient),
    Webhook(DWebhook),
}

/// The reserved segment the runtime mints anonymous channels under.
///
/// A matcher reaching into it is refused: an anonymous channel's endpoint set
/// *is* its authority, so a matcher over the namespace would attach a third
/// party to a connection nobody named it in.
/// TODO(dsl-vocabulary-config-parity): held equal to
/// `brenn_lib::messaging::addressing::AUTO_CHANNEL_SEGMENT` by review.
const AUTO_CHANNEL_SEGMENT: &str = "auto";

/// Does this bare channel name sit in the anonymous namespace?
fn is_auto_channel_name(name: &str) -> bool {
    name == AUTO_CHANNEL_SEGMENT
        || name
            .strip_prefix(AUTO_CHANNEL_SEGMENT)
            .is_some_and(|rest| rest.starts_with('.') || rest.starts_with('/'))
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

/// One principal, as every phase of the authority pass sees it.
///
/// Statements, grants, bindings and the grants walk all run over one list of
/// these, so a rule that must hold for every principal is written once and a
/// per-kind exemption is a value on the row rather than a missing loop.
struct Subject<'a> {
    kind: EntityKind,
    /// The dotted handle, spelled once per principal.
    label: String,
    /// Where the identity is written: what a refusal about the whole entity cites.
    span: Span,
    acls: &'a [RAcl],
    /// Every position this principal attaches through. Empty for a remote, which
    /// holds no ports and states no subscriptions, so its authority is the
    /// entries it writes and the ones granted to it.
    bounds: Vec<Bound<'a>>,
    /// Its `grants` words, or `None` where the entity states no list at all —
    /// which an agent may do (no rights is a posture, as its config counterpart
    /// allows) and a consumer may not (the field is required with no default).
    words: Option<&'a [Word]>,
}

impl Subject<'_> {
    /// This principal, as a diagnostic names it.
    fn principal(&self) -> Principal<'_> {
        Principal {
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
                if self.kind == EntityKind::Consumer {
                    *refused = true;
                    errors.push(Diagnostic::at(
                        format!(
                            "consumer `{}` states no `grants`: what a component is given is \
                             deny-by-default, so an empty list is written `grants = [];` \
                             rather than left out",
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

/// Every principal in the document, in the order the derived model holds them:
/// surfaces, consumers, agents, remotes.
fn subjects(config: &ResolvedConfig) -> Vec<Subject<'_>> {
    let surfaces = config.surfaces.iter().map(|entity| Subject {
        kind: EntityKind::Surface,
        label: entity.handle.dotted(),
        span: handle_span(&entity.handle),
        acls: &entity.acls,
        bounds: surface_bounds(entity),
        words: Some(&entity.attrs.grants.value.words),
    });
    let consumers = config.consumers.iter().map(|entity| Subject {
        kind: EntityKind::Consumer,
        label: entity.handle.dotted(),
        span: handle_span(&entity.handle),
        acls: &entity.acls,
        bounds: binding_bounds(&entity.bindings),
        words: entity.grants.as_ref().map(|list| list.words.as_slice()),
    });
    let agents = config.agents.iter().map(|entity| Subject {
        kind: EntityKind::Agent,
        label: entity.handle.dotted(),
        span: handle_span(&entity.handle),
        acls: &entity.acls,
        bounds: agent_bounds(entity),
        words: entity
            .attrs
            .grants
            .as_ref()
            .map(|attr| attr.value.words.as_slice()),
    });
    let remotes = config.remotes.iter().map(|entity| Subject {
        kind: EntityKind::Remote,
        label: entity.handle.dotted(),
        span: handle_span(&entity.handle),
        acls: &entity.acls,
        bounds: Vec::new(),
        words: Some(&entity.attrs.grants.value.words),
    });
    surfaces
        .chain(consumers)
        .chain(agents)
        .chain(remotes)
        .collect()
}

/// One principal's entries as they accumulate, and where the explicit `acl`
/// statements it holds are.
///
/// The spans are what a coverage refusal cites: an explicit statement is the
/// whole authority for its plane, so a binding that derives nothing is answered
/// by the statement that stopped it from deriving.
#[derive(Default)]
struct Stated {
    entries: Vec<(Family, DEntry)>,
    explicit: Vec<(Plane, Span)>,
    /// Where the first position on the publish plane is, if there is one. What a
    /// wasm consumer's `ports` rule is about: an output is a right to send
    /// whether or not an entry was ever filed for it.
    output: Option<Span>,
    /// Whether anything this principal wrote was refused.
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
            .any(|(held_family, held)| (*held_family, held.key()) == (family, key))
        {
            return;
        }
        self.entries.push((family, entry));
    }

    /// Is anything filed under this family?
    ///
    /// What agreement reads: a right with an empty list authorizes nothing, and a
    /// list no right admits is never consulted.
    fn first(&self, family: Family) -> Option<&DEntry> {
        self.entries
            .iter()
            .find(|(held, _)| *held == family)
            .map(|(_, entry)| entry)
    }

    /// Is anything here enough for a position on this family to be authorized?
    fn covers(&self, family: Family, name: &str) -> bool {
        self.entries
            .iter()
            .any(|(held_family, entry)| *held_family == family && entry.covers(name))
    }
}

/// Every principal's effective authority: what its own body states, what other
/// statements grant it, and what its bindings derive.
///
/// Four phases over one principal list, so every entity type takes every phase:
/// its own statements, the grants aimed at it, its bindings, then its `grants`
/// words against the lists the first three came to.
///
/// Order is the statement set's, not a hash's: explicit entries in source order,
/// then `grant` entries in source order, then derived entries in binding order,
/// so the derived model is a pure function of the document.
fn derive_authorities(config: &ResolvedConfig, errors: &mut Vec<Diagnostic>) -> DAuthorities {
    let refs = Refs::of(config);
    let subjects = subjects(config);
    let slots: HashMap<&str, usize> = subjects
        .iter()
        .enumerate()
        .map(|(index, subject)| (subject.label.as_str(), index))
        .collect();
    let mut stated: Vec<Stated> = subjects
        .iter()
        .map(|subject| collect_statements(subject, &refs, errors))
        .collect();

    for grant in &config.grants {
        let label = grant.principal.dotted();
        let Some(&index) = slots.get(label.as_str()) else {
            unreachable!("resolution refuses a grant that names no principal");
        };
        let Some(plane) = Plane::parse(grant.plane.value()) else {
            unreachable!("resolution refuses a grant on a plane that is not a plane");
        };
        let held = &mut stated[index];
        let entry = resolve_entry(
            &grant.m,
            plane,
            subjects[index].principal(),
            Source::Grant,
            &refs,
            errors,
            &mut held.refused,
        );
        // A refused grant is a refused part of this principal's authority, so
        // agreement stops asking about the principal it was aimed at.
        match entry {
            Some(entry) => held.entries.push(entry),
            None => held.refused = true,
        }
    }

    // Bindings after grants: a derived entry is a duplicate of one already held
    // if anything states it, and what states it may be a grant.
    for (subject, held) in subjects.iter().zip(stated.iter_mut()) {
        derive_bounds(held, subject, &refs, errors);
    }

    let mut authorities = DAuthorities::default();
    for (subject, held) in subjects.iter().zip(stated) {
        let grants = derive_grants(subject, &held, errors);
        match subject.kind {
            EntityKind::Surface => authorities.surfaces.push(authority(held, grants)),
            EntityKind::Consumer => authorities.consumers.push(authority(held, grants)),
            EntityKind::Agent => authorities.agents.push(authority(held, grants)),
            EntityKind::Remote => authorities.remotes.push(remote_authority(held, grants)),
        }
    }
    authorities
}

/// One principal's entries, filed into the families an app-side entity holds.
fn authority(stated: Stated, grants: Vec<Spanned<String>>) -> DAuthority {
    let mut acl = DAclSet::default();
    for (family, entry) in stated.entries {
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
    for (family, entry) in stated.entries {
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

/// One entity's own `acl` statements, resolved into the entries they name.
fn collect_statements(
    subject: &Subject<'_>,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) -> Stated {
    let principal = subject.principal();
    let mut stated = Stated::default();
    for acl in subject.acls {
        let Some(plane) = Plane::parse(acl.plane.value()) else {
            stated.refused = true;
            errors.push(Diagnostic::at(
                format!(
                    "`{}` is not a plane; an acl statement names `subscribe` or `publish`, \
                     and which scheme it is about comes from its matchers",
                    acl.plane.value()
                ),
                acl.plane.span().clone(),
            ));
            continue;
        };
        stated.explicit.push((plane, acl.plane.span().clone()));
        for matcher in &acl.matchers {
            let entry = resolve_entry(
                matcher,
                plane,
                principal,
                Source::Statement,
                refs,
                errors,
                &mut stated.refused,
            );
            match entry {
                Some(entry) => stated.entries.push(entry),
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
    principal: Principal<'_>,
    source: Source,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
    refused: &mut bool,
) -> Option<(Family, DEntry)> {
    let kind = *matcher.kind.value();
    let span = matcher.val.span().clone();
    let (scheme, bare) = matcher_address(matcher, kind, refs, errors)?;
    let Some(family) = Family::of(scheme, plane) else {
        errors.push(Diagnostic::at(
            "a webhook is inbound only, so there is no publishing to one: an endpoint \
             belongs on the subscribe plane"
                .to_string(),
            span,
        ));
        return None;
    };
    if !family.held_by(principal.kind) {
        errors.push(Diagnostic::at(
            no_such_family(family, principal),
            span.clone(),
        ));
        return None;
    }
    if !family.admits(kind) {
        errors.push(Diagnostic::at(
            format!(
                "`{}` is not how an entry in `{}` is written; that family takes {}",
                kind.as_str(),
                family.name(),
                family.kinds()
            ),
            matcher.kind.span().clone(),
        ));
        return None;
    }
    let ceilings = principal.kind == EntityKind::Remote && family.carries_ceilings();
    if ceilings && source == Source::Grant {
        errors.push(Diagnostic::at(
            format!(
                "a grant cannot reach the subscribe plane of remote `{}`: its entries cap \
                 how deep a subscription may be held, and one ceiling per remote is what \
                 makes that a bound — write the entry in the remote's own `acl subscribe`",
                principal.label
            ),
            span,
        ));
        return None;
    }
    let entry = match family {
        Family::MqttSubscribe => {
            *refused |= refuse_tail(matcher, family, errors);
            DEntry::MqttSub(mqtt_sub_entry(&bare, &span, refs, errors)?)
        }
        Family::MqttPublish => {
            *refused |= refuse_tail(matcher, family, errors);
            DEntry::MqttPub(DMqttClient {
                client: mqtt_client(&bare, &span, refs, errors)?,
            })
        }
        Family::Webhook => {
            *refused |= refuse_tail(matcher, family, errors);
            if !refs.endpoint(&bare, &span, errors) {
                return None;
            }
            DEntry::Webhook(DWebhook {
                endpoint: Spanned::new(bare, span),
            })
        }
        _ => {
            let pattern = channel_matcher(kind, bare, &span, errors)?;
            match ceilings {
                true => {
                    let (push_depth, retain_depth) = remote_ceilings(matcher, &span, errors)?;
                    DEntry::Ceiling(DRemoteSubEntry {
                        m: pattern,
                        push_depth,
                        retain_depth,
                    })
                }
                false => {
                    *refused |= refuse_tail(matcher, family, errors);
                    DEntry::Chan(pattern)
                }
            }
        }
    };
    Some((family, entry))
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
) -> Option<(Scheme, String)> {
    match matcher.val.value() {
        RMatcherVal::Lit(text) => match Scheme::split(text) {
            Some((scheme, bare)) => Some((scheme, bare.to_string())),
            None => {
                errors.push(Diagnostic::at(
                    format!(
                        "`{text}` names no scheme, so there is no family it is about; a \
                         matcher pattern leads with {}",
                        Scheme::quoted_list()
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
            let Some((scheme, bare)) = Scheme::split(address) else {
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
/// a refusal here beats the same refusal at boot.
/// TODO(dsl-vocabulary-config-parity): held equal to
/// `brenn_lib::access::resolve::resolve_channel` by review.
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
            if !ends_at_segment_boundary(&bare, &CHANNEL_BOUNDARIES) {
                errors.push(Diagnostic::at(
                    format!(
                        "the prefix `{bare}` does not end at a segment boundary ({}), so \
                         it over-matches every sibling name it is the start of",
                        or_list(CHANNEL_BOUNDARIES)
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

/// The depths a remote's subscribe entry caps a matching subscription at.
///
/// Both required, and plain counts: a remote has no `channel` block of its own to
/// inherit a depth from, and an unbounded window is not an answer a network
/// principal may be given.
/// TODO(dsl-vocabulary-config-parity): held equal to `RemoteSubscribeAclRaw` and
/// the depth and one-of assertions in
/// `brenn_lib::messaging::remote::{lower_subscribe_matchers, zip_ceilings}` by
/// review.
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
            "push_depth" => &mut push,
            "retain_depth" => &mut retain,
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
        ("push_depth", push.is_some()),
        ("retain_depth", retain.is_some()),
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
                     window is not an answer a network principal may be given",
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

/// The refusal for a family the principal's entity type does not have.
fn no_such_family(family: Family, principal: Principal<'_>) -> String {
    format!(
        "{} `{}` can hold no `{}` authority: {}",
        principal.kind.label(),
        principal.label,
        family.name(),
        family.absent_reason(principal.kind)
    )
}

/// The lists an entry is filed into: an app-side principal's families, or the
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

/// One position that attaches a principal to a channel.
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

/// The schemes a position on this plane may name.
///
/// Not the same question as which families an entity holds ([`Family::held_by`]),
/// and it deviates from it in exactly three places, each deliberate:
///
/// - a surface may bind a `local:` channel although it holds no local family —
///   the page it is served to authorizes those frames, out of band;
/// - a wasm consumer holds an `mqtt_publish` family but names no `mqtt:` output —
///   its egress to a broker is its own block;
/// - an agent holds publish families but states no outbound position at all — it
///   publishes through the tools its grants admit.
///
/// Everywhere else this is the family table read through the plane.
/// TODO(dsl-vocabulary-config-parity): held equal to
/// `brenn_lib::messaging::boot::surfaces::validate_binding`,
/// `WasmConsumerSubscriptionRaw` and `AppAclRaw` by review.
fn bindable(kind: EntityKind, plane: Plane) -> &'static [Scheme] {
    match (kind, plane) {
        (EntityKind::Surface, _) => &[Scheme::Brenn, Scheme::Ephemeral, Scheme::Local],
        (EntityKind::Consumer, Plane::Subscribe) => &[
            Scheme::Brenn,
            Scheme::Ephemeral,
            Scheme::Local,
            Scheme::Webhook,
            Scheme::Mqtt,
        ],
        (EntityKind::Consumer, Plane::Publish) => {
            &[Scheme::Brenn, Scheme::Ephemeral, Scheme::Local]
        }
        (EntityKind::Agent, Plane::Subscribe) => &[
            Scheme::Brenn,
            Scheme::Ephemeral,
            Scheme::Webhook,
            Scheme::Mqtt,
        ],
        // An agent states no outbound position: it publishes through the tools
        // its grants admit, against the authority its `acl publish` states.
        (EntityKind::Agent, Plane::Publish) | (EntityKind::Remote, _) => &[],
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
/// about.
fn binding_bounds(bindings: &[RBinding]) -> Vec<Bound<'_>> {
    bindings
        .iter()
        .filter_map(|binding| {
            Some(Bound {
                dir: binding.dir,
                chan: binding.chan.as_ref()?,
                span: binding.port.span().clone(),
                what: "binding",
            })
        })
        .collect()
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

/// What one principal's positions come to: the entries they derive where nothing
/// explicit holds the plane, and that every one of them is authorized by the
/// entries the principal ends up with.
fn derive_bounds(
    stated: &mut Stated,
    subject: &Subject<'_>,
    refs: &Refs<'_>,
    errors: &mut Vec<Diagnostic>,
) {
    let kind = subject.kind;
    let label = &subject.label;
    let mut positions = Vec::new();
    for bound in &subject.bounds {
        let Some((scheme, bare)) = bound_address(bound, kind, label, refs, errors) else {
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
            let family =
                Family::of(scheme, plane).filter(|_| bindable(kind, plane).contains(&scheme));
            let Some(family) = family else {
                errors.push(Diagnostic::at(
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
            let Some(entry) = bound_entry(family, &bare, &bound.span, refs, errors) else {
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
    scheme: Scheme,
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
    let schemes = bindable(kind, plane);
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
) -> Option<(Scheme, String)> {
    let address = match bound.chan {
        RChanRef::Decl(id) => refs.address(*id),
        RChanRef::Addr(address) => address.value().as_str(),
    };
    let Some((scheme, bare)) = Scheme::split(address) else {
        unreachable!("a resolved address names a scheme");
    };
    if matches!(bound.chan, RChanRef::Addr(_))
        && matches!(scheme, Scheme::Brenn | Scheme::Ephemeral)
        && !(scheme == Scheme::Brenn && in_a_tool_namespace(bare))
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

// ── grants: the rights an entity states, and that they match its lists ───────
//
// A `grants` list names rights; the ACL lists say what each right reaches. The
// two are written separately and mean nothing apart: a right over an empty list
// authorizes nothing, and a list no right admits is never consulted. Both are
// refused, so a document says once what it means and the runtime is never handed
// configuration nothing reads.

/// The words one entity type's `grants` list may hold.
///
/// TODO(dsl-vocabulary-config-parity): held equal to `SurfaceGrant`,
/// `RemoteGrant`, the authorable subset of `AppCapability` and `WasmGrant` by
/// review.
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
        EntityKind::Surface => Vocabulary {
            planes: true,
            capabilities: &[Capability::Alert, Capability::Takeover],
            compound: &[
                ("ephemeral_subscribe", "subscribe"),
                ("ephemeral_publish", "publish"),
            ],
        },
        EntityKind::Remote => Vocabulary {
            planes: true,
            capabilities: &[Capability::Alert],
            compound: &[
                ("ephemeral_subscribe", "subscribe"),
                ("ephemeral_publish", "publish"),
            ],
        },
        EntityKind::Agent => Vocabulary {
            planes: true,
            capabilities: &[Capability::DynamicSubscribe, Capability::PwaPush],
            compound: &[
                ("messaging_subscribe", "subscribe"),
                ("messaging_publish", "publish"),
                ("ephemeral_subscribe", "subscribe"),
                ("ephemeral_publish", "publish"),
                ("local_publish", "publish"),
                ("mqtt_subscribe", "subscribe"),
                ("mqtt_publish", "publish"),
                ("webhook", "subscribe"),
            ],
        },
        EntityKind::Consumer => Vocabulary {
            planes: false,
            capabilities: &[
                Capability::Ports,
                Capability::Store,
                Capability::Log,
                Capability::Alert,
                Capability::Config,
                Capability::Mqtt,
            ],
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
/// TODO(dsl-vocabulary-config-parity): held equal to `SurfaceGrant`,
/// `RemoteGrant`, `AppCapability` and `WasmGrant` by review.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Capability {
    Alert,
    Takeover,
    DynamicSubscribe,
    PwaPush,
    Ports,
    Store,
    Log,
    Config,
    Mqtt,
}

impl Capability {
    /// Every capability any entity type states, in the order the vocabularies
    /// list them.
    const ALL: [Capability; 9] = [
        Capability::Alert,
        Capability::Takeover,
        Capability::DynamicSubscribe,
        Capability::PwaPush,
        Capability::Ports,
        Capability::Store,
        Capability::Log,
        Capability::Config,
        Capability::Mqtt,
    ];

    /// The word this right is written as.
    fn word(self) -> &'static str {
        match self {
            Self::Alert => "alert",
            Self::Takeover => "takeover",
            Self::DynamicSubscribe => "dynamic_subscribe",
            Self::PwaPush => "pwa_push",
            Self::Ports => "ports",
            Self::Store => "store",
            Self::Log => "log",
            Self::Config => "config",
            Self::Mqtt => "mqtt",
        }
    }

    /// The right a word spells, or `None` when it spells none.
    fn parse(word: &str) -> Option<Capability> {
        Self::ALL.into_iter().find(|right| right.word() == word)
    }
}

/// What a plane word expands to: one raw token per scheme the plane reaches,
/// in the order they are written out.
///
/// A plane word is one right in the DSL and one token per scheme in the config
/// it lowers to, so the expansion is the whole translation. Only the schemes the
/// entity has entries on are written: a token over an empty list is dead
/// configuration, which is what agreement refuses.
/// TODO(dsl-vocabulary-config-parity): held equal to the `SurfaceGrant` and
/// `RemoteGrant` variants and the authorable `AppCapability` tokens by review.
fn expansions(kind: EntityKind) -> &'static [(Plane, Scheme, &'static str)] {
    match kind {
        EntityKind::Surface | EntityKind::Remote => &[
            (Plane::Subscribe, Scheme::Brenn, "subscribe"),
            (Plane::Subscribe, Scheme::Ephemeral, "ephemeral_subscribe"),
            (Plane::Publish, Scheme::Brenn, "publish"),
            (Plane::Publish, Scheme::Ephemeral, "ephemeral_publish"),
        ],
        EntityKind::Agent => &[
            (Plane::Subscribe, Scheme::Brenn, "messaging_subscribe"),
            (Plane::Subscribe, Scheme::Ephemeral, "ephemeral_subscribe"),
            (Plane::Subscribe, Scheme::Mqtt, "mqtt_subscribe"),
            (Plane::Subscribe, Scheme::Webhook, "webhook"),
            (Plane::Publish, Scheme::Brenn, "messaging_publish"),
            (Plane::Publish, Scheme::Ephemeral, "ephemeral_publish"),
            (Plane::Publish, Scheme::Local, "local_publish"),
            (Plane::Publish, Scheme::Mqtt, "mqtt_publish"),
        ],
        // A wasm consumer's words are capabilities, which name no scheme and
        // expand to nothing: they cross into the config as written.
        EntityKind::Consumer => &[],
    }
}

/// One word, once it is known what kind of word it is.
enum Right<'a> {
    Plane(Plane, &'a Spanned<String>),
    Capability(Capability, &'a Spanned<String>),
}

/// One entity's `grants`, classified, checked against its lists, and expanded.
fn derive_grants(
    subject: &Subject<'_>,
    stated: &Stated,
    errors: &mut Vec<Diagnostic>,
) -> Vec<Spanned<String>> {
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
                "consumer `{label}` states no `{}` right: a wasm consumer's grants name \
                 the capability interfaces it is given, and its transport rights are \
                 read off its acl statements",
                name.value()
            )),
            None => match Capability::parse(name.value())
                .filter(|right| vocabulary.capabilities.contains(right))
            {
                Some(right) => {
                    rights.push(Right::Capability(right, name));
                    None
                }
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
    if !stated.refused && !refused {
        check_agreement(kind, label, &rights, stated, errors);
    }
    expand(kind, &rights, stated)
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
    if kind == EntityKind::Consumer {
        check_wasm_agreement(label, rights, stated, errors);
    }
}

/// The rights a wasm consumer's lists demand, and the lists its rights demand.
///
/// Its transport rights are read off the ACLs, so only two words pair with a
/// list at all: `ports`, which is the right to send anywhere, and `mqtt`, which
/// is the right to reach a broker.
fn check_wasm_agreement(
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
    match (word(Capability::Ports), sends) {
        (None, Some(span)) => errors.push(Diagnostic::at(
            format!(
                "consumer `{label}` sends and grants no `ports`: the messaging interface it \
                 publishes through is what `ports` gives it"
            ),
            span,
        )),
        (Some(word), None) => errors.push(Diagnostic::at(
            format!(
                "consumer `{label}` grants `ports` and neither binds an output nor states a \
                 publish entry, so the interface reaches nothing"
            ),
            word.span().clone(),
        )),
        _ => {}
    }
    match (word(Capability::Mqtt), stated.first(Family::MqttPublish)) {
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
/// positions in the source and one contract on the wire.
fn same_class_facts(left: &ClassRef, right: &ClassRef) -> bool {
    left.name.value() == right.name.value()
        && left.abi.value() == right.abi.value()
        && same_value(left.component_path.as_ref(), right.component_path.as_ref())
        && left.ports.len() == right.ports.len()
        && left
            .ports
            .iter()
            .zip(&right.ports)
            .all(|(left, right)| same_port(left, right))
}

/// One port's facts, span excluded.
fn same_port(left: &RPort, right: &RPort) -> bool {
    left.name.value() == right.name.value()
        && left.dir == right.dir
        && left.doctype.as_ref().map(Spanned::value) == right.doctype.as_ref().map(Spanned::value)
}

/// Two optional values, span excluded.
fn same_value(left: Option<&RVal>, right: Option<&RVal>) -> bool {
    fn strip(value: &RVal) -> &RValue {
        value.value()
    }
    left.map(strip) == right.map(strip)
}

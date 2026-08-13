//! Auto channels: the pass that turns `[[connection]]` blocks into channels,
//! port addresses, and the ACL grants those bindings need.
//!
//! Post-lowering a connection endpoint is an ordinary resolved binding, so
//! nothing downstream of the resolvers knows auto channels exist.
//!
//! Pure over resolved config types — no DB, no directory — so the delicate
//! `build_messaging` assembly can call it and it can be unit-tested in isolation.

use std::collections::{HashMap, HashSet};

use brenn_lib::access::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::messaging::config::{
    ConnectionConfigRaw, Depth, MessagingGlobalConfig, ResolvedChannel, Sink, SurfaceConfigRaw,
    SurfaceIoPortRaw, SurfaceOutputRaw, SurfaceSubscriptionRaw, WasmConsumerConfigRaw,
    WasmConsumerIoPortRaw, WasmConsumerOutputRaw, WasmConsumerSubscriptionRaw,
};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, auto_channel_cid, auto_channel_name, durable_auto_channel_uuid,
    is_reserved_channel_name, is_unreserved_char, nondurable_channel_uuid,
};
use uuid::Uuid;

/// The canonical endpoint ref of a port on a backend `[[wasm_consumer]]`.
pub(crate) fn wasm_endpoint_ref(slug: &str, port: &str) -> String {
    format!("wasm:{slug}/{port}")
}

/// The canonical endpoint ref of a port on a surface-hosted component instance.
/// The text before `/<port>` is exactly the component's participant id, so an
/// endpoint ref greps against logs and cursor rows.
pub(crate) fn surface_endpoint_ref(slug: &str, instance: &str, port: &str) -> String {
    format!("surface:{slug}#{instance}/{port}")
}

/// Where a connection's endpoints live, which is what decides an anonymous
/// channel's scheme: a channel is non-transportable when everything on it sits on
/// one side of a wire, and transportable only when the connection spans one.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Realm {
    /// Every endpoint is a backend `[[wasm_consumer]]` port — the server ring.
    Backend,
    /// Every endpoint is a port on one surface — page-local, per session.
    Page(String),
    /// The endpoint set crosses the wire (backend + surface, or two surfaces).
    Spanning,
}

/// Which host a connection endpoint lives on.
#[derive(Debug, Clone, PartialEq, Eq)]
enum EndpointHost {
    Wasm { slug: String },
    Surface { slug: String, instance: String },
}

/// One resolved connection endpoint: the port it names, the roles that port's
/// declarations give it, and the depths its subscribing half contributes to the
/// channel's ring.
#[derive(Debug, Clone)]
struct Endpoint {
    /// Canonical ref text — the cid key material and the label in messages.
    reference: String,
    host: EndpointHost,
    publishes: bool,
    subscribes: bool,
    /// Whether the port is an io_port — both roles by declaration, and already
    /// entitled to a channel of its own.
    io_port: bool,
    /// The subscribing half's declared depths, `None` for a publish-only
    /// endpoint, which contributes nothing to the fold.
    depths: Option<(Depth, Depth)>,
}

/// The depths a subscribing endpoint contributes to its channel's fold.
///
/// Both are required on the port itself. An auto channel derives every depth it
/// has from these numbers, so a port that states neither leaves the derivation
/// with nothing to fold — and a global rung underneath would make that silence
/// resolve to a window nobody sized. The panic names the port, which is the
/// handle the operator has: an anonymous auto channel's address is computed and
/// appears nowhere in their config.
fn endpoint_depths(
    reference: &str,
    push_depth: Option<Depth>,
    retain_depth: Option<Depth>,
) -> (Depth, Depth) {
    let (Some(push), Some(retain)) = (push_depth, retain_depth) else {
        panic!(
            "config: port {reference:?} is a subscribing endpoint of an auto channel but does \
             not state both push_depth and retain_depth — an auto channel's retention is folded \
             from its subscribing ports, so each one states what it needs to see"
        )
    };
    (push, retain)
}

/// The endpoint a backend io_port presents: both roles on one port name, with the
/// input half's depths as its contribution to the ring.
fn wasm_io_endpoint(slug: &str, io: &WasmConsumerIoPortRaw) -> Endpoint {
    let reference = wasm_endpoint_ref(slug, &io.port);
    let depths = endpoint_depths(&reference, io.push_depth, io.retain_depth);
    Endpoint {
        reference,
        host: EndpointHost::Wasm {
            slug: slug.to_string(),
        },
        publishes: true,
        subscribes: true,
        io_port: true,
        depths: Some(depths),
    }
}

/// The endpoint a surface io_port presents. See [`wasm_io_endpoint`].
fn surface_io_endpoint(slug: &str, io: &SurfaceIoPortRaw) -> Endpoint {
    let reference = surface_endpoint_ref(slug, &io.instance, &io.port);
    let depths = endpoint_depths(&reference, io.push_depth, io.retain_depth);
    Endpoint {
        reference,
        host: EndpointHost::Surface {
            slug: slug.to_string(),
            instance: io.instance.clone(),
        },
        publishes: true,
        subscribes: true,
        io_port: true,
        depths: Some(depths),
    }
}

/// One principal's authorization on one auto channel: the transport capability
/// and exact channel matcher its role needs.
#[derive(Debug, Clone)]
struct AutoGrant {
    scheme: ChannelScheme,
    /// Scheme-stripped channel name — the ACL matcher vocabulary.
    bare: String,
    publishes: bool,
    subscribes: bool,
}

/// Where an auto channel lives: its address, its delivery class, and the
/// identity of the directory entry behind it (`None` for a page-local channel,
/// which has no server entry at all — it exists only as lowered surface
/// bindings, like every surface `local:` channel).
#[derive(Debug, Clone)]
struct Placement {
    scheme: ChannelScheme,
    bare: String,
    address: String,
    page_local: bool,
    uuid: Option<Uuid>,
    durable: bool,
}

/// Everything the lowering pass produces: the synthesized channel entries, the
/// address every free port resolves to, and the grants to inject into each
/// endpoint's policy.
#[derive(Debug, Default)]
pub(crate) struct AutoWiring {
    durable_entries: Vec<ChannelEntry>,
    nondurable_entries: Vec<ChannelEntry>,
    /// Endpoint ref → full channel address.
    port_channels: HashMap<String, String>,
    /// `[[wasm_consumer]]` slug → its grants.
    wasm_grants: HashMap<String, Vec<AutoGrant>>,
    /// `[[surface]]` slug → its grants. A surface's policy is per surface, not
    /// per instance, so every instance's grants land on the one policy.
    surface_grants: HashMap<String, Vec<AutoGrant>>,
}

impl AutoWiring {
    /// Synthesized durable entries — `[[channel]]`-equivalent, carrying a DB row.
    pub(crate) fn durable_entries(&self) -> &[ChannelEntry] {
        &self.durable_entries
    }

    /// Synthesized non-durable entries — in-memory rings with no DB row.
    pub(crate) fn nondurable_entries(&self) -> &[ChannelEntry] {
        &self.nondurable_entries
    }

    /// The address a backend free port is bound to, or `None` when no connection
    /// claimed it.
    pub(crate) fn wasm_channel(&self, slug: &str, port: &str) -> Option<&str> {
        self.port_channels
            .get(&wasm_endpoint_ref(slug, port))
            .map(String::as_str)
    }

    /// The address a surface free port is bound to. See [`Self::wasm_channel`].
    pub(crate) fn surface_channel(&self, slug: &str, instance: &str, port: &str) -> Option<&str> {
        self.port_channels
            .get(&surface_endpoint_ref(slug, instance, port))
            .map(String::as_str)
    }

    /// Inject a consumer's auto-channel grants into its just-built policy.
    ///
    /// Called before every existing coverage assert, so the asserts hold with no
    /// operator-written ACL for an auto channel: the connection *is* the
    /// authorization signal, exactly as a tool grant is for the async-tool
    /// substrate.
    pub(crate) fn inject_wasm_grants(&self, slug: &str, policy: &mut AppPolicy) {
        inject(
            &format!("wasm:{slug}"),
            self.wasm_grants.get(slug).map(Vec::as_slice).unwrap_or(&[]),
            policy,
        );
    }

    /// Inject a surface's auto-channel grants into its just-built policy. See
    /// [`Self::inject_wasm_grants`].
    pub(crate) fn inject_surface_grants(&self, slug: &str, policy: &mut AppPolicy) {
        inject(
            &format!("surface:{slug}"),
            self.surface_grants
                .get(slug)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            policy,
        );
    }
}

/// The binding pair each io_port in `io_ports` resolves to: one subscription and
/// one output, in declaration order.
///
/// Both are channel-less, so each takes the one address the lowering pass
/// assigned the port — two halves that *cannot* name two channels is the whole
/// guarantee of the block. The port's own tuning splits between them: the depths,
/// noise, and amplification are the input half's, the urgency and publish budget
/// the output half's.
pub(crate) fn wasm_io_bindings(
    io_ports: &[WasmConsumerIoPortRaw],
) -> (Vec<WasmConsumerSubscriptionRaw>, Vec<WasmConsumerOutputRaw>) {
    let subscriptions = io_ports
        .iter()
        .map(|io| WasmConsumerSubscriptionRaw {
            channel: None,
            port: io.port.clone(),
            push_depth: io.push_depth,
            retain_depth: io.retain_depth,
            noise: io.noise,
            // The block carries no `wake_min` field: it is rejected on every
            // WASM subscription, so there is nothing to carry across.
            wake_min: None,
            amplification: io.amplification,
        })
        .collect();
    let outputs = io_ports
        .iter()
        .map(|io| WasmConsumerOutputRaw {
            port: io.port.clone(),
            channel: None,
            urgency: io.urgency,
            publish_per_activation: io.publish_per_activation,
            publish_capacity: io.publish_capacity,
        })
        .collect();
    (subscriptions, outputs)
}

/// The binding pair each of a surface's io_ports resolves to. See
/// [`wasm_io_bindings`].
pub(crate) fn surface_io_bindings(
    io_ports: &[SurfaceIoPortRaw],
) -> (Vec<SurfaceSubscriptionRaw>, Vec<SurfaceOutputRaw>) {
    let subscriptions = io_ports
        .iter()
        .map(|io| SurfaceSubscriptionRaw {
            channel: None,
            instance: io.instance.clone(),
            port: io.port.clone(),
            push_depth: io.push_depth,
            retain_depth: io.retain_depth,
            noise: io.noise,
            wake_min: None,
        })
        .collect();
    let outputs = io_ports
        .iter()
        .map(|io| SurfaceOutputRaw {
            instance: io.instance.clone(),
            port: io.port.clone(),
            channel: None,
            urgency: io.urgency,
            publish_per_activation: io.publish_per_activation,
            publish_capacity: io.publish_capacity,
        })
        .collect();
    (subscriptions, outputs)
}

/// Which half of a channel one injected grant authorizes.
#[derive(Debug, Clone, Copy)]
enum Role {
    Publish,
    Subscribe,
}

/// The ACL list a `(scheme, role)` matcher belongs in, and the capability that
/// list is dead without.
fn acl_slot(
    policy: &mut AppPolicy,
    scheme: ChannelScheme,
    role: Role,
) -> (&mut Vec<ChannelMatcher>, AppCapability) {
    match (scheme, role) {
        (ChannelScheme::Brenn, Role::Publish) => (
            &mut policy.acls.brenn_publish,
            AppCapability::MessagingPublish,
        ),
        (ChannelScheme::Ephemeral, Role::Publish) => (
            &mut policy.acls.ephemeral_publish,
            AppCapability::EphemeralPublish,
        ),
        (ChannelScheme::Local, Role::Publish) => {
            (&mut policy.acls.local_publish, AppCapability::LocalPublish)
        }
        (ChannelScheme::Brenn, Role::Subscribe) => (
            &mut policy.acls.brenn_subscribe,
            AppCapability::MessagingSubscribe,
        ),
        (ChannelScheme::Ephemeral, Role::Subscribe) => (
            &mut policy.acls.ephemeral_subscribe,
            AppCapability::EphemeralSubscribe,
        ),
        (ChannelScheme::Local, Role::Subscribe) => (
            &mut policy.acls.local_subscribe,
            AppCapability::LocalSubscribe,
        ),
        (other, _) => unreachable!("auto channels carry pub/sub schemes only, got {other:?}"),
    }
}

/// Apply one principal's grants to its policy, logging each at info.
///
/// Auto-injection means a principal's ACL lists in config no longer enumerate its
/// full reach, so the boot log is what restores a single complete accounting for
/// a config security review: config plus one grep of these lines.
fn inject(principal: &str, grants: &[AutoGrant], policy: &mut AppPolicy) {
    for grant in grants {
        for role in [Role::Publish, Role::Subscribe] {
            let active = match role {
                Role::Publish => grant.publishes,
                Role::Subscribe => grant.subscribes,
            };
            if !active {
                continue;
            }
            let (matchers, capability) = acl_slot(policy, grant.scheme, role);
            matchers.push(ChannelMatcher::Exact(grant.bare.clone()));
            policy.grants.insert(capability);
            tracing::info!(
                principal,
                capability = ?capability,
                channel = %format!("{}{}", grant.scheme.prefix(), grant.bare),
                "auto channel grant injected",
            );
        }
    }
}

/// Lower every `[[connection]]` block and every io_port declaration into
/// channels, port addresses, and grants.
///
/// `declared_addresses` is every channel address already claimed by another
/// declaration (`[[channel]]` blocks, webhook and mqtt derivations): an auto
/// channel address that also appears there is the seam through which auto-ACLs
/// could reach a channel other parties legitimately use, so it is a boot panic.
///
/// # Panics
///
/// On every malformed, dead, or ambiguous connection — a malformed endpoint ref,
/// an endpoint naming an unknown consumer/surface/instance/port, a port that
/// already binds a channel of its own or is claimed by another connection, a
/// connection with no publisher or no subscriber or naming one io_port alone, a
/// named address that is reserved / mis-schemed / colliding, a `local:` name on a
/// wire-spanning endpoint set, or a `uuid` on a channel that has no DB row to
/// name.
pub(crate) fn lower_auto_wiring(
    connections: &[ConnectionConfigRaw],
    consumers: &[WasmConsumerConfigRaw],
    surfaces: &[SurfaceConfigRaw],
    declared_addresses: &[&str],
    defaults: &MessagingGlobalConfig,
) -> AutoWiring {
    let declared: HashSet<&str> = declared_addresses.iter().copied().collect();
    let mut wiring = AutoWiring::default();
    // Endpoint ref → the connection that claimed it. Doubles as the
    // twice-in-one-list check: the second occurrence finds its own connection.
    let mut claimed: HashMap<String, String> = HashMap::new();
    let mut synthesized: HashMap<String, String> = HashMap::new();

    for (index, conn) in connections.iter().enumerate() {
        let label = format!(
            "[[connection]] #{} (endpoints: {})",
            index + 1,
            conn.endpoints.join(", "),
        );
        assert!(
            !conn.endpoints.is_empty(),
            "config: {label}: endpoints is empty — a connection exists to wire ports \
             together, so it needs at least one publisher and one subscriber",
        );

        let mut endpoints: Vec<Endpoint> = Vec::with_capacity(conn.endpoints.len());
        for reference in &conn.endpoints {
            let endpoint = resolve_endpoint(&label, reference, consumers, surfaces);
            if let Some(owner) = claimed.get(&endpoint.reference) {
                panic!(
                    "config: {label}: endpoint {:?} is already bound by {owner} — a free port \
                     is named by exactly one connection",
                    endpoint.reference,
                );
            }
            claimed.insert(endpoint.reference.clone(), label.clone());
            endpoints.push(endpoint);
        }

        // An io_port already has a channel of its own serving both of its
        // directions, so a connection naming nothing else adds nothing.
        assert!(
            !(endpoints.len() == 1 && endpoints[0].io_port),
            "config: {label}: names one io_port and nothing else — an io_port is already wired \
             to itself through its own channel, so this block changes nothing; name the io_port's \
             `channel` field if it needs an address, or add the other ports that share the \
             channel",
        );

        // A channel with no publisher can never carry a message; one with no
        // subscriber can never deliver. Either way the connection is dead config.
        assert!(
            endpoints.iter().any(|e| e.publishes),
            "config: {label}: no endpoint publishes — every port named here is declared as a \
             subscription only, so nothing can ever put a message on this channel",
        );
        assert!(
            endpoints.iter().any(|e| e.subscribes),
            "config: {label}: no endpoint subscribes — every port named here is declared as an \
             output only, so nothing can ever receive from this channel",
        );

        place_channel(
            &label,
            &endpoints,
            conn.channel.as_deref(),
            conn.uuid.as_deref(),
            conn.description.clone(),
            &declared,
            &mut synthesized,
            defaults,
            &mut wiring,
        );
    }

    lower_io_ports(
        consumers,
        surfaces,
        &declared,
        defaults,
        &claimed,
        &mut synthesized,
        &mut wiring,
    );

    wiring
}

/// Give one endpoint set its channel: place the address, synthesize the directory
/// entry the address needs, and record what each endpoint on it gets — the
/// address its bindings resolve to, and the grants its roles need.
///
/// # Panics
///
/// On an address another auto channel or another declaration already owns, and on
/// anything [`decide_placement`] or [`fold_retain_depth`] rejects.
#[allow(clippy::too_many_arguments)]
fn place_channel(
    label: &str,
    endpoints: &[Endpoint],
    declared: Option<&str>,
    declared_uuid: Option<&str>,
    description: Option<String>,
    declared_addresses: &HashSet<&str>,
    synthesized: &mut HashMap<String, String>,
    defaults: &MessagingGlobalConfig,
    wiring: &mut AutoWiring,
) {
    let placement = decide_placement(label, endpoints, declared, declared_uuid);

    if let Some(owner) = synthesized.get(&placement.address) {
        panic!(
            "config: {label}: channel {:?} is already declared by {owner} — two auto \
             channels cannot share an address (their endpoints' ACLs would merge)",
            placement.address,
        );
    }
    // Operator-declared surface `local:` names are deliberately absent from
    // `declared_addresses`: they reach no directory, and page-local traffic
    // has no bus gate, so a page-local auto channel creates no ACL that could
    // leak.  A named page-local auto channel sharing its address with an
    // operator binding folds into one ring, just as two operator-declared
    // bindings on the same name already do.  Anonymous auto addresses stay
    // unreachable: `bound_channel` rejects the `auto` namespace on every
    // operator-written binding.
    assert!(
        !declared_addresses.contains(placement.address.as_str()),
        "config: {label}: channel {:?} is also declared elsewhere (a [[channel]] block, or \
         a webhook/mqtt-derived channel) — an auto channel's ACLs are injected from its \
         endpoints, so it must own its address; rename it or bind the existing channel with \
         ordinary bindings and ACLs",
        placement.address,
    );
    synthesized.insert(placement.address.clone(), label.to_string());

    if let Some(uuid) = placement.uuid {
        let retain_depth = fold_retain_depth(label, endpoints, placement.durable);
        let entry = synthesize_entry(
            uuid,
            &placement,
            retain_depth,
            description,
            endpoints,
            defaults,
        );
        if placement.durable {
            wiring.durable_entries.push(entry);
        } else {
            wiring.nondurable_entries.push(entry);
        }
    }

    for endpoint in endpoints {
        wiring
            .port_channels
            .insert(endpoint.reference.clone(), placement.address.clone());
        // Page-local traffic never reaches the bus, so there is no delivery
        // or publish for a bus ACL to authorize — the same reason surface
        // `local:` bindings are exempt from the coverage checks.
        if placement.page_local {
            continue;
        }
        let grant = AutoGrant {
            scheme: placement.scheme,
            bare: placement.bare.clone(),
            publishes: endpoint.publishes,
            subscribes: endpoint.subscribes,
        };
        match &endpoint.host {
            EndpointHost::Wasm { slug } => wiring
                .wasm_grants
                .entry(slug.clone())
                .or_default()
                .push(grant),
            EndpointHost::Surface { slug, .. } => wiring
                .surface_grants
                .entry(slug.clone())
                .or_default()
                .push(grant),
        }
    }
}

/// Record every declared io_port, and give each one no `[[connection]]` claimed a
/// channel of its own.
///
/// Each io_port ends up on exactly one channel: either the one a `[[connection]]`
/// placed, or one this function synthesizes.
///
/// # Panics
///
/// On a surface io_port naming an undeclared instance, and on anything
/// [`place_channel`] rejects.
fn lower_io_ports(
    consumers: &[WasmConsumerConfigRaw],
    surfaces: &[SurfaceConfigRaw],
    declared_addresses: &HashSet<&str>,
    defaults: &MessagingGlobalConfig,
    claimed: &HashMap<String, String>,
    synthesized: &mut HashMap<String, String>,
    wiring: &mut AutoWiring,
) {
    for consumer in consumers {
        for io in &consumer.io_ports {
            let label = format!(
                "[[wasm_consumer]] {:?} io_port {:?}",
                consumer.slug, io.port,
            );
            let endpoint = wasm_io_endpoint(&consumer.slug, io);
            if !claimed.contains_key(&endpoint.reference) {
                place_channel(
                    &label,
                    std::slice::from_ref(&endpoint),
                    io.channel.as_deref(),
                    None,
                    None,
                    declared_addresses,
                    synthesized,
                    defaults,
                    wiring,
                );
            }
        }
    }
    for surface in surfaces {
        for io in &surface.io_ports {
            let label = format!(
                "[[surface]] {:?} io_port {:?} on instance {:?}",
                surface.slug, io.port, io.instance,
            );
            assert!(
                surface
                    .components
                    .iter()
                    .any(|c| c.instance.as_deref().unwrap_or(&c.kind) == io.instance),
                "config: {label}: names instance {:?}, which is not declared as a \
                 [[surface.component]] on this surface",
                io.instance,
            );
            let endpoint = surface_io_endpoint(&surface.slug, io);
            if !claimed.contains_key(&endpoint.reference) {
                place_channel(
                    &label,
                    std::slice::from_ref(&endpoint),
                    io.channel.as_deref(),
                    None,
                    None,
                    declared_addresses,
                    synthesized,
                    defaults,
                    wiring,
                );
            }
        }
    }
}

/// Build the synthesized directory entry for one auto channel.
///
/// Every depth is the fold; every other channel-level knob comes from the
/// `[messaging]` defaults. A channel that needs channel-level tuning has
/// outgrown auto declaration.
///
/// The fold is the only number the declaration grounded, so it answers all three
/// depth questions. `push_depth` is a rung nothing on the connection itself
/// reads — each endpoint carries its own — and is consulted only by a later
/// third-party binding on a *named* auto channel, which is exactly the case
/// where "as deep as the ports that declared this channel" is the right answer.
/// Durable `standing_retain_depth` takes the fold for the same reason the
/// non-durable arm below has no choice: the standing buffer covers the retained
/// window, and here that window *is* the fold.
fn synthesize_entry(
    uuid: Uuid,
    placement: &Placement,
    retain_depth: Depth,
    description: Option<String>,
    endpoints: &[Endpoint],
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let (standing_retain_depth, sink) = if placement.durable {
        (retain_depth, defaults.default_sink)
    } else {
        // The standing buffer is the retained window itself: a non-durable
        // channel has no subscriber-independent store off-disk.
        (retain_depth, Sink::Drop)
    };
    assert!(
        !(sink == Sink::Archive && defaults.archive_path.is_none()),
        "config: auto channel {:?} inherits sink = \"archive\" but [messaging].archive_path is \
         not set",
        placement.address,
    );
    let description = description.unwrap_or_else(|| {
        let refs: Vec<&str> = endpoints.iter().map(|e| e.reference.as_str()).collect();
        format!("auto channel: {}", refs.join(", "))
    });
    ChannelEntry {
        uuid,
        address: placement.address.clone(),
        description: Some(description),
        resolved_channel: ResolvedChannel {
            send_rate: defaults.default_send_rate,
            push_depth: retain_depth,
            retain_depth,
            standing_retain_depth,
            noise: defaults.default_noise,
            sink,
            wake_min: defaults.default_wake_min,
        },
        subscribers: vec![],
        transport_type: placement.scheme,
        mount: None,
    }
}

/// The channel's `retain_depth`: the max over subscribing endpoints of
/// `max(push_depth, retain_depth)`, floor 1.
///
/// The subscribers' consumption parameters are what the ring must cover, so
/// publish-only endpoints contribute nothing and the hungriest subscriber sets
/// the depth. The floor exists because a depth-0 ring retains nothing at all,
/// which no connection asked for. `Depth`'s own ordering is the fold: every
/// `Bounded(_)` sorts below `Unbounded`, so "no cap declared" dominates a cap
/// that bounds only one port's need.
///
/// # Panics
///
/// On an `Unbounded` fold over a non-durable channel — non-durable retention is
/// process memory, the same rule a `[[channel]]` block takes.
fn fold_retain_depth(label: &str, endpoints: &[Endpoint], durable: bool) -> Depth {
    let mut folded = Depth::Bounded(1);
    for endpoint in endpoints {
        let Some((push, retain)) = endpoint.depths else {
            continue;
        };
        folded = folded.max(push).max(retain);
    }
    assert!(
        durable || folded != Depth::Unbounded,
        "config: {label}: the connection's subscribing port(s) fold to retain_depth = \
         \"unbounded\", but this channel is non-durable and its retention is process memory; \
         the fold takes max(push_depth, retain_depth) per subscribing port, so both halves \
         must be bounded: give the subscribing port(s) bounded depths, or name the channel \
         with a brenn: address",
    );
    folded
}

/// Which realm a connection's endpoint set lives in.
fn realm_of(endpoints: &[Endpoint]) -> Realm {
    let mut surface_slug: Option<&str> = None;
    let mut any_wasm = false;
    for endpoint in endpoints {
        match &endpoint.host {
            EndpointHost::Wasm { .. } => any_wasm = true,
            EndpointHost::Surface { slug, .. } => match surface_slug {
                Some(seen) if seen != slug.as_str() => return Realm::Spanning,
                Some(_) => {}
                None => surface_slug = Some(slug),
            },
        }
    }
    match (any_wasm, surface_slug) {
        (true, None) => Realm::Backend,
        (false, Some(slug)) => Realm::Page(slug.to_string()),
        _ => Realm::Spanning,
    }
}

/// Decide an auto channel's address, delivery class, and entry identity.
///
/// Anonymous (no operator `channel`): the bare name is `auto.<cid>` over the
/// sorted endpoint set, and the scheme follows the realm — non-transportable
/// when every endpoint is on one side of a wire, `ephemeral:` when the connection
/// spans one. Named: the operator's scheme picks the capability, and `brenn:` is
/// what buys durability.
fn decide_placement(
    label: &str,
    endpoints: &[Endpoint],
    declared: Option<&str>,
    declared_uuid: Option<&str>,
) -> Placement {
    let realm = realm_of(endpoints);
    let Some(address) = declared else {
        assert!(
            declared_uuid.is_none(),
            "config: {label}: uuid is set but no channel name is — an anonymous auto channel is \
             never durable and has no DB row to name; give the connection a brenn: channel or \
             drop the uuid",
        );
        let refs: Vec<String> = endpoints.iter().map(|e| e.reference.clone()).collect();
        let bare = auto_channel_name(auto_channel_cid(&refs));
        let (scheme, page_local) = match realm {
            Realm::Backend => (ChannelScheme::Local, false),
            Realm::Page(_) => (ChannelScheme::Local, true),
            Realm::Spanning => (ChannelScheme::Ephemeral, false),
        };
        return Placement {
            uuid: (!page_local).then(|| nondurable_channel_uuid(scheme, &bare)),
            address: format!("{}{bare}", scheme.prefix()),
            scheme,
            bare,
            page_local,
            durable: false,
        };
    };

    let Some((scheme, bare)) = ChannelScheme::split(address) else {
        panic!(
            "config: {label}: channel {address:?} carries no scheme prefix — a named auto \
             channel is a full address: brenn: for durability, ephemeral: for a shared \
             in-memory ring, local: for one that never crosses the wire",
        )
    };
    assert!(
        matches!(
            scheme,
            ChannelScheme::Brenn | ChannelScheme::Ephemeral | ChannelScheme::Local
        ),
        "config: {label}: channel {address:?} must be a brenn:, ephemeral:, or local: address — \
         a connection wires pub/sub ports, and ingress/egress transports are declared by their \
         own config blocks",
    );
    assert!(
        !bare.is_empty(),
        "config: {label}: channel {address:?} must name a channel after its scheme",
    );
    assert!(
        bare.chars().all(is_unreserved_char),
        "config: {label}: channel {address:?} must consist of RFC 3986 unreserved characters \
         only (A-Za-z0-9._~-) after its scheme",
    );
    assert!(
        !is_reserved_channel_name(bare),
        "config: {label}: channel {address:?} is in a reserved namespace (tools/tool-results are \
         owned by the tool substrate; auto is owned by the auto-channel machinery)",
    );

    let durable = scheme == ChannelScheme::Brenn;
    let page_local = scheme == ChannelScheme::Local
        && match &realm {
            Realm::Page(_) => true,
            Realm::Backend => false,
            Realm::Spanning => panic!(
                "config: {label}: channel {address:?} is a local: address, but the endpoint set \
                 spans the wire (backend and surface, or two surfaces) — local: cannot span the \
                 wire; use an ephemeral: address",
            ),
        };
    let uuid =
        match (durable, declared_uuid, page_local) {
            (true, Some(raw), _) => Some(Uuid::parse_str(raw).unwrap_or_else(|e| {
                panic!("config: {label}: uuid {raw:?} is not a valid UUID: {e}")
            })),
            (true, None, _) => Some(durable_auto_channel_uuid(bare)),
            (false, Some(_), _) => panic!(
                "config: {label}: uuid is set but channel {address:?} is not durable — only a \
             brenn: channel has a DB row to name; drop the uuid or make the channel brenn:",
            ),
            // A page-local channel has no server entry at all: it exists only as the
            // surface bindings the pass lowers, like every surface `local:` channel.
            (false, None, true) => None,
            (false, None, false) => Some(nondurable_channel_uuid(scheme, bare)),
        };

    Placement {
        scheme,
        bare: bare.to_string(),
        address: address.to_string(),
        page_local,
        uuid,
        durable,
    }
}

/// Resolve one endpoint ref against the declarations it names, inferring the
/// port's roles from the lists it appears in.
///
/// Direction is never stated on the connection: a port declared in a
/// `subscriptions` list subscribes, one in an `outputs` list publishes. Stating
/// it here would only invite contradiction with the port declaration.
fn resolve_endpoint(
    label: &str,
    reference: &str,
    consumers: &[WasmConsumerConfigRaw],
    surfaces: &[SurfaceConfigRaw],
) -> Endpoint {
    let malformed = || -> ! {
        panic!(
            "config: {label}: endpoint {reference:?} is malformed — an endpoint is \
             \"wasm:<slug>/<port>\" for a [[wasm_consumer]] port, or \
             \"surface:<slug>#<instance>/<port>\" for a surface component's port",
        )
    };
    let Some((kind, rest)) = reference.split_once(':') else {
        malformed()
    };
    match kind {
        "wasm" => {
            let Some((slug, port)) = rest.split_once('/') else {
                malformed()
            };
            assert!(
                !slug.is_empty() && !port.is_empty() && port.chars().all(is_unreserved_char),
                "config: {label}: endpoint {reference:?} is malformed — \"wasm:<slug>/<port>\" \
                 takes one port name, of RFC 3986 unreserved characters (A-Za-z0-9._~-)",
            );
            let consumer = consumers
                .iter()
                .find(|c| c.slug == slug)
                .unwrap_or_else(|| {
                    panic!(
                        "config: {label}: endpoint {reference:?} names no declared \
                         [[wasm_consumer]] (slug {slug:?})",
                    )
                });
            if let Some(io) = consumer.io_ports.iter().find(|p| p.port == port) {
                assert_free(label, reference, io.channel.as_deref());
                return wasm_io_endpoint(slug, io);
            }
            let subscription = consumer.subscriptions.iter().find(|s| s.port == port);
            let output = consumer.outputs.iter().find(|o| o.port == port);
            assert!(
                subscription.is_some() || output.is_some(),
                "config: {label}: endpoint {reference:?} names no port declared on \
                 [[wasm_consumer]] {slug:?} — a connection binds ports that already declare \
                 themselves (and their tuning) on their own block",
            );
            if let Some(sub) = subscription {
                assert_free(label, reference, sub.channel.as_deref());
            }
            if let Some(out) = output {
                assert_free(label, reference, out.channel.as_deref());
            }
            Endpoint {
                reference: wasm_endpoint_ref(slug, port),
                host: EndpointHost::Wasm {
                    slug: slug.to_string(),
                },
                publishes: output.is_some(),
                subscribes: subscription.is_some(),
                io_port: false,
                depths: subscription.map(|s| {
                    endpoint_depths(&wasm_endpoint_ref(slug, port), s.push_depth, s.retain_depth)
                }),
            }
        }
        "surface" => {
            let Some((head, port)) = rest.split_once('/') else {
                malformed()
            };
            let Some((slug, instance)) = head.split_once('#') else {
                malformed()
            };
            assert!(
                !slug.is_empty()
                    && !instance.is_empty()
                    && !port.is_empty()
                    && port.chars().all(is_unreserved_char),
                "config: {label}: endpoint {reference:?} is malformed — \
                 \"surface:<slug>#<instance>/<port>\" takes one port name, of RFC 3986 \
                 unreserved characters (A-Za-z0-9._~-)",
            );
            let surface = surfaces.iter().find(|s| s.slug == slug).unwrap_or_else(|| {
                panic!(
                    "config: {label}: endpoint {reference:?} names no declared [[surface]] \
                     (slug {slug:?})",
                )
            });
            assert!(
                surface
                    .components
                    .iter()
                    .any(|c| c.instance.as_deref().unwrap_or(&c.kind) == instance),
                "config: {label}: endpoint {reference:?} names instance {instance:?}, which is \
                 not declared as a [[surface.component]] on surface {slug:?}",
            );
            if let Some(io) = surface
                .io_ports
                .iter()
                .find(|p| p.instance == instance && p.port == port)
            {
                assert_free(label, reference, io.channel.as_deref());
                return surface_io_endpoint(slug, io);
            }
            let subscription = surface
                .subscriptions
                .iter()
                .find(|s| s.instance == instance && s.port == port);
            let output = surface
                .outputs
                .iter()
                .find(|o| o.instance == instance && o.port == port);
            assert!(
                subscription.is_some() || output.is_some(),
                "config: {label}: endpoint {reference:?} names no port declared on surface \
                 {slug:?} — a connection binds ports that already declare themselves (and their \
                 tuning) on their own block",
            );
            if let Some(sub) = subscription {
                assert_free(label, reference, sub.channel.as_deref());
            }
            if let Some(out) = output {
                assert_free(label, reference, out.channel.as_deref());
            }
            Endpoint {
                reference: surface_endpoint_ref(slug, instance, port),
                host: EndpointHost::Surface {
                    slug: slug.to_string(),
                    instance: instance.to_string(),
                },
                publishes: output.is_some(),
                subscribes: subscription.is_some(),
                io_port: false,
                depths: subscription.map(|s| {
                    endpoint_depths(
                        &surface_endpoint_ref(slug, instance, port),
                        s.push_depth,
                        s.retain_depth,
                    )
                }),
            }
        }
        _ => malformed(),
    }
}

/// A port a connection names must be *free* — declared with no `channel` of its
/// own. A port binds exactly one channel, so an address on the binding and a
/// connection claiming it are two answers to one question.
fn assert_free(label: &str, reference: &str, declared: Option<&str>) {
    assert!(
        declared.is_none(),
        "config: {label}: endpoint {reference:?} already binds channel {:?} on its own block — \
         a port binds exactly one channel; remove the binding's channel or drop the endpoint \
         from this connection",
        declared.unwrap_or_default(),
    );
}

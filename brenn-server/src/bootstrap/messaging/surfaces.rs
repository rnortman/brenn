use std::collections::BTreeMap;
use std::time::Duration;

use brenn_budget::MAX_PUBLISHES_PER_ACTIVATION;
use brenn_lib::messaging::config::{
    AttachSendBudget, DEFAULT_PARKED_BATCH_DEPTH, DEFAULT_WASM_PUBLISH_CAPACITY,
    DEFAULT_WASM_PUBLISH_PER_ACTIVATION, Depth, MessagingGlobalConfig, NoiseLevel,
    ResolvedComponent, ResolvedLocalChannel, ResolvedSubscription, ResolvedSurface,
    ResolvedSurfaceSubscription, SurfaceBinding, SurfaceComponentRaw, SurfaceConfigRaw,
    SurfaceOutput, SurfaceOutputRaw,
};
use brenn_lib::messaging::{ChannelScheme, MessagingDirectory, Urgency};
use brenn_surface_schema::Abi;
use indexmap::IndexMap;

use super::auto::AutoWiring;
use super::resolve_publish_millitokens;

/// Fold one `local:` binding's `retain_depth` into the channel's resolved ring
/// depth.
///
/// A `local:` channel has no `[[channel]]` block to inherit from — the
/// per-surface bindings *are* the declaration — so the ring depth is resolved
/// from the bindings themselves: **the max over the channel's declared bindings'
/// `retain_depth`, floor 1**. The floor exists because a depth-0 ring would
/// silently break the late-attach handoff the local class is for (a chrome that
/// mounts after a theme publish would never see the theme). Reserved control
/// channels instead carry contract-fixed depths, so a `retain_depth` on one is
/// rejected rather than quietly ignored.
///
/// # Panics
///
/// On an unbounded `retain_depth` (the ring is page memory — "unbounded" is a
/// page that grows until the tab dies, not a retention policy), or on a
/// `retain_depth` set for a reserved control channel.
fn accumulate_local_ring_depth(
    slug: &str,
    depths: &mut IndexMap<String, u64>,
    channel: &str,
    retain_depth: Option<Depth>,
) {
    if let Some(reserved) = brenn_surface_schema::reserved_local_channel(channel) {
        assert!(
            retain_depth.is_none(),
            "config: [[surface]] {slug:?}: binding on reserved control channel {channel:?} sets \
             retain_depth — reserved channels carry contract-fixed ring depths ({} here), so the \
             override would be silently ignored; remove it",
            reserved.ring_depth,
        );
        depths.insert(channel.to_string(), reserved.ring_depth);
        return;
    }
    let depth = match retain_depth {
        None => 1,
        Some(Depth::Bounded(n)) => n.max(1),
        Some(Depth::Unbounded) => panic!(
            "config: [[surface]] {slug:?}: binding on local channel {channel:?} sets \
             retain_depth = \"unbounded\" — a local channel's retained ring lives in page memory \
             and must be bounded; give it a number",
        ),
    };
    let entry = depths.entry(channel.to_string()).or_insert(depth);
    *entry = (*entry).max(depth);
}

/// How deep one `local:` binding reads into its channel's router ring when the
/// kernel windows its port.
///
/// Deliberately the same per-binding resolution [`accumulate_local_ring_depth`]
/// folds into the ring depth — reserved channels take the contract-fixed depth,
/// operator-declared channels the binding's `retain_depth` or the floor 1. The
/// ring is the fold across every binding; this is one binding's read out of it,
/// so a binding never windows deeper than the ring it reads and never shallower
/// than what it asked for.
///
/// This is where `local:` parts from the stated 0 default of the other classes,
/// and it must: the router keeps a ring for every local channel whether or not a
/// binding asks, and the reserved planes exist precisely to replay their last
/// value to whoever attaches. A 0 default would leave an unstated binding blind
/// to a ring the kernel is already filling for it.
///
/// Unbounded is not handled here — [`accumulate_local_ring_depth`] panics on it
/// first, on the same binding.
fn local_context_depth(channel: &str, retain_depth: Option<Depth>) -> u64 {
    if let Some(reserved) = brenn_surface_schema::reserved_local_channel(channel) {
        return reserved.ring_depth;
    }
    match retain_depth {
        None => 1,
        Some(Depth::Bounded(n)) => n.max(1),
        Some(Depth::Unbounded) => {
            unreachable!("accumulate_local_ring_depth rejects unbounded local retain_depth")
        }
    }
}

/// Resolve a surface binding's page-side port-queue depth: the binding's own
/// `push_depth`, else the `channel_rung` the caller passes — the channel entry's
/// own `push_depth` for a transportable binding, and `None` for `local:`, which
/// has no `[[channel]]` block to read and so must state the depth on the binding.
///
/// # Panics
///
/// On a depth that cannot be a page queue: unbounded (the queue is browser
/// memory — "unbounded" is a tab that grows until it dies), or unstated with no
/// rung to read. The rule is class-uniform: every surface binding's port queue is
/// page memory and must resolve bounded, whatever its class. No default is
/// invented. Depth 0 is not judged here — it is legal or not depending on the
/// binding's retained context and delivery model, which is
/// [`assert_page_queue_deliverable`]'s call once both are resolved.
fn resolve_page_queue_depth(
    slug: &str,
    context: &str,
    channel: &str,
    push_depth: Option<Depth>,
    channel_rung: Option<Depth>,
) -> u64 {
    let Some(resolved) = push_depth.or(channel_rung) else {
        panic!(
            "config: [[surface]] {slug:?}: {context} channel {channel:?} states no push_depth — \
             a local: channel has no [[channel]] block to inherit one from, so the binding sizes \
             its own page queue"
        )
    };
    let Depth::Bounded(n) = resolved else {
        panic!(
            "config: [[surface]] {slug:?}: {context} channel {channel:?} resolves to push_depth = \
             {resolved:?} — a surface binding's port queue lives in page memory and must resolve \
             to a bounded push_depth; set it on the binding or on the channel rung"
        )
    };
    n
}

/// The rules a surface input binding's resolved `(push_depth, retain_depth,
/// noise)` must satisfy — the bus's own set.
///
/// Adopted from `resolve_wasm_consumers`, which validates the same three facts
/// about a `[[wasm_consumer]]` subscription, because a surface binding is the
/// same shape of port: a page queue in front, a retained ring behind, an
/// overflow policy over the queue.
///
/// Depth 0 is legal on every ABI and every class: it is the bus's
/// sampled/context-only port — it never activates its component, and when some
/// other port does, its window is pure retained context. Every surface component
/// rides activations, so every depth-0 binding has a window to hang that context
/// on.
///
/// # Panics
///
/// On an explicit `noise` at depth 0 (no push window exists to overflow, so the
/// policy has no referent); on a port that is neither triggering nor
/// context-carrying (`push_depth = 0` and `retain_depth = 0`) — a dead port.
pub(super) fn assert_page_queue_deliverable(
    slug: &str,
    context: &str,
    channel: &str,
    push_depth: u64,
    retain_depth: u64,
    noise: Option<NoiseLevel>,
) {
    if push_depth >= 1 {
        return;
    }
    assert!(
        noise.is_none(),
        "config: [[surface]] {slug:?}: {context} channel {channel:?} has noise configured but \
         push_depth = 0 (sampled/context-only) — no push-overflow events are possible, so the \
         noise setting has no referent; remove the noise setting or set push_depth > 0",
    );
    assert!(
        retain_depth >= 1,
        "config: [[surface]] {slug:?}: {context} channel {channel:?} has push_depth = 0 AND \
         retain_depth = 0 — this port can never activate its component and never carries context \
         (dead config); set push_depth > 0 to make it triggering, or retain_depth > 0 to make it \
         a sampled/context-only port",
    );
}

/// Refuse to start when a component instance has input bindings but not one that
/// can ever activate it.
///
/// The instance grain, not the surface's: activations are minted per instance,
/// so a surface with one triggering component says nothing about a sibling whose
/// every port is context-only. `resolve_wasm_consumers` makes the same check at
/// its own principal's grain.
///
/// An instance with *no* input bindings is untouched — a purely presentational
/// component is live config. It still publishes: a gesture on it causes an
/// activation of its own, which is where its publishes are made.
///
/// # Panics
///
/// When every one of the instance's input bindings resolves to `push_depth = 0`.
pub(super) fn assert_instance_can_activate(slug: &str, instance: &str, push_depths: &[u64]) {
    assert!(
        push_depths.is_empty() || push_depths.iter().any(|d| *d >= 1),
        "config: [[surface]] {slug:?}: component {instance:?} has {} input binding(s), all with \
         push_depth = 0 (sampled/context-only) — this component can never activate, so its \
         context windows are never read; at least one of its bindings must have push_depth > 0",
        push_depths.len(),
    );
}

/// Resolve a declared component's `abi` — which artifact shape backs it, and so
/// how the shell loads it.
///
/// `dom` (wasm-bindgen module) and `processor` (jco-transpiled component-model
/// artifact) both load. `dom-ts`/`html` are reserved names and are named boot
/// panics rather than values that resolve and then fail somewhere less obvious.
/// The message distinguishes the two cases, because they are different operator
/// mistakes: a defined-but-unsupported ABI is early, an unknown string is a typo
/// or a config written against something that does not exist.
fn resolve_abi(slug: &str, instance: &str, abi: &str) -> Abi {
    let Some(parsed) = Abi::parse(abi) else {
        panic!(
            "config: [[surface]] {slug:?}: component {instance:?} declares abi = {abi:?}, which \
             names no component ABI. Known: {}",
            Abi::ALL.map(Abi::as_str).join(", "),
        )
    };
    assert!(
        matches!(parsed, Abi::Dom | Abi::Processor),
        "config: [[surface]] {slug:?}: component {instance:?} declares abi = {abi:?}, which is \
         reserved but not yet supported — the shell loads abi = \"dom\" and abi = \"processor\" \
         today. The name is reserved so this config keeps its meaning when the loader learns it; \
         it is rejected now rather than half-honoured.",
    );
    parsed
}

/// Assert one resolved backstop burst covers a maximal conforming activation
/// flush.
///
/// The invariant, quantified over every server-side bucket **drawn in
/// whole-publish units against a flush's entries**: burst >=
/// `MAX_PUBLISHES_PER_ACTIVATION`. The kernel meters a flush at buffer time and
/// refuses the publish that would cross the cap, so a flush arriving at the
/// backstop is at most that wide; a burst below it refuses truthful traffic
/// forever, since admission is sufficiency and refill clamps at capacity. An
/// operator who wants an instance slower tunes `send_refill_secs`, which is the
/// knob that means "sustained rate".
fn assert_backstop_covers_a_maximal_flush(burst: u32, context: &str) {
    let cap =
        u32::try_from(MAX_PUBLISHES_PER_ACTIVATION).expect("MAX_PUBLISHES_PER_ACTIVATION fits u32");
    assert!(
        burst >= cap,
        "config: {context} resolves a send-budget burst of {burst}, below the \
         {cap}-publish per-activation cap. This bucket is drawn whole against an activation \
         flush's entries, so a burst under the cap refuses a maximal conforming flush every \
         time — and refill clamps at capacity, so it never becomes admissible. Set send_burst \
         >= {cap}; tune send_refill_secs for a slower sustained rate.",
    );
}

/// Resolve a declared component's send budget: its own overrides layered on the
/// defaults.
///
/// One knob per constant it replaces, and each is rejected at the value that
/// would make the bucket meaningless rather than silently substituted: a burst
/// of 0 admits no publish at all (an instance that may never speak is dead
/// config, not a rate limit), and a refill of 0 seconds is a bucket that never
/// binds — and would divide by zero in `TokenBucket`. An operator who wants an
/// instance silent removes its output bindings; one who wants it unmetered has
/// no such option by design.
fn resolve_send_budget(slug: &str, instance: &str, comp: &SurfaceComponentRaw) -> AttachSendBudget {
    let default = AttachSendBudget::default();
    let burst = comp.send_burst.unwrap_or(default.burst);
    assert!(
        burst >= 1,
        "config: [[surface]] {slug:?}: component {instance:?} sets send_burst = 0, which admits no \
         publish at all — a permanently-silent principal is dead config, not a budget. Remove the \
         instance's output bindings to silence it, or set send_burst >= 1.",
    );
    assert_backstop_covers_a_maximal_flush(
        burst,
        &format!("[[surface]] {slug:?}: component {instance:?}"),
    );
    let refill_secs = comp.send_refill_secs.unwrap_or(default.refill.as_secs());
    assert!(
        refill_secs >= 1,
        "config: [[surface]] {slug:?}: component {instance:?} sets send_refill_secs = 0, which is \
         a budget that never refills against the clock — i.e. no budget. Every \
         component-identity publish is metered by design; set send_refill_secs >= 1.",
    );
    AttachSendBudget {
        burst,
        refill: Duration::from_secs(refill_secs),
    }
}

/// Resolve one output binding's per-activation sink budget to millitokens.
///
/// The knobs, their spelling, their defaults, and their validation are
/// `[[wasm_consumer.output]]`'s — shared resolver, so the operator meets one
/// vocabulary on both blocks and a component moved between hostings keeps its
/// budget. The numbers ride the bindings document; the kernel enforces them
/// because the kernel is what mints this component's activations.
fn resolve_output_budget(slug: &str, out: &SurfaceOutputRaw) -> brenn_budget::SinkBudget {
    let field = |knob: &str| {
        format!(
            "config: [[surface]] {slug:?}: output instance {:?} port {:?} {knob}",
            out.instance, out.port
        )
    };
    brenn_budget::SinkBudget {
        fill_mt: resolve_publish_millitokens(
            out.publish_per_activation,
            DEFAULT_WASM_PUBLISH_PER_ACTIVATION,
            &field("publish_per_activation"),
        ),
        capacity_mt: resolve_publish_millitokens(
            out.publish_capacity,
            DEFAULT_WASM_PUBLISH_CAPACITY,
            &field("publish_capacity"),
        ),
    }
}

/// Resolve one transportable binding's retained-context depth: how many retained
/// messages precede `new_from` when the kernel windows this port, and half of
/// the bound on every replay the wire serves that subscription.
///
/// The same shape [`resolve_page_queue_depth`] takes for the port queue, on the
/// other knob: binding → the `[[channel]]` rung the caller passes as
/// `channel_default`. One ladder for every transportable channel — the store
/// holding the channel's retention has no say in it.
///
/// # Panics
///
/// On a depth that resolves unbounded: the retained ring is page memory, the
/// same rule the port queue takes, and every replay the wire serves must be
/// bounded. No default is invented — the operator states the depth on the
/// binding or on the channel rung.
fn resolve_context_ring_depth(
    slug: &str,
    context: &str,
    channel: &str,
    retain_depth: Option<Depth>,
    channel_default: Depth,
) -> u64 {
    let resolved = retain_depth.unwrap_or(channel_default);
    let Depth::Bounded(n) = resolved else {
        panic!(
            "config: [[surface]] {slug:?}: {context} channel {channel:?} resolves to retain_depth \
             = {resolved:?} — a binding's retained context ring lives in page memory and its \
             replays must be bounded; set it on the binding or on the channel rung"
        )
    };
    n
}

/// Resolve a declared component's parked-batch depth: how many activation
/// flushes the kernel holds for it across a disconnect before dropping the
/// oldest whole batch.
///
/// # Panics
///
/// On `0` (every offline flush dropped on arrival — an instance that can never
/// land work from a disconnected activation is dead config, not a bound) and on
/// unbounded (the parked queue is page memory, the same rule every other page
/// queue takes; "unbounded" is a tab that grows for the length of the outage).
fn resolve_parked_batch_depth(slug: &str, instance: &str, comp: &SurfaceComponentRaw) -> u64 {
    let resolved = comp
        .parked_batch_depth
        .unwrap_or(Depth::Bounded(DEFAULT_PARKED_BATCH_DEPTH));
    let Depth::Bounded(n) = resolved else {
        panic!(
            "config: [[surface]] {slug:?}: component {instance:?} sets parked_batch_depth = \
             \"unbounded\" — the kernel parks an instance's activation flushes in page memory \
             while the link is down, so the queue must be bounded; give it a number"
        )
    };
    assert!(
        n >= 1,
        "config: [[surface]] {slug:?}: component {instance:?} sets parked_batch_depth = 0, which \
         drops every flush an activation makes while the link is down — an instance that can \
         never land offline work is dead config, not a bound. Set parked_batch_depth >= 1.",
    );
    n
}

/// Resolve a component instance's static config map — the page-lifetime
/// analogue of the backend's process-lifetime map seeded from host TOML and read
/// through the `config` import.
///
/// Only a `processor` reads it: no other ABI is handed a `config` import, so a
/// map declared elsewhere has no reader. Absent means the empty map — an
/// operator need not write `config = {}`.
///
/// # Panics
///
/// On a `config` table declared for a non-`processor` component (a dead
/// declaration is a config error, not a silent no-op) and on any key in the
/// host-reserved `brenn.` namespace (a collision-in-waiting or a typo; the host
/// injects none browser-side).
fn resolve_component_config(
    slug: &str,
    instance: &str,
    abi: Abi,
    comp: &SurfaceComponentRaw,
) -> BTreeMap<String, String> {
    let Some(map) = comp.config.as_ref() else {
        return BTreeMap::new();
    };
    assert!(
        abi == Abi::Processor,
        "config: [[surface]] {slug:?}: component {instance:?} declares a `config` table but its \
         abi is {abi:?} — only a `processor` component is handed a `config` import, so this map \
         would have no reader; remove it or declare the component as abi = \"processor\"",
    );
    for key in map.keys() {
        assert!(
            !key.starts_with("brenn."),
            "config: [[surface]] {slug:?}: component {instance:?} declares config key {key:?} — \
             the `brenn.` prefix is the host-reserved namespace and is not an operator's to set; \
             rename the key",
        );
    }
    map.clone()
}

/// Per-binding overrides a surface input binding may layer on the channel's
/// resolved rungs (`None` = inherit the channel value).
struct SubOverrides {
    push_depth: Option<Depth>,
    retain_depth: Option<Depth>,
    noise: Option<NoiseLevel>,
}

/// Resolve one transportable channel a surface binds (a
/// `[[surface.subscription]]`) into the wire [`ResolvedSubscription`] the
/// directory registers and the bridge reads.
///
/// One resolver for every transportable input binding: one directory lookup,
/// one ladder per knob (binding → the channel's own rung), one boundedness
/// rule, one gate set. Which store holds the channel's retention is not a
/// question asked here — the entry answers every
/// knob the same way whichever it is, and a second per-class arm is how the two
/// drift apart.
///
/// `context` labels the binding in panic messages. Returns the channel uuid
/// alongside the resolution so the caller runs its own per-path dedup, plus the
/// **bounded** `push_depth` and `retain_depth` this function proved: the
/// numbers, not the `Depth`s, so a caller needing the page-side capacities
/// carries the proof in the type instead of re-destructuring invariants this
/// function owns. What each depth is allowed to be beyond bounded is the
/// caller's call ([`assert_page_queue_deliverable`]), because it turns on the
/// binding's delivery model, which this resolver cannot see.
///
/// # Panics
///
/// On an unknown channel, on a channel whose own `retain_depth` resolves 0 (it
/// would strand the subscription permanently), or on a depth that resolves
/// unbounded — all boot-time config errors.
fn resolve_wire_subscription(
    slug: &str,
    context: &str,
    channel: &str,
    directory: &MessagingDirectory,
    overrides: SubOverrides,
) -> (uuid::Uuid, ResolvedSubscription, u64, u64) {
    let entry = directory.resolve(channel).unwrap_or_else(|| {
        panic!(
            "config: [[surface]] {slug:?}: {context} channel {channel:?} names no declared \
             [[channel]]"
        )
    });
    let ch = &entry.resolved_channel;
    // The delivery model's one precondition on the channel itself: a surface
    // subscription holds its position on the connection and recovers it by
    // re-reading the channel's retention, so a channel that retains nothing
    // strands the subscription the moment it misses a message — every later row
    // arrives non-contiguous, every drain reads an empty window, and the
    // position never moves again. Silent permanent non-delivery is the wrong
    // thing, so the config is refused here instead.
    assert!(
        ch.retain_depth != Depth::Bounded(0),
        "config: [[surface]] {slug:?}: {context} channel {channel:?} resolves to a channel-level \
         retain_depth = 0. A surface subscription's delivery position is recovered by re-reading \
         the channel's retention, so a channel that retains nothing strands the subscription \
         permanently after its first missed message, with nothing on the wire to say so. Set \
         retain_depth >= 1 on the [[channel]] block",
    );
    let push = resolve_page_queue_depth(
        slug,
        context,
        channel,
        overrides.push_depth,
        Some(ch.push_depth),
    );
    let retain = resolve_context_ring_depth(
        slug,
        context,
        channel,
        overrides.retain_depth,
        ch.retain_depth,
    );
    // wake_min is meaningless on a surface subscription (always delivered
    // eagerly), rejected class-blind at the one call site in `resolve_surfaces`
    // before this resolver is reached.
    (
        entry.uuid,
        ResolvedSubscription {
            channel_uuid: entry.uuid,
            channel_address: entry.address.clone(),
            push_depth: Depth::Bounded(push),
            retain_depth: Depth::Bounded(retain),
            noise: overrides.noise.unwrap_or(ch.noise),
            wake_min: ch.wake_min,
        },
        push,
        retain,
    )
}

/// Resolve `[[surface]]` blocks against the channel directory, applying
/// boot-time cross-validation. Mirrors `resolve_wasm_consumers` in placement,
/// shape, and panic style — including its directory, whose entry set covers
/// every declared channel, durable or not, so one lookup answers every binding.
///
/// Returns `Vec<ResolvedSurface>` in declaration order. The resolved surfaces
/// are carried on `MessagingResult` for later consumers; boot wires them no
/// further than boot validation + the boot observability log.
///
/// # Panics (all operator-authored config — fail-fast)
///
/// 1. `slug` empty / non-unreserved charset / duplicate across `[[surface]]`
///    blocks. The unreserved charset excludes `:`/`@`/`#`, so a config slug
///    satisfies `ParticipantId::for_surface`'s asserts by construction — this
///    *enforces* the charset a wasm-consumer slug only documents.
/// 2. `component.kind` empty / not matching `^[a-z0-9][a-z0-9-]*$` (the
///    tightened charset makes the kind a valid custom-element name
///    (`brenn-<kind>`) and module filename); the resolved `instance` id
///    (defaults to `kind`) not matching that charset, or duplicate within the
///    surface. Instances — not kinds — are unique per surface: one kind may
///    back several instances.
/// 3. A subscription/output `channel` whose scheme is none of `brenn:`,
///    `ephemeral:`, `local:` — this is the boot-time scheme restriction that
///    makes the `WakeRouter::deliver_ingress` `Surface` panic arm structurally
///    unreachable.
/// 4. A transportable binding channel absent from `directory`, or one whose
///    entry wears a transport its address does not name.
/// 5. A binding naming an undeclared instance; an empty / non-unreserved port;
///    a duplicate `(instance, port)` within subscriptions (or within outputs).
/// 6. A binding the surface's own resolved policy does not authorize
///    (`allows_channel_access` for subscriptions; `allows_brenn_publish` /
///    `allows_ephemeral_publish` by scheme for outputs) — dead config.
/// 7. A surface with zero `[[surface.component]]` blocks — dead config: every
///    binding must name a declared component (item 5) and subscriptions are
///    static-only, so a component-less surface can never carry traffic.
/// 8. An input binding whose channel retains nothing
///    ([`resolve_wire_subscription`]), or whose resolved depths are not both
///    bounded.
///
/// Two adjacent cases deliberately do **not** panic (recorded design
/// decisions): a declared component with no port bindings (a purely presentational
/// component is live config), and an `ephemeral:` `[[channel]]` referenced by no
/// surface binding (ephemeral channels are LLM-app publish targets independently
/// of any surface).
pub(crate) fn resolve_surfaces(
    raw_surfaces: &[SurfaceConfigRaw],
    directory: &MessagingDirectory,
    globals: &MessagingGlobalConfig,
    auto_wiring: &AutoWiring,
) -> Vec<ResolvedSurface> {
    use brenn_lib::messaging::config::{
        DEFAULT_SURFACE_PUBLISH_BURST, DEFAULT_SURFACE_PUBLISH_PER_SEC,
    };
    use brenn_lib::messaging::{ChannelScheme, is_unreserved_char};
    use std::collections::HashSet;

    // Item 1: slug uniqueness across [[surface]] blocks.
    let mut seen_slugs: HashSet<&str> = HashSet::new();
    for s in raw_surfaces {
        assert!(
            seen_slugs.insert(s.slug.as_str()),
            "config: duplicate [[surface]] slug {:?} — each surface slug must be unique",
            s.slug,
        );
    }

    let mut result = Vec::with_capacity(raw_surfaces.len());
    for surface in raw_surfaces {
        let slug = &surface.slug;

        // Item 1: slug charset (defense in depth for non-config callers; the
        // config path enforces it here, `for_surface` re-asserts it downstream).
        assert!(
            !slug.is_empty(),
            "config: [[surface]] slug must be non-empty",
        );
        assert!(
            slug.chars().all(is_unreserved_char),
            "config: [[surface]] slug {slug:?} must consist of RFC 3986 unreserved \
             characters only (A-Za-z0-9._~-)",
        );

        // Item 7: zero components is dead config.
        assert!(
            !surface.components.is_empty(),
            "config: [[surface]] {slug:?} declares no [[surface.component]] blocks — a \
             component-less surface can never carry traffic (every binding must name a \
             declared component and subscriptions are static-only); dead config",
        );

        // Item 2: component kind + instance resolution. The kind becomes a
        // custom-element name (`brenn-<kind>`) and a module filename
        // (`brenn_<kind>.js`), so it is tightened beyond the general unreserved
        // charset to the `^[a-z0-9][a-z0-9-]*$` rule owned by
        // `contract::is_valid_kind` (lowercase ASCII per the PCEN custom-element
        // grammar; no leading `-`; no `--` run, reserved as the instance-tag
        // separator in `element_name_for_instance`). The instance id (the routing/mount key that
        // bindings reference) defaults to the kind and shares its charset;
        // instances — not kinds — must be unique within the surface, so one kind
        // may back several instances (one wasm module, N elements).
        let mut instances: HashSet<&str> = HashSet::new();
        let mut resolved_components: Vec<ResolvedComponent> =
            Vec::with_capacity(surface.components.len());
        for comp in &surface.components {
            assert!(
                !comp.kind.is_empty(),
                "config: [[surface]] {slug:?}: [[surface.component]] kind must be non-empty",
            );
            assert!(
                brenn_surface_contract::is_valid_kind(&comp.kind),
                "config: [[surface]] {slug:?}: component kind {:?} must match \
                 ^[a-z0-9][a-z0-9-]*$ (lowercase ASCII, digits, and hyphens; no \
                 leading hyphen) and must not contain consecutive hyphens (`--` is \
                 reserved as the instance-tag separator) — it becomes a \
                 custom-element name and module filename",
                comp.kind,
            );
            let instance = comp.instance.as_deref().unwrap_or(&comp.kind);
            assert!(
                brenn_surface_contract::is_valid_kind(instance),
                "config: [[surface]] {slug:?}: component instance {instance:?} must match \
                 ^[a-z0-9][a-z0-9-]*$ (same charset as kind) and must not contain \
                 consecutive hyphens (`--` is reserved as the instance-tag separator) — \
                 it is the routing/mount key",
            );
            assert!(
                instances.insert(instance),
                "config: [[surface]] {slug:?}: duplicate component instance {instance:?} — \
                 instance ids must be unique within a surface (a kind may repeat, an instance \
                 may not)",
            );
            let abi = resolve_abi(slug, instance, &comp.abi);
            resolved_components.push(ResolvedComponent {
                instance: instance.to_string(),
                kind: comp.kind.clone(),
                abi,
                config: resolve_component_config(slug, instance, abi, comp),
                send_budget: resolve_send_budget(slug, instance, comp),
                parked_batch_depth: resolve_parked_batch_depth(slug, instance, comp),
                chrome: comp.chrome,
            });
        }

        // Chrome singleton: exactly one component per surface carries the
        // privileged rendering authority (layout/theme/banner/takeover/toast).
        // Zero leaves the surface with no chrome and the wire's
        // `chrome_instance` unfillable; two or more make the designation
        // ambiguous and the `String` wire field unrepresentable-right. Both are
        // config typos the boot must refuse by name rather than mis-wire.
        let chrome_count = resolved_components.iter().filter(|c| c.chrome).count();
        assert!(
            chrome_count == 1,
            "config: [[surface]] {slug:?} declares {chrome_count} components with `chrome = true` \
             — exactly one component per surface must be the chrome (the privileged \
             layout/theme/banner/takeover renderer); {}",
            if chrome_count == 0 {
                "add `chrome = true` to exactly one [[surface.component]]"
            } else {
                "set `chrome = true` on exactly one [[surface.component]] and remove it from the others"
            },
        );

        // Access check: no empty usernames, no duplicates.
        let mut seen_users: HashSet<&str> = HashSet::new();
        for user in &surface.allowed_users {
            assert!(
                !user.is_empty(),
                "config: [[surface]] {slug:?}: allowed_users entry must be non-empty",
            );
            assert!(
                seen_users.insert(user.as_str()),
                "config: [[surface]] {slug:?}: duplicate allowed_users entry {user:?}",
            );
        }

        // Publish token-bucket caps. Floor: both must be >= 1 when present (a
        // surface with publish grants and a zero budget is a config
        // contradiction; a surface that shouldn't publish simply omits the
        // grants and outputs). Ceiling: neither may exceed the global default
        // send rate, so the per-connection bucket trips no later than the
        // substrate's per-(sender, channel) gate for that connection — the
        // documented "connection bucket trips first" layering. Equality is safe:
        // equal-sized buckets both start full, so the connection bucket is never
        // more permissive.
        //
        // Checked against the global default rather than each channel's
        // resolved rate: a channel may override *downward*, and a surface
        // publishing to such a channel trips the substrate gate first. That is
        // the operator asking for a tighter bound on that channel, not a
        // layering violation.
        //
        // This is a single-connection guard, not defense in depth. The aggregate
        // across all sessions/users of a surface shares the one surface:<slug>
        // participant and its send-rate gate — shared-fate: N sessions can still
        // jointly drain it. Those aggregate bounds are recorded design
        // decisions, not gaps this check closes.
        //
        // The per-second comparison below is unit-valid only while the refill
        // interval is one second.
        let send_rate = globals.default_send_rate;
        assert_eq!(
            send_rate.refill_interval_secs, 1,
            "config: [messaging].default_send_rate.refill_interval_secs must be 1 — the \
             per-surface publish_per_sec ceiling is compared against `refill` as a per-second \
             rate",
        );
        let publish_burst = surface
            .publish_burst
            .unwrap_or(DEFAULT_SURFACE_PUBLISH_BURST);
        let publish_per_sec = surface
            .publish_per_sec
            .unwrap_or(DEFAULT_SURFACE_PUBLISH_PER_SEC);
        assert!(
            publish_burst >= 1,
            "config: [[surface]] {slug:?}: publish_burst must be >= 1 (a zero budget with \
             publish grants is a contradiction; omit the grants instead)",
        );
        assert!(
            publish_per_sec >= 1,
            "config: [[surface]] {slug:?}: publish_per_sec must be >= 1 (a zero budget with \
             publish grants is a contradiction; omit the grants instead)",
        );
        assert!(
            publish_burst <= send_rate.burst,
            "config: [[surface]] {slug:?}: publish_burst {publish_burst} exceeds the default \
             send-rate burst ({}); the per-connection bucket must trip first — see the \
             shared-fate note on the publish budget block",
            send_rate.burst,
        );
        assert!(
            publish_per_sec <= send_rate.refill,
            "config: [[surface]] {slug:?}: publish_per_sec {publish_per_sec} exceeds the default \
             send rate ({}/s); the per-connection bucket must trip first — see the shared-fate \
             note on the publish budget block",
            send_rate.refill,
        );

        // Skin (CSS pack + fonts): default `bench`, validated against the
        // compiled-in registry. An unknown skin is dead config — the page handler
        // would emit a `<link>` to a nonexistent stylesheet — so panic at boot.
        let skin = surface
            .skin
            .clone()
            .unwrap_or_else(|| crate::routes::surface::DEFAULT_SKIN.to_string());
        assert!(
            crate::routes::surface::skin_stylesheet_path(&skin).is_some(),
            "config: [[surface]] {slug:?}: unknown skin {skin:?} — not in the compiled-in skin \
             registry (known skins: {:?})",
            crate::routes::surface::SKIN_REGISTRY
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>(),
        );

        // Build the surface's resolved policy up front — the binding
        // coverage check (item 6) consults it.
        let mut policy = brenn_lib::access::resolve::build_surface_policy(
            slug,
            surface.grants.iter().copied(),
            &surface.subscribe_acl,
            &surface.publish_acl,
            &surface.ephemeral_subscribe_acl,
            &surface.ephemeral_publish_acl,
        );
        // Auto-channel grants, injected before the binding coverage checks read
        // the policy: a `[[connection]]` is the operator's authorization signal,
        // so an endpoint needs no authored ACL for the channel it wires.
        auto_wiring.inject_surface_grants(slug, &mut policy);
        let policy = policy;

        // Items 3–5 shared by both binding directions: component must be
        // declared, port charset valid, channel scheme restricted to
        // brenn:/ephemeral:/local: and the referenced channel must exist and
        // wear the transport its address names. Returns `()`; each caller
        // re-derives the binding's scheme for its own coverage check (item 6)
        // after this validation has guaranteed the scheme is one of the three
        // accepted ones.
        let validate_binding = |direction: &str, channel: &str, instance: &str, port: &str| {
            // Item 5: instance must be declared on this surface.
            assert!(
                instances.contains(instance),
                "config: [[surface]] {slug:?}: {direction} names instance {instance:?} which \
                 is not declared as a [[surface.component]] on this surface",
            );
            // Item 5: port charset.
            assert!(
                !port.is_empty(),
                "config: [[surface]] {slug:?}: {direction} port name must be non-empty",
            );
            assert!(
                port.chars().all(is_unreserved_char),
                "config: [[surface]] {slug:?}: {direction} port name {port:?} must consist of \
                 RFC 3986 unreserved characters only (A-Za-z0-9._~-)",
            );
            // Items 3 + 4: scheme restriction and channel existence.
            match ChannelScheme::split(channel) {
                // Both transportable schemes take one lookup in the one
                // directory. The transport check is defense-in-depth for the
                // "surfaces off ingress paths" guarantee: an address must
                // resolve to a channel of its own transport, never an
                // ingress-fed one wearing a `brenn:` name — if a future change
                // ever ingress-feeds one, a surface bound to it fails here at
                // boot rather than reaching the permanent `deliver_ingress`
                // Surface panic at runtime.
                Some((scheme @ (ChannelScheme::Ephemeral | ChannelScheme::Brenn), _)) => {
                    let entry = directory.resolve(channel).unwrap_or_else(|| {
                        panic!(
                            "config: [[surface]] {slug:?}: {direction} channel {channel:?} names \
                             no declared [[channel]]"
                        )
                    });
                    assert!(
                        entry.transport_type == scheme,
                        "config: [[surface]] {slug:?}: {direction} channel {channel:?} resolves \
                         to a {:?}-transport channel — a surface binding's address names the \
                         transport it binds (the scheme restriction that keeps surfaces off \
                         ingress paths)",
                        entry.transport_type,
                    );
                }
                // `local:` — page-local pub/sub. There is nothing to look up:
                // local channels are declared per-surface (the binding *is* the
                // declaration), so the directory has no opinion. What must be
                // checked is the name, since no other validator ever sees it.
                Some((ChannelScheme::Local, name)) => {
                    assert!(
                        !name.is_empty(),
                        "config: [[surface]] {slug:?}: {direction} channel {channel:?} names an \
                         empty local channel",
                    );
                    if brenn_surface_schema::is_reserved_local_namespace(channel) {
                        // Reserved namespace: the name must be one the contract
                        // actually defines. `local:brenn/nonesuch` is reserved
                        // (an operator cannot declare it) but undefined — a
                        // typo'd control plane that would otherwise route as an
                        // ordinary channel and silently never reach chrome.
                        assert!(
                            brenn_surface_schema::reserved_local_channel(channel).is_some(),
                            "config: [[surface]] {slug:?}: {direction} channel {channel:?} is in \
                             the reserved local:brenn/ namespace but names no control channel the \
                             contract defines ({:?})",
                            brenn_surface_schema::RESERVED_LOCAL_CHANNELS
                                .iter()
                                .map(|c| c.address)
                                .collect::<Vec<_>>(),
                        );
                    } else {
                        // An operator-declared local channel wears the same
                        // charset as every other channel name — which is what
                        // keeps the reserved namespace reserved: `/` is outside
                        // the set, so no declared name can ever collide with a
                        // `local:brenn/*` one.
                        assert!(
                            name.chars().all(is_unreserved_char),
                            "config: [[surface]] {slug:?}: {direction} channel {channel:?} must \
                             consist of RFC 3986 unreserved characters only (A-Za-z0-9._~-) \
                             after the local: prefix",
                        );
                    }
                }
                Some((
                    ChannelScheme::Mqtt | ChannelScheme::Webhook | ChannelScheme::PwaPush,
                    _,
                ))
                | None => panic!(
                    "config: [[surface]] {slug:?}: {direction} channel {channel:?} must be a \
                     brenn:, ephemeral:, or local: address — surfaces bind only those three \
                     schemes (the scheme restriction that keeps surfaces off ingress paths)",
                ),
            }
        };

        // The reserved-control-plane rules for a `local:brenn/*` binding, shared
        // by both directions. Ordinary operator-declared
        // local channels have no rules here: the server mediates no access to
        // page-local traffic, so it polices the *declaration* and nothing else.
        let validate_local_binding = |direction: &str, channel: &str, is_output: bool| {
            let Some(reserved) = brenn_surface_schema::reserved_local_channel(channel) else {
                return;
            };
            // Capability-as-binding: the takeover grant gates the wiring itself,
            // replacing the v0 runtime DOM-event gate. A surface without the
            // grant declaring a takeover binding is dead config — the component
            // could publish takeover requests no one is allowed to honour.
            assert!(
                !reserved.requires_takeover_grant
                    || policy
                        .grants
                        .has(brenn_lib::access::AppCapability::SurfaceTakeover),
                "config: [[surface]] {slug:?}: {direction} binds reserved control channel \
                 {channel:?}, which requires the surface's `takeover` grant — add it to \
                 [[surface]] grants or drop the binding",
            );
            // Kernel-publish-only planes have no component producers in v1. A
            // component output bound here would publish into a plane the kernel
            // owns and overwrite the kernel's own state reports.
            assert!(
                !(is_output && reserved.kernel_publish_only),
                "config: [[surface]] {slug:?}: [[surface.output]] targets reserved control \
                 channel {channel:?}, which is kernel-publish-only — only the surface kernel \
                 publishes link-state/surface-state/toast; components may subscribe it",
            );
        };

        // Both halves of each io_port are channel-less and take the one address
        // the lowering pass assigned — the two directions cannot be wired apart.
        let (io_subscriptions, io_outputs) = super::auto::surface_io_bindings(&surface.io_ports);

        // Ring depth per declared `local:` channel, accumulated across bindings:
        // reserved channels take the contract-fixed depth, operator-declared
        // channels the max over their bindings' `retain_depth` (floor 1). First
        // insertion order is preserved so the bindings document lists them
        // predictably.
        let mut local_ring_depths: IndexMap<String, u64> = IndexMap::new();

        // Subscriptions (input bindings): item 6 coverage is `allows_channel_access`
        // over the full address. Every *transportable* binding additionally
        // resolves a `ResolvedSubscription` (depth/noise/wake inheritance) that
        // folds into the surface's `SubscriberEntryKind::Surface` directory entry
        // for that channel — the one subscriber shape the live feed resolves,
        // whichever store holds the channel's retention.
        let mut subscriptions =
            Vec::with_capacity(surface.subscriptions.len() + io_subscriptions.len());
        let mut wire_subscriptions: Vec<ResolvedSurfaceSubscription> = Vec::new();
        let mut seen_sub_ports: HashSet<(&str, &str)> = HashSet::new();
        // One resolved subscription per **(instance, channel)**: each
        // component's depths and noise on a channel are resolved independently.
        //
        // Index into `wire_subscriptions`, so a repeated (instance, channel) —
        // one instance binding one channel on two ports — folds into the entry
        // already resolved rather than resolving a second one.
        let mut seen_wire: std::collections::HashMap<(String, uuid::Uuid), usize> =
            std::collections::HashMap::new();
        for (sub, direction) in surface
            .subscriptions
            .iter()
            .map(|sub| (sub, "subscription"))
            .chain(io_subscriptions.iter().map(|sub| (sub, "io_port")))
        {
            let channel = super::bound_channel(
                &format!("[[surface]] {slug:?}"),
                &format!(
                    "{direction} instance {:?} port {:?}",
                    sub.instance, sub.port
                ),
                sub.channel.as_deref(),
                auto_wiring.surface_channel(slug, &sub.instance, &sub.port),
            );
            validate_binding(direction, &channel, &sub.instance, &sub.port);
            assert!(
                seen_sub_ports.insert((sub.instance.as_str(), sub.port.as_str())),
                "config: [[surface]] {slug:?}: duplicate {direction} binding for instance \
                 {:?} port {:?} — one binding per (instance, port) direction",
                sub.instance,
                sub.port,
            );
            // `local:` traffic never reaches the server, so there is no delivery
            // for a policy to authorize — the server's ACLs speak about the bus,
            // and a page-local channel is not on it. (`allows_channel_access`
            // denies every `local:` address for exactly that reason, so asking it
            // here would reject all local bindings.) Enforcement for local
            // traffic is kernel-side = bug containment, acceptable because the
            // blast radius is the page; what the server polices is the binding
            // declaration, which is `validate_binding` plus the reserved-channel
            // rules below.
            let local = brenn_envelope::is_local_channel(&channel);
            assert!(
                local || policy.allows_channel_access(&channel),
                "config: [[surface]] {slug:?}: {direction} binds channel {:?} but the \
                 surface's access policy does not authorize delivery there (missing transport \
                 grant and/or a covering ACL matcher) — dead config",
                channel,
            );

            // `wake_min` is rejected on every surface binding, class-blind: a
            // surface subscription is always delivered eagerly (its registration
            // is `Eager`), so `wake_min` — which gates whether a publish wakes a
            // *parked* subscriber — can never have a referent here, whatever the
            // channel's class. One text for all three schemes.
            assert!(
                sub.wake_min.is_none(),
                "config: [[surface]] {slug:?}: {direction} on channel {:?} sets wake_min, but \
                 surface subscriptions are always delivered eagerly — wake_min gates parked \
                 dispatch and has no referent on a surface binding of any class; remove it",
                channel,
            );

            // The label every depth resolver and gate below puts in its panic
            // text. It names the binding the way config spells it: the
            // direction, because an io_port half is not a
            // `[[surface.subscription]]` and telling an operator to go look for
            // one sends them hunting a block they never wrote; plus instance and
            // port, which for a lowered binding is the only handle they have —
            // its channel is a computed `auto.` address nothing in their config
            // names.
            let context = format!(
                "{}{direction} instance {:?} port {:?}",
                if local { "local " } else { "" },
                sub.instance,
                sub.port,
            );
            // Every binding resolves the same three page-side facts —
            // (push_depth, retain_depth, noise) — down one binding → channel
            // ladder. The one dispatch here is transportability, the one
            // channel characteristic the wire ranks: a transportable binding
            // installs a wire subscription and reads its rungs off the channel
            // directory; a `local:` one never leaves the page and has no
            // `[[channel]]` block to read. `push_depth` puts a bounded page
            // queue in front of the port; `retain_depth` a bounded retained ring
            // behind it; `noise` is resolved and held for the overflow ladder
            // that lands in a later phase — no surface path reads it yet.
            let (push_depth, retain_depth, noise, wire) = if local {
                validate_local_binding(direction, &channel, false);
                // A `local:` channel has no `[[channel]]` block, so its noise
                // ladder is binding → global. `push_depth` has no such fallback:
                // a depth is sized, not defaulted, so the binding states it. The
                // kernel enacts the resolved noise rung on overflow.
                accumulate_local_ring_depth(
                    slug,
                    &mut local_ring_depths,
                    &channel,
                    sub.retain_depth,
                );
                (
                    resolve_page_queue_depth(slug, &context, &channel, sub.push_depth, None),
                    // The router's ring is the context source for a `local:`
                    // channel, so this binding's own number is only how deep it
                    // reads into that ring — the ring itself is the max fold
                    // `accumulate_local_ring_depth` just took, and a reserved
                    // plane's is contract-fixed. A binding that states nothing
                    // still reads 1: the ring's floor is 1 and the planes' whole
                    // point is last-value replay on attach, so 0 here would make
                    // every unstated binding blind to a ring the kernel is
                    // already keeping.
                    local_context_depth(&channel, sub.retain_depth),
                    sub.noise.unwrap_or(globals.default_noise),
                    // Page-local traffic never crosses the wire, so it installs
                    // no subscriber anywhere on the bus.
                    None,
                )
            } else {
                // Transportable — validate_binding guaranteed the channel is
                // declared and wears its own transport. The resolver hands back
                // the bounded push_depth and retain_depth it proved; those
                // numbers are this port's page-side queue capacity and
                // context-window depth, always — both are the port's own, so
                // both take this binding's numbers. Only the *subscription* is
                // ever shared, and only between bindings of the same instance,
                // where its window and ring fold to the max below.
                let (channel_uuid, resolved, page_depth, context_depth) = resolve_wire_subscription(
                    slug,
                    &context,
                    &channel,
                    directory,
                    SubOverrides {
                        push_depth: sub.push_depth,
                        retain_depth: sub.retain_depth,
                        noise: sub.noise,
                    },
                );
                // Held for the binding below before `resolved` is folded/moved
                // into `wire_subscriptions` — the same resolved value the
                // directory subscriber entry already carries unread.
                let resolved_noise = resolved.noise;
                (
                    page_depth,
                    context_depth,
                    resolved_noise,
                    Some((channel_uuid, resolved)),
                )
            };

            // One fold for both transportable classes: a repeated (instance,
            // channel) is one instance binding one channel on two ports — the
            // only case where a surface subscription is genuinely shared.
            if let Some((channel_uuid, resolved)) = wire {
                let key = (sub.instance.clone(), channel_uuid);
                match seen_wire.get(&key) {
                    Some(&idx) => {
                        // Fold rather than let one binding's number win — which
                        // binding "wins" would otherwise depend on config order,
                        // and depth is a capacity, so the fold with meaning is
                        // max: the window must cover the hungriest port on it or
                        // that port starves. Same fold `reap_frontier` already
                        // applies across subscribers, and the local-ring resolver
                        // applies across bindings.
                        let shared: &mut ResolvedSubscription =
                            &mut wire_subscriptions[idx].subscription;
                        shared.push_depth = shared.push_depth.widened_by(resolved.push_depth);
                        shared.retain_depth = shared.retain_depth.widened_by(resolved.retain_depth);
                        // Noise is a policy, not a capacity: "max" over a loudness
                        // ladder is a rule nothing in this codebase states, and
                        // picking one binding's would be positional. Require the
                        // bindings to agree instead — order-independent, and the
                        // operator is told exactly what to reconcile.
                        assert!(
                            shared.noise == resolved.noise,
                            "config: [[surface]] {slug:?}: instance {:?} binds channel {:?} on \
                             more than one port with conflicting noise ({:?} vs {:?}) — those \
                             ports share one subscription, so its overflow loudness cannot be \
                             both; set the same noise on every binding of this (instance, \
                             channel) or leave them all unset",
                            sub.instance,
                            channel,
                            shared.noise,
                            resolved.noise,
                        );
                    }
                    None => {
                        seen_wire.insert(key, wire_subscriptions.len());
                        wire_subscriptions.push(ResolvedSurfaceSubscription {
                            instance: sub.instance.clone(),
                            subscription: resolved,
                        });
                    }
                }
            }

            // A binding that asks to be loud on overflow (`alarm` or `fatal`)
            // needs somewhere to shout: the kernel emits its overflow alert on
            // the surface alert plane, which `handle_alert` denies unless the
            // surface holds the alert grant. An operator who marks a binding
            // `alarm`/`fatal` without granting the plane has declared alerting the
            // kernel is not permitted to deliver — dead config. Same posture as the
            // takeover-binding-needs-takeover-grant check above.
            assert!(
                noise < NoiseLevel::Alarm
                    || policy
                        .grants
                        .has(brenn_lib::access::AppCapability::SurfaceAlert),
                "config: [[surface]] {slug:?}: instance {:?} binds channel {:?} with noise {:?}, \
                 which alerts on overflow — that requires the surface's `alert` grant so the \
                 kernel may deliver the alert; add it to [[surface]] grants or lower the noise",
                sub.instance,
                channel,
                noise,
            );

            // Every class and every ABI arrives here with the same three facts,
            // so the rules over them are stated once.
            assert_page_queue_deliverable(
                slug,
                &context,
                &channel,
                push_depth,
                retain_depth,
                sub.noise,
            );

            subscriptions.push(SurfaceBinding {
                channel_address: channel.clone(),
                instance: sub.instance.clone(),
                port: sub.port.clone(),
                push_depth,
                retain_depth,
                noise,
            });
        }

        // Per instance, not per surface: activations are minted per instance, so
        // one triggering component says nothing about a sibling whose every port
        // is context-only.
        for comp in &resolved_components {
            let push_depths: Vec<u64> = subscriptions
                .iter()
                .filter(|b| b.instance == comp.instance)
                .map(|b| b.push_depth)
                .collect();
            assert_instance_can_activate(slug, &comp.instance, &push_depths);
        }

        // A subscription count over the burst bound trips the peer's meter at
        // first connect.
        let startup_subscribe_burst = subscriptions.len();
        assert!(
            startup_subscribe_burst <= brenn_surface_schema::MAX_SURFACE_SUBSCRIPTION_BINDINGS,
            "config: [[surface]] {slug:?}: {startup_subscribe_burst} first-connect subscribes \
             (subscription bindings) exceed the shell's synchronous startup-subscribe bound ({}); \
             split the surface or reduce its subscriptions",
            brenn_surface_schema::MAX_SURFACE_SUBSCRIPTION_BINDINGS,
        );

        // A component count over the mount bound fills the kernel's control
        // channel at first connect, which panics it.
        let startup_mount_burst = resolved_components.len();
        assert!(
            startup_mount_burst <= brenn_surface_schema::MAX_SURFACE_COMPONENTS,
            "config: [[surface]] {slug:?}: {startup_mount_burst} components exceed the shell's \
             synchronous startup-mount bound ({}); split the surface or reduce its components",
            brenn_surface_schema::MAX_SURFACE_COMPONENTS,
        );

        // Outputs (publish bindings). The item-6 publish-coverage decision (the
        // policy authorizes the binding's channel) is deferred to
        // `assert_output_bindings_covered`, run after the substrate error-report
        // grant is injected — so a `[[surface.output]]` bound to the configured
        // error channel is covered by the injected grant rather than tripping a
        // dead-config panic before that grant exists.
        let mut outputs = Vec::with_capacity(surface.outputs.len() + io_outputs.len());
        let mut seen_out_ports: HashSet<(&str, &str)> = HashSet::new();
        for (out, direction) in surface
            .outputs
            .iter()
            .map(|out| (out, "output"))
            .chain(io_outputs.iter().map(|out| (out, "io_port")))
        {
            let channel = super::bound_channel(
                &format!("[[surface]] {slug:?}"),
                &format!(
                    "{direction} instance {:?} port {:?}",
                    out.instance, out.port
                ),
                out.channel.as_deref(),
                auto_wiring.surface_channel(slug, &out.instance, &out.port),
            );
            validate_binding(direction, &channel, &out.instance, &out.port);
            assert!(
                seen_out_ports.insert((out.instance.as_str(), out.port.as_str())),
                "config: [[surface]] {slug:?}: duplicate {direction} binding for instance {:?} \
                 port {:?} — one binding per (instance, port) direction",
                out.instance,
                out.port,
            );
            if brenn_envelope::is_local_channel(&channel) {
                validate_local_binding(direction, &channel, true);
                // Outputs carry no depth knobs, so an output-only local channel
                // takes the floor. Registering it here is what makes a
                // publish-only local channel exist for the router at all.
                accumulate_local_ring_depth(slug, &mut local_ring_depths, &channel, None);
            }
            outputs.push(SurfaceOutput {
                channel_address: channel.clone(),
                instance: out.instance.clone(),
                port: out.port.clone(),
                // Port → global default. A surface output has no `[[channel]]`
                // rung between them: `local:` channels have no `[[channel]]`
                // block at all, and for the bus schemes urgency is the
                // publisher's statement of intent about *this* port's traffic,
                // not a property the channel imposes on everyone who writes it.
                // Same one-step ladder `[[wasm_consumer]] [[output]]` uses.
                default_urgency: out.urgency.unwrap_or(Urgency::Normal),
                budget: resolve_output_budget(slug, out),
            });
        }

        let local_channels = local_ring_depths
            .into_iter()
            .map(|(address, ring_depth)| ResolvedLocalChannel {
                address,
                ring_depth,
            })
            .collect();

        result.push(ResolvedSurface {
            slug: slug.clone(),
            skin,
            components: resolved_components,
            subscriptions,
            wire_subscriptions,
            local_channels,
            outputs,
            policy,
            allowed_users: surface.allowed_users.clone(),
            publish_burst,
            publish_per_sec,
        });
    }
    result
}

/// Assert every surface output binding is covered by the surface's publish
/// policy: the binding's channel must be authorized by a transport grant plus a
/// covering publish ACL matcher, else the output can never publish — dead
/// config, fail fast (the item-6 decision, lifted out of [`resolve_surfaces`]).
///
/// Runs as a post-pass **after** [`inject_surface_error_grant`], so a
/// `[[surface.output]]` bound to the configured `surface_error_channel` is
/// covered by the injected substrate grant (the many-writer shape the operator
/// opted into) rather than panicking on a grant that is injected moments later.
pub(crate) fn assert_output_bindings_covered(surfaces: &[ResolvedSurface]) {
    for surface in surfaces {
        for out in &surface.outputs {
            let covered = match ChannelScheme::split(&out.channel_address) {
                Some((ChannelScheme::Ephemeral, name)) => {
                    surface.policy.allows_ephemeral_publish(name)
                }
                Some((ChannelScheme::Brenn, name)) => surface.policy.allows_brenn_publish(name),
                // A `local:` output publishes into the page's own router and
                // never onto the bus, so there is no publish for a bus ACL to
                // authorize — requiring coverage would demand the operator grant
                // a right the server does not mediate. Its declaration is policed
                // by `resolve_surfaces` (reserved-channel rules incl. the
                // takeover grant and the kernel-publish-only planes).
                Some((ChannelScheme::Local, _)) => true,
                // resolve_surfaces' validate_binding already enforced the
                // brenn:/ephemeral:/local: scheme on every output.
                Some((
                    ChannelScheme::Mqtt | ChannelScheme::Webhook | ChannelScheme::PwaPush,
                    _,
                ))
                | None => {
                    unreachable!("output channel scheme validated by validate_binding")
                }
            };
            assert!(
                covered,
                "config: [[surface]] {:?}: output binds channel {:?} but the surface's \
                 access policy does not authorize publishing there (missing transport grant \
                 and/or a covering publish ACL matcher) — dead config",
                surface.slug, out.channel_address,
            );
        }
    }
}

/// Inject the substrate error-reporting grant onto every resolved surface's
/// policy: when `[observability] surface_error_channel` is configured, every
/// surface may publish its own error reports onto it under its own
/// `surface:<slug>` identity. Applied immediately after [`resolve_surfaces`],
/// before the policies fan out to the runtimes, the subscriber registry, and the
/// boot validators, so each policy is complete everywhere it is read.
///
/// Error reporting is a substrate right, not a per-`[[surface]]` operator grant:
/// requiring each surface to hand-author a `publish_acl` entry would let a
/// forgotten surface vanish from the channel with no error anywhere (the
/// silent-stranding class). The `MessagingPublish` grant is idempotent (a set
/// insert); the exact matcher is appended unconditionally — ACL evaluation is
/// any-match, so a redundant matcher on a surface that already covers the channel
/// is harmless.
///
/// `bare` is the scheme-stripped channel name (the `brenn_publish` matcher
/// convention).
pub(crate) fn inject_surface_error_grant(surfaces: &mut [ResolvedSurface], bare: &str) {
    use brenn_lib::access::AppCapability;
    use brenn_lib::access::acl::ChannelMatcher;
    for surface in surfaces {
        surface
            .policy
            .grants
            .insert(AppCapability::MessagingPublish);
        surface
            .policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(bare.to_string()));
    }
}

/// Inject each surface's config-channel subscribe grant: a surface may subscribe
/// its own `ephemeral:<prefix>.surface.<slug>.bindings` channel under its own
/// `surface:<slug>` identity, and no other. Applied immediately after
/// [`resolve_surfaces`], alongside the geometry/status publish grants, so each
/// policy is complete everywhere it is read.
///
/// A substrate right, not a per-`[[surface]]` operator grant, for the same reason
/// the telemetry grants are: the channel is derived, not authored, so requiring
/// the operator to hand-write an ACL for it would let a forgotten entry strand a
/// surface with no wiring and no error anywhere. Each surface receives an exact
/// `ephemeral_subscribe` matcher for its own channel only. `prefix` roots the
/// derived bare names.
pub(crate) fn inject_surface_config_subscribe_grants(
    surfaces: &mut [ResolvedSurface],
    prefix: &str,
) {
    use crate::routes::surface::description::surface_config_bare;
    use brenn_lib::access::AppCapability;
    use brenn_lib::access::acl::ChannelMatcher;
    for surface in surfaces {
        let config = surface_config_bare(prefix, &surface.slug);
        surface
            .policy
            .grants
            .insert(AppCapability::EphemeralSubscribe);
        surface
            .policy
            .acls
            .ephemeral_subscribe
            .push(ChannelMatcher::Exact(config));
    }
}

/// Inject the surface self-description telemetry grant onto every resolved
/// surface's policy: each surface may publish its own geometry and status
/// documents onto its two derived runtime channels under its own
/// `surface:<slug>` identity. Applied immediately after
/// [`resolve_surfaces`], alongside the error-report grant, so each policy is
/// complete everywhere it is read (the runtimes, the subscriber registry, and the
/// single-writer sweep, which excludes exactly this owning-surface coverage).
///
/// Like error reporting, geometry/status is a substrate right every surface
/// has, not a per-`[[surface]]` operator grant — a forgotten surface would
/// otherwise vanish from telemetry with no error. Each surface receives an
/// exact `brenn_publish` matcher for its own two channels only; the sweep proves
/// no *other* principal can write them. `prefix` roots the derived bare names.
pub(crate) fn inject_surface_geometry_status_grants(
    surfaces: &mut [ResolvedSurface],
    prefix: &str,
) {
    use crate::routes::surface::description::{surface_geometry_bare, surface_status_bare};
    use brenn_lib::access::AppCapability;
    use brenn_lib::access::acl::ChannelMatcher;
    for surface in surfaces {
        let geometry = surface_geometry_bare(prefix, &surface.slug);
        let status = surface_status_bare(prefix, &surface.slug);
        surface
            .policy
            .grants
            .insert(AppCapability::MessagingPublish);
        surface
            .policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(geometry));
        surface
            .policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(status));
    }
}

//! Level 2: what moved in the lowered plan, and whether it may move live.
//!
//! Level 1 has already established that the two documents agree everywhere
//! outside `channels`, `links` and `wasm_consumers`. What is left is a plan
//! computed from each, and two questions about the pair: which directory
//! entries and which consumers differ, and whether every one of those
//! differences is one the running process can be walked to without a restart.
//!
//! The delta is computed over exactly two things per side — the finalized
//! directory and the resolved consumers — because those are the only plan
//! outputs a reload converges. Taking them as facts rather than taking a whole
//! plan is also what lets the classification be exercised over hand-built
//! directories.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use indexmap::IndexMap;

use brenn_lib::config::AppConfig;
use brenn_lib::messaging::config::{ResolvedSurface, ResolvedWasmConsumer};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
};
use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
use brenn_lib::wasm_package::Verified;
use uuid::Uuid;

use super::NEEDS_RESTART;
use super::agents::{AgentChange, AgentClosure, AgentInputs, agent_delta};
use super::dynamic::{
    DynamicSnapshot, dormant_rows_the_reload_cannot_follow, dynamic_ingress, folded_now,
};
use super::mqtt::{MqttDelta, MqttIngressSet, mqtt_delta};
use super::surfaces::{SurfaceClosure, SurfaceDelta, surface_delta};

/// One side of the comparison: everything a reload reads off a plan.
///
/// `records` holds the package binding of every consumer named in `consumers`,
/// keyed by slug — for the baseline what the running consumer was loaded from,
/// for the candidate what re-resolving its package on disk says it would be
/// loaded from now. Comparing them is what makes a bundle upgrade under an
/// unchanged document a change rather than an invisible drift.
pub(crate) struct PlanFacts<'a> {
    pub directory: &'a MessagingDirectory,
    /// The resolved agent map this side's plan was derived from — the booted
    /// one for the baseline, the candidate's own for the candidate. The agent
    /// half of the delta compares the two.
    pub apps: &'a IndexMap<String, AppConfig>,
    pub consumers: &'a [ResolvedWasmConsumer],
    pub records: &'a HashMap<String, Verified>,
    /// The plan's *static* `mqtt:` ingress channels: the ones this side's
    /// document declares. Only half of what the broker's SUBSCRIBE union is
    /// diffed over — a fresh boot of this document would also derive every
    /// dynamic `mqtt:` subscription its merge kept, so the other half comes off
    /// the live process ([`LiveFacts`]) and the two are unioned per side.
    pub mqtt_ingress: &'a [ResolvedMqttIngressChannel],
    /// The plan's resolved surfaces, which the surface delta is keyed on.
    pub surfaces: &'a [ResolvedSurface],
}

/// What a reload reads off the running process rather than off either plan.
///
/// Everything here is a fact about *this* process that no document describes: a
/// dynamic subscription an agent asked for at runtime, and the directory it was
/// folded into. Both sides of the comparison need them — the baseline stands
/// behind what the process holds now, and the candidate behind what it would
/// hold after the re-merge — so they are one input rather than a member of
/// either side.
pub(crate) struct LiveFacts<'a> {
    pub directory: &'a MessagingDirectory,
    pub dynamic: &'a DynamicSnapshot,
    /// The declared MQTT clients, which is where a dynamic `mqtt:`
    /// subscription's injection urgency comes from — the row carries the qos
    /// and the address carries the filter, and neither carries that.
    pub mqtt_clients: &'a IndexMap<String, brenn_lib::mqtt::config::MqttClientIdentity>,
}

/// A channel entry that is in both plans under one uuid but is not the same
/// entry: the commit takes it out and puts the new one in.
pub(crate) struct ChannelChange {
    pub old: Arc<ChannelEntry>,
    pub new: Arc<ChannelEntry>,
}

/// Everything that differs between two plans, in the vocabulary the commit and
/// the status document both speak.
#[derive(Default)]
pub(crate) struct PlanDelta {
    /// Entries the candidate has and the baseline does not.
    pub channels_added: Vec<Arc<ChannelEntry>>,
    /// Entries the baseline has and the candidate does not.
    pub channels_removed: Vec<Arc<ChannelEntry>>,
    /// Entries present in both whose identity or tuning moved.
    pub channels_changed: Vec<ChannelChange>,
    /// Entries present in both, identical but for their `description` — the
    /// candidate's side, which is the text to install. Metadata only: nothing
    /// here routes, sizes or authorizes, so the entry is edited in place and no
    /// subscriber and no consumer is touched.
    pub channels_described: Vec<Arc<ChannelEntry>>,
    pub consumers_added: Vec<String>,
    pub consumers_removed: Vec<String>,
    /// Consumers whose resolved value or package binding moved, plus the ones
    /// promoted by delta closure because a channel they are wired to moved.
    pub consumers_changed: Vec<String>,
    /// What the broker's SUBSCRIBE set and the ingress route table have to
    /// become. Derived from the two plans' ingress channel lists and from the
    /// channel delta above, not declared alongside them.
    pub mqtt: MqttDelta,
    /// Which surfaces the commit retires, starts or replaces. Derived from the
    /// two plans' surface lists, from the channel delta above and from the two
    /// sides' kind fingerprints.
    pub surfaces: SurfaceDelta,
    /// Kinds the two sides' scans of the declared mounts disagree about. The
    /// kind half of the surface delta's closure, kept because the status body
    /// reports it: "this reload's surfaces moved because that bundle upgraded"
    /// is not readable off the surface list alone.
    ///
    /// Deliberately not part of [`PlanDelta::is_empty`]: a kind nothing
    /// instantiates moving changes no projection, and one that something
    /// instantiates has already put its surfaces in `surfaces`.
    ///
    /// Always empty in this build: prepare refuses a candidate whose scan of
    /// the declared mounts disagrees with the trees the process is serving,
    /// which is every case this set could name.
    pub kinds_changed: BTreeSet<String>,
    /// The agents whose authority, per-call settings, per-process view or
    /// static subscriptions moved.
    pub agents_changed: Vec<AgentChange>,
    /// The dynamic subscriptions the agent half was classified against, as they
    /// stood when prepare read them.
    ///
    /// An input rather than a difference, carried here because it is the one
    /// input to a reload that a live session can change while prepare runs: the
    /// commit re-reads the set and declines a walk whose subject moved
    /// underneath it. Deliberately absent from [`PlanDelta::is_empty`] — it
    /// says nothing about whether the two documents project differently.
    pub dynamic_observed: DynamicSnapshot,
}

/// The channel delta's uuids, split by which way each entry goes.
///
/// A named set per side because the rules that read them answer differently on
/// each, and three same-typed `HashSet<Uuid>` parameters in a row are a
/// transposition waiting to happen: a dormant dynamic row on a *retuned*
/// channel is refused while one on a *removed* operator-declared channel is
/// applied, so swapping the two sets swaps refuse for apply.
pub(crate) struct ChannelSides {
    /// Entries the candidate has and the baseline did not.
    pub added: HashSet<Uuid>,
    /// Entries the candidate drops outright.
    pub removed: HashSet<Uuid>,
    /// The old side of every retune. Equal to `retuned_new` today — a retune
    /// keeps its uuid — and named apart because nothing here depends on that.
    pub retuned_old: HashSet<Uuid>,
    /// The new side of every retune.
    pub retuned_new: HashSet<Uuid>,
}

impl ChannelSides {
    /// Every uuid either side of the delta names. What the consumer, surface
    /// and agent closures read: an entry wired to a channel that moved is
    /// re-derived against the new entry, whichever way it moved.
    pub(crate) fn moved(&self) -> HashSet<Uuid> {
        self.added
            .union(&self.removed)
            .chain(self.retuned_new.iter())
            .copied()
            .collect()
    }

    /// The narrower set this commit takes *away*: removed outright, or the old
    /// side of a retune. No dynamic pair may be classified against one, because
    /// the entry the classification would read is one the channel walk deletes.
    ///
    /// Deliberately not `moved`, which also carries the arrivals: a channel
    /// this reload adds is one the live directory does not hold yet, and the
    /// re-merge skips it for that reason instead.
    pub(crate) fn departing(&self) -> HashSet<Uuid> {
        self.removed.union(&self.retuned_old).copied().collect()
    }
}

impl PlanDelta {
    /// Whether the two plans project the same running state. An empty delta
    /// after a passing level 1 is the `unchanged` outcome: the file bytes moved
    /// and the projection did not.
    pub fn is_empty(&self) -> bool {
        self.channels_added.is_empty()
            && self.channels_removed.is_empty()
            && self.channels_changed.is_empty()
            && self.channels_described.is_empty()
            && self.consumers_added.is_empty()
            && self.consumers_removed.is_empty()
            && self.consumers_changed.is_empty()
            && self.mqtt.is_empty()
            && self.surfaces.is_empty()
            && self.agents_changed.is_empty()
    }

    /// The slugs of every agent this reload walks.
    fn moving_agents(&self) -> HashSet<&str> {
        self.agents_changed
            .iter()
            .map(|change| change.slug.as_str())
            .collect()
    }

    /// The channel delta's uuids, one set per side, so every rule that asks
    /// "which uuids move, and which way" reads one derivation instead of
    /// walking the three lists itself.
    pub(crate) fn channel_sides(&self) -> ChannelSides {
        ChannelSides {
            added: self.channels_added.iter().map(|e| e.uuid).collect(),
            removed: self.channels_removed.iter().map(|e| e.uuid).collect(),
            retuned_old: self.channels_changed.iter().map(|c| c.old.uuid).collect(),
            retuned_new: self.channels_changed.iter().map(|c| c.new.uuid).collect(),
        }
    }

    /// The entries leaving the directory: removed outright, or the old side of
    /// a change, which the commit treats as a removal followed by an addition.
    pub(crate) fn leaving(&self) -> impl Iterator<Item = &Arc<ChannelEntry>> {
        self.channels_removed
            .iter()
            .chain(self.channels_changed.iter().map(|change| &change.old))
    }

    /// The entries joining the directory: added outright, or the new side of a
    /// change.
    pub(crate) fn joining(&self) -> impl Iterator<Item = &Arc<ChannelEntry>> {
        self.channels_added
            .iter()
            .chain(self.channels_changed.iter().map(|change| &change.new))
    }

    /// The addresses of both sets. Both sides of a changed entry are named: an
    /// entry cannot change its address without changing its uuid today, but
    /// nothing here depends on that staying true.
    fn moved_addresses(&self) -> HashSet<String> {
        self.leaving()
            .chain(self.joining())
            .map(|e| e.address.clone())
            .collect()
    }
}

/// Whether two entries under one uuid are the same channel: identity and
/// tuning, which is everything that routes, sizes or authorizes.
///
/// `subscribers` is excluded on purpose — subscribers are edited in place, and
/// a consumer joining a channel an agent already reads must not re-create the
/// channel under the agent. `description` is excluded because it is metadata,
/// and gets its own in-place path.
fn same_channel(a: &ChannelEntry, b: &ChannelEntry) -> bool {
    a.uuid == b.uuid
        && a.address == b.address
        && a.resolved_channel == b.resolved_channel
        && a.transport_type == b.transport_type
        && a.mount == b.mount
}

/// Classify every difference between two plans.
///
/// `kinds_changed` is the caller's comparison of the two sides' surface asset
/// scans: it is asked for before the delta is built, so the answer is passed in
/// rather than computed a second time here.
pub(crate) fn plan_delta(
    baseline: &PlanFacts<'_>,
    candidate: &PlanFacts<'_>,
    kinds_changed: BTreeSet<String>,
    agents: &AgentInputs<'_>,
    live: &LiveFacts<'_>,
) -> PlanDelta {
    let old_entries = Entries::of(baseline.directory);
    let new_entries = Entries::of(candidate.directory);
    let mut delta = PlanDelta::default();

    for entry in &new_entries.list {
        match old_entries.by_uuid.get(&entry.uuid) {
            None => delta.channels_added.push(Arc::clone(entry)),
            Some(old) if !same_channel(old, entry) => delta.channels_changed.push(ChannelChange {
                old: Arc::clone(old),
                new: Arc::clone(entry),
            }),
            Some(old) if old.description != entry.description => {
                delta.channels_described.push(Arc::clone(entry));
            }
            Some(_) => {}
        }
    }
    for entry in &old_entries.list {
        if !new_entries.by_uuid.contains_key(&entry.uuid) {
            delta.channels_removed.push(Arc::clone(entry));
        }
    }

    let old_consumers = by_slug(baseline.consumers);
    let new_consumers = by_slug(candidate.consumers);
    let sides = delta.channel_sides();
    let moved = sides.moved();
    for consumer in candidate.consumers {
        match old_consumers.get(consumer.slug.as_str()) {
            None => delta.consumers_added.push(consumer.slug.clone()),
            Some(old) => {
                let resolved_moved = *old != consumer;
                // The package's *release*, not its record: the mount install
                // scheme moves a package's canonical paths on every install,
                // and a byte-identical re-deploy is not a change to converge.
                let record_moved = match (
                    baseline.records.get(&consumer.slug),
                    candidate.records.get(&consumer.slug),
                ) {
                    (Some(old), Some(new)) => !old.same_release(new),
                    (old, new) => old.is_some() != new.is_some(),
                };
                // Delta closure: a consumer wired to an entry that moved is
                // re-derived against the new entry, because that is what a
                // fresh boot would give it. Both sides' ports are consulted —
                // a removed channel is named only by the old value, an added
                // one only by the new.
                let wiring_moved = wired_channels(old).iter().any(|u| moved.contains(u))
                    || wired_channels(consumer).iter().any(|u| moved.contains(u));
                if resolved_moved || record_moved || wiring_moved {
                    delta.consumers_changed.push(consumer.slug.clone());
                }
            }
        }
    }
    for consumer in baseline.consumers {
        if !new_consumers.contains_key(consumer.slug.as_str()) {
            delta.consumers_removed.push(consumer.slug.clone());
        }
    }

    // Surface half — depends on the channel delta and the kind fingerprints.
    let moved_channels = moved.clone();
    let moved_addresses = delta.moved_addresses();
    delta.kinds_changed = kinds_changed;
    delta.surfaces = surface_delta(
        baseline.surfaces,
        candidate.surfaces,
        &SurfaceClosure {
            moved_channels: &moved_channels,
            moved_addresses: &moved_addresses,
            kinds_changed: &delta.kinds_changed,
        },
    );

    // Agent half — must run after the channel delta so `moved_channels` is
    // complete, and before the MQTT half, which reads the dynamic
    // subscriptions it re-authorized.
    delta.agents_changed = agent_delta(
        baseline.apps,
        candidate.apps,
        &old_entries.list,
        &new_entries.list,
        &AgentClosure {
            moved: &moved_channels,
            departing: &sides.departing(),
        },
        agents,
        live,
    );

    delta.dynamic_observed = live.dynamic.clone();

    // The MQTT half, last: it reads the channel delta for its static routes,
    // the two plans' ingress lists and the two sides' dynamic subscriptions for
    // the broker set, which are three grains of the same move.
    let leaving: Vec<&ChannelEntry> = delta.leaving().map(Arc::as_ref).collect();
    let joining: Vec<&ChannelEntry> = delta.joining().map(Arc::as_ref).collect();
    let (baseline_dynamic, candidate_dynamic) =
        dynamic_ingress_sides(baseline, candidate, &delta, live);
    delta.mqtt = mqtt_delta(
        &MqttIngressSet {
            static_: baseline.mqtt_ingress,
            dynamic: baseline_dynamic,
        },
        &MqttIngressSet {
            static_: candidate.mqtt_ingress,
            dynamic: candidate_dynamic,
        },
        &leaving,
        &joining,
    );
    delta
}

/// The dynamic `mqtt:` ingress each side of this reload stands behind.
///
/// The baseline's is what the process holds folded right now. The candidate's
/// is that set less what the re-merge revoked or pruned, plus what it revived —
/// which is exactly what a fresh boot of the candidate would derive from the
/// same rows. Each is taken against its own side's static channels, because a
/// filter the document declares is subscribed and routed as a static channel
/// and must not be counted twice.
///
/// One channel folded by two agents is one filter and one route: the
/// projection dedupes by channel uuid, so a revoke by one agent while another
/// still holds it folded leaves the filter in the candidate's set.
fn dynamic_ingress_sides(
    baseline: &PlanFacts<'_>,
    candidate: &PlanFacts<'_>,
    delta: &PlanDelta,
    live: &LiveFacts<'_>,
) -> (
    Vec<ResolvedMqttIngressChannel>,
    Vec<ResolvedMqttIngressChannel>,
) {
    let folded: Vec<&brenn_lib::messaging::DynamicSubscriptionRow> = live
        .dynamic
        .rows
        .iter()
        .filter(|row| folded_now(live.directory, &row.channel_uuid, &row.app_slug))
        .collect();
    let withdrawn: HashSet<(&str, Uuid)> = delta
        .agents_changed
        .iter()
        .flat_map(|change| {
            change
                .dynamic
                .revoke
                .iter()
                .map(|revoked| &revoked.moved)
                .chain(&change.dynamic.prune)
                .map(|moved| (change.slug.as_str(), moved.channel_uuid))
        })
        .collect();

    let before: Vec<brenn_lib::messaging::DynamicSubscriptionRow> =
        folded.iter().map(|row| (*row).clone()).collect();
    let mut after: Vec<brenn_lib::messaging::DynamicSubscriptionRow> = folded
        .iter()
        .filter(|row| !withdrawn.contains(&(row.app_slug.as_str(), row.channel_uuid)))
        .map(|row| (*row).clone())
        .collect();
    for change in &delta.agents_changed {
        for revived in &change.dynamic.revive {
            if let Some(row) = &revived.row {
                after.push(row.clone());
            }
        }
    }

    let baseline_static: HashSet<Uuid> = baseline
        .mqtt_ingress
        .iter()
        .map(|channel| channel.channel_uuid)
        .collect();
    let candidate_static: HashSet<Uuid> = candidate
        .mqtt_ingress
        .iter()
        .map(|channel| channel.channel_uuid)
        .collect();
    (
        dynamic_ingress(&before, live.directory, live.mqtt_clients, &baseline_static),
        dynamic_ingress(&after, live.directory, live.mqtt_clients, &candidate_static),
    )
}

/// The uuid of every channel a consumer reads or writes.
fn wired_channels(consumer: &ResolvedWasmConsumer) -> BTreeSet<Uuid> {
    consumer
        .inputs
        .iter()
        .map(|port| port.sub.channel_uuid)
        .chain(consumer.outputs.iter().map(|port| port.channel_uuid))
        .collect()
}

/// A directory's entries in declaration order and by uuid, materialized once.
///
/// Declaration order is what the delta's lists — and so the status body an
/// operator reads — come out in; the map is what the classification looks
/// entries up through.
struct Entries {
    list: Vec<Arc<ChannelEntry>>,
    by_uuid: HashMap<Uuid, Arc<ChannelEntry>>,
}

impl Entries {
    fn of(directory: &MessagingDirectory) -> Self {
        let list = directory.list();
        let by_uuid = list
            .iter()
            .map(|entry| (entry.uuid, Arc::clone(entry)))
            .collect();
        Self { list, by_uuid }
    }
}

fn by_slug(consumers: &[ResolvedWasmConsumer]) -> HashMap<&str, &ResolvedWasmConsumer> {
    consumers
        .iter()
        .map(|consumer| (consumer.slug.as_str(), consumer))
        .collect()
}

/// Every reason this delta cannot be applied to a running process, as refusal
/// lines. Empty means the reload may commit.
///
/// `live` is the directory as it stands right now, which is not the baseline
/// plan's: dynamic app subscriptions and attach-minted surface and remote
/// entries are added to it after boot, and a channel one of them sits on cannot
/// be taken out from under them.
///
/// # Panics
///
/// If an *unchanged* entry's non-consumer subscribers differ between the two
/// plans. Every entity that mints such a subscriber is non-convergible, so
/// level 1 has already proved the two documents agree about all of them; a
/// difference here means the planner derived one of them from something other
/// than the document, which is a host bug and not an operator's problem.
pub(crate) fn convergibility_refusals(
    baseline: &PlanFacts<'_>,
    candidate: &PlanFacts<'_>,
    delta: &PlanDelta,
    live: &MessagingDirectory,
) -> Vec<String> {
    let mut out = Vec::new();
    // The one derivation of "what moved", shared by rule 2's live check and the
    // corollary assert below, so the two cannot come to disagree about it.
    let moved = delta.channel_sides().moved();
    let departing: HashSet<&str> = delta
        .consumers_removed
        .iter()
        .chain(&delta.consumers_changed)
        .map(String::as_str)
        .collect();
    let arriving: HashSet<&str> = delta
        .consumers_added
        .iter()
        .chain(&delta.consumers_changed)
        .map(String::as_str)
        .collect();

    // Rule 3 first: the scheme is a property of the entry alone, and reporting
    // it before the subscriber rules gives the operator the address rather than
    // a list of who happens to sit on it.
    for entry in &delta.channels_added {
        rule_3(entry, "is newly minted", &mut out);
    }
    for entry in &delta.channels_removed {
        rule_3(entry, "is no longer minted", &mut out);
    }
    for change in &delta.channels_changed {
        rule_3(&change.new, "retuned", &mut out);
    }

    // Rule 1, over both plans: every subscriber on an entry in the channel
    // delta must belong to a consumer or a surface that is itself moving,
    // because a re-created entry re-wires its subscribers and nothing else here
    // can be re-wired. A surface qualifies on the same terms a consumer does:
    // the commit retires it before the channels move and starts it after, so
    // its entries are folded onto the new channel from the candidate's plan.
    let leaving_surfaces = departing_surfaces(delta);
    let joining_surfaces = arriving_surfaces(delta);
    // An agent on a moving channel qualifies on the same terms as a surface:
    // the commit folds its subscriber entries out and back in.
    let moving_agents = delta.moving_agents();
    for entry in &delta.channels_added {
        rule_1(
            entry,
            "added",
            &arriving,
            &joining_surfaces,
            &moving_agents,
            &mut out,
        );
    }
    for entry in &delta.channels_removed {
        rule_1(
            entry,
            "removed",
            &departing,
            &leaving_surfaces,
            &moving_agents,
            &mut out,
        );
    }
    for change in &delta.channels_changed {
        rule_1(
            &change.old,
            "changed",
            &departing,
            &leaving_surfaces,
            &moving_agents,
            &mut out,
        );
        rule_1(
            &change.new,
            "changed",
            &arriving,
            &joining_surfaces,
            &moving_agents,
            &mut out,
        );
    }

    // Rule 2: the same question asked of the directory as it actually stands.
    out.extend(live_subscriber_refusals(delta, live));
    // Rule 2's other half, over the dynamic subscriptions the directory holds
    // no subscriber entry for. Separate because it reads the row set rather
    // than the directory, and because it needs no re-asking at commit: a
    // dormant row cannot arrive while a reload runs.
    for (slug, address) in
        dormant_rows_the_reload_cannot_follow(&delta.dynamic_observed, live, &delta.channel_sides())
    {
        out.push(format!(
            "{address} is going away but agent {slug:?} holds a dormant dynamic subscription to \
             it: {NEEDS_RESTART}",
        ));
    }

    // Every surface the delta walks, either side: the corollary assert below
    // measures what did *not* move, and a moving surface's entries move with
    // it whether or not the channel they sit on did.
    let moving_surfaces: HashSet<&str> =
        leaving_surfaces.union(&joining_surfaces).copied().collect();
    assert_unchanged_entries_agree(
        baseline,
        candidate,
        &moved,
        &moving_surfaces,
        &moving_agents,
    );
    // A changed entry is read on both sides, so a subscriber that sits on it in
    // both plans states its refusal twice. One problem, one line.
    let mut seen = HashSet::new();
    out.retain(|line| seen.insert(line.clone()));
    out
}

/// Rule 2 alone: what the directory as it actually stands holds that the two
/// plans do not — a subscriber on a channel this delta takes away, or an
/// address this delta mints that is already there.
///
/// A boot-shaped plan cannot see a dynamic subscription or an attach-minted
/// entry, and those are precisely the subscribers a live process has that a
/// fresh boot would not. Separated out because it is the one rule whose answer
/// can change after prepare has given it: the other rules read two plans, which
/// do not move, while this one reads a directory three other writers may add to
/// at any moment. So the commit phase asks it again, twice — once before it
/// touches anything, and once after the wait for a stopping consumer, which is
/// unbounded.
pub(crate) fn live_subscriber_refusals(
    delta: &PlanDelta,
    live: &MessagingDirectory,
) -> Vec<String> {
    let departing: HashSet<&str> = delta
        .consumers_removed
        .iter()
        .chain(&delta.consumers_changed)
        .map(String::as_str)
        .collect();
    let leaving_surfaces = departing_surfaces(delta);
    let mut out = Vec::new();
    // The added arm. A channel the candidate mints that the live directory
    // already holds under that address was minted by something the plan cannot
    // see — a dynamic subscription, an attach. `add_channels` would reach
    // `MessagingDirectory::add_channel`'s address assert in commit, which is
    // past the point where anything may decline, so it is refused here.
    for entry in &delta.channels_added {
        if live.resolve(&entry.address).is_some() {
            out.push(format!(
                "{} is newly minted but already exists (a dynamic subscription created it): \
                 {NEEDS_RESTART}",
                entry.address,
            ));
        }
    }
    for entry in delta.leaving() {
        let Some(live_entry) = live.by_uuid(&entry.uuid) else {
            continue;
        };
        let planned: HashSet<&SubscriberEntryKind> =
            entry.subscribers.iter().map(|s| &s.kind).collect();
        for subscriber in &live_entry.subscribers {
            // Subscribers the baseline plan already holds are rule 1's, and it
            // has answered for them. What is left is what boot did not put
            // there: a dynamic app row, an attach-minted surface or remote, a
            // live session streaming from the channel.
            // No agent is accounted here: only *dynamic* rows reach this
            // arm, and those are in neither plan.
            if !planned.contains(&subscriber.kind)
                && !accounted(
                    &subscriber.kind,
                    &departing,
                    &leaving_surfaces,
                    &HashSet::new(),
                )
            {
                out.push(format!(
                    "{} is going away but {} subscribes to it right now: {NEEDS_RESTART}",
                    entry.address,
                    describe(&subscriber.kind),
                ));
            }
        }
    }
    out
}

/// Rule 3: a `webhook:` entry cannot move.
///
/// A `webhook:` entry reaches the channel delta whenever a convergible block
/// moves what mints it — a tuning block retuning the entry, or the consumer
/// subscription that was its sole minter appearing or leaving. Its route is a
/// literal axum path built once into the router and the `WebhookService` behind
/// it is immutable, so the entry cannot follow. `mqtt:` is not here: the
/// broker's SUBSCRIBE set and the ingress route table are both runtime-mutable,
/// and [`super::mqtt`] walks them.
// TODO(reload-webhooks): converge webhook endpoints — one wildcard route over a
// swappable endpoint table with the per-endpoint body ceiling applied
// in-handler, plus a swappable `WebhookService` — and retire this rule.
fn rule_3(entry: &ChannelEntry, what: &str, out: &mut Vec<String>) {
    match entry.transport_type {
        ChannelScheme::Brenn
        | ChannelScheme::Ephemeral
        | ChannelScheme::Local
        | ChannelScheme::Mqtt => {}
        _ => out.push(format!("{} {what}: {NEEDS_RESTART}", entry.address)),
    }
}

/// The surfaces leaving service on this reload: removed outright, or the old
/// half of a replacement.
fn departing_surfaces(delta: &PlanDelta) -> HashSet<&str> {
    delta
        .surfaces
        .removed
        .iter()
        .chain(delta.surfaces.changed.iter().map(|change| &change.old))
        .map(|surface| surface.slug.as_str())
        .collect()
}

/// The surfaces entering service: added outright, or the new half of a
/// replacement.
fn arriving_surfaces(delta: &PlanDelta) -> HashSet<&str> {
    delta
        .surfaces
        .added
        .iter()
        .chain(delta.surfaces.changed.iter().map(|change| &change.new))
        .map(|surface| surface.slug.as_str())
        .collect()
}

/// Rule 1: every subscriber on a moving entry must be a consumer, a surface or
/// an agent that moves with it.
fn rule_1(
    entry: &ChannelEntry,
    what: &str,
    moving: &HashSet<&str>,
    moving_surfaces: &HashSet<&str>,
    moving_agents: &HashSet<&str>,
    out: &mut Vec<String>,
) {
    for subscriber in &entry.subscribers {
        if !accounted(&subscriber.kind, moving, moving_surfaces, moving_agents) {
            out.push(format!(
                "{} is {what} but {} subscribes to it: {NEEDS_RESTART}",
                entry.address,
                describe(&subscriber.kind),
            ));
        }
    }
}

/// Whether a subscriber on a moving entry is one this reload already takes out
/// of service and puts back.
fn accounted(
    kind: &SubscriberEntryKind,
    moving: &HashSet<&str>,
    moving_surfaces: &HashSet<&str>,
    moving_agents: &HashSet<&str>,
) -> bool {
    match kind {
        SubscriberEntryKind::Wasm(slug) => moving.contains(slug.as_str()),
        SubscriberEntryKind::Surface(slug) => moving_surfaces.contains(slug.as_str()),
        SubscriberEntryKind::App(slug) => moving_agents.contains(slug.as_str()),
        _ => false,
    }
}

/// A subscriber as a refusal names it: the kind an operator reads in the
/// document, and the slug they look it up by.
fn describe(kind: &SubscriberEntryKind) -> String {
    match kind {
        SubscriberEntryKind::App(slug) => format!("agent {slug:?}"),
        SubscriberEntryKind::Wasm(slug) => format!("component {slug:?}"),
        SubscriberEntryKind::Surface(slug) => format!("surface {slug:?}"),
        SubscriberEntryKind::Remote(slug) => format!("remote {slug:?}"),
        SubscriberEntryKind::System(name) => format!("the {name:?} system participant"),
        SubscriberEntryKind::ChatConversation {
            app_slug,
            conversation_id,
        } => format!("conversation {conversation_id} of agent {app_slug:?}"),
    }
}

/// The corollary of rule 1, asserted rather than refused: on an entry that did
/// not move, the two plans agree about every subscriber that is not a consumer.
fn assert_unchanged_entries_agree(
    baseline: &PlanFacts<'_>,
    candidate: &PlanFacts<'_>,
    moved: &HashSet<Uuid>,
    moving_surfaces: &HashSet<&str>,
    moving_agents: &HashSet<&str>,
) {
    let old_entries = Entries::of(baseline.directory);
    for entry in candidate.directory.list() {
        if moved.contains(&entry.uuid) {
            continue;
        }
        let Some(old) = old_entries.by_uuid.get(&entry.uuid) else {
            continue;
        };
        let old_foreign = foreign_subscribers(old, moving_surfaces, moving_agents);
        let new_foreign = foreign_subscribers(&entry, moving_surfaces, moving_agents);
        assert!(
            old_foreign == new_foreign,
            "channel {:?} did not move but its non-component subscribers did — {} before, {} \
             after — which means a non-convergible entity reached the plan through something \
             other than the document",
            entry.address,
            named(&old_foreign),
            named(&new_foreign),
        );
    }
}

/// Every subscriber on an entry that this reload does not walk: not a WASM
/// consumer, not a surface the surface delta moves, and not an agent the agent
/// delta moves. A difference in any of those is expected (they re-fold from the
/// candidate's plan), not evidence of a non-convergible entity.
///
/// Kinds, not their rendered text: the identity of a subscriber is the value,
/// and refusal wording is free to change without silently making two of them
/// compare equal.
fn foreign_subscribers<'a>(
    entry: &'a ChannelEntry,
    moving_surfaces: &HashSet<&str>,
    moving_agents: &HashSet<&str>,
) -> HashSet<&'a SubscriberEntryKind> {
    entry
        .subscribers
        .iter()
        .filter(|s| !matches!(&s.kind, SubscriberEntryKind::Wasm(_)))
        .filter(|s| !matches!(&s.kind, SubscriberEntryKind::Surface(slug) if moving_surfaces.contains(slug.as_str())))
        .filter(|s| !matches!(&s.kind, SubscriberEntryKind::App(slug) if moving_agents.contains(slug.as_str())))
        .map(|s: &SubscriberEntry| &s.kind)
        .collect()
}

/// A subscriber set formatted for a diagnostic, in a stable order.
fn named(kinds: &HashSet<&SubscriberEntryKind>) -> String {
    let named: BTreeSet<String> = kinds.iter().map(|kind| describe(kind)).collect();
    if named.is_empty() {
        "nobody".to_string()
    } else {
        named.into_iter().collect::<Vec<_>>().join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No agents on either side: these cases are about channels, consumers and
    /// surfaces, and an empty map on both sides keeps the agent half of the
    /// delta out of them.
    use std::collections::BTreeMap;

    static NO_APPS: std::sync::LazyLock<IndexMap<String, AppConfig>> =
        std::sync::LazyLock::new(IndexMap::new);
    static NO_DIFFS: std::sync::LazyLock<BTreeMap<String, crate::reload::compare::AppFieldDiff>> =
        std::sync::LazyLock::new(BTreeMap::new);

    static NO_TOOLS: std::sync::LazyLock<brenn_tool_registry::ToolRegistry> =
        std::sync::LazyLock::new(|| brenn_tool_registry::ToolRegistry::new(vec![]));

    fn no_agents() -> AgentInputs<'static> {
        AgentInputs {
            app_diffs: &NO_DIFFS,
            tool_registry: &NO_TOOLS,
        }
    }

    static EMPTY_DIRECTORY: std::sync::LazyLock<MessagingDirectory> =
        std::sync::LazyLock::new(|| MessagingDirectory::with_entries(Vec::new()));
    static NO_DYNAMIC: std::sync::LazyLock<DynamicSnapshot> =
        std::sync::LazyLock::new(DynamicSnapshot::default);
    static NO_CLIENTS: std::sync::LazyLock<
        IndexMap<String, brenn_lib::mqtt::config::MqttClientIdentity>,
    > = std::sync::LazyLock::new(IndexMap::new);

    /// A process holding no dynamic subscription at all, which is every case in
    /// this module: they are about two plans, and a dynamic subscription is in
    /// neither.
    fn no_live() -> LiveFacts<'static> {
        LiveFacts {
            directory: &EMPTY_DIRECTORY,
            dynamic: &NO_DYNAMIC,
            mqtt_clients: &NO_CLIENTS,
        }
    }

    /// Every plan in this module is compared with an empty `kinds_changed`:
    /// these cases are about channels and consumers, and a kind set that never
    /// moves keeps the surface delta's kind closure out of them.
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, Depth, NoiseLevel, WasmConsumerConfigRaw,
    };
    use brenn_lib::messaging::test_support::test_channel_entry;
    use brenn_messaging_boot::test_fixtures::{
        durable_channel, surface_index_channel, webhook_endpoint_raw,
    };
    use brenn_messaging_boot::{MessagingPlan, PlanInputs, plan_messaging};

    // ---------------------------------------------------------------------
    // Plans built from documents: the classification half.
    // ---------------------------------------------------------------------

    /// A durable `brenn:` block at fixed depths and a fixed uuid, so two
    /// documents that declare the same channel name the same entry.
    fn durable(
        address: &str,
        uuid: &str,
        standing: u64,
        description: Option<&str>,
    ) -> ChannelConfigRaw {
        ChannelConfigRaw {
            uuid: Some(uuid.to_string()),
            description: description.map(str::to_string),
            ..durable_channel(address, Depth::Bounded(standing))
        }
    }

    /// A `[[wasm_consumer]]` block reading `channels`, with the port grant and
    /// the subscribe ACL its subscriptions need to be deliverable.
    fn consumer(slug: &str, channels: &[&str]) -> WasmConsumerConfigRaw {
        WasmConsumerConfigRaw {
            grants: vec![brenn_lib::messaging::ComponentGrant::Ports],
            subscribe_acl: channels
                .iter()
                .map(|address| {
                    brenn_lib::access::raw::ChannelMatcherRaw::Exact(
                        address.trim_start_matches("brenn:").to_string(),
                    )
                })
                .collect(),
            ..WasmConsumerConfigRaw::minimal(slug, "processor-demo", channels)
        }
    }

    const WORK_UUID: &str = "5f1d1a9e-0000-4000-8000-00000000000a";
    const SPARE_UUID: &str = "5f1d1a9e-0000-4000-8000-00000000000b";

    /// The floor every fixture starts from: the description index and one
    /// durable work channel.
    fn base() -> BrennConfig {
        let mut config = BrennConfig::default();
        config.channels.push(surface_index_channel());
        config
            .channels
            .push(durable("brenn:work", WORK_UUID, 4, Some("the work")));
        config
    }

    fn plan_of(config: &BrennConfig) -> MessagingPlan {
        // The identities come off the document being planned, as boot's do: a
        // fixture declaring an `[[mqtt_client]]` has to resolve its own.
        let clients = brenn_lib::mqtt::config::resolve_client_identities(&config.mqtt_clients);
        plan_messaging(&PlanInputs {
            config,
            apps: None,
            mqtt_clients: &clients,
            tool_registry: None,
            replay_store_paths: &[],
        })
        .expect("a document declaring channels configures messaging")
    }

    /// A record a consumer's package would bind to, `sha` distinguishing one
    /// installed artifact from another.
    fn record(sha: &str) -> Verified {
        Verified {
            artifact: std::path::PathBuf::from("/components/demo/demo.wasm"),
            root: std::path::PathBuf::from("/components"),
            world: "brenn:processor".to_string(),
            artifact_sha256: sha.to_string(),
            spec_sha256: None,
        }
    }

    /// One record per consumer in `plan`, all bound to the same artifact.
    fn records(plan: &MessagingPlan, sha: &str) -> HashMap<String, Verified> {
        plan.wasm_consumers
            .iter()
            .map(|c| (c.slug.clone(), record(sha)))
            .collect()
    }

    fn facts<'a>(plan: &'a MessagingPlan, records: &'a HashMap<String, Verified>) -> PlanFacts<'a> {
        PlanFacts {
            directory: &plan.directory,
            apps: &NO_APPS,
            consumers: &plan.wasm_consumers,
            records,
            mqtt_ingress: &plan.mqtt_ingress_channels,
            surfaces: &plan.surfaces,
        }
    }

    /// The delta between two documents, each consumer bound to the artifact its
    /// side's `sha` names.
    fn delta_between(a: &BrennConfig, b: &BrennConfig, sha_a: &str, sha_b: &str) -> PlanDelta {
        let (plan_a, plan_b) = (plan_of(a), plan_of(b));
        let (records_a, records_b) = (records(&plan_a, sha_a), records(&plan_b, sha_b));
        plan_delta(
            &facts(&plan_a, &records_a),
            &facts(&plan_b, &records_b),
            BTreeSet::new(),
            &no_agents(),
            &no_live(),
        )
    }

    fn addresses(entries: &[Arc<ChannelEntry>]) -> Vec<&str> {
        entries.iter().map(|e| e.address.as_str()).collect()
    }

    /// A one-component surface writing to one `brenn:` channel — the arm of
    /// the channel closure that survives rule 1, since an output binding holds
    /// no subscriber entry on the channel it writes to.
    fn writer_surface(slug: &str, writes: &str) -> ResolvedSurface {
        brenn_surface_server::fixtures_config::SurfaceFixture::new(slug, "chart")
            .output(writes, "chart", "out")
            .build()
    }

    /// The two facts, with the surface list substituted on both sides: these
    /// fixtures' documents declare no `[[surface]]`, and what is under test is
    /// the wiring from the channel delta into the surface closure.
    fn delta_over_surfaces(
        a: &BrennConfig,
        b: &BrennConfig,
        surfaces_a: &[ResolvedSurface],
        surfaces_b: &[ResolvedSurface],
    ) -> PlanDelta {
        let (plan_a, plan_b) = (plan_of(a), plan_of(b));
        let (records_a, records_b) = (records(&plan_a, "aa"), records(&plan_b, "aa"));
        let facts_a = PlanFacts {
            surfaces: surfaces_a,
            ..facts(&plan_a, &records_a)
        };
        let facts_b = PlanFacts {
            surfaces: surfaces_b,
            ..facts(&plan_b, &records_b)
        };
        plan_delta(
            &facts_a,
            &facts_b,
            BTreeSet::new(),
            &no_agents(),
            &no_live(),
        )
    }

    /// The wiring the surface half of `plan_delta` rests on: the channel delta
    /// is projected to uuids and addresses and handed to the surface closure,
    /// so a surface whose bound channel was retuned is promoted even though its
    /// own declaration is untouched.
    #[test]
    fn a_retuned_channel_promotes_the_surface_bound_to_it() {
        let wall = writer_surface("wall", "brenn:work");
        let before = base();
        let mut after = BrennConfig::default();
        after.channels.push(surface_index_channel());
        after
            .channels
            .push(durable("brenn:work", WORK_UUID, 16, Some("the work")));

        let delta = delta_over_surfaces(
            &before,
            &after,
            std::slice::from_ref(&wall),
            std::slice::from_ref(&wall),
        );

        assert!(
            delta.moved_addresses().contains("brenn:work"),
            "the changed entry's address is what the output closure reads",
        );
        assert!(
            delta
                .channel_sides()
                .moved()
                .contains(&delta.channels_changed[0].new.uuid),
            "and its uuid is what the subscription closure reads",
        );
        assert_eq!(
            delta
                .surfaces
                .changed
                .iter()
                .map(|change| change.new.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["wall"],
        );
        assert!(!delta.is_empty());
    }

    /// Both sides of a changed entry are in `moved_addresses`, which is what
    /// lets a surface bound to either name be promoted.
    #[test]
    fn moved_addresses_names_every_entry_in_the_channel_delta() {
        let mut before = base();
        before
            .channels
            .push(durable("brenn:spare", SPARE_UUID, 4, None));
        let mut after = BrennConfig::default();
        after.channels.push(surface_index_channel());
        after
            .channels
            .push(durable("brenn:work", WORK_UUID, 16, Some("the work")));

        let delta = delta_between(&before, &after, "aa", "aa");
        let moved = delta.moved_addresses();
        assert!(moved.contains("brenn:work"), "changed: {moved:?}");
        assert!(moved.contains("brenn:spare"), "removed: {moved:?}");
        assert!(
            !moved.contains("brenn:surface.index"),
            "an untouched entry is not moved: {moved:?}",
        );
    }

    /// `is_empty` gates the "nothing to do" path. The surface term is the only
    /// one a surface-only movement trips, so a delta whose channels and
    /// consumers are identical is still not empty.
    #[test]
    fn a_delta_whose_only_movement_is_a_surface_is_not_empty() {
        let wall = writer_surface("wall", "brenn:work");
        let delta = delta_over_surfaces(&base(), &base(), &[], std::slice::from_ref(&wall));

        assert!(delta.channels_added.is_empty());
        assert!(delta.channels_changed.is_empty());
        assert!(delta.consumers_added.is_empty());
        assert_eq!(
            delta
                .surfaces
                .added
                .iter()
                .map(|s| s.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["wall"],
        );
        assert!(
            !delta.is_empty(),
            "a reload that only re-derives surfaces has something to do",
        );
    }

    #[test]
    fn a_document_that_did_not_move_yields_an_empty_delta() {
        assert!(delta_between(&base(), &base(), "aa", "aa").is_empty());
    }

    #[test]
    fn a_new_channel_and_the_consumer_on_it_are_added() {
        let before = base();
        let mut after = base();
        after
            .channels
            .push(durable("brenn:spare", SPARE_UUID, 4, None));
        after.wasm_consumers = vec![consumer("sifter", &["brenn:spare"])];
        let delta = delta_between(&before, &after, "aa", "aa");
        assert_eq!(addresses(&delta.channels_added), vec!["brenn:spare"]);
        assert_eq!(delta.consumers_added, vec!["sifter".to_string()]);
        assert!(delta.channels_removed.is_empty());
        assert!(delta.channels_changed.is_empty());
    }

    #[test]
    fn a_dropped_channel_and_its_consumer_are_removed() {
        let mut before = base();
        before
            .channels
            .push(durable("brenn:spare", SPARE_UUID, 4, None));
        before.wasm_consumers = vec![consumer("sifter", &["brenn:spare"])];
        let delta = delta_between(&before, &base(), "aa", "aa");
        assert_eq!(addresses(&delta.channels_removed), vec!["brenn:spare"]);
        assert_eq!(delta.consumers_removed, vec!["sifter".to_string()]);
    }

    /// The depth of a channel moved, so the entry is re-created — and the
    /// consumer reading it is re-derived against the new entry even though its
    /// own block is untouched. That is delta closure, and it is what spares the
    /// operator a restart for a retune on a channel only components touch.
    #[test]
    fn a_retuned_channel_promotes_the_consumer_wired_to_it() {
        let sifter = consumer("sifter", &["brenn:work"]);
        let mut before = base();
        before.wasm_consumers = vec![sifter.clone()];
        let mut after = BrennConfig::default();
        after.channels.push(surface_index_channel());
        after
            .channels
            .push(durable("brenn:work", WORK_UUID, 16, Some("the work")));
        after.wasm_consumers = vec![sifter];
        let delta = delta_between(&before, &after, "aa", "aa");
        assert_eq!(
            delta
                .channels_changed
                .iter()
                .map(|c| c.new.address.as_str())
                .collect::<Vec<_>>(),
            vec!["brenn:work"],
        );
        assert_eq!(delta.consumers_changed, vec!["sifter".to_string()]);
    }

    /// A `description` carries no routing, so an entry that differs only in it
    /// is edited in place rather than re-created — and nothing wired to it
    /// moves.
    #[test]
    fn a_description_only_edit_is_an_update_and_not_a_change() {
        let sifter = consumer("sifter", &["brenn:work"]);
        let mut before = base();
        before.wasm_consumers = vec![sifter.clone()];
        let mut after = BrennConfig::default();
        after.channels.push(surface_index_channel());
        after
            .channels
            .push(durable("brenn:work", WORK_UUID, 4, Some("the werk")));
        after.wasm_consumers = vec![sifter];
        let delta = delta_between(&before, &after, "aa", "aa");
        assert_eq!(addresses(&delta.channels_described), vec!["brenn:work"]);
        assert!(delta.channels_changed.is_empty());
        assert!(delta.consumers_changed.is_empty());
        assert!(!delta.is_empty());
    }

    /// The subscriber list is not part of an entry's identity: a component
    /// arriving on a channel joins it rather than re-creating it.
    #[test]
    fn a_consumer_arriving_on_a_channel_does_not_change_the_channel() {
        let mut after = base();
        after.wasm_consumers = vec![consumer("sifter", &["brenn:work"])];
        let delta = delta_between(&base(), &after, "aa", "aa");
        assert_eq!(delta.consumers_added, vec!["sifter".to_string()]);
        assert!(delta.channels_changed.is_empty());
        assert!(delta.channels_added.is_empty());
    }

    /// The document is unmoved and the bundle under it is not: the record
    /// comparison is the only thing that can see it, and without it the process
    /// would keep executing bytes the roots no longer hold.
    #[test]
    fn a_package_that_moved_under_an_unmoved_consumer_is_changed() {
        let mut config = base();
        config.wasm_consumers = vec![consumer("sifter", &["brenn:work"])];
        let delta = delta_between(&config, &config, "aa", "bb");
        assert_eq!(delta.consumers_changed, vec!["sifter".to_string()]);
        assert!(delta.channels_changed.is_empty());
    }

    /// An `io` port with no channel mints an entry named by nothing in the
    /// document, and it takes part in the delta like any other: adding the
    /// consumer adds its auto channel.
    #[test]
    fn an_auto_channel_from_an_io_port_participates() {
        let mut after = base();
        let mut sifter = consumer("sifter", &["brenn:work"]);
        sifter.declared_out_ports = vec!["tick".to_string()];
        sifter.io_ports = vec![brenn_messaging_boot::test_fixtures::io_port_raw(
            "tick",
            None,
            Depth::Bounded(1),
            Depth::Bounded(2),
        )];
        after.wasm_consumers = vec![sifter];
        let delta = delta_between(&base(), &after, "aa", "aa");
        assert_eq!(delta.consumers_added, vec!["sifter".to_string()]);
        assert_eq!(
            delta.channels_added.len(),
            1,
            "{:?}",
            addresses(&delta.channels_added)
        );
        assert_eq!(
            delta.channels_added[0].transport_type,
            ChannelScheme::Local,
            "an anonymous `io` port mints a confined auto channel: {}",
            delta.channels_added[0].address,
        );
    }

    /// The operator edited the consumer's own block. This is the most ordinary
    /// reload there is — a changed `config` map, a widened ACL, a retuned
    /// pacing, a deeper input window — and each of them has to make the
    /// consumer `changed` on the resolved value alone, with no channel moving.
    /// `ResolvedWasmConsumer`'s equality is derived over a large struct, so a
    /// member that stopped taking part would let the process keep running a
    /// configuration the document no longer describes, reported as `unchanged`.
    #[test]
    fn every_edit_to_a_consumers_own_block_makes_it_changed() {
        /// One edit to a consumer's block, by the name the case reports.
        type Edit = (&'static str, fn(&mut WasmConsumerConfigRaw));

        let edits: Vec<Edit> = vec![
            ("an input port's noise level", |c| {
                c.subscriptions[0].noise = Some(NoiseLevel::Alarm);
            }),
            ("an input port's amplification", |c| {
                c.subscriptions[0].amplification = Some(0.5);
            }),
            ("a subscribe ACL clause", |c| {
                c.subscribe_acl
                    .push(brenn_lib::access::raw::ChannelMatcherRaw::Prefix(
                        "spare.".to_string(),
                    ));
            }),
            ("the activation pacing", |c| {
                c.activation_burst = Some(9);
            }),
            ("an input port's push depth", |c| {
                c.subscriptions[0].push_depth = Some(Depth::Bounded(3));
            }),
        ];
        for (what, edit) in edits {
            let mut before = base();
            before.wasm_consumers = vec![consumer("sifter", &["brenn:work"])];
            let mut after = base();
            let mut edited = consumer("sifter", &["brenn:work"]);
            edit(&mut edited);
            after.wasm_consumers = vec![edited];

            let delta = delta_between(&before, &after, "aa", "aa");
            assert_eq!(
                delta.consumers_changed,
                vec!["sifter".to_string()],
                "{what} must make the consumer changed",
            );
            assert!(delta.channels_changed.is_empty(), "{what}");
            assert!(delta.channels_added.is_empty(), "{what}");
            assert!(delta.channels_removed.is_empty(), "{what}");
        }
    }

    /// A `link` is one of the three blocks a reload converges, and the entry it
    /// mints is nobody's declaration — so the delta has to see it like any
    /// other entry, and rule 3 has to admit it. A link edit that minted nothing
    /// visible would land as `unchanged` with the wiring not there.
    #[test]
    fn a_link_derived_entry_participates_and_converges() {
        use brenn_lib::messaging::config::{
            LinkConfigRaw, LinkEndpointRaw, LinkHostRaw, WasmConsumerOutputRaw,
            WasmConsumerSubscriptionRaw,
        };

        let mut after = base();
        let mut producer = consumer("emitter", &["brenn:work"]);
        producer.declared_out_ports = vec!["out".to_string()];
        producer.outputs = vec![WasmConsumerOutputRaw {
            port: "out".to_string(),
            channel: None,
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }];
        let mut reader = consumer("reader", &[]);
        reader.subscriptions = vec![WasmConsumerSubscriptionRaw {
            channel: None,
            port: "in".to_string(),
            push_depth: Some(Depth::Bounded(4)),
            retain_depth: Some(Depth::Bounded(4)),
            noise: None,
            wake_min: None,
            amplification: None,
        }];
        after.wasm_consumers = vec![producer, reader];
        after.links = vec![LinkConfigRaw {
            link: "hand-off".to_string(),
            description: None,
            endpoints: vec![
                LinkEndpointRaw {
                    host: LinkHostRaw::Wasm {
                        slug: "emitter".to_string(),
                    },
                    port: "out".to_string(),
                    publishes: true,
                    subscribes: false,
                    io_port: false,
                    push_depth: None,
                    retain_depth: None,
                },
                LinkEndpointRaw {
                    host: LinkHostRaw::Wasm {
                        slug: "reader".to_string(),
                    },
                    port: "in".to_string(),
                    publishes: false,
                    subscribes: true,
                    io_port: false,
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(4)),
                },
            ],
        }];

        let before = base();
        let (plan_a, plan_b) = (plan_of(&before), plan_of(&after));
        let (records_a, records_b) = (records(&plan_a, "aa"), records(&plan_b, "aa"));
        let (old, new) = (facts(&plan_a, &records_a), facts(&plan_b, &records_b));
        let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());

        assert_eq!(
            delta.consumers_added,
            vec!["emitter".to_string(), "reader".to_string()]
        );
        assert_eq!(
            delta.channels_added.len(),
            1,
            "{:?}",
            addresses(&delta.channels_added)
        );
        assert_eq!(
            delta.channels_added[0].transport_type,
            ChannelScheme::Local,
            "the link's anonymous channel is confined: {}",
            delta.channels_added[0].address,
        );
        // Every subscriber on the new entry is a consumer the same delta brings
        // into service, which is rule 1's accepted shape.
        assert!(convergibility_refusals(&old, &new, &delta, &plan_a.directory).is_empty());
    }

    /// The `mqtt:` ingress population is derived from the candidate document,
    /// so a consumer subscription that is the sole minter of one appearing or
    /// disappearing moves that entry — and that move converges: the broker's
    /// SUBSCRIBE set and the ingress route table are both runtime-mutable, so
    /// rule 3 lets the entry through and the MQTT half of the delta carries the
    /// filter and the route it needs.
    #[test]
    fn a_consumer_that_is_the_sole_minter_of_an_mqtt_entry_converges() {
        const TOPIC: &str = "mqtt:ha:home/+/state";
        let without = {
            let mut config = base();
            config
                .mqtt_clients
                .push(brenn_messaging_boot::test_fixtures::minimal_mqtt_client(
                    "ha",
                ));
            config
        };
        let with = {
            let mut config = without.clone();
            let mut listener = consumer("listener", &[]);
            listener.subscriptions =
                vec![brenn_lib::messaging::config::WasmConsumerSubscriptionRaw {
                    channel: Some(TOPIC.to_string()),
                    port: "in".to_string(),
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(4)),
                    noise: None,
                    wake_min: None,
                    amplification: None,
                }];
            listener.mqtt_subscribe_acl = vec![brenn_lib::access::raw::MqttSubMatcherRaw {
                client: "ha".to_string(),
                topic_filter: "home/#".to_string(),
            }];
            config.wasm_consumers = vec![listener];
            config
        };

        for (before, after, direction) in [(&without, &with, "added"), (&with, &without, "removed")]
        {
            let (plan_a, plan_b) = (plan_of(before), plan_of(after));
            let (records_a, records_b) = (records(&plan_a, "aa"), records(&plan_b, "aa"));
            let (old, new) = (facts(&plan_a, &records_a), facts(&plan_b, &records_b));
            let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
            let moved: Vec<&str> = if direction == "added" {
                addresses(&delta.channels_added)
            } else {
                addresses(&delta.channels_removed)
            };
            assert_eq!(moved, vec![TOPIC], "the entry must be {direction}");
            let refusals = convergibility_refusals(&old, &new, &delta, &plan_a.directory);
            assert!(
                refusals.is_empty(),
                "{direction}: rule 3 no longer holds mqtt back: {refusals:?}",
            );
            // The filter and the route follow the entry, in the direction it
            // moved. The client has a session either way: it is declared.
            let client = &delta.mqtt.clients[0];
            let (subscribed, unsubscribed) = (delta.mqtt.subscribed(), delta.mqtt.unsubscribed());
            assert_eq!(client.client, "ha");
            if direction == "added" {
                assert_eq!(subscribed, vec![TOPIC.to_string()]);
                assert!(unsubscribed.is_empty(), "{unsubscribed:?}");
                assert_eq!(delta.mqtt.routes_added.len(), 1);
                assert!(delta.mqtt.routes_removed.is_empty());
            } else {
                assert_eq!(unsubscribed, vec![TOPIC.to_string()]);
                assert!(subscribed.is_empty(), "{subscribed:?}");
                assert!(delta.mqtt.routes_added.is_empty());
                assert_eq!(delta.mqtt.routes_removed.len(), 1);
            }
        }
    }

    /// Rule 2's departing arm on an `mqtt:` address: the shared-filter case
    /// `unsubscribe_filter`'s "only when the last subscriber leaves" contract
    /// is about. A dynamic subscription sitting on a config channel's filter is
    /// a live subscriber the plan does not hold, so dropping that channel is
    /// refused — which is what keeps the walk from issuing an UNSUBSCRIBE that
    /// would take the dynamic subscriber's deliveries with it.
    #[test]
    fn dropping_an_mqtt_channel_a_dynamic_subscriber_sits_on_is_refused() {
        const ADDRESS: &str = "mqtt:ha:home/state";
        let uuid = Uuid::from_u128(7);
        let mut planned = entry(ADDRESS, uuid, 4, vec![]);
        planned.transport_type = ChannelScheme::Mqtt;
        // The live entry is the same channel with the dynamic subscriber's row
        // folded in, which is all `subscribe_dynamic` leaves on an existing
        // config filter.
        let mut live_entry = planned.clone();
        live_entry
            .subscribers
            .push(subscriber(SubscriberEntryKind::App("reader".to_string())));
        let live = one_entry(live_entry);
        let before = one_entry(planned.clone());
        let after = MessagingDirectory::with_entries(vec![]);
        let ingress = vec![ResolvedMqttIngressChannel {
            channel_uuid: uuid,
            channel_address: ADDRESS.to_string(),
            client_slug: "ha".to_string(),
            topic: "home/state".to_string(),
            qos: 1,
            urgency: brenn_lib::messaging::Urgency::Normal,
        }];
        let empty = HashMap::new();
        let old = PlanFacts {
            apps: &NO_APPS,
            directory: &before,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &ingress,
            surfaces: &[],
        };
        let new = PlanFacts {
            apps: &NO_APPS,
            directory: &after,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        // The delta does hold the UNSUBSCRIBE; the refusal is what stops it
        // from ever being issued.
        assert_eq!(delta.mqtt.unsubscribed(), vec![ADDRESS.to_string()]);
        let refusals = convergibility_refusals(&old, &new, &delta, &live);
        assert!(
            refusals.iter().any(
                |line| line.starts_with(&format!("{ADDRESS} is going away but "))
                    && line.ends_with(NEEDS_RESTART)
            ),
            "{refusals:?}",
        );
    }

    /// Rule 2's added arm, over both schemes a dynamic subscription can leave
    /// behind. A document that starts declaring a channel a dynamic subscribe
    /// already minted would reach `add_channel`'s address assert in commit,
    /// which is past the point where anything may decline — and on the `mqtt:`
    /// side it is also what keeps `add_subscription`'s qos assert and
    /// `add_route`'s idempotence assert unreachable from the walk, since
    /// `mqtt_subscribe` mints exactly these entries.
    #[test]
    fn minting_an_address_the_live_directory_already_holds_is_refused() {
        for address in ["brenn:work", "mqtt:ha:home/state"] {
            let mut minted = entry(address, Uuid::from_u128(7), 4, vec![]);
            let ingress = match ChannelScheme::of(address) {
                Some(ChannelScheme::Mqtt) => {
                    minted.transport_type = ChannelScheme::Mqtt;
                    vec![ResolvedMqttIngressChannel {
                        channel_uuid: minted.uuid,
                        channel_address: address.to_string(),
                        client_slug: "ha".to_string(),
                        topic: "home/state".to_string(),
                        qos: 1,
                        urgency: brenn_lib::messaging::Urgency::Normal,
                    }]
                }
                _ => Vec::new(),
            };
            let before = MessagingDirectory::with_entries(vec![]);
            let after = one_entry(minted.clone());
            // The live entry carries a different uuid, which is what a
            // dynamically minted channel on the same address looks like.
            let mut live_entry = entry(address, Uuid::from_u128(9), 4, vec![]);
            live_entry.transport_type = minted.transport_type;
            let live = one_entry(live_entry);
            let empty = HashMap::new();
            let old = PlanFacts {
                apps: &NO_APPS,
                directory: &before,
                consumers: &[],
                records: &empty,
                mqtt_ingress: &[],
                surfaces: &[],
            };
            let new = PlanFacts {
                apps: &NO_APPS,
                directory: &after,
                consumers: &[],
                records: &empty,
                mqtt_ingress: &ingress,
                surfaces: &[],
            };
            let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
            assert_eq!(delta.channels_added.len(), 1);
            assert_eq!(
                convergibility_refusals(&old, &new, &delta, &live),
                vec![format!(
                    "{address} is newly minted but already exists (a dynamic subscription \
                     created it): this change needs a restart"
                )],
            );
        }
    }

    /// A `channel` block addressed at a system-minted channel does not declare
    /// it, it tunes it — and the planner resolves the entry's depths from the
    /// candidate document, so a retune reaches level 2 as an identity change on
    /// an entry of a scheme reload cannot converge.
    #[test]
    fn a_retuned_webhook_entry_is_refused_by_rule_3() {
        let with_tuning = |standing: u64| {
            let mut config = BrennConfig::default();
            config.channels.push(surface_index_channel());
            config.webhook_endpoints = vec![webhook_endpoint_raw("gh-events")];
            let mut tuning = durable("webhook:gh-events", "", standing, None);
            // A tuning block over a system-minted channel states no uuid: the
            // address derives it, and an operator-supplied one could only
            // disagree.
            tuning.uuid = None;
            config.channels.push(tuning);
            config
        };
        let (before, after) = (with_tuning(4), with_tuning(16));
        let (plan_a, plan_b) = (plan_of(&before), plan_of(&after));
        let empty = HashMap::new();
        let delta = plan_delta(
            &facts(&plan_a, &empty),
            &facts(&plan_b, &empty),
            BTreeSet::new(),
            &no_agents(),
            &no_live(),
        );
        assert_eq!(
            delta
                .channels_changed
                .iter()
                .map(|c| c.new.address.as_str())
                .collect::<Vec<_>>(),
            vec!["webhook:gh-events"],
        );
        let refusals = convergibility_refusals(
            &facts(&plan_a, &empty),
            &facts(&plan_b, &empty),
            &delta,
            &plan_a.directory,
        );
        assert_eq!(
            refusals,
            vec!["webhook:gh-events retuned: this change needs a restart".to_string()],
        );
    }

    // ---------------------------------------------------------------------
    // Hand-built directories: the rules half. Every subscriber kind has to be
    // reachable, and two of them are minted at runtime rather than by any
    // document.
    // ---------------------------------------------------------------------

    fn entry(
        address: &str,
        uuid: Uuid,
        standing: u64,
        subscribers: Vec<SubscriberEntry>,
    ) -> ChannelEntry {
        let mut entry = test_channel_entry(address, subscribers);
        entry.uuid = uuid;
        entry.address = address.to_string();
        entry.resolved_channel.standing_retain_depth = Depth::Bounded(standing);
        entry
    }

    fn subscriber(kind: SubscriberEntryKind) -> SubscriberEntry {
        SubscriberEntry {
            kind,
            push_depth: Depth::Bounded(1),
            retain_depth: Depth::Bounded(1),
            noise: NoiseLevel::Metered,
            wake_min: None,
        }
    }

    /// A directory holding one entry, plus the facts a rules check reads
    /// alongside it.
    fn one_entry(entry: ChannelEntry) -> MessagingDirectory {
        MessagingDirectory::with_entries(vec![entry])
    }

    /// The refusals for a retune of one entry whose subscriber list is
    /// `subscribers` on both sides, with no consumer moving.
    fn refusals_for_retune(subscribers: Vec<SubscriberEntry>) -> Vec<String> {
        refusals_for_retune_with(subscribers, &[])
    }

    /// The same, with `moving` naming the consumer slugs the delta takes out of
    /// service and puts back — the set rule 1 measures every subscriber on a
    /// moving entry against.
    ///
    /// The consumer sets are written onto the delta rather than derived from a
    /// consumer list, because what rule 1 reads is the delta: a `PlanFacts`
    /// carrying resolved consumers would test the classification a second time
    /// instead of the rule.
    fn refusals_for_retune_with(subscribers: Vec<SubscriberEntry>, moving: &[&str]) -> Vec<String> {
        let uuid = Uuid::from_u128(7);
        let before = one_entry(entry("brenn:work", uuid, 4, subscribers.clone()));
        let after = one_entry(entry("brenn:work", uuid, 16, subscribers));
        let empty = HashMap::new();
        let old = PlanFacts {
            apps: &NO_APPS,
            directory: &before,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let new = PlanFacts {
            apps: &NO_APPS,
            directory: &after,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let mut delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        assert_eq!(delta.channels_changed.len(), 1);
        delta.consumers_changed = moving.iter().map(|slug| (*slug).to_string()).collect();
        convergibility_refusals(&old, &new, &delta, &before)
    }

    /// One changed agent, as the agent delta would carry it: the fields rule 1
    /// and the corollary assert read are the slug alone.
    fn changed_agent(slug: &str) -> super::super::agents::AgentChange {
        super::super::agents::AgentChange {
            slug: slug.to_string(),
            subs_removed: Vec::new(),
            subs_added: Vec::new(),
            dynamic: crate::reload::dynamic::DynamicRemerge::default(),
            respawn: false,
            virtual_tools_staged: false,
            owner_changed: false,
            previous_owner: None,
            allowed_users: Vec::new(),
            users_restricted: false,
            users_removed: Vec::new(),
        }
    }

    /// The same retune, with `agents` naming the agents the delta moves.
    fn refusals_for_retune_with_agents(
        subscribers: Vec<SubscriberEntry>,
        agents: &[&str],
    ) -> Vec<String> {
        let uuid = Uuid::from_u128(7);
        let before = one_entry(entry("brenn:work", uuid, 4, subscribers.clone()));
        let after = one_entry(entry("brenn:work", uuid, 16, subscribers));
        let empty = HashMap::new();
        let old = PlanFacts {
            apps: &NO_APPS,
            directory: &before,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let new = PlanFacts {
            apps: &NO_APPS,
            directory: &after,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let mut delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        assert_eq!(delta.channels_changed.len(), 1);
        delta.agents_changed = agents.iter().map(|slug| changed_agent(slug)).collect();
        convergibility_refusals(&old, &new, &delta, &before)
    }

    /// Rule 1's agent arm: an agent the delta re-folds onto the re-created
    /// channel is accounted for, and every other kind on the same entry still
    /// refuses. Widening the arm to account *any* agent would silently drop a
    /// non-converging agent's subscription from a channel this reload takes
    /// away and puts back.
    #[test]
    fn a_moving_entry_whose_agent_moves_with_it_is_accepted_while_a_remote_still_refuses() {
        let assistant = subscriber(SubscriberEntryKind::App("assistant".into()));
        let pod = subscriber(SubscriberEntryKind::Remote("pod".into()));

        assert!(
            refusals_for_retune_with_agents(vec![assistant.clone()], &["assistant"]).is_empty(),
            "the agent this delta re-folds is accounted for",
        );
        assert_eq!(
            refusals_for_retune_with_agents(vec![assistant.clone()], &["scribe"]),
            vec![
                "brenn:work is changed but agent \"assistant\" subscribes to it: \
                 this change needs a restart"
                    .to_string(),
            ],
            "another agent moving accounts for nothing here",
        );
        assert_eq!(
            refusals_for_retune_with_agents(vec![assistant, pod], &["assistant"]),
            vec![
                "brenn:work is changed but remote \"pod\" subscribes to it: \
                 this change needs a restart"
                    .to_string(),
            ],
            "a kind no delta walks is refused beside an accounted agent",
        );
    }

    /// The corollary assert, both directions: a changed agent's entries on an
    /// unchanged channel legitimately differ between the two plans, and an
    /// unchanged agent's do not.
    #[test]
    fn the_unchanged_entries_assert_tolerates_a_changed_agent_and_no_other() {
        let uuid = Uuid::from_u128(9);
        let assistant = subscriber(SubscriberEntryKind::App("assistant".into()));
        let before = one_entry(entry("brenn:work", uuid, 4, vec![]));
        let after = one_entry(entry("brenn:work", uuid, 4, vec![assistant]));
        let empty = HashMap::new();
        let old = PlanFacts {
            apps: &NO_APPS,
            directory: &before,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let new = PlanFacts {
            apps: &NO_APPS,
            directory: &after,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let mut delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        delta.agents_changed = vec![changed_agent("assistant")];
        assert!(convergibility_refusals(&old, &new, &delta, &before).is_empty());
    }

    #[test]
    #[should_panic(expected = "did not move but its non-component subscribers did")]
    fn the_unchanged_entries_assert_still_fires_for_an_agent_no_delta_moves() {
        let uuid = Uuid::from_u128(9);
        let assistant = subscriber(SubscriberEntryKind::App("assistant".into()));
        let before = one_entry(entry("brenn:work", uuid, 4, vec![]));
        let after = one_entry(entry("brenn:work", uuid, 4, vec![assistant]));
        let empty = HashMap::new();
        let old = PlanFacts {
            apps: &NO_APPS,
            directory: &before,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let new = PlanFacts {
            apps: &NO_APPS,
            directory: &after,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        let _ = convergibility_refusals(&old, &new, &delta, &before);
    }

    /// Rule 1's accepted shape, which every successful reload has: the entry
    /// moves, and the only subscribers on it are components the same delta
    /// retires and restarts. An inverted or slug-blind `accounted` would refuse
    /// every real reload — or admit a channel being torn out from under a
    /// component that is staying put — and no other test here would notice,
    /// because they all run with an empty consumer delta.
    #[test]
    fn a_moving_entry_whose_only_subscriber_moves_with_it_is_accepted() {
        let sifter = subscriber(SubscriberEntryKind::Wasm("sifter".into()));
        assert!(refusals_for_retune_with(vec![sifter.clone()], &["sifter"]).is_empty());

        // A delta naming some other consumer accounts for nothing here.
        assert_eq!(
            refusals_for_retune_with(vec![sifter.clone()], &["other"]),
            vec![
                "brenn:work is changed but component \"sifter\" subscribes to it: \
                 this change needs a restart"
                    .to_string(),
            ],
        );

        // A stationary agent beside a moving component: one refusal, and it
        // names the agent.
        let refusals = refusals_for_retune_with(
            vec![
                sifter,
                subscriber(SubscriberEntryKind::App("assistant".into())),
            ],
            &["sifter"],
        );
        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(refusals[0].contains("agent \"assistant\""), "{refusals:?}");
    }

    /// Rule 1 admits a surface on the same terms it admits a consumer: the
    /// commit retires it before the channels move and starts it after, folding
    /// its entries back on from the candidate's plan. A surface the delta does
    /// *not* move is still a refusal — nothing would re-wire it.
    #[test]
    fn a_moving_entry_whose_only_subscriber_is_a_moving_surface_is_accepted() {
        let uuid = Uuid::from_u128(11);
        let wall = subscriber(SubscriberEntryKind::Surface("wall".into()));
        let before = one_entry(entry("brenn:work", uuid, 4, vec![wall.clone()]));
        let after = one_entry(entry("brenn:work", uuid, 16, vec![wall]));
        let empty = HashMap::new();
        let facts = |directory| PlanFacts {
            apps: &NO_APPS,
            directory,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let old = facts(&before);
        let new = facts(&after);
        let surface =
            || brenn_surface_server::fixtures_config::SurfaceFixture::new("wall", "chart").build();

        let stationary = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        assert_eq!(
            convergibility_refusals(&old, &new, &stationary, &before),
            vec![
                "brenn:work is changed but surface \"wall\" subscribes to it: \
                 this change needs a restart"
                    .to_string(),
            ],
        );

        let mut moving = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        moving
            .surfaces
            .changed
            .push(crate::reload::surfaces::SurfaceChange {
                old: surface(),
                new: surface(),
            });
        assert!(
            convergibility_refusals(&old, &new, &moving, &before).is_empty(),
            "a surface the delta retires and starts accounts for its own entries",
        );
    }

    /// The corollary assert's surface filter, on the shape it exists for: a
    /// surface that joins or leaves a channel the delta does not move.
    ///
    /// `same_channel` ignores subscribers, so a channel whose only difference
    /// is which surfaces sit on it is in neither `moved` nor the channel delta,
    /// and the corollary compares the two sides' non-consumer subscribers
    /// directly. A surface the surface delta accounts for is filtered out of
    /// both sides; one it does not is the non-convergible entity the assert is
    /// there to catch.
    #[test]
    fn a_surface_leaving_an_unmoved_channel_is_accounted_for_by_the_surface_delta() {
        let uuid = Uuid::from_u128(23);
        let wall = subscriber(SubscriberEntryKind::Surface("wall".into()));
        let agent = subscriber(SubscriberEntryKind::App("assistant".into()));
        let before = one_entry(entry("brenn:work", uuid, 4, vec![agent.clone(), wall]));
        let after = one_entry(entry("brenn:work", uuid, 4, vec![agent]));
        let empty = HashMap::new();
        let facts = |directory| PlanFacts {
            apps: &NO_APPS,
            directory,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let old = facts(&before);
        let new = facts(&after);

        let mut delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        assert!(
            delta.channels_changed.is_empty() && delta.channels_removed.is_empty(),
            "the channel itself did not move",
        );
        delta.surfaces.removed.push(
            brenn_surface_server::fixtures_config::SurfaceFixture::new("wall", "chart").build(),
        );
        assert!(
            convergibility_refusals(&old, &new, &delta, &before).is_empty(),
            "the surface delta retires this surface, so its entries are the walk\'s own",
        );
    }

    /// The other half of the same filter: a surface the delta says nothing
    /// about cannot appear or vanish on an unmoved channel. Nothing in the
    /// document can produce this, which is why it is an assert and not a
    /// refusal — and why it has to stay one.
    #[test]
    #[should_panic(expected = "did not move but its non-component subscribers did")]
    fn a_surface_leaving_an_unmoved_channel_without_a_surface_delta_is_a_host_bug() {
        let uuid = Uuid::from_u128(24);
        let wall = subscriber(SubscriberEntryKind::Surface("wall".into()));
        let agent = subscriber(SubscriberEntryKind::App("assistant".into()));
        let before = one_entry(entry("brenn:work", uuid, 4, vec![agent.clone(), wall]));
        let after = one_entry(entry("brenn:work", uuid, 4, vec![agent]));
        let empty = HashMap::new();
        let facts = |directory| PlanFacts {
            apps: &NO_APPS,
            directory,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let old = facts(&before);
        let new = facts(&after);
        let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        let _ = convergibility_refusals(&old, &new, &delta, &before);
    }

    /// A changed entry is read on both sides, so a subscriber sitting on it in
    /// both plans would state its refusal twice. One problem, one line — and
    /// two problems, two lines, so the deduplication is not collapsing distinct
    /// refusals.
    #[test]
    fn one_problem_is_one_line_and_two_are_two() {
        assert_eq!(
            refusals_for_retune(vec![subscriber(SubscriberEntryKind::App(
                "assistant".into()
            ))])
            .len(),
            1,
            "the same subscriber on both sides of a change is one refusal",
        );
        let two = refusals_for_retune(vec![
            subscriber(SubscriberEntryKind::App("assistant".into())),
            subscriber(SubscriberEntryKind::Surface("wall".into())),
        ]);
        assert_eq!(two.len(), 2, "{two:?}");
    }

    #[test]
    fn every_foreign_subscriber_kind_on_a_moving_entry_is_named() {
        let cases = [
            (
                SubscriberEntryKind::App("assistant".into()),
                "agent \"assistant\"",
            ),
            (
                SubscriberEntryKind::Surface("wall".into()),
                "surface \"wall\"",
            ),
            (SubscriberEntryKind::Remote("pod".into()), "remote \"pod\""),
            (
                SubscriberEntryKind::System("config-reload".into()),
                "the \"config-reload\" system participant",
            ),
            (
                SubscriberEntryKind::ChatConversation {
                    app_slug: "assistant".into(),
                    conversation_id: 12,
                },
                "conversation 12 of agent \"assistant\"",
            ),
        ];
        for (kind, described) in cases {
            let refusals = refusals_for_retune(vec![subscriber(kind.clone())]);
            assert!(
                refusals.iter().any(|r| r.contains(described)),
                "{kind:?} must be named in {refusals:?}",
            );
            assert!(
                refusals.iter().all(|r| r.ends_with(NEEDS_RESTART)),
                "{refusals:?}",
            );
        }
    }

    /// A component's own subscriber entry on a channel that is moving is fine
    /// exactly when the component moves with it. Nothing moves here, so it is
    /// not.
    #[test]
    fn a_component_that_is_not_moving_is_refused_like_any_other_subscriber() {
        let refusals =
            refusals_for_retune(vec![subscriber(SubscriberEntryKind::Wasm("sifter".into()))]);
        assert_eq!(
            refusals,
            vec![
                "brenn:work is changed but component \"sifter\" subscribes to it: \
                 this change needs a restart"
                    .to_string(),
            ],
        );
    }

    /// The motivating shape: an agent already reads the channel, a component
    /// arrives on it. The entry does not move, so rules 1 and 2 never look at
    /// it and the agent's subscription is untouched.
    #[test]
    fn a_component_joining_a_channel_an_agent_reads_is_not_refused() {
        let uuid = Uuid::from_u128(7);
        let agent = subscriber(SubscriberEntryKind::App("assistant".into()));
        let before = one_entry(entry("brenn:work", uuid, 4, vec![agent.clone()]));
        let after = one_entry(entry(
            "brenn:work",
            uuid,
            4,
            vec![
                agent,
                subscriber(SubscriberEntryKind::Wasm("sifter".into())),
            ],
        ));
        let empty = HashMap::new();
        let old = PlanFacts {
            apps: &NO_APPS,
            directory: &before,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let new = PlanFacts {
            apps: &NO_APPS,
            directory: &after,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        assert!(delta.is_empty(), "a subscriber list is not an identity");
        assert!(convergibility_refusals(&old, &new, &delta, &before).is_empty());
    }

    /// Rule 2 is the one that reads the process rather than the plan: a
    /// subscription minted after boot — a dynamic app row, an attached surface
    /// — is invisible to both plans and is exactly what a boot-shaped
    /// comparison would take a channel out from under.
    #[test]
    fn a_subscriber_only_the_live_directory_knows_about_is_refused() {
        let uuid = Uuid::from_u128(7);
        let before = one_entry(entry("brenn:work", uuid, 4, vec![]));
        let after = MessagingDirectory::with_entries(vec![]);
        let live = one_entry(entry(
            "brenn:work",
            uuid,
            4,
            vec![subscriber(SubscriberEntryKind::Surface("wall".into()))],
        ));
        let empty = HashMap::new();
        let old = PlanFacts {
            apps: &NO_APPS,
            directory: &before,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let new = PlanFacts {
            apps: &NO_APPS,
            directory: &after,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        assert_eq!(delta.channels_removed.len(), 1);
        assert_eq!(
            convergibility_refusals(&old, &new, &delta, &live),
            vec![
                "brenn:work is going away but surface \"wall\" subscribes to it right now: \
                 this change needs a restart"
                    .to_string(),
            ],
        );
    }

    /// The corollary of rule 1: a non-component subscriber that appears on an
    /// entry nothing moved cannot have come from the document, because level 1
    /// proved the two documents agree about every entity that mints one.
    #[test]
    #[should_panic(expected = "did not move but its non-component subscribers did")]
    fn a_foreign_subscriber_appearing_on_an_unchanged_entry_is_a_host_bug() {
        let uuid = Uuid::from_u128(7);
        let before = one_entry(entry("brenn:work", uuid, 4, vec![]));
        let after = one_entry(entry(
            "brenn:work",
            uuid,
            4,
            vec![subscriber(SubscriberEntryKind::App("assistant".into()))],
        ));
        let empty = HashMap::new();
        let old = PlanFacts {
            apps: &NO_APPS,
            directory: &before,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let new = PlanFacts {
            apps: &NO_APPS,
            directory: &after,
            consumers: &[],
            records: &empty,
            mqtt_ingress: &[],
            surfaces: &[],
        };
        let delta = plan_delta(&old, &new, BTreeSet::new(), &no_agents(), &no_live());
        convergibility_refusals(&old, &new, &delta, &before);
    }

    // -----------------------------------------------------------------------
    // The dynamic `mqtt:` projection at the seam: which filters each side of
    // this reload stands behind.
    // -----------------------------------------------------------------------

    /// A durable `mqtt:` row as a dynamic subscribe leaves it, with the
    /// SUBSCRIBE QoS the projection re-asserts the filter at.
    fn mqtt_row(uuid: Uuid, slug: &str) -> brenn_lib::messaging::DynamicSubscriptionRow {
        brenn_lib::messaging::DynamicSubscriptionRow {
            channel_uuid: uuid,
            app_slug: slug.to_string(),
            push_depth: Depth::Bounded(0),
            retain_depth: Depth::Bounded(1),
            noise: NoiseLevel::Metered,
            wake_min: brenn_lib::messaging::WakeMin::Never,
            qos: Some(1),
            created_at: "2026-09-07T00:00:00Z".to_string(),
        }
    }

    /// One changed agent whose only move is the revoke of `row`.
    fn revoking(
        slug: &str,
        address: &str,
        row: &brenn_lib::messaging::DynamicSubscriptionRow,
    ) -> AgentChange {
        AgentChange {
            slug: slug.to_string(),
            subs_removed: Vec::new(),
            subs_added: Vec::new(),
            dynamic: crate::reload::dynamic::DynamicRemerge {
                revoke: vec![crate::reload::dynamic::DynamicRevoke {
                    moved: crate::reload::dynamic::DynamicMove {
                        channel_uuid: row.channel_uuid,
                        address: address.to_string(),
                        row: Some(row.clone()),
                    },
                    reason: crate::reload::dynamic::RevokeReason::AclDenies,
                }],
                revive: Vec::new(),
                prune: Vec::new(),
            },
            respawn: false,
            virtual_tools_staged: false,
            owner_changed: false,
            previous_owner: None,
            allowed_users: Vec::new(),
            users_restricted: false,
            users_removed: Vec::new(),
        }
    }

    /// Two agents hold one `mqtt:` channel folded and the reload revokes one of
    /// them. One channel is one filter and one route, so the candidate's set
    /// still carries it: unsubscribing here would take the broker filter and
    /// the ingress route out from under the agent that keeps the subscription,
    /// which no test above can see because every `mqtt_delta` case is handed a
    /// set built by hand and every broker case has one agent on the channel.
    #[test]
    fn a_revoke_by_one_agent_leaves_a_filter_another_still_holds() {
        const ADDRESS: &str = "mqtt:ha:home/state";
        let mut entry = test_channel_entry(ADDRESS, vec![]);
        // Spelled rather than canonicalized, and folded by hand: this is the
        // state a pair of dynamic subscribes leaves, which no plan describes.
        entry.address = ADDRESS.to_string();
        entry.transport_type = ChannelScheme::Mqtt;
        entry.subscribers = ["reader", "writer"]
            .into_iter()
            .map(|slug| SubscriberEntry {
                kind: SubscriberEntryKind::App(slug.to_string()),
                push_depth: Depth::Bounded(0),
                retain_depth: Depth::Bounded(1),
                noise: NoiseLevel::Metered,
                wake_min: Some(brenn_lib::messaging::WakeMin::Never),
            })
            .collect();
        let uuid = entry.uuid;
        let directory = MessagingDirectory::with_entries(vec![entry]);
        let rows = vec![mqtt_row(uuid, "reader"), mqtt_row(uuid, "writer")];
        let dynamic = DynamicSnapshot {
            rows: rows.clone(),
            nondurable: Vec::new(),
        };
        let clients: IndexMap<String, brenn_lib::mqtt::config::MqttClientIdentity> =
            IndexMap::from([(
                "ha".to_string(),
                brenn_lib::mqtt::test_support::test_client_identity("ha"),
            )]);
        let live = LiveFacts {
            directory: &directory,
            dynamic: &dynamic,
            mqtt_clients: &clients,
        };
        // Two plans that declare nothing: the filter under test is purely
        // dynamic, so it is in neither side's static ingress list.
        let no_consumers: Vec<ResolvedWasmConsumer> = Vec::new();
        let no_records: HashMap<String, Verified> = HashMap::new();
        let no_ingress: Vec<ResolvedMqttIngressChannel> = Vec::new();
        let no_surfaces: Vec<ResolvedSurface> = Vec::new();
        let facts = PlanFacts {
            directory: &EMPTY_DIRECTORY,
            apps: &NO_APPS,
            consumers: &no_consumers,
            records: &no_records,
            mqtt_ingress: &no_ingress,
            surfaces: &no_surfaces,
        };

        let mut delta = PlanDelta {
            agents_changed: vec![revoking("reader", ADDRESS, &rows[0])],
            ..PlanDelta::default()
        };
        let (before, after) = dynamic_ingress_sides(&facts, &facts, &delta, &live);
        assert_eq!(before.len(), 1, "one filter for the two folded rows");
        assert_eq!(
            after.iter().map(|c| c.channel_uuid).collect::<Vec<_>>(),
            vec![uuid],
            "the writer still holds it folded, so the candidate still needs it",
        );

        // Both revoked, and the filter leaves: the withdrawal is keyed on the
        // pair, so it takes exactly the rows the two agents' re-merges name.
        delta
            .agents_changed
            .push(revoking("writer", ADDRESS, &rows[1]));
        let (_, after) = dynamic_ingress_sides(&facts, &facts, &delta, &live);
        assert!(after.is_empty(), "nobody holds it any more");
    }
}

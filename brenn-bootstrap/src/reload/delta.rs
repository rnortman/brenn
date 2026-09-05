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

use brenn_lib::messaging::config::ResolvedWasmConsumer;
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
};
use brenn_lib::wasm_package::Verified;
use uuid::Uuid;

use super::NEEDS_RESTART;

/// One side of the comparison: everything a reload reads off a plan.
///
/// `records` holds the package binding of every consumer named in `consumers`,
/// keyed by slug — for the baseline what the running consumer was loaded from,
/// for the candidate what re-resolving its package on disk says it would be
/// loaded from now. Comparing them is what makes a bundle upgrade under an
/// unchanged document a change rather than an invisible drift.
pub(crate) struct PlanFacts<'a> {
    pub directory: &'a MessagingDirectory,
    pub consumers: &'a [ResolvedWasmConsumer],
    pub records: &'a HashMap<String, Verified>,
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
    }

    /// The uuids of every entry in the *channel delta* — added, removed or
    /// changed. Description updates are deliberately absent: they change no
    /// wiring, so they promote no consumer and they answer to no rule.
    fn moved_channels(&self) -> HashSet<Uuid> {
        self.channels_added
            .iter()
            .map(|e| e.uuid)
            .chain(self.channels_removed.iter().map(|e| e.uuid))
            .chain(self.channels_changed.iter().map(|c| c.new.uuid))
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
pub(crate) fn plan_delta(baseline: &PlanFacts<'_>, candidate: &PlanFacts<'_>) -> PlanDelta {
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
    let moved = delta.moved_channels();
    for consumer in candidate.consumers {
        match old_consumers.get(consumer.slug.as_str()) {
            None => delta.consumers_added.push(consumer.slug.clone()),
            Some(old) => {
                let resolved_moved = *old != consumer;
                let record_moved =
                    baseline.records.get(&consumer.slug) != candidate.records.get(&consumer.slug);
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
    delta
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
    let moved = delta.moved_channels();
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
    // delta must belong to a consumer that is itself moving, because a
    // re-created entry re-wires its subscribers and nothing else here can be
    // re-wired.
    for entry in &delta.channels_added {
        rule_1(entry, "added", &arriving, &mut out);
    }
    for entry in &delta.channels_removed {
        rule_1(entry, "removed", &departing, &mut out);
    }
    for change in &delta.channels_changed {
        rule_1(&change.old, "changed", &departing, &mut out);
        rule_1(&change.new, "changed", &arriving, &mut out);
    }

    // Rule 2: the same question asked of the directory as it actually stands.
    out.extend(live_subscriber_refusals(delta, live));

    assert_unchanged_entries_agree(baseline, candidate, &moved);
    // A changed entry is read on both sides, so a subscriber that sits on it in
    // both plans states its refusal twice. One problem, one line.
    let mut seen = HashSet::new();
    out.retain(|line| seen.insert(line.clone()));
    out
}

/// Rule 2 alone: what the directory as it actually stands holds on a channel
/// this delta takes away, that the baseline plan did not.
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
    let mut out = Vec::new();
    for entry in delta
        .channels_removed
        .iter()
        .chain(delta.channels_changed.iter().map(|c| &c.old))
    {
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
            if !planned.contains(&subscriber.kind) && !accounted(&subscriber.kind, &departing) {
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

/// Rule 3: only the three declarable schemes converge.
///
/// A `webhook:` or `mqtt:` entry reaches the channel delta whenever a
/// convergible block moves what mints it — a tuning block retuning the entry,
/// or the consumer subscription that was its sole minter appearing or leaving.
/// The broker's SUBSCRIBE union and the HTTP layer's mount table are built once
/// at boot, so the entry cannot follow.
fn rule_3(entry: &ChannelEntry, what: &str, out: &mut Vec<String>) {
    match entry.transport_type {
        ChannelScheme::Brenn | ChannelScheme::Ephemeral | ChannelScheme::Local => {}
        _ => out.push(format!("{} {what}: {NEEDS_RESTART}", entry.address)),
    }
}

/// Rule 1: every subscriber on a moving entry must be a consumer that moves
/// with it.
fn rule_1(entry: &ChannelEntry, what: &str, moving: &HashSet<&str>, out: &mut Vec<String>) {
    for subscriber in &entry.subscribers {
        if !accounted(&subscriber.kind, moving) {
            out.push(format!(
                "{} is {what} but {} subscribes to it: {NEEDS_RESTART}",
                entry.address,
                describe(&subscriber.kind),
            ));
        }
    }
}

/// Whether a subscriber on a moving entry is one the consumer delta already
/// takes out of service and puts back.
fn accounted(kind: &SubscriberEntryKind, moving: &HashSet<&str>) -> bool {
    matches!(kind, SubscriberEntryKind::Wasm(slug) if moving.contains(slug.as_str()))
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
) {
    let old_entries = Entries::of(baseline.directory);
    for entry in candidate.directory.list() {
        if moved.contains(&entry.uuid) {
            continue;
        }
        let Some(old) = old_entries.by_uuid.get(&entry.uuid) else {
            continue;
        };
        let old_foreign = foreign_subscribers(old);
        let new_foreign = foreign_subscribers(&entry);
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

/// Every subscriber on an entry that is not a WASM consumer.
///
/// Kinds, not their rendered text: the identity of a subscriber is the value,
/// and refusal wording is free to change without silently making two of them
/// compare equal.
fn foreign_subscribers(entry: &ChannelEntry) -> HashSet<&SubscriberEntryKind> {
    entry
        .subscribers
        .iter()
        .filter(|s| !matches!(s.kind, SubscriberEntryKind::Wasm(_)))
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
            consumers: &plan.wasm_consumers,
            records,
        }
    }

    /// The delta between two documents, each consumer bound to the artifact its
    /// side's `sha` names.
    fn delta_between(a: &BrennConfig, b: &BrennConfig, sha_a: &str, sha_b: &str) -> PlanDelta {
        let (plan_a, plan_b) = (plan_of(a), plan_of(b));
        let (records_a, records_b) = (records(&plan_a, sha_a), records(&plan_b, sha_b));
        plan_delta(&facts(&plan_a, &records_a), &facts(&plan_b, &records_b))
    }

    fn addresses(entries: &[Arc<ChannelEntry>]) -> Vec<&str> {
        entries.iter().map(|e| e.address.as_str()).collect()
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
        let delta = plan_delta(&old, &new);

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
    /// disappearing moves that entry — under a scheme reload cannot converge,
    /// because the broker's SUBSCRIBE union and the ingress route table are
    /// built once at boot. The *remove* direction is the dangerous one: absent
    /// the refusal the reload lands as applied and the directory keeps an entry
    /// a fresh boot would not have.
    #[test]
    fn a_consumer_that_is_the_sole_minter_of_an_mqtt_entry_is_refused_by_rule_3() {
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
            let delta = plan_delta(&old, &new);
            let moved: Vec<&str> = if direction == "added" {
                addresses(&delta.channels_added)
            } else {
                addresses(&delta.channels_removed)
            };
            assert_eq!(moved, vec![TOPIC], "the entry must be {direction}");
            let refusals = convergibility_refusals(&old, &new, &delta, &plan_a.directory);
            assert!(
                refusals
                    .iter()
                    .any(|line| line.starts_with(TOPIC) && line.ends_with(NEEDS_RESTART)),
                "{direction}: {refusals:?}",
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
        let delta = plan_delta(&facts(&plan_a, &empty), &facts(&plan_b, &empty));
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
            directory: &before,
            consumers: &[],
            records: &empty,
        };
        let new = PlanFacts {
            directory: &after,
            consumers: &[],
            records: &empty,
        };
        let mut delta = plan_delta(&old, &new);
        assert_eq!(delta.channels_changed.len(), 1);
        delta.consumers_changed = moving.iter().map(|slug| (*slug).to_string()).collect();
        convergibility_refusals(&old, &new, &delta, &before)
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
            directory: &before,
            consumers: &[],
            records: &empty,
        };
        let new = PlanFacts {
            directory: &after,
            consumers: &[],
            records: &empty,
        };
        let delta = plan_delta(&old, &new);
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
            directory: &before,
            consumers: &[],
            records: &empty,
        };
        let new = PlanFacts {
            directory: &after,
            consumers: &[],
            records: &empty,
        };
        let delta = plan_delta(&old, &new);
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
            directory: &before,
            consumers: &[],
            records: &empty,
        };
        let new = PlanFacts {
            directory: &after,
            consumers: &[],
            records: &empty,
        };
        let delta = plan_delta(&old, &new);
        convergibility_refusals(&old, &new, &delta, &before);
    }
}

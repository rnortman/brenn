//! The dynamic subscriptions a reload inherits, re-authorized against the
//! candidate.
//!
//! A dynamic subscription is one a live agent asked for at runtime through
//! `MessageSubscribe`. It is in no plan: a durable one is a row in
//! `messaging_dynamic_subscriptions` folded into the directory at boot, and a
//! non-durable one is an in-memory registration that a restart simply loses.
//! Boot re-asks the same three questions of every durable row it finds — does
//! the document still declare a static subscription for this pair, does the
//! agent's policy still authorize delivery here, do the row's depths still fit
//! the channel's standing — and folds, holds dormant, or prunes accordingly
//! ([`brenn_lib::messaging::config::merge_dynamic_subscriptions`]).
//!
//! A reload owes the same three answers, because the oracle is a fresh boot of
//! the candidate: an ACL narrowed under a live dynamic row leaves it dormant,
//! one widened again folds it back, and a `subscribe` line declared where a row
//! already sits replaces the row. This module asks them over the candidate and
//! says which rows move; the commit's own steps are what move them.
//!
//! Only a *changed* agent's rows are classified. An agent this reload does not
//! move answers all three questions the same way it did at boot: its policy and
//! its static subscriptions are the same, and its rows' channels cannot have
//! moved — almost no reload that takes a channel out from under a dynamic row
//! of any agent reaches the classification at all. Two rules together are what
//! hold that: rule 2 refuses a *folded* row's channel, which is the one the
//! live directory can see, and [`dormant_rows_the_reload_cannot_follow`] refuses
//! a *dormant* row's, which it cannot. The one departing channel neither refuses
//! — a removed operator-declared address under a dormant row, which a fresh
//! boot leaves dormant — is excluded from the classification instead, so no arm
//! reads the entry the channel walk is deleting.

use std::collections::{BTreeSet, HashMap, HashSet};

use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::{
    ChannelEntry, DynamicSubscriptionRow, MessagingDirectory, SubscriberEntryKind,
    config::SystemChannelFamily,
};
use indexmap::IndexMap;
use uuid::Uuid;

use super::delta::ChannelSides;

/// The dynamic subscriptions as they stood when prepare read them.
///
/// Read once, before the synchronous prepare, and carried through to the
/// commit — which re-reads it, because this is the one input to a reload that a
/// live session can change while prepare runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DynamicSnapshot {
    /// Every durable row in the table, every agent's.
    pub rows: Vec<DynamicSubscriptionRow>,
    /// Every non-durable registration the messenger holds, as
    /// `(channel_uuid, app_slug)`.
    pub nondurable: Vec<(Uuid, String)>,
}

impl DynamicSnapshot {
    /// The pairs one agent holds, durable and non-durable, as the identity the
    /// commit's re-read compares on. Order-independent: what matters is whether
    /// a pair arrived, left, or came back on different terms, not where the
    /// store listed it.
    pub(crate) fn keys_of(&self, slug: &str) -> HashSet<DynamicKey> {
        self.rows
            .iter()
            .filter(|row| row.app_slug == slug)
            .map(DynamicKey::durable)
            .chain(
                self.nondurable
                    .iter()
                    .filter(|(_, app)| app == slug)
                    .map(|(uuid, _)| DynamicKey::Nondurable(*uuid)),
            )
            .collect()
    }
}

/// One dynamic subscription's whole identity, as the commit's re-read compares
/// it.
///
/// A durable row is keyed on the *terms* it was stored with, not just its
/// channel: an in-place retune is refused at the runtime door
/// (`AlreadySubscribedDiffers`), so unsubscribe-then-resubscribe is how an
/// agent retunes, and a re-mint at different depths would otherwise cancel in
/// a channel-keyed set — leaving the commit to fold the row back in and retune
/// its cursor at depths the prepare snapshot carried and the stored row no
/// longer states. `created_at` is deliberately not part of it: a re-mint on the
/// same terms is a row the commit acts on identically.
///
/// A non-durable registration's channel is its whole identity: the messenger
/// holds it as a `(channel_uuid, app_slug)` pair and no terms travel with it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DynamicKey {
    Durable {
        channel_uuid: Uuid,
        push_depth: brenn_lib::messaging::config::Depth,
        retain_depth: brenn_lib::messaging::config::Depth,
        noise: brenn_lib::messaging::config::NoiseLevel,
        wake_min: brenn_lib::messaging::WakeMin,
        qos: Option<u8>,
    },
    Nondurable(Uuid),
}

impl DynamicKey {
    fn durable(row: &DynamicSubscriptionRow) -> Self {
        Self::Durable {
            channel_uuid: row.channel_uuid,
            push_depth: row.push_depth,
            retain_depth: row.retain_depth,
            noise: row.noise,
            wake_min: row.wake_min,
            qos: row.qos,
        }
    }

    /// The channel the pair stands on, which is how the commit's refusal names
    /// it.
    pub(crate) fn channel_uuid(&self) -> Uuid {
        match self {
            Self::Durable { channel_uuid, .. } => *channel_uuid,
            Self::Nondurable(uuid) => *uuid,
        }
    }
}

/// One dynamic subscription this reload moves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynamicMove {
    pub channel_uuid: Uuid,
    pub address: String,
    /// The durable row behind it, when there is one. A non-durable registration
    /// has none: folding it out removes the in-memory entry and leaves nothing
    /// to keep dormant or to prune.
    pub row: Option<DynamicSubscriptionRow>,
}

/// Why the candidate holds a dynamic subscription back, in the two words boot
/// uses for the same verdict.
///
/// Carried so the journal line and an operator reading it can say which gate
/// fired: an ACL edit that was meant to widen and narrowed something else is
/// the incident this feature's motivating case runs into, and boot hands the
/// diagnosis over for free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RevokeReason {
    /// The candidate policy no longer authorizes delivery on this channel.
    AclDenies,
    /// The row's granted depth exceeds the channel's current standing depth.
    OverStanding {
        /// `push_depth` or `retain_depth`, whichever exceeds standing.
        field: &'static str,
        granted: brenn_lib::messaging::config::Depth,
        standing: brenn_lib::messaging::config::Depth,
    },
}

/// One dynamic subscription the candidate holds back, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynamicRevoke {
    pub moved: DynamicMove,
    pub reason: RevokeReason,
}

/// What the candidate does to one agent's dynamic subscriptions.
///
/// Three disjoint lists, each the input to one commit step. A row in none of
/// them is untouched: it is folded and the candidate keeps it, or it is dormant
/// and the candidate still holds it back.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DynamicRemerge {
    /// Folded now, revoked by the candidate: the subscriber entry is folded
    /// out and the durable row is kept, so the subscription resumes if the
    /// document change behind it is reverted. This is the dormant state boot
    /// puts the same row in.
    pub revoke: Vec<DynamicRevoke>,
    /// Dormant now, authorized by the candidate: folded back in at the row's
    /// own depths, and its cursor re-attached where it was.
    pub revive: Vec<DynamicMove>,
    /// The candidate declares a static subscription for this pair, so static
    /// config wins: the row is deleted and the static entry replaces it. Boot's
    /// rule 3, asked of the candidate.
    pub prune: Vec<DynamicMove>,
}

impl DynamicRemerge {
    /// Whether the candidate leaves every one of this agent's dynamic
    /// subscriptions exactly where it is. Read by the cases below; the commit
    /// walks the three lists rather than asking.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.revoke.is_empty() && self.revive.is_empty() && self.prune.is_empty()
    }
}

/// The two uuid sets [`remerge_of`] decides against, named rather than
/// positional: both are `HashSet<Uuid>` and they have opposite meanings — one
/// makes a pair untouchable, the other makes it a prune — so a transposition
/// would compile and silently swap the two verdicts.
pub(crate) struct RemergeSides<'a> {
    /// The uuids this commit takes away, from [`ChannelSides::departing`].
    pub departing: &'a HashSet<Uuid>,
    /// The channels the candidate declares as this agent's *static*
    /// subscriptions, by uuid, with the candidate's address for each. What
    /// decides the prune arm, and the address the prune arm reports for a
    /// channel the live directory does not hold yet.
    pub candidate_static: &'a HashMap<Uuid, String>,
}

/// Classify one changed agent's dynamic subscriptions against the candidate.
///
/// `live` is the directory as it stands, which is the only place a dynamic
/// subscription is visible at all: whether a row is currently folded is whether
/// the live entry carries an `App(slug)` subscriber, and the channel's address
/// and standing depth are read from there too. They are the candidate's values
/// as well for every pair the revoke and revive arms act on: a pair on a
/// channel this commit takes away is excluded by `departing` before any arm
/// reads the entry's address or standing depth, and a pair on a channel this
/// commit adds is excluded by the `live.by_uuid` skip below, because the
/// directory does not hold it yet. So every channel those two arms name is one
/// both documents describe the same way, which is what makes the address the
/// commit reports and the standing depth the conformance gate reads the
/// candidate's own. The prune arm reads neither address nor depth off the
/// entry — it takes the address from `candidate_static` — so it is the one arm
/// that acts on a channel this reload adds.
///
/// `departing` is the uuids this commit removes or retunes. Most of them never
/// reach here at all — rule 2 refuses a folded row's departing channel and
/// [`dormant_rows_the_reload_cannot_follow`] a dormant row's — but one class is
/// deliberately applied rather than refused: a dormant durable row on a
/// removed operator-declared channel, which a fresh boot of the candidate
/// leaves dormant with its cursor. Left untouched here so the commit does not
/// fold it onto an entry the channel walk is deleting.
///
/// `candidate_static` is the candidate plan's set of `(channel_uuid)` this
/// agent statically subscribes to, which is what decides the prune arm.
pub(crate) fn remerge_of(
    slug: &str,
    policy: &AppPolicy,
    snapshot: &DynamicSnapshot,
    live: &MessagingDirectory,
    sides: &RemergeSides<'_>,
) -> DynamicRemerge {
    let mut out = DynamicRemerge::default();

    let durable = snapshot
        .rows
        .iter()
        .filter(|row| row.app_slug == slug)
        .map(|row| (row.channel_uuid, Some(row.clone())));
    let nondurable = snapshot
        .nondurable
        .iter()
        .filter(|(_, app_slug)| app_slug == slug)
        .map(|(uuid, _)| (*uuid, None));

    for (channel_uuid, row) in durable.chain(nondurable) {
        // A pair on a channel this commit removes or retunes: nothing here may
        // act on it, because every arm's verdict would be read off an entry
        // the channel walk is about to delete. The only pair that reaches this
        // arm on an applied reload is the carved-out one named above; the rest
        // stopped the reload at prepare.
        if sides.departing.contains(&channel_uuid) {
            continue;
        }
        // The prune arm is asked before the directory, because it is the one
        // verdict that needs neither the entry nor a live channel: the row is
        // deleted and the static entry the candidate declares replaces it,
        // which the arrival step folds in from the plan. A channel this reload
        // *adds* under a row of this agent reaches here, and it must: boot's
        // rule 3 deletes such a row, and leaving it beside the static entry
        // would put the process in the one state the runtime treats as
        // impossible — a directory subscriber with a durable row behind it
        // (`RuntimeUnsubscribeError::StaticSubscription`'s "structurally
        // unreachable" invariant).
        if let Some(address) = sides.candidate_static.get(&channel_uuid) {
            out.prune.push(DynamicMove {
                channel_uuid,
                address: address.clone(),
                row: row.clone(),
            });
            continue;
        }
        // A pair whose channel the live directory does not hold is one this
        // process cannot see, name or fold: boot reconstructs every channel a
        // surviving row references, so the ways here are a row minted against a
        // channel that has since gone and a channel this reload adds, and no
        // remaining arm can act on either. Left exactly where it is, for the
        // next boot to classify.
        //
        // TODO(reload-revive-on-redeclared-channel): a reload that *adds* the
        // channel under a dormant durable row this policy allows, and declares
        // no static subscription for it, lands here, and a fresh boot of that
        // document folds the row. Reviving it needs the classification asked
        // against the candidate directory and the fold deferred until after the
        // entry is added.
        let Some(entry) = live.by_uuid(&channel_uuid) else {
            continue;
        };
        let folded = folded_now(live, &channel_uuid, slug);
        let moved = DynamicMove {
            channel_uuid,
            address: entry.address.clone(),
            row: row.clone(),
        };

        let denied = if !policy.allows_channel_access(&entry.address) {
            Some(RevokeReason::AclDenies)
        } else {
            row.as_ref().and_then(|row| over_standing(row, &entry))
        };
        match (folded, denied) {
            (true, Some(reason)) => out.revoke.push(DynamicRevoke { moved, reason }),
            (false, None) => out.revive.push(moved),
            _ => {}
        }
    }
    out
}

/// Whether this pair is folded into the live directory right now.
///
/// The only place a dynamic subscription is visible: the live entry carries an
/// `App(slug)` subscriber the plans do not. A pair whose channel the directory
/// does not hold is not folded and cannot be.
pub(crate) fn folded_now(live: &MessagingDirectory, channel_uuid: &Uuid, slug: &str) -> bool {
    let kind = SubscriberEntryKind::App(slug.to_string());
    live.by_uuid(channel_uuid).is_some_and(|entry| {
        entry
            .subscribers
            .iter()
            .any(|subscriber| subscriber.kind.same_principal(&kind))
    })
}

/// Which of a row's granted depths no longer fits the channel's standing depth,
/// or `None` when both do.
///
/// Boot's depth-conformance gate: a row granted more than the operator now
/// stands behind is held dormant rather than folded, and rather than pruned —
/// the operator may raise standing back. The field and the pair of depths are
/// what boot's own warning names.
fn over_standing(row: &DynamicSubscriptionRow, entry: &ChannelEntry) -> Option<RevokeReason> {
    let standing = entry.resolved_channel.standing_retain_depth;
    [
        ("push_depth", row.push_depth),
        ("retain_depth", row.retain_depth),
    ]
    .into_iter()
    .find(|(_, granted)| *granted > standing)
    .map(|(field, granted)| RevokeReason::OverStanding {
        field,
        granted,
        standing,
    })
}

/// Rule 2's question over the rows the live directory cannot answer for: which
/// agents hold a *dormant* durable row on a departing channel **this reload
/// cannot follow the row onto**.
///
/// Deliberately not every departing channel: one class of them is applied
/// rather than refused (the carve-out below), which is what the name says and
/// what a caller building the next rule on top of this one has to know.
///
/// Rule 2 walks the subscribers a departing or retuned entry carries, which
/// covers every folded dynamic subscription. A dormant row has no subscriber
/// entry to be found that way, and leaving it unasked is not a smaller
/// question:
///
/// - a dormant row the candidate authorizes again is classified `revive`
///   against the entry that is still in the directory at prepare, and the
///   commit then folds a subscriber onto a channel step 4 has just removed —
///   an `add_subscriber` that returns `false`, past the point where anything
///   may decline;
/// - a dormant row on a *retuned* channel keeps its uuid, so the fold
///   succeeds — onto an entry whose standing depth the conformance gate never
///   read. The verdict was taken against the old entry, so the reload can fold
///   a row back in above the standing depth the candidate declares, or hold one
///   dormant that the candidate now stands behind. Either way the state after
///   `applied` is not the state a fresh boot of the candidate produces.
///
/// Refused instead, in rule 2's words: a reload that moves the channel of any
/// dynamic subscription — folded or dormant, of a changed agent or not — needs
/// a restart, which is what the operator got for every agent edit before this
/// slice. The pair is named by address, taken from the entry that is still
/// there.
///
/// One class is carved out of that and applied rather than refused: a dormant
/// durable row on a *removed* channel whose address is operator-declared
/// (`SystemChannelFamily::of(..).is_none()`, the predicate the store's
/// reconstruction uses). A fresh boot of the candidate puts such an address in
/// `load_channels_by_uuids`' skip report, mints no directory entry for it, and
/// holds the row dormant with its cursor; the commit's channel walk keeps the
/// durable channel row too, so leaving the pair alone reproduces the boot
/// exactly and there is no in-flight transition to decline. Refusing it instead
/// cost a restart on the only restart-free path for retiring a channel an agent
/// subscribed to dynamically: narrow the ACL, reload; remove the block, reload.
///
/// A *removed* channel whose address boot *would* reconstruct — an `mqtt:` or
/// `webhook:` family address — is still named: a fresh boot has a directory
/// entry there and may fold the row onto it, and reproducing a reconstruction
/// in flight is not this facility's work.
///
/// Asked once, at prepare, unlike rule 2: a dormant row cannot arrive while a
/// reload runs. Only boot's merge and this facility's own revoke step put a row
/// into dormancy, and neither runs concurrently with a prepare.
pub(crate) fn dormant_rows_the_reload_cannot_follow(
    snapshot: &DynamicSnapshot,
    live: &MessagingDirectory,
    sides: &ChannelSides,
) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for row in &snapshot.rows {
        let departing = sides.removed.contains(&row.channel_uuid)
            || sides.retuned_old.contains(&row.channel_uuid);
        if !departing || folded_now(live, &row.channel_uuid, &row.app_slug) {
            continue;
        }
        // A row whose channel the directory does not hold is one no step of
        // this reload can reach: it is in no plan, so it is in no channel list
        // either, and the pair is left where it is for the next boot.
        let Some(entry) = live.by_uuid(&row.channel_uuid) else {
            continue;
        };
        if sides.removed.contains(&row.channel_uuid)
            && SystemChannelFamily::of(&entry.address).is_none()
        {
            continue;
        }
        out.push((row.app_slug.clone(), entry.address.clone()));
    }
    out.sort();
    out.dedup();
    out
}

/// The `mqtt:` ingress channels a set of dynamic rows stands for.
///
/// The broker's SUBSCRIBE union and the ingress route table are built from a
/// list of resolved ingress channels, and a fresh boot builds them from the
/// static ones *plus* every dynamic `mqtt:` row the merge kept — so a reload
/// that diffed only the static lists would unsubscribe a filter the candidate
/// still needs, or leave one subscribed that it does not. This is how a dynamic
/// row joins that list (parallel to boot's `DynamicMqttIngress` conversion;
/// both must produce the same shape).
///
/// A pair whose channel is not `mqtt:`, or whose client the candidate does not
/// declare, is not one: the first is another transport and the second cannot be
/// subscribed at any broker this process holds.
///
/// The second of those has two meanings and they are told apart, because only
/// one of them is expected. A row on a client in `stopping` is the ordinary
/// shape of a removal — the agent's ACL lost the client, the re-merge revokes
/// the row, and the session it named is going — and is skipped quietly. A row
/// on a client that is in neither the candidate nor `stopping` is host state no
/// document accounts for: a durable subscription naming a broker session this
/// process does not have and will not get, invisible to every status field and
/// to the oracle, which compares registries and not stored rows. It is named in
/// the journal so it is visible at all.
pub(crate) fn dynamic_ingress(
    rows: &[DynamicSubscriptionRow],
    live: &MessagingDirectory,
    clients: &IndexMap<String, brenn_lib::mqtt::config::MqttClientIdentity>,
    stopping: &BTreeSet<String>,
    exclude: &HashSet<Uuid>,
) -> Vec<brenn_lib::mqtt::config::ResolvedMqttIngressChannel> {
    let mut seen: HashSet<Uuid> = HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        let channel_uuid = &row.channel_uuid;
        if exclude.contains(channel_uuid) || !seen.insert(*channel_uuid) {
            continue;
        }
        let Some(entry) = live.by_uuid(channel_uuid) else {
            continue;
        };
        if entry.transport_type != brenn_lib::messaging::ChannelScheme::Mqtt {
            continue;
        }
        // A stored `mqtt:` channel address that does not parse is host-state
        // corruption.
        let parsed =
            brenn_lib::mqtt::address::parse_mqtt_address(&entry.address).unwrap_or_else(|error| {
                panic!(
                    "reload: stored dynamic mqtt channel address {:?} does not parse ({error}) — \
                     channel-address corruption (host bug)",
                    entry.address
                )
            });
        let Some(client) = clients.get(&parsed.client) else {
            if stopping.contains(&parsed.client) {
                tracing::debug!(
                    app = %row.app_slug,
                    address = %entry.address,
                    client = %parsed.client,
                    "reload: dynamic mqtt row on a client this reload stops",
                );
            } else {
                tracing::warn!(
                    app = %row.app_slug,
                    address = %entry.address,
                    client = %parsed.client,
                    "reload: dynamic mqtt row on a client no document declares — the row \
                     names a broker session this process does not hold",
                );
            }
            continue;
        };
        let qos = row.qos.unwrap_or_else(|| {
            panic!(
                "reload: dynamic mqtt subscription on {:?} has no stored qos — mqtt dynamic rows \
                 always persist one (host bug)",
                entry.address
            )
        });
        out.push(brenn_lib::mqtt::config::ResolvedMqttIngressChannel {
            channel_uuid: *channel_uuid,
            channel_address: entry.address.clone(),
            client_slug: parsed.client,
            topic: parsed.topic,
            qos,
            urgency: client.urgency,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::GrantSet;
    use brenn_lib::access::acl::{AclSet, ChannelMatcher};
    use brenn_lib::messaging::ChannelScheme;
    use brenn_lib::messaging::config::{Depth, NoiseLevel};
    use brenn_lib::messaging::test_support::test_channel_entry;
    use brenn_lib::messaging::{SubscriberEntry, WakeMin};
    use tracing_test::traced_test;

    /// A channel delta that removes these uuids and moves nothing else.
    fn removing(uuids: &[Uuid]) -> ChannelSides {
        ChannelSides {
            added: HashSet::new(),
            removed: uuids.iter().copied().collect(),
            retuned_old: HashSet::new(),
            retuned_new: HashSet::new(),
        }
    }

    /// A channel delta that retunes these uuids, which keep them on both sides.
    fn retuning(uuids: &[Uuid]) -> ChannelSides {
        ChannelSides {
            added: HashSet::new(),
            removed: HashSet::new(),
            retuned_old: uuids.iter().copied().collect(),
            retuned_new: uuids.iter().copied().collect(),
        }
    }

    fn row(uuid: Uuid, slug: &str, depth: Depth) -> DynamicSubscriptionRow {
        row_at(uuid, slug, Depth::Bounded(0), depth)
    }

    /// The same with both granted depths spelled, for the cases about which
    /// one the conformance gate reports.
    fn row_at(
        uuid: Uuid,
        slug: &str,
        push_depth: Depth,
        retain_depth: Depth,
    ) -> DynamicSubscriptionRow {
        DynamicSubscriptionRow {
            channel_uuid: uuid,
            app_slug: slug.to_string(),
            push_depth,
            retain_depth,
            noise: NoiseLevel::Metered,
            wake_min: WakeMin::Never,
            qos: None,
            created_at: "2026-09-07T00:00:00Z".to_string(),
        }
    }

    /// A directory holding one channel, optionally with the agent folded onto
    /// it, at a standing depth the tests vary.
    fn directory(address: &str, folded: bool, standing: Depth) -> (MessagingDirectory, Uuid) {
        let subscribers = if folded {
            vec![SubscriberEntry {
                kind: SubscriberEntryKind::App("reader".to_string()),
                push_depth: Depth::Bounded(0),
                retain_depth: Depth::Bounded(1),
                noise: NoiseLevel::Metered,
                wake_min: Some(WakeMin::Never),
            }]
        } else {
            Vec::new()
        };
        let mut entry = test_channel_entry(address, subscribers);
        entry.resolved_channel.standing_retain_depth = standing;
        let uuid = entry.uuid;
        (MessagingDirectory::with_entries(vec![entry]), uuid)
    }

    /// A policy whose subscribe ACL covers exactly `addresses`, on both the
    /// `brenn:` and the `ephemeral:` families. Hand-rolled because
    /// `AppPolicy::with_grants` is gated inside `brenn-lib`.
    fn policy(addresses: &[&str]) -> AppPolicy {
        let mut grants = GrantSet::default();
        grants.insert(AppCapability::MessagingSubscribe);
        grants.insert(AppCapability::EphemeralSubscribe);
        let mut acls = AclSet::default();
        for address in addresses {
            let (scheme, bare) = ChannelScheme::split(address).expect("test address");
            let matcher = ChannelMatcher::Exact(bare.to_string());
            match scheme {
                ChannelScheme::Ephemeral => acls.ephemeral_subscribe.push(matcher),
                _ => acls.brenn_subscribe.push(matcher),
            }
        }
        AppPolicy {
            grants,
            acls,
            tool_grants: Default::default(),
        }
    }

    #[test]
    fn a_folded_row_the_candidate_denies_is_revoked() {
        let (live, uuid) = directory("work", true, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let remerge = remerge_of(
            "reader",
            &policy(&[]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::new(),
            },
        );
        assert_eq!(remerge.revoke.len(), 1);
        assert_eq!(remerge.revoke[0].moved.address, "brenn:work");
        assert!(remerge.revoke[0].moved.row.is_some());
        assert_eq!(remerge.revoke[0].reason, RevokeReason::AclDenies);
        assert!(remerge.revive.is_empty() && remerge.prune.is_empty());
    }

    #[test]
    fn a_dormant_row_the_candidate_allows_is_revived() {
        let (live, uuid) = directory("work", false, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let remerge = remerge_of(
            "reader",
            &policy(&["brenn:work"]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::new(),
            },
        );
        assert_eq!(remerge.revive.len(), 1);
        assert_eq!(remerge.revive[0].channel_uuid, uuid);
        assert!(remerge.revoke.is_empty() && remerge.prune.is_empty());
    }

    #[test]
    fn a_row_the_candidate_declares_statically_is_pruned() {
        let (live, uuid) = directory("work", true, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let remerge = remerge_of(
            "reader",
            &policy(&["brenn:work"]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::from([(uuid, "brenn:work".to_string())]),
            },
        );
        assert_eq!(remerge.prune.len(), 1);
        assert!(remerge.revoke.is_empty() && remerge.revive.is_empty());
    }

    /// A channel this reload *adds* under a row of the agent, with the
    /// candidate declaring a static subscription for it: pruned, on the
    /// candidate's address, with no live entry to read it off.
    ///
    /// The one arm that acts on a channel the live directory does not hold.
    /// Boot's rule 3 deletes such a row, and leaving it beside the static entry
    /// would create the pairing `RuntimeUnsubscribeError::StaticSubscription`
    /// calls "structurally unreachable".
    #[test]
    fn a_row_on_an_arriving_channel_the_candidate_declares_statically_is_pruned() {
        let (live, _) = directory("work", false, Depth::Unbounded);
        let arriving = Uuid::new_v4();
        let snapshot = DynamicSnapshot {
            rows: vec![row(arriving, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let remerge = remerge_of(
            "reader",
            &policy(&["brenn:spill"]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::from([(arriving, "brenn:spill".to_string())]),
            },
        );
        assert_eq!(remerge.prune.len(), 1);
        assert_eq!(remerge.prune[0].channel_uuid, arriving);
        assert_eq!(
            remerge.prune[0].address, "brenn:spill",
            "the address comes from the candidate, since the directory holds none",
        );
        assert!(remerge.revoke.is_empty() && remerge.revive.is_empty());
    }

    /// The same channel arriving with *no* static declaration for it is
    /// untouched, which is what `TODO(reload-revive-on-redeclared-channel)`
    /// tracks: a fresh boot of that document would fold the row.
    #[test]
    fn a_row_on_an_arriving_channel_the_candidate_does_not_declare_is_untouched() {
        let (live, _) = directory("work", false, Depth::Unbounded);
        let arriving = Uuid::new_v4();
        let snapshot = DynamicSnapshot {
            rows: vec![row(arriving, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let remerge = remerge_of(
            "reader",
            &policy(&["brenn:spill"]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::new(),
            },
        );
        assert!(remerge.is_empty());
    }

    /// The prune arm is asked before the ACL arm, exactly as boot asks it: a
    /// static declaration wins whether or not the row would still have been
    /// authorized.
    #[test]
    fn a_static_declaration_prunes_a_row_the_candidate_would_also_revoke() {
        let (live, uuid) = directory("work", true, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let remerge = remerge_of(
            "reader",
            &policy(&[]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::from([(uuid, "brenn:work".to_string())]),
            },
        );
        assert_eq!(remerge.prune.len(), 1);
        assert!(remerge.revoke.is_empty());
    }

    /// A row granted more depth than the channel now stands behind is dormant
    /// for the same reason a revoked one is, and is neither folded nor pruned.
    ///
    /// Both granted depths are gated, and `push_depth` is asked first — boot's
    /// order — so each arm is exercised on its own and the field the journal
    /// line reports is pinned along with the verdict.
    #[test]
    fn a_row_over_the_channels_standing_depth_is_revoked() {
        for (field, push_depth, retain_depth) in [
            ("push_depth", Depth::Bounded(9), Depth::Bounded(1)),
            ("retain_depth", Depth::Bounded(0), Depth::Bounded(9)),
        ] {
            let (live, uuid) = directory("work", true, Depth::Bounded(1));
            let snapshot = DynamicSnapshot {
                rows: vec![row_at(uuid, "reader", push_depth, retain_depth)],
                nondurable: Vec::new(),
            };
            let remerge = remerge_of(
                "reader",
                &policy(&["brenn:work"]),
                &snapshot,
                &live,
                &RemergeSides {
                    departing: &HashSet::new(),
                    candidate_static: &HashMap::new(),
                },
            );
            assert_eq!(remerge.revoke.len(), 1, "{field}");
            assert_eq!(
                remerge.revoke[0].reason,
                RevokeReason::OverStanding {
                    field,
                    granted: Depth::Bounded(9),
                    standing: Depth::Bounded(1),
                },
            );
        }
    }

    #[test]
    fn a_folded_row_the_candidate_still_allows_is_untouched() {
        let (live, uuid) = directory("work", true, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let remerge = remerge_of(
            "reader",
            &policy(&["brenn:work"]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::new(),
            },
        );
        assert!(remerge.is_empty());
    }

    /// A non-durable registration carries no row: revoking it removes the
    /// in-memory entry and leaves nothing dormant, which is what a restart
    /// would have done to it anyway.
    #[test]
    fn a_nondurable_registration_the_candidate_denies_is_revoked_with_no_row() {
        let (live, uuid) = directory("ephemeral:work", true, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: Vec::new(),
            nondurable: vec![(uuid, "reader".to_string())],
        };
        let remerge = remerge_of(
            "reader",
            &policy(&[]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::new(),
            },
        );
        assert_eq!(remerge.revoke.len(), 1);
        assert!(remerge.revoke[0].moved.row.is_none());
    }

    #[test]
    fn another_agents_rows_are_not_this_agents_business() {
        let (live, uuid) = directory("work", true, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "writer", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let remerge = remerge_of(
            "reader",
            &policy(&[]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::new(),
                candidate_static: &HashMap::new(),
            },
        );
        assert!(remerge.is_empty());
    }

    /// The re-read commit performs: a pair minted after prepare took the set
    /// is in the symmetric difference, which is what the commit gate refuses
    /// on. A pair the agent dropped in the same window is there too, and the
    /// agents whose rows did not move compare equal.
    #[test]
    fn a_pair_minted_after_the_set_was_taken_is_a_difference() {
        let held = Uuid::new_v4();
        let minted = Uuid::new_v4();
        let observed = DynamicSnapshot {
            rows: vec![row(held, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let now = DynamicSnapshot {
            rows: vec![
                row(held, "reader", Depth::Bounded(1)),
                row(minted, "reader", Depth::Bounded(1)),
            ],
            nondurable: Vec::new(),
        };
        assert_eq!(
            now.keys_of("reader")
                .symmetric_difference(&observed.keys_of("reader"))
                .cloned()
                .collect::<Vec<_>>(),
            vec![DynamicKey::durable(&row(
                minted,
                "reader",
                Depth::Bounded(1)
            ))],
        );
        // A non-durable registration minted in the same window moves the set
        // the same way: the pair carries which table it came from, so the two
        // shapes cannot cancel each other out.
        let nondurable = DynamicSnapshot {
            rows: observed.rows.clone(),
            nondurable: vec![(held, "reader".to_string())],
        };
        assert_eq!(
            nondurable
                .keys_of("reader")
                .symmetric_difference(&observed.keys_of("reader"))
                .cloned()
                .collect::<Vec<_>>(),
            vec![DynamicKey::Nondurable(held)],
        );
        // And an untouched agent's set is equal, which is the gate's
        // early-continue: one agent subscribing does not refuse another's edit.
        assert_eq!(now.keys_of("writer"), observed.keys_of("writer"));
    }

    #[test]
    fn the_observed_keys_of_an_agent_are_its_durable_and_nondurable_pairs() {
        let one = Uuid::new_v4();
        let two = Uuid::new_v4();
        let snapshot = DynamicSnapshot {
            rows: vec![row(one, "reader", Depth::Bounded(1))],
            nondurable: vec![(two, "reader".to_string()), (one, "writer".to_string())],
        };
        assert_eq!(
            snapshot.keys_of("reader"),
            HashSet::from([
                DynamicKey::durable(&row(one, "reader", Depth::Bounded(1))),
                DynamicKey::Nondurable(two),
            ]),
        );
    }

    /// An agent retunes by unsubscribing and resubscribing — an in-place
    /// retune is refused at the runtime door — so the same channel can come
    /// back on different terms inside the prepare window. Both keys are in the
    /// difference, so the commit gate declines rather than acting on the stale
    /// row.
    #[test]
    fn a_row_re_minted_at_other_depths_is_a_difference() {
        let uuid = Uuid::new_v4();
        let observed = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        let now = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(4))],
            nondurable: Vec::new(),
        };
        assert_eq!(
            now.keys_of("reader")
                .symmetric_difference(&observed.keys_of("reader"))
                .count(),
            2,
        );
        // And a re-mint on the same terms is not: `created_at` moved, and the
        // commit acts on the row identically either way.
        let same = DynamicSnapshot {
            rows: vec![DynamicSubscriptionRow {
                created_at: "2026-09-08T00:00:00Z".to_string(),
                ..row(uuid, "reader", Depth::Bounded(1))
            }],
            nondurable: Vec::new(),
        };
        assert_eq!(same.keys_of("reader"), observed.keys_of("reader"));
    }

    // -----------------------------------------------------------------------
    // Rule 2's other half: the dormant rows the directory cannot answer for.
    // -----------------------------------------------------------------------

    /// A dormant row on a channel this reload *retunes* is refused: the uuid
    /// survives the retune, so a classification would read the candidate's
    /// standing depth off the entry the walk is replacing and fold the row back
    /// in against a verdict taken on the old one.
    #[test]
    fn a_dormant_row_on_a_retuned_channel_is_named() {
        let (live, uuid) = directory("work", false, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        assert_eq!(
            dormant_rows_the_reload_cannot_follow(&snapshot, &live, &retuning(&[uuid])),
            vec![("reader".to_string(), "brenn:work".to_string())],
        );
    }

    /// A dormant row on a *removed* operator-declared channel is not named:
    /// a fresh boot of the candidate finds no `[[channel]]` block for the
    /// address, mints no entry, and holds the row dormant with its cursor —
    /// which is byte-for-byte what leaving the pair alone produces.
    #[test]
    fn a_dormant_row_on_a_removed_operator_declared_channel_is_not_named() {
        let (live, uuid) = directory("work", false, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        assert!(
            dormant_rows_the_reload_cannot_follow(&snapshot, &live, &removing(&[uuid])).is_empty(),
        );
    }

    /// A dormant row on a *removed* channel whose address boot reconstructs
    /// from its store row — an `mqtt:` ingress address — is still named: the
    /// fresh boot has a directory entry there and may fold the row onto it, and
    /// the reload has taken the entry away.
    #[test]
    fn a_dormant_row_on_a_removed_reconstructible_channel_is_named() {
        let (live, uuids) = mqtt_directory(&["mqtt:ha:home/state"]);
        let snapshot = DynamicSnapshot {
            rows: vec![mqtt_row(uuids[0], "reader")],
            nondurable: Vec::new(),
        };
        assert_eq!(
            dormant_rows_the_reload_cannot_follow(&snapshot, &live, &removing(&[uuids[0]])),
            vec![("reader".to_string(), "mqtt:ha:home/state".to_string())],
        );
    }

    /// A *folded* row on the same channel is rule 2's own: the live entry
    /// carries its subscriber, so the directory walk names it and this half
    /// must not name it a second time.
    #[test]
    fn a_folded_row_on_a_departing_channel_is_rule_2s() {
        let (live, uuid) = directory("work", true, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        assert!(
            dormant_rows_the_reload_cannot_follow(&snapshot, &live, &retuning(&[uuid])).is_empty(),
        );
    }

    /// A dormant row whose channel this reload leaves alone is the ordinary
    /// case the re-merge classifies: both documents describe the channel the
    /// same way, so there is nothing to refuse.
    #[test]
    fn a_dormant_row_on_a_channel_this_reload_leaves_alone_is_not_named() {
        let (live, uuid) = directory("work", false, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        assert!(dormant_rows_the_reload_cannot_follow(&snapshot, &live, &removing(&[])).is_empty(),);
    }

    /// The pair the carve-out above lets through is classified as nothing at
    /// all: a `revive` here would fold a subscriber onto an entry the commit's
    /// channel walk is deleting, and a fresh boot folds nothing either.
    #[test]
    fn a_pair_on_a_departing_channel_is_untouched() {
        let (live, uuid) = directory("work", false, Depth::Unbounded);
        let snapshot = DynamicSnapshot {
            rows: vec![row(uuid, "reader", Depth::Bounded(1))],
            nondurable: Vec::new(),
        };
        // The candidate's policy allows the channel, which without `departing`
        // is exactly the `revive` case.
        let remerge = remerge_of(
            "reader",
            &policy(&["brenn:work"]),
            &snapshot,
            &live,
            &RemergeSides {
                departing: &HashSet::from([uuid]),
                candidate_static: &HashMap::new(),
            },
        );
        assert!(remerge.is_empty());
    }

    // -----------------------------------------------------------------------
    // The `mqtt:` projection: what a dynamic row contributes to the broker's
    // SUBSCRIBE union and the ingress route table.
    // -----------------------------------------------------------------------

    /// One `mqtt:` channel per address, with the transport a stored `mqtt:`
    /// entry carries. `test_channel_entry` mints a `brenn:` entry, and the
    /// scheme is what the projection filters on.
    fn mqtt_directory(addresses: &[&str]) -> (MessagingDirectory, Vec<Uuid>) {
        let entries: Vec<ChannelEntry> = addresses
            .iter()
            .map(|address| {
                let mut entry = test_channel_entry(address, Vec::new());
                // Spelled rather than canonicalized: `canonical_address`
                // defaults a scheme-less name to `brenn:`, and an `mqtt:`
                // address is what the projection parses the client and topic
                // out of.
                entry.address = (*address).to_string();
                entry.transport_type = ChannelScheme::Mqtt;
                entry
            })
            .collect();
        let uuids = entries.iter().map(|entry| entry.uuid).collect();
        (MessagingDirectory::with_entries(entries), uuids)
    }

    /// A durable row as a dynamic `mqtt:` subscribe leaves it: with the
    /// SUBSCRIBE QoS the projection re-asserts the filter at.
    fn mqtt_row(uuid: Uuid, slug: &str) -> DynamicSubscriptionRow {
        DynamicSubscriptionRow {
            qos: Some(1),
            ..row(uuid, slug, Depth::Bounded(1))
        }
    }

    fn clients(slugs: &[&str]) -> IndexMap<String, brenn_lib::mqtt::config::MqttClientIdentity> {
        slugs
            .iter()
            .map(|slug| {
                (
                    (*slug).to_string(),
                    brenn_lib::mqtt::test_support::test_client_identity(slug),
                )
            })
            .collect()
    }

    /// A row on a uuid the side already declares statically is not a second
    /// filter: the static channel is subscribed and routed on its own, and
    /// counting it twice would have the delta unsubscribe a filter the
    /// candidate still declares.
    #[test]
    fn a_row_on_a_statically_declared_channel_is_not_projected() {
        let (live, uuids) = mqtt_directory(&["mqtt:ha:home/state"]);
        let rows = vec![mqtt_row(uuids[0], "reader")];
        assert!(
            dynamic_ingress(
                &rows,
                &live,
                &clients(&["ha"]),
                &BTreeSet::new(),
                &HashSet::from([uuids[0]])
            )
            .is_empty(),
        );
    }

    /// One channel folded by two agents is one filter and one route. This is
    /// what makes a revoke by one agent, while another still holds the same
    /// channel folded, leave the filter in the candidate's set.
    #[test]
    fn two_agents_on_one_channel_project_one_filter() {
        let (live, uuids) = mqtt_directory(&["mqtt:ha:home/state"]);
        let rows = vec![mqtt_row(uuids[0], "reader"), mqtt_row(uuids[0], "writer")];
        let projected = dynamic_ingress(
            &rows,
            &live,
            &clients(&["ha"]),
            &BTreeSet::new(),
            &HashSet::new(),
        );
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].channel_address, "mqtt:ha:home/state");
        assert_eq!(projected[0].topic, "home/state");
        assert_eq!(projected[0].client_slug, "ha");
    }

    /// A row on another transport is nothing the broker knows about, and one
    /// whose client the document does not declare cannot be subscribed at any
    /// broker this process holds.
    #[test]
    fn a_row_that_is_not_a_declared_mqtt_channel_is_not_projected() {
        let (brenn_live, brenn_uuid) = directory("work", true, Depth::Unbounded);
        assert!(
            dynamic_ingress(
                &[row(brenn_uuid, "reader", Depth::Bounded(1))],
                &brenn_live,
                &clients(&["ha"]),
                &BTreeSet::new(),
                &HashSet::new(),
            )
            .is_empty(),
            "a `brenn:` row is another transport's",
        );

        let (live, uuids) = mqtt_directory(&["mqtt:gone:home/state"]);
        assert!(
            dynamic_ingress(
                &[mqtt_row(uuids[0], "reader")],
                &live,
                &clients(&["ha"]),
                &BTreeSet::new(),
                &HashSet::new(),
            )
            .is_empty(),
            "the row's client is not declared",
        );
    }

    /// The two meanings of an undeclared client are told apart in the journal:
    /// a client this reload stops is the ordinary shape of a removal, and one
    /// no document accounts for is host state nothing else surfaces.
    #[tokio::test]
    #[traced_test]
    async fn an_undeclared_clients_row_is_journalled_by_which_kind_it_is() {
        let (live, uuids) = mqtt_directory(&["mqtt:gone:home/state"]);
        let rows = [mqtt_row(uuids[0], "reader")];

        assert!(
            dynamic_ingress(
                &rows,
                &live,
                &clients(&["ha"]),
                &BTreeSet::from(["gone".to_string()]),
                &HashSet::new(),
            )
            .is_empty(),
        );
        assert!(
            !logs_contain("no document declares"),
            "a row on a client this reload stops is expected and is not warned about",
        );

        assert!(
            dynamic_ingress(
                &rows,
                &live,
                &clients(&["ha"]),
                &BTreeSet::new(),
                &HashSet::new(),
            )
            .is_empty(),
        );
        assert!(
            logs_contain("no document declares") && logs_contain("mqtt:gone:home/state"),
            "and one nothing declares is named",
        );
    }
}

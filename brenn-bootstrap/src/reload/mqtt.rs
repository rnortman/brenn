//! What a reload does to the broker's SUBSCRIBE set and the ingress route
//! table.
//!
//! An `mqtt:` channel has two runtime parts beside its directory entry: a
//! filter on the client's reconnect-survival set, which is what makes the
//! broker deliver anything, and an `IngressRoute`, which is what makes a
//! delivery reach the channel. Both are runtime-mutable — the dynamic
//! `mqtt:`-subscribe path drives exactly this pair — so an `mqtt:` entry
//! converges like a `brenn:` one, with these two steps around the channel walk.
//!
//! The unit of the broker's set is the **filter**, not the channel: one filter
//! on one client is one address is one channel uuid, so two channels never
//! share a filter, but a *dynamic* subscription and a config channel can. The
//! diff is therefore taken over the two plans' subscription unions rather than
//! over the channel delta, and the channel delta decides only the routes. What
//! keeps `unsubscribe_filter`'s "only when the last subscriber leaves" contract
//! is the pair of convergibility rules in [`super::delta`]: a live subscriber
//! the plan does not hold refuses the reload before any UNSUBSCRIBE is issued.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use brenn_envelope::ChannelScheme;
use brenn_lib::messaging::ChannelEntry;
use brenn_lib::mqtt::config::{MqttClientConfig, ResolvedMqttIngressChannel};
use brenn_mqtt::union_subscriptions;
use brenn_server::mqtt_router::IngressRoute;
use indexmap::IndexMap;
use uuid::Uuid;

/// Which broker clients a reload registers, restarts, or stops.
///
/// The baseline is the live registry, not a plan field: a client is resolved
/// off the document and registered as a session, and what this process is
/// connected as right now is the only baseline that answers "would a fresh
/// boot of the candidate dial something else". Both sides are compared in
/// resolved form — broker coordinates, tuning, and the bytes of the password
/// and the CA — so a rotated credential under an unmoved document is a change
/// and a re-spelled block that resolves to the same client is not.
///
/// The arriving sides carry the candidate's resolved client behind an `Arc`:
/// commit hands the same value to `MqttClientHandle`, so the CA bundle is
/// copied once at prepare and never again. The old side of a change is not
/// carried — commit reaches the predecessor through the registry, which is
/// where the subscription set it hands the successor lives.
#[derive(Default)]
pub(crate) struct MqttClientsDelta {
    /// Clients the candidate declares and the registry does not hold, in
    /// candidate document order.
    pub added: Vec<Arc<MqttClientConfig>>,
    /// Slugs the registry holds and the candidate does not declare. A set, so
    /// the status body that reads it verbatim and the filter suppression that
    /// tests membership both get what they need without a sort.
    pub removed: BTreeSet<String>,
    /// Clients on both sides whose resolved value moved, in candidate document
    /// order.
    pub changed: Vec<Arc<MqttClientConfig>>,
    /// The slugs in [`Self::changed`] whose `qos` moved: the running session's
    /// `qos` differs from the candidate's. Each such client's successor
    /// subscribes every filter it inherits at the new `qos` on its first
    /// connect.
    pub qos_moved: BTreeSet<String>,
}

/// The slugs of a list of clients, as the status body names them.
pub(crate) fn slugs(clients: &[Arc<MqttClientConfig>]) -> Vec<String> {
    clients
        .iter()
        .map(|client| client.identity.slug.clone())
        .collect()
}

impl MqttClientsDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

/// Which clients moved, comparing the live registry against the candidate's
/// resolved clients.
pub(crate) fn mqtt_clients_delta(
    live: &HashMap<String, Arc<MqttClientConfig>>,
    candidate: &IndexMap<String, MqttClientConfig>,
) -> MqttClientsDelta {
    let mut delta = MqttClientsDelta::default();
    for (slug, client) in candidate {
        match live.get(slug) {
            None => delta.added.push(Arc::new(client.clone())),
            Some(running) if running.as_ref() != client => {
                if running.identity.qos != client.identity.qos {
                    delta.qos_moved.insert(slug.clone());
                }
                delta.changed.push(Arc::new(client.clone()));
            }
            Some(_) => {}
        }
    }
    delta.removed = live
        .keys()
        .filter(|slug| !candidate.contains_key(*slug))
        .cloned()
        .collect();
    delta
}

/// What moves on one client's broker session. Each list holds topic filters.
///
/// `resubscribe` names the filters a `qos` edit moves: every filter both unions
/// hold on a client whose `qos` this reload changes. It is a report, not an
/// action. The `qos` edit restarts the client, and the successor inherits the
/// filter set and subscribes every filter in it at its own `qos` on its first
/// connect; MQTT 5 has the broker replace its subscription for the filter in
/// place, so the filter is in neither [`Self::leaving`] nor [`Self::joining`]
/// and no copy the broker sends for it meanwhile goes unrouted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MqttClientDelta {
    pub client: String,
    pub subscribe: Vec<String>,
    pub unsubscribe: Vec<String>,
    pub resubscribe: Vec<String>,
}

impl MqttClientDelta {
    fn is_empty(&self) -> bool {
        self.subscribe.is_empty() && self.unsubscribe.is_empty() && self.resubscribe.is_empty()
    }

    /// The filters the outgoing step issues an UNSUBSCRIBE for.
    pub(crate) fn leaving(&self) -> impl Iterator<Item = &String> {
        self.unsubscribe.iter()
    }

    /// The filters the incoming step adds to the session with a SUBSCRIBE.
    pub(crate) fn joining(&self) -> impl Iterator<Item = &String> {
        self.subscribe.iter()
    }
}

/// A route this reload takes out of the table, named for the diagnostics and
/// the status body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RouteRemoval {
    pub channel_uuid: Uuid,
    pub channel_address: String,
}

/// Everything the two commit steps do to the MQTT runtime.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MqttDelta {
    /// One entry per client whose broker set moves, in candidate-then-baseline
    /// declaration order.
    pub clients: Vec<MqttClientDelta>,
    /// The routes the candidate's `mqtt:` entries need, built as boot builds
    /// them.
    pub routes_added: Vec<IngressRoute>,
    /// The routes the departing `mqtt:` entries leave behind.
    pub routes_removed: Vec<RouteRemoval>,
}

impl MqttDelta {
    pub fn is_empty(&self) -> bool {
        self.clients.iter().all(MqttClientDelta::is_empty)
            && self.routes_added.is_empty()
            && self.routes_removed.is_empty()
    }

    /// Every filter this reload subscribes, as the status body names it: the
    /// filters joining the session, then those a `qos` edit re-subscribes.
    pub fn subscribed(&self) -> Vec<String> {
        self.clients
            .iter()
            .flat_map(|client| {
                client
                    .joining()
                    .chain(client.resubscribe.iter())
                    .map(|filter| address_of(&client.client, filter))
            })
            .collect()
    }

    /// Every filter the outgoing step unsubscribes, as the status body names
    /// it.
    pub fn unsubscribed(&self) -> Vec<String> {
        self.clients
            .iter()
            .flat_map(|client| {
                client
                    .leaving()
                    .map(|filter| address_of(&client.client, filter))
            })
            .collect()
    }
}

/// A filter as an operator reads it: the address of the channel it carries.
pub(crate) fn address_of(client: &str, topic_filter: &str) -> String {
    format!("mqtt:{client}:{topic_filter}")
}

/// One side's whole `mqtt:` ingress: what a process running this document
/// would have subscribed at the broker and routed.
///
/// Two parts, because they are derived from two places and only one of them is
/// in the plan. `static_` is the document's own `mqtt_subscription` lines.
/// `dynamic` is the live dynamic `mqtt:` subscriptions that side stands behind —
/// on the baseline the ones this process holds folded, on the candidate the ones
/// it would still hold after the re-merge. Both are the oracle's: a fresh boot
/// derives its SUBSCRIBE union and its route table from the static channels
/// *plus* every dynamic row its merge kept, so a diff over the static half alone
/// would unsubscribe filters the candidate still needs.
///
/// A uuid in `static_` is never also in `dynamic`: the merge that produces the
/// dynamic half excludes the side's own static channels, because one filter on
/// one client is one channel and the static declaration is the one that stands.
pub(crate) struct MqttIngressSet<'a> {
    pub static_: &'a [ResolvedMqttIngressChannel],
    pub dynamic: Vec<ResolvedMqttIngressChannel>,
}

impl<'a> MqttIngressSet<'a> {
    /// A side that stands behind no dynamic subscription. Only a test is ever
    /// that side: a running process reaches this through the two sets the
    /// re-merge derives, empty or not.
    #[cfg(test)]
    pub(crate) fn only_static(static_: &'a [ResolvedMqttIngressChannel]) -> Self {
        Self {
            static_,
            dynamic: Vec::new(),
        }
    }

    /// Both halves as one list, which is the grain the broker's union and the
    /// route table are built at.
    fn all(&self) -> Vec<ResolvedMqttIngressChannel> {
        self.static_.iter().chain(&self.dynamic).cloned().collect()
    }

    fn uuids(&self) -> HashSet<Uuid> {
        self.static_
            .iter()
            .chain(&self.dynamic)
            .map(|channel| channel.channel_uuid)
            .collect()
    }
}

/// The MQTT half of the plan delta.
///
/// The broker's SUBSCRIBE set is diffed over the two sides' whole ingress —
/// static and dynamic — because that is the set a fresh boot of each document
/// would assert. The route table is diffed at two grains: a static channel's
/// route moves when the channel delta moves the channel, and a dynamic
/// subscription's route moves when the re-merge revoked or revived it, which no
/// channel delta can see.
///
/// `clients.removed` are the slugs whose session this reload stops. They
/// contribute no filter move at all: their filters leave with the session, and
/// an UNSUBSCRIBE reported against a client that is about to be disconnected
/// would name a packet nothing sent — a disconnected supervisor defers the
/// packet, and a stopped one never reconnects to send it. Their routes still
/// go: the ingress router's table has to lose them.
///
/// `clients.qos_moved` are the slugs whose `qos` this reload changes. Every
/// filter on both sides of such a client, static or dynamic, is in its
/// [`MqttClientDelta::resubscribe`]; for any other client that list is empty.
pub(crate) fn mqtt_delta(
    baseline: &MqttIngressSet<'_>,
    candidate: &MqttIngressSet<'_>,
    channels_leaving: &[&ChannelEntry],
    channels_joining: &[&ChannelEntry],
    clients: &MqttClientsDelta,
) -> MqttDelta {
    let baseline_all = baseline.all();
    let candidate_all = candidate.all();
    let mut slugs: Vec<&str> = Vec::new();
    for channel in candidate_all.iter().chain(&baseline_all) {
        if !slugs.contains(&channel.client_slug.as_str()) {
            slugs.push(channel.client_slug.as_str());
        }
    }

    let mut delta = MqttDelta::default();
    for client in slugs {
        if clients.removed.contains(client) {
            continue;
        }
        let before = union_subscriptions(client, &baseline_all);
        let after = union_subscriptions(client, &candidate_all);
        let mut moved = MqttClientDelta {
            client: client.to_string(),
            ..MqttClientDelta::default()
        };
        let qos_moved = clients.qos_moved.contains(client);
        for sub in &after {
            let filter = sub.topic_filter.clone();
            if !before
                .iter()
                .any(|old| old.topic_filter == sub.topic_filter)
            {
                moved.subscribe.push(filter);
            } else if qos_moved {
                moved.resubscribe.push(filter);
            }
        }
        for sub in &before {
            if !after.iter().any(|new| new.topic_filter == sub.topic_filter) {
                moved.unsubscribe.push(sub.topic_filter.clone());
            }
        }
        if !moved.is_empty() {
            delta.clients.push(moved);
        }
    }

    let by_uuid: BTreeMap<Uuid, &ResolvedMqttIngressChannel> = candidate
        .static_
        .iter()
        .map(|channel| (channel.channel_uuid, channel))
        .collect();
    for entry in channels_joining {
        if entry.transport_type != ChannelScheme::Mqtt {
            continue;
        }
        // The planner minted this entry from one of the candidate's ingress
        // channels, so the lookup is total; a miss means the two derivations
        // disagree, which is a host bug rather than an operator's problem.
        let channel = by_uuid.get(&entry.uuid).unwrap_or_else(|| {
            panic!(
                "reload: mqtt channel {:?} is in the channel delta but the candidate plan holds \
                 no ingress channel with its uuid — host bug",
                entry.address
            )
        });
        delta.routes_added.push(IngressRoute::from(*channel));
    }
    for entry in channels_leaving {
        if entry.transport_type != ChannelScheme::Mqtt {
            continue;
        }
        delta.routes_removed.push(RouteRemoval {
            channel_uuid: entry.uuid,
            channel_address: entry.address.clone(),
        });
    }

    // The dynamic half of the route table, which the channel delta cannot
    // name: a revived subscription needs the route a dormant row never had, and
    // a revoked one leaves a route nothing will match. Both are asked against
    // the *whole* other side, so a row the candidate declares statically
    // instead — its route already in the table, its channel already in the
    // directory — moves nothing.
    let baseline_uuids = baseline.uuids();
    let candidate_uuids = candidate.uuids();
    for channel in &candidate.dynamic {
        if !baseline_uuids.contains(&channel.channel_uuid) {
            delta.routes_added.push(IngressRoute::from(channel));
        }
    }
    for channel in &baseline.dynamic {
        if !candidate_uuids.contains(&channel.channel_uuid) {
            delta.routes_removed.push(RouteRemoval {
                channel_uuid: channel.channel_uuid,
                channel_address: channel.channel_address.clone(),
            });
        }
    }
    delta
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_lib::messaging::test_support::test_channel_entry;
    use brenn_lib::messaging::{Urgency, mqtt_channel_uuid_from_address};
    use brenn_lib::mqtt::config::parsed_address_canonical;

    fn ingress(client: &str, topic: &str) -> ResolvedMqttIngressChannel {
        let address = parsed_address_canonical(client, topic);
        ResolvedMqttIngressChannel {
            channel_uuid: mqtt_channel_uuid_from_address(&address),
            channel_address: address,
            client_slug: client.to_string(),
            topic: topic.to_string(),
            urgency: Urgency::Normal,
        }
    }

    /// The directory entry the planner mints for an ingress channel.
    fn entry(channel: &ResolvedMqttIngressChannel) -> ChannelEntry {
        let mut entry = test_channel_entry(&channel.channel_address, Vec::new());
        entry.uuid = channel.channel_uuid;
        entry.transport_type = ChannelScheme::Mqtt;
        entry
    }

    #[test]
    fn a_new_ingress_channel_subscribes_its_filter_and_adds_its_route() {
        let arriving = ingress("chef", "a/b");
        let entry = entry(&arriving);
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(&[]),
            &MqttIngressSet::only_static(std::slice::from_ref(&arriving)),
            &[],
            &[&entry],
            &MqttClientsDelta::default(),
        );

        assert_eq!(delta.clients.len(), 1);
        assert_eq!(delta.clients[0].subscribe, vec!["a/b"]);
        assert!(delta.clients[0].unsubscribe.is_empty());
        assert_eq!(delta.routes_added.len(), 1);
        assert_eq!(delta.routes_added[0].topic_filter, "a/b");
        assert_eq!(delta.routes_added[0].channel_uuid, arriving.channel_uuid);
        assert!(delta.routes_removed.is_empty());
        assert_eq!(delta.subscribed(), vec!["mqtt:chef:a/b".to_string()]);
    }

    #[test]
    fn a_departing_ingress_channel_unsubscribes_and_loses_its_route() {
        let leaving = ingress("chef", "a/b");
        let entry = entry(&leaving);
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(std::slice::from_ref(&leaving)),
            &MqttIngressSet::only_static(&[]),
            &[&entry],
            &[],
            &MqttClientsDelta::default(),
        );

        assert_eq!(delta.clients[0].unsubscribe, vec!["a/b"]);
        assert!(delta.clients[0].subscribe.is_empty());
        assert_eq!(delta.routes_removed.len(), 1);
        assert_eq!(delta.routes_removed[0].channel_uuid, leaving.channel_uuid);
        assert_eq!(delta.unsubscribed(), vec!["mqtt:chef:a/b".to_string()]);
    }

    /// Two filters on one client, one of them departing: the diff is per
    /// filter, so the survivor's subscription is not touched.
    ///
    /// The case where a filter is *shared* — a dynamic subscription sitting on
    /// a config channel's filter — cannot be modelled here, because one filter
    /// on one client is one address is one channel: it lives in
    /// [`super::delta::live_subscriber_refusals`], which refuses that reload
    /// before any UNSUBSCRIBE is computed.
    #[test]
    fn one_of_two_filters_departing_leaves_the_other_subscribed() {
        let stays = ingress("chef", "a/b");
        let goes = ingress("chef", "c/d");
        let goes_entry = entry(&goes);
        let both = [stays.clone(), goes.clone()];
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(&both),
            &MqttIngressSet::only_static(std::slice::from_ref(&stays)),
            &[&goes_entry],
            &[],
            &MqttClientsDelta::default(),
        );

        assert_eq!(delta.clients[0].unsubscribe, vec!["c/d"]);
        assert!(
            !delta.clients[0].unsubscribe.iter().any(|one| one == "a/b"),
            "the surviving filter was unsubscribed",
        );
        assert_eq!(delta.routes_removed.len(), 1);
    }

    /// A client delta whose only fact is that `slug`'s `qos` moved.
    fn qos_moved(slug: &str) -> MqttClientsDelta {
        MqttClientsDelta {
            qos_moved: [slug.to_string()].into_iter().collect(),
            ..MqttClientsDelta::default()
        }
    }

    /// On a client whose `qos` this reload moves, every filter both sides hold,
    /// static or dynamic, is re-subscribed in place: it neither leaves the
    /// session nor joins it, and the status body names it as subscribed only.
    /// A filter only the candidate holds still joins, one only the baseline
    /// holds still leaves.
    #[test]
    fn a_qos_move_resubscribes_every_filter_both_sides_hold() {
        let static_kept = ingress("chef", "a/b");
        let dynamic_kept = ingress("chef", "d/e");
        let arriving = ingress("chef", "c/d");
        let departing = ingress("chef", "x/y");
        let arriving_entry = entry(&arriving);
        let departing_entry = entry(&departing);
        let baseline_static = [static_kept.clone(), departing.clone()];
        let candidate_static = [static_kept.clone(), arriving.clone()];
        let baseline = MqttIngressSet {
            static_: &baseline_static,
            dynamic: vec![dynamic_kept.clone()],
        };
        let candidate = MqttIngressSet {
            static_: &candidate_static,
            dynamic: vec![dynamic_kept.clone()],
        };
        let delta = mqtt_delta(
            &baseline,
            &candidate,
            &[&departing_entry],
            &[&arriving_entry],
            &qos_moved("chef"),
        );

        assert_eq!(delta.clients.len(), 1);
        assert_eq!(delta.clients[0].resubscribe, vec!["a/b", "d/e"]);
        assert_eq!(delta.clients[0].subscribe, vec!["c/d"]);
        assert_eq!(delta.clients[0].unsubscribe, vec!["x/y"]);
        assert_eq!(
            delta.subscribed(),
            vec![
                "mqtt:chef:c/d".to_string(),
                "mqtt:chef:a/b".to_string(),
                "mqtt:chef:d/e".to_string(),
            ]
        );
        assert_eq!(delta.unsubscribed(), vec!["mqtt:chef:x/y".to_string()]);

        let unmoved = mqtt_delta(
            &baseline,
            &candidate,
            &[&departing_entry],
            &[&arriving_entry],
            &MqttClientsDelta::default(),
        );
        assert!(
            unmoved.clients[0].resubscribe.is_empty(),
            "a client whose qos did not move re-subscribes nothing",
        );
        assert_eq!(unmoved.clients[0].subscribe, vec!["c/d"]);
        assert_eq!(unmoved.clients[0].unsubscribe, vec!["x/y"]);
    }

    /// A `qos` move on a client whose filters are unchanged still names each of
    /// them, a dynamic-only one included.
    #[test]
    fn a_qos_move_on_unmoved_filters_names_each_as_subscribed() {
        let dynamic = ingress("chef", "a/b");
        let side = || MqttIngressSet {
            static_: &[],
            dynamic: vec![dynamic.clone()],
        };
        let delta = mqtt_delta(&side(), &side(), &[], &[], &qos_moved("chef"));

        assert!(delta.clients[0].subscribe.is_empty());
        assert!(delta.clients[0].unsubscribe.is_empty());
        assert_eq!(delta.clients[0].leaving().count(), 0);
        assert_eq!(delta.clients[0].joining().count(), 0);
        assert_eq!(delta.subscribed(), vec!["mqtt:chef:a/b".to_string()]);
        assert!(delta.routes_added.is_empty());
        assert!(delta.routes_removed.is_empty());
    }

    /// The diff is over the two plans and the channel delta, and over nothing
    /// else: a filter no plan holds and a route no plan entry keys are not
    /// inputs here at all, which is why a purely dynamic subscription survives
    /// a reload that moves another filter on the same client. This asserts the
    /// grain — one client moving one filter yields exactly that filter and
    /// exactly that route; the live-runtime half of the case is
    /// `a_dynamically_minted_filter_and_route_survive_a_reload` in
    /// [`super::driver`].
    #[test]
    fn only_what_the_plans_and_the_channel_delta_hold_is_in_the_delta() {
        let kept = ingress("chef", "a/b");
        let arriving = ingress("chef", "c/d");
        let arriving_entry = entry(&arriving);
        let candidate = [kept.clone(), arriving.clone()];
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(std::slice::from_ref(&kept)),
            &MqttIngressSet::only_static(&candidate),
            &[],
            &[&arriving_entry],
            &MqttClientsDelta::default(),
        );

        assert_eq!(delta.clients.len(), 1);
        assert_eq!(delta.clients[0].subscribe, vec!["c/d"]);
        assert!(delta.clients[0].unsubscribe.is_empty());
        assert!(delta.clients[0].resubscribe.is_empty());
        assert_eq!(delta.routes_added.len(), 1);
        assert_eq!(delta.routes_added[0].channel_uuid, arriving.channel_uuid);
        assert!(delta.routes_removed.is_empty());
    }

    /// A dynamic subscription the re-merge revoked leaves the broker set and
    /// takes its route with it — the state a fresh boot of the candidate would
    /// be in, where the merge holds the row dormant and derives neither.
    #[test]
    fn a_revoked_dynamic_subscription_unsubscribes_its_filter_and_loses_its_route() {
        let dynamic = ingress("chef", "a/b");
        let delta = mqtt_delta(
            &MqttIngressSet {
                static_: &[],
                dynamic: vec![dynamic.clone()],
            },
            &MqttIngressSet::only_static(&[]),
            &[],
            &[],
            &MqttClientsDelta::default(),
        );

        assert_eq!(delta.clients[0].unsubscribe, vec!["a/b"]);
        assert_eq!(delta.routes_removed.len(), 1);
        assert_eq!(delta.routes_removed[0].channel_uuid, dynamic.channel_uuid);
        assert!(delta.routes_added.is_empty());
    }

    /// The mirror: a dormant row the candidate authorizes again is subscribed
    /// and routed, which is what boot does with the same row once the ACL is
    /// back.
    #[test]
    fn a_revived_dynamic_subscription_subscribes_its_filter_and_gains_its_route() {
        let dynamic = ingress("chef", "a/b");
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(&[]),
            &MqttIngressSet {
                static_: &[],
                dynamic: vec![dynamic.clone()],
            },
            &[],
            &[],
            &MqttClientsDelta::default(),
        );

        assert_eq!(delta.clients[0].subscribe, vec!["a/b"]);
        assert_eq!(delta.routes_added.len(), 1);
        assert_eq!(delta.routes_added[0].channel_uuid, dynamic.channel_uuid);
        assert!(delta.routes_removed.is_empty());
    }

    /// The case the plan-only diff got wrong, at the filter grain: a static
    /// channel departs while a dynamic subscription on the *same* filter stands
    /// on both sides. The union still holds the filter after, so nothing is
    /// unsubscribed — which is `unsubscribe_filter`'s "only when the last
    /// subscriber leaves" contract, discharged by the diff rather than argued
    /// around it.
    ///
    /// The pairing itself is unreachable in production and is not a state this
    /// asserts is desirable: an `mqtt:` channel's uuid is derived from its
    /// address, so one filter is one channel, and rule 2
    /// (`live_subscriber_refusals`) refuses any reload that takes a channel
    /// away under a dynamic row on it. What is under test is the union
    /// arithmetic, not a shape the walk should ever produce — the route
    /// assertion below records that the static channel's route goes with the
    /// channel, which is what leaves the filter with nothing to route to and
    /// is exactly why rule 2 refuses first.
    #[test]
    fn a_dynamic_subscription_on_a_departing_filter_keeps_it_subscribed() {
        let shared = ingress("chef", "a/b");
        let entry = entry(&shared);
        let delta = mqtt_delta(
            &MqttIngressSet {
                static_: std::slice::from_ref(&shared),
                dynamic: Vec::new(),
            },
            &MqttIngressSet {
                static_: &[],
                dynamic: vec![shared.clone()],
            },
            &[&entry],
            &[],
            &MqttClientsDelta::default(),
        );

        assert!(
            delta.clients.is_empty() || delta.clients[0].unsubscribe.is_empty(),
            "a filter the candidate still stands behind was unsubscribed",
        );
        // The static channel's own route still goes: the channel delta is what
        // says the entry left, and the dynamic side keeps the filter, not the
        // channel.
        assert_eq!(delta.routes_removed.len(), 1);
    }

    /// The reachable shape of the same arithmetic: a static channel departs
    /// while a dynamic subscription on a *different* topic stands on both
    /// sides. The departing filter is unsubscribed and its route removed; the
    /// dynamic one keeps both.
    #[test]
    fn a_dynamic_subscription_beside_a_departing_one_keeps_its_filter_and_route() {
        let departing = ingress("chef", "a/b");
        let kept = ingress("chef", "c/d");
        let entry = entry(&departing);
        let delta = mqtt_delta(
            &MqttIngressSet {
                static_: std::slice::from_ref(&departing),
                dynamic: vec![kept.clone()],
            },
            &MqttIngressSet {
                static_: &[],
                dynamic: vec![kept.clone()],
            },
            &[&entry],
            &[],
            &MqttClientsDelta::default(),
        );

        assert_eq!(delta.clients[0].unsubscribe, vec!["a/b"]);
        assert_eq!(
            delta
                .routes_removed
                .iter()
                .map(|removal| removal.channel_uuid)
                .collect::<Vec<_>>(),
            vec![departing.channel_uuid],
            "the kept dynamic channel's route stays",
        );
        assert!(delta.routes_added.is_empty());
    }

    /// A dynamic subscription both sides stand behind moves nothing at all —
    /// the common case, and the one that says the dynamic half is a set
    /// comparison rather than a re-derivation.
    #[test]
    fn a_dynamic_subscription_on_both_sides_moves_nothing() {
        let dynamic = ingress("chef", "a/b");
        let delta = mqtt_delta(
            &MqttIngressSet {
                static_: &[],
                dynamic: vec![dynamic.clone()],
            },
            &MqttIngressSet {
                static_: &[],
                dynamic: vec![dynamic],
            },
            &[],
            &[],
            &MqttClientsDelta::default(),
        );

        assert!(delta.is_empty());
    }

    /// A resolved client, with the password as the only thing a case varies:
    /// the delta is over the whole value, so one field standing in for the
    /// credentials proves the comparison reaches them.
    fn client(slug: &str, password: Option<&str>) -> MqttClientConfig {
        let mut config = brenn_lib::mqtt::test_support::test_client_config(slug);
        config.password = password.map(ToString::to_string);
        config
    }

    fn live(clients: &[MqttClientConfig]) -> HashMap<String, Arc<MqttClientConfig>> {
        clients
            .iter()
            .map(|client| (client.identity.slug.clone(), Arc::new(client.clone())))
            .collect()
    }

    fn candidate(clients: &[MqttClientConfig]) -> IndexMap<String, MqttClientConfig> {
        clients
            .iter()
            .map(|client| (client.identity.slug.clone(), client.clone()))
            .collect()
    }

    #[test]
    fn an_unmoved_declaration_set_moves_no_client() {
        let clients = [client("ha", None), client("spare", Some("s3cret"))];
        let delta = mqtt_clients_delta(&live(&clients), &candidate(&clients));

        assert!(delta.is_empty());
    }

    #[test]
    fn a_rotated_password_is_a_changed_client() {
        let delta = mqtt_clients_delta(
            &live(&[client("ha", Some("old"))]),
            &candidate(&[client("ha", Some("new"))]),
        );

        assert_eq!(slugs(&delta.changed), vec!["ha".to_string()]);
        assert_eq!(delta.changed[0].password.as_deref(), Some("new"));
        assert!(delta.added.is_empty() && delta.removed.is_empty());
        assert!(
            delta.qos_moved.is_empty(),
            "a client changed in another field only has not moved its qos",
        );
    }

    #[test]
    fn a_qos_edit_is_a_changed_client_whose_qos_moved() {
        let running = client("ha", None);
        let mut edited = running.clone();
        edited.identity.qos = 0;
        let delta = mqtt_clients_delta(&live(&[running]), &candidate(&[edited]));

        assert_eq!(slugs(&delta.changed), vec!["ha".to_string()]);
        assert_eq!(
            delta.qos_moved.iter().cloned().collect::<Vec<_>>(),
            vec!["ha".to_string()]
        );
    }

    /// `added` and `changed` follow the candidate's declaration order, which is
    /// the order commit registers them in and the order the status body reads.
    /// `removed`'s side is the registry's hash map, so it is a set, read out
    /// in slug order.
    #[test]
    fn the_three_lists_are_ordered_as_the_status_body_reads_them() {
        let delta = mqtt_clients_delta(
            &live(&[
                client("zeta", None),
                client("alpha", None),
                client("moved", Some("old")),
            ]),
            &candidate(&[
                client("second", None),
                client("moved", Some("new")),
                client("first", None),
            ]),
        );

        assert_eq!(
            slugs(&delta.added),
            vec!["second".to_string(), "first".to_string()]
        );
        assert_eq!(
            delta.removed.iter().cloned().collect::<Vec<_>>(),
            vec!["alpha".to_string(), "zeta".to_string()],
        );
        assert_eq!(slugs(&delta.changed), vec!["moved".to_string()]);
    }

    /// A client the candidate stops contributes no filter move: its filters
    /// leave with its session, and an UNSUBSCRIBE reported against it would
    /// name a packet nothing sent. Its routes still go.
    #[test]
    fn a_removed_clients_filters_are_not_moves_but_its_routes_are() {
        let leaving = ingress("chef", "a/b");
        let entry = entry(&leaving);
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(std::slice::from_ref(&leaving)),
            &MqttIngressSet::only_static(&[]),
            &[&entry],
            &[],
            &MqttClientsDelta {
                removed: ["chef".to_string()].into_iter().collect(),
                ..MqttClientsDelta::default()
            },
        );

        assert!(
            delta.clients.is_empty(),
            "{:?}",
            delta.clients.first().map(|c| c.client.clone())
        );
        assert!(delta.unsubscribed().is_empty());
        assert_eq!(delta.routes_removed.len(), 1);
    }
}

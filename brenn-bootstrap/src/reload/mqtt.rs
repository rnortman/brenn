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

use std::collections::{BTreeMap, HashSet};

use brenn_envelope::ChannelScheme;
use brenn_lib::messaging::ChannelEntry;
use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
use brenn_mqtt::union_subscriptions;
use brenn_server::mqtt_router::IngressRoute;
use uuid::Uuid;

/// One filter's place in a client's broker set after this reload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilterMove {
    pub topic_filter: String,
    /// The qos the candidate's union asks for. On an unsubscribe this is the
    /// baseline's, and nothing reads it.
    pub qos: u8,
}

/// What moves on one client's broker session.
///
/// `resubscribe` is a qos move on a filter both unions hold:
/// `add_subscription` asserts qos equality for a filter already in the set, so
/// the only way to raise or lower one is to take it out and put it back. It is
/// therefore in both the outgoing and the incoming step, in that order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MqttClientDelta {
    pub client: String,
    pub subscribe: Vec<FilterMove>,
    pub unsubscribe: Vec<FilterMove>,
    pub resubscribe: Vec<FilterMove>,
}

impl MqttClientDelta {
    fn is_empty(&self) -> bool {
        self.subscribe.is_empty() && self.unsubscribe.is_empty() && self.resubscribe.is_empty()
    }

    /// The filters the outgoing step issues an UNSUBSCRIBE for, in that order.
    pub(crate) fn leaving(&self) -> impl Iterator<Item = &FilterMove> {
        self.unsubscribe.iter().chain(self.resubscribe.iter())
    }

    /// The filters the incoming step issues a SUBSCRIBE for, in that order.
    pub(crate) fn joining(&self) -> impl Iterator<Item = &FilterMove> {
        self.subscribe.iter().chain(self.resubscribe.iter())
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

    /// Every filter the incoming step subscribes, as the status body names it.
    pub fn subscribed(&self) -> Vec<String> {
        self.clients
            .iter()
            .flat_map(|client| {
                client
                    .joining()
                    .map(|filter| address_of(&client.client, &filter.topic_filter))
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
                    .map(|filter| address_of(&client.client, &filter.topic_filter))
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
pub(crate) fn mqtt_delta(
    baseline: &MqttIngressSet<'_>,
    candidate: &MqttIngressSet<'_>,
    channels_leaving: &[&ChannelEntry],
    channels_joining: &[&ChannelEntry],
) -> MqttDelta {
    let baseline_all = baseline.all();
    let candidate_all = candidate.all();
    let mut clients: Vec<&str> = Vec::new();
    for channel in candidate_all.iter().chain(&baseline_all) {
        if !clients.contains(&channel.client_slug.as_str()) {
            clients.push(channel.client_slug.as_str());
        }
    }

    let mut delta = MqttDelta::default();
    for client in clients {
        let before = union_subscriptions(client, &baseline_all);
        let after = union_subscriptions(client, &candidate_all);
        let mut moved = MqttClientDelta {
            client: client.to_string(),
            ..MqttClientDelta::default()
        };
        for sub in &after {
            match before
                .iter()
                .find(|old| old.topic_filter == sub.topic_filter)
            {
                None => moved.subscribe.push(FilterMove {
                    topic_filter: sub.topic_filter.clone(),
                    qos: sub.qos,
                }),
                Some(old) if old.qos != sub.qos => moved.resubscribe.push(FilterMove {
                    topic_filter: sub.topic_filter.clone(),
                    qos: sub.qos,
                }),
                Some(_) => {}
            }
        }
        for sub in &before {
            if !after.iter().any(|new| new.topic_filter == sub.topic_filter) {
                moved.unsubscribe.push(FilterMove {
                    topic_filter: sub.topic_filter.clone(),
                    qos: sub.qos,
                });
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

    fn ingress(client: &str, topic: &str, qos: u8) -> ResolvedMqttIngressChannel {
        let address = parsed_address_canonical(client, topic);
        ResolvedMqttIngressChannel {
            channel_uuid: mqtt_channel_uuid_from_address(&address),
            channel_address: address,
            client_slug: client.to_string(),
            topic: topic.to_string(),
            qos,
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

    fn filters(moves: &[FilterMove]) -> Vec<(&str, u8)> {
        moves
            .iter()
            .map(|one| (one.topic_filter.as_str(), one.qos))
            .collect()
    }

    #[test]
    fn a_new_ingress_channel_subscribes_its_filter_and_adds_its_route() {
        let arriving = ingress("chef", "a/b", 1);
        let entry = entry(&arriving);
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(&[]),
            &MqttIngressSet::only_static(std::slice::from_ref(&arriving)),
            &[],
            &[&entry],
        );

        assert_eq!(delta.clients.len(), 1);
        assert_eq!(filters(&delta.clients[0].subscribe), vec![("a/b", 1)]);
        assert!(delta.clients[0].unsubscribe.is_empty());
        assert_eq!(delta.routes_added.len(), 1);
        assert_eq!(delta.routes_added[0].topic_filter, "a/b");
        assert_eq!(delta.routes_added[0].channel_uuid, arriving.channel_uuid);
        assert!(delta.routes_removed.is_empty());
        assert_eq!(delta.subscribed(), vec!["mqtt:chef:a/b".to_string()]);
    }

    #[test]
    fn a_departing_ingress_channel_unsubscribes_and_loses_its_route() {
        let leaving = ingress("chef", "a/b", 1);
        let entry = entry(&leaving);
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(std::slice::from_ref(&leaving)),
            &MqttIngressSet::only_static(&[]),
            &[&entry],
            &[],
        );

        assert_eq!(filters(&delta.clients[0].unsubscribe), vec![("a/b", 1)]);
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
        let stays = ingress("chef", "a/b", 1);
        let goes = ingress("chef", "c/d", 1);
        let goes_entry = entry(&goes);
        let both = [stays.clone(), goes.clone()];
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(&both),
            &MqttIngressSet::only_static(std::slice::from_ref(&stays)),
            &[&goes_entry],
            &[],
        );

        assert_eq!(filters(&delta.clients[0].unsubscribe), vec![("c/d", 1)]);
        assert!(
            !delta.clients[0]
                .unsubscribe
                .iter()
                .any(|one| one.topic_filter == "a/b"),
            "the surviving filter was unsubscribed",
        );
        assert_eq!(delta.routes_removed.len(), 1);
    }

    /// The union takes the max qos across a client's channels, so a raise on
    /// one of two channels sharing a client's filter is a re-SUBSCRIBE: the
    /// filter leaves the set and rejoins it at the new qos.
    #[test]
    fn a_qos_move_is_an_unsubscribe_then_a_subscribe() {
        let before = ingress("chef", "a/b", 0);
        let after = ingress("chef", "a/b", 2);
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(&[before]),
            &MqttIngressSet::only_static(&[after]),
            &[],
            &[],
        );

        assert!(delta.clients[0].subscribe.is_empty());
        assert!(delta.clients[0].unsubscribe.is_empty());
        assert_eq!(filters(&delta.clients[0].resubscribe), vec![("a/b", 2)]);
        // The order the two commit steps take it in: out first, then in.
        assert_eq!(
            filters(&delta.clients[0].leaving().cloned().collect::<Vec<_>>()),
            vec![("a/b", 2)]
        );
        assert_eq!(
            filters(&delta.clients[0].joining().cloned().collect::<Vec<_>>()),
            vec![("a/b", 2)]
        );
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
        let kept = ingress("chef", "a/b", 1);
        let arriving = ingress("chef", "c/d", 1);
        let arriving_entry = entry(&arriving);
        let candidate = [kept.clone(), arriving.clone()];
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(std::slice::from_ref(&kept)),
            &MqttIngressSet::only_static(&candidate),
            &[],
            &[&arriving_entry],
        );

        assert_eq!(delta.clients.len(), 1);
        assert_eq!(filters(&delta.clients[0].subscribe), vec![("c/d", 1)]);
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
        let dynamic = ingress("chef", "a/b", 1);
        let delta = mqtt_delta(
            &MqttIngressSet {
                static_: &[],
                dynamic: vec![dynamic.clone()],
            },
            &MqttIngressSet::only_static(&[]),
            &[],
            &[],
        );

        assert_eq!(filters(&delta.clients[0].unsubscribe), vec![("a/b", 1)]);
        assert_eq!(delta.routes_removed.len(), 1);
        assert_eq!(delta.routes_removed[0].channel_uuid, dynamic.channel_uuid);
        assert!(delta.routes_added.is_empty());
    }

    /// The mirror: a dormant row the candidate authorizes again is subscribed
    /// and routed, which is what boot does with the same row once the ACL is
    /// back.
    #[test]
    fn a_revived_dynamic_subscription_subscribes_its_filter_and_gains_its_route() {
        let dynamic = ingress("chef", "a/b", 1);
        let delta = mqtt_delta(
            &MqttIngressSet::only_static(&[]),
            &MqttIngressSet {
                static_: &[],
                dynamic: vec![dynamic.clone()],
            },
            &[],
            &[],
        );

        assert_eq!(filters(&delta.clients[0].subscribe), vec![("a/b", 1)]);
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
        let shared = ingress("chef", "a/b", 1);
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
        let departing = ingress("chef", "a/b", 1);
        let kept = ingress("chef", "c/d", 1);
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
        );

        assert_eq!(filters(&delta.clients[0].unsubscribe), vec![("a/b", 1)]);
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
        let dynamic = ingress("chef", "a/b", 1);
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
        );

        assert!(delta.is_empty());
    }
}

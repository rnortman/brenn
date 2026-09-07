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

use std::collections::BTreeMap;

use brenn_envelope::ChannelScheme;
use brenn_lib::messaging::ChannelEntry;
use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
use brenn_mqtt::union_subscriptions;
use brenn_server::mqtt_router::IngressRoute;
use uuid::Uuid;

use super::NEEDS_RESTART;

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

/// The MQTT half of the plan delta.
///
/// `baseline` and `candidate` are the two plans' *static* ingress channel
/// lists. A filter a dynamic subscription minted is in neither, which is what
/// keeps it out of both unions and out of every step below — its route is keyed
/// by a uuid no plan entry carries, so it is not in `routes_removed` either.
pub(crate) fn mqtt_delta(
    baseline: &[ResolvedMqttIngressChannel],
    candidate: &[ResolvedMqttIngressChannel],
    channels_leaving: &[&ChannelEntry],
    channels_joining: &[&ChannelEntry],
) -> MqttDelta {
    let mut clients: Vec<&str> = Vec::new();
    for channel in candidate.iter().chain(baseline) {
        if !clients.contains(&channel.client_slug.as_str()) {
            clients.push(channel.client_slug.as_str());
        }
    }

    let mut delta = MqttDelta::default();
    for client in clients {
        let before = union_subscriptions(client, baseline);
        let after = union_subscriptions(client, candidate);
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
    delta
}

/// Rule 6: a broker session is a boot-time fact, and this delta must not need
/// one that is not there or leave one that is.
///
/// Starting or stopping a supervisor at reload is possible but requires the
/// whole subsystem to exist lazily when the boot document referenced no client,
/// so a reference-set change is a refusal in both directions. Both keep the
/// oracle exact: a fresh boot has sessions for exactly the referenced set.
///
/// `sessions` is the set of clients that have a live handle right now;
/// `referenced` is the candidate's referenced-client set, computed the way boot
/// computes the set it spawns supervisors for, each with the thing in the
/// candidate that named it.
///
/// The needs-a-session arm is over the whole referenced set and not only over
/// the clients whose filters moved: a client enters the set through an
/// `mqtt_publish` matcher as well as through an ingress channel, and a
/// consumer authorized to publish on a client with no session panics the
/// egress path on its first publish (or fails closed with no service at all).
// TODO(reload-mqtt-sessions): start and stop broker supervisors at reload, so
// both arms below converge instead of asking for a restart.
pub(crate) fn session_refusals(
    delta: &MqttDelta,
    sessions: &[String],
    referenced: &[ReferencedClient],
) -> Vec<String> {
    let mut out = Vec::new();
    let mut said: Vec<&str> = Vec::new();
    // The filter-move arm first, so a client an ingress channel brought in is
    // named by the address an operator wrote rather than by the derived
    // reference. A client whose filters only leave is here and not in
    // `referenced`; it cannot be unsubscribed without a session either.
    let moving = delta.clients.iter().map(|client| {
        let filter = client
            .joining()
            .chain(client.leaving())
            .next()
            .map(|filter| filter.topic_filter.as_str())
            .unwrap_or_default();
        (client.client.as_str(), address_of(&client.client, filter))
    });
    let named = referenced
        .iter()
        .map(|one| (one.client.as_str(), one.named_by.clone()));
    for (client, named_by) in moving.chain(named) {
        if sessions.iter().any(|live| live == client) || said.contains(&client) {
            continue;
        }
        said.push(client);
        out.push(format!(
            "{named_by}: client {client:?} has no broker session (it was not referenced when the \
             process booted): {NEEDS_RESTART}",
        ));
    }
    for live in sessions {
        if !referenced.iter().any(|one| &one.client == live) {
            out.push(format!(
                "mqtt client {live:?} would lose its last reference: {NEEDS_RESTART}"
            ));
        }
    }
    out
}

/// One client the candidate references, and what in the candidate names it.
///
/// The attribution lets a refusal name the line that asked for the session —
/// as often an ACL matcher as an ingress binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReferencedClient {
    pub client: String,
    pub named_by: String,
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
        let delta = mqtt_delta(&[], std::slice::from_ref(&arriving), &[], &[&entry]);

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
        let delta = mqtt_delta(std::slice::from_ref(&leaving), &[], &[&entry], &[]);

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
        let delta = mqtt_delta(
            &[stays.clone(), goes.clone()],
            std::slice::from_ref(&stays),
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
        let delta = mqtt_delta(&[before], &[after], &[], &[]);

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
        let delta = mqtt_delta(
            std::slice::from_ref(&kept),
            &[kept.clone(), arriving.clone()],
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

    #[test]
    fn a_client_without_a_session_is_a_restart() {
        let arriving = ingress("chef", "a/b", 1);
        let entry = entry(&arriving);
        let delta = mqtt_delta(&[], &[arriving], &[], &[&entry]);
        let refusals = session_refusals(&delta, &[], &[referenced("chef", "mqtt:chef:a/b")]);

        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(refusals[0].contains("mqtt:chef:a/b"), "{refusals:?}");
        assert!(
            refusals[0].contains("has no broker session"),
            "{refusals:?}"
        );
        assert!(refusals[0].ends_with(NEEDS_RESTART), "{refusals:?}");
    }

    #[test]
    fn losing_a_clients_last_reference_is_a_restart() {
        let leaving = ingress("chef", "a/b", 1);
        let entry = entry(&leaving);
        let delta = mqtt_delta(&[leaving], &[], &[&entry], &[]);
        let refusals = session_refusals(&delta, &["chef".to_string()], &[]);

        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(
            refusals[0].contains("would lose its last reference"),
            "{refusals:?}"
        );
    }

    fn referenced(client: &str, named_by: &str) -> ReferencedClient {
        ReferencedClient {
            client: client.to_string(),
            named_by: named_by.to_string(),
        }
    }

    /// The arm no filter move reaches: a client enters the candidate's
    /// reference set through an `mqtt_publish` matcher, with no ingress channel
    /// anywhere. The egress path's "ACL-authorized implies a session" invariant
    /// is a panic, so this has to be a refusal.
    #[test]
    fn a_publish_matcher_on_a_client_without_a_session_is_a_restart() {
        let delta = mqtt_delta(&[], &[], &[], &[]);
        let refusals = session_refusals(
            &delta,
            &["chef".to_string()],
            &[
                referenced("chef", "mqtt:chef:a/b"),
                referenced("spare", "consumer `sifter`'s `mqtt_publish` matcher"),
            ],
        );

        assert_eq!(refusals.len(), 1, "{refusals:?}");
        assert!(
            refusals[0].starts_with("consumer `sifter`'s `mqtt_publish` matcher: client \"spare\""),
            "{refusals:?}",
        );
        assert!(
            refusals[0].contains("has no broker session"),
            "{refusals:?}"
        );
        assert!(refusals[0].ends_with(NEEDS_RESTART), "{refusals:?}");
    }

    #[test]
    fn a_client_that_keeps_its_session_and_its_reference_is_not_refused() {
        let arriving = ingress("chef", "a/b", 1);
        let entry = entry(&arriving);
        let delta = mqtt_delta(&[], &[arriving], &[], &[&entry]);
        assert!(
            session_refusals(
                &delta,
                &["chef".to_string()],
                &[referenced("chef", "mqtt:chef:a/b")],
            )
            .is_empty()
        );
    }
}

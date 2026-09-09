//! Build and start the MQTT service.

use std::sync::Arc;

use brenn_lib::mqtt::config::{MqttClientConfig, ResolvedMqttIngressChannel};
use brenn_mqtt::MqttService;
use brenn_mqtt::{ArrivingFilters, register_and_spawn, union_subscriptions};
use indexmap::IndexMap;
use tracing::info;

use brenn_server::mqtt_router::{IngressRoute, MqttEventRouterImpl};

/// Outcome of starting the MQTT service.
pub(crate) struct MqttResult {
    pub(crate) service: Arc<MqttService>,
    pub(crate) event_router: Arc<MqttEventRouterImpl>,
}

/// Build the MQTT service and spawn one unified supervisor per **declared**
/// `[[mqtt_client]]`. Each session carries both the publish path and the
/// ingress delivery + reconnect re-assert path.
///
/// The service and its router are built whether or not any client is declared;
/// a document with none gets an empty registry.
///
/// A declared client has a broker session for as long as the document declares
/// it, whether or not anything is bound through it: the operator wrote the
/// declaration, and an idle session costs a keepalive. That is what lets a
/// reload converge the first `mqtt:` binding a document ever puts on a broker.
///
/// `AppState` injection (`set_state` + `set_router`) must happen after
/// `AppState` construction — same deferred-state pattern as `WakeRouterImpl`.
///
/// # Panics
///
/// Panics if an ingress channel names a client this map does not declare. Such
/// a channel would get a router route and no subscription — a channel that
/// exists and can never receive.
pub(crate) async fn start_mqtt(
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
    clients: &IndexMap<String, MqttClientConfig>,
) -> MqttResult {
    // The subscription union below silently skips a channel whose client is
    // not the one being built, so an ingress channel on an undeclared client
    // would subscribe nothing and still be routed — a channel that exists and
    // can never receive. Callers must refuse such a channel before it reaches
    // here; this assert is the tripwire for that invariant.
    for channel in mqtt_ingress_channels {
        assert!(
            clients.contains_key(&channel.client_slug),
            "mqtt ingress channel {:?} names client {:?}, which no `[[mqtt_client]]` declares",
            channel.channel_address,
            channel.client_slug,
        );
    }

    let svc = MqttService::new();
    let router = Arc::new(MqttEventRouterImpl::new());
    let router_trait: Arc<dyn brenn_mqtt::MqttEventRouter> = router.clone();

    // One unified supervisor per declared client, in the map's order — which
    // nothing observes: `client_slugs` sorts, and the handles are reached by
    // slug. A client's subscription set is
    // the deduplicated union of its ingress-channel filters (empty for a client
    // nothing receives through — a connected publisher with zero
    // subscriptions).
    for (client_slug, broker_cfg) in clients {
        let subscriptions = union_subscriptions(client_slug, mqtt_ingress_channels);
        let subscription_count = subscriptions.len();
        register_and_spawn(
            &svc,
            Arc::new(broker_cfg.clone()),
            ArrivingFilters::Declared(subscriptions),
            router_trait.clone(),
        )
        .await;

        // Per client, because a declared client with no binding is otherwise
        // invisible: the channel listing decorates `mqtt:` channel entries and
        // an unbound client has none, so this line is where an operator reads
        // that the process holds a session for it at all.
        info!(
            client = %client_slug,
            host = %broker_cfg.identity.host,
            port = broker_cfg.identity.port,
            subscriptions = subscription_count,
            "MQTT client supervisor spawned"
        );
    }

    MqttResult {
        service: svc,
        event_router: router,
    }
}

/// Inject AppState into the MQTT event router and service.
///
/// # Panics
///
/// Panics if called more than once (the `OnceCell` inside `MqttEventRouterImpl`
/// does not allow re-setting).
pub(crate) async fn wire_mqtt_state(
    service: &Arc<MqttService>,
    router: &Arc<MqttEventRouterImpl>,
    state: brenn_server::state::AppState,
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
) {
    // Build the router's routing table from the distinct ingress channels, one
    // route per channel. The router fans inbound deliveries out to every
    // matching route.
    let routes: Vec<IngressRoute> = mqtt_ingress_channels
        .iter()
        .map(IngressRoute::from)
        .collect();
    let route_count = routes.len();
    router.set_state(state, routes);
    service
        .set_router(router.clone() as Arc<dyn brenn_mqtt::MqttEventRouter>)
        .await;
    info!(
        clients = service.client_slugs().len(),
        routes = route_count,
        "MQTT service started; supervisors running"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::messaging::{Urgency, mqtt_channel_uuid_from_address};
    use brenn_lib::mqtt::config::parsed_address_canonical;

    fn test_client(slug: &str) -> MqttClientConfig {
        brenn_server::test_support::mqtt::test_client_config(slug)
    }

    fn client_map(slugs: &[&str]) -> IndexMap<String, MqttClientConfig> {
        slugs
            .iter()
            .map(|s| ((*s).to_string(), test_client(s)))
            .collect()
    }

    fn test_ingress_channel(client: &str, topic: &str) -> ResolvedMqttIngressChannel {
        let address = parsed_address_canonical(client, topic);
        ResolvedMqttIngressChannel {
            channel_uuid: mqtt_channel_uuid_from_address(&address),
            channel_address: address,
            client_slug: client.to_string(),
            topic: topic.to_string(),
            urgency: Urgency::Normal,
            qos: 1,
        }
    }

    // --- start_mqtt activation ---

    #[tokio::test]
    async fn activates_with_ingress_only_no_connectors() {
        let result = start_mqtt(
            &[test_ingress_channel("cl", "sensors/#")],
            &client_map(&["cl"]),
        )
        .await;

        assert_eq!(result.service.client_slugs(), vec!["cl".to_string()]);
        assert_eq!(result.service.stop_all(), 1);
    }

    #[tokio::test]
    async fn activates_for_a_declared_client_nothing_references() {
        let result = start_mqtt(&[], &client_map(&["cl"])).await;

        assert_eq!(result.service.client_slugs(), vec!["cl".to_string()]);
        assert_eq!(result.service.stop_all(), 1);
    }

    #[tokio::test]
    async fn spawns_one_session_per_declared_client() {
        // Declared in non-alphabetical order, so the sorted `client_slugs`
        // below is a statement about the set and not about the map's order.
        let result = start_mqtt(
            &[test_ingress_channel("ha", "home/state")],
            &client_map(&["spare", "ha"]),
        )
        .await;

        assert_eq!(
            result.service.client_slugs(),
            vec!["ha".to_string(), "spare".to_string()]
        );
        assert_eq!(result.service.stop_all(), 2);
    }

    /// A route with no subscription behind it is a channel that exists and can
    /// never receive. Boot asserts the invariant rather than relying on callers
    /// alone.
    #[tokio::test]
    #[should_panic(expected = "which no `[[mqtt_client]]` declares")]
    async fn an_ingress_channel_on_an_undeclared_client_panics() {
        start_mqtt(
            &[test_ingress_channel("ghost", "sensors/#")],
            &client_map(&["cl"]),
        )
        .await;
    }

    /// A document declaring no client still gets a service and a router, both
    /// with an empty registry.
    #[tokio::test]
    async fn an_empty_declaration_set_builds_an_empty_registry() {
        let result = start_mqtt(&[], &IndexMap::new()).await;

        assert!(result.service.client_slugs().is_empty());
        assert_eq!(result.service.stop_all(), 0);
        assert!(result.event_router.route_uuids().is_empty());
    }
}

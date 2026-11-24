//! Build and start the MQTT service.

use std::sync::Arc;

use brenn_envelope::mqtt_connection::{PreviousEnd, PreviousSession};
use brenn_lib::mqtt::config::{MqttClientConfig, ResolvedMqttIngressChannel};
use brenn_mqtt::reader::EgressBus;
use brenn_mqtt::state::MqttClientHandle;
use brenn_mqtt::{MqttService, UnspawnedSession, register_client, union_subscriptions};
use indexmap::IndexMap;
use tracing::info;

use brenn_server::mqtt_connection::MqttConnectionHub;
use brenn_server::mqtt_egress::MqttEgressHub;
use brenn_server::mqtt_router::{IngressRoute, MqttEventRouterImpl};

/// Outcome of starting the MQTT service.
pub(crate) struct MqttResult {
    pub(crate) service: Arc<MqttService>,
    pub(crate) event_router: Arc<MqttEventRouterImpl>,
}

/// The ingress set boot builds its sessions and route table from: `static_`,
/// then one channel per durable dynamic `mqtt:` subscription the boot merge
/// kept, with `urgency` filled from the client the row names (the same
/// per-client constant a static channel on that client carries).
///
/// Without the appended channels a runtime-created `mqtt:` subscription to a
/// filter no static channel declares would stop delivering after a restart:
/// its SUBSCRIBE would not be re-asserted on connect and its deliveries would
/// have no route.
///
/// A kept row whose client is no longer declared in `[[mqtt_client]]` is
/// dropped with a warning: it was created against a configured client that the
/// operator has since removed, which is durable user state that config has
/// overridden, not a host bug. Its directory subscriber remains and nothing
/// delivers to it.
pub(crate) fn boot_ingress(
    static_: &[ResolvedMqttIngressChannel],
    clients: &IndexMap<String, MqttClientConfig>,
    dynamic: &[brenn_messaging_boot::DynamicMqttIngress],
) -> Vec<ResolvedMqttIngressChannel> {
    let mut ingress = static_.to_vec();
    for kept in dynamic {
        let Some(client) = clients.get(&kept.client_slug) else {
            tracing::warn!(
                client = %kept.client_slug,
                channel = %kept.channel_address,
                "boot: dropping dynamic mqtt subscription whose client is no longer a \
                 configured [[mqtt_client]] — not re-activating its broker SUBSCRIBE/route"
            );
            continue;
        };
        ingress.push(ResolvedMqttIngressChannel {
            channel_address: kept.channel_address.clone(),
            channel_uuid: kept.channel_uuid,
            client_slug: kept.client_slug.clone(),
            topic: kept.topic.clone(),
            urgency: client.identity.urgency,
        });
    }
    ingress
}

/// Build the MQTT service and its router, and register one client handle per
/// **declared** `[[mqtt_client]]`. Each session will carry both the publish
/// path and the ingress delivery + reconnect re-assert path. Nothing connects:
/// the returned sessions are spawned by [`wire_and_spawn`], once the router
/// has its state.
///
/// The service and its router are built whether or not any client is declared;
/// a document with none gets an empty registry.
///
/// A declared client has a broker session for as long as the document declares
/// it, whether or not anything is bound through it: the operator wrote the
/// declaration, and an idle session costs a keepalive. That is what lets a
/// reload converge the first `mqtt:` binding a document ever puts on a broker.
///
/// # Panics
///
/// Panics if an ingress channel names a client this map does not declare. Such
/// a channel would get a router route and no subscription — a channel that
/// exists and can never receive.
pub(crate) fn build_mqtt(
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
    clients: &IndexMap<String, MqttClientConfig>,
) -> (MqttResult, Vec<UnspawnedSession>) {
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

    // One handle per declared client, in the map's order — which nothing
    // observes: `client_slugs` sorts, and the handles are reached by slug. A
    // client's subscription set is the deduplicated union of its
    // ingress-channel filters (empty for a client nothing receives through — a
    // connected publisher with zero subscriptions).
    let mut sessions = Vec::with_capacity(clients.len());
    for (client_slug, broker_cfg) in clients {
        let subscriptions = union_subscriptions(client_slug, mqtt_ingress_channels);
        let subscription_count = subscriptions.len();
        sessions.push(register_client(
            &svc,
            Arc::new(broker_cfg.clone()),
            subscriptions,
        ));

        // Per client, because a declared client with no binding is otherwise
        // invisible: the channel listing decorates `mqtt:` channel entries and
        // an unbound client has none, so this line is where an operator reads
        // that the process holds a session for it at all.
        info!(
            client = %client_slug,
            host = %broker_cfg.identity.host,
            port = broker_cfg.identity.port,
            subscriptions = subscription_count,
            "MQTT client registered"
        );
    }

    (
        MqttResult {
            service: svc,
            event_router: router,
        },
        sessions,
    )
}

/// Give the MQTT event router its `AppState` and route table, install it on
/// the service, then spawn the supervisor of every session [`build_mqtt`]
/// registered. The one place boot spawns a supervisor, so that none can hand
/// the router a message before it can route it.
///
/// # Panics
///
/// Panics if called more than once.
pub(crate) async fn wire_and_spawn(
    service: &Arc<MqttService>,
    router: &Arc<MqttEventRouterImpl>,
    state: brenn_server::state::AppState,
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
    sessions: Vec<UnspawnedSession>,
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
    let router_trait: Arc<dyn brenn_mqtt::MqttEventRouter> = router.clone();
    service.set_router(router_trait.clone()).await;
    let spawned = sessions.len();
    for session in sessions {
        session.spawn(router_trait.clone()).await;
    }
    info!(
        clients = service.client_slugs().len(),
        spawned,
        routes = route_count,
        "MQTT router wired; supervisors spawned"
    );
}

/// The bus side of the MQTT clients.
pub(crate) struct MqttHubs {
    pub(crate) egress: Arc<MqttEgressHub>,
    pub(crate) connection: Arc<MqttConnectionHub>,
}

/// Do everything that must precede a client's first reader in this process,
/// and return the bus its reader is started from:
///
/// 1. attach the client's egress position, learning whether it resumed;
/// 2. read how the client's previous session ended;
/// 3. if uncleanly, tell every status channel named behind the position;
/// 4. publish the client's current connection state, which overwrites the
///    record, and start following its state.
///
/// The order is the guarantee: the markers are committed before the record is
/// overwritten, and the record is overwritten before anything is handed.
/// `found_at_ms` is when this process opened the client.
pub(crate) async fn prepare_client(
    egress: &Arc<MqttEgressHub>,
    connection: &MqttConnectionHub,
    handle: &MqttClientHandle,
    found_at_ms: i64,
) -> Arc<dyn EgressBus> {
    let client = handle.config.identity.slug.as_str();
    let opened = egress.open(client).await;
    let end = connection.previous_end(client, opened.resumed).await;
    if end == PreviousEnd::Unclean {
        let told = egress.tell_restarted(client, found_at_ms).await;
        connection.report_unclean(client, told);
    }
    connection
        .begin(
            client,
            handle.subscribe_state(),
            PreviousSession { end, found_at_ms },
        )
        .await;
    opened.bus
}

/// Put every registered client's egress channel into service: build the
/// hubs, prepare each client and start its reader, and spawn the relay that
/// turns the participant's one delivery wake into a wake for every reader.
///
/// Must run before anything can publish to an egress channel, so a fresh
/// position's skip covers only what an earlier client of the slug left, and
/// before any supervisor is spawned, so nothing is handed before a client's
/// connection-state record is overwritten.
///
/// The relay holds no timer: it waits on `notify` alone. A reader woken for
/// another client's channel reads its own once and finds nothing.
///
/// # Panics
///
/// If a registered client's egress channel is not in the directory (the plan
/// mints one per declared client), or a reader was already started on a
/// client's handle.
pub(crate) async fn start_mqtt_egress(
    service: &MqttService,
    messenger: Arc<brenn_messaging::Messenger>,
    alerts: brenn_obs::alerting::AlertDispatcher,
    notify: Arc<tokio::sync::Notify>,
) -> MqttHubs {
    let hubs = MqttHubs {
        egress: MqttEgressHub::new(Arc::clone(&messenger), alerts.clone()),
        connection: MqttConnectionHub::new(messenger, alerts),
    };
    let found_at_ms = chrono::Utc::now().timestamp_millis();
    // TODO(mqtt-egress-removed-client-rows): a client deleted from the
    // configuration while the server was down is not in this list, so no
    // reader is started for its channel; the messages it holds unread are
    // never reported `dropped`, `removed`, and the boot cursor reconcile
    // deletes the position that says which they are.
    for slug in service.client_slugs() {
        let handle = service
            .get_client(&slug)
            .unwrap_or_else(|| panic!("MQTT client {slug:?} was listed and is not registered"));
        let bus = prepare_client(&hubs.egress, &hubs.connection, &handle, found_at_ms).await;
        brenn_mqtt::start_egress_reader(&handle, bus);
        info!(client = %slug, "MQTT egress reader started");
    }
    // The relay does not keep the hub alive: a hub nothing else holds has no
    // reader to wake, and the relay ends at the first wake after it is gone.
    // The handle is dropped, as for the other drain tasks; a panic is process
    // death.
    let relay = Arc::downgrade(&hubs.egress);
    drop(tokio::spawn(brenn_lib::panic_util::abort_on_panic(
        "MQTT egress wake relay panicked",
        async move {
            loop {
                notify.notified().await;
                let Some(hub) = relay.upgrade() else {
                    return;
                };
                hub.wake_all();
            }
        },
    )));
    hubs
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
        }
    }

    // --- build_mqtt registration ---

    #[tokio::test]
    async fn activates_with_ingress_only_no_connectors() {
        let (result, sessions) = build_mqtt(
            &[test_ingress_channel("cl", "sensors/#")],
            &client_map(&["cl"]),
        );

        assert_eq!(result.service.client_slugs(), vec!["cl".to_string()]);
        assert_eq!(sessions.len(), 1);
        assert_eq!(result.service.stop_all(), 1);
    }

    #[tokio::test]
    async fn activates_for_a_declared_client_nothing_references() {
        let (result, sessions) = build_mqtt(&[], &client_map(&["cl"]));

        assert_eq!(result.service.client_slugs(), vec!["cl".to_string()]);
        assert_eq!(sessions.len(), 1);
        assert_eq!(result.service.stop_all(), 1);
    }

    /// Every declared client is registered and none has a supervisor; after
    /// `wire_and_spawn` every one has.
    #[tokio::test]
    async fn build_registers_every_client_unspawned_and_wire_and_spawn_spawns_each() {
        // Declared in non-alphabetical order, so the sorted `client_slugs`
        // below is a statement about the set and not about the map's order.
        let ingress = [test_ingress_channel("ha", "home/state")];
        let (result, sessions) = build_mqtt(&ingress, &client_map(&["spare", "ha"]));

        assert_eq!(
            result.service.client_slugs(),
            vec!["ha".to_string(), "spare".to_string()]
        );
        for slug in ["ha", "spare"] {
            let handle = result.service.get_client(slug).expect("registered");
            assert!(
                !handle.supervisor_spawned().await,
                "{slug} has no supervisor before the router is wired",
            );
        }
        assert!(!brenn_mqtt::MqttEventRouter::ready(&*result.event_router));

        let db = brenn_server::test_support::init_db_memory();
        wire_and_spawn(
            &result.service,
            &result.event_router,
            brenn_server::test_support::state::test_state(&db),
            &ingress,
            sessions,
        )
        .await;

        assert!(brenn_mqtt::MqttEventRouter::ready(&*result.event_router));
        assert_eq!(result.event_router.route_uuids().len(), 1);
        for slug in ["ha", "spare"] {
            let handle = result.service.get_client(slug).expect("registered");
            assert!(handle.supervisor_spawned().await, "{slug} is spawned");
        }
        assert_eq!(result.service.stop_all(), 2);
        assert_eq!(result.service.join_all().await, 2);
    }

    /// A route with no subscription behind it is a channel that exists and can
    /// never receive. Boot asserts the invariant rather than relying on callers
    /// alone.
    #[tokio::test]
    #[should_panic(expected = "which no `[[mqtt_client]]` declares")]
    async fn an_ingress_channel_on_an_undeclared_client_panics() {
        let _ = build_mqtt(
            &[test_ingress_channel("ghost", "sensors/#")],
            &client_map(&["cl"]),
        );
    }

    /// A document declaring no client still gets a service and a router, both
    /// with an empty registry.
    #[tokio::test]
    async fn an_empty_declaration_set_builds_an_empty_registry() {
        let (result, sessions) = build_mqtt(&[], &IndexMap::new());

        assert!(sessions.is_empty());
        assert!(result.service.client_slugs().is_empty());
        assert_eq!(result.service.stop_all(), 0);
        assert!(result.event_router.route_uuids().is_empty());
    }
}

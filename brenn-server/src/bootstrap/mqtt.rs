//! Build and start the MQTT service.

use std::sync::Arc;

use brenn_lib::access::AppPolicy;
use brenn_lib::config::AppConfig;
use brenn_lib::config::BrennConfig;
use brenn_lib::messaging::config::ResolvedWasmConsumer;
use brenn_lib::mqtt::MqttService;
use brenn_lib::mqtt::config::{MqttClientConfig, ResolvedMqttIngressChannel};
use brenn_lib::mqtt::{MqttClientHandle, spawn_client_supervisor, union_subscriptions};
use indexmap::{IndexMap, IndexSet};
use tracing::info;

use crate::mqtt_router::{IngressRoute, MqttEventRouterImpl};

/// Outcome of starting the MQTT service.
pub(crate) struct MqttResult {
    pub(crate) service: Option<Arc<MqttService>>,
    pub(crate) event_router: Option<Arc<MqttEventRouterImpl>>,
    /// Stop-signal senders — one per client supervisor. Passed to
    /// `ShutdownHandle::mqtt_stop_txs`; each sender is fired on SIGTERM/SIGINT
    /// to send MQTT DISCONNECT before process exit.
    pub(crate) stop_txs: Vec<tokio::sync::watch::Sender<bool>>,
}

/// The set of clients that get a session: a client is **referenced** iff it is
/// named by at least one resolved ingress channel, one `mqtt_publish` ACL matcher
/// (app or WASM consumer), or one `mqtt_subscribe` ACL matcher (app only — WASM
/// policies carry no subscribe matchers). Deduplicated, first-seen (config) order
/// for deterministic supervisor spawn order.
///
/// "ACL-authorized ⇒ session exists" holds in both directions: every client any
/// matcher can authorize a publish or a dynamic subscribe against has a running
/// session.
fn referenced_clients<'a>(
    ingress_channels: &'a [ResolvedMqttIngressChannel],
    app_policies: impl Iterator<Item = &'a AppPolicy>,
    wasm_policies: impl Iterator<Item = &'a AppPolicy>,
) -> IndexSet<&'a str> {
    let mut referenced: IndexSet<&str> = IndexSet::new();
    for ch in ingress_channels {
        referenced.insert(ch.client_slug.as_str());
    }
    for policy in app_policies {
        for m in &policy.acls.mqtt_publish {
            referenced.insert(m.client.as_str());
        }
        for m in &policy.acls.mqtt_subscribe {
            referenced.insert(m.client.as_str());
        }
    }
    for policy in wasm_policies {
        for m in &policy.acls.mqtt_publish {
            referenced.insert(m.client.as_str());
        }
    }
    referenced
}

/// Build the MQTT service and spawn one unified supervisor per **referenced**
/// `[[mqtt_client]]` (see [`referenced_clients`]). Each session carries both the
/// publish path and the ingress delivery + reconnect re-assert path.
///
/// Returns `None` values when no `[[mqtt_client]]` is declared OR no client is
/// referenced by any ingress channel or ACL matcher.
///
/// `AppState` injection (`set_state` + `set_router`) must happen after
/// `AppState` construction — same deferred-state pattern as `WakeRouterImpl`.
pub(crate) async fn start_mqtt(
    config: &BrennConfig,
    apps: &Arc<IndexMap<String, AppConfig>>,
    wasm_consumers: &[ResolvedWasmConsumer],
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
    clients: &IndexMap<String, MqttClientConfig>,
) -> MqttResult {
    let referenced = referenced_clients(
        mqtt_ingress_channels,
        apps.values().map(|a| &a.policy),
        wasm_consumers.iter().map(|c| &c.policy),
    );

    if config.mqtt_clients.is_empty() || referenced.is_empty() {
        return MqttResult {
            service: None,
            event_router: None,
            stop_txs: vec![],
        };
    }

    let svc = MqttService::new();
    let router = Arc::new(MqttEventRouterImpl::new());
    let router_trait: Arc<dyn brenn_lib::mqtt::MqttEventRouter> = router.clone();
    let mut stop_txs: Vec<tokio::sync::watch::Sender<bool>> = Vec::new();

    // One unified supervisor per referenced client. A client's subscription set is
    // the deduplicated union of its ingress-channel filters (empty for an
    // egress-only client — a connected publisher with zero subscriptions).
    for client_slug in &referenced {
        let broker_cfg = clients.get(*client_slug).unwrap_or_else(|| {
            // Every ACL matcher's client is boot-validated against the declared
            // client set (LLM `validate_mqtt_client` + the WASM matcher check), and
            // ingress channels likewise, so a referenced client absent from the
            // resolved map is a host invariant break.
            panic!(
                "mqtt: referenced client {client_slug:?} is not in the resolved client map (bug)"
            )
        });
        let subscriptions = union_subscriptions(client_slug, mqtt_ingress_channels);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let config = Arc::new(broker_cfg.clone());
        let handle = MqttClientHandle::new(config, subscriptions, stop_tx.clone());
        stop_txs.push(stop_tx);

        // Register the handle on the service before spawning: dynamic `mqtt:`
        // subscribe needs the live `AsyncClient` via `get_client`, egress publishes
        // resolve the handle by client slug, and the listing health enrichment reads
        // per-client session state. The supervisor consumes the handle, so register
        // the clone first.
        svc.add_client(handle.clone()).await;

        spawn_client_supervisor(handle, router_trait.clone(), stop_rx);
    }

    MqttResult {
        service: Some(svc),
        event_router: Some(router),
        stop_txs,
    }
}

/// Inject AppState into the MQTT event router and service. Returns the
/// `stop_txs` senders so the caller can pass them to the shutdown handler
/// (which sends `true` on each, causing every supervisor to send MQTT
/// DISCONNECT before process exit).
///
/// # Panics
///
/// Panics if called more than once (the `OnceCell` inside `MqttEventRouterImpl`
/// does not allow re-setting).
pub(crate) async fn wire_mqtt_state(
    service: &Arc<MqttService>,
    router: &Arc<MqttEventRouterImpl>,
    state: crate::state::AppState,
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
    stop_txs: Vec<tokio::sync::watch::Sender<bool>>,
) -> Vec<tokio::sync::watch::Sender<bool>> {
    // Build the router's routing table from the distinct ingress channels. One
    // `IngressRoute` per channel: `(client_slug, topic_filter)` is the match key,
    // `mqtt:<client>:<topic>` (uuid carried on the resolved channel) is the
    // destination, and `urgency` is the client's `[[mqtt_client]].urgency`. The
    // router fans inbound deliveries out to every matching route.
    let routes: Vec<IngressRoute> = mqtt_ingress_channels
        .iter()
        .map(|c| IngressRoute {
            client_slug: c.client_slug.clone(),
            topic_filter: c.topic.clone(),
            channel_address: c.channel_address.clone(),
            channel_uuid: c.channel_uuid,
            urgency: c.urgency,
        })
        .collect();
    router.set_state(state, routes);
    service
        .set_router(router.clone() as Arc<dyn brenn_lib::mqtt::MqttEventRouter>)
        .await;
    info!("MQTT service started; supervisors running");
    stop_txs
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::access::acl::{MqttClientMatcher, MqttSubMatcher};
    use brenn_lib::messaging::{Urgency, mqtt_channel_uuid_from_address};
    use brenn_lib::mqtt::config::{MqttClientConfigRaw, parsed_address_canonical};

    fn test_client(slug: &str) -> MqttClientConfig {
        crate::test_support::mqtt::test_client_config(slug)
    }

    fn test_raw_client(slug: &str) -> MqttClientConfigRaw {
        toml::from_str(&format!("slug = \"{slug}\"\nurl = \"mqtts://127.0.0.1:1\""))
            .expect("minimal raw client config parses")
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

    fn policy_with_publish(client: &str) -> AppPolicy {
        let mut p = AppPolicy::default();
        p.acls.mqtt_publish.push(MqttClientMatcher {
            client: client.to_string(),
        });
        p
    }

    fn policy_with_subscribe(client: &str) -> AppPolicy {
        let mut p = AppPolicy::default();
        p.acls.mqtt_subscribe.push(MqttSubMatcher {
            client: client.to_string(),
            topic_filter: "sensors/#".to_string(),
        });
        p
    }

    // --- referenced_clients derivation (spawn-set) ---

    #[test]
    fn referenced_ingress_only_client() {
        let ch = vec![test_ingress_channel("ing", "sensors/#")];
        let refs = referenced_clients(&ch, std::iter::empty(), std::iter::empty());
        assert!(refs.contains("ing"));
        assert_eq!(refs.len(), 1);
    }

    #[test]
    fn referenced_egress_only_client_via_publish_matcher() {
        let pol = policy_with_publish("egress");
        let refs = referenced_clients(&[], std::iter::once(&pol), std::iter::empty());
        assert!(refs.contains("egress"));
    }

    #[test]
    fn referenced_subscribe_matcher_only_client() {
        let pol = policy_with_subscribe("subonly");
        let refs = referenced_clients(&[], std::iter::once(&pol), std::iter::empty());
        assert!(refs.contains("subonly"));
    }

    #[test]
    fn referenced_wasm_publish_matcher_client() {
        let pol = policy_with_publish("wasmcl");
        let refs = referenced_clients(&[], std::iter::empty(), std::iter::once(&pol));
        assert!(refs.contains("wasmcl"));
    }

    #[test]
    fn referenced_none_when_unreferenced() {
        let refs = referenced_clients(&[], std::iter::empty(), std::iter::empty());
        assert!(refs.is_empty());
    }

    // --- start_mqtt activation ---

    #[tokio::test]
    async fn activates_with_ingress_only_no_connectors() {
        let config = BrennConfig {
            mqtt_clients: vec![test_raw_client("cl")],
            ..Default::default()
        };
        let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IndexMap::new());
        let mut clients: IndexMap<String, MqttClientConfig> = IndexMap::new();
        clients.insert("cl".to_string(), test_client("cl"));

        let result = start_mqtt(
            &config,
            &apps,
            &[],
            &[test_ingress_channel("cl", "sensors/#")],
            &clients,
        )
        .await;

        assert!(result.service.is_some());
        assert!(result.event_router.is_some());
        assert_eq!(result.stop_txs.len(), 1);
        assert!(result.stop_txs[0].send(true).is_ok());
    }

    #[tokio::test]
    async fn inactive_when_client_but_nothing_references_it() {
        let config = BrennConfig {
            mqtt_clients: vec![test_raw_client("cl")],
            ..Default::default()
        };
        let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IndexMap::new());
        let clients: IndexMap<String, MqttClientConfig> = IndexMap::new();

        let result = start_mqtt(&config, &apps, &[], &[], &clients).await;

        assert!(result.service.is_none());
        assert!(result.event_router.is_none());
        assert!(result.stop_txs.is_empty());
    }

    #[tokio::test]
    async fn inactive_when_ingress_but_no_clients() {
        let config = BrennConfig::default();
        let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IndexMap::new());
        let clients: IndexMap<String, MqttClientConfig> = IndexMap::new();

        let result = start_mqtt(
            &config,
            &apps,
            &[],
            &[test_ingress_channel("cl", "sensors/#")],
            &clients,
        )
        .await;

        assert!(result.service.is_none());
        assert!(result.event_router.is_none());
        assert!(result.stop_txs.is_empty());
    }
}

//! Shared fixtures for the boot-lowering test modules, and for the composition
//! root above, which boots a real messaging layer through `boot_messaging_with`.

use super::*;
use brenn_lib::config::AppConfig;
use brenn_lib::messaging::ComponentGrant;
#[cfg(test)]
use brenn_lib::messaging::WakeMin;
use brenn_lib::messaging::config::{
    Depth, SurfaceComponentRaw, SurfaceConfigRaw, WasmConsumerConfigRaw,
};
#[cfg(test)]
use brenn_lib::messaging::config::{
    ResolvedChannel, Sink, SurfaceSubscriptionRaw, WasmConsumerOutputRaw,
    WasmConsumerSubscriptionRaw,
};
pub use brenn_server::test_support::app_config::minimal_app_config;

/// Builds `SystemChannelTuning` from `config`'s `[[channel]]` blocks.
pub(crate) fn tuning_for(
    config: &brenn_lib::config::BrennConfig,
) -> brenn_lib::messaging::config::SystemChannelTuning {
    brenn_lib::messaging::config::build_system_channel_tuning(&config.channels, &config.messaging)
}

/// Boot `build_messaging` over `config` with an inert periphery: no webhook
/// endpoints, no active bridges, no dynamic subscriptions, and an empty tool
/// registry (so no `brenn:tools/*` channels or `system:tool-executor` policy are
/// derived).
pub async fn boot_messaging_with(
    config: &brenn_lib::config::BrennConfig,
    db: brenn_db::Db,
    apps: &Arc<IndexMap<String, AppConfig>>,
    alert_dispatcher: AlertDispatcher,
    origin: &str,
) -> MessagingResult {
    let webhooks: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IndexMap::new();
    build_messaging(
        config,
        db,
        apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from(origin)),
        &webhooks,
        &[],
        &tuning_for(config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &Arc::new(brenn_tool_registry::ToolRegistry::new(vec![])),
    )
    .await
}

/// A `WasmConsumerIoPortRaw` on `port` (`channel` absent ⇒ anonymous auto
/// channel) at the given depths, every other knob unset.
pub fn io_port_raw(
    port: &str,
    channel: Option<&str>,
    push_depth: Depth,
    retain_depth: Depth,
) -> brenn_lib::messaging::config::WasmConsumerIoPortRaw {
    brenn_lib::messaging::config::WasmConsumerIoPortRaw {
        port: port.to_string(),
        channel: channel.map(str::to_string),
        push_depth: Some(push_depth),
        retain_depth: Some(retain_depth),
        noise: None,
        amplification: None,
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

/// A `SurfaceIoPortRaw` on `instance`/`port` (`channel` absent ⇒ anonymous auto
/// channel) at the given depths, every other knob unset.
pub fn surface_io_port_raw(
    instance: &str,
    port: &str,
    channel: Option<&str>,
    push_depth: Depth,
    retain_depth: Depth,
) -> brenn_lib::messaging::config::SurfaceIoPortRaw {
    brenn_lib::messaging::config::SurfaceIoPortRaw {
        instance: instance.to_string(),
        port: port.to_string(),
        channel: channel.map(str::to_string),
        push_depth: Some(push_depth),
        retain_depth: Some(retain_depth),
        noise: None,
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

#[cfg(test)]
/// A resolved MQTT ingress subscription for `mqtt:<client>:<topic>` with
/// push-enabled (Unbounded) depths.
pub(super) fn resolved_ingress_sub(
    address: &str,
) -> brenn_lib::mqtt::config::ResolvedMqttIngressSubscription {
    let parsed = brenn_lib::mqtt::parse_mqtt_address(address).expect("valid mqtt address");
    brenn_lib::mqtt::config::ResolvedMqttIngressSubscription {
        channel_address: address.to_string(),
        channel_uuid: brenn_lib::messaging::mqtt_channel_uuid_from_address(address),
        client_slug: parsed.client,
        topic: parsed.topic,
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: brenn_lib::messaging::WakeMin::Normal,
    }
}

/// A minimal `[[wasm_consumer]]` with empty grants, no store, and no
/// subscriptions/outputs — trips none of `resolve_wasm_consumers`'
/// validation panics. The single base `WasmConsumerConfigRaw` literal in
/// this module; other minimal-consumer fixtures build on it via struct update.
pub fn minimal_wasm_consumer() -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw {
        slug: "probe".to_string(),
        component_path: std::path::PathBuf::from("/nonexistent/probe.wasm"),
        grants: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![],
        outputs: vec![],
        io_ports: vec![],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }
}

#[cfg(test)]
/// A single `brenn:` channel entry with the given resolved knobs. Its
/// `standing_retain_depth` mirrors `retain_depth`; sink is `Drop` and wake_min
/// `Normal`.
pub(super) fn brenn_entry_with(
    addr: &str,
    push_depth: Depth,
    retain_depth: Depth,
    noise: NoiseLevel,
) -> ChannelEntry {
    ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        address: addr.to_string(),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth,
            retain_depth,
            standing_retain_depth: retain_depth,
            noise,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

#[cfg(test)]
/// A single `brenn:` channel entry with Unbounded depths and Silent noise.
pub(super) fn brenn_entry(addr: &str) -> ChannelEntry {
    brenn_entry_with(addr, Depth::Unbounded, Depth::Unbounded, NoiseLevel::Silent)
}

#[cfg(test)]
/// A `MessagingDirectory` holding the given entries.
pub(super) fn dir_of(entries: Vec<ChannelEntry>) -> MessagingDirectory {
    MessagingDirectory::with_entries(entries)
}

#[cfg(test)]
/// A one-channel `brenn:` directory (Unbounded/Silent defaults) plus its
/// address, for tests that bind a single channel.
pub(super) fn make_brenn_dir(chan_addr: &str) -> (MessagingDirectory, String) {
    (dir_of(vec![brenn_entry(chan_addr)]), chan_addr.to_string())
}

#[cfg(test)]
/// A `WasmConsumerSubscriptionRaw` on `channel`/`port` with every optional knob
/// unset; callers set the knob(s) under test via struct-update.
pub(super) fn sub_raw(channel: &str, port: &str) -> WasmConsumerSubscriptionRaw {
    WasmConsumerSubscriptionRaw {
        channel: Some(channel.to_string()),
        port: port.to_string(),
        push_depth: None,
        retain_depth: None,
        noise: None,
        wake_min: None,
        amplification: None,
    }
}

#[cfg(test)]
/// A `WasmConsumerOutputRaw` on `port` → `channel` with every optional knob
/// unset; callers set the knob(s) under test via struct-update.
pub(super) fn out_raw(port: &str, channel: &str) -> WasmConsumerOutputRaw {
    WasmConsumerOutputRaw {
        port: port.to_string(),
        channel: Some(channel.to_string()),
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

#[cfg(test)]
/// A minimal single-consumer `WasmConsumerConfigRaw` carrying one subscription
/// to `chan_addr` (port `in`), with empty grants and no store. Callers needing
/// a store or specific grants build on `minimal_wasm_consumer()` directly.
pub(super) fn minimal_wasm_consumer_raw(
    slug: &str,
    component_path: &str,
    chan_addr: &str,
) -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw {
        slug: slug.to_string(),
        component_path: component_path.into(),
        subscriptions: vec![sub_raw(chan_addr, "in")],
        ..minimal_wasm_consumer()
    }
}

#[cfg(test)]
/// A `SurfaceSubscriptionRaw` on `channel`/`component`/`port` with every
/// optional knob unset; callers set the knob(s) under test via struct-update.
///
/// `push_depth` stays unset, which resolves off the channel's rung. A `local:`
/// binding has no rung and must state one — [`local_sub_raw`] is that shape.
pub(super) fn surface_sub_raw(
    channel: &str,
    component: &str,
    port: &str,
) -> SurfaceSubscriptionRaw {
    SurfaceSubscriptionRaw {
        channel: Some(channel.to_string()),
        instance: component.to_string(),
        port: port.to_string(),
        push_depth: None,
        retain_depth: None,
        noise: None,
        wake_min: None,
    }
}

#[cfg(test)]
/// A `local:` surface binding: [`surface_sub_raw`] plus the `push_depth` a
/// page-local binding has to state for itself, since there is no `[[channel]]`
/// block behind a `local:` address to carry one.
pub(super) fn local_sub_raw(channel: &str, component: &str, port: &str) -> SurfaceSubscriptionRaw {
    SurfaceSubscriptionRaw {
        push_depth: Some(Depth::Bounded(8)),
        ..surface_sub_raw(channel, component, port)
    }
}

/// A minimal `SurfaceConfigRaw` (`deskbar` slug, one `protobar` component plus
/// the required `chrome` singleton, no grants/ACLs/subscriptions/outputs, no
/// budgets). The single base surface literal; callers add the grants, ACLs, and
/// bindings under test via struct update.
pub fn minimal_surface_raw() -> SurfaceConfigRaw {
    SurfaceConfigRaw {
        slug: "deskbar".to_string(),
        grants: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        ephemeral_publish_acl: vec![],
        components: vec![
            SurfaceComponentRaw {
                kind: "protobar".to_string(),
                instance: None,
                abi: "dom".to_string(),
                send_burst: None,
                send_refill_secs: None,
                parked_batch_depth: None,
                config: None,
                grants: vec![ComponentGrant::Ports],
                chrome: false,
            },
            SurfaceComponentRaw {
                kind: "chrome".to_string(),
                instance: None,
                abi: "dom".to_string(),
                send_burst: None,
                send_refill_secs: None,
                parked_batch_depth: None,
                config: None,
                grants: vec![ComponentGrant::Ports],
                chrome: true,
            },
        ],
        subscriptions: vec![],
        outputs: vec![],
        io_ports: vec![],
        skin: None,
        allowed_users: vec![],
        publish_burst: None,
        publish_per_sec: None,
    }
}

#[cfg(test)]
/// An empty auto wiring, for resolver calls whose every binding is address-bound.
pub(super) fn no_auto() -> super::auto::AutoWiring {
    super::auto::AutoWiring::default()
}

#[cfg(test)]
/// Call `resolve_wasm_consumers` with the global default size limit, no declared
/// MQTT clients (callers exercise no `mqtt_publish` ACL matchers), and an empty
/// auto wiring — every binding carries its own channel address.
pub(super) fn resolve(
    raw: &[WasmConsumerConfigRaw],
    dir: &MessagingDirectory,
) -> Vec<ResolvedWasmConsumer> {
    resolve_with_auto(raw, dir, &super::auto::AutoWiring::default())
}

#[cfg(test)]
/// Call `resolve_wasm_consumers` against a lowered auto wiring, for the
/// connection-bound port cases.
pub(super) fn resolve_with_auto(
    raw: &[WasmConsumerConfigRaw],
    dir: &MessagingDirectory,
    auto: &super::auto::AutoWiring,
) -> Vec<ResolvedWasmConsumer> {
    resolve_wasm_consumers(raw, dir, "64MiB", &IndexMap::new(), auto)
}

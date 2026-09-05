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

/// Boot `build_messaging` over `config` with an inert periphery: no active
/// bridges, no dynamic subscriptions, and an empty tool registry (so no
/// `brenn:tools/*` channels or `system:tool-executor` policy are derived).
pub async fn boot_messaging_with(
    config: &brenn_lib::config::BrennConfig,
    db: brenn_db::Db,
    apps: &Arc<IndexMap<String, AppConfig>>,
    alert_dispatcher: AlertDispatcher,
    origin: &str,
) -> MessagingResult {
    boot_messaging_with_tools(
        config,
        db,
        apps,
        alert_dispatcher,
        origin,
        &Arc::new(brenn_tool_registry::ToolRegistry::new(vec![])),
    )
    .await
}

/// [`boot_messaging_with`] with a caller-supplied tool registry: the async tool
/// substrate is the one part of the periphery a boot test may need real, and
/// everything else — bridges, the origin, the client identities the document
/// determines, the replay store paths — is the derivation's to compute or inert.
///
/// Every boot-lowering test goes through this, so a `build_messaging` parameter
/// that moves sides is one edit here rather than one per test.
pub async fn boot_messaging_with_tools(
    config: &brenn_lib::config::BrennConfig,
    db: brenn_db::Db,
    apps: &Arc<IndexMap<String, AppConfig>>,
    alert_dispatcher: AlertDispatcher,
    origin: &str,
    tool_registry: &Arc<brenn_tool_registry::ToolRegistry>,
) -> MessagingResult {
    boot_messaging_carrying(config, db, apps, alert_dispatcher, origin, tool_registry)
        .await
        .0
}

/// [`boot_messaging_with_tools`] keeping the plan-carried half: the ingress set
/// the broker SUBSCRIBE union and the router's route table are built from, and
/// the surface-error advisory. For the tests whose subject is what boot hands
/// onward rather than what it installed.
pub async fn boot_messaging_carrying(
    config: &brenn_lib::config::BrennConfig,
    db: brenn_db::Db,
    apps: &Arc<IndexMap<String, AppConfig>>,
    alert_dispatcher: AlertDispatcher,
    origin: &str,
    tool_registry: &Arc<brenn_tool_registry::ToolRegistry>,
) -> (MessagingResult, PlanCarried) {
    build_messaging(
        config,
        db,
        apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from(origin)),
        &brenn_lib::mqtt::config::resolve_client_identities(&config.mqtt_clients),
        tool_registry,
        &[],
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
        package: "probe".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
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

/// The one `[[channel]]` block every document that activates messaging owes the
/// self-description derivation: `brenn:surface.index`, retained so a
/// non-subscriber pull sees the latest document.
///
/// A document declaring `[[surface]]` blocks owes the per-surface and per-kind
/// families beside it; this is the floor, which is what a document with no
/// surface at all still has to declare.
pub fn surface_index_channel() -> brenn_lib::messaging::config::ChannelConfigRaw {
    brenn_lib::messaging::config::ChannelConfigRaw {
        // Fixed rather than random: two boots of one fixture's document must
        // name the same row, which is what the across-restart tests rest on.
        uuid: Some("5f1d1a9e-0000-4000-8000-000000000001".to_string()),
        ..durable_channel("brenn:surface.index", Depth::Bounded(1))
    }
}

/// A durable `[[channel]]` block at `address`, retaining `standing` for a
/// subscriber that was not there, at depth 1 everywhere else and every other
/// knob defaulted.
///
/// The single base `ChannelConfigRaw` literal for the test modules of this
/// crate and of the composition root above it: a fixture wanting a fixed uuid,
/// a description or a different depth says so by struct update, so a new field
/// on the block type is one edit rather than one per test module.
pub fn durable_channel(
    address: &str,
    standing: Depth,
) -> brenn_lib::messaging::config::ChannelConfigRaw {
    brenn_lib::messaging::config::ChannelConfigRaw {
        send_rate: None,
        uuid: Some(uuid::Uuid::new_v4().to_string()),
        address: Some(address.to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(1)),
        retain_depth: Some(Depth::Bounded(1)),
        standing_retain_depth: Some(standing),
        noise: None,
        sink: None,
        wake_min: None,
    }
}

/// Every `[[channel]]` block the self-description derivation requires of a
/// document declaring one `[[surface]]` named `slug` whose components are
/// `kinds`: the boot-published index and per-surface help, each kind's help and
/// schema, the surface's runtime geometry/status pair, and its ephemeral
/// bindings channel — each at the depths the derivation's validation demands.
///
/// The durable blocks take a uuid derived from the address, so a fixture's
/// document names the same rows on a second boot.
pub fn surface_description_channels(
    slug: &str,
    kinds: &[&str],
) -> Vec<brenn_lib::messaging::config::ChannelConfigRaw> {
    let retained = |address: String| brenn_lib::messaging::config::ChannelConfigRaw {
        uuid: Some(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, address.as_bytes()).to_string()),
        ..durable_channel(&address, Depth::Bounded(1))
    };
    let mut channels = vec![
        surface_index_channel(),
        retained(format!("brenn:surface.surface.{slug}.help")),
        retained(format!("brenn:surface.surface.{slug}.geometry")),
        retained(format!("brenn:surface.surface.{slug}.status")),
    ];
    for kind in kinds {
        channels.push(retained(format!("brenn:surface.kind.{kind}.help")));
        channels.push(retained(format!("brenn:surface.kind.{kind}.schema")));
    }
    // The bindings channel is ephemeral: its retain depth *is* its standing
    // buffer, which is what a surface attaching later replays from.
    channels.push(brenn_lib::messaging::config::ChannelConfigRaw {
        uuid: None,
        standing_retain_depth: None,
        ..durable_channel(
            &format!("ephemeral:surface.surface.{slug}.bindings"),
            Depth::Bounded(1),
        )
    });
    channels
}

/// A `[[webhook_endpoint]]` block stating only its slug: bearer-token scheme
/// with no tokens, every other knob at its default. Enough for the boot
/// derivation, which reads a block's slug, description and mount and nothing
/// else; the secret material is `resolve_webhook_endpoints`' business.
pub fn webhook_endpoint_raw(slug: &str) -> brenn_lib::webhook::WebhookEndpointConfigRaw {
    brenn_lib::webhook::WebhookEndpointConfigRaw {
        slug: slug.to_string(),
        mount: None,
        description: None,
        transport_ceiling_bytes: 1024 * 1024,
        content_type: "application/json".to_string(),
        signature: brenn_lib::webhook::config::WebhookSignatureConfigRaw::BearerToken {
            header: "authorization".to_string(),
            token_id_header: None,
        },
        keys: vec![],
        tokens: vec![],
        replay_protection: None,
        urgency: None,
    }
}

/// An `[[mqtt_client]]` block on a stock broker URL, every other knob at its
/// default — the client an `mqtt:<slug>:<topic>` address needs declared before
/// it resolves.
pub fn minimal_mqtt_client(slug: &str) -> brenn_lib::mqtt::config::MqttClientConfigRaw {
    brenn_lib::mqtt::config::MqttClientConfigRaw::minimal(slug, "mqtts://broker.invalid:8883")
}

/// An `[[app]]` block whose only messaging content is one
/// `[[app.mqtt_subscription]]` on `channel` — the shape that mints an
/// `mqtt:<client>:<topic>` ingress channel from the app side.
pub fn app_raw_with_mqtt_subscription(
    slug: &str,
    channel: &str,
) -> brenn_lib::config::AppConfigRaw {
    brenn_lib::config::AppConfigRaw {
        slug: slug.to_string(),
        mqtt_subscriptions: vec![brenn_lib::mqtt::config::AppMqttIngressSubscriptionRaw {
            channel: channel.to_string(),
            push_depth: Some(Depth::Bounded(4)),
            retain_depth: Some(Depth::Bounded(4)),
            noise: None,
            wake_min: None,
        }],
        ..Default::default()
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
    package: &str,
    chan_addr: &str,
) -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw {
        slug: slug.to_string(),
        package: package.to_string(),
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
                grants: vec![ComponentGrant::Ports],
                ..SurfaceComponentRaw::minimal("protobar")
            },
            SurfaceComponentRaw {
                grants: vec![
                    ComponentGrant::Ports,
                    ComponentGrant::Dom,
                    ComponentGrant::PageDom,
                ],
                chrome: true,
                ..SurfaceComponentRaw::minimal("chrome")
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
/// `resolve_surfaces` against a lowered auto wiring, with each fixture's
/// components taken to declare exactly the out ports their bindings imply —
/// what a hand-written fixture means when it says nothing about the
/// vocabulary. One that says something is left alone.
pub(super) fn resolve_surfaces_with_auto(
    raw: &[brenn_lib::messaging::config::SurfaceConfigRaw],
    dir: &MessagingDirectory,
    globals: &brenn_lib::messaging::config::MessagingGlobalConfig,
    auto: &super::auto::AutoWiring,
) -> Vec<super::ResolvedSurface> {
    let raw: Vec<brenn_lib::messaging::config::SurfaceConfigRaw> = raw
        .iter()
        .cloned()
        .map(brenn_lib::messaging::config::SurfaceConfigRaw::implying_component_vocabularies)
        .collect();
    super::resolve_surfaces(&raw, dir, globals, auto)
}

#[cfg(test)]
/// Call `resolve_wasm_consumers` against a lowered auto wiring, for the
/// connection-bound port cases.
pub(super) fn resolve_with_auto(
    raw: &[WasmConsumerConfigRaw],
    dir: &MessagingDirectory,
    auto: &super::auto::AutoWiring,
) -> Vec<ResolvedWasmConsumer> {
    // Each fixture's class is taken to declare exactly the out ports its
    // instance binds, which is what a hand-written fixture means when it says
    // nothing about the vocabulary. One that says something is left alone.
    let raw: Vec<WasmConsumerConfigRaw> = raw
        .iter()
        .cloned()
        .map(WasmConsumerConfigRaw::implying_its_vocabulary)
        .collect();
    resolve_wasm_consumers(&raw, dir, "64MiB", &IndexMap::new(), auto)
}

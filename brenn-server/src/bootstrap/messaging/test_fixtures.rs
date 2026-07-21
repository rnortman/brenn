//! Shared `#[cfg(test)]` fixtures for the messaging-bootstrap test modules.

use super::*;
use brenn_lib::config::{
    AppConfig, FrontmatterRenderConfig, PathMapper, PostPullHooksConfig, StartHooksConfig,
    StartupHooksConfig,
};
use brenn_lib::messaging::WakeMin;
use brenn_lib::messaging::config::{
    ResolvedChannel, Sink, SurfaceComponentRaw, SurfaceConfigRaw, SurfaceSubscriptionRaw,
    WasmConsumerConfigRaw, WasmConsumerOutputRaw, WasmConsumerSubscriptionRaw,
};
use brenn_lib::webhook::ResolvedWebhookSubscription;

/// Build a resolved MQTT ingress subscription for `mqtt:<client>:<topic>`
/// with push-enabled (Unbounded) depths, mirroring the default resolution.
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

/// Construct a minimal `AppConfig` for bootstrap tests.
pub(super) fn minimal_app_config(
    slug: &str,
    messaging: Option<ResolvedMessagingConfig>,
    webhook_subscriptions: Vec<ResolvedWebhookSubscription>,
) -> AppConfig {
    AppConfig {
        slug: slug.to_string(),
        name: slug.to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: std::path::PathBuf::from("/tmp"),
        model: String::new(),
        single_instance: false,
        singleton: true,
        persistent: false,
        idle_timeout: None,
        compaction: None,
        idle_hook_secs: 0,
        allowed_users: vec!["alice".to_string()],
        disabled_tools: vec![],
        mcp_servers: Default::default(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: false,
        path_mapper: PathMapper::Identity,
        container_spawn: None,
        start_hooks: StartHooksConfig::default(),
        post_pull_hooks: PostPullHooksConfig::default(),
        startup_hooks: StartupHooksConfig::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: Default::default(),
        mounts: vec![],
        history_replay_limit: 100,
        frontmatter: FrontmatterRenderConfig::default(),
        state_dir: std::path::PathBuf::from("/tmp"),
        messaging,
        messaging_default_send_budget: 100,
        policy: brenn_lib::access::AppPolicy::default(),
        pwa_push: None,
        webhook_subscriptions,
        mqtt_subscriptions: vec![],
    }
}

/// A minimal `[[wasm_consumer]]` with empty grants, no store, and no
/// subscriptions/outputs — trips none of `resolve_wasm_consumers`'
/// validation panics. The single base `WasmConsumerConfigRaw` literal in
/// this module; other minimal-consumer fixtures build on it via struct update.
pub(super) fn minimal_wasm_consumer() -> WasmConsumerConfigRaw {
    WasmConsumerConfigRaw {
        slug: "probe".to_string(),
        component_path: std::path::PathBuf::from("/nonexistent/probe.wasm"),
        grants: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![],
        outputs: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
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

/// A single `brenn:` channel entry with Unbounded depths and Silent noise.
pub(super) fn brenn_entry(addr: &str) -> ChannelEntry {
    brenn_entry_with(addr, Depth::Unbounded, Depth::Unbounded, NoiseLevel::Silent)
}

/// A `MessagingDirectory` holding the given entries.
pub(super) fn dir_of(entries: Vec<ChannelEntry>) -> MessagingDirectory {
    MessagingDirectory::with_entries(entries)
}

/// A one-channel `brenn:` directory (Unbounded/Silent defaults) plus its
/// address, for tests that bind a single channel.
pub(super) fn make_brenn_dir(chan_addr: &str) -> (MessagingDirectory, String) {
    (dir_of(vec![brenn_entry(chan_addr)]), chan_addr.to_string())
}

/// A `WasmConsumerSubscriptionRaw` on `channel`/`port` with every optional knob
/// unset; callers set the knob(s) under test via struct-update.
pub(super) fn sub_raw(channel: &str, port: &str) -> WasmConsumerSubscriptionRaw {
    WasmConsumerSubscriptionRaw {
        channel: channel.to_string(),
        port: port.to_string(),
        push_depth: None,
        retain_depth: None,
        noise: None,
        wake_min: None,
        amplification: None,
    }
}

/// A `WasmConsumerOutputRaw` on `port` → `channel` with every optional knob
/// unset; callers set the knob(s) under test via struct-update.
pub(super) fn out_raw(port: &str, channel: &str) -> WasmConsumerOutputRaw {
    WasmConsumerOutputRaw {
        port: port.to_string(),
        channel: channel.to_string(),
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    }
}

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

/// A `SurfaceSubscriptionRaw` on `channel`/`component`/`port` with every
/// optional knob unset; callers set the knob(s) under test via struct-update.
pub(super) fn surface_sub_raw(
    channel: &str,
    component: &str,
    port: &str,
) -> SurfaceSubscriptionRaw {
    SurfaceSubscriptionRaw {
        channel: channel.to_string(),
        instance: component.to_string(),
        port: port.to_string(),
        push_depth: None,
        retain_depth: None,
        noise: None,
        wake_min: None,
    }
}

/// A minimal `SurfaceConfigRaw` (`deskbar` slug, one `protobar` component plus
/// the required `chrome` singleton, no grants/ACLs/subscriptions/outputs, no
/// budgets). The single base surface literal; callers add the grants, ACLs, and
/// bindings under test via struct update.
pub(super) fn minimal_surface_raw() -> SurfaceConfigRaw {
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
                chrome: true,
            },
        ],
        subscriptions: vec![],
        outputs: vec![],
        skin: None,
        allowed_users: vec![],
        publish_burst: None,
        publish_per_sec: None,
    }
}

/// Call `resolve_wasm_consumers` with the global default size limit and no
/// declared MQTT clients (callers exercise no `mqtt_publish` ACL matchers).
pub(super) fn resolve(
    raw: &[WasmConsumerConfigRaw],
    dir: &MessagingDirectory,
) -> Vec<ResolvedWasmConsumer> {
    resolve_wasm_consumers(raw, dir, "64MiB", &IndexMap::new())
}

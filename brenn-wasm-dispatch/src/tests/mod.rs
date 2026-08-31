//! Test subtree for the dispatch task.
//!
//! Integration tests: fan-out, durability, and batching.
//!
//! These tests verify the full dispatch + Messenger + ProcessorComponent
//! integration path: window assembly, guest invocation, per-batch disposition,
//! crash-recovery (startup sweep), always-trap quarantine, batching coalescing,
//! retained-context prefix, push-overflow dropped counter, and the webhook
//! (publish_transport_ingress) fan-out path.
//!
//! Tests use an in-memory DB and the real WASM fixtures (processor-demo for
//! Ok/Err paths; the fuel-exhaustion path is covered in
//! brenn-wasm/tests/consume_engine.rs). The always-trap path uses the
//! processor-demo sentinel ("__trap__") body.
//!
//! Shared scaffolding lives here: block-level imports, `NoopAlerter`, fixture
//! consts (`DEMO_WASM`, `MULTIPORT_WASM`), and the builder helpers consumed by
//! the family submodules below. The module is `pub` behind `testutils` because
//! the suites in `brenn-server` and `brenn-bootstrap` that drive this dispatcher
//! through server wiring build on the same scaffolding.

pub use super::*;

pub use std::sync::Arc;

// The alias encodes which migration slice the test DB uses; suites at higher
// crate levels have their own alias over a wider slice.
#[cfg(test)]
use brenn_lib::messaging::WebhookEnvelope;
pub use brenn_lib::messaging::config::{
    ActivationPacing, Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel,
    ResolvedSubscription, Sink, WasmInputPort,
};
pub use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, WEBHOOK_ADDRESS_PREFIX, WakeMin, webhook_channel_uuid_from_slug,
};
pub use brenn_messaging::WakeRouter;
pub use brenn_messaging::query::NoopWakeRouter;
pub use brenn_messaging::testutils;
pub use brenn_messaging_store::db::init_db_memory as init_db_memory_lib_slice;
pub use brenn_messaging_store::db::upsert_channels;
#[cfg(test)]
use brenn_obs::alerting::make_capturing_alerter_with_severity;
pub use brenn_obs::alerting::noop_alert_dispatcher;
pub use brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT;
pub use brenn_wasm::{
    ComponentGrant, GuestAlertSeverity, ProcessorAlerter, ProcessorComponent, ProcessorLoadSpec,
};
#[cfg(test)]
use chrono::Utc;
pub use indexmap::IndexMap;
pub use tokio::sync::Notify;

struct NoopAlerter;
impl ProcessorAlerter for NoopAlerter {
    fn alert(&self, _: GuestAlertSeverity, _: &str, _: &str) {}
}
pub fn noop_proc_alerter() -> std::sync::Arc<dyn ProcessorAlerter> {
    std::sync::Arc::new(NoopAlerter)
}

/// Activation pacing that never throttles: a huge burst so no existing
/// non-pacing test trips the gate. The dedicated pacing tests
/// (`tests/pacing.rs`) build their own tight pacing to exercise throttling.
pub fn unthrottled_pacing() -> ActivationPacing {
    ActivationPacing {
        burst: u32::MAX,
        min_period: std::time::Duration::from_millis(1),
    }
}

/// Poll until `subscriber` is owed nothing anywhere, or `deadline` elapses.
/// Returns whether it drained. For the tests that drive the real consumer task
/// and so cannot observe a drain step directly.
pub async fn wait_pending_empty(
    messenger: &brenn_messaging::Messenger,
    subscriber: &ParticipantId,
    deadline: std::time::Duration,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        if brenn_messaging::testutils::owed_everywhere(messenger, subscriber)
            .await
            .is_empty()
        {
            return true;
        }
        if start.elapsed() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Allow-all output ACL for test `ProcessorLoadSpec` constructors. Pass when the
/// test does not exercise the publish-ACL gate; publish is then gated only by
/// port binding.
pub fn allow_all() -> brenn_wasm::OutputAclFn {
    std::sync::Arc::new(|_| true)
}

/// Publish amplification map for dispatch tests, covering the input port names
/// these fixtures drive (`in`, `in0`..`in9`) at the default 1.0 (1000 mt).
/// Extra keys are harmless — the grant only sums over ports present in a window.
pub fn test_amp_map() -> std::collections::HashMap<String, u64> {
    let mut m = std::collections::HashMap::new();
    m.insert("in".to_string(), 1000u64);
    for i in 0..10 {
        m.insert(format!("in{i}"), 1000u64);
    }
    m
}

/// A bound output port with a generous per-sink budget (never trips first) for
/// dispatch tests that assert non-budget behavior.
pub fn test_out_spec(channel_address: String) -> brenn_wasm::OutputPortSpec {
    brenn_wasm::OutputPortSpec {
        channel_address,
        default_urgency: brenn_wasm::ProcessorUrgency::Normal,
        budget: brenn_wasm::SinkBudget {
            fill_mt: 1_000_000_000,
            capacity_mt: 1_000_000_000,
        },
    }
}

/// Latest message body on `addr`, parsed as JSON, or `None` if the channel has
/// no rows. Shared by the git-pipeline test families (parser, consumer, e2e).
pub async fn read_latest(
    messenger: &brenn_messaging::Messenger,
    addr: &str,
) -> Option<serde_json::Value> {
    let body: Option<String> = {
        let conn = messenger.db().lock().await;
        conn.query_row(
            "SELECT m.body FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 ORDER BY m.publish_ts_ns DESC LIMIT 1",
            rusqlite::params![addr],
            |row| row.get::<_, String>(0),
        )
        .ok()
    };
    body.map(|b| serde_json::from_str(&b).expect("valid JSON"))
}

/// A `brenn:` channel entry with a single Wasm subscriber `sub_slug`. Shared by
/// the git-pipeline consumer and e2e test families.
pub fn brenn_channel(address: &str, sub_slug: &str) -> ChannelEntry {
    ChannelEntry {
        uuid: uuid::Uuid::new_v4(),
        address: address.to_string(),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(sub_slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

#[cfg(test)]
use brenn_lib::access::test_fixtures::delivery_policy_for_addresses;
pub use brenn_lib::access::test_fixtures::wasm_policies_from_entries;

// Workspace-relative, not manifest-relative: a suite in a crate above this one
// runs from its own runfiles tree, where this crate's directory does not exist
// and a `<manifest dir>/..` hop would fail to resolve. Every test target runs
// from a runfiles root laid out like the workspace, so this path holds in all
// of them.
pub const DEMO_WASM: &str = "brenn-wasm/target/components/brenn_processor_demo.wasm";

pub const MULTIPORT_WASM: &str = "brenn-wasm/target/components/brenn_processor_multiport.wasm";

/// Give `sub` its position on every channel it reads, primed at head — what boot
/// does before any message reaches the component. A port's delivery state is a
/// cursor, so one that was never attached is a wiring error rather than an idle
/// port.
pub async fn attach_input_ports(
    messenger: &brenn_messaging::Messenger,
    slug: &str,
    sub: &ParticipantId,
    ports: &[(&ChannelEntry, Depth)],
) {
    for (entry, push_depth) in ports {
        brenn_messaging::testutils::attach_wasm_port(messenger, entry, slug, sub, *push_depth)
            .await;
    }
}

/// Build a `WasmConsumerConfig` using the demo WASM and a noop alerter.
/// Returns the config, the alert join handle, and the store tempfile — the
/// caller must keep the tempfile alive (bind to `_db`) for the component's lifetime.
pub fn build_cfg(
    slug: &str,
    messenger: Arc<brenn_messaging::Messenger>,
    channel_entry: &ChannelEntry,
    push_depth: Depth,
    retain_depth: Depth,
) -> (
    WasmConsumerConfig,
    tokio::task::JoinHandle<()>,
    tempfile::NamedTempFile,
) {
    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
    let db = tempfile::NamedTempFile::new().unwrap();
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(DEMO_WASM),
        slug,
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: test_amp_map(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),

        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: None,
    }));
    let notify = Arc::new(Notify::new());
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify,
        messenger,
        alert_dispatcher,
        inputs: vec![WasmInputPort {
            port: "in".to_string(),
            sub: ResolvedSubscription {
                channel_uuid: channel_entry.uuid,
                channel_address: channel_entry.address.clone(),
                push_depth,
                retain_depth,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }],
        outputs: vec![],
        activation_pacing: unthrottled_pacing(),
    };
    (cfg, alert_handle, db)
}

/// Build a K-channel setup where one WASM consumer subscribes to all `channel_names`,
/// each as its own input port (`in0`, `in1`, …). Used by the single-scan / ordering /
/// visit / empty-skip family.
pub async fn build_multi_channel_setup(
    slug: &str,
    channel_names: &[&str],
) -> (
    Arc<brenn_messaging::Messenger>,
    Vec<Arc<ChannelEntry>>,
    ParticipantId,
    WasmConsumerConfig,
    tokio::task::JoinHandle<()>,
    tempfile::NamedTempFile,
) {
    let db = init_db_memory_lib_slice();
    let wasm_sub = ParticipantId::for_wasm(slug);
    let mut entries: Vec<ChannelEntry> = Vec::new();
    for name in channel_names {
        entries.push(
            (*testutils::wasm_channel_entry(slug, name, Depth::Unbounded, Depth::Unbounded))
                .clone(),
        );
    }
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &entries);
    }
    let directory = Arc::new(MessagingDirectory::with_entries(entries.clone()));
    let router = Arc::new(NoopWakeRouter);
    let messenger = brenn_messaging::Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        router as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        wasm_policies_from_entries(&entries),
    ));
    let arc_entries: Vec<Arc<ChannelEntry>> = entries.into_iter().map(Arc::new).collect();
    let ports: Vec<(&ChannelEntry, Depth)> = arc_entries
        .iter()
        .map(|e| (e.as_ref(), Depth::Unbounded))
        .collect();
    attach_input_ports(&messenger, slug, &wasm_sub, &ports).await;

    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
    let store_db = tempfile::NamedTempFile::new().unwrap();
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(DEMO_WASM),
        slug,
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: test_amp_map(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),

        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: None,
    }));
    let notify = Arc::new(Notify::new());
    let inputs = arc_entries
        .iter()
        .enumerate()
        .map(|(i, e)| WasmInputPort {
            port: format!("in{i}"),
            sub: ResolvedSubscription {
                channel_uuid: e.uuid,
                channel_address: e.address.clone(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        })
        .collect();
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify,
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs,
        outputs: vec![],
        activation_pacing: unthrottled_pacing(),
    };
    (
        messenger,
        arc_entries,
        wasm_sub,
        cfg,
        alert_handle,
        store_db,
    )
}

/// Build a multi-port setup using the `processor-multiport` WASM fixture,
/// with per-port subscriber depths.
///
/// `input_ports`: `(channel_name, push_depth, retain_depth)` for each input.
/// Creates K input channels with a WASM subscriber and one output channel
/// (`brenn:<slug>:out`) for the fixture's "out" port.
///
/// Returns `(messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, alert_handle, store_db)`.
#[allow(clippy::type_complexity)]
pub async fn build_multiport_setup_with_depths(
    slug: &str,
    input_ports: &[(&str, Depth, Depth)],
) -> (
    Arc<brenn_messaging::Messenger>,
    Vec<Arc<ChannelEntry>>,
    Arc<ChannelEntry>,
    ParticipantId,
    ParticipantId,
    WasmConsumerConfig,
    tokio::task::JoinHandle<()>,
    tempfile::NamedTempFile,
) {
    use brenn_lib::messaging::config::MessagingGlobalConfig;
    use brenn_lib::messaging::config::{NoiseLevel, ResolvedChannel, Sink};
    use brenn_lib::messaging::{
        ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind, WakeMin,
    };
    use uuid::Uuid;

    let db = init_db_memory_lib_slice();
    let wasm_sub = ParticipantId::for_wasm(slug);
    let out_sub_slug = format!("{slug}-out-reader");
    let out_sub = ParticipantId::for_wasm(&out_sub_slug);

    // Build K input channel entries with per-port depths.
    let mut all_entries: Vec<ChannelEntry> = Vec::new();
    for (name, push_depth, retain_depth) in input_ports {
        all_entries
            .push((*testutils::wasm_channel_entry(slug, name, *push_depth, *retain_depth)).clone());
    }

    // Build the output channel.
    let out_uuid = Uuid::new_v4();
    let out_addr = format!("brenn:{slug}:out");
    let out_entry_raw = ChannelEntry {
        uuid: out_uuid,
        address: out_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(out_sub_slug.clone()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    all_entries.push(out_entry_raw.clone());

    {
        let conn = db.lock().await;
        upsert_channels(&conn, &all_entries);
    }
    let directory = Arc::new(MessagingDirectory::with_entries(all_entries.clone()));
    let router = Arc::new(NoopWakeRouter);
    let messenger = brenn_messaging::Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        router as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        wasm_policies_from_entries(&all_entries),
    ));

    let in_entries: Vec<Arc<ChannelEntry>> = all_entries[..input_ports.len()]
        .iter()
        .map(|e| Arc::new(e.clone()))
        .collect();
    let out_entry = Arc::new(out_entry_raw);
    let ports: Vec<(&ChannelEntry, Depth)> = in_entries
        .iter()
        .zip(input_ports)
        .map(|(e, (_, push_depth, _))| (e.as_ref(), *push_depth))
        .collect();
    attach_input_ports(&messenger, slug, &wasm_sub, &ports).await;
    // The output channel's reader holds a position too, or a case that reads back
    // what the component published would be owed nothing.
    brenn_messaging::testutils::attach_wasm_port(
        &messenger,
        &out_entry,
        &out_sub_slug,
        &out_sub,
        Depth::Unbounded,
    )
    .await;

    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
    let store_db = tempfile::NamedTempFile::new().unwrap();

    let mut output_ports = std::collections::HashMap::new();
    output_ports.insert("out".to_string(), test_out_spec(out_addr));

    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(MULTIPORT_WASM),
        slug,
        output_ports,
        input_amplification_mt: test_amp_map(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),

        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: None,
    }));
    let notify = Arc::new(Notify::new());
    let inputs: Vec<WasmInputPort> = in_entries
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let (_, push_depth, retain_depth) = input_ports[i];
            WasmInputPort {
                port: format!("in{i}"),
                sub: ResolvedSubscription {
                    channel_uuid: e.uuid,
                    channel_address: e.address.clone(),
                    push_depth,
                    retain_depth,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
                amplification_mt: 1000,
            }
        })
        .collect();
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify,
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs,
        outputs: vec![],
        activation_pacing: unthrottled_pacing(),
    };

    (
        messenger,
        in_entries,
        out_entry,
        out_sub,
        wasm_sub,
        cfg,
        alert_handle,
        store_db,
    )
}

/// Build a multi-port setup using the `processor-multiport` WASM fixture.
///
/// Creates K input channels (named `<slug>:in0`, `<slug>:in1`, …) with a WASM
/// subscriber (both depths `Unbounded`) and one output channel (`brenn:<slug>:out`)
/// for the fixture's "out" port. The fixture publishes one summary message per
/// activation, making window composition directly assertable from the output channel.
///
/// Returns `(messenger, in_entries, out_entry, out_sub, wasm_sub, cfg, alert_handle, store_db)`.
#[allow(clippy::type_complexity)]
pub async fn build_multiport_setup(
    slug: &str,
    input_channel_names: &[&str],
) -> (
    Arc<brenn_messaging::Messenger>,
    Vec<Arc<ChannelEntry>>,
    Arc<ChannelEntry>,
    ParticipantId,
    ParticipantId,
    WasmConsumerConfig,
    tokio::task::JoinHandle<()>,
    tempfile::NamedTempFile,
) {
    let ports: Vec<(&str, Depth, Depth)> = input_channel_names
        .iter()
        .map(|name| (*name, Depth::Unbounded, Depth::Unbounded))
        .collect();
    build_multiport_setup_with_depths(slug, &ports).await
}

/// Build a two-channel messenger setup for publish-path tests:
/// - `in_ch`: webhook channel with WASM subscriber (`wasm:<slug>`).
/// - `out_ch`: brenn: channel with a second subscriber (another wasm slug or app).
///
/// Returns `(messenger, in_channel_entry, out_channel_entry, wasm_sub,
///           out_subscriber_id, cfg, alert_handle)`.
pub async fn build_two_channel_setup(
    slug: &str,
    second_sub_slug: &str,
) -> (
    Arc<brenn_messaging::Messenger>,
    Arc<ChannelEntry>,
    Arc<ChannelEntry>,
    ParticipantId,
    ParticipantId,
    WasmConsumerConfig,
    tokio::task::JoinHandle<()>,
    tempfile::NamedTempFile,
) {
    use brenn_lib::messaging::config::MessagingGlobalConfig;
    use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
    use brenn_lib::messaging::{
        ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind, WakeMin,
    };
    use uuid::Uuid;

    let db = init_db_memory_lib_slice();
    let wasm_sub = ParticipantId::for_wasm(slug);
    let out_sub = ParticipantId::for_wasm(second_sub_slug);

    // Input channel: webhook: type, WASM subscriber.
    let in_uuid = webhook_channel_uuid_from_slug("e2e-in");
    let in_addr = format!("{WEBHOOK_ADDRESS_PREFIX}e2e-in");
    let in_entry = Arc::new(ChannelEntry {
        uuid: in_uuid,
        address: in_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Webhook,
        mount: Some("/webhooks/e2e-in".to_string()),
    });

    // Output channel: brenn: type, second WASM subscriber.
    let out_uuid = Uuid::new_v4();
    let out_addr = "brenn:e2e-out".to_string();
    let out_entry = Arc::new(ChannelEntry {
        uuid: out_uuid,
        address: out_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(second_sub_slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    });

    {
        let conn = db.lock().await;
        upsert_channels(&conn, &[(*in_entry).clone(), (*out_entry).clone()]);
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![
        (*in_entry).clone(),
        (*out_entry).clone(),
    ]));
    let router = Arc::new(NoopWakeRouter);
    let messenger = brenn_messaging::Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        router as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_messaging::testutils::wasm_registrations(
        wasm_policies_from_entries(&[(*in_entry).clone(), (*out_entry).clone()]),
    ));
    attach_input_ports(
        &messenger,
        slug,
        &wasm_sub,
        &[(in_entry.as_ref(), Depth::Unbounded)],
    )
    .await;
    // The downstream reader is a component too: it holds a position on the
    // output channel.
    attach_input_ports(
        &messenger,
        second_sub_slug,
        &out_sub,
        &[(out_entry.as_ref(), Depth::Unbounded)],
    )
    .await;

    // Build the ProcessorComponent with the "out" port bound.
    let mut output_ports = std::collections::HashMap::new();
    output_ports.insert("out".to_string(), test_out_spec(out_addr.clone()));
    let store_db = tempfile::NamedTempFile::new().unwrap();
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new(DEMO_WASM),
        slug,
        output_ports,
        input_amplification_mt: test_amp_map(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),

        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: None,
    }));
    let notify = Arc::new(Notify::new());
    let (alert_dispatcher, alert_handle) = noop_alert_dispatcher();
    let cfg = WasmConsumerConfig {
        slug: slug.to_string(),
        component,
        notify,
        messenger: Arc::clone(&messenger),
        alert_dispatcher,
        inputs: vec![WasmInputPort {
            port: "in".to_string(),
            sub: ResolvedSubscription {
                channel_uuid: in_uuid,
                channel_address: in_addr,
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
            amplification_mt: 1000,
        }],
        outputs: vec![brenn_lib::messaging::config::WasmOutputPort {
            port: "out".to_string(),
            channel_uuid: out_uuid,
            channel_address: out_addr,
            default_urgency: brenn_lib::messaging::Urgency::Normal,
            budget: brenn_lib::messaging::config::WasmSinkBudget {
                fill_mt: 1_000_000,
                capacity_mt: 1_000_000,
            },
        }],
        activation_pacing: unthrottled_pacing(),
    };

    (
        messenger,
        in_entry,
        out_entry,
        wasm_sub,
        out_sub,
        cfg,
        alert_handle,
        store_db,
    )
}

// The suites themselves stay in this crate's own test build; only the harness
// above them is compiled into the `testutils` library.
#[cfg(test)]
mod alerter;
#[cfg(test)]
mod fanout;
#[cfg(test)]
mod git_forge_parser;
#[cfg(test)]
mod multiport;
#[cfg(test)]
mod pacing;
#[cfg(test)]
mod renotify;
#[cfg(test)]
mod scan;

//! The backend adapter: the probe as an ordinary WASM consumer.
//!
//! A mount here is `spawn_wasm_consumer_task` — the consumer task starting,
//! which is what boot and every reload that arrives or replaces a consumer do —
//! and a remount is stopping that task and starting another over the same
//! positions, which is what a process restart does.
//!
//! Two things the adapter supplies that a running backend supplies for it:
//! releasing parked messages that have come due, and waking the consumer. In a
//! live backend both are the background dispatcher's pass; here they run once
//! per [`Host::drain`], which the scenario helpers poll, so a scenario's own
//! waiting is what drives time forward.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use brenn_lib::access::test_fixtures::wasm_policies_from_entries;
use brenn_lib::messaging::config::{
    ActivationPacing, Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel,
    ResolvedSubscription, Sink, WasmInputPort, WasmOutputPort, WasmSinkBudget,
};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, Urgency, WakeMin,
};
use brenn_messaging::{Messenger, WakeRouter, query::NoopWakeRouter, testutils};
use brenn_messaging_store::db::init_db_memory;
use brenn_obs::alerting::noop_alert_dispatcher;
use brenn_wasm::{
    ComponentGrant, GuestAlertSeverity, ProcessorAlerter, ProcessorComponent, ProcessorLoadSpec,
    ProcessorUrgency, SinkBudget, store::DEFAULT_MAX_PAGE_COUNT,
};
use brenn_wasm_dispatch::{ConsumerHandle, WasmConsumerConfig, spawn_wasm_consumer_task};

use crate::{Host, MountSpec, Realm, Report, TrapDisposition, port, scenarios};

/// Workspace-relative, as every runfiles tree is laid out like the workspace.
const PROBE_WASM: &str = "brenn-wasm/target/components/brenn_processor_transplant.wasm";

struct NoopAlerter;
impl ProcessorAlerter for NoopAlerter {
    fn alert(&self, _: GuestAlertSeverity, _: &str, _: &str) {}
}

/// A bounded depth. `0` is the sampled binding: no position, context only.
fn depth(n: u32) -> Depth {
    Depth::Bounded(n as u64)
}

/// A `brenn:` channel the probe subscribes to at the given depths.
fn subscribed_channel(slug: &str, name: &str, push: u32, retain: u32) -> ChannelEntry {
    channel(
        &format!("brenn:{slug}:{name}"),
        vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(slug.to_string()),
            push_depth: depth(push),
            retain_depth: depth(retain),
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
    )
}

/// A `brenn:` channel nobody subscribes to — the probe's own outputs, which this
/// adapter reads off the store rather than through a subscription.
fn output_channel(slug: &str, name: &str) -> ChannelEntry {
    channel(&format!("brenn:{slug}:{name}"), vec![])
}

fn channel(address: &str, subscribers: Vec<SubscriberEntry>) -> ChannelEntry {
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
        subscribers,
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// One probe instance hosted by the backend.
struct Backend {
    /// The consumer task's whole configuration. A task takes it by value, so a
    /// mount clones it; a remount clones it again, which is what makes restart
    /// over the same positions expressible at all.
    template: WasmConsumerConfig,
    subscriber: ParticipantId,
    /// Channel per port name, both directions.
    channels: HashMap<String, ChannelEntry>,
    handle: Option<ConsumerHandle>,
    /// Highest `messaging_messages.id` on the report channel this adapter has
    /// already returned.
    read_through: i64,
    /// Whether the probe's positions have been attached. A remount reuses them,
    /// which is the whole point of the scenario.
    attached: bool,
    /// While set, `drain` reads reports without running the release pass.
    releases_held: bool,
    _alerts: tokio::task::JoinHandle<()>,
}

impl Backend {
    async fn new(slug: &str, spec: &MountSpec) -> Self {
        let db = init_db_memory();
        let subscriber = ParticipantId::for_wasm(slug);

        let mut channels: HashMap<String, ChannelEntry> = HashMap::new();
        for binding in &spec.inputs {
            channels.insert(
                binding.port.to_string(),
                subscribed_channel(slug, binding.port, binding.push_depth, binding.retain_depth),
            );
        }
        if let Some(realm) = spec.tick {
            // TODO(backend-wasm-ephemeral-binding): a backend WASM consumer
            // cannot bind an `ephemeral:` channel, so the probe's tick chain can
            // only be driven here in the durable realm.
            assert_eq!(
                realm,
                Realm::Durable,
                "the backend binds `brenn:` channels only",
            );
            channels.insert(
                port::TICK.to_string(),
                subscribed_channel(slug, port::TICK, 4, 8),
            );
        }
        for out in [port::OUT, port::REPORT] {
            channels.insert(out.to_string(), output_channel(slug, out));
        }

        let entries: Vec<ChannelEntry> = channels.values().cloned().collect();
        {
            let conn = db.lock().await;
            brenn_messaging_store::db::upsert_channels(&conn, &entries);
        }
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries.clone())),
            Arc::from("host-conformance"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(testutils::wasm_registrations(
            wasm_policies_from_entries(&entries),
        ));

        // Output ports: the two the probe's specification makes mandatory, plus
        // the io port when it is bound. An io port is an output and an input at
        // once, so it appears in both lists over one channel.
        let mut out_names = vec![port::OUT, port::REPORT];
        if spec.tick.is_some() {
            out_names.push(port::TICK);
        }
        let mut output_ports = HashMap::new();
        let mut outputs = Vec::new();
        for name in &out_names {
            let entry = &channels[*name];
            output_ports.insert(
                name.to_string(),
                brenn_wasm::OutputPortSpec {
                    channel_address: entry.address.clone(),
                    default_urgency: ProcessorUrgency::Normal,
                    budget: SinkBudget {
                        fill_mt: 1_000_000_000,
                        capacity_mt: 1_000_000_000,
                    },
                },
            );
            outputs.push(WasmOutputPort {
                port: name.to_string(),
                channel_uuid: entry.uuid,
                channel_address: entry.address.clone(),
                default_urgency: Urgency::Normal,
                budget: WasmSinkBudget {
                    fill_mt: 1_000_000_000,
                    capacity_mt: 1_000_000_000,
                },
            });
        }

        let mut in_names: Vec<&str> = spec.inputs.iter().map(|b| b.port).collect();
        if spec.tick.is_some() {
            in_names.push(port::TICK);
        }
        let inputs: Vec<WasmInputPort> = in_names
            .iter()
            .map(|name| {
                let entry = &channels[*name];
                let sub = entry.subscribers[0].clone();
                WasmInputPort {
                    port: (*name).to_string(),
                    sub: ResolvedSubscription {
                        channel_uuid: entry.uuid,
                        channel_address: entry.address.clone(),
                        push_depth: sub.push_depth,
                        retain_depth: sub.retain_depth,
                        noise: NoiseLevel::Silent,
                        wake_min: WakeMin::Normal,
                    },
                    amplification_mt: 1000,
                }
            })
            .collect();

        let mut config = HashMap::new();
        if let Some(tick_ms) = spec.tick_ms {
            config.insert("tick_ms".to_string(), tick_ms.to_string());
        }

        let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
            component_path: std::path::Path::new(PROBE_WASM),
            slug,
            declared_out_ports: output_ports.keys().cloned().collect::<BTreeSet<_>>(),
            output_ports,
            input_amplification_mt: in_names
                .iter()
                .map(|name| ((*name).to_string(), 1000u64))
                .collect(),
            mqtt_sinks: HashMap::new(),
            config,
            grants: [
                ComponentGrant::Ports,
                ComponentGrant::Log,
                ComponentGrant::Config,
            ]
            .into_iter()
            .collect(),
            store_path: None,
            max_page_count: DEFAULT_MAX_PAGE_COUNT,
            max_payload_bytes: 1024 * 1024,
            alerter: Arc::new(NoopAlerter),
            output_acl: Arc::new(|_| true),
            mqtt_publish: None,
            tool_host: None,
        }));

        let (alert_dispatcher, alerts) = noop_alert_dispatcher();
        Backend {
            template: WasmConsumerConfig {
                slug: slug.to_string(),
                component,
                notify: Arc::new(tokio::sync::Notify::new()),
                messenger,
                alert_dispatcher,
                inputs,
                outputs,
                // Effectively no pacing: what a starved consumer does is the
                // pacing suite's subject, not this one's.
                activation_pacing: ActivationPacing {
                    burst: u32::MAX,
                    min_period: std::time::Duration::from_millis(1),
                },
            },
            subscriber,
            channels,
            handle: None,
            read_through: 0,
            attached: false,
            releases_held: false,
            _alerts: alerts,
        }
    }

    fn config(&self) -> WasmConsumerConfig {
        self.template.clone()
    }
}

impl Host for Backend {
    fn trap_disposition(&self) -> TrapDisposition {
        TrapDisposition::Quarantine
    }

    async fn mount(&mut self) {
        if !self.attached {
            // Positions exist before the task does, in both of the backend's
            // start paths, so the probe's debt is `Owed` from the task's first
            // instruction rather than from its first delivery.
            for input in &self.template.inputs {
                let entry = &self.channels[&input.port];
                testutils::attach_wasm_port(
                    &self.template.messenger,
                    entry,
                    &self.template.slug,
                    &self.subscriber,
                    input.sub.push_depth,
                )
                .await;
            }
            self.attached = true;
        }
        self.handle = Some(spawn_wasm_consumer_task(self.config()));
    }

    async fn publish(&mut self, port: &str, body: &str) {
        let entry = self
            .channels
            .get(port)
            .unwrap_or_else(|| panic!("scenario published onto unbound port {port:?}"));
        testutils::insert_bus_message(&self.template.messenger, entry, body, ChannelScheme::Brenn)
            .await;
    }

    async fn remount_after(&mut self, idle: std::time::Duration) {
        let handle = self.handle.take().expect("remount before mount");
        handle.stop_and_join().await;
        // Nothing releases while the task is down: the dispatcher's pass is
        // this adapter's `drain`, and the scenario is not draining.
        tokio::time::sleep(idle).await;
        self.handle = Some(spawn_wasm_consumer_task(self.config()));
    }

    fn hold_releases(&mut self, hold: bool) {
        self.releases_held = hold;
    }

    async fn drain(&mut self) -> Vec<Report> {
        // What the background dispatcher's pass does in a live backend: release
        // what has come due, then wake the consumer. A wake with nothing ready
        // costs one snapshot read that answers `None`.
        if !self.releases_held {
            self.template
                .messenger
                .release_due_messages(chrono::Utc::now())
                .await;
        }
        self.template.notify.notify_one();

        let address = self.channels[port::REPORT].address.clone();
        let rows: Vec<(i64, String)> = {
            let conn = self.template.messenger.db().lock().await;
            let mut stmt = conn
                .prepare(
                    "SELECT m.id, m.body FROM messaging_messages m \
                     JOIN messaging_channels c ON c.uuid = m.channel_uuid \
                     WHERE c.address = ?1 AND m.id > ?2 ORDER BY m.id",
                )
                .expect("prepare the report read");
            let mapped = stmt
                .query_map(rusqlite::params![address, self.read_through], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .expect("query the probe's reports");
            mapped.map(|r| r.expect("report row")).collect()
        };
        if let Some((last, _)) = rows.last() {
            self.read_through = *last;
        }
        rows.iter().map(|(_, body)| Report::parse(body)).collect()
    }
}

macro_rules! backend_scenario {
    ($name:ident) => {
        #[tokio::test]
        async fn $name() {
            let mut host = Backend::new(
                concat!("conf-", stringify!($name)),
                &scenarios::$name::spec(),
            )
            .await;
            scenarios::$name::run(&mut host).await;
        }
    };
}

backend_scenario!(mount_over_empty_channels);
backend_scenario!(mount_over_history);
backend_scenario!(self_tick_chain);
backend_scenario!(sampled_only_wiring);
backend_scenario!(err_consumes);
backend_scenario!(trap_disposition);
backend_scenario!(state_does_not_survive);
backend_scenario!(remount);
backend_scenario!(due_tick_is_shown_at_mount);

/// The one known-failing scenario: the self-tick chain over an `ephemeral:`
/// channel. The gap is a registry fork on the address realm, never a decision.
///
/// TODO(backend-wasm-ephemeral-binding): remove the `ignore` when a backend WASM
/// consumer can bind an `ephemeral:` channel. Listed as a failing scenario
/// rather than a comment in a test header so the gap is visible in the suite.
#[tokio::test]
#[ignore = "backend-wasm-ephemeral-binding: backend WASM consumers cannot bind ephemeral: channels"]
async fn self_tick_chain_ephemeral() {
    let mut host = Backend::new(
        "conf-self-tick-ephemeral",
        &scenarios::self_tick_chain::spec_in(Realm::Ephemeral),
    )
    .await;
    scenarios::self_tick_chain::run(&mut host).await;
}

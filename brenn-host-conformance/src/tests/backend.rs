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
//!
//! # Everything else is the production path
//!
//! The `Messenger` is built in boot's own order — durable entries upserted,
//! non-durable entries given rings, registrations installed before either — so
//! the adapter can bind a port in any scheme a top-level component may bind.
//! Publishes go through `publish_from_system`; reports are read off the
//! channel's own [`RetentionStore`]. Neither half is aware of the channel's
//! scheme, which is what lets one scenario body run over three schemes.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use brenn_envelope::addressing::nondurable_channel_uuid;
use brenn_envelope::grants::{ComponentHost, EntityKind, Plane, bindable_schemes};
use brenn_lib::access::test_fixtures::{publish_policy_for_addresses, wasm_policies_from_entries};
use brenn_lib::messaging::config::{
    ActivationPacing, Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel,
    ResolvedSubscription, Sink, WasmInputPort, WasmOutputPort, WasmSinkBudget,
};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, Urgency, WakeMin,
};
use brenn_messaging::{
    Messenger, WakeRouter, publish::PublishResult, query::NoopWakeRouter, testutils,
};
use brenn_messaging_store::db::init_db_memory;
use brenn_messaging_store::store::{MessageSeq, RingStores};
use brenn_obs::alerting::noop_alert_dispatcher;
use brenn_wasm::{
    ComponentGrant, GuestAlertSeverity, ProcessorAlerter, ProcessorComponent, ProcessorLoadSpec,
    ProcessorUrgency, SinkBudget, store::DEFAULT_MAX_PAGE_COUNT,
};
use brenn_wasm_dispatch::{ConsumerHandle, WasmConsumerConfig, spawn_wasm_consumer_task};

use crate::{Host, MountSpec, Report, TrapDisposition, port, scenarios};

/// Workspace-relative, as every runfiles tree is laid out like the workspace.
const PROBE_WASM: &str = "brenn-wasm/target/components/brenn_processor_transplant.wasm";

/// The system component this adapter publishes and reads as. An ordinary
/// in-process principal with a code-built policy, not the probe or the kernel.
const HOST: &str = "host-conformance";

/// The channel-level retain depth every entry gets, whatever its scheme.
///
/// Bounded rather than unbounded for two reasons. A non-durable entry cannot be
/// unbounded at all — an in-memory ring with no cap is a leak — and the
/// `brenn:` and non-durable runs of a scheme-varied scenario must be compared on
/// equal retention rather than one capped and one not. 64 is an order of
/// magnitude above what any scenario produces on any channel (the longest
/// reports four activations; the tick channel holds one parked message at a
/// time) and far below the ring's own sanity cap. It is also what [`Host::drain`]
/// reads the report channel at, so a scenario that outgrew it would lose reports
/// rather than read them: a new scenario that needs more raises this, it does not
/// lower its expectations.
///
/// Load-bearing for the read: this adapter reads the report channel as a
/// whole-window read filtered by sequence, not a cursor read, so this cap is
/// also the cap on what one drain can see. [`Host::drain`] refuses when the
/// window has outrun what it last returned rather than letting an evicted
/// report read as a missing activation.
///
/// Both adapters must use the same read shape — bounded retained view filtered
/// by the caller's read point, refusing on overflow — so that a scheme-varied
/// divergence is attributable to hosting, not to adapter mechanics.
const RETAIN_DEPTH: u64 = 64;

/// The scheme every port a scenario does not vary is bound in.
///
/// Read off [`Host::schemes`] rather than written out, so the trait's statement
/// that the unvaried ports use the first entry stays true under a reordering of
/// that list instead of quietly becoming false.
fn default_scheme() -> ChannelScheme {
    Backend::schemes()[0]
}

struct NoopAlerter;
impl ProcessorAlerter for NoopAlerter {
    fn alert(&self, _: GuestAlertSeverity, _: &str, _: &str) {}
}

/// A bounded depth. `0` is the sampled binding: no position, context only.
fn depth(n: u32) -> Depth {
    Depth::Bounded(n as u64)
}

/// A channel address in the given scheme. `.`-separated because the publish
/// path's unreserved-charset gate rejects `:`.
fn address(scheme: ChannelScheme, bare: &str) -> String {
    format!("{}{bare}", scheme.prefix())
}

/// The scheme-local name of the channel a port is bound to.
fn bare_name(slug: &str, name: &str) -> String {
    format!("{slug}.{name}")
}

/// A channel the probe subscribes to at the given depths.
fn subscribed_channel(
    scheme: ChannelScheme,
    slug: &str,
    name: &str,
    push: u32,
    retain: u32,
) -> ChannelEntry {
    channel(
        scheme,
        &bare_name(slug, name),
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
    channel(default_scheme(), &bare_name(slug, name), vec![])
}

/// One directory entry. [`address`] is the sole formatter; callers pass scheme
/// and bare name rather than a pre-formatted address. A non-durable channel's
/// UUID is derived from its bare name so the entry and the ring
/// `RingStores::build` creates agree on the key.
fn channel(scheme: ChannelScheme, bare: &str, subscribers: Vec<SubscriberEntry>) -> ChannelEntry {
    let capabilities = scheme
        .capabilities()
        .expect("the adapter binds pub/sub schemes only");
    ChannelEntry {
        uuid: if capabilities.durable {
            uuid::Uuid::new_v4()
        } else {
            nondurable_channel_uuid(scheme, bare)
        },
        address: address(scheme, bare),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Bounded(RETAIN_DEPTH),
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers,
        transport_type: scheme,
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
    /// The report channel's own store, read under [`HOST`]'s identity.
    reports: Arc<dyn brenn_messaging_store::store::RetentionStore>,
    /// The identity that read holds — a sampled reader, so it holds no position
    /// and the store keeps nothing for it.
    reader: ParticipantId,
    /// Highest report-channel sequence this adapter has already returned.
    read_through: Option<MessageSeq>,
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
        // Only the `io tick` port varies: it is an input and an output over one
        // channel, so the scheme under test is exercised on both planes by one
        // varied port, and a divergence is attributable to it rather than to the
        // adapter's own read path.
        for binding in &spec.inputs {
            channels.insert(
                binding.port.to_string(),
                subscribed_channel(
                    default_scheme(),
                    slug,
                    binding.port,
                    binding.push_depth,
                    binding.retain_depth,
                ),
            );
        }
        if let Some(scheme) = spec.tick {
            channels.insert(
                port::TICK.to_string(),
                subscribed_channel(scheme, slug, port::TICK, 4, 8),
            );
        }
        for out in [port::OUT, port::REPORT] {
            channels.insert(out.to_string(), output_channel(slug, out));
        }

        let entries: Vec<ChannelEntry> = channels.values().cloned().collect();
        // Non-durable channels have no DB row; they get a ring instead.
        let (durable, nondurable): (Vec<ChannelEntry>, Vec<ChannelEntry>) = entries
            .iter()
            .cloned()
            .partition(|entry| entry.capabilities().durable);
        {
            let conn = db.lock().await;
            brenn_messaging_store::db::upsert_channels(&conn, &durable);
        }
        // Publish authority on the channels a scenario publishes onto, and on
        // nothing else: the probe's own outputs are the probe's to write, so a
        // stray `Host::publish` onto one meets the production gate's refusal
        // rather than this adapter's convenience.
        let inbound: Vec<&str> = entries
            .iter()
            .filter(|entry| !entry.subscribers.is_empty())
            .map(|entry| entry.address.as_str())
            .collect();
        let mut system_policies = HashMap::new();
        system_policies.insert(HOST.to_string(), publish_policy_for_addresses(inbound));
        // Order matters: registrations while the `Arc` is unique, ring stores
        // before any retention store holds the target resolver, consumer clone last.
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries.clone())),
            Arc::from(HOST),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(testutils::wasm_registrations(wasm_policies_from_entries(
            &entries,
        )))
        .with_subscriber_registrations(testutils::system_registrations(system_policies))
        .with_ring_stores(Arc::new(RingStores::build(&nondurable)));
        let reports = messenger.store_for(&channels[port::REPORT]);
        // The varied scheme must actually select the store class it names. The
        // expectation is written out here rather than read back off
        // `capabilities()` — which is what selects the store — so a
        // reclassification that made `ephemeral:` durable trips here instead of
        // running all three schemes over one store class and reading as three
        // agreeing answers.
        if let Some(scheme) = spec.tick {
            let expected_durable = match scheme {
                ChannelScheme::Brenn => true,
                ChannelScheme::Ephemeral | ChannelScheme::Local => false,
                other => panic!("the adapter binds no port in {other:?}"),
            };
            let tick = messenger.store_for(&channels[port::TICK]);
            assert_eq!(
                tick.capabilities().durable,
                expected_durable,
                "the {scheme:?} tick channel resolved to the wrong store class",
            );
        }

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
            reports,
            reader: ParticipantId::for_system(HOST),
            read_through: None,
            attached: false,
            releases_held: false,
            _alerts: alerts,
        }
    }

    fn config(&self) -> WasmConsumerConfig {
        self.template.clone()
    }

    /// A publish this adapter expects the production gate to refuse.
    /// [`Host::publish`] asserts success, which is what a scenario wants; this
    /// is the other arm, so the narrowed publish authority has a run behind it.
    async fn publish_expecting_refusal(&self, port: &str, body: &str) -> PublishResult {
        let entry = &self.channels[port];
        self.template
            .messenger
            .publish_from_system(HOST, &entry.address, body, Urgency::Normal, None)
            .await
    }
}

impl Host for Backend {
    fn schemes() -> &'static [ChannelScheme] {
        &[
            ChannelScheme::Brenn,
            ChannelScheme::Ephemeral,
            ChannelScheme::Local,
        ]
    }

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
        // The production publish path, not a direct row insert.
        let result = self
            .template
            .messenger
            .publish_from_system(HOST, &entry.address, body, Urgency::Normal, None)
            .await;
        assert!(
            matches!(result, PublishResult::Ok { .. }),
            "the adapter's publish onto {} was refused: {result:?}",
            entry.address,
        );
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

        // Sampled read: `push_limit = 0` holds no position and is served the
        // store's retain-only view, regardless of scheme.
        let window = self
            .reports
            .window(
                &self.reader,
                Depth::Bounded(0),
                Depth::Bounded(RETAIN_DEPTH),
            )
            .await
            .expect("a sampled read needs no position and is always served");
        // The window is the newest `RETAIN_DEPTH` entries, so a full one may
        // have evicted a report this adapter never returned, and the store
        // cannot tell that apart from a quiet channel. Refuse instead: a lost
        // report reads as a missing activation, which is exactly the
        // misattribution this suite exists to prevent.
        assert!(
            window.entries.len() < RETAIN_DEPTH as usize
                || window.entries.first().is_some_and(|(oldest, _)| {
                    self.read_through.is_some_and(|read| *oldest <= read)
                }),
            "the report window is full and no longer reaches what this adapter last \
             returned, so a report may have been evicted unread — raise RETAIN_DEPTH",
        );
        let fresh: Vec<(MessageSeq, String)> = window
            .entries
            .iter()
            .filter(|(seq, _)| self.read_through.is_none_or(|read| *seq > read))
            .map(|(seq, envelope)| (*seq, envelope.body.clone()))
            .collect();
        if let Some((seq, _)) = fresh.last() {
            self.read_through = Some(*seq);
        }
        fresh.iter().map(|(_, body)| Report::parse(body)).collect()
    }
}

/// One scenario against a fresh adapter, in one arm per shape.
///
/// The spec is built once and both the mount and the scenario body read that
/// same value: a test cannot stand the host up on one wiring and assert
/// another. The scheme-varied arm takes the whole list of schemes the scenario
/// is driven in, and generates — beside the runs — the assertion that the list
/// is [`Host::schemes`] entire, so adding a scheme the adapter can bind without
/// driving it here fails rather than passes quietly.
macro_rules! backend_scenario {
    ($name:ident) => {
        #[tokio::test]
        async fn $name() {
            let spec = scenarios::$name::spec();
            let mut host = Backend::new(concat!("conf-", stringify!($name)), &spec).await;
            scenarios::$name::run(&mut host, &spec).await;
        }
    };
    ($name:ident, [$($run:ident => $scheme:expr),+ $(,)?]) => {
        /// One test per scheme, named for it, so a divergence is legible from
        /// the failing test's name rather than from a parameter in its body.
        mod $name {
            use super::*;

            $(
                #[tokio::test]
                async fn $run() {
                    let spec = scenarios::$name::spec_in($scheme);
                    let mut host = Backend::new(
                        concat!("conf-", stringify!($name), "-", stringify!($run)),
                        &spec,
                    )
                    .await;
                    scenarios::$name::run(&mut host, &spec).await;
                }
            )+

            #[test]
            fn covers_every_scheme() {
                assert_eq!(
                    &[$($scheme),+][..],
                    Backend::schemes(),
                    "every scheme the adapter says it drives is driven by a run \
                     of this scenario, in the adapter's own order",
                );
            }
        }
    };
}

backend_scenario!(mount_over_empty_channels);
backend_scenario!(mount_over_history);
backend_scenario!(sampled_only_wiring);
backend_scenario!(err_consumes);
backend_scenario!(trap_disposition);
backend_scenario!(state_does_not_survive);

backend_scenario!(self_tick_chain, [
    brenn => ChannelScheme::Brenn,
    ephemeral => ChannelScheme::Ephemeral,
    local => ChannelScheme::Local,
]);
backend_scenario!(remount, [
    brenn => ChannelScheme::Brenn,
    ephemeral => ChannelScheme::Ephemeral,
    local => ChannelScheme::Local,
]);
backend_scenario!(due_tick_is_shown_at_mount, [
    brenn => ChannelScheme::Brenn,
    ephemeral => ChannelScheme::Ephemeral,
    local => ChannelScheme::Local,
]);

/// The scheme list is the adapter's claim about what it drives, and this is what
/// ties it to the table the host itself is gated on: a top-level component may
/// bind a scheme here only if it may bind it on *both* planes, since the varied
/// port is an input and an output at once. Widening or narrowing the pub/sub
/// part of either row moves the intersection and trips this.
#[test]
fn schemes_are_the_both_planes_intersection_of_the_consumer_row() {
    let kind = EntityKind::Component(ComponentHost::TopLevel);
    let subscribe = bindable_schemes(kind, Plane::Subscribe);
    let publish = bindable_schemes(kind, Plane::Publish);
    let both: Vec<ChannelScheme> = subscribe
        .iter()
        .copied()
        .filter(|scheme| publish.contains(scheme))
        .collect();
    assert_eq!(
        Backend::schemes(),
        both,
        "the adapter drives exactly the schemes a top-level component may bind on \
         both planes",
    );
}

/// The adapter grants itself publish authority on the channels a scenario
/// publishes onto and on nothing else, and this is what makes that narrowing
/// more than a comment: the probe's own output channel meets the production
/// gate's refusal, on the ACL rather than on an absent grant.
#[tokio::test]
async fn a_publish_onto_an_output_channel_is_refused_by_the_acl() {
    let spec = scenarios::self_tick_chain::spec_in(default_scheme());
    let host = Backend::new("conf-publish-refusal", &spec).await;
    let result = host.publish_expecting_refusal(port::OUT, "{}").await;
    assert!(
        matches!(result, PublishResult::AclDenied(_)),
        "the probe's own output is outside the adapter's publish authority: {result:?}",
    );
}

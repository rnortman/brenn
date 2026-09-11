//! The surface adapter: the probe as an ordinary page-placed instance.
//!
//! A mount here is `Input::ActivationRegistered` over a page with a bindings
//! document in force — the registration a component's own bring-up raises — and a
//! remount is the withdrawal and re-registration of the same instance id, which is
//! what a page reload does to every instance it holds.
//!
//! The page is the real [`SurfacePage`], driven through [`turn`]: real stores, a
//! real confined router carrying the surface's plane policy, real positions, the
//! real assembly and the real flush-on-ok. What the browser supplies and this
//! adapter supplies for it is the clock, the invocation, and the release deadline
//! the driver would have armed — none of which is hosting policy.
//!
//! # One instance per activation, from the first line
//!
//! The entry compiles the probe once and instantiates it **per call**, so the
//! contract's activation-scoped linear memory is what this adapter hosts under.
//! That pins the *contract*, not the page loader: the loader's lifetime policy is
//! TypeScript and is pinned by the vitest beside `processor-transplant.test.ts`.
//!
//! # Why every channel is `local:`
//!
//! A transportable channel's authority is the peer, so driving one here would mean
//! scripting a server rather than hosting a component. A page-confined channel's
//! authority is the page itself, which is the half under test — and it is also the
//! realm with the lifetime [`Realm::Durable`] names on this host: a `local:`
//! channel's store outlives any one registration, so a message parked on it is
//! still parked across a remount, exactly as a `brenn:` channel's is on the
//! backend.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use brenn_attach_client::Millis;
use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::driver::{flush_stamps, new_stamp};
use brenn_attach_client::router::{Origin, RouteOutcome, RouteRequest};
use brenn_attach_proto::SubscribeOutcome;
use brenn_envelope::Urgency;
use brenn_envelope::grants::ComponentGrant;
use brenn_page_harness::{Kind, Page, types};
use brenn_surface_contract::ActivationError;
use brenn_surface_kernel::ActivationOutcome;
use brenn_surface_kernel::activation::ReadyActivation;
use brenn_surface_kernel::outward::Completed;
use brenn_surface_kernel::page::SurfacePage;
use brenn_surface_kernel::publish_buffer::PublishBuffer;
use brenn_surface_kernel::turn::{self, Input};
use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::{
    Binding, ComponentEntry, LocalChannel, NoiseLevel, OutputBinding, Urgency as SchemaUrgency,
};

use crate::{Host, MountSpec, Realm, Report, TrapDisposition, port, scenarios};

/// Workspace-relative, as every runfiles tree is laid out like the workspace.
const PROBE_WASM: &str = "brenn-wasm/target/components/brenn_processor_transplant.wasm";

const PROBE: &str = "probe";

/// The chrome singleton every bindings document names. Declared and never
/// registered: the page owes an unregistered instance nothing, and the probe is
/// the only component this suite hosts.
const CHROME: &str = "chrome";

/// The channel the page's own wiring is retained on. Transportable, as it is on a
/// real page — nothing is ever delivered on it here, since the document is applied
/// directly.
const CONFIG_CHANNEL: &str = "ephemeral:site.surface.conf.bindings";

/// The identity an external publish is attributed to: not the probe, and not the
/// kernel either, which is what `Origin::Sub` of some other name means to the
/// plane policy.
const EXTERNAL: &str = "external";

/// Deep enough that no scenario's history is evicted before the probe is mounted
/// over it, so a short window is the binding's own cap and never the ring's.
const RING_DEPTH: u64 = 64;

/// Generous enough that no scenario meets a refusal: what a starved output does is
/// the budget suite's subject, not this one's.
const FILL_MT: u64 = 1_000_000_000;

/// The page-confined channel a port is bound to.
fn channel_for(port: &str) -> String {
    format!("local:probe/{port}")
}

/// One page-placed probe instance.
struct Surface {
    page: SurfacePage,
    /// The compiled artifact. Compiled once, instantiated per activation.
    kind: Kind,
    /// The operator config the probe reads — `tick_ms` where a scenario names one.
    config: BTreeMap<String, String>,
    /// Channel per port name, both directions.
    channels: HashMap<String, String>,
    /// The page's monotonic origin, which every `Millis` here is stated against.
    started: Instant,
    /// Highest report-channel sequence this adapter has already returned.
    read_through: u64,
    registered: bool,
    /// While set, `drain` reads reports without feeding [`Input::ReleaseDue`].
    releases_held: bool,
}

/// The attachment the page comes up under. No alert grant: the probe asks for
/// none.
fn facts() -> AttachmentFacts {
    AttachmentFacts {
        version: brenn_attach_proto::SUPPORTED_VERSIONS.max,
        participant_id: "surface:conf".to_string(),
        session_id: "s-conf".to_string(),
        heartbeat_secs: 20,
        max_body_bytes: 64 * 1024,
        max_frame_bytes: 256 * 1024,
        alert_granted: false,
    }
}

/// The wiring the spec describes, as a bindings document.
fn document(spec: &MountSpec) -> BindingsDocument {
    let mut subscriptions: Vec<Binding> = spec
        .inputs
        .iter()
        .map(|b| Binding {
            channel: channel_for(b.port),
            instance: PROBE.to_string(),
            port: b.port.to_string(),
            push_depth: u64::from(b.push_depth),
            retain_depth: u64::from(b.retain_depth),
            noise: NoiseLevel::Metered,
        })
        .collect();
    let mut outputs: Vec<OutputBinding> = [port::OUT, port::REPORT]
        .into_iter()
        .map(output_binding)
        .collect();
    if let Some(realm) = spec.tick {
        // TODO(backend-wasm-ephemeral-binding): the ephemeral variant of the
        // self-tick chain is a backend gap and is driven against the backend
        // adapter alone; the surface's durable realm is its confined store.
        assert_eq!(
            realm,
            Realm::Durable,
            "the surface's remount-surviving realm is the confined store",
        );
        subscriptions.push(Binding {
            channel: channel_for(port::TICK),
            instance: PROBE.to_string(),
            port: port::TICK.to_string(),
            push_depth: 4,
            retain_depth: 8,
            noise: NoiseLevel::Metered,
        });
        outputs.push(output_binding(port::TICK));
    }
    let local_channels: Vec<LocalChannel> = subscriptions
        .iter()
        .map(|b| b.channel.clone())
        .chain(outputs.iter().map(|b| b.channel.clone()))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|channel| LocalChannel {
            channel,
            ring_depth: RING_DEPTH,
        })
        .collect();
    let mut declared_out_ports: Vec<String> =
        outputs.iter().map(|b| b.port.clone()).collect::<Vec<_>>();
    declared_out_ports.sort();
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components: vec![
            ComponentEntry {
                instance: PROBE.to_string(),
                kind: "processor-transplant".to_string(),
                parked_batch_depth: 2,
                config: BTreeMap::new(),
                grants: ["ports", "log", "config"]
                    .iter()
                    .map(|g| (*g).to_string())
                    .collect(),
                declared_out_ports,
            },
            ComponentEntry {
                instance: CHROME.to_string(),
                kind: "chrome".to_string(),
                parked_batch_depth: 2,
                config: BTreeMap::new(),
                grants: vec!["ports".to_string()],
                declared_out_ports: vec![],
            },
        ],
        subscriptions,
        outputs,
        local_channels,
        chrome_instance: CHROME.to_string(),
        platform: PlatformSection {
            geometry_channel: "brenn:site.surface.conf.geometry".to_string(),
            status_channel: "brenn:site.surface.conf.status".to_string(),
            status_interval_secs: 60,
            error_channel: None,
            error_report_floor: None,
        },
    }
}

fn output_binding(port: &str) -> OutputBinding {
    OutputBinding {
        channel: channel_for(port),
        instance: PROBE.to_string(),
        port: port.to_string(),
        urgency: SchemaUrgency::Normal,
        fill_mt: FILL_MT,
        capacity_mt: FILL_MT,
    }
}

/// The wall clock, in the currency a release time is named in.
fn wall_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).expect("an instant after the epoch")
}

impl Surface {
    fn new(spec: &MountSpec) -> Surface {
        let document = document(spec);
        let mut channels: HashMap<String, String> = HashMap::new();
        for binding in &document.subscriptions {
            channels.insert(binding.port.clone(), binding.channel.clone());
        }
        for binding in &document.outputs {
            channels.insert(binding.port.clone(), binding.channel.clone());
        }

        let mut page = SurfacePage::new(CONFIG_CHANNEL.to_string(), uuid::Uuid::from_u128(0xc0f));
        page.on_attached(facts());
        page.subs
            .on_subscribe_result(CONFIG_CHANNEL, SubscribeOutcome::Ok, 1, None)
            .expect("the config channel is pending");
        page.apply_config(&document.to_body(), Millis(0))
            .expect("the adapter's document applies");

        let mut config = BTreeMap::new();
        if let Some(tick_ms) = spec.tick_ms {
            config.insert("tick_ms".to_string(), tick_ms.to_string());
        }

        Surface {
            page,
            kind: Kind::compile(
                std::path::Path::new(PROBE_WASM),
                &[
                    ComponentGrant::Ports,
                    ComponentGrant::Log,
                    ComponentGrant::Config,
                ],
            ),
            config,
            channels,
            started: Instant::now(),
            read_through: 0,
            registered: false,
            releases_held: false,
        }
    }

    /// The driver's monotonic reading.
    fn now(&self) -> Millis {
        Millis(u64::try_from(self.started.elapsed().as_millis()).expect("a page-lifetime uptime"))
    }

    fn feed(&mut self, input: Input) {
        let (now, now_ms) = (self.now(), wall_ms());
        turn::on_input(&mut self.page, input, now, now_ms);
    }

    /// Assemble and invoke until nothing is ready, exactly as the runner's
    /// activations arm does.
    fn run_ready(&mut self) {
        // Bounded rather than `while`: a page that never stops being ready is the
        // bug this adapter would otherwise hang the suite on.
        for _ in 0..64 {
            let (now, now_ms) = (self.now(), wall_ms());
            let (ready, _effects) = turn::dispatch(&mut self.page, now, now_ms);
            let Some(ready) = ready else { return };
            let done = invoke(&self.kind, &self.config, ready);
            self.feed(Input::ActivationDone(Box::new(done)));
        }
        panic!("the page never stopped being ready");
    }

    /// The reports the probe has published on its report channel since the last
    /// read.
    fn take_reports(&mut self) -> Vec<Report> {
        let channel = self.channels[port::REPORT].clone();
        let fresh: Vec<(u64, String)> = self
            .page
            .stores
            .get(&channel)
            .expect("the report channel is declared by the adapter's document")
            .retained()
            .filter(|(_, seq)| *seq > self.read_through)
            .map(|(envelope, seq)| (seq, envelope.body.clone()))
            .collect();
        if let Some((seq, _)) = fresh.last() {
            self.read_through = *seq;
        }
        fresh.iter().map(|(_, body)| Report::parse(body)).collect()
    }
}

/// Instantiate the probe, drive the activation at it, and answer the completion
/// the page is owed.
///
/// A fresh [`Page`] and a fresh instance per call: the harness records what the
/// guest asked of its ports, and the calls are replayed into the activation's own
/// [`PublishBuffer`], which is the quota authority and the thing the page flushes.
fn invoke(kind: &Kind, config: &BTreeMap<String, String>, ready: ReadyActivation) -> Completed {
    let ReadyActivation {
        instance,
        generation,
        activation,
        mut buffer,
        ..
    } = ready;
    let mut harness = kind.instantiate(Page::with_config(config.clone()));
    let answer = harness.receive(&to_wit(&activation));

    let outcome = match answer {
        Ok(Ok(reply)) => ActivationOutcome::Ok(reply),
        Ok(Err(error)) => ActivationOutcome::Err(ActivationError {
            // Both arms are one fact to the page — the activation returned err —
            // and the text is the component's own account of it, never parsed.
            message: match error {
                types::ReceiveError::MalformedEnvelope(detail) => {
                    format!("malformed envelope: {detail}")
                }
                types::ReceiveError::ProcessingFailed(detail) => detail,
            },
        }),
        Err(trap) => ActivationOutcome::Trap(format!("{trap:#}")),
    };
    // Replayed whatever the outcome: the page discards the buffer of a failed
    // activation itself, and the buffer is where a publish is *charged*, so an err
    // that spent budget spends it here too.
    let accepted = replay(harness.page(), &mut buffer);

    Completed {
        instance,
        generation,
        outcome,
        buffer,
        stamps: flush_stamps(accepted),
    }
}

/// Replay the recording page's port calls into the activation's buffer, and answer
/// how many the buffer accepted — the count of stamps the flush needs.
///
/// The two records keep their own call order and are replayed publishes-first. The
/// probe publishes its report and its markers before it parks anything, so the
/// interleaving the harness does not preserve is one no scenario here can observe;
/// a suite that needed it would record an ordered call log instead.
fn replay(page: &mut Page, buffer: &mut PublishBuffer) -> usize {
    let mut accepted = 0;
    for (port, body) in std::mem::take(&mut page.published) {
        buffer
            .publish(&port, body)
            .unwrap_or_else(|fault| panic!("the probe's publish on {port} was refused: {fault:?}"));
        accepted += 1;
    }
    for (port, body, deliver_after) in std::mem::take(&mut page.parked) {
        buffer
            .publish_deferred(&port, body, deliver_after)
            .unwrap_or_else(|fault| panic!("the probe's park on {port} was refused: {fault:?}"));
        accepted += 1;
    }
    accepted
}

/// The kernel's activation as the WIT world carries it. One shape, two spellings:
/// the kernel holds the contract crate's types and the guest is bound to the
/// generated ones.
fn to_wit(activation: &brenn_surface_contract::Activation) -> types::Activation {
    types::Activation {
        ports: activation
            .ports
            .iter()
            .map(|window| types::PortWindow {
                port: window.port.clone(),
                envelopes: window.envelopes.clone(),
                new_from: window.new_from,
                // Saturating: the carrier counts in u64 and the world types
                // this u32, and a saturated count still says "you lost more
                // than you can count".
                dropped: u32::try_from(window.dropped).unwrap_or(u32::MAX),
            })
            .collect(),
        deferred: activation
            .deferred
            .iter()
            .map(|window| types::DeferredWindow {
                port: window.port.clone(),
                entries: window
                    .entries
                    .iter()
                    .map(|entry| types::DeferredEntry {
                        index: entry.index,
                        payload: entry.payload.clone(),
                        deliver_after: entry.deliver_after,
                    })
                    .collect(),
            })
            .collect(),
        now: activation.now,
        sync: activation.sync.clone(),
    }
}

impl Host for Surface {
    fn trap_disposition(&self) -> TrapDisposition {
        TrapDisposition::Terminal
    }

    async fn mount(&mut self) {
        assert!(!self.registered, "the probe is already in service");
        self.registered = true;
        self.feed(Input::ActivationRegistered {
            instance: PROBE.to_string(),
        });
    }

    async fn publish(&mut self, port: &str, body: &str) {
        let channel = self
            .channels
            .get(port)
            .unwrap_or_else(|| panic!("scenario published onto unbound port {port:?}"))
            .clone();
        let SurfacePage { router, stores, .. } = &mut self.page;
        let outcome = router.route(
            stores,
            RouteRequest {
                channel: &channel,
                origin: Origin::Sub(EXTERNAL),
                body: body.to_string(),
                stamp: new_stamp(),
                urgency: Urgency::Normal,
                deliver_after: None,
            },
        );
        assert!(
            matches!(outcome, RouteOutcome::Routed { .. }),
            "an external publish onto {channel} was not routed",
        );
    }

    async fn remount_after(&mut self, idle: std::time::Duration) {
        assert!(self.registered, "remount before mount");
        self.feed(Input::ActivationDeregistered {
            instance: PROBE.to_string(),
        });
        // No `Input::ReleaseDue` across the span: on a live page the driver's
        // release deadline belongs to the page that went away, so an entry
        // coming due inside it waits for the next one.
        tokio::time::sleep(idle).await;
        self.feed(Input::ActivationRegistered {
            instance: PROBE.to_string(),
        });
    }

    fn hold_releases(&mut self, hold: bool) {
        self.releases_held = hold;
    }

    async fn drain(&mut self) -> Vec<Report> {
        // What the driver's release deadline does on a live page: whatever is due
        // enters retention now.
        if !self.releases_held {
            self.feed(Input::ReleaseDue);
        }
        self.run_ready();
        self.take_reports()
    }

    fn quiescent(&self) -> Option<bool> {
        // Held releases are this adapter's own state, not the page's: answering
        // `true` from them would let `settle` return before it had waited for
        // the reports a scenario asked for, on this host and not on the other,
        // and the difference would read as a host divergence in a suite whose
        // whole question is whether the two hosts agree. So say nothing, and
        // spend the quiet period as the backend adapter does.
        if self.releases_held {
            return None;
        }
        // `drain` ran the turn loop to exhaustion, so the only thing that can
        // still make this page ready without an external publish is a parked
        // message reaching its release time. No parked message, nothing further
        // — stated exactly, rather than inferred from a wall-clock silence.
        Some(self.page.stores.next_release().is_none())
    }
}

macro_rules! surface_scenario {
    ($name:ident) => {
        #[tokio::test]
        async fn $name() {
            let mut host = Surface::new(&scenarios::$name::spec());
            scenarios::$name::run(&mut host).await;
        }
    };
}

surface_scenario!(mount_over_empty_channels);
surface_scenario!(mount_over_history);
surface_scenario!(self_tick_chain);
surface_scenario!(sampled_only_wiring);
surface_scenario!(err_consumes);
surface_scenario!(trap_disposition);
surface_scenario!(state_does_not_survive);
surface_scenario!(remount);
surface_scenario!(due_tick_is_shown_at_mount);

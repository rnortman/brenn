//! Activation assembly, driven against real stores, a real registration table,
//! a real confined router and a real publish buffer — so what a component would
//! actually be handed is what the assertions read.

use brenn_attach_client::router::{MessageStamp, Origin, RouteOutcome, RouteRequest};
use brenn_attach_client::subs::Subscriptions;
use brenn_attach_proto::DeferredViewEntry;
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_surface_contract::PublishError;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{Binding, ComponentEntry, NoiseLevel, OutputBinding};

use crate::planes::SurfacePlanes;
use crate::registry::{new_stores, reconcile_stores};
use crate::test_support::bindings as fixtures;
use crate::test_support::bindings::{output, subscription};

use super::*;

const WIRE: &str = "brenn:site.bar.in";
const PEEK: &str = "brenn:site.bar.peek";
const OUT: &str = "ephemeral:site.bar.signal";
const LOCAL: &str = "local:app/notes";
const EPOCH: Uuid = Uuid::from_u128(0x5107);
const PRINCIPAL: &str = "surface:bar";

/// A component whose parked-batch depth holds more than one flush, so a suite
/// about assembly is never bounded by the outbox.
fn component(instance: &str) -> ComponentEntry {
    ComponentEntry {
        parked_batch_depth: 4,
        ..fixtures::component(instance)
    }
}

fn loud(binding: Binding, noise: NoiseLevel) -> Binding {
    Binding { noise, ..binding }
}

/// The standard wiring: `p1` reads one wire channel through two ports at
/// different depths and a page-local channel through a third, and publishes onto
/// one channel of each class; `p2` shares the wire channel and one output.
fn doc(subscriptions: Vec<Binding>, outputs: Vec<OutputBinding>) -> BindingsDocument {
    fixtures::doc(
        vec![
            component("p1"),
            component("p2"),
            component(fixtures::CHROME),
        ],
        subscriptions,
        outputs,
        vec![fixtures::local(LOCAL, 4)],
    )
}

fn standard_inputs() -> Vec<Binding> {
    vec![
        subscription("p1", "in", WIRE, 2, 1),
        subscription("p1", "aux", WIRE, 1, 0),
        subscription("p1", "notes", LOCAL, 1, 0),
        subscription("p2", "in", WIRE, 4, 0),
    ]
}

fn standard_outputs() -> Vec<OutputBinding> {
    vec![
        output("p1", "out", OUT),
        output("p1", "local", LOCAL),
        output("p2", "out", OUT),
    ]
}

fn applied(subscriptions: Vec<Binding>, outputs: Vec<OutputBinding>) -> AppliedBindings {
    AppliedBindings::apply(&doc(subscriptions, outputs).to_body())
        .expect("the fixture document applies")
}

fn standard() -> AppliedBindings {
    applied(standard_inputs(), standard_outputs())
}

fn env(channel: &str, body: &str) -> MessageEnvelope {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&(channel, body), &mut hasher);
    MessageEnvelope {
        message_id: Uuid::from_u128(u128::from(std::hash::Hasher::finish(&hasher))),
        source: "test".into(),
        channel: channel.into(),
        sender: "surface:bar#p1".into(),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
        body: body.into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Brenn,
    }
}

/// A page with the wiring in force, both components registered and scheduled, and
/// a confined router that has its identity.
struct Page {
    bindings: AppliedBindings,
    stores: SurfaceStores,
    subs: Subscriptions,
    registrations: Registrations,
    schedules: Schedules,
    router: LocalRouter<SurfacePlanes>,
    views: DeferredViews,
}

impl Page {
    fn new(bindings: AppliedBindings) -> Self {
        let mut stores = new_stores(EPOCH);
        let mut subs = Subscriptions::new();
        subs.go_live();
        let mut registrations = Registrations::new();
        let mut schedules = Schedules::new();
        reconcile_stores(&bindings, &mut stores);
        let mut planes = SurfacePlanes::new();
        planes.apply(&bindings);
        let mut router = LocalRouter::new(planes);
        router.set_principal(PRINCIPAL.to_string());
        for instance in ["p1", "p2"] {
            registrations.register(instance, Some(&bindings), &mut stores, &mut subs);
            schedules.track(instance, false);
        }
        Self {
            bindings,
            stores,
            subs,
            registrations,
            schedules,
            router,
            views: DeferredViews::new(),
        }
    }

    fn standard() -> Self {
        Self::new(standard())
    }

    fn arrive(&mut self, channel: &str, bodies: &[&str]) {
        for body in bodies {
            self.stores
                .get_mut(channel)
                .expect("the fixture hosts the channel")
                .insert(env(channel, body));
        }
    }

    fn ctx(&mut self, now_ms: u64) -> ActivationCtx<'_, SurfacePlanes> {
        ActivationCtx {
            bindings: &self.bindings,
            stores: &mut self.stores,
            router: &self.router,
            views: &self.views,
            max_body_bytes: 4_096,
            now_ms,
        }
    }

    fn assemble(&mut self, instance: &str) -> ReadyActivation {
        let mut ctx = ActivationCtx {
            bindings: &self.bindings,
            stores: &mut self.stores,
            router: &self.router,
            views: &self.views,
            max_body_bytes: 4_096,
            now_ms: 1_000,
        };
        // Zero for an instance this fixture never registered: the cases that do
        // that assert an assembly panic, which fires before the generation is read
        // by anything.
        let generation = self.registrations.generation(instance).unwrap_or(0);
        self.schedules.assemble(instance, generation, &mut ctx)
    }

    /// Park one message under `instance` on the confined channel, due at
    /// `release_at`.
    fn park(&mut self, instance: &str, body: &str, release_at: u64) -> Uuid {
        let message_id = Uuid::from_u128(u128::from(release_at) + 1);
        let outcome = self.router.route(
            &mut self.stores,
            RouteRequest {
                channel: LOCAL,
                origin: Origin::Sub(instance),
                body: body.to_string(),
                stamp: MessageStamp {
                    message_id,
                    publish_ts: chrono::DateTime::from_timestamp(0, 0)
                        .expect("a representable instant"),
                },
                urgency: Urgency::Normal,
                deliver_after: Some(release_at),
            },
        );
        assert!(
            matches!(outcome, RouteOutcome::Parked { .. }),
            "the fixture parks its message"
        );
        message_id
    }
}

fn ports(activation: &Activation) -> Vec<(&str, usize, u32, u64)> {
    activation
        .ports
        .iter()
        .map(|w| (w.port.as_str(), w.envelopes.len(), w.new_from, w.dropped))
        .collect()
}

fn bodies(activation: &Activation, port: &str) -> Vec<String> {
    activation
        .ports
        .iter()
        .find(|w| w.port == port)
        .expect("the activation windows the port")
        .envelopes
        .iter()
        .map(|e| e.body.clone())
        .collect()
}

#[test]
fn tracking_and_forgetting_an_instance() {
    let mut schedules = Schedules::new();
    assert!(schedules.is_empty());
    schedules.track("p1", false);
    assert!(schedules.is_tracked("p1"));
    assert_eq!(schedules.len(), 1);
    schedules.forget("p1");
    assert!(!schedules.is_tracked("p1"));
    assert!(schedules.is_empty());
}

#[test]
#[should_panic(expected = "already scheduled")]
fn tracking_an_instance_twice_panics() {
    let mut schedules = Schedules::new();
    schedules.track("p1", false);
    schedules.track("p1", false);
}

#[test]
#[should_panic(expected = "forgetting unscheduled instance")]
fn forgetting_an_untracked_instance_panics() {
    Schedules::new().forget("p1");
}

#[test]
fn an_untracked_instance_reports_zero_everywhere() {
    let schedules = Schedules::new();
    assert!(!schedules.in_flight("p1"));
    assert_eq!(schedules.activation_failures("p1"), 0);
    assert_eq!(schedules.metered_drops("p1", "in"), 0);
    assert_eq!(schedules.deferred_dropped("p1"), 0);
    assert_eq!(schedules.deferred_races("p1"), 0);
}

#[test]
fn the_deferral_counters_accumulate_and_ignore_a_stranger() {
    let mut schedules = Schedules::new();
    schedules.track("p1", false);
    schedules.count_deferred_drop("p1");
    schedules.count_deferred_drop("p1");
    schedules.count_deferred_race("p1");
    // A flush can outlive its registration, so a charge against an instance the
    // table no longer holds is absorbed rather than panicked.
    schedules.count_deferred_drop("gone");
    schedules.count_deferred_race("gone");
    assert_eq!(schedules.deferred_dropped("p1"), 2);
    assert_eq!(schedules.deferred_races("p1"), 1);
    assert_eq!(schedules.deferred_dropped("gone"), 0);
}

#[test]
fn a_forgotten_instance_takes_its_counters_with_it() {
    let mut schedules = Schedules::new();
    schedules.track("p1", false);
    schedules.count_deferred_race("p1");
    schedules.forget("p1");
    schedules.track("p1", false);
    assert_eq!(schedules.deferred_races("p1"), 0);
}

#[test]
fn nothing_is_ready_until_something_arrives() {
    let page = Page::standard();
    assert!(!page.schedules.has_ready(&page.registrations, &page.stores));
}

#[test]
fn an_owed_binding_makes_its_instance_ready() {
    let mut page = Page::standard();
    page.arrive(LOCAL, &["n1"]);
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p1"),
        "only p1 binds the confined channel"
    );
}

#[test]
fn ready_answers_in_instance_order() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p1")
    );
    // p1 taken out of the running: the next answer is its sibling, not a hash
    // seed's choice.
    page.assemble("p1");
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p2")
    );
}

#[test]
fn a_self_re_readying_instance_does_not_starve_its_sibling() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p1")
    );
    page.assemble("p1");
    page.schedules.finish_ok("p1", HashMap::new());
    // Both are owed again — p1 by the new message, p2 by both. A lowest-name pick
    // would hand p1 every activation forever, which is what an instance
    // republishing onto a channel it reads does to its siblings.
    page.arrive(WIRE, &["w2"]);
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p2")
    );
}

#[test]
fn the_dispatch_rotation_wraps_to_the_lowest_name() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    page.assemble("p2");
    page.schedules.finish_ok("p2", HashMap::new());
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p1"),
        "nothing sorts after p2, so the pass starts over at the front"
    );
}

#[test]
fn asking_twice_without_dispatching_answers_the_same_instance() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p1")
    );
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p1"),
        "the rotation advances at assembly, not at the question"
    );
}

#[test]
fn an_in_flight_instance_is_not_ready() {
    let mut page = Page::standard();
    page.arrive(LOCAL, &["n1"]);
    page.assemble("p1");
    page.arrive(LOCAL, &["n2"]);
    assert!(
        !page.schedules.has_ready(&page.registrations, &page.stores),
        "an arrival during a handler coalesces into the next activation"
    );
}

#[test]
fn a_terminal_instance_is_not_ready() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    assert!(page.registrations.fail("p1", &mut page.stores));
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p2"),
        "p1's positions went with its failure; p2's did not"
    );
}

#[test]
fn a_scheduled_but_deregistered_instance_is_not_ready() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    page.registrations
        .deregister("p2", &mut page.stores, &mut page.subs);
    assert_eq!(
        page.schedules.ready(&page.registrations, &page.stores),
        Some("p1")
    );
}

#[test]
fn every_bound_port_is_windowed_in_declaration_order() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    let ready = page.assemble("p1");
    assert_eq!(
        ports(&ready.activation),
        vec![
            ("in", 1, 0, 0),
            ("aux", 1, 0, 0),
            // Bound, nothing new: a pure-context window, which every port gets on
            // every activation.
            ("notes", 0, 0, 0),
        ]
    );
    assert_eq!(ready.instance, "p1");
    assert_eq!(ready.activation.now, Some(1_000));
}

/// `p1` reads one channel through a push-enabled port and a second through a
/// retain-only one — `push_depth: 0`, which is the mechanism of "never activate me"
/// rather than an optimization. The two channels are separate so the retain-only
/// binding is the only thing that could wake the instance on `PEEK`.
fn peek_page() -> Page {
    Page::new(applied(
        vec![
            subscription("p1", "in", WIRE, 2, 1),
            subscription("p1", "peek", PEEK, 0, 2),
        ],
        vec![output("p1", "out", OUT)],
    ))
}

#[test]
fn a_retain_only_binding_is_context_that_wakes_nobody_and_funds_nothing() {
    let mut page = peek_page();
    page.arrive(PEEK, &["a", "b", "c"]);
    assert!(
        !page.schedules.has_ready(&page.registrations, &page.stores),
        "a retain-only binding is owed nothing, however much arrives on its channel"
    );

    let mut ready = page.assemble("p1");
    let peek = &ready.activation.ports[1];
    assert_eq!(peek.port, "peek");
    assert_eq!(
        bodies(&ready.activation, "peek"),
        vec!["b", "c"],
        "the retained tail at the binding's own retain_depth"
    );
    assert_eq!(
        peek.new_from as usize,
        peek.envelopes.len(),
        "all of it is context: the window holds no position to advance"
    );
    assert_eq!(
        peek.dropped, 0,
        "a binding that is never delivered to is never reported against"
    );
    assert!(ready.drops.is_quiet());
    assert_eq!(page.schedules.metered_drops("p1", "peek"), 0);
    // And the grant is the fill alone, exactly as on an empty channel: retained
    // context funds no publish.
    assert!(ready.buffer.publish("out", "a".into()).is_ok());
    assert_eq!(
        ready.buffer.publish("out", "b".into()),
        Err(PublishError::QuotaExceeded)
    );
}

#[test]
fn each_port_reads_at_its_own_depths() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    page.assemble("p1");
    page.schedules.finish_ok("p1", HashMap::new());
    page.arrive(WIRE, &["w2"]);
    let ready = page.assemble("p1");
    // `in` is push 2 / retain 1: the new one, with one of context behind it.
    assert_eq!(bodies(&ready.activation, "in"), vec!["w1", "w2"]);
    assert_eq!(ready.activation.ports[0].new_from, 1);
    // `aux` is push 1 / retain 0: the new one and nothing behind it.
    assert_eq!(bodies(&ready.activation, "aux"), vec!["w2"]);
    assert_eq!(ready.activation.ports[1].new_from, 0);
}

#[test]
fn a_window_is_capped_at_the_deeper_of_the_two_knobs() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1", "w2", "w3"]);
    let ready = page.assemble("p1");
    // `in` is push 2 / retain 1, so its window is the newest two — and the one it
    // stepped over is its own accountable drop.
    assert_eq!(bodies(&ready.activation, "in"), vec!["w2", "w3"]);
    assert_eq!(ready.activation.ports[0].new_from, 0);
    assert_eq!(ready.activation.ports[0].dropped, 1);
    assert_eq!(bodies(&ready.activation, "aux"), vec!["w3"]);
    assert_eq!(ready.activation.ports[1].dropped, 2);
}

#[test]
fn the_window_advances_the_position_before_the_entry_runs() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    let first = page.assemble("p1");
    assert_eq!(first.activation.ports[0].new_envelopes().len(), 1);
    page.schedules.finish_ok("p1", HashMap::new());
    // Nothing new arrived, so the same message is only context now — the advance
    // happened at assembly, whatever the entry did with it.
    let second = page.assemble("p1");
    assert_eq!(bodies(&second.activation, "in"), vec!["w1"]);
    assert_eq!(second.activation.ports[0].new_from, 1);
    assert!(
        !page
            .stores
            .get(WIRE)
            .expect("the fixture hosts the channel")
            .has_deliverable(&BindingKey::new("p1", "in"))
    );
}

#[test]
fn assembling_marks_the_instance_in_flight() {
    let mut page = Page::standard();
    assert!(!page.schedules.in_flight("p1"));
    page.assemble("p1");
    assert!(page.schedules.in_flight("p1"));
    assert!(!page.schedules.in_flight("p2"));
}

#[test]
#[should_panic(expected = "already has an activation in flight")]
fn assembling_twice_for_one_instance_panics() {
    let mut page = Page::standard();
    page.assemble("p1");
    page.assemble("p1");
}

#[test]
#[should_panic(expected = "assembling for unscheduled")]
fn assembling_for_an_untracked_instance_panics() {
    let mut page = Page::standard();
    page.assemble("chrome");
}

/// The sibling invariant: a push-enabled port whose store is there but whose
/// position is not. Answering it with an empty window would leave the component
/// silently starved of a port it is bound to, so it is a panic.
#[test]
#[should_panic(expected = "holds no position")]
fn a_push_enabled_port_holding_no_position_panics() {
    let mut page = Page::standard();
    // Deregistration drops the positions and leaves the stores standing, and the
    // scheduler entry outlives it — which is exactly the broken pairing.
    page.registrations
        .deregister("p1", &mut page.stores, &mut page.subs);
    page.assemble("p1");
}

#[test]
#[should_panic(expected = "which has no store")]
fn a_bound_channel_with_no_store_panics() {
    let bindings = standard();
    let mut stores = new_stores(EPOCH);
    let mut schedules = Schedules::new();
    schedules.track("p1", false);
    let router = LocalRouter::new(SurfacePlanes::new());
    let views = DeferredViews::new();
    let mut ctx = ActivationCtx {
        bindings: &bindings,
        stores: &mut stores,
        router: &router,
        views: &views,
        max_body_bytes: 4_096,
        now_ms: 0,
    };
    schedules.assemble("p1", 0, &mut ctx);
}

// ── The loudness ladder ────────────────────────────────────────────────────

/// `p1` reads the wire channel one message at a time at `noise`, beside a deep
/// sibling that keeps the channel retaining four — so what `p1`'s window steps
/// over is still retained, which is the charge assembly accounts for.
fn ladder_page(noise: NoiseLevel) -> Page {
    Page::new(applied(
        vec![
            loud(subscription("p1", "in", WIRE, 1, 0), noise),
            subscription("p2", "deep", WIRE, 4, 0),
        ],
        vec![output("p1", "out", OUT)],
    ))
}

/// `p1` alone at `noise`, so the channel retains one message and an arrival
/// *evicts* what `p1` had not been served — the charge the retirement site
/// accounts for.
fn evicting_page(noise: NoiseLevel) -> Page {
    Page::new(applied(
        vec![loud(subscription("p1", "in", WIRE, 1, 0), noise)],
        vec![output("p1", "out", OUT)],
    ))
}

fn arrive(page: &mut Page, n: usize) {
    for i in 0..n {
        page.arrive(WIRE, &[&format!("w{i}")]);
    }
}

/// Arrivals plus the charge a caller enacts at the retirement site: an insert
/// that evicts what a position had not been served.
fn arrive_evicting(page: &mut Page, n: usize) -> DropVerdicts {
    let mut verdicts = DropVerdicts::default();
    for i in 0..n {
        let overflow = page
            .stores
            .get_mut(WIRE)
            .expect("the fixture hosts the channel")
            .insert(env(WIRE, &format!("w{i}")));
        verdicts.merge(
            page.schedules
                .charge_overflow(&page.bindings, WIRE, overflow),
        );
    }
    verdicts
}

#[test]
fn a_silent_binding_reports_its_drop_and_counts_nothing() {
    let mut page = ladder_page(NoiseLevel::Silent);
    arrive(&mut page, 3);
    let ready = page.assemble("p1");
    assert_eq!(ready.activation.ports[0].dropped, 2, "honestly reported");
    assert!(ready.drops.is_quiet());
    assert_eq!(page.schedules.metered_drops("p1", "in"), 0);
}

#[test]
fn a_metered_binding_counts_its_drops_and_announces_nothing() {
    let mut page = ladder_page(NoiseLevel::Metered);
    arrive(&mut page, 3);
    let ready = page.assemble("p1");
    assert!(ready.drops.is_quiet());
    assert_eq!(page.schedules.metered_drops("p1", "in"), 2);
    // Counters are lifetime: a second lag adds to the first.
    page.schedules.finish_ok("p1", HashMap::new());
    page.arrive(WIRE, &["x0", "x1"]);
    page.assemble("p1");
    assert_eq!(page.schedules.metered_drops("p1", "in"), 3);
}

#[test]
fn an_alarm_binding_announces_the_whole_delta_once() {
    let mut page = ladder_page(NoiseLevel::Alarm);
    arrive(&mut page, 4);
    let ready = page.assemble("p1");
    assert_eq!(
        ready.drops.announce,
        vec![DropAnnouncement {
            instance: "p1".to_string(),
            port: "in".to_string(),
            channel: WIRE.to_string(),
            dropped: 3,
        }],
        "one announcement per binding per activation, naming the coalesced delta"
    );
    assert!(ready.drops.fatal.is_empty());
    assert_eq!(page.schedules.metered_drops("p1", "in"), 3);
}

#[test]
fn the_announcement_names_the_binding_and_the_delta() {
    let announcement = DropAnnouncement {
        instance: "p1".to_string(),
        port: "in".to_string(),
        channel: WIRE.to_string(),
        dropped: 3,
    };
    assert_eq!(
        announcement.describe(),
        format!("p1: dropped 3 message(s) on port in ({WIRE}) — input overflow")
    );
}

#[test]
fn a_fatal_binding_asks_for_the_kill_and_announces_too() {
    let mut page = ladder_page(NoiseLevel::Fatal);
    arrive(&mut page, 2);
    let ready = page.assemble("p1");
    assert_eq!(
        ready.drops.fatal,
        vec![DropAnnouncement {
            instance: "p1".to_string(),
            port: "in".to_string(),
            channel: WIRE.to_string(),
            dropped: 1,
        }]
    );
    assert_eq!(ready.drops.announce.len(), 1, "the rungs are cumulative");
}

#[test]
fn two_fatal_bindings_ask_for_one_kill() {
    let mut page = Page::new(applied(
        vec![
            loud(subscription("p1", "in", WIRE, 1, 0), NoiseLevel::Fatal),
            loud(subscription("p1", "aux", WIRE, 1, 0), NoiseLevel::Fatal),
            subscription("p2", "deep", WIRE, 4, 0),
        ],
        vec![output("p1", "out", OUT)],
    ));
    arrive(&mut page, 3);
    let ready = page.assemble("p1");
    let [fatal] = &ready.drops.fatal[..] else {
        panic!("one kill for the one instance: {:?}", ready.drops)
    };
    assert_eq!(
        fatal.port, "in",
        "the first such binding, in declaration order"
    );
    assert_eq!(
        ready.drops.announce.len(),
        2,
        "each binding still announces its own loss"
    );
}

#[test]
fn a_binding_that_lost_nothing_charges_nothing() {
    let mut page = ladder_page(NoiseLevel::Fatal);
    arrive(&mut page, 1);
    let ready = page.assemble("p1");
    assert!(ready.drops.is_quiet());
    assert_eq!(ready.activation.ports[0].dropped, 0);
}

#[test]
fn an_eviction_is_counted_where_it_happens_and_announced_at_the_next_window() {
    let mut page = evicting_page(NoiseLevel::Alarm);
    let charged = arrive_evicting(&mut page, 3);
    assert!(
        charged.is_quiet(),
        "a softer rung's announcement waits for the binding's next window"
    );
    assert_eq!(
        page.schedules.metered_drops("p1", "in"),
        2,
        "on the books the instant it happened, run or not"
    );
    let ready = page.assemble("p1");
    assert_eq!(
        ready.drops.announce,
        vec![DropAnnouncement {
            instance: "p1".to_string(),
            port: "in".to_string(),
            channel: WIRE.to_string(),
            dropped: 2,
        }],
        "the whole delta, announced once"
    );
    assert_eq!(
        page.schedules.metered_drops("p1", "in"),
        2,
        "and counted once — the window excludes the span the eviction charged"
    );
}

#[test]
fn a_fatal_eviction_announces_at_the_retirement_site() {
    let mut page = evicting_page(NoiseLevel::Fatal);
    let charged = arrive_evicting(&mut page, 2);
    assert_eq!(
        charged.fatal,
        vec![DropAnnouncement {
            instance: "p1".to_string(),
            port: "in".to_string(),
            channel: WIRE.to_string(),
            dropped: 1,
        }],
        "the kill ends the instance, so there is no next window to wait for"
    );
    assert_eq!(charged.announce.len(), 1);
}

/// One retirement can overflow several of an instance's bindings at once, and the
/// store hands them over in no particular order. Both of the ladder's promises for
/// that case are here: the charges are answered in binding order, and the instance
/// dies once.
#[test]
fn a_retirement_charges_each_overflowed_binding_in_order_and_kills_once() {
    let mut page = Page::new(applied(
        vec![
            loud(subscription("p1", "in", WIRE, 1, 0), NoiseLevel::Fatal),
            loud(subscription("p1", "aux", WIRE, 1, 0), NoiseLevel::Fatal),
        ],
        vec![output("p1", "out", OUT)],
    ));
    // The channel retains one, so the second arrival evicts the first out from under
    // both positions.
    page.arrive(WIRE, &["w0"]);
    let overflow = page
        .stores
        .get_mut(WIRE)
        .expect("the fixture hosts the channel")
        .insert(env(WIRE, "w1"));

    let charged = page
        .schedules
        .charge_overflow(&page.bindings, WIRE, overflow);

    assert_eq!(
        charged
            .announce
            .iter()
            .map(|a| a.port.as_str())
            .collect::<Vec<_>>(),
        vec!["aux", "in"],
        "both bindings announce, in binding order"
    );
    let [kill] = &charged.fatal[..] else {
        panic!("one kill for the one instance: {:?}", charged)
    };
    assert_eq!(kill.port, "aux", "one kill, naming the first of them");
    assert_eq!(page.schedules.metered_drops("p1", "in"), 1);
    assert_eq!(page.schedules.metered_drops("p1", "aux"), 1);
}

#[test]
fn merging_verdicts_accumulates_announcements_and_keeps_one_kill_per_instance() {
    let announcement = |instance: &str, port: &str| DropAnnouncement {
        instance: instance.to_string(),
        port: port.to_string(),
        channel: WIRE.to_string(),
        dropped: 1,
    };
    let mut verdicts = DropVerdicts::default();
    verdicts.merge(DropVerdicts {
        announce: vec![announcement("p1", "in")],
        fatal: vec![announcement("p1", "in")],
    });
    verdicts.merge(DropVerdicts {
        announce: vec![announcement("p1", "aux")],
        fatal: vec![announcement("p1", "aux")],
    });
    assert_eq!(
        verdicts.announce,
        vec![announcement("p1", "in"), announcement("p1", "aux")]
    );
    assert_eq!(
        verdicts.fatal,
        vec![announcement("p1", "in")],
        "an instance dies once, for the binding that asked first"
    );

    // A second instance's kill is not the first one's: it was configured to die of
    // this loss too, and one retirement can evict both their positions.
    verdicts.merge(DropVerdicts {
        announce: vec![announcement("p2", "in")],
        fatal: vec![announcement("p2", "in")],
    });
    assert_eq!(
        verdicts.fatal,
        vec![announcement("p1", "in"), announcement("p2", "in")]
    );
}

/// One append can push retention past the positions of *several* instances, and each
/// of them was configured to die of the loss. The kill is a set, not a slot.
#[test]
fn a_retirement_evicting_two_instances_kills_both() {
    let mut page = Page::new(applied(
        vec![
            loud(subscription("p1", "in", WIRE, 1, 0), NoiseLevel::Fatal),
            loud(subscription("p2", "in", WIRE, 1, 0), NoiseLevel::Fatal),
        ],
        vec![output("p1", "out", OUT)],
    ));
    // The channel retains one, so the second arrival evicts the first out from under
    // both instances' positions at once.
    page.arrive(WIRE, &["w0"]);
    let overflow = page
        .stores
        .get_mut(WIRE)
        .expect("the fixture hosts the channel")
        .insert(env(WIRE, "w1"));

    let charged = page
        .schedules
        .charge_overflow(&page.bindings, WIRE, overflow);

    assert_eq!(
        charged
            .fatal
            .iter()
            .map(|kill| kill.instance.as_str())
            .collect::<Vec<_>>(),
        vec!["p1", "p2"],
        "one kill each, in binding order"
    );
}

#[test]
fn a_position_outliving_its_binding_is_not_charged() {
    let mut page = evicting_page(NoiseLevel::Fatal);
    // The wiring the charge resolves against no longer holds the binding — the
    // window a store trim and a position drop leave open.
    let unwired = applied(
        vec![subscription("p2", "deep", WIRE, 4, 0)],
        vec![output("p1", "out", OUT)],
    );
    let overflow = page
        .stores
        .get_mut(WIRE)
        .expect("the fixture hosts the channel")
        .insert(env(WIRE, "w0"));
    let charged = page.schedules.charge_overflow(&unwired, WIRE, overflow);
    assert!(charged.is_quiet());
    assert_eq!(page.schedules.metered_drops("p1", "in"), 0);
}

#[test]
fn every_bound_output_gets_a_window_in_declaration_order() {
    let mut page = Page::standard();
    let ready = page.assemble("p1");
    assert_eq!(
        ready
            .activation
            .deferred
            .iter()
            .map(|w| (w.port.as_str(), w.entries.len()))
            .collect::<Vec<_>>(),
        vec![("out", 0), ("local", 0)],
        "empty or not, so an index means the same thing on every activation"
    );
}

#[test]
fn a_transportable_output_reads_the_peers_mirror() {
    let mut page = Page::standard();
    page.views.on_view(
        OUT.to_string(),
        Some("p1".to_string()),
        vec![
            DeferredViewEntry {
                message_id: Uuid::from_u128(1),
                body: "later".into(),
                deliver_after: 5_000,
            },
            DeferredViewEntry {
                message_id: Uuid::from_u128(2),
                body: "later still".into(),
                deliver_after: 9_000,
            },
        ],
    );
    // A sibling's schedule on the same channel is nobody else's business.
    page.views.on_view(
        OUT.to_string(),
        Some("p2".to_string()),
        vec![DeferredViewEntry {
            message_id: Uuid::from_u128(3),
            body: "p2's".into(),
            deliver_after: 7_000,
        }],
    );
    let ready = page.assemble("p1");
    let window = &ready.activation.deferred[0];
    assert_eq!(window.port, "out");
    assert_eq!(
        window.entries,
        vec![
            DeferredEntry {
                index: 0,
                payload: "later".into(),
                deliver_after: 5_000,
            },
            DeferredEntry {
                index: 1,
                payload: "later still".into(),
                deliver_after: 9_000,
            },
        ]
    );
}

#[test]
fn a_confined_output_reads_the_pages_own_deferred_set() {
    let mut page = Page::standard();
    page.park("p1", "mine", 5_000);
    page.park("p2", "not mine", 6_000);
    let ready = page.assemble("p1");
    let window = &ready.activation.deferred[1];
    assert_eq!(window.port, "local");
    assert_eq!(
        window.entries,
        vec![DeferredEntry {
            index: 0,
            payload: "mine".into(),
            deliver_after: 5_000,
        }],
        "scoped to the instance's own sender, on either channel class"
    );
}

#[test]
fn a_confined_entry_already_due_is_out_of_the_window() {
    let mut page = Page::standard();
    page.park("p1", "due", 500);
    // Assembled at 1_000: the release time has arrived, so the schedule no longer
    // shows it even though the sweep has not taken it yet.
    let ready = page.assemble("p1");
    assert!(ready.activation.deferred[1].entries.is_empty());
}

#[test]
fn a_parked_message_is_resolvable_by_the_index_it_was_shown() {
    let mut page = Page::standard();
    let message_id = page.park("p1", "mine", 5_000);
    let mut ready = page.assemble("p1");
    ready
        .buffer
        .defer_cancel("local", 0)
        .expect("the index the window presented resolves");
    let flush = ready.buffer.take();
    assert_eq!(flush.defer_ops.len(), 1);
    assert_eq!(flush.defer_ops[0].message_id, message_id);
    assert_eq!(flush.defer_ops[0].channel, LOCAL);
}

#[test]
fn an_index_past_the_window_is_out_of_range() {
    let mut page = Page::standard();
    let mut ready = page.assemble("p1");
    assert!(ready.buffer.defer_cancel("local", 0).is_err());
}

#[test]
fn the_buffer_admits_the_instances_own_ports_only() {
    let mut page = Page::standard();
    let mut ready = page.assemble("p1");
    assert!(ready.buffer.publish("out", "hi".into()).is_ok());
    assert_eq!(
        ready.buffer.publish("nope", "hi".into()),
        Err(PublishError::NotPermitted)
    );
    // An input port is not a place to publish, and p2's ports are p2's.
    assert_eq!(
        ready.buffer.publish("in", "hi".into()),
        Err(PublishError::NotPermitted)
    );
    let flush = ready.buffer.take();
    assert_eq!(flush.publishes.len(), 1);
    assert_eq!(flush.publishes[0].channel, OUT);
    assert_eq!(flush.publishes[0].urgency, Urgency::Normal);
}

#[test]
fn the_grant_counts_new_envelopes_and_not_context() {
    let mut page = Page::standard();
    page.arrive(WIRE, &["w1"]);
    let mut ready = page.assemble("p1");
    // Fill 1000 plus a grant of 1000 per new envelope: `in` and `aux` each saw
    // w1, so three publishes are funded and the fourth is not.
    assert!(ready.buffer.publish("out", "a".into()).is_ok());
    assert!(ready.buffer.publish("out", "b".into()).is_ok());
    assert!(ready.buffer.publish("out", "c".into()).is_ok());
    assert_eq!(
        ready.buffer.publish("out", "d".into()),
        Err(PublishError::QuotaExceeded)
    );
    page.schedules.finish_ok("p1", ready.buffer.take().carry);

    // Second activation, nothing new: the fill alone, and the carry was spent.
    let mut second = page.assemble("p1");
    assert!(second.buffer.publish("out", "e".into()).is_ok());
    assert_eq!(
        second.buffer.publish("out", "f".into()),
        Err(PublishError::QuotaExceeded)
    );
}

#[test]
fn unspent_millitokens_carry_into_the_next_activation() {
    let mut page = Page::standard();
    let ready = page.assemble("p1");
    // Nothing published: the whole fill carries.
    page.schedules.finish_ok("p1", ready.buffer.take().carry);
    let mut second = page.assemble("p1");
    assert!(second.buffer.publish("out", "a".into()).is_ok());
    assert!(second.buffer.publish("out", "b".into()).is_ok());
    assert_eq!(
        second.buffer.publish("out", "c".into()),
        Err(PublishError::QuotaExceeded)
    );
}

#[test]
fn an_err_counts_the_failure_and_still_returns_the_carry() {
    let mut page = Page::standard();
    let mut ready = page.assemble("p1");
    ready
        .buffer
        .publish("out", "spent".into())
        .expect("the fill funds one publish");
    page.schedules.finish_err("p1", ready.buffer.into_carry());
    assert_eq!(page.schedules.activation_failures("p1"), 1);
    assert!(!page.schedules.in_flight("p1"));
    // What the component spent is a fact about the activation that happened: the
    // next one is funded by the fill alone.
    let mut second = page.assemble("p1");
    assert!(second.buffer.publish("out", "a".into()).is_ok());
    assert_eq!(
        second.buffer.publish("out", "b".into()),
        Err(PublishError::QuotaExceeded)
    );
}

#[test]
fn a_terminal_completion_clears_the_flight_and_keeps_the_counters() {
    let mut page = ladder_page(NoiseLevel::Metered);
    arrive(&mut page, 2);
    page.assemble("p1");
    page.schedules.count_deferred_race("p1");
    page.schedules.finish_terminal("p1");
    assert!(!page.schedules.in_flight("p1"));
    assert_eq!(page.schedules.metered_drops("p1", "in"), 1);
    assert_eq!(page.schedules.deferred_races("p1"), 1);
    assert_eq!(page.schedules.activation_failures("p1"), 0);
}

#[test]
#[should_panic(expected = "unscheduled instance")]
fn completing_for_an_untracked_instance_panics() {
    Schedules::new().finish_ok("p1", HashMap::new());
}

#[test]
fn the_buffer_carries_the_attachments_body_cap() {
    let mut page = Page::standard();
    let mut ctx = page.ctx(1_000);
    ctx.max_body_bytes = 8;
    let mut schedules = Schedules::new();
    schedules.track("p1", false);
    let mut ready = schedules.assemble("p1", 0, &mut ctx);
    assert_eq!(
        ready.buffer.publish("out", "far too long a body".into()),
        Err(PublishError::InvalidPayload)
    );
}

//! Activation delivery: rings, windows, batching, budgets, flush, parking.
//!
//! The design's client-core test block. These are the executable statements of
//! the backend-parity claims — most of all the recovery property, which is the
//! whole reason the per-message dialect's gap vocabulary can be deleted rather
//! than ported.

use super::*;
use brenn_surface_contract::{ActivationError, PublishError};
use brenn_surface_proto::{
    BatchEntry, LOCAL_THEME_CHANNEL, LOCAL_TOAST_CHANNEL, LocalChannel, PublishBatchOutcome,
};

/// The instance every fixture here registers.
const INST: &str = "protobar";

/// An envelope with a distinct `message_id`, which the shared `sample_envelope`
/// fixture cannot give (it pins one id, deliberately — every other suite asserts
/// exact envelopes). Identity is the whole subject of the ring's dedup and the
/// window's context/new split, so these tests must be able to tell two messages
/// apart.
fn env(body: &str, id: u128) -> MessageEnvelope {
    let mut e = sample_envelope(body);
    e.message_id = Uuid::from_u128(id);
    e
}

/// An input binding on `ephemeral:demo` at explicit depths.
fn binding(port: &str, push_depth: u64, retain_depth: u64) -> Binding {
    Binding {
        channel: "ephemeral:demo".into(),
        instance: INST.into(),
        port: port.into(),
        push_depth,
        retain_depth,
        noise: brenn_surface_proto::NoiseLevel::Silent,
    }
}

/// An input binding on `ephemeral:demo` at explicit depths and noise.
fn binding_noise(
    port: &str,
    push_depth: u64,
    retain_depth: u64,
    noise: brenn_surface_proto::NoiseLevel,
) -> Binding {
    Binding {
        noise,
        ..binding(port, push_depth, retain_depth)
    }
}

/// An input binding on a caller-named channel at explicit depths.
fn binding_on(channel: &str, port: &str, push_depth: u64, retain_depth: u64) -> Binding {
    Binding {
        channel: channel.into(),
        instance: INST.into(),
        port: port.into(),
        push_depth,
        retain_depth,
        noise: brenn_surface_proto::NoiseLevel::Silent,
    }
}

/// An output binding at the resolved default budget (one publish per activation,
/// one carried over).
fn output(port: &str, channel: &str) -> OutputBinding {
    output_budget(port, channel, TEST_FILL_MT, TEST_CAPACITY_MT)
}

fn output_budget(port: &str, channel: &str, fill_mt: u64, capacity_mt: u64) -> OutputBinding {
    OutputBinding {
        channel: channel.into(),
        instance: INST.into(),
        port: port.into(),
        urgency: Urgency::Normal,
        fill_mt,
        capacity_mt,
    }
}

/// A core `Active` with the given wiring and `INST` registered for activation
/// delivery, its subscription live.
fn registered_core(subscriptions: Vec<Binding>, outputs: Vec<OutputBinding>) -> ClientCore {
    let mut core = registered_core_unsubscribed(subscriptions, outputs);
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    core
}

/// As [`registered_core`] but without acking any subscription — for local-only
/// wiring, which has no wire subscription at all.
fn registered_core_unsubscribed(
    subscriptions: Vec<Binding>,
    outputs: Vec<OutputBinding>,
) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(crate::test_support::welcome_frame(subscriptions, outputs)),
        Millis(2),
    );
    register(&mut core, INST, Millis(5));
    core
}

/// Feed one `Deliver` on `ephemeral:demo` to `INST`'s subscription.
fn deliver(core: &mut ClientCore, envelope: &MessageEnvelope, seq: u64) -> Vec<Effect> {
    deliver_dropped(core, envelope, seq, 0)
}

fn deliver_dropped(
    core: &mut ClientCore,
    envelope: &MessageEnvelope,
    seq: u64,
    dropped: u64,
) -> Vec<Effect> {
    core.on_input(
        Input::TextFrame(deliver_frame_dropped(
            "ephemeral:demo",
            envelope,
            seq,
            dropped,
        )),
        Millis(10 + seq),
    )
}

/// Drop the link and come back on the same bindings, leaving `ephemeral:demo`
/// `Active` again — a transport blip, not a page reload, so every page-lifetime
/// structure must survive it.
fn reconnect(core: &mut ClientCore, subscriptions: Vec<Binding>, outputs: Vec<OutputBinding>) {
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(1_000),
    );
    core.on_input(Input::Tick, Millis(4_000));
    core.on_input(Input::Opened, Millis(4_001));
    core.on_input(
        Input::TextFrame(crate::test_support::welcome_frame(subscriptions, outputs)),
        Millis(4_002),
    );
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(4_003),
    );
}

/// An err outcome carrying the component's stated reason.
fn err(message: &str) -> ActivationOutcome {
    ActivationOutcome::Err(ActivationError {
        message: message.into(),
    })
}

/// The `PublishBatch` frames in an effect list.
fn batches(effects: &[Effect]) -> Vec<(String, u64, Vec<BatchEntry>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendFrame(ClientFrame::PublishBatch {
                instance,
                correlation,
                publishes,
            }) => Some((instance.clone(), *correlation, publishes.clone())),
            _ => None,
        })
        .collect()
}

// ── Window assembly ────────────────────────────────────────────────────────

/// Every bound input port is windowed on every activation, in config order,
/// whether or not it has anything new. A port with no new rows is a pure-context
/// window — the component reads its view, not its mail.
#[test]
fn every_bound_port_windows_every_activation_in_config_order() {
    let mut core = registered_core(
        vec![
            binding("alpha", 4, 0),
            binding_on("ephemeral:other", "beta", 4, 0),
            binding("gamma", 4, 0),
        ],
        vec![],
    );
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:other", SubscribeOutcome::Ok)),
        Millis(6),
    );
    deliver(&mut core, &env("m1", 1), 1);
    let ready = take_one(&mut core);
    let ports: Vec<&str> = ready
        .activation
        .ports
        .iter()
        .map(|w| w.port.as_str())
        .collect();
    assert_eq!(ports, vec!["alpha", "beta", "gamma"], "config order");
    // `alpha` and `gamma` share `ephemeral:demo`, so one delivery makes both new.
    assert_eq!(split(window(&ready.activation, "alpha")).1, vec!["m1"]);
    assert_eq!(split(window(&ready.activation, "gamma")).1, vec!["m1"]);
    // `beta`'s channel saw nothing: a pure-context window, and its context is
    // empty because its retain depth is 0.
    let beta = window(&ready.activation, "beta");
    assert!(beta.envelopes.is_empty());
    assert_eq!(beta.new_from, 0, "pure context: new_from == len");
}

/// Context comes from the subscription's ring at **this binding's own** depth,
/// even though the ring is folded to the max over the instance's bindings.
///
/// `retain_depth` bounds the whole *view*, not the context half: the ring is fed
/// before the window is assembled, so a new message occupies one of its own
/// binding's slots and the context is what is left under the depth. A depth-3
/// port with one new message therefore sees 2 context + 1 new — three messages,
/// which is what "a view three deep" means.
#[test]
fn each_binding_reads_context_at_its_own_depth_from_the_folded_ring() {
    let mut core = registered_core(
        vec![binding("deep", 4, 3), binding("shallow", 4, 1)],
        vec![],
    );
    for (i, body) in ["m1", "m2", "m3"].iter().enumerate() {
        deliver(&mut core, &env(body, i as u128 + 1), i as u64 + 1);
        let ready = take_one(&mut core);
        complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    }
    // The ring folded to 3 (the deeper binding), and now holds m2..m4.
    deliver(&mut core, &env("m4", 4), 4);
    let ready = take_one(&mut core);
    let deep = window(&ready.activation, "deep");
    assert_eq!(
        split(deep),
        (vec!["m2", "m3"], vec!["m4"]),
        "a 3-deep view: two retained, one new"
    );
    let shallow = window(&ready.activation, "shallow");
    assert_eq!(
        split(shallow),
        (vec![], vec!["m4"]),
        "a 1-deep view out of the same ring: the one message it can see is the new one"
    );
}

/// A message that is both retained and newly delivered appears **once**, after
/// the boundary. It is new — that is why the component was woken — and reporting
/// it as context too would tell the component it had already seen what it is
/// being woken for.
#[test]
fn context_is_deduped_by_message_id_against_the_new_rows() {
    let mut core = registered_core(vec![binding("in", 4, 4)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    let w = window(&ready.activation, "in");
    // Both are in the ring (it is fed by the same delivery that queued them), but
    // both are new, so context is empty rather than a duplicate of `new`.
    assert_eq!(split(w), (vec![], vec!["m1", "m2"]));
    assert_eq!(w.envelopes.len(), 2, "no message appears twice");
}

// ── The recovery property ──────────────────────────────────────────────────

/// **The executable backend-recovery property**, and the reason the per-message
/// dialect's gap vocabulary is deleted rather than ported.
///
/// `push_depth = 1`, `retain_depth = 2`, two deliveries in one dispatch turn:
/// the first is evicted from the pending queue by the second, and it is
/// nonetheless visible — as retained context, in the **same** activation, with
/// `dropped = 1`. Overflow retired the delivery obligation; it did not retire the
/// message. No gap event, no replay, no component-visible loss.
#[test]
fn a_message_dropped_from_the_pending_queue_is_context_in_the_same_activation() {
    let mut core = registered_core(vec![binding("in", 1, 2)], vec![]);
    deliver(&mut core, &env("evicted", 1), 1);
    deliver(&mut core, &env("survivor", 2), 2);
    let ready = take_one(&mut core);
    let w = window(&ready.activation, "in");
    assert_eq!(
        split(w),
        (vec!["evicted"], vec!["survivor"]),
        "the evicted message is still in the view, as context"
    );
    assert_eq!(w.dropped, 1, "and the drop is reported honestly");
}

/// Ring displacement is retention, not push overflow: a message that falls out of
/// the ring is simply no longer in the view, and no drop counter moves for it.
#[test]
fn ring_displacement_is_not_a_drop() {
    let mut core = registered_core(vec![binding("in", 4, 1)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 0);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    let w = window(&ready.activation, "in");
    // `m1` fell out of the depth-1 ring — gone from the view, but nothing was
    // dropped: the queue delivered it.
    assert_eq!(split(w), (vec![], vec!["m2"]));
    assert_eq!(w.dropped, 0);
}

// ── Batching and serialization ─────────────────────────────────────────────

/// N deliveries before a dispatch coalesce into **one** activation carrying all
/// of them. This is the batching, and it is why no per-message effect exists.
#[test]
fn deliveries_before_dispatch_coalesce_into_one_activation() {
    let mut core = registered_core(vec![binding("in", 8, 0)], vec![]);
    for i in 1..=4u64 {
        let effects = deliver(&mut core, &env(&format!("m{i}"), i as u128), i);
        assert_eq!(
            effects,
            vec![Effect::SetWakeup(Some(Millis(60_010 + i)))],
            "no per-message effect: delivery {i} batches instead"
        );
    }
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")).1,
        vec!["m1", "m2", "m3", "m4"]
    );
}

/// Deliveries arriving while a handler is in flight do not overlap it: they
/// coalesce into exactly one follow-up activation, which appears only once the
/// first completes.
#[test]
fn deliveries_during_an_in_flight_handler_become_one_follow_up_activation() {
    let mut core = registered_core(vec![binding("in", 8, 0)], vec![]);
    deliver(&mut core, &env("first", 1), 1);
    let ready = take_one(&mut core);
    // In flight: two more arrive.
    deliver(&mut core, &env("during-a", 2), 2);
    deliver(&mut core, &env("during-b", 3), 3);
    assert!(
        core.take_ready_activation().is_none(),
        "invocations never overlap for one instance"
    );
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")).1,
        vec!["during-a", "during-b"],
        "everything that arrived during the handler, in one activation"
    );
}

/// The dispatch pick rotates through the ready set. A stable order alone is not
/// enough: an instance that re-readies itself synchronously — which is what a
/// component republishing onto a `local:` channel it reads does on every flush —
/// would take every activation forever under a lowest-name-wins pick, and no
/// sibling would run again.
#[test]
fn the_dispatch_pick_rotates_so_a_self_feeding_instance_cannot_starve_a_sibling() {
    let sibling = Binding {
        channel: "ephemeral:demo".into(),
        // Sorts after `protobar`, so a lowest-name-wins pick would never reach it.
        instance: "zz-sibling".into(),
        port: "in".into(),
        push_depth: 4,
        retain_depth: 0,
        noise: brenn_surface_proto::NoiseLevel::Silent,
    };
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(crate::test_support::welcome_frame(
            vec![binding("in", 4, 0), sibling],
            vec![],
        )),
        Millis(2),
    );
    register(&mut core, INST, Millis(5));
    register(&mut core, "zz-sibling", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    core.on_input(
        Input::TextFrame(subscribe_result_for(
            "ephemeral:demo",
            "zz-sibling",
            SubscribeOutcome::Ok,
        )),
        Millis(6),
    );
    // Keep both permanently ready — a message for each before every dispatch, so
    // whichever one just ran is ready again immediately. That is the shape a
    // `local:` republisher creates for itself, and the reason the pick cannot be
    // "lowest name wins". Without rotation every dispatch below is `protobar` and
    // `zz-sibling` never runs at all.
    let mut order = Vec::new();
    for i in 1..=4u128 {
        deliver(&mut core, &env(&format!("m{i}"), i), i as u64);
        core.on_input(
            Input::TextFrame(deliver_frame_for(
                "ephemeral:demo",
                "zz-sibling",
                &env(&format!("s{i}"), 100 + i),
                i as u64,
            )),
            Millis(10 + i as u64),
        );
        let ready = core
            .take_ready_activation()
            .expect("both instances are ready");
        order.push(ready.instance.clone());
        complete(
            &mut core,
            &ready.instance,
            ActivationOutcome::Ok,
            ready.buffer,
        );
    }
    assert_eq!(
        order,
        vec!["protobar", "zz-sibling", "protobar", "zz-sibling"],
        "the pick rotates through the ready set rather than pinning its first member"
    );
}

/// Two registered instances are scheduled independently: one in flight does not
/// hold the other back, and each windows only its own subscription's messages.
#[test]
fn two_registered_instances_activate_independently() {
    let sibling = Binding {
        channel: "ephemeral:demo".into(),
        instance: "sibling".into(),
        port: "in".into(),
        push_depth: 4,
        retain_depth: 0,
        noise: brenn_surface_proto::NoiseLevel::Silent,
    };
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(crate::test_support::welcome_frame(
            vec![binding("in", 4, 0), sibling],
            vec![],
        )),
        Millis(2),
    );
    register(&mut core, INST, Millis(5));
    register(&mut core, "sibling", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    core.on_input(
        Input::TextFrame(subscribe_result_for(
            "ephemeral:demo",
            "sibling",
            SubscribeOutcome::Ok,
        )),
        Millis(6),
    );
    // A Deliver is a *subscription's*, so each instance is fed its own.
    deliver(&mut core, &env("m1", 1), 1);
    core.on_input(
        Input::TextFrame(deliver_frame_for(
            "ephemeral:demo",
            "sibling",
            &env("m1", 1),
            1,
        )),
        Millis(11),
    );
    let first = core.take_ready_activation().expect("one is ready");
    let second = core
        .take_ready_activation()
        .expect("the other is ready too, independently");
    let mut names = vec![first.instance.clone(), second.instance.clone()];
    names.sort();
    assert_eq!(names, vec!["protobar", "sibling"]);
}

// ── Ack semantics ──────────────────────────────────────────────────────────

/// An err consumes: the messages the failed activation was assembled with are
/// gone from the queue and never re-window as new. Recovery is retention, not
/// redelivery.
#[test]
fn err_consumes_the_messages_it_was_activated_for() {
    let mut core = registered_core(vec![binding("in", 4, 4)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    let ready = take_one(&mut core);
    let effects = complete(&mut core, INST, err("bad row"), ready.buffer);
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitEvent(Event::ActivationFailed { instance, .. })] if instance == INST
    ));
    assert!(
        core.take_ready_activation().is_none(),
        "the consumed message does not re-activate the instance"
    );
    // It reappears only as context, on the next activation something else causes.
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")),
        (vec!["m1"], vec!["m2"]),
        "the failed activation's message is context now, never new again"
    );
}

/// Drop deltas advance at ack, not at completion: each window reports the drops
/// since the previous activation consumed the queue, and never re-reports them.
#[test]
fn drop_deltas_advance_at_ack_and_are_never_double_reported() {
    let mut core = registered_core(vec![binding("in", 1, 4)], vec![]);
    deliver(&mut core, &env("a", 1), 1);
    deliver(&mut core, &env("b", 2), 2);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 1);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    deliver(&mut core, &env("c", 3), 3);
    let ready = take_one(&mut core);
    assert_eq!(
        window(&ready.activation, "in").dropped,
        0,
        "the earlier drop was already reported; this window reports its own delta"
    );
}

/// A server-reported subscription drop is every binding's drop: each of them
/// missed those messages.
#[test]
fn server_reported_drops_count_against_every_binding_on_the_subscription() {
    let mut core = registered_core(vec![binding("one", 4, 0), binding("two", 4, 0)], vec![]);
    deliver_dropped(&mut core, &env("m1", 1), 1, 3);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "one").dropped, 3);
    assert_eq!(window(&ready.activation, "two").dropped, 3);
}

// ── Loudness ladder: metered counters ──────────────────────────────────────

use brenn_surface_proto::NoiseLevel;

/// The `metered` rung counts a pending-queue overflow (drop-oldest): the drop the
/// window reports at assembly advances the binding's kernel-internal counter.
#[test]
fn metered_binding_counts_pending_queue_overflow() {
    let mut core = registered_core(vec![binding_noise("in", 1, 4, NoiseLevel::Metered)], vec![]);
    deliver(&mut core, &env("a", 1), 1);
    deliver(&mut core, &env("b", 2), 2);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 1);
    assert_eq!(core.metered_drop_count(INST, "in"), 1);
}

/// A `silent` binding is never counted, even though the drop is still reported
/// honestly on the window — the counter is a rung, not the drop accounting.
#[test]
fn silent_binding_is_uncounted() {
    let mut core = registered_core(vec![binding_noise("in", 1, 4, NoiseLevel::Silent)], vec![]);
    deliver(&mut core, &env("a", 1), 1);
    deliver(&mut core, &env("b", 2), 2);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 1);
    assert_eq!(core.metered_drop_count(INST, "in"), 0);
}

/// The `metered` rung counts the other drop origin too: a server-reported
/// subscription drop delta advances the same counter as a kernel-queue overflow.
#[test]
fn metered_binding_counts_server_reported_delta() {
    let mut core = registered_core(vec![binding_noise("in", 4, 0, NoiseLevel::Alarm)], vec![]);
    deliver_dropped(&mut core, &env("m1", 1), 1, 3);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 3);
    // `Alarm` is louder than `Metered`, so the metered half (counting) fires:
    // the ladder is cumulative.
    assert_eq!(core.metered_drop_count(INST, "in"), 3);
}

/// The counter is lifetime and additive across activations, and reports its own
/// delta each time (no double count).
#[test]
fn metered_counter_accumulates_across_activations() {
    let mut core = registered_core(vec![binding_noise("in", 1, 4, NoiseLevel::Metered)], vec![]);
    deliver(&mut core, &env("a", 1), 1);
    deliver(&mut core, &env("b", 2), 2);
    let ready = take_one(&mut core);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    assert_eq!(core.metered_drop_count(INST, "in"), 1);
    deliver(&mut core, &env("c", 3), 3);
    deliver(&mut core, &env("d", 4), 4);
    let ready = take_one(&mut core);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    assert_eq!(core.metered_drop_count(INST, "in"), 2);
}

// ── Loudness ladder: alarm and fatal ─────────────────────────────────────────

use brenn_surface_proto::{AlertSeverity, ToastBody, ToastSeverity, ToastSource};

/// A registered single-instance core on `channel` at the given noise, holding the
/// alert grant — which the boot check proves present for any `alarm`/`fatal`
/// binding, so a faithful ladder fixture has it.
fn ladder_core(channel: &str, noise: NoiseLevel, push_depth: u64) -> ClientCore {
    let binding = Binding {
        channel: channel.into(),
        instance: INST.into(),
        port: "in".into(),
        push_depth,
        retain_depth: 4,
        noise,
    };
    let welcome =
        brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
            subscriptions: vec![binding],
            components: vec![INST],
            alert_granted: true,
            ..Default::default()
        });
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(Input::TextFrame(welcome), Millis(2));
    register(&mut core, INST, Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result(channel, SubscribeOutcome::Ok)),
        Millis(6),
    );
    core
}

/// A ladder core with two bindings on one instance, each on its own channel and
/// port, so per-*binding* behavior can be told apart from per-*instance*.
fn two_binding_ladder_core(
    a: (&str, &str, NoiseLevel, u64),
    b: (&str, &str, NoiseLevel, u64),
) -> ClientCore {
    let mk = |(channel, port, noise, push_depth): (&str, &str, NoiseLevel, u64)| Binding {
        channel: channel.into(),
        instance: INST.into(),
        port: port.into(),
        push_depth,
        retain_depth: 4,
        noise,
    };
    let welcome =
        brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
            subscriptions: vec![mk(a), mk(b)],
            components: vec![INST],
            alert_granted: true,
            ..Default::default()
        });
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(Input::TextFrame(welcome), Millis(2));
    register(&mut core, INST, Millis(5));
    for channel in [a.0, b.0] {
        core.on_input(
            Input::TextFrame(subscribe_result(channel, SubscribeOutcome::Ok)),
            Millis(6),
        );
    }
    core
}

/// The contract is one coalesced alert + toast **per binding** per activation,
/// and metered counters keyed **per port**. Every other ladder test uses a single
/// binding, which cannot distinguish that from per-instance coalescing or from a
/// counter map keyed by something other than the port. Two `alarm` bindings
/// overflowing on the same activation pin both.
#[test]
fn alarm_coalesces_per_binding_not_per_instance() {
    let mut core = two_binding_ladder_core(
        ("ephemeral:demo", "in", NoiseLevel::Alarm, 4),
        ("ephemeral:alt", "alt", NoiseLevel::Alarm, 4),
    );
    // Distinct deltas, so a merged counter is visible as a merged number.
    deliver_ch(&mut core, "ephemeral:demo", &env("a", 1), 1, 2);
    deliver_ch(&mut core, "ephemeral:alt", &env("b", 2), 2, 5);
    let ready = take_one(&mut core);

    assert_eq!(window(&ready.activation, "in").dropped, 2);
    assert_eq!(window(&ready.activation, "alt").dropped, 5);

    // Counters stay separated by port.
    assert_eq!(core.metered_drop_count(INST, "in"), 2);
    assert_eq!(core.metered_drop_count(INST, "alt"), 5);

    // One alert and one toast per overflowing binding, each naming its own port
    // and its own delta.
    let alerts = alerts(&ready.effects);
    assert_eq!(alerts.len(), 2, "one alert per binding: {alerts:?}");
    assert!(
        alerts
            .iter()
            .any(|(_, _, body)| body.contains("in") && body.contains("dropped 2")),
        "the `in` binding's alert names its own delta: {alerts:?}"
    );
    assert!(
        alerts
            .iter()
            .any(|(_, _, body)| body.contains("alt") && body.contains("dropped 5")),
        "the `alt` binding's alert names its own delta: {alerts:?}"
    );

    let toasts = toasts(&ready.effects);
    assert_eq!(toasts.len(), 2, "one toast per binding: {toasts:?}");
    assert!(toasts.iter().any(|t| t.text.contains("dropped 2")));
    assert!(toasts.iter().any(|t| t.text.contains("dropped 5")));
}

/// Feed one `Deliver` with a caller-named channel and drop count.
fn deliver_ch(
    core: &mut ClientCore,
    channel: &str,
    envelope: &MessageEnvelope,
    seq: u64,
    dropped: u64,
) -> Vec<Effect> {
    core.on_input(
        Input::TextFrame(deliver_frame_dropped(channel, envelope, seq, dropped)),
        Millis(10 + seq),
    )
}

/// The `Alert` frames in an effect list, as (severity, title, body).
fn alerts(effects: &[Effect]) -> Vec<(AlertSeverity, String, String)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendFrame(ClientFrame::Alert {
                severity,
                title,
                body,
            }) => Some((*severity, title.clone(), body.clone())),
            _ => None,
        })
        .collect()
}

/// The decoded `local:brenn/toast` bodies in an effect list.
fn toasts(effects: &[Effect]) -> Vec<ToastBody> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::PublishControl { channel, body } if channel == LOCAL_TOAST_CHANNEL => {
                Some(serde_json::from_str(body).expect("a kernel toast body decodes"))
            }
            _ => None,
        })
        .collect()
}

/// The `InstanceFailed` (instance, reason) pairs in an effect list.
fn instance_failures(effects: &[Effect]) -> Vec<(String, String)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::EmitEvent(Event::InstanceFailed { instance, reason }) => {
                Some((instance.clone(), reason.clone()))
            }
            _ => None,
        })
        .collect()
}

/// `alarm` on a pending-queue overflow: the cumulative rung counts, then raises
/// exactly one backend `Alert` (severity `Warning`) and one coalesced toast,
/// both naming the delta. The instance is not killed.
#[test]
fn alarm_binding_alerts_and_toasts_on_pending_queue_overflow() {
    let mut core = ladder_core("ephemeral:demo", NoiseLevel::Alarm, 1);
    deliver_ch(&mut core, "ephemeral:demo", &env("a", 1), 1, 0);
    deliver_ch(&mut core, "ephemeral:demo", &env("b", 2), 2, 0);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 1);
    assert_eq!(core.metered_drop_count(INST, "in"), 1);

    let alerts = alerts(&ready.effects);
    assert_eq!(alerts.len(), 1, "one alert: {:?}", ready.effects);
    assert_eq!(alerts[0].0, AlertSeverity::Warning);
    assert!(
        alerts[0].2.contains("dropped 1"),
        "names the delta: {}",
        alerts[0].2
    );

    let toasts = toasts(&ready.effects);
    assert_eq!(toasts.len(), 1, "one coalesced toast: {:?}", ready.effects);
    assert_eq!(toasts[0].severity, ToastSeverity::Warning);
    assert_eq!(toasts[0].source, ToastSource::Kernel);
    assert!(toasts[0].text.contains("dropped 1"));

    assert!(instance_failures(&ready.effects).is_empty());
    assert!(!core.is_failed(INST));
}

/// `alarm` fires on the other drop origin too: a server-reported subscription
/// delta raises the same alert and toast as a kernel-queue overflow.
#[test]
fn alarm_binding_alerts_on_server_reported_delta() {
    let mut core = ladder_core("ephemeral:demo", NoiseLevel::Alarm, 4);
    deliver_ch(&mut core, "ephemeral:demo", &env("m1", 1), 1, 3);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 3);
    assert_eq!(alerts(&ready.effects).len(), 1);
    let toasts = toasts(&ready.effects);
    assert_eq!(toasts.len(), 1);
    assert!(toasts[0].text.contains("dropped 3"));
    assert!(!core.is_failed(INST));
}

/// `fatal` is cumulative — it still alerts and toasts — and then kills the
/// instance via the trap-terminal path: `InstanceFailed` naming the binding and
/// the overflow, `is_failed`, and no further activation on new traffic.
#[test]
fn fatal_binding_kills_the_instance_and_stays_terminal() {
    let mut core = ladder_core("ephemeral:demo", NoiseLevel::Fatal, 1);
    deliver_ch(&mut core, "ephemeral:demo", &env("a", 1), 1, 0);
    deliver_ch(&mut core, "ephemeral:demo", &env("b", 2), 2, 0);
    let ready = take_one(&mut core);

    assert_eq!(
        alerts(&ready.effects).len(),
        1,
        "fatal is cumulative: alerts"
    );
    assert_eq!(
        toasts(&ready.effects).len(),
        1,
        "fatal is cumulative: toasts"
    );

    let failures = instance_failures(&ready.effects);
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, INST);
    assert!(
        failures[0].1.contains("fatal"),
        "reason names the rung: {}",
        failures[0].1
    );
    assert!(
        failures[0].1.contains("in"),
        "reason names the port: {}",
        failures[0].1
    );

    assert!(core.is_failed(INST));
    // Terminal: new traffic never re-activates the killed instance.
    deliver_ch(&mut core, "ephemeral:demo", &env("c", 3), 3, 0);
    assert!(
        core.take_ready_activation().is_none(),
        "a killed instance never activates again"
    );
}

/// The maxim, executable: the ladder runs identically over a `brenn:`-shaped and
/// an `ephemeral:`-shaped delivery. The only difference is the channel string in
/// the human text; the enacted shape (drop count, alert count, toast count and
/// severity) is identical.
#[test]
fn the_ladder_is_class_blind_over_brenn_and_ephemeral() {
    let run = |channel: &str| {
        let mut core = ladder_core(channel, NoiseLevel::Alarm, 1);
        deliver_ch(&mut core, channel, &env("a", 1), 1, 0);
        deliver_ch(&mut core, channel, &env("b", 2), 2, 0);
        let ready = take_one(&mut core);
        (
            window(&ready.activation, "in").dropped,
            alerts(&ready.effects).len(),
            toasts(&ready.effects).len(),
            toasts(&ready.effects)[0].severity,
        )
    };
    assert_eq!(run("ephemeral:demo"), run("brenn:demo"));
}

/// A `fatal` overflow on one instance kills only that instance: a sibling on its
/// own binding keeps activating and delivering. Per-instance containment, pinned
/// for the fatal trigger specifically.
#[test]
fn fatal_kill_leaves_a_sibling_delivering() {
    const SIB: &str = "sib";
    let doomed = Binding {
        channel: "ephemeral:demo".into(),
        instance: INST.into(),
        port: "in".into(),
        push_depth: 1,
        retain_depth: 4,
        noise: NoiseLevel::Fatal,
    };
    let sibling = Binding {
        channel: "ephemeral:sib".into(),
        instance: SIB.into(),
        port: "in".into(),
        push_depth: 4,
        retain_depth: 4,
        noise: NoiseLevel::Silent,
    };
    let welcome =
        brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
            subscriptions: vec![doomed, sibling],
            components: vec![INST, SIB],
            alert_granted: true,
            ..Default::default()
        });
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(Input::TextFrame(welcome), Millis(2));
    register(&mut core, INST, Millis(5));
    register(&mut core, SIB, Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result_for(
            "ephemeral:demo",
            INST,
            SubscribeOutcome::Ok,
        )),
        Millis(6),
    );
    core.on_input(
        Input::TextFrame(subscribe_result_for(
            "ephemeral:sib",
            SIB,
            SubscribeOutcome::Ok,
        )),
        Millis(7),
    );

    // Overflow the doomed instance's queue and deliver one to the sibling.
    core.on_input(
        Input::TextFrame(deliver_frame_for_dropped(
            "ephemeral:demo",
            INST,
            &env("a", 1),
            1,
            0,
        )),
        Millis(20),
    );
    core.on_input(
        Input::TextFrame(deliver_frame_for_dropped(
            "ephemeral:demo",
            INST,
            &env("b", 2),
            2,
            0,
        )),
        Millis(21),
    );
    core.on_input(
        Input::TextFrame(deliver_frame_for_dropped(
            "ephemeral:sib",
            SIB,
            &env("s", 3),
            1,
            0,
        )),
        Millis(22),
    );

    // Drain both ready activations: the doomed one is killed at assembly, the
    // sibling activates normally.
    let mut failed = None;
    let mut sibling_windowed = false;
    while let Some(ready) = core.take_ready_activation() {
        if ready.instance == INST {
            failed = Some(ready);
        } else {
            assert_eq!(ready.instance, SIB);
            assert_eq!(
                window(&ready.activation, "in")
                    .envelopes
                    .last()
                    .unwrap()
                    .body,
                "s"
            );
            sibling_windowed = true;
        }
    }
    let failed = failed.expect("the doomed instance was dispatched");
    assert_eq!(instance_failures(&failed.effects).len(), 1);
    assert!(core.is_failed(INST));
    assert!(!core.is_failed(SIB), "the sibling is untouched");
    assert!(sibling_windowed, "the sibling still delivered");
}

// ── Flush ──────────────────────────────────────────────────────────────────

/// An ok activation's wire publishes flush as **one** `PublishBatch`, in call
/// order, carrying the raw urgency override (or nothing, leaving the server's
/// resolved default to win).
#[test]
fn ok_flushes_one_batch_in_call_order() {
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output("out", "ephemeral:sink")],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "first".into()).unwrap();
    ready
        .buffer
        .publish_with_urgency("out", "second".into(), Urgency::High)
        .unwrap();
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    let sent = batches(&effects);
    assert_eq!(sent.len(), 1, "one activation, one batch");
    let (instance, _correlation, entries) = &sent[0];
    assert_eq!(instance, INST);
    assert_eq!(
        entries,
        &vec![
            BatchEntry {
                port: "out".into(),
                body: "first".into(),
                urgency: None,
            },
            BatchEntry {
                port: "out".into(),
                body: "second".into(),
                urgency: Some(Urgency::High),
            },
        ]
    );
}

/// `local:` entries commit through the router at the flush point — seq assigned,
/// ring fed, fan-out — and never ride the wire.
#[test]
fn ok_routes_local_entries_through_the_router() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(crate::test_support::welcome_frame_local(
            vec![binding("in", 4, 0)],
            vec![output("theme", LOCAL_THEME_CHANNEL)],
            vec![LocalChannel {
                channel: LOCAL_THEME_CHANNEL.into(),
                ring_depth: 1,
            }],
        )),
        Millis(2),
    );
    register(&mut core, INST, Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    ready
        .buffer
        .publish("theme", "{\"v\":1,\"theme\":\"dark\"}".into())
        .unwrap();
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    assert!(
        batches(&effects).is_empty(),
        "local traffic never rides the wire"
    );
    // Committed through the router at the flush point: seq assigned, ring fed —
    // the depth-1 plane holds it for whatever attaches later.
    let ring = core
        .local_rings
        .get(LOCAL_THEME_CHANNEL)
        .expect("the reserved plane's ring");
    let replayed: Vec<(String, String)> = ring
        .ring
        .entries()
        .map(|(e, _)| (e.body.clone(), e.sender.clone()))
        .collect();
    assert_eq!(
        replayed,
        vec![(
            "{\"v\":1,\"theme\":\"dark\"}".to_string(),
            // The router derives the sender from its own wiring — the component
            // named only its port.
            "surface:deskbar#protobar".to_string()
        )]
    );
}

/// An err discards the buffer whole: nothing reaches the router or the wire, and
/// a failure is counted. The instance keeps running.
#[test]
fn err_discards_the_buffer_and_keeps_the_instance_running() {
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output("out", "ephemeral:sink")],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "never sent".into()).unwrap();
    let effects = complete(&mut core, INST, err("sink refused"), ready.buffer);
    assert!(batches(&effects).is_empty(), "an err publishes nothing");
    // Still alive and still delivered.
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    assert_eq!(split(window(&ready.activation, "in")).1, vec!["m2"]);
}

/// An err discards the entries but **not** the spending. What the component
/// burned is a fact about the activation that ran, and returning err does not
/// un-burn it.
///
/// The bucket is the only backstop against a component that publishes and then
/// errs on purpose — err, spend, err, spend, forever, on somebody else's budget.
/// If the err arm dropped the carry (or skipped `into_carry`), every failed
/// activation would hand back a free refill and the loop would cost nothing. So
/// this is pinned by contrast: the same wiring, the same err, differing only in
/// whether activation 1 spent, must leave activation 2 with different budgets.
#[test]
fn an_err_returns_the_carryover_but_the_spending_survives_it() {
    // Fill 0 — purely input-driven, so the grant is the only income and every
    // millitoken in play is traceable to a delivered message. Capacity is
    // generous, so nothing below is a clamp in disguise.
    let wiring = || {
        (
            vec![binding("in", 8, 0)],
            vec![output_budget(
                "out",
                "ephemeral:sink",
                0,
                16 * brenn_budget::MILLITOKENS_PER_PUBLISH,
            )],
        )
    };

    // The spender: activation 1 takes its whole 3-envelope grant, then errs.
    let (subs, outs) = wiring();
    let mut core = registered_core(subs, outs);
    for i in 1..=3u128 {
        deliver(&mut core, &env(&format!("m{i}"), i), i as u64);
    }
    let mut ready = take_one(&mut core);
    for i in 0..3 {
        assert_eq!(ready.buffer.publish("out", format!("p{i}")), Ok(()));
    }
    complete(
        &mut core,
        INST,
        err("spent it all, then failed"),
        ready.buffer,
    );
    // Activation 2 is woken by one message, so its income is one publish and
    // nothing carried in.
    deliver(&mut core, &env("m4", 4), 4);
    let mut ready = take_one(&mut core);
    assert_eq!(
        ready.buffer.publish("out", "a".into()),
        Ok(()),
        "the new message's own grant is still income"
    );
    assert_eq!(
        ready.buffer.publish("out", "b".into()),
        Err(PublishError::QuotaExceeded),
        "nothing carried: the err did not refund what activation 1 spent"
    );

    // The miser: same shape, same err, spends nothing.
    let (subs, outs) = wiring();
    let mut core = registered_core(subs, outs);
    for i in 1..=3u128 {
        deliver(&mut core, &env(&format!("m{i}"), i), i as u64);
    }
    let ready = take_one(&mut core);
    complete(
        &mut core,
        INST,
        err("failed without spending"),
        ready.buffer,
    );
    deliver(&mut core, &env("m4", 4), 4);
    let mut ready = take_one(&mut core);
    for i in 0..4 {
        assert_eq!(
            ready.buffer.publish("out", format!("q{i}")),
            Ok(()),
            "publish {i}: the unspent grant of 3 carried through the err, plus this \
             activation's 1"
        );
    }
    assert_eq!(
        ready.buffer.publish("out", "fifth".into()),
        Err(PublishError::QuotaExceeded),
        "carry(3) + grant(1) and not a millitoken more"
    );
}

/// A trap discards the buffer and is terminal for that instance — and for that
/// instance only. Its rings survive and keep being fed; a sibling is untouched.
#[test]
fn trap_is_terminal_for_one_instance_and_its_rings_survive() {
    let mut core = registered_core(
        vec![binding("in", 4, 2)],
        vec![output("out", "ephemeral:sink")],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "never sent".into()).unwrap();
    let effects = complete(
        &mut core,
        INST,
        ActivationOutcome::Trap("boom".into()),
        ready.buffer,
    );
    assert!(batches(&effects).is_empty(), "a trap publishes nothing");
    assert!(matches!(
        effects.as_slice(),
        [Effect::EmitEvent(Event::InstanceFailed { instance, .. })] if instance == INST
    ));
    // Delivery stops: no further activation, ever.
    deliver(&mut core, &env("m2", 2), 2);
    assert!(
        core.take_ready_activation().is_none(),
        "a failed instance never activates again"
    );
    // But its ring kept filling — rings are the subscription's, page-lifetime,
    // and inert rather than corrupt.
    let ring = core
        .wire_rings
        .get(&SubKey::for_instance(INST, "ephemeral:demo"))
        .expect("the subscription's ring outlives the instance");
    assert_eq!(
        ring.entries()
            .map(|(e, _)| e.body.clone())
            .collect::<Vec<_>>(),
        vec!["m1", "m2"]
    );
}

/// The component's own account of a failure reaches the diagnostic event. The
/// kernel never parses it, but it is the only answer to "failed *how*?" — an
/// event carrying a constant would be diagnostic in shape only.
#[test]
fn a_failure_event_carries_the_components_own_message() {
    let mut core = registered_core(vec![binding("in", 4, 0)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    let ready = take_one(&mut core);
    let effects = complete(
        &mut core,
        INST,
        err("row 42: unparseable amount"),
        ready.buffer,
    );
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::EmitEvent(Event::ActivationFailed { message, .. })]
                if message == "row 42: unparseable amount"
        ),
        "the err's message rides through, not a constant: {effects:?}"
    );

    // Same for a trap, whose message the driver recovers from the unwind.
    let mut core = registered_core(vec![binding("in", 4, 0)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    let ready = take_one(&mut core);
    let effects = complete(
        &mut core,
        INST,
        ActivationOutcome::Trap("index out of bounds: len is 0".into()),
        ready.buffer,
    );
    assert!(
        matches!(
            effects.as_slice(),
            [Effect::EmitEvent(Event::InstanceFailed { reason, .. })]
                if reason == "index out of bounds: len is 0"
        ),
        "the trap's message rides through: {effects:?}"
    );
}

// ── Budgets ────────────────────────────────────────────────────────────────

/// The sink bucket is seeded `clamp(carry) + fill + grant(new count)`, and a
/// publish past it is refused inline with the buffer otherwise intact.
#[test]
fn sink_budget_exhaustion_is_quota_exceeded_and_leaves_the_buffer_intact() {
    // Zero fill and zero capacity: purely input-driven. One new envelope grants
    // exactly one publish.
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output_budget("out", "ephemeral:sink", 0, 0)],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    assert_eq!(ready.buffer.publish("out", "granted".into()), Ok(()));
    assert_eq!(
        ready.buffer.publish("out", "over".into()),
        Err(PublishError::QuotaExceeded),
        "the grant paid for one publish, not two"
    );
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    let (_, _, entries) = &batches(&effects)[0];
    assert_eq!(entries.len(), 1, "the refused publish is not buffered");
    assert_eq!(
        entries[0].body, "granted",
        "the rest of the buffer survives"
    );
}

/// A rejected publish never fails the activation — the component decides what to
/// do about it, exactly as on the backend.
#[test]
fn unbound_port_is_not_permitted_and_oversized_body_is_invalid_payload() {
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output("out", "ephemeral:sink")],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    assert_eq!(
        ready.buffer.publish("nope", "x".into()),
        Err(PublishError::NotPermitted),
        "a port the config does not give this instance"
    );
    let huge = "x".repeat(70_000);
    assert_eq!(
        ready.buffer.publish("out", huge),
        Err(PublishError::InvalidPayload),
        "past the surface's advertised body cap"
    );
    // Neither refusal failed the activation, and a good publish still works.
    assert_eq!(ready.buffer.publish("out", "fine".into()), Ok(()));
}

/// Unspent millitokens carry across activations, clamped to `capacity_mt` at the
/// next seed. The clamp is what bounds what an idle component accumulates.
#[test]
fn carryover_persists_across_activations_and_clamps_to_capacity() {
    // Fill 2 publishes/activation, capacity 1 publish carried.
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output_budget(
            "out",
            "ephemeral:sink",
            2 * brenn_budget::MILLITOKENS_PER_PUBLISH,
            brenn_budget::MILLITOKENS_PER_PUBLISH,
        )],
    );
    // Activation 1: spend nothing. Seed was fill(2) + grant(1) = 3 publishes'
    // worth; all 3 carry, but capacity clamps the carry to 1 at the next seed.
    deliver(&mut core, &env("m1", 1), 1);
    let ready = take_one(&mut core);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    // Activation 2: seed = clamp(3 → 1) + fill(2) + grant(1) = 4 publishes.
    deliver(&mut core, &env("m2", 2), 2);
    let mut ready = take_one(&mut core);
    for i in 0..4 {
        assert_eq!(
            ready.buffer.publish("out", format!("p{i}")),
            Ok(()),
            "publish {i} is within clamp(carry)+fill+grant"
        );
    }
    assert_eq!(
        ready.buffer.publish("out", "fifth".into()),
        Err(PublishError::QuotaExceeded),
        "the carry was clamped to capacity, so the fifth is over"
    );
}

/// The per-activation publish cap is the outer backstop, independent of any
/// bucket: a component with a generous budget still cannot buffer more than the
/// page will hold.
#[test]
fn the_per_activation_publish_cap_bounds_a_generous_budget() {
    let generous = 10_000 * brenn_budget::MILLITOKENS_PER_PUBLISH;
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output_budget("out", "ephemeral:sink", generous, generous)],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    for i in 0..brenn_budget::MAX_PUBLISHES_PER_ACTIVATION {
        assert_eq!(ready.buffer.publish("out", format!("p{i}")), Ok(()));
    }
    assert_eq!(
        ready.buffer.publish("out", "one too many".into()),
        Err(PublishError::QuotaExceeded)
    );
}

/// A refused publish is not a free publish. The call counter increments *ahead*
/// of the port lookup, the body check, and the bucket, so a component looping on
/// `not-permitted` pays for every attempt and is eventually cut off — without
/// that ordering the rejection path costs nothing, and free is what makes it a
/// flood.
///
/// The assertion that matters is the last one: the cap outranks the port check,
/// so call 513 answers `QuotaExceeded` rather than the `NotPermitted` the port
/// alone would give.
#[test]
fn refused_calls_are_charged_and_the_calls_cap_outranks_the_port_check() {
    let generous = 10_000 * brenn_budget::MILLITOKENS_PER_PUBLISH;
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output_budget("out", "ephemeral:sink", generous, generous)],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    for i in 0..brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION {
        assert_eq!(
            ready.buffer.publish("nope", "x".into()),
            Err(PublishError::NotPermitted),
            "call {i} is refused by the port check, and charged for it"
        );
    }
    assert_eq!(
        ready.buffer.publish("nope", "x".into()),
        Err(PublishError::QuotaExceeded),
        "the calls cap fires on a call the port check would otherwise answer"
    );
    // The cap is on calls, not on the port: a bound port is cut off too.
    assert_eq!(
        ready.buffer.publish("out", "x".into()),
        Err(PublishError::QuotaExceeded),
        "the calls cap is the activation's, not the port's"
    );
}

/// The per-activation byte ceiling is what bounds the page's own memory when a
/// component's bucket is generous: the buffer holds every accepted body until
/// the flush, so without it a solvent component can grow the page without limit.
///
/// It refuses only the publish that would cross it — the accepted prefix stays
/// buffered and still flushes, exactly like every other inline refusal.
#[test]
fn the_per_activation_byte_cap_bounds_a_generous_budget_and_keeps_the_prefix() {
    let generous = 10_000 * brenn_budget::MILLITOKENS_PER_PUBLISH;
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output_budget("out", "ephemeral:sink", generous, generous)],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    // `welcome_frame` advertises a 65_536-byte body cap, so a full body is a
    // whole legal maximum and 64 of them reach the 4 MiB ceiling exactly. Fill to
    // one short of that, leaving exactly one body's headroom.
    let body_len = 65_536usize;
    let full = brenn_budget::MAX_PUBLISH_BYTES_PER_ACTIVATION / body_len - 1;
    for i in 0..full {
        assert_eq!(
            ready.buffer.publish("out", "x".repeat(body_len)),
            Ok(()),
            "publish {i} is within the byte ceiling"
        );
    }
    // Eat into the headroom, so the next full body no longer fits.
    assert_eq!(ready.buffer.publish("out", "tiny".into()), Ok(()));
    assert_eq!(
        ready.buffer.publish("out", "x".repeat(body_len)),
        Err(PublishError::QuotaExceeded),
        "the publish that would cross the byte ceiling is refused — and it is a body \
         the per-publish cap and the bucket both allow, so only the ceiling can be \
         refusing it"
    );
    // Refused inline, buffer otherwise intact: the ceiling turns away the body
    // that does not fit, not the component. A smaller one still lands.
    assert_eq!(ready.buffer.publish("out", "also tiny".into()), Ok(()));
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    let entries = &batches(&effects)[0].2;
    assert_eq!(
        entries.len(),
        full + 2,
        "the refusal cost the batch nothing but the body that did not fit"
    );
    assert_eq!(entries[full].body, "tiny");
    assert_eq!(entries[full + 1].body, "also tiny");
}

// ── Depth 0 ────────────────────────────────────────────────────────────────

/// A depth-0 binding never activates its instance and never queues — but its
/// ring is fed throughout, and it windows as pure context when a sibling port
/// does the waking. Depth 0 means "don't activate me", never "don't show me".
#[test]
fn a_depth_zero_port_never_activates_and_windows_as_pure_context() {
    let mut core = registered_core(
        vec![
            binding_on("ephemeral:demo", "waker", 4, 0),
            binding_on("ephemeral:sampled", "sampled", 0, 2),
        ],
        vec![],
    );
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:sampled", SubscribeOutcome::Ok)),
        Millis(6),
    );
    // Two deliveries on the depth-0 port's channel: no activation at all.
    for (i, body) in ["s1", "s2"].iter().enumerate() {
        core.on_input(
            Input::TextFrame(deliver_frame_dropped(
                "ephemeral:sampled",
                &env(body, 100 + i as u128),
                10 + i as u64,
                0,
            )),
            Millis(20 + i as u64),
        );
    }
    assert!(
        core.take_ready_activation().is_none(),
        "a depth-0 port never activates its instance"
    );
    // A sibling port wakes it; the depth-0 port is there, as pure context.
    deliver(&mut core, &env("w1", 1), 1);
    let ready = take_one(&mut core);
    let sampled = window(&ready.activation, "sampled");
    assert_eq!(
        split(sampled),
        (vec!["s1", "s2"], vec![]),
        "the ring was fed all along; the window is pure context"
    );
    assert_eq!(sampled.new_from, 2, "new_from == len");
    assert_eq!(
        sampled.dropped, 0,
        "no queue, so no push overflow to report"
    );
}

// ── Registration seam ──────────────────────────────────────────────────────

/// Deregistration drops the entry's queues but not the subscription's rings: a
/// re-register reads the retained history, exactly as a reconnect would.
#[test]
fn deregistration_drops_queues_but_not_rings() {
    let mut core = registered_core(vec![binding("in", 4, 2)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    core.on_input(
        Input::ActivationDeregistered {
            instance: INST.into(),
        },
        Millis(30),
    );
    assert!(
        core.take_ready_activation().is_none(),
        "no entry, no activation"
    );
    // Deregistering released the instance's last reference on the subscription,
    // so re-registering opens it afresh — a registered instance is a subscriber
    // like any other.
    register(&mut core, INST, Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(31),
    );
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")),
        (vec!["m1"], vec!["m2"]),
        "the ring outlived the registration"
    );
}

/// A double registration would silently orphan the first entry's queued
/// messages.
#[test]
#[should_panic(expected = "registered twice")]
fn double_registration_panics() {
    let mut core = registered_core(vec![binding("in", 4, 0)], vec![]);
    register(&mut core, INST, Millis(30));
}

// ── Rings ──────────────────────────────────────────────────────────────────

/// The ring's depth is the max fold over the instance's bindings on the channel,
/// and the ring is fed for the subscription — not per binding. Retention is a
/// property of the subscription, which is why one ring serves two ports reading
/// it at two depths.
#[test]
fn ring_depth_is_the_max_fold_over_the_instances_bindings() {
    let mut core = active_core_with(vec![binding("shallow", 4, 1), binding("deep", 4, 3)]);
    register(&mut core, INST, Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    let key = SubKey::for_instance(INST, "ephemeral:demo");
    assert_eq!(
        core.wire_rings.get(&key).expect("ring exists").depth(),
        3,
        "max over the instance's bindings on the channel"
    );
    for i in 1..=4u64 {
        deliver(&mut core, &env(&format!("m{i}"), i as u128), i);
    }
    let held: Vec<String> = core.wire_rings[&key]
        .entries()
        .map(|(e, _)| e.body.clone())
        .collect();
    assert_eq!(
        held,
        vec!["m2", "m3", "m4"],
        "bounded by the fold, oldest out"
    );
}

/// **The ring feed is idempotent by `message_id`.** Rings survive reconnect while
/// several reconnect paths legitimately re-deliver what the ring already holds
/// (fresh-attach replay, gap-past-ring replay, epoch-change replay). Without the
/// dedup a post-reconnect window's context would carry the same message twice —
/// a shape the backend's distinct-row context read can never produce.
#[test]
fn the_ring_feed_is_idempotent_and_survives_reconnect() {
    let mut core = registered_core(vec![binding("in", 4, 4)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    // Drop the link and come back; the page did not reload, so the ring must not
    // have been discarded.
    reconnect(&mut core, vec![binding("in", 4, 4)], vec![]);
    // The server replays what it retained: the same two envelopes, same ids.
    deliver(&mut core, &env("m1", 1), 3);
    deliver(&mut core, &env("m2", 2), 4);
    let ready = take_one(&mut core);
    let w = window(&ready.activation, "in");
    let ids: Vec<Uuid> = w.envelopes.iter().map(|e| e.message_id).collect();
    let mut deduped = ids.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        ids.len(),
        deduped.len(),
        "each message_id appears at most once in the window: {:?}",
        w.envelopes.iter().map(|e| &e.body).collect::<Vec<_>>()
    );
    // The replayed pair is new (it was re-delivered), and the ring held only one
    // copy of each throughout.
    assert_eq!(split(w), (vec![], vec!["m1", "m2"]));
}

/// A subscription no surviving binding names loses its ring: nothing can route
/// on it again.
#[test]
fn a_ring_whose_binding_vanished_is_dropped_at_reconcile() {
    let mut core = registered_core(vec![binding("in", 4, 2)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    let key = SubKey::for_instance(INST, "ephemeral:demo");
    assert!(core.wire_rings.contains_key(&key));
    // Bindings change only across a reconnect: a second `Welcome` on a live
    // connection is a fatal protocol error.
    reconnect(
        &mut core,
        vec![binding_on("ephemeral:other", "in", 4, 2)],
        vec![],
    );
    assert!(
        !core.wire_rings.contains_key(&key),
        "the operator un-declared the binding; its ring goes with it"
    );
}

// ── Parked batches ─────────────────────────────────────────────────────────

/// A page-local channel wired back into `INST`'s own input, plus a wire output.
///
/// This is how an activation happens with the link down — which is the whole
/// premise of parking, and not a contrivance: `local:` delivery never touches
/// the wire, so a page whose link died goes right on minting activations (the
/// kiosk taking a takeover at T−2min with the network out). A test that could
/// only make activations by receiving `Deliver`s could not reach this state at
/// all.
const LOOP_CHANNEL: &str = "local:wiring";

fn loop_outputs() -> Vec<OutputBinding> {
    vec![
        output_budget(
            "loop",
            LOOP_CHANNEL,
            16 * brenn_budget::MILLITOKENS_PER_PUBLISH,
            16 * brenn_budget::MILLITOKENS_PER_PUBLISH,
        ),
        output_budget(
            "out",
            "ephemeral:sink",
            16 * brenn_budget::MILLITOKENS_PER_PUBLISH,
            16 * brenn_budget::MILLITOKENS_PER_PUBLISH,
        ),
    ]
}

fn local_loop_core(outputs: Vec<OutputBinding>) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(crate::test_support::welcome_frame_local(
            vec![binding_on(LOOP_CHANNEL, "in", 64, 0)],
            outputs,
            vec![LocalChannel {
                channel: LOOP_CHANNEL.into(),
                ring_depth: 1,
            }],
        )),
        Millis(2),
    );
    register(&mut core, INST, Millis(5));
    core
}

/// Feed the local loop, waking `INST` — with or without a link.
fn tick_loop(core: &mut ClientCore, n: u64) {
    publish(core, n, INST, "loop", &format!("tick{n}"), Millis(100 + n));
}

/// Reconnect a `local_loop_core` on the same wiring.
fn loop_reconnect(core: &mut ClientCore, outputs: Vec<OutputBinding>) -> Vec<Effect> {
    loop_reconnect_at_body_cap(
        core,
        outputs,
        brenn_surface_test_fixtures::FIXTURE_MAX_BODY_BYTES,
    )
}

/// As [`loop_reconnect`] but with the new connection advertising `max_body_bytes`
/// — an operator lowering `messaging.max_body_bytes` and restarting, which needs
/// no build change and so forces no page reload.
fn loop_reconnect_at_body_cap(
    core: &mut ClientCore,
    outputs: Vec<OutputBinding>,
    max_body_bytes: u64,
) -> Vec<Effect> {
    core.on_input(Input::Tick, Millis(4_000));
    core.on_input(Input::Opened, Millis(4_001));
    core.on_input(
        Input::TextFrame(brenn_surface_test_fixtures::welcome_frame(
            brenn_surface_test_fixtures::WelcomeParams {
                subscriptions: vec![binding_on(LOOP_CHANNEL, "in", 64, 0)],
                outputs,
                components: vec!["protobar"],
                local_channels: vec![LocalChannel {
                    channel: LOOP_CHANNEL.into(),
                    ring_depth: 1,
                }],
                max_body_bytes,
                ..Default::default()
            },
        )),
        Millis(4_002),
    )
}

/// Answer the batch `correlation` carries and return the core's effects.
fn answer(
    core: &mut ClientCore,
    correlation: u64,
    outcome: PublishBatchOutcome,
    now: Millis,
) -> Vec<Effect> {
    core.on_input(
        Input::TextFrame(
            serde_json::to_string(&ServerFrame::PublishBatchResult {
                correlation,
                outcome,
            })
            .unwrap(),
        ),
        now,
    )
}

/// Drain an instance's outbox by answering each flush `Ok`, starting from the
/// effects that sent the head, and collect every batch's bodies in send order.
///
/// The outbox carries one flush on the wire at a time, so draining it *is* this
/// loop: each `Ok` frees the wire and the next head goes out on the same turn.
fn drain_outbox(core: &mut ClientCore, head: &[Effect]) -> Vec<Vec<String>> {
    let mut sent = Vec::new();
    let mut pending = batches(head);
    let mut now = 5_000;
    while let Some((_, correlation, entries)) = pending.first().cloned() {
        assert_eq!(pending.len(), 1, "one flush on the wire at a time");
        sent.push(entries.iter().map(|e| e.body.clone()).collect::<Vec<_>>());
        now += 10;
        pending = batches(&answer(
            core,
            correlation,
            PublishBatchOutcome::Ok,
            Millis(now),
        ));
    }
    sent
}

fn toast_count(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|e| matches!(e, Effect::PublishControl { channel, .. } if channel == LOCAL_TOAST_CHANNEL))
        .count()
}

/// A flush while disconnected parks rather than failing: the activation already
/// returned ok, so the guarantee is "flushed, not discarded" up to a stated
/// bound. The queued batches go out after the next `Welcome`, in order — the
/// head first, each successor as its predecessor is answered.
#[test]
fn a_disconnected_flush_parks_and_sends_in_order_after_welcome() {
    let mut core = local_loop_core(loop_outputs());
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(40),
    );
    for i in 0..2u64 {
        tick_loop(&mut core, i);
        let mut ready = take_one(&mut core);
        ready.buffer.publish("out", format!("parked{i}")).unwrap();
        let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
        assert!(batches(&effects).is_empty(), "nothing rides a dead link");
    }
    let effects = loop_reconnect(&mut core, loop_outputs());
    let bodies: Vec<Vec<String>> = drain_outbox(&mut core, &effects);
    assert_eq!(
        bodies,
        vec![vec!["parked0"], vec!["parked1"]],
        "in order, after the handshake"
    );
}

/// At the cap the **oldest whole batch** drops — never a split one, since the
/// batch is the unit the server applies atomically — counted, and announced on
/// the toast plane (which works offline; a backend alert queued against a dead
/// link would be a message to nobody).
#[test]
fn parked_batches_drop_oldest_whole_at_the_cap_and_toast() {
    let mut core = local_loop_core(loop_outputs());
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(40),
    );
    // The fixture cap is 8; make 9 flushes.
    let mut toasts = 0;
    for i in 0..9u64 {
        tick_loop(&mut core, i);
        let mut ready = take_one(&mut core);
        // Two entries per batch, so a split batch would be visible as a
        // half-length one rather than passing for a whole.
        ready.buffer.publish("out", format!("batch{i}a")).unwrap();
        ready.buffer.publish("out", format!("batch{i}b")).unwrap();
        let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
        toasts += toast_count(&effects);
    }
    assert_eq!(toasts, 1, "one batch over the cap, one toast");
    let effects = loop_reconnect(&mut core, loop_outputs());
    let bodies = drain_outbox(&mut core, &effects);
    assert_eq!(bodies.len(), 8, "the cap, exactly");
    assert_eq!(
        bodies,
        (1..9)
            .map(|i| vec![format!("batch{i}a"), format!("batch{i}b")])
            .collect::<Vec<_>>(),
        "the oldest whole batch dropped; every survivor is whole and in order"
    );
}

/// A trap takes the instance's parked flushes with it. They were produced by a
/// component whose memory is now presumed poisoned and there is nobody left to
/// answer for them, so sending them on the next `Welcome` would put publishes
/// from a dead component on the wire.
///
/// **The drop is silent, and that is deliberate** — this is the one parked-drop
/// path that does not toast. A cap drop and a reconcile orphan both happen to a
/// live component that will keep running and whose user is owed the news; a trap
/// already emitted `InstanceFailed`, which is the news. A second toast per parked
/// batch would be N notifications for one event. Asserted here so the asymmetry
/// is stated rather than incidental.
#[test]
fn a_trap_drops_the_instances_parked_batches_silently() {
    let mut core = local_loop_core(loop_outputs());
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(40),
    );
    // Two flushes park against the down link.
    for i in 0..2u64 {
        tick_loop(&mut core, i);
        let mut ready = take_one(&mut core);
        ready.buffer.publish("out", format!("parked{i}")).unwrap();
        let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
        assert!(batches(&effects).is_empty(), "the link is down: parked");
    }
    // The third activation traps.
    tick_loop(&mut core, 2);
    let ready = take_one(&mut core);
    let effects = complete(
        &mut core,
        INST,
        ActivationOutcome::Trap("boom".into()),
        ready.buffer,
    );
    assert_eq!(
        toast_count(&effects),
        0,
        "the trap drops two parked batches without a toast; InstanceFailed is the signal"
    );
    // Reconnect: nothing rides out for the dead instance.
    let effects = loop_reconnect(&mut core, loop_outputs());
    assert!(
        batches(&effects).is_empty(),
        "a poisoned component's parked flushes never reach the server"
    );
    assert_eq!(
        toast_count(&effects),
        0,
        "and are not announced late either"
    );
}

/// A parked batch whose body no longer fits the *new* connection's cap is
/// dropped whole and toasted, exactly like one naming a vanished port.
///
/// The entry was legal when it was buffered: the kernel checked it against the
/// cap in force on the old connection. An operator can lower
/// `messaging.max_body_bytes` and restart with no build change — so no forced
/// reload — and the page reconnects to a smaller contract holding batches
/// validated against the larger one. Replaying one is a violation-grade body at
/// the batch handler: connection killed, fail2ban fed, surviving parked batches
/// discarded with the teardown, for a page that did nothing but honestly replay
/// what it buffered. The port-survival check exists to prevent exactly this; the
/// body cap is the other gate the server kills over, so it is re-checked too.
#[test]
fn a_parked_batch_over_the_new_body_cap_is_dropped_not_sent() {
    let mut core = local_loop_core(loop_outputs());
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(40),
    );
    // Two flushes park: a big body legal under the old 65_536-byte cap, then a
    // small one. Only the first is over the cap the reconnect advertises.
    tick_loop(&mut core, 0);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "x".repeat(4_096)).unwrap();
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    tick_loop(&mut core, 1);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "small".into()).unwrap();
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);

    // Reconnect to an operator who shrank the cap under the page's feet.
    let effects = loop_reconnect_at_body_cap(&mut core, loop_outputs(), 1_024);
    let sent = batches(&effects);
    assert_eq!(
        sent.len(),
        1,
        "only the batch the new cap still admits rides"
    );
    assert_eq!(sent[0].2[0].body, "small");
    assert_eq!(
        toast_count(&effects),
        1,
        "the over-cap batch is dropped whole and announced, not sent into a kill"
    );
}

/// A parked batch naming an output the new bindings no longer carry is dropped
/// whole and toasted. Sending it would present the server with an unbound port —
/// a violation it kills the connection over — so the page would take a protocol
/// death for honestly replaying what an operator un-wired underneath it.
#[test]
fn a_parked_batch_orphaned_by_reconcile_is_dropped_not_sent() {
    let mut core = local_loop_core(loop_outputs());
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(40),
    );
    tick_loop(&mut core, 0);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "orphan".into()).unwrap();
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    // Reconnect into bindings where `out` is gone.
    let surviving = vec![output_budget(
        "loop",
        LOOP_CHANNEL,
        16 * brenn_budget::MILLITOKENS_PER_PUBLISH,
        16 * brenn_budget::MILLITOKENS_PER_PUBLISH,
    )];
    let effects = loop_reconnect(&mut core, surviving);
    assert!(
        batches(&effects).is_empty(),
        "the batch names a port the server would now reject"
    );
    assert_eq!(
        toast_count(&effects),
        1,
        "and the drop is announced, like any other"
    );
}

// ── Batch results ──────────────────────────────────────────────────────────

/// `RateLimited` is not a drop and not a death: the batch goes back to the head
/// of its instance's outbox and is retried on the timer. The activation's
/// guarantee — "flushed, not discarded, up to a stated bound" — holds in the
/// refusal case exactly as in the disconnect case.
#[test]
fn a_rate_limited_batch_is_parked_at_the_head_and_retried_whole() {
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output("out", "ephemeral:sink")],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "a".into()).unwrap();
    ready.buffer.publish("out", "b".into()).unwrap();
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    let (_, correlation, _) = batches(&effects)[0].clone();

    let effects = answer(
        &mut core,
        correlation,
        PublishBatchOutcome::RateLimited,
        Millis(80),
    );
    assert!(
        batches(&effects).is_empty(),
        "the retry waits for the timer, not the same turn"
    );
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::CloseTransport)),
        "a refusal is metering, not a protocol error"
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::SetRetryWakeup(Some(_)))),
        "and the timer is armed to carry it: {effects:?}"
    );

    // The timer fires: the same batch, whole and in call order.
    let effects = core.on_input(Input::RetryTick, Millis(1_080));
    let sent = batches(&effects);
    assert_eq!(sent.len(), 1, "one retry, the head");
    let bodies: Vec<String> = sent[0].2.iter().map(|e| e.body.clone()).collect();
    assert_eq!(bodies, vec!["a", "b"], "retried whole, in call order");
    assert!(
        effects.contains(&Effect::SetRetryWakeup(None)),
        "the head is on the wire, so nothing is owed a retry: {effects:?}"
    );

    // Answered this time: nothing re-parks, nothing re-arms.
    let effects = answer(&mut core, sent[0].1, PublishBatchOutcome::Ok, Millis(1_090));
    assert!(batches(&effects).is_empty(), "the outbox is empty");
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::SetRetryWakeup(Some(_)))),
        "and stays disarmed: {effects:?}"
    );
    // Still live and still delivering.
    deliver(&mut core, &env("m2", 2), 2);
    assert!(core.take_ready_activation().is_some());
}

/// A newer flush during the refusal window queues *behind* the refused head and
/// lands after it. Order among an instance's own flushes is total: the component
/// published a, b then c, and no reordering of ok'd publishes is a thing any
/// backend component could experience.
#[test]
fn a_newer_flush_queues_behind_a_refused_head_and_lands_after_it() {
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output("out", "ephemeral:sink")],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "first".into()).unwrap();
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    let (_, correlation, _) = batches(&effects)[0].clone();
    answer(
        &mut core,
        correlation,
        PublishBatchOutcome::RateLimited,
        Millis(80),
    );

    // A second activation flushes while the head sits refused.
    deliver(&mut core, &env("m2", 2), 2);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "second".into()).unwrap();
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    assert!(
        batches(&effects).is_empty(),
        "it must not leapfrog the head onto the wire"
    );

    let effects = core.on_input(Input::RetryTick, Millis(1_080));
    assert_eq!(
        drain_outbox(&mut core, &effects),
        vec![vec!["first"], vec!["second"]],
        "the head first, then what queued behind it"
    );
}

/// A sibling instance's steady stream of `Ok` results must not move a parked
/// head's retry deadline. The timer is armed once at the refusal and re-armed
/// only by its own firing; an unrelated instance's traffic re-arming it every
/// result would push the deadline past every tick and starve the head forever.
#[test]
fn a_siblings_steady_results_do_not_starve_a_parked_heads_retry() {
    let b_in = Binding {
        channel: "ephemeral:demo".into(),
        instance: INST.into(),
        port: "in".into(),
        push_depth: 4,
        retain_depth: 0,
        noise: brenn_surface_proto::NoiseLevel::Silent,
    };
    let a_in = Binding {
        channel: "ephemeral:demo".into(),
        instance: "sibling".into(),
        port: "in".into(),
        push_depth: 4,
        retain_depth: 0,
        noise: brenn_surface_proto::NoiseLevel::Silent,
    };
    let b_out = output("out", "ephemeral:sink");
    let a_out = OutputBinding {
        channel: "ephemeral:sink".into(),
        instance: "sibling".into(),
        port: "out".into(),
        urgency: Urgency::Normal,
        fill_mt: TEST_FILL_MT,
        capacity_mt: TEST_CAPACITY_MT,
    };
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(crate::test_support::welcome_frame(
            vec![b_in, a_in],
            vec![b_out, a_out],
        )),
        Millis(2),
    );
    register(&mut core, INST, Millis(5));
    register(&mut core, "sibling", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    core.on_input(
        Input::TextFrame(subscribe_result_for(
            "ephemeral:demo",
            "sibling",
            SubscribeOutcome::Ok,
        )),
        Millis(6),
    );

    // B flushes and the server refuses it: parked at the head, timer armed.
    deliver(&mut core, &env("b1", 1), 1);
    let b = core.take_ready_activation().expect("B is ready");
    assert_eq!(b.instance, INST);
    let mut buf = b.buffer;
    buf.publish("out", "b-head".into()).unwrap();
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, buf);
    let (_, b_corr, _) = batches(&effects)[0].clone();
    let effects = answer(
        &mut core,
        b_corr,
        PublishBatchOutcome::RateLimited,
        Millis(100),
    );
    let armed = effects
        .iter()
        .find_map(|e| match e {
            Effect::SetRetryWakeup(Some(t)) => Some(*t),
            _ => None,
        })
        .expect("the refusal arms the retry");
    assert_eq!(armed, Millis(100).saturating_add_ms(RETRY_INTERVAL_MS));

    // A flushes repeatedly, faster than the retry cadence. Not one of its `Ok`
    // results may emit a retry-wakeup: B's deadline stays exactly where it was.
    for i in 1..=5u128 {
        core.on_input(
            Input::TextFrame(deliver_frame_for(
                "ephemeral:demo",
                "sibling",
                &env(&format!("a{i}"), 100 + i),
                i as u64,
            )),
            Millis(100 + (i as u64) * 100),
        );
        let a = core.take_ready_activation().expect("A is ready");
        assert_eq!(a.instance, "sibling");
        let mut buf = a.buffer;
        buf.publish("out", format!("a{i}")).unwrap();
        let effects = complete(&mut core, "sibling", ActivationOutcome::Ok, buf);
        let (_, a_corr, _) = batches(&effects)[0].clone();
        let effects = answer(
            &mut core,
            a_corr,
            PublishBatchOutcome::Ok,
            Millis(100 + (i as u64) * 100 + 10),
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::SetRetryWakeup(_))),
            "a sibling's Ok must not touch B's retry deadline: {effects:?}"
        );
    }

    // B's retry fires on its original deadline and re-offers its head.
    let effects = core.on_input(Input::RetryTick, armed);
    let sent = batches(&effects);
    assert_eq!(sent.len(), 1, "B's head is re-offered");
    assert_eq!(sent[0].0, INST);
    assert_eq!(sent[0].2[0].body, "b-head");
}

/// The retry timer is torn down on the way out of `Active`: a page that
/// disconnects with a blocked outbox must not tick against a dead socket, and a
/// straggler tick that beat the teardown is a disarm no-op.
#[test]
fn a_blocked_outbox_disarms_the_retry_on_disconnect() {
    let mut core = registered_core(
        vec![binding("in", 4, 0)],
        vec![output("out", "ephemeral:sink")],
    );
    deliver(&mut core, &env("m1", 1), 1);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "a".into()).unwrap();
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    let (_, correlation, _) = batches(&effects)[0].clone();
    let effects = answer(
        &mut core,
        correlation,
        PublishBatchOutcome::RateLimited,
        Millis(80),
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::SetRetryWakeup(Some(_)))),
        "the refusal arms the retry"
    );

    let effects = core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(1_000),
    );
    assert!(
        effects.contains(&Effect::SetRetryWakeup(None)),
        "disarmed on the way out of Active: {effects:?}"
    );

    let effects = core.on_input(Input::RetryTick, Millis(1_001));
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::SetRetryWakeup(_))),
        "a straggler tick in Backoff is a no-op: {effects:?}"
    );
}

/// A head the server keeps refusing does not retry forever without evidence: the
/// outbox fills to its cap and then degrades to counted, toasted drops of the
/// oldest — never unbounded page memory, never a silent discard.
#[test]
fn a_persistently_refused_head_converges_to_counted_toasted_drops() {
    let mut core = local_loop_core(loop_outputs());
    tick_loop(&mut core, 0);
    let mut ready = take_one(&mut core);
    ready.buffer.publish("out", "head".into()).unwrap();
    let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    let (_, correlation, _) = batches(&effects)[0].clone();
    let effects = answer(
        &mut core,
        correlation,
        PublishBatchOutcome::RateLimited,
        Millis(80),
    );
    assert_eq!(toast_count(&effects), 0, "one refusal is not yet a loss");

    // Fill the outbox (fixture cap 8) behind the refused head, then one more.
    let mut toasts = 0;
    for i in 0..8u64 {
        tick_loop(&mut core, i + 1);
        let mut ready = take_one(&mut core);
        ready.buffer.publish("out", format!("q{i}")).unwrap();
        let effects = complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
        assert!(
            batches(&effects).is_empty(),
            "the head still blocks the wire"
        );
        toasts += toast_count(&effects);
    }
    assert_eq!(toasts, 1, "exactly the one batch past the cap, announced");

    // What survived is whole and in order — the oldest (the refused head) is
    // what went, which is the drop-oldest rule the queue always had.
    let effects = core.on_input(Input::RetryTick, Millis(1_080));
    let bodies = drain_outbox(&mut core, &effects);
    assert_eq!(
        bodies,
        (0..8).map(|i| vec![format!("q{i}")]).collect::<Vec<_>>(),
        "the refused head dropped as the oldest; every survivor whole and in order"
    );
}

/// A result for a correlation the kernel never minted is inexplicable — the
/// space is its own and monotone — so it is fatal, like any other unreconcilable
/// server value.
#[test]
fn a_batch_result_for_an_unknown_correlation_is_fatal() {
    let mut core = registered_core(vec![binding("in", 4, 0)], vec![]);
    let effects = core.on_input(
        Input::TextFrame(
            serde_json::to_string(&ServerFrame::PublishBatchResult {
                correlation: 999,
                outcome: PublishBatchOutcome::Ok,
            })
            .unwrap(),
        ),
        Millis(80),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("unknown correlation"), "{detail}");
}

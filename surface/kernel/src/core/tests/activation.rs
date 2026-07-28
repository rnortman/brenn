//! Activation delivery: stores, windows, batching, budgets, flush, parking.
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
/// exact envelopes). Identity is the whole subject of the store's dedup and the
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

/// Feed one `Deliver` per body on `channel`, seqs ascending from 1.
///
/// The shape a page-side drop needs: a binding's view is `max(push_depth,
/// retain_depth)` deep, and only a message pushed out of *that* is lost — what
/// the window still serves was delivered, as context if not as new. So a burst
/// longer than the deepest view on the channel is what charges a cursor.
/// Returns every effect the burst produced: eviction is charged at insert, so the
/// loudness ladder's alert, toast and fatal kill are the *delivery's* effects, not
/// a later activation's.
fn deliver_burst_on(core: &mut ClientCore, channel: &str, bodies: &[&str]) -> Vec<Effect> {
    let mut effects = Vec::new();
    for (i, body) in bodies.iter().enumerate() {
        let seq = i as u64 + 1;
        effects.extend(deliver_ch(core, channel, &env(body, i as u128 + 1), seq, 0));
    }
    effects
}

/// [`deliver_burst_on`] for the suite's default channel.
fn deliver_burst(core: &mut ClientCore, bodies: &[&str]) -> Vec<Effect> {
    deliver_burst_on(core, "ephemeral:demo", bodies)
}

/// Five bodies against a view four deep: the oldest is pushed out of the view
/// entirely, so exactly one message is charged as dropped.
const OVERFLOWING_BURST: &[&str] = &["a", "b", "c", "d", "e"];

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
                ..
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

/// Each binding reads a view **`max(push_depth, retain_depth)` deep of its own**,
/// out of the one store the subscription's bindings share.
///
/// Both depths bound the whole view, not the context half: the store is fed before
/// the window is assembled, so a new message occupies one of the binding's own
/// slots and the context is what is left under the depth. A binding with a 3-deep
/// view and one new message therefore sees 2 context + 1 new, while its sibling
/// reading 2 deep out of the same store sees 1 context + 1 new.
#[test]
fn each_binding_reads_its_own_view_depth_from_the_folded_store() {
    let mut core = registered_core(
        vec![binding("deep", 2, 3), binding("shallow", 2, 1)],
        vec![],
    );
    for (i, body) in ["m1", "m2", "m3"].iter().enumerate() {
        deliver(&mut core, &env(body, i as u128 + 1), i as u64 + 1);
        let ready = take_one(&mut core);
        complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    }
    // The store folded to 3 (the deeper binding's view), and now holds m2..m4.
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
        (vec!["m3"], vec!["m4"]),
        "a 2-deep view out of the same store: one context behind the new one"
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
    // Both are in the store (it is fed by the same delivery), but
    // both are new, so context is empty rather than a duplicate of `new`.
    assert_eq!(split(w), (vec![], vec!["m1", "m2"]));
    assert_eq!(w.envelopes.len(), 2, "no message appears twice");
}

// ── The recovery property ──────────────────────────────────────────────────

/// **The executable backend-recovery property**, and the reason the per-message
/// dialect's gap vocabulary is deleted rather than ported.
///
/// `push_depth = 1`, `retain_depth = 2`, two deliveries in one dispatch turn: only
/// the newest is handed over as new, and the other is nonetheless visible — as
/// context, in the **same** activation. Nothing is charged as dropped: a message
/// the window serves was delivered, whichever side of the boundary it lands on.
/// Loss is the window losing it, not the push half of the window losing it. No gap
/// event, no replay, no component-visible loss.
#[test]
fn a_message_past_the_push_depth_is_context_in_the_same_activation() {
    let mut core = registered_core(vec![binding("in", 1, 2)], vec![]);
    deliver(&mut core, &env("earlier", 1), 1);
    deliver(&mut core, &env("newest", 2), 2);
    let ready = take_one(&mut core);
    let w = window(&ready.activation, "in");
    assert_eq!(
        split(w),
        (vec!["earlier"], vec!["newest"]),
        "the message past the push depth is still in the view, as context"
    );
    assert_eq!(w.dropped, 0, "what the window serves was not dropped");
}

/// Retention displacing a message the binding already saw is not a drop: it is
/// simply no longer in the view, and no drop counter moves for it.
#[test]
fn displacing_a_seen_message_is_not_a_drop() {
    let mut core = registered_core(vec![binding("in", 1, 1)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 0);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    let w = window(&ready.activation, "in");
    // `m1` fell out of the one-deep store — gone from the view, but nothing was
    // dropped: this binding had already been served it.
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
        core.take_ready_activation(TEST_WALL_MS).is_none(),
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
            .take_ready_activation(TEST_WALL_MS)
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
    let first = core
        .take_ready_activation(TEST_WALL_MS)
        .expect("one is ready");
    let second = core
        .take_ready_activation(TEST_WALL_MS)
        .expect("the other is ready too, independently");
    let mut names = vec![first.instance.clone(), second.instance.clone()];
    names.sort();
    assert_eq!(names, vec!["protobar", "sibling"]);
}

// ── Ack semantics ──────────────────────────────────────────────────────────

/// An err consumes: the messages the failed activation was assembled with are
/// behind its position and never re-window as new. Recovery is retention, not
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
        core.take_ready_activation(TEST_WALL_MS).is_none(),
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

/// Drop deltas advance at assembly, not at completion: each window reports the
/// drops since the previous activation moved this binding's position, and never
/// re-reports them.
#[test]
fn drop_deltas_advance_at_ack_and_are_never_double_reported() {
    let mut core = registered_core(vec![binding("in", 1, 4)], vec![]);
    deliver_burst(&mut core, OVERFLOWING_BURST);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 1);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    deliver(&mut core, &env("f", 6), 6);
    let ready = take_one(&mut core);
    assert_eq!(
        window(&ready.activation, "in").dropped,
        0,
        "the earlier drop was already reported; this window reports its own delta"
    );
}

/// A server-reported subscription drop is every *delivered-to* binding's drop:
/// each of them missed those messages. A sampled binding holds no position on the
/// subscription, is never delivered to, and so takes none of the count.
///
/// And the count is *drained* by the activation that reports it, not merely read:
/// the next window reports its own delta, and the ladder's counters do not move
/// again for a loss already accounted for.
#[test]
fn server_reported_drops_count_against_every_binding_on_the_subscription() {
    let mut core = ladder_core_with(
        vec![
            binding_noise("one", 4, 0, NoiseLevel::Metered),
            binding_noise("two", 4, 0, NoiseLevel::Metered),
            binding_noise("sampled", 0, 4, NoiseLevel::Metered),
        ],
        &["ephemeral:demo"],
    );
    deliver_dropped(&mut core, &env("m1", 1), 1, 3);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "one").dropped, 3);
    assert_eq!(window(&ready.activation, "two").dropped, 3);
    assert_eq!(
        window(&ready.activation, "sampled").dropped,
        0,
        "a sampled binding is never delivered to, so it is never reported against"
    );
    assert_eq!(core.metered_drop_count(INST, "one"), 3);
    assert_eq!(core.metered_drop_count(INST, "two"), 3);
    assert_eq!(core.metered_drop_count(INST, "sampled"), 0);

    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    for port in ["one", "two"] {
        assert_eq!(
            window(&ready.activation, port).dropped,
            0,
            "{port} was told about the server's loss once"
        );
        assert_eq!(core.metered_drop_count(INST, port), 3);
    }
}

// ── Loudness ladder: metered counters ──────────────────────────────────────

use brenn_surface_proto::NoiseLevel;

/// The `metered` rung counts a message lost past the binding's view: the drop the
/// window reports at assembly advances the binding's kernel-internal counter.
#[test]
fn metered_binding_counts_a_message_lost_past_its_view() {
    let mut core = registered_core(vec![binding_noise("in", 1, 4, NoiseLevel::Metered)], vec![]);
    deliver_burst(&mut core, OVERFLOWING_BURST);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 1);
    assert_eq!(core.metered_drop_count(INST, "in"), 1);
}

/// A `silent` binding is never counted, even though the drop is still reported
/// honestly on the window — the counter is a rung, not the drop accounting.
#[test]
fn silent_binding_is_uncounted() {
    let mut core = registered_core(vec![binding_noise("in", 1, 4, NoiseLevel::Silent)], vec![]);
    deliver_burst(&mut core, OVERFLOWING_BURST);
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 1);
    assert_eq!(core.metered_drop_count(INST, "in"), 0);
}

/// The `metered` rung counts the other drop origin too: a server-reported
/// subscription drop delta advances the same counter as a page-side eviction.
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
    deliver_burst(&mut core, OVERFLOWING_BURST);
    let ready = take_one(&mut core);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    assert_eq!(core.metered_drop_count(INST, "in"), 1);
    // A second burst past the view charges once more, from a position that had
    // already caught up.
    for (i, body) in ["f", "g", "h", "i", "j"].iter().enumerate() {
        let seq = i as u64 + 6;
        deliver(&mut core, &env(body, u128::from(seq)), seq);
    }
    let ready = take_one(&mut core);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    assert_eq!(core.metered_drop_count(INST, "in"), 2);
}

// ── Loudness ladder: alarm and fatal ─────────────────────────────────────────

use brenn_surface_proto::{AlertSeverity, ToastSeverity, ToastSource};

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

/// Reconnect a [`ladder_core`] on the same single binding at new depths — an
/// operator retuning the wiring, which is the one thing that shrinks a store.
///
/// Returns the `Welcome`'s own effects, which is where a retune's loss lands.
fn ladder_reconnect(
    core: &mut ClientCore,
    noise: NoiseLevel,
    push_depth: u64,
    retain_depth: u64,
) -> Vec<Effect> {
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(1_000),
    );
    core.on_input(Input::Tick, Millis(4_000));
    core.on_input(Input::Opened, Millis(4_001));
    let welcome =
        brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
            subscriptions: vec![Binding {
                channel: "ephemeral:demo".into(),
                instance: INST.into(),
                port: "in".into(),
                push_depth,
                retain_depth,
                noise,
            }],
            components: vec![INST],
            alert_granted: true,
            ..Default::default()
        });
    let effects = core.on_input(Input::TextFrame(welcome), Millis(4_002));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(4_003),
    );
    effects
}

/// A `Welcome` whose depths shrink a store retires messages out from under a
/// lagging position, and that is loss like any other: counted at the reconcile
/// that trimmed it, announced at the binding's next window, and never counted
/// twice. An operator lowering a depth is as accountable a cause of loss as a
/// burst is.
#[test]
fn a_depth_shrink_at_reconnect_charges_the_lagging_binding() {
    // View `max(push 1, retain 4)` = 4 deep, filled exactly: nothing retired yet.
    let mut core = ladder_core("ephemeral:demo", NoiseLevel::Alarm, 1);
    let filling = deliver_burst(&mut core, &["a", "b", "c", "d"]);
    assert_eq!(core.metered_drop_count(INST, "in"), 0);
    assert!(toasts(&filling).is_empty());

    // Retune to a one-deep view. Three of the four leave retention with the
    // position still behind them.
    let retuning = ladder_reconnect(&mut core, NoiseLevel::Alarm, 1, 1);
    assert_eq!(
        core.metered_drop_count(INST, "in"),
        3,
        "the trim is counted where it happened"
    );
    assert!(
        toasts(&retuning).is_empty(),
        "the announcement is still the next window's: {retuning:?}"
    );

    let ready = take_one(&mut core);
    let view = window(&ready.activation, "in");
    assert_eq!(view.dropped, 3, "the guest is told what the trim cost it");
    assert_eq!(split(view).1, vec!["d"], "the survivor is still new");
    let shown = toasts(&ready.effects);
    assert_eq!(shown.len(), 1, "one announcement: {:?}", ready.effects);
    assert!(
        shown[0].text.contains("dropped 3"),
        "naming the trimmed span: {}",
        shown[0].text
    );
    assert_eq!(alerts(&ready.effects).len(), 1);
    assert_eq!(
        core.metered_drop_count(INST, "in"),
        3,
        "the window's own charge is the still-retained remainder: nothing"
    );
}

/// The same trim under a `fatal` binding kills the instance at the reconcile: the
/// kill is not deferrable, so it and its announcement ride the trim.
#[test]
fn a_depth_shrink_kills_a_fatal_binding_at_the_reconcile() {
    let mut core = ladder_core("ephemeral:demo", NoiseLevel::Fatal, 1);
    deliver_burst(&mut core, &["a", "b", "c", "d"]);
    assert!(!core.is_failed(INST));

    // The `Welcome` that trimmed is the frame that kills.
    let retuning = ladder_reconnect(&mut core, NoiseLevel::Fatal, 1, 1);
    let failures = instance_failures(&retuning);
    assert_eq!(failures.len(), 1, "one kill: {retuning:?}");
    assert_eq!(failures[0].0, INST);
    assert!(
        failures[0].1.contains("3 message(s)"),
        "the reason names the trimmed span: {}",
        failures[0].1
    );
    // Cumulative: the fatal rung announces here too, because there is no next
    // window to defer to.
    assert_eq!(toasts(&retuning).len(), 1);
    assert_eq!(alerts(&retuning).len(), 1);

    assert!(core.is_failed(INST));
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "a killed instance never activates again"
    );
}

/// A registered core on arbitrary input bindings, holding the alert grant every
/// `alarm`/`fatal` binding is proven to have at boot, with every named
/// subscription acked.
fn ladder_core_with(subscriptions: Vec<Binding>, channels: &[&str]) -> ClientCore {
    let welcome =
        brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
            subscriptions,
            components: vec![INST],
            alert_granted: true,
            ..Default::default()
        });
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(Input::TextFrame(welcome), Millis(2));
    register(&mut core, INST, Millis(5));
    for channel in channels {
        core.on_input(
            Input::TextFrame(subscribe_result(channel, SubscribeOutcome::Ok)),
            Millis(6),
        );
    }
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
    ladder_core_with(vec![mk(a), mk(b)], &[a.0, b.0])
}

/// Two bindings of one instance on **one** channel: a deep sibling whose view
/// covers the whole store, and a shallow one that lags inside it. That is the only
/// shape in which a binding loses messages the store still retains — the ladder's
/// third drop origin, `Advance::noise_charge`, which no single-binding fixture can
/// produce because there a binding's view is the store's whole depth.
fn shallow_and_deep_ladder_core(shallow_noise: NoiseLevel) -> ClientCore {
    let mk = |port: &str, push_depth: u64, retain_depth: u64, noise: NoiseLevel| Binding {
        channel: "ephemeral:demo".into(),
        instance: INST.into(),
        port: port.into(),
        push_depth,
        retain_depth,
        noise,
    };
    ladder_core_with(
        vec![
            mk("deep", 8, 8, NoiseLevel::Metered),
            mk("shallow", 1, 1, shallow_noise),
        ],
        &["ephemeral:demo"],
    )
}

/// The third drop origin, on its own: a shallow binding whose advance passes a
/// span the store still retains. Nothing is evicted and the server reports
/// nothing, so every figure here comes from `Advance::noise_charge` — counted and
/// announced at the assembly that passed it.
#[test]
fn a_shallow_binding_is_charged_for_the_retained_span_its_window_skipped() {
    let mut core = shallow_and_deep_ladder_core(NoiseLevel::Alarm);
    // Four into an eight-deep store: nothing leaves retention.
    let arrivals = deliver_burst(&mut core, &["a", "b", "c", "d"]);
    assert_eq!(wire_bodies(&core, INST, "ephemeral:demo").len(), 4);
    assert!(
        toasts(&arrivals).is_empty() && instance_failures(&arrivals).is_empty(),
        "no retirement happened: {arrivals:?}"
    );

    let ready = take_one(&mut core);
    // The deep sibling's view covers the store, so it loses nothing.
    let deep = window(&ready.activation, "deep");
    assert_eq!(deep.dropped, 0);
    assert_eq!(split(deep).1, vec!["a", "b", "c", "d"]);
    assert_eq!(core.metered_drop_count(INST, "deep"), 0);
    // The shallow one is served the newest and charged the three its one-deep
    // window never showed it — messages the store still holds for its sibling.
    let shallow = window(&ready.activation, "shallow");
    assert_eq!(shallow.dropped, 3);
    assert_eq!(split(shallow), (vec![], vec!["d"]));
    assert_eq!(core.metered_drop_count(INST, "shallow"), 3);

    let shown = toasts(&ready.effects);
    assert_eq!(shown.len(), 1, "the shallow binding alone: {shown:?}");
    assert!(shown[0].text.contains("dropped 3"));
    assert!(shown[0].text.contains("shallow"));
    let raised = alerts(&ready.effects);
    assert_eq!(raised.len(), 1);
    assert!(raised[0].2.contains("shallow"));
}

/// The ladder's books balance against the cursor model's own, per binding, over a
/// history mixing all three drop origins: an eviction the store outran, a
/// still-retained span a shallow window skipped, and a server-reported delta.
///
/// The invariant: what the ladder counted for a binding equals what the guest was
/// told it lost. Nothing double-counted (the eviction charge and the advance's
/// charge are disjoint spans), nothing lost (the two page-side sites plus the wire
/// addend are the whole of it).
#[test]
fn the_ladder_totals_match_the_guests_dropped_over_a_mixed_history() {
    let mut core = shallow_and_deep_ladder_core(NoiseLevel::Metered);
    // Ten into an eight-deep store, the last one reporting two lost upstream:
    // seqs 1 and 2 are evicted with both positions still behind them.
    for seq in 1..=10u64 {
        let dropped = if seq == 10 { 2 } else { 0 };
        deliver_ch(
            &mut core,
            "ephemeral:demo",
            &env(&format!("m{seq}"), u128::from(seq)),
            seq,
            dropped,
        );
    }
    // Counted at the evictions: each position was outrun by two of them.
    assert_eq!(core.metered_drop_count(INST, "deep"), 2);
    assert_eq!(core.metered_drop_count(INST, "shallow"), 2);

    let ready = take_one(&mut core);
    // The deep binding: two evicted + two reported by the server.
    let deep = window(&ready.activation, "deep");
    assert_eq!(deep.dropped, 4);
    assert_eq!(split(deep).1.len(), 8, "the whole store, all of it new");
    // The shallow binding: the same two evictions and the same server pair, plus
    // the seven retained messages its one-deep window skipped.
    let shallow = window(&ready.activation, "shallow");
    assert_eq!(shallow.dropped, 11);
    assert_eq!(split(shallow), (vec![], vec!["m10"]));

    for (port, expected) in [("deep", 4u64), ("shallow", 11)] {
        assert_eq!(
            core.metered_drop_count(INST, port),
            expected,
            "the ladder's total for {port} is the figure the guest was given",
        );
    }
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

/// `alarm` on a message lost past the view: the cumulative rung counts at the
/// loss, then raises exactly one backend `Alert` (severity `Warning`) and one
/// coalesced toast at the binding's next window, both naming the delta. The
/// instance is not killed.
///
/// The two moments are the contract: the counter moves where the loss happens —
/// the delivery whose insert pushed the oldest entry out from under a lagging
/// position — while the announcement waits for the window that reports the drop,
/// so a binding that lags by many messages is announced once, not once per
/// message.
#[test]
fn alarm_binding_alerts_and_toasts_on_a_message_lost_past_its_view() {
    let mut core = ladder_core("ephemeral:demo", NoiseLevel::Alarm, 1);
    let evicting = deliver_burst(&mut core, OVERFLOWING_BURST);
    assert_eq!(core.metered_drop_count(INST, "in"), 1);
    assert!(
        alerts(&evicting).is_empty() && toasts(&evicting).is_empty(),
        "the announcement is the next window's, not the delivery's: {evicting:?}"
    );
    assert!(instance_failures(&evicting).is_empty());
    assert!(!core.is_failed(INST));

    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 1);

    let raised = alerts(&ready.effects);
    assert_eq!(raised.len(), 1, "one alert: {:?}", ready.effects);
    assert_eq!(raised[0].0, AlertSeverity::Warning);
    assert!(
        raised[0].2.contains("dropped 1"),
        "names the delta: {}",
        raised[0].2
    );

    let shown = toasts(&ready.effects);
    assert_eq!(shown.len(), 1, "one coalesced toast: {:?}", ready.effects);
    assert_eq!(shown[0].severity, ToastSeverity::Warning);
    assert_eq!(shown[0].source, ToastSource::Kernel);
    assert!(shown[0].text.contains("dropped 1"));

    // Counted once, at the eviction: the window's own charge is the still-retained
    // remainder, which is nothing here.
    assert_eq!(core.metered_drop_count(INST, "in"), 1);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    deliver(&mut core, &env("f", 6), 6);
    let ready = take_one(&mut core);
    assert!(
        alerts(&ready.effects).is_empty() && toasts(&ready.effects).is_empty(),
        "an announced drop is never announced again: {:?}",
        ready.effects
    );
}

/// A sustained lag announces once per activation, not once per lost message.
/// Ten messages against a four-deep view retires six of them, and the binding
/// hears about all six in one alert and one toast naming six.
#[test]
fn a_sustained_lag_announces_once_naming_the_whole_delta() {
    let mut core = ladder_core("ephemeral:demo", NoiseLevel::Alarm, 1);
    let mut effects = Vec::new();
    for seq in 1..=10u64 {
        effects.extend(deliver_ch(
            &mut core,
            "ephemeral:demo",
            &env(&format!("m{seq}"), u128::from(seq)),
            seq,
            0,
        ));
    }
    assert!(
        alerts(&effects).is_empty() && toasts(&effects).is_empty(),
        "no alert storm across the burst: {} alerts, {} toasts",
        alerts(&effects).len(),
        toasts(&effects).len()
    );
    // The view is `max(push 1, retain 4)` = 4 deep: six of the ten were retired
    // under the position, and of the four the window still serves, three are
    // context and the newest is new.
    let ready = take_one(&mut core);
    assert_eq!(window(&ready.activation, "in").dropped, 6);
    assert_eq!(split(window(&ready.activation, "in")).1, vec!["m10"]);
    assert_eq!(alerts(&ready.effects).len(), 1);
    let shown = toasts(&ready.effects);
    assert_eq!(shown.len(), 1);
    assert!(
        shown[0].text.contains("dropped 6"),
        "the coalesced delta: {}",
        shown[0].text
    );
    assert_eq!(core.metered_drop_count(INST, "in"), 6);
}

/// `alarm` fires on the other drop origin too: a server-reported subscription
/// delta raises the same alert and toast as a page-side eviction.
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
/// the loss, `is_failed`, and no further activation on new traffic.
///
/// The kill lands at the loss, so the instance never runs again — not even for the
/// activation the surviving messages would otherwise have caused.
#[test]
fn fatal_binding_kills_the_instance_and_stays_terminal() {
    let mut core = ladder_core("ephemeral:demo", NoiseLevel::Fatal, 1);
    let evicting = deliver_burst(&mut core, OVERFLOWING_BURST);

    assert_eq!(alerts(&evicting).len(), 1, "fatal is cumulative: alerts");
    assert_eq!(toasts(&evicting).len(), 1, "fatal is cumulative: toasts");

    let failures = instance_failures(&evicting);
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
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "the killed instance was dispatched for the messages it survived"
    );
    // Terminal: new traffic never re-activates the killed instance either.
    deliver_ch(&mut core, "ephemeral:demo", &env("z", 26), 6, 0);
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
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
        deliver_burst_on(&mut core, channel, OVERFLOWING_BURST);
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

    // Push a message past the doomed instance's view — the kill lands right there,
    // on the delivery that evicted it.
    let mut doomed_effects = Vec::new();
    for (i, body) in OVERFLOWING_BURST.iter().enumerate() {
        let seq = i as u64 + 1;
        doomed_effects.extend(core.on_input(
            Input::TextFrame(deliver_frame_for_dropped(
                "ephemeral:demo",
                INST,
                &env(body, i as u128 + 1),
                seq,
                0,
            )),
            Millis(20 + seq),
        ));
    }
    assert_eq!(instance_failures(&doomed_effects).len(), 1);
    core.on_input(
        Input::TextFrame(deliver_frame_for_dropped(
            "ephemeral:sib",
            SIB,
            &env("s", 30),
            1,
            0,
        )),
        Millis(30),
    );

    // Every dispatch left is the sibling's: the doomed instance is terminal and
    // holds no position, while its neighbour windows and delivers as usual.
    let mut sibling_windowed = false;
    while let Some(ready) = core.take_ready_activation(TEST_WALL_MS) {
        assert_eq!(
            ready.instance, SIB,
            "the killed instance was dispatched anyway"
        );
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
                deliver_after: None,
            },
            BatchEntry {
                port: "out".into(),
                body: "second".into(),
                urgency: Some(Urgency::High),
                deliver_after: None,
            },
        ]
    );
}

/// `local:` entries commit through the router at the flush point — seq assigned,
/// store fed, fan-out — and never ride the wire.
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
    // Committed through the router at the flush point: seq assigned, store fed —
    // the depth-1 plane holds it for whatever attaches later.
    let replayed: Vec<(String, String)> = core
        .stores
        .get(&StoreKey::Confined(LOCAL_THEME_CHANNEL.to_string()))
        .expect("the reserved plane's store")
        .retained()
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
/// instance only. Its stores survive and keep being fed; a sibling is untouched.
#[test]
fn trap_is_terminal_for_one_instance_and_its_stores_survive() {
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
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "a failed instance never activates again"
    );
    // But its store kept filling — retention is the channel's, page-lifetime,
    // and inert rather than corrupt.
    assert_eq!(wire_bodies(&core, INST, "ephemeral:demo"), vec!["m1", "m2"]);
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

/// A depth-0 binding never activates its instance and holds no position — but
/// its channel's store is fed throughout, and it windows as pure context when a
/// sibling port does the waking. Depth 0 means "don't activate me", never
/// "don't show me".
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
        core.take_ready_activation(TEST_WALL_MS).is_none(),
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
        "no position, so nothing is ever reported against it"
    );
}

// ── Registration seam ──────────────────────────────────────────────────────

/// Deregistration drops the entry's positions, and — because it releases the
/// instance's last reference on the subscription — the mirror those positions read
/// too. A re-register is a fresh consumer of the subscription: it starts from an
/// empty store, so only what arrives after it reaches its window.
#[test]
fn deregistration_drops_positions_and_the_mirror_with_them() {
    let mut core = registered_core(vec![binding("in", 4, 2)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    core.on_input(
        Input::ActivationDeregistered {
            instance: INST.into(),
        },
        Millis(30),
    );
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "no entry, no activation"
    );
    // Re-registering opens the subscription afresh — a registered instance is a
    // subscriber like any other — and the fresh `Subscribe` is what catches it up.
    register(&mut core, INST, Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(31),
    );
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")),
        (vec![], vec!["m2"]),
        "the mirror went with the reference; nothing older than the new subscription \
         is page-side history"
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

// ── Stores ─────────────────────────────────────────────────────────────────

/// The store's depth is the fold over the subscription's bindings of
/// `max(push_depth, retain_depth)`, and the store is fed for the subscription —
/// not per binding. Retention is a property of the channel as the subscription
/// sees it, which is why one store serves two ports reading it at two depths,
/// and the push halves are in the fold because the store is what holds what
/// those ports will be served.
#[test]
fn store_depth_is_the_fold_over_the_subscriptions_bindings() {
    let mut core = active_core_with(vec![binding("shallow", 4, 1), binding("deep", 4, 3)]);
    register(&mut core, INST, Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    let key = SubKey::for_instance(INST, "ephemeral:demo");
    assert_eq!(
        core.stores
            .get(&StoreKey::Wire(key.clone()))
            .expect("store exists")
            .depth(),
        4,
        "max over the instance's bindings of max(push_depth, retain_depth)"
    );
    for i in 1..=4u64 {
        deliver(&mut core, &env(&format!("m{i}"), i as u128), i);
    }
    assert_eq!(
        wire_bodies(&core, INST, "ephemeral:demo"),
        vec!["m1", "m2", "m3", "m4"],
        "bounded by the fold, oldest out"
    );
}

/// A late position on a surviving mirror is primed from the mirror's tail,
/// capped at its own push depth, and charged nothing.
///
/// The mirror is the subscription's honest recent history of the channel — a
/// store holds no messages it disavows — so a binding coming into existence on
/// one is owed `min(push_depth, tail)` of it as new, exactly as on any other
/// store. This is the surviving-subscription case specifically: no fresh
/// `Subscribe` will replay for it, so the prime is the only catch-up there is.
#[test]
fn a_late_position_is_primed_from_the_surviving_mirrors_tail() {
    let mut core = registered_core(vec![binding("in", 4, 1)], vec![]);
    let store = core
        .stores
        .get(&StoreKey::Wire(SubKey::for_instance(
            INST,
            "ephemeral:demo",
        )))
        .expect("store exists");
    assert_eq!(store.depth(), 4, "the fold over the bindings");

    for i in 1..=4u64 {
        deliver(&mut core, &env(&format!("m{i}"), i as u128), i);
    }
    let ready = take_one(&mut core);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    // A second port bound on the same channel: the instance keeps its reference
    // throughout (references are diffed, never dropped and retaken), so the mirror
    // survives and priming from it is the new position's only catch-up.
    reconnect(
        &mut core,
        vec![binding("in", 4, 1), binding("late", 4, 1)],
        vec![],
    );
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "late")),
        (vec![], vec!["m1", "m2", "m3", "m4"]),
        "the whole tail is new: four held, a push depth of four to hold them"
    );
    assert_eq!(
        window(&ready.activation, "late").dropped,
        0,
        "priming charges nothing"
    );
}

/// **The store's insert is idempotent by `message_id`.** Stores survive
/// reconnect while several reconnect paths legitimately re-present what a store
/// already holds (fresh-attach replay, gap-past-retention replay, epoch-change
/// replay). Without the dedup a post-reconnect window's context would carry the
/// same message twice — a shape the backend's distinct-row context read can
/// never produce.
#[test]
fn the_store_insert_is_idempotent_and_survives_reconnect() {
    let mut core = registered_core(vec![binding("in", 4, 4)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    deliver(&mut core, &env("m2", 2), 2);
    let ready = take_one(&mut core);
    complete(&mut core, INST, ActivationOutcome::Ok, ready.buffer);
    // Drop the link and come back; the page did not reload, so the ring must not
    // have been discarded.
    reconnect(&mut core, vec![binding("in", 4, 4)], vec![]);
    // The server replays what it retained: the same two envelopes, same ids. The
    // store already holds them, so nothing is taken and nothing is owed — a
    // message already delivered is not delivered a second time.
    deliver(&mut core, &env("m1", 1), 3);
    deliver(&mut core, &env("m2", 2), 4);
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "a replayed envelope re-woke the instance"
    );
    assert_eq!(
        wire_bodies(&core, INST, "ephemeral:demo"),
        vec!["m1", "m2"],
        "one copy of each, whatever the replay presented"
    );
    // A genuinely new message activates, and carries the replayed pair as context
    // exactly once each.
    deliver(&mut core, &env("m3", 3), 5);
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
    assert_eq!(split(w), (vec!["m1", "m2"], vec!["m3"]));
}

/// A subscription no surviving binding names loses its store: nothing can route
/// on it again.
#[test]
fn a_store_whose_binding_vanished_is_dropped_at_reconcile() {
    let mut core = registered_core(vec![binding("in", 4, 2)], vec![]);
    deliver(&mut core, &env("m1", 1), 1);
    let key = SubKey::for_instance(INST, "ephemeral:demo");
    assert!(core.stores.contains_key(&StoreKey::Wire(key.clone())));
    // Bindings change only across a reconnect: a second `Welcome` on a live
    // connection is a fatal protocol error.
    reconnect(
        &mut core,
        vec![binding_on("ephemeral:other", "in", 4, 2)],
        vec![],
    );
    assert!(
        !core.stores.contains_key(&StoreKey::Wire(key)),
        "the operator un-declared the binding; its store goes with it"
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
pub(super) fn answer(
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
    assert!(core.take_ready_activation(TEST_WALL_MS).is_some());
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
    let b = core
        .take_ready_activation(TEST_WALL_MS)
        .expect("B is ready");
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
        let a = core
            .take_ready_activation(TEST_WALL_MS)
            .expect("A is ready");
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

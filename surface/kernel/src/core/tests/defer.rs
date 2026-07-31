//! Deferred publishes on a confined channel: the page parks them, times them,
//! releases them into retention, and lets the component that scheduled them
//! cancel or rewrite one before it releases.
//!
//! A confined channel's retention authority is the page, so its schedule is the
//! page's too — the same rule that puts the mint here. What is under test is that
//! a parked message is *not on the channel*: nothing retains it, nothing is woken
//! by it, and no window shows it, until its release time arrives and it enters
//! retention as an ordinary arrival — and that a component can act on its own
//! schedule by naming an index into the window it was handed, with the identity
//! behind that index doing the work.

use super::*;
use crate::publish_buffer::OutputSpec;
use crate::test_support::welcome_frame_local;
use brenn_surface_contract::{DeferError, PublishError};
use brenn_surface_schema::{LocalChannel, NoiseLevel, OutputBinding};
use brenn_surface_test_fixtures::FIXTURE_MAX_BODY_BYTES;

/// An operator-declared confined channel — nothing about it is contract-fixed, so
/// a fixture picks its depth.
const EVENTS: &str = "local:app-events";
/// A second one, for the timer's across-channels minimum.
const OTHER: &str = "local:app-other";
/// A transportable channel, for the port whose deferral authority is the backend.
const WIRE: &str = "ephemeral:demo";

/// One minute past the fixture wall clock: the ordinary "schedule it for later".
const LATER: u64 = TEST_WALL_MS + 60_000;

fn local_channel(channel: &str, ring_depth: u64) -> LocalChannel {
    LocalChannel {
        channel: channel.into(),
        ring_depth,
    }
}

fn input(channel: &str, instance: &str, port: &str, push_depth: u64, retain_depth: u64) -> Binding {
    Binding {
        channel: channel.into(),
        instance: instance.into(),
        port: port.into(),
        push_depth,
        retain_depth,
        noise: NoiseLevel::Silent,
    }
}

/// An output binding with a generous sink bucket: these tests are about schedules,
/// not budgets, so several publishes from one activation must be affordable.
fn output(channel: &str, instance: &str, port: &str) -> OutputBinding {
    OutputBinding {
        channel: channel.into(),
        instance: instance.into(),
        port: port.into(),
        urgency: brenn_surface_schema::Urgency::Normal,
        fill_mt: 10 * brenn_budget::MILLITOKENS_PER_PUBLISH,
        capacity_mt: 10 * brenn_budget::MILLITOKENS_PER_PUBLISH,
    }
}

/// A core with `protobar` reading [`EVENTS`] on port `in` and publishing to it on
/// port `out`, the channel declared at `depth`.
///
/// The publisher reads what it publishes deliberately: a released message is
/// supposed to reach its channel's readers exactly as a fresh publish does, and
/// the shortest way to see that is the component's own next window.
fn deferring_core(depth: u64) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![input(EVENTS, "protobar", "in", depth, depth)],
            vec![output(EVENTS, "protobar", "out")],
            vec![local_channel(EVENTS, depth)],
        )),
        Millis(2),
    );
    register(&mut core, "protobar", Millis(3));
    core
}

/// The armed release deadline in an effect list, or `None` when the list re-armed
/// nothing — the core states the deadline only when it moves.
fn armed_release(effects: &[Effect]) -> Option<Option<u64>> {
    effects.iter().find_map(|e| match e {
        Effect::SetReleaseWakeup(deadline) => Some(*deadline),
        _ => None,
    })
}

/// What a confined channel retains, oldest first, with the seq each entry holds.
fn retained_with_seqs(core: &ClientCore, channel: &str) -> Vec<(String, u64)> {
    core.stores
        .get(&StoreKey::Confined(channel.to_string()))
        .expect("the channel's store")
        .retained()
        .map(|(e, seq)| (e.body.clone(), seq))
        .collect()
}

/// A core with `protobar` publishing to a transportable channel on `wire-out` and
/// to [`EVENTS`] on `out`, reading [`EVENTS`] on `in`.
fn wire_and_confined_core() -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![input(EVENTS, "protobar", "in", 4, 4)],
            vec![
                output(WIRE, "protobar", "wire-out"),
                output(EVENTS, "protobar", "out"),
            ],
            vec![local_channel(EVENTS, 4)],
        )),
        Millis(2),
    );
    register(&mut core, "protobar", Millis(3));
    core
}

/// A core with `protobar` and `sink` both reading and publishing [`EVENTS`] — two
/// senders on one channel, which is what makes the sender scoping observable.
fn shared_channel_core() -> ClientCore {
    let welcome =
        brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
            subscriptions: vec![
                input(EVENTS, "protobar", "in", 4, 4),
                input(EVENTS, "sink", "in", 4, 4),
            ],
            outputs: vec![
                output(EVENTS, "protobar", "out"),
                output(EVENTS, "sink", "out"),
            ],
            alert_granted: false,
            takeover_granted: false,
            components: vec!["protobar", "sink"],
            error_report_floor: None,
            surface_description: brenn_surface_schema::SurfaceDescription {
                status_interval_secs: 60,
            },
            local_channels: vec![local_channel(EVENTS, 4)],
            max_body_bytes: brenn_surface_test_fixtures::FIXTURE_MAX_BODY_BYTES,
        });
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(Input::TextFrame(welcome), Millis(2));
    register(&mut core, "protobar", Millis(3));
    register(&mut core, "sink", Millis(3));
    core
}

/// Run every ready activation, parking `body` for `at` from `parker`'s and from
/// nobody else's.
fn park_from(core: &mut ClientCore, parker: &str, body: &str, at: u64) {
    while let Some(ready) = core.take_ready_activation(TEST_WALL_MS) {
        let instance = ready.instance.clone();
        let mut buffer = ready.buffer;
        if instance == parker {
            buffer
                .publish_deferred("out", body.to_string(), at)
                .expect("the port is bound");
        }
        complete(core, &instance, ActivationOutcome::Ok, buffer);
    }
}

/// Wake `protobar` with a gesture publish, then park `body` for `release_at` from
/// inside the activation that wakes. Returns the flush's effects.
fn park_one(core: &mut ClientCore, wake: &str, body: &str, release_at: u64) -> Vec<Effect> {
    publish(core, 1, "protobar", "out", wake, Millis(4));
    let ready = take_one(core);
    let mut buffer = ready.buffer;
    buffer
        .publish_deferred("out", body.to_string(), release_at)
        .expect("the port is bound and the release time is representable");
    complete(core, "protobar", ActivationOutcome::Ok, buffer)
}

/// Wake `protobar` with the gesture publish `wake`, hand its buffer to `act`, and
/// complete the activation ok. Returns the flush's effects.
///
/// The wake is a real publish on [`EVENTS`], so the assembly that follows carries
/// the deferred window an op indexes into.
fn with_activation(
    core: &mut ClientCore,
    correlation: u64,
    wake: &str,
    act: impl FnOnce(&mut PublishBuffer),
) -> Vec<Effect> {
    publish(core, correlation, "protobar", "out", wake, Millis(6));
    let ready = take_one(core);
    let mut buffer = ready.buffer;
    act(&mut buffer);
    complete(core, "protobar", ActivationOutcome::Ok, buffer)
}

/// The deferred window `protobar` sees for `port` on its next activation, as
/// `(index, payload, deliver_after)` triples, plus the `now` it was handed.
fn deferred_view(core: &mut ClientCore, port: &str) -> (Vec<(u32, String, u64)>, Option<u64>) {
    let ready = take_one(core);
    let window = ready
        .activation
        .deferred
        .iter()
        .find(|w| w.port == port)
        .expect("a window for every bound output port");
    let entries = window
        .entries
        .iter()
        .map(|e| (e.index, e.payload.clone(), e.deliver_after))
        .collect();
    let now = ready.activation.now;
    complete(core, "protobar", ActivationOutcome::Ok, ready.buffer);
    (entries, now)
}

#[test]
fn a_deferred_publish_parks_out_of_retention_and_wakes_nobody() {
    // The whole of what parking means: the channel does not hold the message, so
    // no window can serve it and no position is owed it. A parked publish is a
    // schedule, not a delivery — and the flush that made it says so by arming the
    // timer instead of fanning anything out.
    let mut core = deferring_core(4);
    let effects = park_one(&mut core, "wake", "later", LATER);

    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake"],
        "the parked body is not retained"
    );
    assert_eq!(
        armed_release(&effects),
        Some(Some(LATER)),
        "the flush arms the release timer at the schedule: {effects:?}"
    );
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "parking wakes nobody"
    );
}

#[test]
fn a_released_message_arrives_as_a_fresh_publish_at_the_tail() {
    // A release is an arrival, not a special case: it takes the next seq at the
    // tail and reaches every position reading the channel as new. That is the
    // whole reason the release goes through the same append the mint does.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "later", LATER);

    let effects = core.on_input(Input::ReleaseDue { now_ms: LATER }, Millis(60));
    assert_eq!(
        armed_release(&effects),
        Some(None),
        "the last schedule released, so the timer disarms: {effects:?}"
    );
    assert_eq!(
        retained_with_seqs(&core, EVENTS),
        vec![("wake".to_string(), 1), ("later".to_string(), 2)],
        "the released message takes a fresh tail seq"
    );

    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")).1,
        vec!["later"],
        "the released message is new to the channel's reader"
    );
}

#[test]
fn a_release_time_already_past_publishes_immediately() {
    // The contract every host gives a `deliver_after` in the past: publish now.
    // The comparison is against the flush's own clock read, so "past" means past
    // at the moment the message would have been parked.
    let mut core = deferring_core(4);
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    buffer
        .publish_deferred("out", "now".to_string(), TEST_WALL_MS - 1)
        .expect("a past release time is a valid one");
    let effects = complete_at(
        &mut core,
        "protobar",
        ActivationOutcome::Ok,
        buffer,
        TEST_WALL_MS,
    );

    assert_eq!(confined_bodies(&core, EVENTS), vec!["wake", "now"]);
    assert_eq!(
        armed_release(&effects),
        None,
        "nothing parked, so no deadline moved: {effects:?}"
    );
}

#[test]
fn a_release_time_at_the_flush_instant_publishes_immediately() {
    // The boundary: parking is exactly `release_at > now`, so a schedule for this
    // very instant has nothing left to wait for.
    let mut core = deferring_core(4);
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    buffer
        .publish_deferred("out", "now".to_string(), TEST_WALL_MS)
        .expect("the release time is representable");
    complete_at(
        &mut core,
        "protobar",
        ActivationOutcome::Ok,
        buffer,
        TEST_WALL_MS,
    );

    assert_eq!(confined_bodies(&core, EVENTS), vec!["wake", "now"]);
}

#[test]
fn a_deferred_publish_from_a_failed_activation_schedules_nothing() {
    // Deferral rides the flush rule unchanged: an err discards the buffer, so the
    // schedule it held never existed. A timer set by an activation that failed
    // would be the one publish an err could not take back.
    let mut core = deferring_core(4);
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    buffer
        .publish_deferred("out", "later".to_string(), LATER)
        .expect("the port is bound");
    let effects = complete(
        &mut core,
        "protobar",
        ActivationOutcome::Err(ActivationError {
            message: "no".into(),
        }),
        buffer,
    );

    assert_eq!(
        armed_release(&effects),
        None,
        "a discarded buffer arms nothing: {effects:?}"
    );
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));
    let (entries, _) = deferred_view(&mut core, "out");
    assert!(entries.is_empty(), "nothing was parked: {entries:?}");
}

#[test]
fn the_deferred_view_is_release_ordered_and_carries_the_activations_clock() {
    // The view is what the component needs to act on its own schedule: release
    // order, contiguous indices, and the bodies it handed the host — not
    // envelopes, which a parked message is not yet one of. `now` is the same read
    // the view's boundary was taken at, so a component computing a new release
    // time from it cannot be off by the assembly.
    let mut core = deferring_core(4);
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    for (body, at) in [("third", 30_000), ("first", 10_000), ("second", 20_000)] {
        buffer
            .publish_deferred("out", body.to_string(), TEST_WALL_MS + at)
            .expect("the port is bound");
    }
    complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);

    // Nothing new on `in`, so the instance is not ready — a gesture publish wakes
    // it so the next assembly can be read.
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));
    let (entries, now) = deferred_view(&mut core, "out");
    assert_eq!(
        entries,
        vec![
            (0, "first".to_string(), TEST_WALL_MS + 10_000),
            (1, "second".to_string(), TEST_WALL_MS + 20_000),
            (2, "third".to_string(), TEST_WALL_MS + 30_000),
        ]
    );
    assert_eq!(now, Some(TEST_WALL_MS));
}

#[test]
fn an_entry_due_at_the_assembly_instant_is_out_of_the_view() {
    // Still parked is `release_at > now`: an entry whose time has come is out of
    // the view before the release pass takes it, because there is nothing left to
    // cancel or edit. Its release is the next thing that happens to it.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "due", LATER);
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));

    let ready = core
        .take_ready_activation(LATER)
        .expect("an activation is ready");
    let window = ready
        .activation
        .deferred
        .iter()
        .find(|w| w.port == "out")
        .expect("a window for the bound output port");
    assert!(
        window.entries.is_empty(),
        "a due entry is out of the view: {:?}",
        window.entries
    );
}

#[test]
fn a_deferred_publish_on_a_transportable_port_rides_the_batch_frame() {
    // A transportable channel's deferral authority is the backend, so the page
    // parks nothing and holds nothing: it states the release time on the wire
    // entry verbatim and lets the server decide park-vs-immediate.
    let mut core = wire_and_confined_core();
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    buffer
        .publish_deferred("wire-out", "scheduled".to_string(), LATER)
        .expect("the port is bound and the release time is representable");
    buffer
        .publish("wire-out", "now".to_string())
        .expect("the port is bound");
    let effects = complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);

    let sent: Vec<(String, Option<u64>)> = effects
        .iter()
        .find_map(|e| match e {
            Effect::SendFrame(brenn_surface_schema::ClientFrame::PublishBatch {
                publishes,
                ..
            }) => Some(publishes.clone()),
            _ => None,
        })
        .expect("the flush frames a batch")
        .into_iter()
        .map(|e| (e.body, e.deliver_after))
        .collect();
    assert_eq!(
        sent,
        vec![
            ("scheduled".to_string(), Some(LATER)),
            ("now".to_string(), None),
        ],
        "the release time rides its own entry and only its own"
    );
    assert_eq!(
        armed_release(&effects),
        None,
        "no page-side schedule, so no deadline moved: {effects:?}"
    );
}

#[test]
fn every_bound_output_port_gets_a_window_in_config_order() {
    // The index a component names is only meaningful against a window that is
    // always there, so every bound output port appears whether or not it has a
    // schedule. The transportable port's window is empty here because no
    // `DeferredView` has arrived for it, which is how an empty set is stated.
    let mut core = wire_and_confined_core();
    park_one(&mut core, "wake", "later", LATER);
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));

    let ready = take_one(&mut core);
    let ports: Vec<&str> = ready
        .activation
        .deferred
        .iter()
        .map(|w| w.port.as_str())
        .collect();
    assert_eq!(ports, vec!["wire-out", "out"], "config order");
    assert!(
        ready.activation.deferred[0].entries.is_empty(),
        "no view has been pushed for the transportable channel"
    );
    assert_eq!(
        ready.activation.deferred[1]
            .entries
            .iter()
            .map(|e| e.payload.as_str())
            .collect::<Vec<_>>(),
        vec!["later"]
    );
}

/// The `DeferredView` frame the backend pushes, as a `(message_id, body,
/// deliver_after)` triple per entry in release order.
fn deferred_view_frame(channel: &str, instance: &str, entries: &[(u128, &str, u64)]) -> String {
    serde_json::to_string(&ServerFrame::DeferredView {
        channel: channel.into(),
        instance: instance.into(),
        entries: entries
            .iter()
            .map(|(id, body, deliver_after)| DeferredViewEntry {
                message_id: Uuid::from_u128(*id),
                body: (*body).to_string(),
                deliver_after: *deliver_after,
            })
            .collect(),
    })
    .unwrap()
}

#[test]
fn a_transportable_ports_window_is_the_view_the_backend_pushed() {
    // The backend is the deferral authority for a transportable channel, so the
    // page's window on that port is the snapshot it was handed — verbatim, in the
    // order it arrived, with the page's own indices laid over it. Nothing is
    // re-derived: the page holds no deferred set for the channel to compute one
    // from.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xa1, "first", LATER), (0xa2, "second", LATER + 1_000)],
        )),
        Millis(4),
    );

    publish(&mut core, 1, "protobar", "out", "wake", Millis(5));
    let (entries, _) = deferred_view(&mut core, "wire-out");
    assert_eq!(
        entries,
        vec![
            (0, "first".to_string(), LATER),
            (1, "second".to_string(), LATER + 1_000),
        ],
        "the window is the pushed view, release-ordered and contiguously indexed"
    );
}

#[test]
fn a_deferred_view_replaces_the_mirror_wholesale() {
    // The frame is a full snapshot, so the last one wins outright — including an
    // empty one, which is how the backend says the set drained. A merge would
    // leave the page holding a schedule the authority no longer has.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xb1, "stale", LATER), (0xb2, "also-stale", LATER)],
        )),
        Millis(4),
    );
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xb3, "fresh", LATER)],
        )),
        Millis(5),
    );

    publish(&mut core, 1, "protobar", "out", "wake", Millis(6));
    let (entries, _) = deferred_view(&mut core, "wire-out");
    assert_eq!(
        entries,
        vec![(0, "fresh".to_string(), LATER)],
        "the second snapshot replaced the first entirely"
    );

    core.on_input(
        Input::TextFrame(deferred_view_frame(WIRE, "protobar", &[])),
        Millis(7),
    );
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(8));
    let (entries, _) = deferred_view(&mut core, "wire-out");
    assert!(entries.is_empty(), "an empty snapshot empties the mirror");
}

#[test]
fn a_deferred_view_is_scoped_to_the_instance_it_names() {
    // The mirror is keyed by `(channel, instance)` because the parked set is the
    // sub-identity's, not the channel's: two components sharing an output channel
    // hold two schedules, and one of them seeing the other's would be the same
    // leak the sender scoping prevents page-side.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(WIRE, "sink", &[(0xc1, "sinks", LATER)])),
        Millis(4),
    );

    publish(&mut core, 1, "protobar", "out", "wake", Millis(5));
    let (entries, _) = deferred_view(&mut core, "wire-out");
    assert!(
        entries.is_empty(),
        "a sibling's view is not this instance's window: {entries:?}"
    );
}

#[test]
fn a_welcome_clears_every_deferred_view_mirror() {
    // Reconnect staleness has one answer: the page forgets, and the backend
    // re-seeds the sets that are nonempty. A set that emptied while the link was
    // down is therefore correctly empty — by the clearance, not by a frame that
    // never comes.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xd1, "old", LATER)],
        )),
        Millis(4),
    );

    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    // Backoff, then the reconnect: the tick is what re-opens the transport.
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![input(EVENTS, "protobar", "in", 4, 4)],
            vec![
                output(WIRE, "protobar", "wire-out"),
                output(EVENTS, "protobar", "out"),
            ],
            vec![local_channel(EVENTS, 4)],
        )),
        Millis(3_007),
    );

    publish(&mut core, 1, "protobar", "out", "wake", Millis(3_008));
    let (entries, _) = deferred_view(&mut core, "wire-out");
    assert!(
        entries.is_empty(),
        "the Welcome cleared the mirror: {entries:?}"
    );
}

#[test]
fn a_deferred_view_wakes_nobody() {
    // A schedule changing is not an arrival. Only a release is, and it reaches the
    // page as an ordinary `Deliver` on the channel — so the frame moves no
    // position and readies no activation.
    let mut core = wire_and_confined_core();
    let effects = core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xe1, "later", LATER)],
        )),
        Millis(4),
    );

    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "the view readied nothing"
    );
    // The exact list, not a universal quantifier over it: `all` over an empty list
    // would pass with the re-arm gone, and a view frame is inbound traffic — on a
    // quiet link it is the only thing keeping the reap timer armed.
    assert_eq!(effects.len(), 1, "one effect: {effects:?}");
    assert!(
        matches!(effects[0], Effect::SetWakeup(Some(_))),
        "the frame's only effect is the liveness re-arm: {effects:?}"
    );
    assert_eq!(
        armed_release(&effects),
        None,
        "the page schedules nothing for a transportable channel: {effects:?}"
    );
}

/// The `PublishBatch` frames in an effect list, as `(publishes, ops)`.
fn batch_frames(effects: &[Effect]) -> Vec<(Vec<BatchEntry>, Vec<BatchDeferredOp>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendFrame(ClientFrame::PublishBatch {
                publishes,
                deferred_ops,
                ..
            }) => Some((publishes.clone(), deferred_ops.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn a_control_op_on_a_transportable_port_rides_the_batch_frame() {
    // The window is honest, so the index resolves and the component gets no
    // refusal. The op then travels to the authority that holds the set: the id the
    // page resolved at buffer time — the backend's own — rides the frame, and
    // nothing page-side is touched, because the page holds no schedule for the
    // channel to change.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xf1, "later", LATER)],
        )),
        Millis(4),
    );

    let effects = with_activation(&mut core, 1, "wake", |buffer| {
        assert_eq!(
            buffer.defer_cancel("wire-out", 0),
            Ok(()),
            "the pushed view is what the index resolves against"
        );
    });
    assert_eq!(
        armed_release(&effects),
        None,
        "no page-side schedule moved: {effects:?}"
    );
    let frames = batch_frames(&effects);
    assert_eq!(
        frames,
        vec![(
            Vec::new(),
            vec![BatchDeferredOp {
                port: "wire-out".to_string(),
                message_id: Uuid::from_u128(0xf1),
                op: DeferredOpKind::Cancel,
            }]
        )],
        "the op travels alone on one frame: {effects:?}"
    );

    // The mirror is untouched: only the authority that holds the set can change
    // what the page is shown, and it says so with the next view it pushes.
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(8));
    let (entries, _) = deferred_view(&mut core, "wire-out");
    assert_eq!(entries, vec![(0, "later".to_string(), LATER)]);
}

#[test]
fn an_edit_on_a_transportable_port_travels_with_both_halves_and_neither() {
    // Each half of the edit rides as the component stated it — a body, a release
    // time, both, or neither — because the server applies exactly what it is
    // handed and `None` means "leave that half alone" all the way down.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xf2, "first", LATER), (0xf3, "second", LATER)],
        )),
        Millis(4),
    );

    let effects = with_activation(&mut core, 1, "wake", |buffer| {
        buffer
            .defer_edit("wire-out", 0, Some("rewritten".into()), Some(LATER + 500))
            .expect("the index resolves against the pushed view");
        buffer
            .defer_edit("wire-out", 1, None, None)
            .expect("an edit that states neither half is still a call");
    });
    let (publishes, ops) = batch_frames(&effects).pop().expect("one batch frame");
    assert!(publishes.is_empty(), "the flush published nothing");
    assert_eq!(
        ops,
        vec![
            BatchDeferredOp {
                port: "wire-out".to_string(),
                message_id: Uuid::from_u128(0xf2),
                op: DeferredOpKind::Edit {
                    body: Some("rewritten".to_string()),
                    deliver_after: Some(LATER + 500),
                },
            },
            BatchDeferredOp {
                port: "wire-out".to_string(),
                message_id: Uuid::from_u128(0xf3),
                op: DeferredOpKind::Edit {
                    body: None,
                    deliver_after: None,
                },
            },
        ],
        "call order, both halves verbatim"
    );
}

#[test]
fn a_flush_splits_its_ops_by_the_channels_deferral_authority() {
    // One activation can act on both kinds of schedule. The confined op applies
    // here, against the page's own set; the transportable one rides the frame. The
    // split is the channel's authority and nothing else — the component made two
    // calls of the same shape.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xf4, "theirs", LATER)],
        )),
        Millis(4),
    );
    park_one(&mut core, "wake", "mine", LATER);

    let effects = with_activation(&mut core, 2, "wake-again", |buffer| {
        buffer
            .defer_cancel("out", 0)
            .expect("the confined window carries the page's own park");
        buffer
            .defer_cancel("wire-out", 0)
            .expect("the transportable window carries the backend's");
    });
    let (_, ops) = batch_frames(&effects).pop().expect("one batch frame");
    assert_eq!(
        ops,
        vec![BatchDeferredOp {
            port: "wire-out".to_string(),
            message_id: Uuid::from_u128(0xf4),
            op: DeferredOpKind::Cancel,
        }],
        "only the transportable op is on the wire"
    );
    assert_eq!(
        armed_release(&effects),
        Some(None),
        "the confined cancel emptied the page's schedule, so the timer disarmed: {effects:?}"
    );

    // The confined message never publishes: the page applied that half itself.
    core.on_input(Input::ReleaseDue { now_ms: LATER + 1 }, Millis(60));
    assert!(
        retained_with_seqs(&core, EVENTS)
            .iter()
            .all(|(body, _)| body != "mine"),
        "the cancelled park never entered retention"
    );
}

#[test]
fn a_parked_flush_of_ops_is_re_validated_against_the_new_welcome() {
    // A batch buffered while the link was down replays under the *new* contract,
    // ops included: an op names a port, so a `Welcome` that no longer binds it
    // drops the batch rather than sending the server something it would answer
    // with a kill.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xf5, "later", LATER)],
        )),
        Millis(4),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    let effects = with_activation(&mut core, 1, "wake", |buffer| {
        buffer
            .defer_cancel("wire-out", 0)
            .expect("the mirror survives the disconnect");
    });
    assert!(
        batch_frames(&effects).is_empty(),
        "nothing goes out with the link down: {effects:?}"
    );

    // The reconnect no longer binds `wire-out`.
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![input(EVENTS, "protobar", "in", 4, 4)],
            vec![output(EVENTS, "protobar", "out")],
            vec![local_channel(EVENTS, 4)],
        )),
        Millis(3_007),
    );
    assert!(
        batch_frames(&effects).is_empty(),
        "the batch was dropped, not replayed onto an unbound port: {effects:?}"
    );
    assert!(
        effects.iter().any(|e| matches!(
            e,
            Effect::PublishControl { channel, .. } if channel == LOCAL_TOAST_CHANNEL
        )),
        "the drop is announced: {effects:?}"
    );
}

/// The correlation of the one `PublishBatch` frame in an effect list.
fn batch_correlation(effects: &[Effect]) -> u64 {
    let found: Vec<u64> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendFrame(ClientFrame::PublishBatch { correlation, .. }) => Some(*correlation),
            _ => None,
        })
        .collect();
    assert_eq!(found.len(), 1, "one batch frame: {effects:?}");
    found[0]
}

/// One wire cancel of the message `id`, as it rides a batch frame.
fn wire_cancel(id: u128) -> BatchDeferredOp {
    BatchDeferredOp {
        port: "wire-out".to_string(),
        message_id: Uuid::from_u128(id),
        op: DeferredOpKind::Cancel,
    }
}

#[test]
fn a_parked_ops_only_flush_replays_its_ops_when_the_link_returns() {
    // An ops-only flush is a whole batch — no publishes, one op — and the park path
    // has to carry both lists. A replay that lost the ops would be a frame with
    // both lists empty, which the server answers by killing the connection, and the
    // component's cancel would be gone as well.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xf6, "later", LATER)],
        )),
        Millis(4),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    let effects = with_activation(&mut core, 1, "wake", |buffer| {
        buffer
            .defer_cancel("wire-out", 0)
            .expect("the mirror survives the disconnect");
    });
    assert!(
        batch_frames(&effects).is_empty(),
        "nothing goes out with the link down: {effects:?}"
    );

    // The reconnect still binds `wire-out`, so the parked flush replays whole.
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![input(EVENTS, "protobar", "in", 4, 4)],
            vec![
                output(WIRE, "protobar", "wire-out"),
                output(EVENTS, "protobar", "out"),
            ],
            vec![local_channel(EVENTS, 4)],
        )),
        Millis(3_007),
    );
    let frames = batch_frames(&effects);
    assert_eq!(
        frames.len(),
        1,
        "the parked flush went back out: {effects:?}"
    );
    assert!(
        frames[0].0.is_empty(),
        "it published nothing before and publishes nothing now: {:?}",
        frames[0].0
    );
    assert_eq!(
        frames[0].1,
        vec![wire_cancel(0xf6)],
        "the op rides the replay verbatim, id and all"
    );
}

#[test]
fn a_rate_limited_ops_only_flush_is_retried_with_its_ops() {
    // `RateLimited` re-parks the flush at the head of the outbox and the timer
    // carries it back out. Same requirement as the disconnect park: the retry must
    // still be an ops-only batch rather than an empty one.
    let mut core = wire_and_confined_core();
    core.on_input(
        Input::TextFrame(deferred_view_frame(
            WIRE,
            "protobar",
            &[(0xf7, "later", LATER)],
        )),
        Millis(4),
    );
    let effects = with_activation(&mut core, 1, "wake", |buffer| {
        buffer
            .defer_cancel("wire-out", 0)
            .expect("the transportable window carries the backend's park");
    });
    let correlation = batch_correlation(&effects);

    let effects = super::activation::answer(
        &mut core,
        correlation,
        brenn_surface_schema::PublishBatchOutcome::RateLimited,
        Millis(80),
    );
    assert!(
        batch_frames(&effects).is_empty(),
        "the retry waits for the timer: {effects:?}"
    );

    let effects = core.on_input(Input::RetryTick, Millis(1_080));
    let frames = batch_frames(&effects);
    assert_eq!(frames.len(), 1, "one retry, the head: {effects:?}");
    assert!(frames[0].0.is_empty(), "still ops-only: {:?}", frames[0].0);
    assert_eq!(
        frames[0].1,
        vec![wire_cancel(0xf7)],
        "the op survived the refusal"
    );
}

#[test]
fn a_deferred_view_shows_only_the_senders_own_parked_messages() {
    // The sender filter is the whole authorization story: two components sharing
    // an output channel each see their own schedule and never each other's, so a
    // shared plane cannot leak one component's timers to another.
    let mut core = shared_channel_core();

    // One gesture publish wakes both instances; only `protobar` parks.
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    park_from(&mut core, "protobar", "mine", LATER);

    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));
    let mut seen: Vec<(String, Vec<String>)> = Vec::new();
    while let Some(ready) = core.take_ready_activation(TEST_WALL_MS) {
        let instance = ready.instance.clone();
        seen.push((
            instance.clone(),
            ready.activation.deferred[0]
                .entries
                .iter()
                .map(|e| e.payload.clone())
                .collect(),
        ));
        complete(&mut core, &instance, ActivationOutcome::Ok, ready.buffer);
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("protobar".to_string(), vec!["mine".to_string()]),
            ("sink".to_string(), vec![]),
        ]
    );
}

#[test]
fn a_full_deferred_set_drops_the_schedule_and_counts_it_against_the_instance() {
    // Quota exhaustion is normal operation, not an error: the flush already
    // happened and the component already returned, so there is no error channel
    // left to answer on. What the page owes instead is an honest count — the only
    // account of a timer the component believes it set.
    //
    // The cap is the store's depth, which a depth-1 channel with depth-1 bindings
    // fixes at one parked message.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![input(EVENTS, "protobar", "in", 1, 1)],
            vec![output(EVENTS, "protobar", "out")],
            vec![local_channel(EVENTS, 1)],
        )),
        Millis(2),
    );
    register(&mut core, "protobar", Millis(3));

    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    for body in ["kept", "refused"] {
        buffer
            .publish_deferred("out", body.to_string(), LATER)
            .expect("the buffer takes both; the cap is the channel's, not the buffer's");
    }
    complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);

    assert_eq!(core.deferred_drop_count("protobar"), 1);
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));
    let (entries, _) = deferred_view(&mut core, "out");
    assert_eq!(
        entries
            .iter()
            .map(|(_, body, _)| body.as_str())
            .collect::<Vec<_>>(),
        vec!["kept"],
        "the schedule that fit survives"
    );
}

#[test]
fn the_release_timer_tracks_the_soonest_schedule_across_confined_channels() {
    // One timer for the whole page, armed at the earliest thing owed: the driver
    // holds one deadline, so the core states the minimum and re-states it whenever
    // it moves. A release pass that empties one channel re-arms at the next.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![input(EVENTS, "protobar", "in", 4, 4)],
            vec![
                output(EVENTS, "protobar", "out"),
                output(OTHER, "protobar", "other-out"),
            ],
            vec![local_channel(EVENTS, 4), local_channel(OTHER, 4)],
        )),
        Millis(2),
    );
    register(&mut core, "protobar", Millis(3));

    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    buffer
        .publish_deferred("other-out", "late".to_string(), TEST_WALL_MS + 90_000)
        .expect("the port is bound");
    buffer
        .publish_deferred("out", "early".to_string(), TEST_WALL_MS + 30_000)
        .expect("the port is bound");
    let effects = complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);
    assert_eq!(
        armed_release(&effects),
        Some(Some(TEST_WALL_MS + 30_000)),
        "armed at the soonest of the two: {effects:?}"
    );

    let effects = core.on_input(
        Input::ReleaseDue {
            now_ms: TEST_WALL_MS + 30_000,
        },
        Millis(60),
    );
    assert_eq!(
        armed_release(&effects),
        Some(Some(TEST_WALL_MS + 90_000)),
        "re-armed at what is still parked: {effects:?}"
    );
    assert_eq!(confined_bodies(&core, EVENTS), vec!["wake", "early"]);
    assert!(
        confined_bodies(&core, OTHER).is_empty(),
        "the later schedule is untouched"
    );
}

#[test]
fn a_release_fire_with_nothing_due_releases_nothing_and_re_arms_nothing() {
    // A fire can be early — a wall clock that stepped back, a timer the browser
    // ran ahead of schedule — and that is not an error: what releases is what is
    // due at the instant the driver read, and the deadline still stands.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "later", LATER);

    let effects = core.on_input(Input::ReleaseDue { now_ms: LATER - 1 }, Millis(59));
    assert_eq!(
        armed_release(&effects),
        None,
        "the deadline did not move: {effects:?}"
    );
    assert_eq!(confined_bodies(&core, EVENTS), vec!["wake"]);
}

#[test]
fn an_unrepresentable_release_time_is_refused_at_buffer_time() {
    // The one deferral refusal a component ever sees, and it sees it while it
    // still holds the error channel: a release time outside the representable
    // range would collapse downstream into an immediate publish, silently turning
    // a schedule into a now.
    let mut core = deferring_core(4);
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    assert_eq!(
        buffer.publish_deferred("out", "never".to_string(), u64::MAX),
        Err(brenn_surface_contract::PublishError::InvalidPayload)
    );
    assert_eq!(
        buffer.publish_deferred("nope", "never".to_string(), LATER),
        Err(brenn_surface_contract::PublishError::NotPermitted),
        "an unbound port is refused ahead of anything about the schedule"
    );
}

#[test]
fn a_cancelled_schedule_never_releases() {
    // Cancel is the component's only way to un-say a schedule, and it is selective:
    // the entry it names goes, its siblings stand, and the timer re-arms at whatever
    // is soonest afterwards.
    let mut core = deferring_core(4);
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    for (body, at) in [("doomed", 10_000u64), ("kept", 20_000)] {
        buffer
            .publish_deferred("out", body.to_string(), TEST_WALL_MS + at)
            .expect("the port is bound");
    }
    complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);

    let effects = with_activation(&mut core, 2, "wake-again", |buffer| {
        assert_eq!(
            buffer.defer_cancel("out", 0),
            Ok(()),
            "index 0 is the soonest entry, which the window showed first"
        );
    });
    assert_eq!(
        armed_release(&effects),
        Some(Some(TEST_WALL_MS + 20_000)),
        "the deadline moves out to the survivor: {effects:?}"
    );

    core.on_input(
        Input::ReleaseDue {
            now_ms: TEST_WALL_MS + 20_000,
        },
        Millis(60),
    );
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake", "wake-again", "kept"],
        "the cancelled body never reached the channel"
    );
}

#[test]
fn an_edit_rewrites_the_body_and_keeps_its_place_in_the_schedule() {
    // An edit keeps the message's identity, so a body-only edit leaves the entry
    // where it was: same index, same release time, new content. That is what makes
    // "revise the message I already scheduled" expressible at all.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "draft", LATER);
    with_activation(&mut core, 2, "wake-again", |buffer| {
        assert_eq!(
            buffer.defer_edit("out", 0, Some("final".into()), None),
            Ok(())
        );
    });

    publish(&mut core, 3, "protobar", "out", "wake-thrice", Millis(8));
    let (entries, _) = deferred_view(&mut core, "out");
    assert_eq!(entries, vec![(0, "final".to_string(), LATER)]);

    core.on_input(Input::ReleaseDue { now_ms: LATER }, Millis(60));
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake", "wake-again", "wake-thrice", "final"],
        "the edited body is what releases"
    );
}

#[test]
fn an_oversize_edit_body_is_refused_like_an_oversize_publish() {
    // An edit body is a body. It answers to the same per-message cap a published
    // body does, on both hostings, and refusing it here is what keeps a conforming
    // kernel from ever wiring an op the server would kill the connection over.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "draft", LATER);
    with_activation(&mut core, 2, "wake-again", |buffer| {
        let over = "x".repeat(FIXTURE_MAX_BODY_BYTES as usize + 1);
        assert_eq!(
            buffer.defer_edit("out", 0, Some(over), None),
            Err(DeferError::QuotaExceeded),
            "the replacement body is one byte over the connection's cap"
        );
        let at_cap = "x".repeat(FIXTURE_MAX_BODY_BYTES as usize);
        assert_eq!(
            buffer.defer_edit("out", 0, Some(at_cap), None),
            Ok(()),
            "and a body exactly at the cap is legal, so only the cap refused above"
        );
    });

    core.on_input(Input::ReleaseDue { now_ms: LATER }, Millis(60));
    let bodies = confined_bodies(&core, EVENTS);
    assert_eq!(
        bodies.last().map(String::len),
        Some(FIXTURE_MAX_BODY_BYTES as usize),
        "the at-cap edit is what released, so the refusal cost the schedule nothing"
    );
}

#[test]
fn an_edit_body_is_charged_to_the_activations_byte_aggregate() {
    // An edit's replacement body is held in memory until the flush exactly as a
    // published body is, so it is charged to the one per-activation aggregate.
    // Without this an activation could hold a full 4 MiB of publishes *and*
    // another 4 MiB of edit bodies.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "draft", LATER);
    let body_len = FIXTURE_MAX_BODY_BYTES as usize;
    let full = brenn_budget::MAX_PUBLISH_BYTES_PER_ACTIVATION / body_len;
    with_activation(&mut core, 2, "wake-again", |buffer| {
        for i in 0..full {
            assert_eq!(
                buffer.defer_edit("out", 0, Some("x".repeat(body_len)), None),
                Ok(()),
                "edit {i} is within the aggregate — the last one spends it exactly"
            );
        }
        assert_eq!(
            buffer.publish("out", "y".into()),
            Err(PublishError::QuotaExceeded),
            "a one-byte publish that would fit any activation of its own is refused, \
             because the edits ahead of it spent the aggregate"
        );
        assert_eq!(
            buffer.defer_edit("out", 0, Some("y".into()), None),
            Err(DeferError::QuotaExceeded),
            "and so is a one-byte edit — the aggregate is one number, not one per \
             call family"
        );
    });
}

#[test]
fn an_edit_can_move_a_release_time_and_the_timer_follows() {
    // Rescheduling is the timer idiom's other half — a component that keeps pushing
    // its own wake-up out, or pulls it in. The armed deadline is re-stated from the
    // edited set, because the re-arm reads the stores rather than remembering what
    // the last flush said.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "later", LATER);
    let effects = with_activation(&mut core, 2, "wake-again", |buffer| {
        assert_eq!(
            buffer.defer_edit("out", 0, None, Some(TEST_WALL_MS + 5_000)),
            Ok(())
        );
    });
    assert_eq!(
        armed_release(&effects),
        Some(Some(TEST_WALL_MS + 5_000)),
        "pulled in: {effects:?}"
    );

    core.on_input(
        Input::ReleaseDue {
            now_ms: TEST_WALL_MS + 5_000,
        },
        Millis(60),
    );
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake", "wake-again", "later"]
    );
}

#[test]
fn an_edit_to_a_time_already_past_releases_at_the_next_pass() {
    // The WIT's past-`deliver_after` rule, in the page's own terms: the edit itself
    // publishes nothing, it makes the message due. The deadline the flush arms is
    // already behind the clock, so the driver's next pass releases it at once.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "soon", LATER);
    let effects = with_activation(&mut core, 2, "wake-again", |buffer| {
        assert_eq!(
            buffer.defer_edit("out", 0, None, Some(TEST_WALL_MS - 1)),
            Ok(())
        );
    });
    assert_eq!(
        armed_release(&effects),
        Some(Some(TEST_WALL_MS - 1)),
        "armed at an instant already past: {effects:?}"
    );
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake", "wake-again"],
        "the edit publishes nothing itself"
    );

    core.on_input(
        Input::ReleaseDue {
            now_ms: TEST_WALL_MS,
        },
        Millis(60),
    );
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake", "wake-again", "soon"]
    );
}

#[test]
fn a_control_op_the_activations_window_cannot_name_is_refused_at_buffer_time() {
    // Every refusal a component can get for a control op, in check order, and all
    // of them while it still holds the error channel. The transportable port is
    // the interesting one: no view has been pushed for it, so its window is empty
    // and there is no index into nothing.
    let mut core = wire_and_confined_core();
    park_one(&mut core, "wake", "later", LATER);
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;

    assert_eq!(
        buffer.defer_cancel("out", 1),
        Err(DeferError::OutOfRange),
        "one entry is parked, so index 1 names nothing"
    );
    assert_eq!(
        buffer.defer_cancel("nope", 0),
        Err(DeferError::NotPermitted),
        "an unbound port is refused ahead of anything about the index"
    );
    assert_eq!(
        buffer.defer_cancel("wire-out", 0),
        Err(DeferError::OutOfRange),
        "no view has been pushed for the transportable channel"
    );
    assert_eq!(
        buffer.defer_edit("out", 0, None, Some(u64::MAX)),
        Err(DeferError::InvalidDeliverAfter),
        "an unrepresentable release time is refused before any budget is charged"
    );
    assert_eq!(
        buffer.defer_edit(
            "out",
            0,
            Some("x".repeat(FIXTURE_MAX_BODY_BYTES as usize + 1)),
            None
        ),
        Err(DeferError::QuotaExceeded),
        "an edited body over the surface's cap is refused, so no oversize body is \
         ever wired anywhere"
    );
    complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);

    publish(&mut core, 3, "protobar", "out", "wake-thrice", Millis(8));
    let (entries, _) = deferred_view(&mut core, "out");
    assert_eq!(
        entries,
        vec![(0, "later".to_string(), LATER)],
        "nothing was accepted, so the schedule is exactly as it was"
    );
}

#[test]
fn control_ops_from_a_failed_activation_change_nothing() {
    // Ops ride the flush rule with the publishes: an err discards the whole buffer,
    // so a cancel the component believes it made is as un-made as a publish it
    // believes it made.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "later", LATER);
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    buffer
        .defer_cancel("out", 0)
        .expect("the window showed the entry");
    complete(
        &mut core,
        "protobar",
        ActivationOutcome::Err(ActivationError {
            message: "no".into(),
        }),
        buffer,
    );

    core.on_input(Input::ReleaseDue { now_ms: LATER }, Millis(60));
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake", "wake-again", "later"],
        "the cancel died with the buffer, so the schedule kept"
    );
}

#[test]
fn a_release_that_beats_the_flush_makes_the_op_a_counted_no_op() {
    // The benign race the contract names: the component read a schedule, the release
    // time arrived while it was still running, and its op now names an ordinary
    // published message. Not an error — it had already returned — but counted, since
    // a component whose ops always race schedules too close to its own rate.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "later", LATER);
    publish(&mut core, 2, "protobar", "out", "wake-again", Millis(6));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    buffer
        .defer_cancel("out", 0)
        .expect("the window showed the entry");
    core.on_input(Input::ReleaseDue { now_ms: LATER }, Millis(59));
    complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);

    assert_eq!(core.deferred_race_count("protobar"), 1);
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake", "wake-again", "later"],
        "a released message stays released"
    );
}

#[test]
#[should_panic(expected = "parked by")]
fn a_control_op_naming_another_senders_message_panics() {
    // A component can only name what its own window showed, and that window is
    // sender-scoped — so a cross-sender identity reaching the flush means the page
    // built the window wrong, which is a kernel bug and not survivable. Only the
    // kernel can produce one, so the buffer is seeded by hand to stand in for that
    // bug.
    let mut core = shared_channel_core();
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    park_from(&mut core, "sink", "theirs", LATER);
    let (theirs, _) = core
        .stores
        .get(&StoreKey::Confined(EVENTS.to_string()))
        .expect("the channel's store")
        .parked()
        .next()
        .expect("sink parked one message");
    let message_id = theirs.message_id;

    let mut buffer = PublishBuffer::new(
        HashMap::from([(
            "out".to_string(),
            OutputSpec {
                channel: EVENTS.to_string(),
                default_urgency: Urgency::Normal,
            },
        )]),
        HashMap::new(),
        FIXTURE_MAX_BODY_BYTES,
        HashMap::from([("out".to_string(), vec![message_id])]),
    );
    buffer
        .defer_cancel("out", 0)
        .expect("the forged snapshot names it");
    complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);
}

/// Two instances on [`EVENTS`] declared at depth 2: `protobar` publishes and
/// parks, `lagger` only reads it, on the `alarm` rung.
///
/// The second instance is what makes a lagging position expressible at all —
/// assembly advances every position it serves, so the instance that parks can
/// never be the one a release outruns.
fn parker_and_lagger_core() -> ClientCore {
    let welcome =
        brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
            subscriptions: vec![
                input(EVENTS, "protobar", "in", 2, 2),
                Binding {
                    noise: NoiseLevel::Alarm,
                    ..input(EVENTS, "lagger", "in", 2, 2)
                },
            ],
            outputs: vec![output(EVENTS, "protobar", "out")],
            alert_granted: true,
            takeover_granted: false,
            components: vec!["protobar", "lagger"],
            error_report_floor: None,
            surface_description: brenn_surface_schema::SurfaceDescription {
                status_interval_secs: 60,
            },
            local_channels: vec![local_channel(EVENTS, 2)],
            max_body_bytes: brenn_surface_test_fixtures::FIXTURE_MAX_BODY_BYTES,
        });
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(Input::TextFrame(welcome), Millis(2));
    core
}

#[test]
fn a_release_is_honoured_after_the_page_goes_terminal() {
    // The release arm sits above the terminal catch-all deliberately, and both
    // halves of that matter. The confined router and its stores outlive the death
    // decision — chrome is still mounted and still reading — so the schedule is
    // still owed. And a fire absorbed without releasing would leave the driver's
    // armed deadline permanently due: nothing releases, so the re-arm restates the
    // same past instant, `sleep_until_release` computes a zero delay, and the
    // select arm re-fires forever. A dead page must quiesce, not spin.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "later", LATER);
    core.on_input(stale_build_close(), Millis(55));

    let effects = core.on_input(Input::ReleaseDue { now_ms: LATER }, Millis(60));
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["wake", "later"],
        "the schedule matured onto a channel the terminal page still routes"
    );
    assert_eq!(
        armed_release(&effects),
        Some(None),
        "and the timer disarms, so the driver stops re-firing an instant already \
         past: {effects:?}"
    );
}

#[test]
fn a_release_that_outruns_retention_charges_the_position_it_passed() {
    // Release is an arrival, and an arrival that evicts charges every position it
    // pushed retention past — release included. It is the arrival a lagging reader
    // is *most* likely to miss, because it lands while nothing is running: without
    // the charge a component provably never saw a message, and no counter moved,
    // no alert fired, and no toast was shown.
    let mut core = parker_and_lagger_core();
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    for body in ["first", "second"] {
        buffer
            .publish_deferred("out", body.to_string(), LATER)
            .expect("a depth-2 channel holds two parked messages");
    }
    complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);
    // `lagger` takes its position now, behind the retained tail, and is never
    // activated — so the release below overruns it and nothing else.
    register(&mut core, "lagger", Millis(5));

    let effects = core.on_input(Input::ReleaseDue { now_ms: LATER }, Millis(60));
    assert_eq!(
        confined_bodies(&core, EVENTS),
        vec!["first", "second"],
        "the depth-2 store kept the pair it released and retired `wake`"
    );
    assert_eq!(
        core.metered_drop_count("lagger", "in"),
        1,
        "the position the release outran is charged at the release, not later"
    );
    assert_eq!(
        core.metered_drop_count("protobar", "in"),
        0,
        "the position that had already passed `wake` lost nothing"
    );
    assert!(
        alerts(&effects).is_empty() && toasts(&effects).is_empty(),
        "the alarm rung announces at the next window, not at the charge: {effects:?}"
    );

    // And the announcement lands at the window that reports the drop.
    let mut announced = None;
    while let Some(ready) = core.take_ready_activation(TEST_WALL_MS) {
        let instance = ready.instance.clone();
        let dropped = window(&ready.activation, "in").dropped;
        let effects = ready.effects.clone();
        complete(&mut core, &instance, ActivationOutcome::Ok, ready.buffer);
        if instance == "lagger" {
            announced = Some((dropped, effects));
        } else {
            assert_eq!(dropped, 0, "{instance} lost nothing");
        }
    }
    let (dropped, effects) = announced.expect("the lagging instance activated");
    assert_eq!(dropped, 1, "the guest is told what it lost");
    assert_eq!(alerts(&effects).len(), 1, "one alert: {effects:?}");
    assert_eq!(toasts(&effects).len(), 1, "one toast: {effects:?}");
}

#[test]
fn one_fire_releases_every_channel_that_is_due() {
    // The sweep is over all due channels, not over the one the deadline was armed
    // from: a page whose several confined channels come due together owes all of
    // them at that instant. Taking only the soonest would make every other channel
    // wait a further timer round-trip — invisible to a fixture where exactly one
    // channel is ever due at a fire.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![input(EVENTS, "protobar", "in", 4, 4)],
            vec![
                output(EVENTS, "protobar", "out"),
                output(OTHER, "protobar", "other-out"),
            ],
            vec![local_channel(EVENTS, 4), local_channel(OTHER, 4)],
        )),
        Millis(2),
    );
    register(&mut core, "protobar", Millis(3));

    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    for port in ["out", "other-out"] {
        buffer
            .publish_deferred(port, format!("{port}-due"), LATER)
            .expect("the port is bound");
    }
    let effects = complete(&mut core, "protobar", ActivationOutcome::Ok, buffer);
    assert_eq!(armed_release(&effects), Some(Some(LATER)));

    let effects = core.on_input(Input::ReleaseDue { now_ms: LATER }, Millis(60));
    assert_eq!(confined_bodies(&core, EVENTS), vec!["wake", "out-due"]);
    assert_eq!(confined_bodies(&core, OTHER), vec!["other-out-due"]);
    assert_eq!(
        armed_release(&effects),
        Some(None),
        "both channels drained, so nothing is left to arm: {effects:?}"
    );
}

#[test]
fn the_call_budget_is_shared_across_publishes_and_control_ops() {
    // One ceiling over both families, so a component cannot double its
    // per-activation call allowance by alternating them. The proof is which error
    // comes back: the last op names an unbound port, which the port check alone
    // would answer `not-permitted`, so `quota-exceeded` can only mean the shared
    // counter fired first.
    let mut core = deferring_core(4);
    publish(&mut core, 1, "protobar", "out", "wake", Millis(4));
    let ready = take_one(&mut core);
    let mut buffer = ready.buffer;
    for i in 0..brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION {
        assert_eq!(
            buffer.publish("nope", "x".into()),
            Err(brenn_surface_contract::PublishError::NotPermitted),
            "publish {i} is refused by the port check, and charged for it"
        );
    }
    assert_eq!(
        buffer.defer_cancel("nope", 0),
        Err(DeferError::QuotaExceeded),
        "the publishes spent the shared budget, so the control op is over it"
    );
}

#[test]
fn the_buffered_control_op_ceiling_bounds_one_activations_ops() {
    // The buffer holds every accepted op until the flush, so the ceiling is what
    // bounds page memory against a component that loops on a deep window. Driven
    // off a hand-seeded snapshot: parking that many messages would need a channel
    // deeper than any fixture, and the ceiling is a property of the buffer.
    let ids: Vec<Uuid> = (0..=brenn_budget::MAX_PUBLISHES_PER_ACTIVATION)
        .map(|i| Uuid::from_u128(i as u128 + 1))
        .collect();
    let mut buffer = PublishBuffer::new(
        HashMap::from([(
            "out".to_string(),
            OutputSpec {
                channel: EVENTS.to_string(),
                default_urgency: Urgency::Normal,
            },
        )]),
        HashMap::new(),
        FIXTURE_MAX_BODY_BYTES,
        HashMap::from([("out".to_string(), ids)]),
    );
    for i in 0..brenn_budget::MAX_PUBLISHES_PER_ACTIVATION {
        assert_eq!(
            buffer.defer_cancel("out", i as u32),
            Ok(()),
            "op {i} is within the ceiling"
        );
    }
    assert_eq!(
        buffer.defer_cancel("out", brenn_budget::MAX_PUBLISHES_PER_ACTIVATION as u32),
        Err(DeferError::QuotaExceeded),
        "the index is in range, so only the buffer ceiling can refuse it"
    );
}

#[test]
fn un_declaring_a_confined_channel_counts_the_schedules_it_takes_with_it() {
    // A store going away takes its deferred set with it. That is a third way a
    // schedule dies, beside the full deferred set and the lost control-op race,
    // and it is the one the component can least explain: its next deferred window
    // is simply empty. Silent state loss on a config edit is what the other two
    // counters exist to prevent, so this one is on the books too.
    let mut core = deferring_core(4);
    park_one(&mut core, "wake", "later", LATER);
    assert_eq!(core.deferred_drop_count("protobar"), 0);

    // Reconnect on a `Welcome` that no longer declares the channel.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(70),
    );
    core.on_input(Input::Tick, Millis(3_070));
    core.on_input(Input::Opened, Millis(3_071));
    core.on_input(
        Input::TextFrame(welcome_frame_local(vec![], vec![], vec![])),
        Millis(3_072),
    );

    assert!(
        !core
            .stores
            .contains_key(&StoreKey::Confined(EVENTS.to_string())),
        "the un-declared channel's store is gone"
    );
    assert_eq!(
        core.deferred_drop_count("protobar"),
        1,
        "the schedule that went with it is charged to the component that set it"
    );
}

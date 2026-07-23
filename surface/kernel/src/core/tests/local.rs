//! The `local:` router: page-local pub/sub whose sole source of truth is the
//! core.
//!
//! The properties under test are the ones that distinguish `local:` from the
//! wire classes — it never emits a frame, it keeps routing with the link down,
//! and its rings outlive a reconnect — plus the envelope synthesis the core only
//! does for this class.

use super::*;
use crate::test_support::{TEST_LOCAL_EPOCH, welcome_frame_local};
use brenn_envelope::{ChannelScheme, Urgency};
use brenn_surface_proto::{
    CONTROL_PLANE_VERSION, ComponentEntry, LocalChannel, OutputBinding, TakeoverAction,
    TakeoverBody,
};

const THEME: &str = "local:brenn/theme";
const TAKEOVER: &str = "local:brenn/takeover";

/// A `local:` channel's router-table entry at `depth`.
fn local_channel(channel: &str, ring_depth: u64) -> LocalChannel {
    LocalChannel {
        channel: channel.into(),
        ring_depth,
    }
}

/// A binding of `(instance, port)` to a `local:` channel. Used for both
/// directions — a local binding's direction is which list it lands in.
fn local_binding(channel: &str, instance: &str, port: &str) -> Binding {
    Binding {
        channel: channel.into(),
        instance: instance.into(),
        port: port.into(),
        push_depth: TEST_PUSH_DEPTH,
        // A `local:` binding reads its channel's router ring; the fixtures'
        // rings are depth 1, so this reads the last value.
        retain_depth: 1,
        noise: brenn_surface_proto::NoiseLevel::Silent,
    }
}

/// The output-direction twin of [`local_binding`], at the port's default
/// urgency. Tests that care about urgency use [`local_output_at`].
fn local_output(channel: &str, instance: &str, port: &str) -> OutputBinding {
    local_output_at(channel, instance, port, Urgency::Normal)
}

/// [`local_output`] with an explicit configured default urgency.
fn local_output_at(channel: &str, instance: &str, port: &str, urgency: Urgency) -> OutputBinding {
    OutputBinding {
        channel: channel.into(),
        instance: instance.into(),
        port: port.into(),
        urgency,
        fill_mt: TEST_FILL_MT,
        capacity_mt: TEST_CAPACITY_MT,
    }
}

/// An operator-declared `local:` channel — no `local:brenn/` prefix, so the
/// contract fixes nothing about it and its ring depth is whatever the bindings
/// resolved to. The channel to reach for when a test's subject *is* the depth.
const APP_EVENTS: &str = "local:app-events";

/// A core driven to `Active` with `protobar` wired both ways onto `channel` at
/// ring depth `depth`: an `in` subscription and an `out` output.
///
/// `depth` must be the contract-fixed depth when `channel` is a reserved plane —
/// a `Welcome` disagreeing is fatal by design, so a test wanting an arbitrary
/// depth wires [`APP_EVENTS`] instead.
fn core_wired_to(channel: &str, depth: u64) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    // The `in` binding reads its channel's ring at its own `retain_depth`, so a
    // fixture wanting the whole depth-`depth` ring as context binds at that
    // depth — a depth-1 binding on a depth-2 ring would only ever see the last
    // entry.
    let in_binding = Binding {
        retain_depth: depth,
        noise: brenn_surface_proto::NoiseLevel::Silent,
        ..local_binding(channel, "protobar", "in")
    };
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![in_binding],
            vec![local_output(channel, "protobar", "out")],
            vec![local_channel(channel, depth)],
        )),
        Millis(2),
    );
    core
}

/// A core driven to `Active` with `protobar` wired both ways onto
/// `local:brenn/theme` at its contract-fixed ring depth of 1: an `in`
/// subscription and an `out` output. The everyday shape — a component
/// publishing a control-plane message that another component (here, itself)
/// consumes.
fn local_core() -> ClientCore {
    core_wired_to(THEME, 1)
}

/// The bodies `instance` sees as **new** on its `in` port, draining every ready
/// activation.
///
/// The router emits no per-message effect: it fills the channel's ring and the
/// bound instances' pending queues, and the messages reach a component as one
/// activation's windows. So a local delivery is observed the same way a wire
/// delivery is — which is the point of the model.
fn new_bodies_for(core: &mut ClientCore, instance: &str) -> Vec<String> {
    let mut bodies = Vec::new();
    while let Some(ready) = core.take_ready_activation() {
        if ready.instance != instance {
            continue;
        }
        bodies.extend(
            split(window(&ready.activation, "in"))
                .1
                .iter()
                .map(|s| s.to_string()),
        );
    }
    bodies
}

/// The envelopes `instance` sees as new on its `in` port, whole.
fn new_envelopes_for(core: &mut ClientCore, instance: &str) -> Vec<MessageEnvelope> {
    let mut out = Vec::new();
    while let Some(ready) = core.take_ready_activation() {
        if ready.instance != instance {
            continue;
        }
        let w = window(&ready.activation, "in");
        out.extend(w.envelopes[w.new_from as usize..].iter().cloned());
    }
    out
}

/// The frames an effect list would put on the wire.
fn frames(effects: &[Effect]) -> Vec<&ClientFrame> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::SendFrame(f) => Some(f),
            _ => None,
        })
        .collect()
}

/// The `PublishResult` statuses an effect list emits.
fn publish_statuses(effects: &[Effect]) -> Vec<PublishStatus> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::EmitEvent(Event::PublishResult { status, .. }) => Some(*status),
            _ => None,
        })
        .collect()
}

#[test]
fn a_local_binding_with_no_router_entry_is_fatal_not_a_panic() {
    // The ring depth has no other source (local channels carry no `[[channel]]`
    // block), so the server declaring one without the other is inexplicable. It
    // must be the designed terminal path, not a panic: the client never panics on
    // peer behavior. Checking it at the handshake is also what lets the router
    // index its rings infallibly afterwards.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(THEME, "protobar", "in")],
            vec![],
            vec![], // bound, but no router entry
        )),
        Millis(2),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(
        detail.contains("declares no router entry"),
        "unexpected fatal detail: {detail}"
    );
}

/// A `Welcome` binding an instance its component map omits is fatal, not a
/// panic.
///
/// The instance map is the sole source of a `local:` publish's sender identity,
/// so `local_sender` cannot proceed without it. Boot resolves bindings and
/// components from one declaration set, so a `Welcome` with the gap is
/// inexplicable — but it is still *peer* input, and the client never panics on
/// peer behavior. Without the handshake check the phantom instance reaches
/// `local_sender`'s assert via the raw `ClientHandle::publish` API (a public
/// surface: native embedders and out-of-tree users) and takes the page down
/// instead of entering the diagnosable `Fatal` state.
#[test]
fn a_binding_naming_an_undeclared_instance_is_fatal_not_a_panic() {
    use brenn_surface_test_fixtures::{WelcomeParams, welcome_frame_raw};

    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame_raw(
            WelcomeParams {
                subscriptions: vec![local_binding(THEME, "protobar", "in")],
                // The output names `phantom`, which the map below omits.
                outputs: vec![local_output(THEME, "phantom", "out")],
                local_channels: vec![local_channel(THEME, 1)],
                ..Default::default()
            },
            vec![ComponentEntry {
                instance: "protobar".into(),
                kind: "protobar".into(),
                abi: brenn_surface_proto::Abi::Dom,
                parked_batch_depth: 8,
                config: Default::default(),
            }],
        )),
        Millis(2),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(
        detail.contains("absent from the component map"),
        "unexpected fatal detail: {detail}"
    );
}

/// A `Welcome` that retunes a reserved plane's contract-fixed ring depth is
/// fatal, not silently honoured.
///
/// A reserved plane's depth *is* its semantics, and the contract owns it: the
/// depth-1 planes' last-value replay is what makes the late-attaching-chrome
/// handoff gap-free, so `link-state` at 0 would leave a chrome that never learns
/// the link state — diagnosable only by diffing ring depths against the contract
/// table. Reserved channels do reach `local_channels` (the server folds them in
/// whenever a component binds one), so this is a live path, not a hypothetical.
/// Fatal rather than ignored for the same reason a zero heartbeat is: the server
/// resolves these from the same contract table the client seeds from, so a
/// disagreement is inexplicable, and the client's answer to an inexplicable
/// `Welcome` is the diagnosable terminal state.
#[test]
fn a_welcome_retuning_a_reserved_ring_depth_is_fatal() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(LINK_STATE, "protobar", "in")],
            vec![],
            // The contract fixes link-state at 1. Zero would kill the replay.
            vec![local_channel(LINK_STATE, 0)],
        )),
        Millis(2),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(
        detail.contains("contract fixes it at 1"),
        "unexpected fatal detail: {detail}"
    );
}

/// The reserved plane's ring keeps its contract-fixed depth even though the
/// `Welcome` restated it — the agreeing case, pinned so the retain/skip halves
/// of the reconcile cannot start rebuilding reserved rings from peer input.
#[test]
fn a_reserved_ring_survives_a_welcome_that_restates_its_depth() {
    let mut core = core_bound_to(LINK_STATE, 1);
    control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"connected"}"#,
        Millis(3),
    );
    // Reconnect, and a second Welcome names the plane again: the ring — and the
    // state a late chrome depends on — must survive it.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(4),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(LINK_STATE, "protobar", "in")],
            vec![],
            vec![local_channel(LINK_STATE, 1)],
        )),
        Millis(3_007),
    );
    register(&mut core, "protobar", Millis(3_008));
    // The value is in the channel's ring, so registration primes it into the
    // component's queue and it arrives as new — that is the handoff the plane's
    // fixed depth exists for.
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")).1,
        vec![r#"{"v":1,"state":"connected"}"#],
        "the ring survived the Welcome and was delivered on attach"
    );
    complete(&mut core, "protobar", ActivationOutcome::Ok, ready.buffer);
    control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"reconnecting"}"#,
        Millis(3_009),
    );
    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    assert_eq!(new, vec![r#"{"v":1,"state":"reconnecting"}"#]);
    assert!(
        context.is_empty(),
        "depth-1 ring: the newer value displaced the old, {context:?}"
    );
}

#[test]
fn local_registration_subscribes_nothing_on_the_wire() {
    let mut core = local_core();
    let effects = register(&mut core, "protobar", Millis(3));
    // The defining property: no server mediates a local channel, so there is
    // nothing to ask it for. A `Subscribe` here would be a protocol violation
    // the server kills the connection over — it never resolved the channel.
    assert!(
        frames(&effects).is_empty(),
        "a local-only registration emitted wire frames: {effects:?}"
    );
}

#[test]
fn local_publish_routes_to_subscribers_and_never_reaches_the_wire() {
    let mut core = local_core();
    register(&mut core, "protobar", Millis(3));
    let effects = publish(
        &mut core,
        7,
        "protobar",
        "out",
        "{\"theme\":\"dark\"}",
        Millis(4),
    );

    assert!(
        frames(&effects).is_empty(),
        "local publish reached the wire: {effects:?}"
    );
    assert_eq!(
        new_bodies_for(&mut core, "protobar"),
        vec!["{\"theme\":\"dark\"}".to_string()]
    );
    // Answered synchronously by the router: no server will send a PublishResult
    // for a message it never saw, so the core must produce it or the caller
    // waits forever.
    assert_eq!(publish_statuses(&effects), vec![PublishStatus::Ok]);
}

#[test]
fn local_publish_synthesizes_the_envelope_from_the_stamp_and_its_own_wiring() {
    let mut core = local_core();
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 7, "protobar", "out", "body", Millis(4));

    let envelope = new_envelopes_for(&mut core, "protobar")
        .into_iter()
        .next()
        .expect("a delivered message");

    // The driver-supplied stamp, verbatim: the core minted neither itself.
    assert_eq!(envelope.message_id, test_stamp(7).message_id);
    assert_eq!(envelope.publish_ts, test_stamp(7).publish_ts);
    // Attribution comes from the router's own wiring, never the body — the
    // page-local twin of the server deriving `sender` from the instance its
    // declaration set admits. The instance-grain sub-identity, taken from the
    // `Welcome` instance map's key. This fixture's instance id equals its kind,
    // so `the_local_sender_is_instance_level_so_sibling_instances_differ` is what
    // pins *which* half is read.
    assert_eq!(envelope.sender, "surface:deskbar#protobar");
    // Provenance: the page produced it, so the surface's own identity stands in
    // for the server origin a wire envelope would carry.
    assert_eq!(envelope.source, "surface:deskbar");
    assert_eq!(envelope.channel, THEME);
    assert_eq!(envelope.envelope_type, ChannelScheme::Local);
    // The port's configured default, resolved by the router (no server sees this
    // message, so nothing downstream would apply it). `local_output` declares
    // `Normal`; the sibling tests below vary it.
    assert_eq!(envelope.urgency, Urgency::Normal);
    // Wire-only fields a local envelope has no use for.
    assert_eq!(envelope.reply_to, None);
    assert_eq!(envelope.delivery_deadline, None);
    assert_eq!(envelope.deliver_after, None);
}

/// A `Welcome` naming an explicit instance→kind map. Hand-built because the
/// shared fixture derives one instance per kind (instance id == kind), and the
/// sub-identity tests exist to prove the router reads the instance half rather
/// than the kind — a distinction that fixture cannot show.
fn welcome_with_instance_map(components: Vec<ComponentEntry>) -> String {
    serde_json::to_string(&ServerFrame::Welcome {
        surface: "deskbar".into(),
        participant_id: "surface:deskbar".into(),
        heartbeat_secs: 20,
        max_body_bytes: 65_536,
        alert_granted: false,
        takeover_granted: false,
        error_report_floor: None,
        surface_description: brenn_surface_proto::SurfaceDescription {
            status_interval_secs: 60,
        },
        bindings: SurfaceBindings {
            components,
            subscriptions: vec![local_binding(THEME, "sink", "in")],
            outputs: vec![
                local_output(THEME, "bar-left", "out"),
                local_output(THEME, "bar-right", "out"),
            ],
            local_channels: vec![local_channel(THEME, 1)],
            chrome_instance: String::new(),
        },
    })
    .unwrap()
}

/// The `sender` a publish from `instance` synthesizes.
fn sender_of(core: &mut ClientCore, correlation: u64, instance: &str, now: Millis) -> String {
    publish(core, correlation, instance, "out", "b", now);
    // Take sink's one activation, read the sender off its new envelope, then
    // complete it: an instance stays in-flight until its activation returns, so
    // completing here is what lets the next publish activate sink again.
    let ready = take_one(core);
    let w = window(&ready.activation, "in");
    let sender = w.envelopes[w.new_from as usize..]
        .last()
        .expect("a delivered message")
        .sender
        .clone();
    complete(core, "sink", ActivationOutcome::Ok, ready.buffer);
    sender
}

#[test]
fn the_local_sender_is_instance_level_so_sibling_instances_differ() {
    // Sub-identity is instance-level: two instances of one kind are two
    // principals, on the page exactly as on the wire. Worth pinning because the
    // kind is right there in the map and stamping it would look equally
    // plausible — and the shared fixture, where instance == kind, cannot tell the
    // two apart. A split-brain here would be observable to out-of-tree
    // components, which see `sender` on every local envelope.
    let entry = |instance: &str, kind: &str| ComponentEntry {
        instance: instance.into(),
        kind: kind.into(),
        abi: brenn_surface_proto::Abi::Dom,
        parked_batch_depth: 8,
        config: Default::default(),
    };
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_with_instance_map(vec![
            entry("bar-left", "protobar"),
            entry("bar-right", "protobar"),
            entry("sink", "echo-stub"),
        ])),
        Millis(2),
    );
    register(&mut core, "sink", Millis(3));

    // Distinct instances, one kind, two identities — and each identity is the
    // *instance* id, not the kind they share.
    assert_eq!(
        sender_of(&mut core, 1, "bar-left", Millis(4)),
        "surface:deskbar#bar-left"
    );
    assert_eq!(
        sender_of(&mut core, 2, "bar-right", Millis(5)),
        "surface:deskbar#bar-right"
    );
}

/// Asserted on the router's own record — its ring — because that is where the
/// position lives: the component seam carries envelopes in order and no
/// positions, so the seq is the router's internal ordering, not something a port
/// reads.
#[test]
fn local_publish_assigns_dense_ascending_seq_per_channel() {
    let mut core = core_wired_to(APP_EVENTS, 4);
    register(&mut core, "protobar", Millis(3));
    for i in 0..4 {
        publish(&mut core, i, "protobar", "out", "b", Millis(4 + i));
    }
    let seqs: Vec<u64> = core
        .local_rings
        .get(APP_EVENTS)
        .expect("the channel's ring")
        .ring
        .entries()
        .map(|(_, pos)| pos.seq)
        .collect();
    // Dense and ascending: the router assigns seq atomically with delivery, so
    // there is no hole for a consumer to mistake for a drop.
    assert_eq!(seqs, vec![0, 1, 2, 3]);
    // And the epoch is the page's, on every one.
    assert!(
        core.local_rings[APP_EVENTS]
            .ring
            .entries()
            .all(|(_, pos)| pos.epoch == TEST_LOCAL_EPOCH)
    );
}

#[test]
fn local_publish_fans_out_to_every_attached_port() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![
                local_binding(THEME, "protobar", "in"),
                local_binding(THEME, "echo", "in"),
            ],
            vec![local_output(THEME, "protobar", "out")],
            vec![local_channel(THEME, 1)],
        )),
        Millis(2),
    );
    register(&mut core, "protobar", Millis(3));
    register(&mut core, "echo", Millis(4));

    publish(&mut core, 9, "protobar", "out", "b", Millis(5));
    // Both bound instances see it — a local channel has no per-instance
    // subscription to scope a delivery to, so the router delivers to everyone
    // bound (including the publisher itself).
    let mut seen: Vec<String> = Vec::new();
    while let Some(ready) = core.take_ready_activation() {
        assert_eq!(split(window(&ready.activation, "in")).1, vec!["b"]);
        seen.push(ready.instance);
    }
    seen.sort();
    assert_eq!(seen, vec!["echo".to_string(), "protobar".to_string()]);
}

/// The boot race, which is what attach-time priming exists for: a value
/// published while nobody is listening reaches the instance that attaches
/// afterwards, **as new**, with no second publish to carry it.
#[test]
fn a_late_registering_instance_is_primed_from_the_local_ring() {
    let mut core = local_core();
    // Publish before anyone is listening — the mode-clock-boots-before-chrome
    // case.
    publish(&mut core, 1, "protobar", "out", "dark", Millis(4));
    register(&mut core, "protobar", Millis(5));
    // Registration alone wakes it: the queue came into existence primed, so the
    // instance is ready without any further input.
    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    assert_eq!(new, vec!["dark"], "the retained value must arrive as new");
    assert!(
        context.is_empty(),
        "a primed envelope is new, never also context: {context:?}"
    );
}

/// A ring deeper than 1 primes the whole retained window, oldest-first, trimmed
/// to the binding's own `retain_depth`.
#[test]
fn the_local_prime_trims_to_the_rings_depth() {
    // A depth-2 ring on the one plane whose depth a `Welcome` may choose, which
    // makes 2 a legal thing for a `Welcome` to say here at all.
    let mut core = core_wired_to(APP_EVENTS, 2);
    for i in 0..5 {
        publish(
            &mut core,
            i,
            "protobar",
            "out",
            &format!("v{i}"),
            Millis(4 + i),
        );
    }
    register(&mut core, "protobar", Millis(20));
    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    // Oldest-first, trimmed to depth: a depth-2 ring holds the last two, and
    // retention already discarded the rest.
    assert_eq!(new, vec!["v3", "v4"]);
    assert!(context.is_empty(), "primed entries are new: {context:?}");
    complete(&mut core, "protobar", ActivationOutcome::Ok, ready.buffer);
    // The next publish is ordinary delivery; the primed pair is now context, at
    // the ring's depth.
    publish(&mut core, 9, "protobar", "out", "v5", Millis(21));
    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    assert_eq!(new, vec!["v5"]);
    assert_eq!(context, vec!["v4"]);
}

/// The prime is capped at the binding's `push_depth`, and the cap costs no
/// drops: the excess was never a delivery obligation on this binding, so
/// reporting it as loss would lie about a counter whose contract is "losses
/// since your last ack". The deeper history is still readable as context.
#[test]
fn the_local_prime_caps_at_push_depth_without_counting_drops() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    // A binding that reads 4 deep but can only hold 2 new: `retain_depth >
    // push_depth`, the case the cap exists for.
    let in_binding = Binding {
        push_depth: 2,
        retain_depth: 4,
        ..local_binding(APP_EVENTS, "protobar", "in")
    };
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![in_binding],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 4)],
        )),
        Millis(2),
    );
    for i in 0..4 {
        publish(
            &mut core,
            i,
            "protobar",
            "out",
            &format!("v{i}"),
            Millis(4 + i),
        );
    }
    register(&mut core, "protobar", Millis(20));
    let ready = take_one(&mut core);
    let w = window(&ready.activation, "in");
    let (context, new) = split(w);
    assert_eq!(new, vec!["v2", "v3"], "primed count equals push_depth");
    assert_eq!(context, vec!["v0", "v1"], "the excess is context, not loss");
    assert_eq!(w.dropped, 0, "the cap is not an overflow");
}

/// `retain_depth` bounds the prime on its own, independently of the queue's
/// capacity: a binding that asks to read one deep on a deeper ring is primed
/// with one. The other arm of the cap — a `push_depth` below `retain_depth` —
/// is `the_local_prime_caps_at_push_depth_without_counting_drops`; both are
/// load-bearing, and chrome's real bindings (read 1, hold 8) are this one.
#[test]
fn a_shallow_binding_is_primed_only_to_its_retain_depth() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let in_binding = Binding {
        push_depth: 8,
        retain_depth: 1,
        ..local_binding(APP_EVENTS, "protobar", "in")
    };
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![in_binding],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 4)],
        )),
        Millis(2),
    );
    for i in 0..4 {
        publish(
            &mut core,
            i,
            "protobar",
            "out",
            &format!("v{i}"),
            Millis(4 + i),
        );
    }
    register(&mut core, "protobar", Millis(20));
    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    assert_eq!(
        new,
        vec!["v3"],
        "the ring holds four, the queue would hold eight, and the binding asked for one"
    );
    // Context is the ring at the same `retain_depth`, deduped against new, so a
    // one-deep reader has nothing left over — the deeper history is not its
    // ambience either.
    assert!(
        context.is_empty(),
        "read one deep, so no context: {context:?}"
    );
}

/// The prime fires once per queue, at its creation — never again. `Welcome`
/// reconciles every registered instance, and a reconnecting page can see many of
/// them, so re-priming a surviving queue would re-deliver the retained tail on
/// every reconnect.
#[test]
fn a_second_welcome_does_not_re_prime_a_surviving_queue() {
    let mut core = local_core();
    publish(&mut core, 1, "protobar", "out", "dark", Millis(4));
    register(&mut core, "protobar", Millis(5));
    let ready = take_one(&mut core);
    assert_eq!(split(window(&ready.activation, "in")).1, vec!["dark"]);
    complete(&mut core, "protobar", ActivationOutcome::Ok, ready.buffer);

    // Blip and reconnect onto the same bindings: the queue survives, so nothing
    // is primed into it and the instance stays quiet.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(6),
    );
    core.on_input(Input::Tick, Millis(3_006));
    core.on_input(Input::Opened, Millis(3_007));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(THEME, "protobar", "in")],
            vec![local_output(THEME, "protobar", "out")],
            vec![local_channel(THEME, 1)],
        )),
        Millis(3_008),
    );
    assert!(
        core.take_ready_activation().is_none(),
        "a surviving queue was re-primed by the reconnect's Welcome"
    );
}

/// A binding that appears at a later `Welcome` gets a fresh queue, and a fresh
/// queue is primed — the operator wired a port onto a plane that already has
/// state, and the component must learn it without waiting for a republish.
#[test]
fn a_binding_added_at_a_later_welcome_is_primed() {
    let mut core = core_wired_to(APP_EVENTS, 2);
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "out", "a1", Millis(4));
    let ready = take_one(&mut core);
    complete(&mut core, "protobar", ActivationOutcome::Ok, ready.buffer);

    // Reconnect onto a config that adds a second port on the same plane.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    let deep = |port: &str| Binding {
        retain_depth: 2,
        ..local_binding(APP_EVENTS, "protobar", port)
    };
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![deep("in"), deep("watch")],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 2)],
        )),
        Millis(3_007),
    );
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "watch")).1,
        vec!["a1"],
        "the newly bound port is primed from the plane's ring"
    );
    assert!(
        split(window(&ready.activation, "in")).1.is_empty(),
        "the surviving port is not re-primed"
    );
}

/// A port rebound to a different channel is a different queue: the old
/// channel's envelopes are shed unread and the new channel's retained tail is
/// primed. Keying the queue by port alone would keep stale envelopes and skip
/// the prime — the silent-loss shape this whole mechanism removes.
#[test]
fn a_port_rebound_to_another_local_channel_sheds_and_re_primes() {
    const OTHER: &str = "local:app-other";
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let deep = |channel: &str, port: &str| Binding {
        retain_depth: 2,
        ..local_binding(channel, "protobar", port)
    };
    let welcome = |in_channel: &str| {
        welcome_frame_local(
            vec![deep(in_channel, "in")],
            vec![
                local_output(APP_EVENTS, "protobar", "out-a"),
                local_output(OTHER, "protobar", "out-b"),
            ],
            vec![local_channel(APP_EVENTS, 2), local_channel(OTHER, 2)],
        )
    };
    core.on_input(Input::TextFrame(welcome(APP_EVENTS)), Millis(2));
    publish(&mut core, 1, "protobar", "out-a", "a1", Millis(3));
    publish(&mut core, 2, "protobar", "out-b", "b1", Millis(4));
    // Primed from the first channel and deliberately left unconsumed, so the
    // rebind has something to shed.
    register(&mut core, "protobar", Millis(5));

    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(6),
    );
    core.on_input(Input::Tick, Millis(3_006));
    core.on_input(Input::Opened, Millis(3_007));
    core.on_input(Input::TextFrame(welcome(OTHER)), Millis(3_008));

    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    assert_eq!(new, vec!["b1"], "the new channel's tail, and only it");
    assert!(
        context.is_empty(),
        "context comes from the new channel's ring too: {context:?}"
    );
}

/// A port rebound across channel *classes*, wire to `local:`, is primed like any
/// other new local queue: the class of the new channel decides, not the class of
/// the old one. The old channel's unconsumed envelope goes with the queue it sat
/// in, and no replay is coming to double the prime — the subscription was
/// released by the same reconcile.
#[test]
fn a_wire_port_rebound_to_a_local_channel_is_primed() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let welcome = |in_channel: &str| {
        welcome_frame_local(
            vec![Binding {
                channel: in_channel.into(),
                ..local_binding(APP_EVENTS, "protobar", "in")
            }],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 1)],
        )
    };
    core.on_input(Input::TextFrame(welcome("ephemeral:demo")), Millis(2));
    register(&mut core, "protobar", Millis(3));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(4),
    );
    // Delivered and deliberately left unconsumed, so the rebind has something to
    // shed.
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("m1"), 1)),
        Millis(5),
    );
    publish(&mut core, 1, "protobar", "out", "a1", Millis(6));

    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(7),
    );
    core.on_input(Input::Tick, Millis(3_007));
    core.on_input(Input::Opened, Millis(3_008));
    let effects = core.on_input(Input::TextFrame(welcome(APP_EVENTS)), Millis(3_009));
    assert!(
        !frames(&effects).iter().any(|f| matches!(
            f,
            ClientFrame::Subscribe { channel, .. } if channel == "ephemeral:demo"
        )),
        "the released wire subscription was reopened: {effects:?}"
    );

    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    assert_eq!(new, vec!["a1"], "the local ring's tail, exactly once");
    assert!(
        context.is_empty(),
        "context comes from the new channel's ring too: {context:?}"
    );
}

/// The mirror: `local:` to wire. The recreated queue is not primed — a fresh
/// wire attach is filled by the server's replay, and priming it here would
/// double every replayed envelope in the first window.
#[test]
fn a_local_port_rebound_to_a_wire_channel_is_not_primed() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let welcome = |in_channel: &str| {
        welcome_frame_local(
            vec![Binding {
                channel: in_channel.into(),
                ..local_binding(APP_EVENTS, "protobar", "in")
            }],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 1)],
        )
    };
    core.on_input(Input::TextFrame(welcome(APP_EVENTS)), Millis(2));
    publish(&mut core, 1, "protobar", "out", "a1", Millis(3));
    // Primed and left unconsumed, so the rebind has something to shed.
    register(&mut core, "protobar", Millis(4));

    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    let effects = core.on_input(Input::TextFrame(welcome("ephemeral:demo")), Millis(3_007));
    assert!(
        frames(&effects).iter().any(|f| matches!(
            f,
            ClientFrame::Subscribe { channel, .. } if channel == "ephemeral:demo"
        )),
        "the new wire channel must be subscribed: {effects:?}"
    );
    assert!(
        core.take_ready_activation().is_none(),
        "the shed local prime surfaced under the wire binding"
    );
    assert!(
        core.registered["protobar"].queues["in"].is_empty(),
        "the rebound port waits for the server's replay, empty"
    );
}

/// A `push_depth = 0` binding has no queue, so there is nothing to prime and
/// nothing to wake on. That is the whole of "never activates me": the retained
/// value still reaches it, as context, when a sibling port does the waking.
#[test]
fn a_depth_zero_binding_is_not_primed_and_never_wakes() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let sampled = Binding {
        push_depth: 0,
        retain_depth: 1,
        ..local_binding(APP_EVENTS, "protobar", "in")
    };
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![sampled],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 1)],
        )),
        Millis(2),
    );
    publish(&mut core, 1, "protobar", "out", "a1", Millis(3));
    register(&mut core, "protobar", Millis(4));
    assert!(
        core.take_ready_activation().is_none(),
        "a sampled port was primed into an activation"
    );
}

/// The positive half of "never activates me": a sampled port still *reads* its
/// plane, and it reads it in a window a sibling port's **prime** minted. Priming
/// is a new activation cause, and a depth-0 port must appear in the window it
/// produces exactly as it does in one a publish produced — otherwise a component
/// reading state it must not be woken by boots blind, which is the boot race one
/// port over.
#[test]
fn a_depth_zero_sibling_is_context_only_when_a_prime_does_the_waking() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let sampled = Binding {
        push_depth: 0,
        retain_depth: 1,
        ..local_binding(APP_EVENTS, "protobar", "in")
    };
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![sampled, local_binding(APP_EVENTS, "protobar", "watch")],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 1)],
        )),
        Millis(2),
    );
    publish(&mut core, 1, "protobar", "out", "a1", Millis(3));
    register(&mut core, "protobar", Millis(4));

    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "watch")).1,
        vec!["a1"],
        "the queued sibling is what wakes the instance"
    );
    let (context, new) = split(window(&ready.activation, "in"));
    assert!(new.is_empty(), "a sampled port has no new rows: {new:?}");
    assert_eq!(
        context,
        vec!["a1"],
        "the sampled port reads the plane's value as context in the primed window"
    );
}

/// A binding added while the instance has an activation out is primed then and
/// delivered after: the prime is queue state, and an in-flight activation's ack
/// covers only the rows it was dispatched with. Once, not twice, and not lost.
#[test]
fn a_binding_added_while_an_activation_is_in_flight_is_primed_once() {
    let mut core = core_wired_to(APP_EVENTS, 2);
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "out", "a1", Millis(4));
    // Dispatched and deliberately left open across the reconnect.
    let in_flight = take_one(&mut core);

    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    let deep = |port: &str| Binding {
        retain_depth: 2,
        ..local_binding(APP_EVENTS, "protobar", port)
    };
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![deep("in"), deep("watch")],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 2)],
        )),
        Millis(3_007),
    );
    assert!(
        core.take_ready_activation().is_none(),
        "a primed queue must not dispatch over an in-flight activation"
    );

    complete(
        &mut core,
        "protobar",
        ActivationOutcome::Ok,
        in_flight.buffer,
    );
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "watch")).1,
        vec!["a1"],
        "the prime survives the in-flight activation's completion"
    );
    assert!(
        split(window(&ready.activation, "in")).1.is_empty(),
        "the surviving port's rows were acked by the activation that carried them"
    );
    complete(&mut core, "protobar", ActivationOutcome::Ok, ready.buffer);
    assert!(
        core.take_ready_activation().is_none(),
        "the primed envelope was delivered twice"
    );
}

/// A trapped instance's queues are recreated at the next `Welcome`, but they
/// are not primed. A dead instance never activates, so priming it would only
/// park stale envelopes waiting to surface as new.
#[test]
fn a_trapped_instance_is_not_primed_at_the_next_welcome() {
    let mut core = local_core();
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "out", "dark", Millis(4));
    let ready = take_one(&mut core);
    complete(
        &mut core,
        "protobar",
        ActivationOutcome::Trap("boom".into()),
        ready.buffer,
    );

    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(THEME, "protobar", "in")],
            vec![local_output(THEME, "protobar", "out")],
            vec![local_channel(THEME, 1)],
        )),
        Millis(3_007),
    );
    assert!(
        core.take_ready_activation().is_none(),
        "a failed instance was primed back into life"
    );
    assert!(
        core.registered["protobar"].queues["in"].is_empty(),
        "the recreated queue of a failed instance must be empty"
    );
}

#[test]
fn the_toast_plane_retains_nothing() {
    // Depth 0 exercised on the plane the contract actually fixes at 0, and via
    // the kernel publish that is toast's only legal producer — rather than by
    // retuning some other plane to 0, which is precisely what a `Welcome` may
    // not do. Toast is an event stream, not a control plane: replaying a stale
    // toast to a late-attaching chrome would resurface an already-past event.
    let mut core = core_bound_to(TOAST, 0);
    control(&mut core, TOAST, r#"{"v":1,"severity":"info"}"#, Millis(4));
    register(&mut core, "protobar", Millis(5));
    // Nothing retained is nothing to prime: a toast published while chrome was
    // still instantiating stays lost, deliberately.
    assert!(
        core.take_ready_activation().is_none(),
        "a depth-0 ring primed something"
    );
    // A second toast dispatches the view; the first is not in its context,
    // because a depth-0 ring retained nothing.
    control(&mut core, TOAST, r#"{"v":1,"severity":"warn"}"#, Millis(6));
    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    assert_eq!(new, vec![r#"{"v":1,"severity":"warn"}"#]);
    assert!(context.is_empty(), "the toast ring retained: {context:?}");
}

#[test]
fn local_publish_routes_while_the_link_is_down() {
    let mut core = local_core();
    register(&mut core, "protobar", Millis(3));
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(4),
    );
    // Offline-correct by construction: the router never touches the WS, so the
    // link being down is no reason to reject. This is the takeover-at-T−2min
    // kiosk case — the whole reason `local:` exists as its own class.
    let effects = publish(&mut core, 7, "protobar", "out", "dark", Millis(5));
    assert_eq!(publish_statuses(&effects), vec![PublishStatus::Ok]);
    assert_eq!(
        new_bodies_for(&mut core, "protobar"),
        vec!["dark".to_string()]
    );
    assert!(frames(&effects).is_empty());
}

#[test]
fn a_wire_publish_while_the_link_is_down_is_still_rejected() {
    // The contrast that proves the offline exemption is scoped to `local:` and
    // did not quietly widen: an ephemeral output on the same disconnected core
    // still fails.
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(4),
    );
    let effects = publish(&mut core, 7, "protobar", "out", "b", Millis(5));
    assert_eq!(
        publish_statuses(&effects),
        vec![PublishStatus::NotConnected]
    );
}

#[test]
fn local_rings_and_seqs_survive_a_reconnect() {
    // A depth-2 ring so the pre-blip value and the post-blip value coexist as
    // context ++ new in one window — the observation the survival claim needs.
    // THEME is contract-fixed at depth 1, so the arbitrary-depth plane is wired.
    let mut core = core_wired_to(APP_EVENTS, 2);
    publish(&mut core, 1, "protobar", "out", "dark", Millis(4));
    // Blip and reconnect. The page did not reload, so page-local state must not
    // die: discarding the ring here would manufacture a loss the link never
    // caused.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    let in_binding = Binding {
        retain_depth: 2,
        noise: brenn_surface_proto::NoiseLevel::Silent,
        ..local_binding(APP_EVENTS, "protobar", "in")
    };
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![in_binding],
            vec![local_output(APP_EVENTS, "protobar", "out")],
            vec![local_channel(APP_EVENTS, 2)],
        )),
        Millis(3_007),
    );

    register(&mut core, "protobar", Millis(3_008));
    // The pre-reconnect value survived the blip: it is still the ring's, so the
    // attaching instance is primed from it and wakes on it.
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")).1,
        vec!["dark"],
        "the ring survived the reconnect"
    );
    complete(&mut core, "protobar", ActivationOutcome::Ok, ready.buffer);
    publish(&mut core, 2, "protobar", "out", "light", Millis(3_009));
    let ready = take_one(&mut core);
    let (context, new) = split(window(&ready.activation, "in"));
    assert_eq!(new, vec!["light"]);
    assert_eq!(context, vec!["dark"]);
}

#[test]
fn a_local_channel_dropped_from_welcome_drops_its_ring_and_its_queues() {
    let mut core = local_core();
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "out", "dark", Millis(4));
    // Reconnect to a config that un-declared the channel.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame_local(vec![], vec![], vec![])),
        Millis(3_007),
    );
    // The binding vanished, so reconcile dropped the instance's queue for it: it
    // simply stops being activated on that channel, which is not a failure and
    // not a deregistration.
    assert!(
        frames(&effects).is_empty(),
        "dropping a local binding touches no wire: {effects:?}"
    );
    // And the publish is now unbound — the ring is gone, not merely unreachable.
    let effects = publish(&mut core, 2, "protobar", "out", "light", Millis(3_008));
    assert_eq!(publish_statuses(&effects), vec![PublishStatus::UnboundPort]);
}

#[test]
fn a_local_publish_after_the_core_goes_terminal_is_not_connected() {
    let mut core = local_core();
    register(&mut core, "protobar", Millis(3));
    // Any fatal protocol error ends delivery for the page; routing after that
    // would queue a message no activation will ever carry.
    core.on_input(Input::BinaryFrame, Millis(4));
    let effects = publish(&mut core, 7, "protobar", "out", "dark", Millis(5));
    assert_eq!(
        publish_statuses(&effects),
        vec![PublishStatus::NotConnected]
    );
    assert!(new_bodies_for(&mut core, "protobar").is_empty());
}

#[test]
fn an_oversized_local_publish_is_rejected_like_any_other() {
    // Ports are ports: a component's body-size contract must not change because
    // an operator rebound its output from `brenn:` to `local:`.
    let mut core = local_core();
    register(&mut core, "protobar", Millis(3));
    let body = "x".repeat(65_537);
    let effects = publish(&mut core, 7, "protobar", "out", &body, Millis(4));
    assert!(matches!(
        publish_statuses(&effects).as_slice(),
        [PublishStatus::BodyTooLarge { .. }]
    ));
    assert!(new_bodies_for(&mut core, "protobar").is_empty());
}

/// Deregistering releases nothing on a local channel and leaves the ring — the
/// ring is the channel's, not any instance's, so a later registration still finds
/// it. Only a page reload clears it.
#[test]
fn deregistering_a_local_only_instance_emits_no_unsubscribe_and_keeps_the_ring() {
    let mut core = local_core();
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "out", "dark", Millis(4));

    let effects = deregister(&mut core, "protobar", Millis(5));
    // No refcount, no subscription, nothing to tell the server.
    assert!(
        frames(&effects).is_empty(),
        "a local-only deregistration emitted wire frames: {effects:?}"
    );
    // The ring kept the value.
    assert_eq!(
        core.local_rings[THEME]
            .ring
            .entries()
            .map(|(e, _)| e.body.clone())
            .collect::<Vec<_>>(),
        vec!["dark".to_string()]
    );
    // And a fresh registration is a fresh attach: it is primed from the ring and
    // receives the value again, as new. Wire symmetry — a re-subscribe re-replays
    // too, and a component with a side-effecting fold owes itself at-most-once
    // handling by `message_id` on either class.
    register(&mut core, "protobar", Millis(6));
    let ready = take_one(&mut core);
    assert_eq!(split(window(&ready.activation, "in")).1, vec!["dark"]);
    complete(&mut core, "protobar", ActivationOutcome::Ok, ready.buffer);
    publish(&mut core, 2, "protobar", "out", "light", Millis(7));
    let ready = take_one(&mut core);
    assert_eq!(split(window(&ready.activation, "in")).1, vec!["light"]);
}

// ── Local publish urgency ─────────────────────────────────────────────

/// [`local_core`] whose output port declares `urgency` as its configured
/// default.
fn local_core_at(urgency: Urgency) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(THEME, "protobar", "in")],
            vec![local_output_at(THEME, "protobar", "out", urgency)],
            vec![local_channel(THEME, 1)],
        )),
        Millis(2),
    );
    core
}

/// The urgency on the single envelope the router delivered to `protobar`.
fn delivered_urgency(core: &mut ClientCore) -> Urgency {
    new_envelopes_for(core, "protobar")
        .into_iter()
        .next()
        .expect("a delivered message")
        .urgency
}

#[test]
fn local_envelope_carries_the_ports_configured_default_urgency() {
    // The router resolves the default itself — it *is* the router, so no server
    // downstream will. A hard-coded `Normal` here would silently ignore the
    // operator's knob on every page-local channel.
    for level in Urgency::ALL {
        let mut core = local_core_at(level);
        register(&mut core, "protobar", Millis(3));
        publish(&mut core, 1, "protobar", "out", "{}", Millis(5));
        assert_eq!(delivered_urgency(&mut core), level, "default {level:?}");
    }
}

#[test]
fn local_envelope_carries_a_stated_override_over_the_default() {
    // Override-beats-default, page-locally — the same precedence the server
    // applies to a wire publish, so a component's publish semantics do not
    // change with the class its output happens to be bound to.
    let mut core = local_core_at(Urgency::Low);
    register(&mut core, "protobar", Millis(3));
    publish_at(
        &mut core,
        1,
        "protobar",
        "out",
        "{}",
        Urgency::High,
        Millis(5),
    );
    assert_eq!(delivered_urgency(&mut core), Urgency::High);
}

#[test]
fn local_publish_still_sends_no_wire_frame_whatever_its_urgency() {
    // Urgency is wake economics and page-local delivery wakes nothing. Pin that
    // carrying the field did not accidentally make the router consult the wire:
    // `high` is exactly the value that would tempt a "wake someone" path.
    let mut core = local_core_at(Urgency::High);
    let effects = publish(&mut core, 1, "protobar", "out", "{}", Millis(5));
    assert!(frames(&effects).is_empty(), "{effects:?}");
}

// ── the takeover plane's instance injection ──────────────────────────────────

#[test]
fn a_takeover_publish_has_its_instance_overwritten_with_the_publisher() {
    // A component names only its port; the identity chrome trusts to grant, deny,
    // and pop the overlay must come from the router's own wiring, never the body.
    // The publisher here lies about being `victim`; the router must overwrite it
    // with the authenticated publishing instance.
    let mut core = core_wired_to(TAKEOVER, 1);
    register(&mut core, "protobar", Millis(3));
    let spoofed = serde_json::to_string(&TakeoverBody {
        v: CONTROL_PLANE_VERSION,
        action: TakeoverAction::Request,
        instance: "victim".into(),
    })
    .unwrap();
    publish(&mut core, 7, "protobar", "out", &spoofed, Millis(4));

    let body = new_bodies_for(&mut core, "protobar")
        .into_iter()
        .next()
        .expect("a delivered takeover message");
    let parsed: TakeoverBody = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed.instance, "protobar");
    // The rest of the body is untouched.
    assert_eq!(parsed.action, TakeoverAction::Request);
    assert_eq!(parsed.v, CONTROL_PLANE_VERSION);
}

#[test]
fn a_malformed_takeover_body_is_passed_through_for_the_consumer_to_reject() {
    // The router cannot stamp an identity onto a body it cannot parse; it passes
    // it through unchanged, and chrome's `on_takeover` drops-and-reports it — a
    // malformed spoof attempt fares no better than a well-formed one.
    let mut core = core_wired_to(TAKEOVER, 1);
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 7, "protobar", "out", "not json", Millis(4));

    let body = new_bodies_for(&mut core, "protobar")
        .into_iter()
        .next()
        .expect("a delivered message");
    assert_eq!(body, "not json");
}

#[test]
fn a_non_takeover_local_body_is_delivered_byte_for_byte() {
    // The instance-injection guard is scoped to the takeover channel. Pin the
    // negative: a body on any *other* local plane is delivered verbatim, never
    // parsed-and-re-stamped. A JSON body carrying an `instance` field is the
    // exact shape that would be rewritten if the guard ever broadened past its
    // channel check.
    let mut core = core_wired_to(THEME, 1);
    register(&mut core, "protobar", Millis(3));
    let body = r#"{"instance":"victim","arbitrary":"payload"}"#;
    publish(&mut core, 7, "protobar", "out", body, Millis(4));

    let delivered = new_bodies_for(&mut core, "protobar")
        .into_iter()
        .next()
        .expect("a delivered message");
    assert_eq!(delivered, body);
}

// ── the takeover plane's instance injection, from an activation's buffer ─────

/// A well-formed takeover body naming `instance`.
fn takeover_body(action: TakeoverAction, instance: &str) -> String {
    serde_json::to_string(&TakeoverBody {
        v: CONTROL_PLANE_VERSION,
        action,
        instance: instance.into(),
    })
    .expect("a TakeoverBody serializes")
}

/// The bodies a `local:` channel's ring retains, oldest first.
fn ring_bodies(core: &ClientCore, channel: &str) -> Vec<String> {
    core.local_rings
        .get(channel)
        .expect("the channel's router ring")
        .ring
        .entries()
        .map(|(e, _)| e.body.clone())
        .collect()
}

/// A core driven to `Active` with `protobar` reading `local:brenn/theme` on
/// `in`, and publishing on both `local:brenn/takeover` (port `takeover`) and
/// `local:brenn/theme` (port `theme-out`).
///
/// The ingestion-publishes-takeover shape: a message arrives, the entry
/// recomputes, and the recompute publishes on the takeover plane from inside
/// the activation — where the publish is buffered rather than routed inline.
fn core_ingesting_theme_publishing_takeover() -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(THEME, "protobar", "in")],
            vec![
                local_output(TAKEOVER, "protobar", "takeover"),
                local_output(THEME, "protobar", "theme-out"),
            ],
            vec![local_channel(THEME, 1), local_channel(TAKEOVER, 1)],
        )),
        Millis(2),
    );
    core
}

/// Take the one ready activation, publish `body` on `port` from inside it, and
/// return it ok — the buffered publish path, exactly as the driver drives it.
fn publish_from_activation(
    core: &mut ClientCore,
    instance: &str,
    port: &str,
    body: &str,
) -> Vec<Effect> {
    let ready = take_one(core);
    assert_eq!(ready.instance, instance, "the expected instance activated");
    let mut buffer = ready.buffer;
    buffer
        .publish(port, body.to_string())
        .expect("the port is bound for output");
    complete(core, instance, ActivationOutcome::Ok, buffer)
}

#[test]
fn a_buffered_takeover_publish_carries_the_activating_instance() {
    // The stamp lives at the mint point, which both publish paths funnel
    // through, so a takeover published from inside an activation entry is
    // attributable exactly like one published from a gesture.
    let mut core = core_ingesting_theme_publishing_takeover();
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "theme-out", "{}", Millis(4));
    publish_from_activation(
        &mut core,
        "protobar",
        "takeover",
        &takeover_body(TakeoverAction::Request, ""),
    );

    let body = ring_bodies(&core, TAKEOVER)
        .pop()
        .expect("a routed takeover message");
    let parsed: TakeoverBody = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed.instance, "protobar");
    assert_eq!(parsed.action, TakeoverAction::Request);
}

#[test]
fn a_buffered_takeover_publish_naming_another_instance_is_overwritten() {
    // The forgery guard covers the buffered path too: a component publishing
    // from inside its own activation names only its port, and the router takes
    // the identity from its own wiring.
    let mut core = core_ingesting_theme_publishing_takeover();
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "theme-out", "{}", Millis(4));
    publish_from_activation(
        &mut core,
        "protobar",
        "takeover",
        &takeover_body(TakeoverAction::Release, "victim"),
    );

    let body = ring_bodies(&core, TAKEOVER)
        .pop()
        .expect("a routed takeover message");
    let parsed: TakeoverBody = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed.instance, "protobar");
}

#[test]
fn a_buffered_non_takeover_publish_is_routed_byte_for_byte() {
    // The stamp is scoped to the takeover channel at the mint point, not to a
    // publish path: a body on any other plane flushed from an activation buffer
    // is routed verbatim, never parsed-and-re-stamped.
    let mut core = core_ingesting_theme_publishing_takeover();
    register(&mut core, "protobar", Millis(3));
    publish(&mut core, 1, "protobar", "theme-out", "{}", Millis(4));
    let body = r#"{"instance":"victim","arbitrary":"payload"}"#;
    publish_from_activation(&mut core, "protobar", "theme-out", body);

    assert_eq!(ring_bodies(&core, THEME).pop().unwrap(), body);
}

#[test]
fn a_release_published_while_ingesting_a_replacement_is_attributable() {
    // The wedge, in shape: a component holds the overlay by an earlier stamped
    // Request; a replacement snapshot arrives; ingesting it flips the component
    // back to not-wanting-takeover and the Release goes out from inside that
    // activation. Both messages must name the same instance, or the consumer
    // holds an overlay whose release it can never attribute.
    let mut core = core_ingesting_theme_publishing_takeover();
    register(&mut core, "protobar", Millis(3));
    // The Request, from a timer tick: no activation in flight, so it routes
    // inline.
    publish(
        &mut core,
        1,
        "protobar",
        "takeover",
        &takeover_body(TakeoverAction::Request, ""),
        Millis(4),
    );
    let request: TakeoverBody =
        serde_json::from_str(&ring_bodies(&core, TAKEOVER).pop().unwrap()).unwrap();
    publish(&mut core, 2, "protobar", "theme-out", "{}", Millis(5));
    publish_from_activation(
        &mut core,
        "protobar",
        "takeover",
        &takeover_body(TakeoverAction::Release, ""),
    );
    let release: TakeoverBody =
        serde_json::from_str(&ring_bodies(&core, TAKEOVER).pop().unwrap()).unwrap();

    assert_eq!(request.action, TakeoverAction::Request);
    assert_eq!(release.action, TakeoverAction::Release);
    assert_eq!(request.instance, "protobar");
    assert_eq!(release.instance, request.instance);
}

#[test]
fn a_takeover_published_from_a_primed_replay_activation_is_attributable() {
    // The replay face of the same defect: a fresh page's first activation is
    // driven by the retained ring primed into its queue at attach, and the
    // publish its ingestion makes takes the same buffered path. Nothing about
    // attribution may depend on how the activation was triggered.
    let mut core = core_ingesting_theme_publishing_takeover();
    // Published before the instance registers: nothing is queued, the ring
    // retains it.
    publish(&mut core, 1, "protobar", "theme-out", "{}", Millis(4));
    assert!(
        core.take_ready_activation().is_none(),
        "an unregistered instance does not activate"
    );
    // Attach primes the retained value in as new, which activates the instance
    // in the same turn.
    register(&mut core, "protobar", Millis(5));
    publish_from_activation(
        &mut core,
        "protobar",
        "takeover",
        &takeover_body(TakeoverAction::Request, ""),
    );

    let body = ring_bodies(&core, TAKEOVER)
        .pop()
        .expect("a routed takeover message");
    let parsed: TakeoverBody = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed.instance, "protobar");
}

// ── the overlay-state plane: chrome's report, the kernel's record ────────────

const OVERLAY_STATE: &str = "local:brenn/overlay-state";

/// A `Welcome` declaring `chrome` as the surface's chrome instance, with
/// `chrome` bound to publish on the overlay-state plane and `meeting` declared
/// as the instance that can hold an overlay.
///
/// Both instances also read and write [`APP_EVENTS`], which is how a test drives
/// one of them into an activation: chrome's real overlay-state publishes are
/// made from inside its activation entry, so the buffered path needs an
/// ingestion to hang off.
fn welcome_frame_with_chrome() -> String {
    welcome_frame_chrome_with(vec!["chrome", "meeting"])
}

/// [`welcome_frame_with_chrome`] declaring exactly `instances` (which must
/// include `chrome`) — the reconnect fixture, for a `Welcome` that drops the
/// instance a recorded overlay names.
fn welcome_frame_chrome_with(instances: Vec<&str>) -> String {
    let entry = |instance: &str| ComponentEntry {
        instance: instance.into(),
        kind: instance.into(),
        abi: brenn_surface_proto::Abi::Dom,
        parked_batch_depth: 8,
        config: Default::default(),
    };
    let components: Vec<ComponentEntry> = instances.iter().map(|i| entry(i)).collect();
    let subscriptions: Vec<Binding> = instances
        .iter()
        .map(|i| local_binding(APP_EVENTS, i, "in"))
        .collect();
    let outputs: Vec<OutputBinding> = instances
        .iter()
        .flat_map(|i| {
            [
                local_output(OVERLAY_STATE, i, "overlay-state"),
                local_output(APP_EVENTS, i, "app-out"),
            ]
        })
        .collect();
    serde_json::to_string(&ServerFrame::Welcome {
        surface: "deskbar".into(),
        participant_id: "surface:deskbar".into(),
        heartbeat_secs: 20,
        max_body_bytes: 65_536,
        alert_granted: false,
        takeover_granted: true,
        error_report_floor: None,
        surface_description: brenn_surface_proto::SurfaceDescription {
            status_interval_secs: 60,
        },
        bindings: SurfaceBindings {
            components,
            subscriptions,
            outputs,
            local_channels: vec![
                local_channel(OVERLAY_STATE, 1),
                local_channel(APP_EVENTS, 1),
            ],
            chrome_instance: "chrome".into(),
        },
    })
    .unwrap()
}

/// A core driven to `Active` on [`welcome_frame_with_chrome`].
fn core_with_chrome() -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(Input::TextFrame(welcome_frame_with_chrome()), Millis(2));
    core
}

/// An overlay-state body naming `holder`.
fn overlay_state_body(holder: Option<&str>) -> String {
    serde_json::to_string(&brenn_surface_proto::OverlayStateBody {
        v: CONTROL_PLANE_VERSION,
        holder: holder.map(|h| h.to_string()),
        since_stamp: 42,
    })
    .expect("an OverlayStateBody serializes")
}

/// The overlay the core reports on a status frame.
fn reported_overlay(core: &mut ClientCore, now: Millis) -> Option<OverlayReport> {
    let effects = core.on_input(
        Input::Command(Command::SendStatus {
            instances: vec![],
            uptime_secs: 1,
            counters: StatusCounters::default(),
        }),
        now,
    );
    match frames(&effects).as_slice() {
        [ClientFrame::Status { overlay, .. }] => overlay.clone(),
        other => panic!("expected one Status frame, got {other:?}"),
    }
}

/// The `OverlayStateRejected` reasons an effect list carries, by publisher.
fn overlay_rejections(effects: &[Effect]) -> Vec<(String, String)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::EmitEvent(Event::OverlayStateRejected { instance, reason }) => {
                Some((instance.clone(), reason.clone()))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn the_kernel_records_the_overlay_chrome_reports() {
    // The status document's one page-state field. The kernel cannot infer it
    // from routed takeover traffic — chrome drops messages the router carried —
    // so it takes chrome's post-fold word for it, at the mint point.
    let mut core = core_with_chrome();
    register(&mut core, "chrome", Millis(3));
    publish(
        &mut core,
        1,
        "chrome",
        "overlay-state",
        &overlay_state_body(Some("meeting")),
        Millis(4),
    );
    let held = reported_overlay(&mut core, Millis(5)).expect("an overlay is held");
    assert_eq!(held.holder, "meeting");
    // The hold began when the transition was minted, on the wall clock — the
    // body's own stamp is page-monotonic and means nothing off the page.
    assert_eq!(held.since, test_stamp(1).publish_ts);

    // The pop is a report like any other, and clears the record.
    publish(
        &mut core,
        2,
        "chrome",
        "overlay-state",
        &overlay_state_body(None),
        Millis(6),
    );
    assert_eq!(reported_overlay(&mut core, Millis(7)), None);
}

#[test]
fn an_overlay_state_publish_from_a_non_chrome_instance_is_refused() {
    // The spoof guard. `chrome = true` is unique per surface, so a publish from
    // anyone else is a component (or an operator binding) claiming to speak for
    // chrome's screen. It is dropped — not retained, not delivered — and
    // reported, and the kernel's record is untouched.
    let mut core = core_with_chrome();
    register(&mut core, "chrome", Millis(3));
    publish(
        &mut core,
        1,
        "chrome",
        "overlay-state",
        &overlay_state_body(Some("meeting")),
        Millis(4),
    );
    let effects = publish(
        &mut core,
        2,
        "meeting",
        "overlay-state",
        &overlay_state_body(None),
        Millis(5),
    );

    let rejections = overlay_rejections(&effects);
    assert_eq!(rejections.len(), 1, "one report: {effects:?}");
    assert_eq!(rejections[0].0, "meeting");
    assert!(
        rejections[0].1.contains("chrome instance"),
        "unexpected reason: {}",
        rejections[0].1
    );
    // The publisher is told its message did not land: a `PublishResult` is the
    // only word it gets, and `Ok` for a dropped body would be a lie.
    assert_eq!(publish_statuses(&effects), vec![PublishStatus::Refused]);
    // Not on the plane, and the recorded overlay still says what chrome said.
    assert_eq!(ring_bodies(&core, OVERLAY_STATE).len(), 1);
    assert_eq!(
        reported_overlay(&mut core, Millis(6))
            .expect("the recorded overlay survives the refusal")
            .holder,
        "meeting"
    );
}

#[test]
fn an_unreportable_overlay_state_body_is_refused() {
    // Two ways chrome itself could hand the kernel something it cannot stand
    // behind: a body it cannot parse, and a holder the surface never declared —
    // which the server treats as a protocol violation and kills the session
    // over. Both are refused at the mint rather than reported onward.
    let mut core = core_with_chrome();
    register(&mut core, "chrome", Millis(3));
    let garbage = publish(
        &mut core,
        1,
        "chrome",
        "overlay-state",
        "not json",
        Millis(4),
    );
    assert_eq!(overlay_rejections(&garbage).len(), 1, "{garbage:?}");
    assert_eq!(publish_statuses(&garbage), vec![PublishStatus::Refused]);

    let ghost = publish(
        &mut core,
        2,
        "chrome",
        "overlay-state",
        &overlay_state_body(Some("ghost")),
        Millis(5),
    );
    let rejections = overlay_rejections(&ghost);
    assert_eq!(rejections.len(), 1, "{ghost:?}");
    assert!(
        rejections[0].1.contains("declared instance"),
        "unexpected reason: {}",
        rejections[0].1
    );
    assert_eq!(publish_statuses(&ghost), vec![PublishStatus::Refused]);
    assert!(ring_bodies(&core, OVERLAY_STATE).is_empty());
    assert_eq!(reported_overlay(&mut core, Millis(6)), None);
}

#[test]
fn a_buffered_overlay_state_publish_from_chrome_is_recorded() {
    // Chrome's *only* real path: it folds an activation window and publishes the
    // resulting transition from inside that entry, so the guard and the record
    // both hang off the buffered flush. A guard enforced on the gesture path
    // alone would leave the buffered path unprotected.
    let mut core = core_with_chrome();
    register(&mut core, "chrome", Millis(3));
    // An ingestion for chrome to fold; its entry publishes the transition.
    publish(&mut core, 1, "chrome", "app-out", "{}", Millis(4));
    publish_from_activation(
        &mut core,
        "chrome",
        "overlay-state",
        &overlay_state_body(Some("meeting")),
    );

    let held = reported_overlay(&mut core, Millis(5)).expect("an overlay is held");
    assert_eq!(held.holder, "meeting");
    assert_eq!(ring_bodies(&core, OVERLAY_STATE).len(), 1);
}

#[test]
fn a_buffered_overlay_state_publish_from_a_non_chrome_instance_is_refused() {
    // The refusal's buffered face, which differs from the gesture path's: the
    // publisher was answered at buffer time and there is no `PublishResult` to
    // carry a status, so the violation report is the whole of the signal. It
    // must survive the flush.
    let mut core = core_with_chrome();
    publish(
        &mut core,
        1,
        "chrome",
        "overlay-state",
        &overlay_state_body(Some("meeting")),
        Millis(3),
    );
    register(&mut core, "meeting", Millis(4));
    publish(&mut core, 2, "meeting", "app-out", "{}", Millis(5));
    let effects = publish_from_activation(
        &mut core,
        "meeting",
        "overlay-state",
        &overlay_state_body(None),
    );

    let rejections = overlay_rejections(&effects);
    assert_eq!(rejections.len(), 1, "one report: {effects:?}");
    assert_eq!(rejections[0].0, "meeting");
    assert!(
        publish_statuses(&effects).is_empty(),
        "a buffered publish was answered at buffer time: {effects:?}"
    );
    // Neither the plane nor the kernel's record moved.
    assert_eq!(ring_bodies(&core, OVERLAY_STATE).len(), 1);
    assert_eq!(
        reported_overlay(&mut core, Millis(6))
            .expect("the recorded overlay survives the refusal")
            .holder,
        "meeting"
    );
}

#[test]
fn a_welcome_that_drops_the_recorded_holder_clears_the_overlay() {
    // The record is validated against the bindings live when chrome published
    // it. A reconnect whose `Welcome` no longer declares that instance would
    // otherwise keep reporting it, and a `Status` naming an unconfigured
    // instance is a protocol violation the server kills the session over —
    // reconnect, report, violate, forever.
    let mut core = core_with_chrome();
    publish(
        &mut core,
        1,
        "chrome",
        "overlay-state",
        &overlay_state_body(Some("meeting")),
        Millis(3),
    );
    assert!(reported_overlay(&mut core, Millis(4)).is_some());

    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    core.on_input(
        Input::TextFrame(welcome_frame_chrome_with(vec!["chrome"])),
        Millis(3_007),
    );
    assert_eq!(
        reported_overlay(&mut core, Millis(3_008)),
        None,
        "a holder the surface no longer declares is not something to report"
    );
}

#[test]
fn a_welcome_that_keeps_the_recorded_holder_keeps_the_overlay() {
    // The other half: an ordinary reconnect is exactly when a live wedge most
    // needs reporting, so a holder the new bindings still declare survives.
    let mut core = core_with_chrome();
    publish(
        &mut core,
        1,
        "chrome",
        "overlay-state",
        &overlay_state_body(Some("meeting")),
        Millis(3),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(4),
    );
    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    core.on_input(Input::TextFrame(welcome_frame_with_chrome()), Millis(3_007));
    assert_eq!(
        reported_overlay(&mut core, Millis(3_008))
            .expect("the overlay survives a reconnect")
            .holder,
        "meeting"
    );
}

#[test]
#[should_panic(expected = "the kernel does not publish on local:brenn/takeover")]
fn the_kernel_may_not_mint_a_takeover_message() {
    // Unreachable from today's callers — `on_publish_control` asserts the plane
    // is kernel-publish-only, which takeover is not — so the guard sits at the
    // mint point, where a future kernel-side publish would arrive. An
    // unattributable takeover body is exactly what the identity model forbids,
    // and softening this to a passthrough would reopen it.
    let mut core = core_wired_to(TAKEOVER, 1);
    let _ = core.mint_and_route_local(
        TAKEOVER,
        LocalOrigin::Kernel,
        takeover_body(TakeoverAction::Release, ""),
        test_stamp(1),
        Urgency::Normal,
    );
}

#[test]
#[should_panic(expected = "the kernel does not publish on local:brenn/overlay-state")]
fn the_kernel_may_not_mint_an_overlay_state_message() {
    // The same guard on the telemetry plane: the kernel holds no overlay and
    // renders none, so a kernel-minted overlay report would be the kernel
    // inventing telemetry about a component's screen.
    let mut core = core_with_chrome();
    let _ = core.mint_and_route_local(
        OVERLAY_STATE,
        LocalOrigin::Kernel,
        overlay_state_body(Some("meeting")),
        test_stamp(1),
        Urgency::Normal,
    );
}

// ── the kernel's reserved control planes ─────────────────────────────────────

const LINK_STATE: &str = "local:brenn/link-state";
const TOAST: &str = "local:brenn/toast";

/// Feed a `Command::PublishControl` on `channel` — the kernel's own publish.
fn control(core: &mut ClientCore, channel: &str, body: &str, now: Millis) -> Vec<Effect> {
    core.on_input(
        Input::Command(Command::PublishControl {
            channel: channel.into(),
            body: body.into(),
            stamp: test_stamp(now.0),
        }),
        now,
    )
}

/// Every envelope `protobar` sees as new, as `(sender, body)`.
fn senders_and_bodies(core: &mut ClientCore) -> Vec<(String, String)> {
    new_envelopes_for(core, "protobar")
        .into_iter()
        .map(|e| (e.sender, e.body))
        .collect()
}

/// A core with `protobar` subscribed to `channel` (a reserved plane), driven to
/// `Active`.
fn core_bound_to(channel: &str, ring_depth: u64) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(channel, "protobar", "in")],
            vec![],
            vec![local_channel(channel, ring_depth)],
        )),
        Millis(2),
    );
    core
}

#[test]
fn a_kernel_control_publish_carries_the_bare_platform_identity() {
    // The two grains, pinned against each other in one test because the whole
    // point is that they differ: the kernel acts on nobody's behalf and takes the
    // bare surface identity, while a component publish on the very same router
    // takes its instance sub-identity. A mutation that stamped `local_sender` on
    // control traffic (or the bare id on component traffic) fails here.
    let mut core = core_bound_to(LINK_STATE, 1);
    register(&mut core, "protobar", Millis(3));
    control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"connected"}"#,
        Millis(4),
    );
    assert_eq!(
        senders_and_bodies(&mut core),
        vec![(
            "surface:deskbar".to_string(),
            r#"{"v":1,"state":"connected"}"#.to_string()
        )]
    );

    let mut component = local_core();
    register(&mut component, "protobar", Millis(3));
    publish(&mut component, 7, "protobar", "out", "{}", Millis(4));
    assert_eq!(
        senders_and_bodies(&mut component),
        vec![("surface:deskbar#protobar".to_string(), "{}".to_string())]
    );
}

#[test]
fn a_control_publish_delivers_page_locally_and_sends_no_frame() {
    let mut core = core_bound_to(LINK_STATE, 1);
    register(&mut core, "protobar", Millis(3));
    let effects = control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"fatal"}"#,
        Millis(4),
    );
    assert_eq!(
        new_bodies_for(&mut core, "protobar"),
        vec![r#"{"v":1,"state":"fatal"}"#.to_string()]
    );
    // The plane is page-local like any other `local:` channel: no wire frame, and
    // no `PublishResult` — the kernel is not a component awaiting an answer.
    assert!(frames(&effects).is_empty());
    assert!(publish_statuses(&effects).is_empty());
}

#[test]
fn a_control_publish_still_routes_after_the_core_goes_terminal() {
    // The terminal transition's own final notification: when the client core goes
    // Fatal it emits `Event::Fatal`, the kernel folds it and publishes the
    // matching link state, and that publish comes back as a command *after* the
    // state is already terminal. It must still reach chrome — the router's rings
    // and chrome's mount both outlive the transition — so the terminal banner is
    // drawn, not dropped on a dead router.
    let mut core = core_bound_to(LINK_STATE, 1);
    register(&mut core, "protobar", Millis(3));
    // Drive the core Fatal: an unexpected binary frame is a protocol violation.
    core.on_input(Input::BinaryFrame, Millis(4));
    let effects = control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"fatal"}"#,
        Millis(5),
    );
    assert_eq!(
        new_bodies_for(&mut core, "protobar"),
        vec![r#"{"v":1,"state":"fatal"}"#.to_string()],
        "the terminal link state must reach the bound port"
    );
    assert!(frames(&effects).is_empty(), "terminal, so no wire frame");
}

#[test]
fn a_reserved_plane_needs_no_declaration_to_be_publishable() {
    // The auto-binding property (§5.3): the reserved planes are contract-defined,
    // so the kernel publishes them whether or not any component ever declares a
    // binding — a `Welcome` is not what brings them into existence. With the
    // rings seeded only from `Welcome.local_channels`, this publish finds no ring
    // and panics; that is the mutation this test exists to catch.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame_local(vec![], vec![], vec![])),
        Millis(2),
    );
    let effects = control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"connected"}"#,
        Millis(3),
    );
    // Nobody is bound, so it lands in the ring and nowhere else — and never on
    // the wire.
    assert!(senders_and_bodies(&mut core).is_empty());
    assert!(frames(&effects).is_empty());
}

/// The depth-1 last-value handoff: a chrome mounting after the kernel has
/// already published the current link state must still learn it, because there
/// is no second copy coming — the kernel publishes transitions, not a heartbeat.
/// Retention holds the value and attach-time priming delivers it, so the handoff
/// is gap-free without a republish.
#[test]
fn a_reserved_planes_ring_hands_the_last_value_to_a_late_registrant() {
    let mut core = core_bound_to(LINK_STATE, 1);
    control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"connected"}"#,
        Millis(3),
    );
    register(&mut core, "protobar", Millis(4));
    // The value is retained…
    assert_eq!(
        core.local_rings[LINK_STATE]
            .ring
            .entries()
            .map(|(e, _)| e.body.clone())
            .collect::<Vec<_>>(),
        vec![r#"{"v":1,"state":"connected"}"#.to_string()],
        "the ring must hold the state published before the instance registered"
    );
    // …and registration alone dispatches it, as new.
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")).1,
        vec![r#"{"v":1,"state":"connected"}"#]
    );
}

/// A registration whose bound `local:` rings are all empty mints nothing: the
/// prime is what wakes an instance, and there is nothing to prime.
#[test]
fn registering_against_an_empty_local_ring_dispatches_nothing() {
    let mut core = core_bound_to(LINK_STATE, 1);
    register(&mut core, "protobar", Millis(4));
    assert!(
        core.take_ready_activation().is_none(),
        "an empty ring primes nothing, so nothing is ready"
    );
}

#[test]
fn a_control_publish_before_the_first_welcome_is_dropped() {
    // No `Welcome`, no participant id, so no identity to publish under — and an
    // unattributable envelope is what the identity model exists to prevent. Not a
    // loss: no component can be mounted this early either (the instance set rides
    // the same `Welcome`), and the next transition republishes.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    let effects = control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"connecting"}"#,
        Millis(1),
    );
    assert!(frames(&effects).is_empty());

    // And it left nothing in the ring for a later registrant: dropped means
    // dropped, not deferred.
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(LINK_STATE, "protobar", "in")],
            vec![],
            vec![local_channel(LINK_STATE, 1)],
        )),
        Millis(3),
    );
    register(&mut core, "protobar", Millis(4));
    assert!(
        core.local_rings[LINK_STATE].ring.entries().next().is_none(),
        "the dropped pre-Welcome publish left a value in the ring"
    );
}

/// The reachable half of the pre-`Welcome` story: a control publish needs an
/// identity, so it must follow some `Welcome`, but it need not follow the one
/// that creates the consuming queue. Published with the link down, retained, and
/// primed into a queue the *next* `Welcome`'s binding table brings into
/// existence — the transition reaches a port that did not exist when it
/// happened.
#[test]
fn a_control_publish_between_welcomes_primes_a_queue_created_later() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    // Nothing bound to the plane yet: the component is registered, but this
    // binding table gives it no port on it.
    core.on_input(
        Input::TextFrame(welcome_frame_local(vec![], vec![], vec![])),
        Millis(2),
    );
    register(&mut core, "protobar", Millis(3));

    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(4),
    );
    let effects = control(
        &mut core,
        LINK_STATE,
        r#"{"v":1,"state":"connecting"}"#,
        Millis(5),
    );
    assert!(frames(&effects).is_empty(), "page-local, link or no link");

    core.on_input(Input::Tick, Millis(3_005));
    core.on_input(Input::Opened, Millis(3_006));
    core.on_input(
        Input::TextFrame(welcome_frame_local(
            vec![local_binding(LINK_STATE, "protobar", "in")],
            vec![],
            vec![local_channel(LINK_STATE, 1)],
        )),
        Millis(3_007),
    );
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")).1,
        vec![r#"{"v":1,"state":"connecting"}"#],
        "the queue this Welcome created is primed with what was published before it"
    );
}

#[test]
#[should_panic(expected = "control publish on component-producer plane")]
fn the_kernel_may_not_publish_a_component_producer_plane() {
    // Theme's producers are components with a declared output binding, which boot
    // checks. The kernel has no such declaration, so nothing checks it there —
    // this assert is that check, on the one party boot cannot see.
    let mut core = local_core();
    control(&mut core, THEME, "{}", Millis(3));
}

#[test]
#[should_panic(expected = "control publish on non-reserved channel")]
fn a_control_publish_outside_the_reserved_table_panics() {
    let mut core = local_core();
    control(&mut core, "local:app/notes", "{}", Millis(3));
}

//! Shared fixtures and helpers for the `core` protocol-state-machine test
//! suite, plus the sibling test-module declarations. Helpers are `pub(super)`
//! so the sibling modules can reach them.

use super::*;
use crate::publish_buffer::PublishBuffer;
use crate::test_support::cfg;
// `GapReason` survives in `surface/schema` as server-interpreted wire encoding;
// these tests play the server, so they construct it. The kernel never matches on
// it.
use brenn_surface_schema::{
    Binding, GapReason, OutputBinding, PublishOutcome, ServerFrame, SubscribeOutcome,
    SurfaceBindings, Urgency,
};
use brenn_surface_test_fixtures::wire_cursor;
pub(super) use brenn_surface_test_fixtures::{deliver_frame_multi, deliver_target as target};

/// The canonical wire envelope with an identity derived from its body.
///
/// The shared fixture pins one constant `message_id` on every envelope it builds,
/// which the page's store reads as one message presented over and over: insertion
/// is idempotent by identity, so two "different" deliveries under a constant id
/// would collapse into one retained entry. These suites need both halves of that
/// — distinct messages to fill a window, and genuine re-presentation to exercise
/// the dedup — so identity follows the body: same body, same message; different
/// body, different message. A test that wants an explicit identity states one.
pub(super) fn sample_envelope(body: &str) -> MessageEnvelope {
    let mut envelope = brenn_surface_test_fixtures::sample_envelope(body);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(body, &mut hasher);
    envelope.message_id = Uuid::from_u128(u128::from(std::hash::Hasher::finish(&hasher)));
    envelope
}

mod activation;
mod defer;
mod deliver;
mod lifecycle;
mod local;
mod publish;
mod subs;

/// The bodies a confined channel's store retains, oldest first.
pub(super) fn confined_bodies(core: &ClientCore, channel: &str) -> Vec<String> {
    core.stores
        .get(&StoreKey::Confined(channel.to_string()))
        .expect("the channel's store")
        .retained()
        .map(|(e, _)| e.body.clone())
        .collect()
}

/// One subscription mirror's depth — the fold over its bindings that decides
/// how much history it holds, and so how much a fresh position on it is owed.
pub(super) fn wire_depth(core: &ClientCore, instance: &str, channel: &str) -> u64 {
    core.stores
        .get(&StoreKey::Wire(SubKey::for_instance(instance, channel)))
        .expect("the subscription's store")
        .depth()
}

/// The bodies one subscription's store retains, oldest first.
pub(super) fn wire_bodies(core: &ClientCore, instance: &str, channel: &str) -> Vec<String> {
    core.stores
        .get(&StoreKey::Wire(SubKey::for_instance(instance, channel)))
        .expect("the subscription's store")
        .retained()
        .map(|(e, _)| e.body.clone())
        .collect()
}

/// The wall clock every assembly and flush in this suite reads, epoch
/// milliseconds UTC (2023-11-14T22:13:20Z).
///
/// Fixed, like [`test_stamp`]: the core takes wall-clock time as data, so a test
/// can state exactly what a component was told the time was and what its release
/// times are measured against.
pub(super) const TEST_WALL_MS: u64 = 1_700_000_000_000;

/// Take the one ready activation, asserting there is exactly one.
pub(super) fn take_one(core: &mut ClientCore) -> ReadyActivation {
    let ready = core
        .take_ready_activation(TEST_WALL_MS)
        .expect("an activation is ready");
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "exactly one activation is ready"
    );
    ready
}

/// Complete an in-flight activation with `outcome`, minting one stamp per
/// buffered publish exactly as the driver does. Clears the instance's in-flight
/// mark so it can activate again.
pub(super) fn complete(
    core: &mut ClientCore,
    instance: &str,
    outcome: ActivationOutcome,
    buffer: PublishBuffer,
) -> Vec<Effect> {
    complete_at(core, instance, outcome, buffer, TEST_WALL_MS)
}

/// [`complete`] with an explicit flush instant, epoch milliseconds UTC — the
/// clock a deferred entry's release time is compared against to decide whether it
/// parks or publishes now.
///
/// One instant across every stamp, exactly as the driver's flush reads it once.
pub(super) fn complete_at(
    core: &mut ClientCore,
    instance: &str,
    outcome: ActivationOutcome,
    buffer: PublishBuffer,
    flush_ms: u64,
) -> Vec<Effect> {
    let publish_ts = DateTime::from_timestamp_millis(
        i64::try_from(flush_ms).expect("test flush instant is representable"),
    )
    .expect("test flush instant is representable");
    let stamps = (0..buffer.len())
        .map(|i| MessageStamp {
            message_id: Uuid::from_u128(0xf000 + i as u128),
            publish_ts,
        })
        .collect();
    core.on_input(
        Input::ActivationDone {
            instance: instance.into(),
            outcome,
            buffer,
            stamps,
        },
        Millis(50),
    )
}

/// The `Alert` frames in an effect list, as (severity, title, body).
pub(super) fn alerts(effects: &[Effect]) -> Vec<(AlertSeverity, String, String)> {
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
pub(super) fn toasts(effects: &[Effect]) -> Vec<ToastBody> {
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

/// The bodies of a window's context half and new half.
pub(super) fn split(window: &PortWindow) -> (Vec<&str>, Vec<&str>) {
    let boundary = window.new_from as usize;
    (
        window.envelopes[..boundary]
            .iter()
            .map(|e| e.body.as_str())
            .collect(),
        window.envelopes[boundary..]
            .iter()
            .map(|e| e.body.as_str())
            .collect(),
    )
}

pub(super) fn window<'a>(activation: &'a Activation, port: &str) -> &'a PortWindow {
    activation
        .ports
        .iter()
        .find(|w| w.port == port)
        .unwrap_or_else(|| panic!("no window for port {port}"))
}

/// The armed wakeup deadline in an effect list.
pub(super) fn wakeup(effects: &[Effect]) -> Millis {
    effects
        .iter()
        .find_map(|e| match e {
            Effect::SetWakeup(Some(d)) => Some(*d),
            _ => None,
        })
        .expect("a SetWakeup(Some) effect")
}

/// Assert the armed wakeup is a jittered backoff deadline within
/// `[now + nominal/2, now + nominal]` (equal jitter, `backoff_delay_ms`), and
/// return the captured deadline so a test that subsequently ticks at it uses
/// the real jittered value rather than the nominal.
pub(super) fn assert_backoff_deadline(effects: &[Effect], now: Millis, nominal: u64) -> Millis {
    let deadline = wakeup(effects);
    let delay = deadline.0 - now.0;
    assert!(
        nominal / 2 <= delay && delay <= nominal,
        "backoff delay {delay} not in [{}, {nominal}] (nominal {nominal}) at now {}",
        nominal / 2,
        now.0
    );
    deadline
}

/// Register `instance` for activation delivery and return the effects. This is
/// what opens an instance's subscriptions: a registered instance is a subscriber
/// like any other, and nothing else opens them for it.
pub(super) fn register(core: &mut ClientCore, instance: &str, now: Millis) -> Vec<Effect> {
    core.on_input(
        Input::ActivationRegistered {
            instance: instance.into(),
        },
        now,
    )
}

/// The instance every single-instance fixture binding in this suite belongs to.
///
/// Subscriptions are per (instance, channel), so a frame naming the wrong
/// instance reaches no subscription at all. The helpers below default to this
/// one and take an explicit override where a test exercises the grain itself —
/// so a sibling-instance test cannot silently pass by matching on channel alone.
pub(super) const TEST_INSTANCE: &str = "protobar";

pub(super) fn subscribe_result(channel: &str, outcome: SubscribeOutcome) -> String {
    subscribe_result_for(channel, TEST_INSTANCE, outcome)
}

/// `subscribe_result` naming an explicit principal.
pub(super) fn subscribe_result_for(
    channel: &str,
    instance: &str,
    outcome: SubscribeOutcome,
) -> String {
    serde_json::to_string(&ServerFrame::SubscribeResult {
        channel: channel.into(),
        instance: instance.to_owned(),
        outcome,
        replay_count: 0,
        gap: None,
    })
    .unwrap()
}

pub(super) fn subscribe_result_gap(
    channel: &str,
    outcome: SubscribeOutcome,
    reason: GapReason,
) -> String {
    subscribe_result_gap_for(channel, TEST_INSTANCE, outcome, reason)
}

pub(super) fn subscribe_result_gap_for(
    channel: &str,
    instance: &str,
    outcome: SubscribeOutcome,
    reason: GapReason,
) -> String {
    serde_json::to_string(&ServerFrame::SubscribeResult {
        channel: channel.into(),
        instance: instance.to_owned(),
        outcome,
        replay_count: 2,
        gap: Some(GapInfo { reason }),
    })
    .unwrap()
}

/// The push depth every test binding carries. Deliberately not 1 (the handle's
/// provisional capacity) so a test asserting a stamped policy would fail if the
/// binding's depth never reached its position.
pub(super) const TEST_PUSH_DEPTH: u64 = 8;

/// Sink-budget fill and carryover ceiling every output helper carries: the
/// resolved default of one publish per activation, one carried over.
pub(super) const TEST_FILL_MT: u64 = brenn_budget::MILLITOKENS_PER_PUBLISH;
pub(super) const TEST_CAPACITY_MT: u64 = brenn_budget::MILLITOKENS_PER_PUBLISH;

/// Context-window depth every binding helper carries: none.
///
/// The stated default for a binding that declares no `retain_depth`, and the
/// right fixture value while these helpers feed tests about subscription and
/// delivery rather than window assembly. A test about context sets its own.
pub(super) const TEST_RETAIN_DEPTH: u64 = 0;

pub(super) fn durable_binding(instance: &str, port: &str) -> Binding {
    Binding {
        channel: "brenn:events".into(),
        instance: instance.into(),
        port: port.into(),
        push_depth: TEST_PUSH_DEPTH,
        retain_depth: TEST_RETAIN_DEPTH,
        noise: brenn_surface_schema::NoiseLevel::Silent,
    }
}

pub(super) fn sub_binding() -> Binding {
    Binding {
        channel: "ephemeral:demo".into(),
        instance: "protobar".into(),
        port: "messages".into(),
        push_depth: TEST_PUSH_DEPTH,
        retain_depth: TEST_RETAIN_DEPTH,
        noise: brenn_surface_schema::NoiseLevel::Silent,
    }
}

pub(super) fn welcome_frame(subscriptions: Vec<Binding>, outputs: Vec<OutputBinding>) -> String {
    crate::test_support::welcome_frame(subscriptions, outputs)
}

/// Drive a fresh core to `Active` via `Opened` + a valid `Welcome`.
pub(super) fn active_core() -> ClientCore {
    active_core_with(vec![sub_binding()])
}

/// An ephemeral subscription binding on `ephemeral:demo`.
pub(super) fn ephemeral_binding(instance: &str, port: &str) -> Binding {
    Binding {
        channel: "ephemeral:demo".into(),
        instance: instance.into(),
        port: port.into(),
        push_depth: TEST_PUSH_DEPTH,
        retain_depth: TEST_RETAIN_DEPTH,
        noise: brenn_surface_schema::NoiseLevel::Silent,
    }
}

/// Assert the exact fatal-path effect shape and return the `Fatal` detail. With
/// no pending publishes, go_fatal emits exactly `[CloseTransport, Fatal,
/// SetWakeup(None)]` — no error-report breadcrumb (a dying connection).
pub(super) fn assert_fatal_shape(effects: &[Effect]) -> String {
    assert_eq!(effects.len(), 3, "fatal emits 3 effects: {effects:?}");
    assert_eq!(effects[0], Effect::CloseTransport);
    assert_eq!(effects[2], Effect::SetWakeup(None));
    match &effects[1] {
        Effect::EmitEvent(Event::Fatal { detail }) => detail.clone(),
        other => panic!("expected Fatal event, got {other:?}"),
    }
}

/// A close carrying the stale-build code and the server's build id.
pub(super) fn stale_build_close() -> Input {
    Input::Disconnected {
        code: Some(STALE_BUILD_CLOSE_CODE),
        reason: "server-build-99".into(),
    }
}

/// Feed a registration to a terminal core and assert it is absorbed silently.
///
/// A terminal core has no bindings, no refcounts, and no wire: there is nothing
/// to open and nobody to tell. The instance simply never activates, which is
/// exactly what a terminal page means.
pub(super) fn assert_post_terminal_register_absorbed(core: &mut ClientCore, now: Millis) {
    let effects = register(core, "protobar", now);
    assert!(
        effects.is_empty(),
        "post-terminal registration is absorbed: {effects:?}"
    );
}

/// A subscription binding for two ports (one channel) plus a durable one, so
/// tests can bind several pairs. Drives a fresh core to `Active`.
pub(super) fn active_core_with(subscriptions: Vec<Binding>) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(welcome_frame(subscriptions, vec![])),
        Millis(2),
    );
    core
}

/// Deregister `instance` and return the effects — the mirror of [`register`],
/// releasing its subscription references.
pub(super) fn deregister(core: &mut ClientCore, instance: &str, now: Millis) -> Vec<Effect> {
    core.on_input(
        Input::ActivationDeregistered {
            instance: instance.into(),
        },
        now,
    )
}

/// Drive `ephemeral:demo` all the way to a live subscription: register one
/// instance and ack its `Subscribe`.
pub(super) fn active_subscribed_core() -> ClientCore {
    let mut core = active_core_with(vec![sub_binding()]);
    register(&mut core, "protobar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    core
}

/// Two sibling instances live on one channel, each with its own subscription —
/// the shape live wire fan-out coalesces into one multi-target `Deliver`.
pub(super) fn active_sibling_core() -> ClientCore {
    let bindings = ["alice", "bob"]
        .into_iter()
        .map(|instance| Binding {
            instance: instance.into(),
            ..sub_binding()
        })
        .collect();
    let mut core = active_core_with(bindings);
    register(&mut core, "alice", Millis(5));
    register(&mut core, "bob", Millis(5));
    for instance in ["alice", "bob"] {
        core.on_input(
            Input::TextFrame(subscribe_result_for(
                "ephemeral:demo",
                instance,
                SubscribeOutcome::Ok,
            )),
            Millis(6),
        );
    }
    core
}

/// A stable wire cursor for a `Deliver` whose cursor content is irrelevant to
/// the test.
pub(super) fn test_cursor() -> Cursor {
    wire_cursor("c1")
}

pub(super) fn deliver_frame(channel: &str, envelope: &MessageEnvelope, seq: u64) -> String {
    deliver_frame_dropped(channel, envelope, seq, 0)
}

pub(super) fn deliver_frame_dropped(
    channel: &str,
    envelope: &MessageEnvelope,
    seq: u64,
    dropped: u64,
) -> String {
    deliver_frame_for_dropped(channel, TEST_INSTANCE, envelope, seq, dropped)
}

/// A `Deliver` to an explicit principal's subscription. No drops; [`deliver_frame_for_dropped`] carries
/// a count.
pub(super) fn deliver_frame_for(
    channel: &str,
    instance: &str,
    envelope: &MessageEnvelope,
    seq: u64,
) -> String {
    deliver_frame_for_dropped(channel, instance, envelope, seq, 0)
}

pub(super) fn deliver_frame_for_dropped(
    channel: &str,
    instance: &str,
    envelope: &MessageEnvelope,
    seq: u64,
    dropped: u64,
) -> String {
    deliver_frame_cursor(channel, instance, envelope, seq, test_cursor(), dropped)
}

/// A `Deliver` naming an explicit cursor blob, for tests that assert the
/// kernel echoes it verbatim as `Subscribe.resume` on reconnect.
pub(super) fn deliver_frame_cursor(
    channel: &str,
    instance: &str,
    envelope: &MessageEnvelope,
    seq: u64,
    cursor: Cursor,
    dropped: u64,
) -> String {
    serde_json::to_string(&ServerFrame::Deliver {
        channel: channel.into(),
        envelope: envelope.clone(),
        targets: vec![DeliverTarget {
            instance: instance.to_owned(),
            seq,
            cursor,
            dropped,
        }],
    })
    .unwrap()
}

/// A subscription binding on a second, distinct channel, so reconcile tests
/// can drop one channel while another survives.
pub(super) fn other_binding() -> Binding {
    Binding {
        channel: "ephemeral:other".into(),
        instance: "protobar".into(),
        port: "other".into(),
        push_depth: TEST_PUSH_DEPTH,
        retain_depth: TEST_RETAIN_DEPTH,
        noise: brenn_surface_schema::NoiseLevel::Silent,
    }
}

/// Drive `active_subscribed_core` through a transport blip and a resumed
/// re-`Subscribe`, leaving `ephemeral:demo` `Active` again. The `token_seq`
/// Deliver stores the channel's opaque resume cursor before the blip; the
/// resumed span tracker itself resets to empty (the class-blind model does not
/// seed it from the cursor). Returns the reconnected, re-activated core.
pub(super) fn resumed_core_seeded_at(token_seq: u64) -> ClientCore {
    let mut core = active_subscribed_core(); // port 1 on ephemeral:demo, Active
    // A Deliver sets the channel's high-water token to `token_seq`.
    core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("v"),
            token_seq,
        )),
        Millis(7),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    ); // blip; the instance stays registered
    core.on_input(Input::Tick, Millis(3_008)); // reconnect
    core.on_input(Input::Opened, Millis(3_009));
    // Welcome resubscribes the survivor echoing the retained cursor; the span
    // tracker resets to empty (not seeded from `token_seq`).
    core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3_010),
    );
    // Ack the resumed Subscribe → Active.
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(3_011),
    );
    core
}

/// An ephemeral output binding.
pub(super) fn output_binding(instance: &str, port: &str) -> OutputBinding {
    output_binding_at(instance, port, Urgency::Normal)
}

/// [`output_binding`] with an explicit configured default urgency, for the tests
/// that assert what the port's default does to a publish.
pub(super) fn output_binding_at(instance: &str, port: &str, urgency: Urgency) -> OutputBinding {
    OutputBinding {
        channel: "ephemeral:outdemo".into(),
        instance: instance.into(),
        port: port.into(),
        urgency,
        fill_mt: TEST_FILL_MT,
        capacity_mt: TEST_CAPACITY_MT,
    }
}

/// Drive a fresh core to `Active` with the given output bindings (no
/// subscriptions), the state the publish path exercises.
pub(super) fn active_core_with_outputs(outputs: Vec<OutputBinding>) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(Input::TextFrame(welcome_frame(vec![], outputs)), Millis(2));
    core
}

/// Feed a `Publish` command and return the effects. Stamps [`test_stamp`], so a
/// local publish's synthesized envelope is exactly assertable.
pub(super) fn publish(
    core: &mut ClientCore,
    correlation: u64,
    instance: &str,
    port: &str,
    body: &str,
    now: Millis,
) -> Vec<Effect> {
    core.on_input(
        Input::Command(Command::Publish {
            correlation,
            instance: instance.into(),
            port: port.into(),
            body: body.into(),
            // The ordinary-publish grain: only the reserved error-report port
            // ever names a subject, and `publish_with_subject` covers that.
            subject_instance: None,
            stamp: test_stamp(correlation),
            urgency: None,
        }),
        now,
    )
}

/// [`publish`] with an explicit per-message urgency override — what
/// `ClientHandle::publish_with_urgency` feeds the core. Separate helper for the
/// same reason as [`publish_with_subject`].
pub(super) fn publish_at(
    core: &mut ClientCore,
    correlation: u64,
    instance: &str,
    port: &str,
    body: &str,
    urgency: Urgency,
    now: Millis,
) -> Vec<Effect> {
    core.on_input(
        Input::Command(Command::Publish {
            correlation,
            instance: instance.into(),
            port: port.into(),
            body: body.into(),
            subject_instance: None,
            urgency: Some(urgency),
            stamp: test_stamp(correlation),
        }),
        now,
    )
}

/// [`publish`] with a report subject attached — the reserved error-report port's
/// shape. Separate helper rather than a fifth parameter on `publish`, so the
/// dozens of ordinary-publish call sites do not each restate `None`.
pub(super) fn publish_with_subject(
    core: &mut ClientCore,
    correlation: u64,
    instance: &str,
    port: &str,
    body: &str,
    subject_instance: Option<&str>,
    now: Millis,
) -> Vec<Effect> {
    core.on_input(
        Input::Command(Command::Publish {
            correlation,
            instance: instance.into(),
            port: port.into(),
            body: body.into(),
            subject_instance: subject_instance.map(str::to_owned),
            stamp: test_stamp(correlation),
            urgency: None,
        }),
        now,
    )
}

/// A deterministic [`MessageStamp`] keyed by `correlation`, standing in for the
/// driver's wall-clock and UUID reads. Distinct per correlation so a test can
/// tell two synthesized envelopes apart, and fixed so it can assert their exact
/// contents — the payoff of the core taking these as data rather than reading
/// them.
pub(super) fn test_stamp(correlation: u64) -> MessageStamp {
    MessageStamp {
        message_id: Uuid::from_u128(
            0xc0de_0000_0000_0000_0000_0000_0000_0000 + u128::from(correlation),
        ),
        publish_ts: DateTime::from_timestamp(1_700_000_000 + correlation as i64, 0)
            .expect("test stamp timestamp is representable"),
    }
}

pub(super) fn publish_result_frame(correlation: Option<u64>, outcome: PublishOutcome) -> String {
    serde_json::to_string(&ServerFrame::PublishResult {
        correlation,
        outcome,
    })
    .unwrap()
}

/// Drive a fresh core to `Active` with the given output bindings and the
/// error-report floor advertised at `warn`, so the reserved
/// `#brenn`/`error-reports` port is live (treated as bound by the publish path).
pub(super) fn active_core_with_reports(outputs: Vec<OutputBinding>) -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let welcome = crate::test_support::welcome_frame_reports_with_outputs(outputs);
    core.on_input(Input::TextFrame(welcome), Millis(2));
    core
}

/// Drive a fresh core to `Active` via a `Welcome` that grants the alert
/// plane (`alert_granted: true`) — the default `active_core` fixture is
/// ungranted, matching the deny-by-default server policy.
pub(super) fn active_core_alert_granted() -> ClientCore {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let welcome = serde_json::to_string(&ServerFrame::Welcome {
        surface: "deskbar".into(),
        participant_id: "surface:deskbar".into(),
        heartbeat_secs: 20,
        max_body_bytes: 65_536,
        alert_granted: true,
        takeover_granted: false,
        error_report_floor: None,
        surface_description: brenn_surface_schema::SurfaceDescription {
            status_interval_secs: 60,
        },
        bindings: SurfaceBindings {
            components: vec![brenn_surface_schema::ComponentEntry {
                instance: "protobar".into(),
                kind: "protobar".into(),
                abi: brenn_surface_schema::Abi::Dom,
                parked_batch_depth: 8,
                config: Default::default(),
            }],
            subscriptions: vec![sub_binding()],
            outputs: vec![],
            local_channels: vec![],
            chrome_instance: String::new(),
        },
    })
    .unwrap();
    core.on_input(Input::TextFrame(welcome), Millis(2));
    core
}

/// Feed an `Alert` command and return the effects.
pub(super) fn alert(
    core: &mut ClientCore,
    severity: AlertSeverity,
    title: &str,
    body: &str,
    now: Millis,
) -> Vec<Effect> {
    core.on_input(
        Input::Command(Command::Alert {
            severity,
            title: title.into(),
            body: body.into(),
        }),
        now,
    )
}

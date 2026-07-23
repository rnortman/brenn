use super::super::*;
use super::*;
use crate::test_support::cfg;
use brenn_surface_proto::{Binding, InstanceState, PublishOutcome, ServerFrame};

#[test]
fn check_publish_maps_each_single_failure_to_its_variant() {
    assert_eq!(
        check_publish(false, || true, 0, 100),
        Err(PublishCheckReject::NotConnected)
    );
    assert_eq!(
        check_publish(true, || false, 0, 100),
        Err(PublishCheckReject::UnboundPort)
    );
    assert_eq!(
        check_publish(true, || true, 101, 100),
        Err(PublishCheckReject::BodyTooLarge { len: 101, max: 100 })
    );
}

#[test]
fn check_publish_order_is_not_connected_then_unbound_then_too_large() {
    // All predicates failing → the first (NotConnected) wins.
    assert_eq!(
        check_publish(false, || false, 999, 100),
        Err(PublishCheckReject::NotConnected)
    );
    // Connected but both unbound and oversized → UnboundPort wins over
    // BodyTooLarge.
    assert_eq!(
        check_publish(true, || false, 999, 100),
        Err(PublishCheckReject::UnboundPort)
    );
}

#[test]
fn check_publish_body_at_cap_is_ok() {
    // Boundary: len == max is within the cap.
    assert_eq!(check_publish(true, || true, 100, 100), Ok(()));
}

#[test]
fn check_publish_is_lazy_when_not_connected() {
    // The bound closure must not run when disconnected — the core's caller
    // `expect`s bindings that exist only while Active, so a non-lazy check
    // would panic on the reconnect path this pins.
    let reject = check_publish(
        false,
        || panic!("output_bound must not be evaluated when disconnected"),
        0,
        100,
    );
    assert_eq!(reject, Err(PublishCheckReject::NotConnected));
}

#[test]
fn initial_connect_appends_build_query_and_arms_handshake() {
    let (_core, effects) = ClientCore::new(cfg(), Millis(0));
    match &effects[0] {
        Effect::Connect { url } => {
            assert_eq!(url, "wss://host/surface/deskbar/ws?build=buildxyz");
        }
        other => panic!("expected Connect, got {other:?}"),
    }
    assert_eq!(wakeup(&effects), Millis(15_000));
}

#[test]
fn build_id_is_url_encoded_in_query() {
    let mut c = cfg();
    c.build_id = "a b/#".into();
    let (_core, effects) = ClientCore::new(c, Millis(0));
    match &effects[0] {
        Effect::Connect { url } => {
            assert!(url.ends_with("?build=a%20b%2F%23"), "{url}");
        }
        other => panic!("expected Connect, got {other:?}"),
    }
}

#[test]
fn backoff_schedule_doubles_and_caps() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    // The doubling shape is still asserted through the per-step jitter window:
    // each step's delay lands in [nominal/2, nominal], and consecutive nominal
    // windows only touch at a single endpoint, so a monotone-increasing (then
    // capped) schedule is still pinned.
    let nominals = [3_000u64, 6_000, 12_000, 24_000, 48_000, 60_000, 60_000];
    let mut now = Millis(0);
    for &nominal in &nominals {
        // In `Connecting` (from `new` or a post-backoff reconnect).
        let effects = core.on_input(Input::ConnectFailed, now);
        let deadline = assert_backoff_deadline(&effects, now, nominal);
        // Advance to the backoff deadline and reconnect.
        now = deadline;
        let effects = core.on_input(Input::Tick, now);
        assert!(matches!(effects[0], Effect::Connect { .. }));
    }
}

/// Collect the jittered backoff delay of each of the first `steps` reconnect
/// backoffs from a core seeded with `seed`.
fn backoff_delays(seed: u64, steps: usize) -> Vec<u64> {
    let mut c = cfg();
    c.backoff_jitter_seed = seed;
    let (mut core, _init) = ClientCore::new(c, Millis(0));
    let mut now = Millis(0);
    let mut delays = Vec::with_capacity(steps);
    for _ in 0..steps {
        let effects = core.on_input(Input::ConnectFailed, now);
        let deadline = wakeup(&effects);
        delays.push(deadline.0 - now.0);
        now = deadline;
        core.on_input(Input::Tick, now); // reconnect, back to Connecting
    }
    delays
}

#[test]
fn backoff_jitter_stays_within_the_equal_jitter_window() {
    // Every step's delay is in [nominal/2, nominal], including several capped
    // steps whose window is [30_000, 60_000].
    let nominals = [
        3_000u64, 6_000, 12_000, 24_000, 48_000, 60_000, 60_000, 60_000,
    ];
    let delays = backoff_delays(0x1234_5678, nominals.len());
    for (&nominal, &delay) in nominals.iter().zip(&delays) {
        assert!(
            nominal / 2 <= delay && delay <= nominal,
            "delay {delay} outside [{}, {nominal}]",
            nominal / 2
        );
    }
    // Jitter actually spreads the cap: the capped steps are not all pinned to
    // the nominal 60_000 (guards against the jitter being computed but the cap
    // returning the nominal unchanged).
    let capped: Vec<u64> = delays[5..].to_vec();
    assert!(
        capped.iter().any(|&d| d < 60_000),
        "capped steps never jittered below the nominal: {capped:?}"
    );
}

#[test]
fn same_seed_and_inputs_produce_identical_backoff() {
    // The purity contract: same seed + same input sequence → same schedule.
    assert_eq!(backoff_delays(0xABCD, 8), backoff_delays(0xABCD, 8));
}

#[test]
fn distinct_seeds_decorrelate_the_backoff_schedule() {
    // Two clients seeded differently must not march in lockstep: their
    // schedules differ in at least one step. Guards against the seed being
    // plumbed through but ignored.
    let a = backoff_delays(1, 8);
    let b = backoff_delays(2, 8);
    assert_ne!(a, b, "distinct seeds produced identical schedules");
}

#[test]
fn reconnect_arms_handshake_from_reconnect_time() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::ConnectFailed, Millis(0)); // backoff deadline 3000
    let effects = core.on_input(Input::Tick, Millis(3_000));
    assert!(matches!(effects[0], Effect::Connect { .. }));
    assert_eq!(wakeup(&effects), Millis(18_000)); // 3000 + 15000
}

#[test]
fn handshake_timeout_after_open_closes_and_backs_off() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    assert!(core.on_input(Input::Opened, Millis(100)).is_empty());
    let effects = core.on_input(Input::Tick, Millis(15_000));
    assert_eq!(effects[0], Effect::CloseTransport);
    assert_backoff_deadline(&effects, Millis(15_000), 3_000);
}

#[test]
fn connect_hang_times_out_and_backs_off() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    // No `Opened`: the connect attempt itself never resolves.
    let effects = core.on_input(Input::Tick, Millis(15_000));
    assert_eq!(effects[0], Effect::CloseTransport);
    assert_backoff_deadline(&effects, Millis(15_000), 3_000);
}

#[test]
fn close_before_welcome_consumes_a_backoff_step() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    assert!(core.on_input(Input::Opened, Millis(50)).is_empty());
    let effects = core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(1_000),
    );
    // Disconnect from AwaitingWelcome surfaces the event, then backs off.
    assert_eq!(
        effects[0],
        Effect::EmitEvent(Event::Disconnected {
            reason: DisconnectReason::TransportClosed,
        })
    );
    let deadline = assert_backoff_deadline(&effects, Millis(1_000), 3_000);
    let effects = core.on_input(Input::Tick, deadline); // reconnect
    assert!(matches!(effects[0], Effect::Connect { .. }));
    let effects = core.on_input(Input::ConnectFailed, deadline);
    // The step doubled: the second backoff nominal is 6s from the reconnect.
    assert_backoff_deadline(&effects, deadline, 6_000);
}

#[test]
fn early_tick_in_backoff_rearms_without_reconnecting() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    let effects = core.on_input(Input::ConnectFailed, Millis(0));
    // Capture the jittered deadline (in [1500, 3000]); Millis(1000) is before
    // its floor, so the early tick re-arms to that same deadline rather than
    // reconnecting.
    let deadline = assert_backoff_deadline(&effects, Millis(0), 3_000);
    let effects = core.on_input(Input::Tick, Millis(1_000)); // fires early
    assert_eq!(effects, vec![Effect::SetWakeup(Some(deadline))]);
    let effects = core.on_input(Input::Tick, deadline);
    assert!(matches!(effects[0], Effect::Connect { .. }));
}

#[test]
fn early_tick_during_handshake_rearms() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    let effects = core.on_input(Input::Tick, Millis(5_000)); // before 15000
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(15_000)))]);
}

#[test]
fn stray_transport_events_are_absorbed_not_panicked() {
    // A transport-sourced input arriving in a state that no longer owns that
    // transport is a post-close straggler (async race), absorbed with no
    // effects rather than panicking.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    // In `Connecting`: a late close/frame from a prior transport.
    assert!(
        core.on_input(
            Input::Disconnected {
                code: None,
                reason: String::new()
            },
            Millis(0)
        )
        .is_empty()
    );
    assert!(
        core.on_input(Input::TextFrame("late".into()), Millis(0))
            .is_empty()
    );
    // Drive to `Backoff` via a handshake timeout, then feed stragglers.
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(Input::Tick, Millis(15_000)); // handshake timeout
    assert_eq!(effects[0], Effect::CloseTransport);
    assert!(
        core.on_input(
            Input::Disconnected {
                code: None,
                reason: String::new()
            },
            Millis(15_001)
        )
        .is_empty()
    );
    assert!(core.on_input(Input::Opened, Millis(15_002)).is_empty());
    assert!(core.on_input(Input::BinaryFrame, Millis(15_003)).is_empty());
}

#[test]
fn stray_transport_event_after_liveness_timeout_is_absorbed() {
    // Liveness timeout emits CloseTransport and enters Backoff; a close event
    // for that just-closed transport must not panic.
    let mut core = active_core(); // deadline 60_002
    let effects = core.on_input(Input::Tick, Millis(60_002));
    assert_eq!(effects[0], Effect::CloseTransport);
    assert!(
        core.on_input(
            Input::Disconnected {
                code: None,
                reason: String::new()
            },
            Millis(60_003)
        )
        .is_empty()
    );
}

#[test]
fn welcome_reaches_active_and_emits_connected() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    assert!(core.on_input(Input::Opened, Millis(1)).is_empty());
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(2),
    );
    // Arm the liveness deadline (3 × 20s = 60s from Welcome at Millis(2)),
    // then surface Connected. No client frame is sent on the happy path.
    assert_eq!(effects.len(), 2);
    assert_eq!(effects[0], Effect::SetWakeup(Some(Millis(60_002))));
    match &effects[1] {
        Effect::EmitEvent(Event::Connected {
            bindings,
            participant_id,
            max_body_bytes,
            alert_granted,
            takeover_granted: _,
            error_report_floor: _,
            surface_description: _,
        }) => {
            assert_eq!(participant_id, "surface:deskbar");
            assert_eq!(*max_body_bytes, 65_536);
            assert!(!*alert_granted);
            assert_eq!(bindings.subscriptions[0].channel, "ephemeral:demo");
        }
        other => panic!("expected Connected, got {other:?}"),
    }
}

#[test]
fn welcome_resets_backoff() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    // Two failed attempts bump the backoff step (3s then 6s).
    core.on_input(Input::ConnectFailed, Millis(0));
    core.on_input(Input::Tick, Millis(3_000));
    core.on_input(Input::ConnectFailed, Millis(3_000));
    core.on_input(Input::Tick, Millis(9_000));
    // This attempt succeeds and receives a Welcome — backoff resets.
    core.on_input(Input::Opened, Millis(9_100));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(9_200),
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::EmitEvent(Event::Connected { .. })))
    );
    // A subsequent disconnect backs off from the reset step: 3s, not doubled.
    let effects = core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(10_000),
    );
    // Reset to the initial 3s nominal (not the doubled 12s the two prior
    // failures would have reached).
    assert_backoff_deadline(&effects, Millis(10_000), 3_000);
}

#[test]
fn unparseable_frame_while_awaiting_is_fatal_and_terminal() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(Input::TextFrame("{not json".into()), Millis(2));
    let detail = assert_fatal_shape(&effects);
    assert!(detail.starts_with("unparseable server frame"), "{detail}");
    // Terminal: further inputs are absorbed, no reconnect.
    assert!(core.on_input(Input::Tick, Millis(100)).is_empty());
    assert!(
        core.on_input(
            Input::Disconnected {
                code: None,
                reason: String::new()
            },
            Millis(200)
        )
        .is_empty()
    );
}

#[test]
fn non_welcome_first_frame_is_fatal() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let hb = serde_json::to_string(&ServerFrame::Heartbeat).unwrap();
    let effects = core.on_input(Input::TextFrame(hb), Millis(2));
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("expected Welcome"), "{detail}");
}

#[test]
fn binary_frame_while_awaiting_is_fatal() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(Input::BinaryFrame, Millis(2));
    assert_fatal_shape(&effects);
}

#[test]
fn welcome_with_unsupported_binding_scheme_is_fatal() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let bad = Binding {
        channel: "weird:demo".into(),
        instance: "protobar".into(),
        port: "messages".into(),
        push_depth: TEST_PUSH_DEPTH,
        retain_depth: TEST_RETAIN_DEPTH,
        noise: brenn_surface_proto::NoiseLevel::Silent,
    };
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![bad], vec![])),
        Millis(2),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("unsupported scheme"), "{detail}");
}

#[test]
fn heartbeat_in_active_resets_liveness_deadline() {
    let mut core = active_core();
    let hb = serde_json::to_string(&ServerFrame::Heartbeat).unwrap();
    // active_core() reached Active at Millis(2); a Heartbeat at Millis(10)
    // pushes the deadline to 10 + 60_000 and carries no other effect.
    let effects = core.on_input(Input::TextFrame(hb), Millis(10));
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_010)))]);
}

#[test]
fn liveness_timeout_closes_and_backs_off() {
    let mut core = active_core(); // Active at Millis(2), deadline 60_002
    let effects = core.on_input(Input::Tick, Millis(60_002));
    assert_eq!(effects[0], Effect::CloseTransport);
    assert_eq!(
        effects[1],
        Effect::EmitEvent(Event::Disconnected {
            reason: DisconnectReason::LivenessTimeout,
        })
    );
    // Backoff was reset by Welcome, so the first delay is the initial 3s.
    let deadline = assert_backoff_deadline(&effects, Millis(60_002), 3_000);
    // And it reconnects at the backoff deadline.
    let effects = core.on_input(Input::Tick, deadline);
    assert!(matches!(effects[0], Effect::Connect { .. }));
}

#[test]
fn clean_close_emits_disconnected_and_backs_off() {
    let mut core = active_core(); // Active at Millis(2), deadline 60_002
    // A clean peer WS close (code 1000) while Active: no CloseTransport (the
    // transport is already gone), Disconnected first, then backoff.
    let effects = core.on_input(
        Input::Disconnected {
            code: Some(1000),
            reason: "bye".to_string(),
        },
        Millis(10),
    );
    assert_eq!(
        effects[0],
        Effect::EmitEvent(Event::Disconnected {
            reason: DisconnectReason::TransportClosed,
        })
    );
    // Backoff was reset by Welcome, so the first delay is the initial 3s.
    let deadline = assert_backoff_deadline(&effects, Millis(10), 3_000);
    // And it reconnects at the backoff deadline.
    let effects = core.on_input(Input::Tick, deadline);
    assert!(matches!(effects[0], Effect::Connect { .. }));
}

#[test]
fn early_tick_in_active_rearms_without_disconnecting() {
    let mut core = active_core(); // deadline 60_002
    let effects = core.on_input(Input::Tick, Millis(30_000));
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_002)))]);
}

#[test]
fn heartbeats_keep_the_connection_alive_past_the_original_deadline() {
    let mut core = active_core(); // deadline 60_002
    let hb = serde_json::to_string(&ServerFrame::Heartbeat).unwrap();
    // A heartbeat at 40s pushes the deadline out; a tick at the old
    // deadline is now early and merely re-arms.
    let effects = core.on_input(Input::TextFrame(hb.clone()), Millis(40_000));
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(100_000)))]);
    let effects = core.on_input(Input::Tick, Millis(60_002));
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(100_000)))]);
}

#[test]
fn second_welcome_in_active_is_fatal() {
    let mut core = active_core();
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(10),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("second Welcome"), "{detail}");
}

#[test]
fn publish_result_with_unknown_correlation_is_fatal() {
    // A PublishResult whose correlation matches no outstanding publish is
    // inexplicable — the server only ever echoes a correlation the client
    // sent — so it is a fatal protocol error.
    let mut core = active_core();
    let pr = serde_json::to_string(&ServerFrame::PublishResult {
        correlation: Some(1),
        outcome: PublishOutcome::Ok,
    })
    .unwrap();
    let effects = core.on_input(Input::TextFrame(pr), Millis(10));
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("unknown correlation"), "{detail}");
}

#[test]
fn binary_frame_in_active_is_fatal() {
    let mut core = active_core();
    let effects = core.on_input(Input::BinaryFrame, Millis(10));
    assert_fatal_shape(&effects);
}

#[test]
fn active_disconnect_backs_off_and_reconnects() {
    let mut core = active_core();
    let effects = core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(10),
    );
    let deadline = assert_backoff_deadline(&effects, Millis(10), 3_000); // reset by Welcome
    let effects = core.on_input(Input::Tick, deadline);
    assert!(matches!(effects[0], Effect::Connect { .. }));
}

// ── Stale-build close ─────────────────────────────────────────────────

#[test]
fn stale_build_close_while_awaiting_reloads_and_is_terminal() {
    // The server sends the stale-build close pre-Welcome: this client is
    // older than the build now served. It surfaces ReloadRequired and enters
    // the terminal state — no reconnect, no backoff.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    assert!(core.on_input(Input::Opened, Millis(1)).is_empty());
    let effects = core.on_input(stale_build_close(), Millis(2));
    assert_eq!(
        effects,
        vec![
            Effect::EmitEvent(Event::ReloadRequired {
                server_build: "server-build-99".into(),
            }),
            Effect::SetWakeup(None),
        ]
    );
    // Terminal: subsequent ticks and disconnects are absorbed, no reconnect.
    assert!(core.on_input(Input::Tick, Millis(100)).is_empty());
    assert!(
        core.on_input(
            Input::Disconnected {
                code: None,
                reason: String::new(),
            },
            Millis(200),
        )
        .is_empty()
    );
}

#[test]
fn stale_build_close_while_active_reloads() {
    // The deploy-while-page-open case: a reconnect hits the new build. From
    // Active the stale-build close is still terminal (no CloseTransport — the
    // transport already went away).
    let mut core = active_core();
    let effects = core.on_input(stale_build_close(), Millis(10));
    assert_eq!(
        effects,
        vec![
            Effect::EmitEvent(Event::ReloadRequired {
                server_build: "server-build-99".into(),
            }),
            Effect::SetWakeup(None),
        ]
    );
}

#[test]
fn non_stale_close_code_backs_off_as_usual() {
    // Any close code other than the stale-build one is an ordinary drop:
    // reset the bus plane and back off, never ReloadRequired.
    let mut core = active_core();
    let effects = core.on_input(
        Input::Disconnected {
            code: Some(1000),
            reason: "bye".into(),
        },
        Millis(10),
    );
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::EmitEvent(Event::ReloadRequired { .. })))
    );
    let deadline = assert_backoff_deadline(&effects, Millis(10), 3_000); // reset by Welcome
    let effects = core.on_input(Input::Tick, deadline);
    assert!(matches!(effects[0], Effect::Connect { .. }));
}

#[test]
fn close_while_active_closes_transport_and_is_terminal() {
    // The kernel requests an orderly shutdown: close the transport, disarm the
    // timer, and enter the terminal Closed state — no event, no reconnect.
    let mut core = active_core();
    let effects = core.on_input(Input::Command(Command::Close), Millis(10));
    assert_eq!(
        effects,
        vec![Effect::CloseTransport, Effect::SetWakeup(None)]
    );
    // Terminal: subsequent ticks, frames, and commands are absorbed, and no
    // reconnect is ever scheduled.
    assert!(core.on_input(Input::Tick, Millis(100)).is_empty());
    assert!(
        core.on_input(Input::TextFrame(welcome_frame(vec![], vec![])), Millis(200))
            .is_empty()
    );
    assert!(
        core.on_input(
            Input::Disconnected {
                code: None,
                reason: String::new(),
            },
            Millis(300),
        )
        .is_empty()
    );
}

#[test]
fn close_fails_outstanding_publishes_connection_lost() {
    // A publish still awaiting its result when the kernel closes is completed
    // with ConnectionLost, ahead of the terminal tail.
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    publish(&mut core, 7, "protobar", "out", "hi", Millis(5));
    let effects = core.on_input(Input::Command(Command::Close), Millis(6));
    assert_eq!(
        effects,
        vec![
            Effect::CloseTransport,
            Effect::EmitEvent(Event::PublishResult {
                instance: "protobar".into(),
                port: "out".into(),
                correlation: 7,
                status: PublishStatus::ConnectionLost,
            }),
            Effect::SetWakeup(None),
        ]
    );
}

#[test]
fn post_terminal_registration_is_absorbed() {
    // Every terminal state absorbs a post-terminal registration silently: there
    // are no bindings to open, no wire to open them on, and nobody to tell. The
    // instance simply never activates, which is what a terminal page means.
    //
    // Fatal: a second Welcome to an active core.
    let mut fatal = active_core();
    let effects = fatal.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(10),
    );
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::EmitEvent(Event::Fatal { .. }))),
        "second Welcome should be fatal: {effects:?}"
    );
    assert_post_terminal_register_absorbed(&mut fatal, Millis(11));

    // ReloadRequired: a stale-build close from active.
    let mut reload = active_core();
    reload.on_input(stale_build_close(), Millis(10));
    assert_post_terminal_register_absorbed(&mut reload, Millis(11));

    // Closed: a kernel-requested close from active.
    let mut closed = active_core();
    closed.on_input(Input::Command(Command::Close), Millis(10));
    assert_post_terminal_register_absorbed(&mut closed, Millis(11));
}

#[test]
fn telemetry_before_welcome_is_dropped() {
    // Not `Active` (Connecting): both telemetry commands send nothing.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    assert!(
        core.on_input(
            Input::Command(Command::SendGeometry {
                width: 800,
                height: 600,
                device_pixel_ratio: 1.0,
            }),
            Millis(1),
        )
        .is_empty()
    );
    assert!(
        core.on_input(
            Input::Command(Command::SendStatus {
                instances: vec![],
                uptime_secs: 5,
                counters: StatusCounters {
                    deliveries: 0,
                    publishes: 0,
                    errors: 0,
                    instances: Default::default(),
                },
            }),
            Millis(2),
        )
        .is_empty()
    );
}

#[test]
fn telemetry_on_active_surface_sends_frames() {
    // Every `Welcome` carries `surface_description`, so an `Active` core puts the
    // geometry/status frames on the wire.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    core.on_input(
        Input::TextFrame(crate::test_support::welcome_frame(vec![], vec![])),
        Millis(2),
    );
    let geometry = core.on_input(
        Input::Command(Command::SendGeometry {
            width: 1920,
            height: 515,
            device_pixel_ratio: 2.0,
        }),
        Millis(3),
    );
    assert_eq!(
        geometry,
        vec![Effect::SendFrame(ClientFrame::Geometry {
            width: 1920,
            height: 515,
            device_pixel_ratio: 2.0,
        })]
    );
    let status = core.on_input(
        Input::Command(Command::SendStatus {
            instances: vec![InstanceReport {
                instance: "protobar".into(),
                kind: "protobar".into(),
                state: InstanceState::Mounted,
                reason: None,
                ports_attached: 1,
            }],
            uptime_secs: 42,
            counters: StatusCounters {
                deliveries: 3,
                publishes: 1,
                errors: 0,
                instances: Default::default(),
            },
        }),
        Millis(4),
    );
    assert_eq!(
        status,
        vec![Effect::SendFrame(ClientFrame::Status {
            instances: vec![InstanceReport {
                instance: "protobar".into(),
                kind: "protobar".into(),
                state: InstanceState::Mounted,
                reason: None,
                ports_attached: 1,
            }],
            uptime_secs: 42,
            counters: StatusCounters {
                deliveries: 3,
                publishes: 1,
                errors: 0,
                instances: Default::default(),
            },
            // Nothing published on the overlay-state plane, so the surface
            // reports holding no overlay.
            overlay: None,
        })]
    );
}

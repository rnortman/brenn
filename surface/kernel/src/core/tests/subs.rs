use super::super::*;
use super::*;
use crate::test_support::cfg;
use brenn_surface_schema::{Binding, ServerFrame, SubscribeOutcome, SurfaceBindings};
use brenn_surface_test_fixtures::wire_cursor;

// ── Subscription table: registration direction ────────────────────────

#[test]
fn registration_while_active_subscribes_with_no_resume() {
    let mut core = active_core_with(vec![sub_binding()]);
    let effects = register(&mut core, "protobar", Millis(5));
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: None,
        })]
    );
}

/// Two ports of one instance on one channel are one subscription, refcounted —
/// the only case where a surface subscription is genuinely shared. One
/// registration opens it once.
#[test]
fn two_ports_one_channel_send_one_subscribe() {
    let mut core = active_core_with(vec![sub_binding(), ephemeral_binding("protobar", "alt")]);
    let effects = register(&mut core, "protobar", Millis(5));
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: None,
        })],
        "two bindings on one channel, one Subscribe"
    );
}

/// An instance with no bindings at all registers without touching the wire: it
/// simply never activates. There is no per-port answer to give — the questions
/// `BindingRemoved` answered stop existing with the dialect.
#[test]
fn registration_of_an_unbound_instance_touches_no_wire() {
    let mut core = active_core_with(vec![sub_binding()]);
    let effects = register(&mut core, "ghost", Millis(5));
    assert!(effects.is_empty(), "{effects:?}");
}

#[test]
#[should_panic(expected = "registered twice")]
fn duplicate_registration_panics() {
    let mut core = active_core_with(vec![sub_binding()]);
    register(&mut core, "protobar", Millis(5));
    register(&mut core, "protobar", Millis(6));
}

#[test]
fn subscribe_result_ok_activates_the_channel() {
    let mut core = active_core_with(vec![sub_binding(), ephemeral_binding("protobar", "alt")]);
    register(&mut core, "protobar", Millis(5));
    // SubscribeResult::Ok resets liveness (60_000 window from Millis(6)) and
    // makes the channel Active — no further frame.
    let effects = core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_006)))]);
}

/// A gap on a `SubscribeResult` stops at the resume layer. The kernel's answer
/// to a gap is the re-resume it already performed; there is no component-visible
/// gap vocabulary for it to reach, and the component observes at most a fresh
/// first window — which the contract defines as unremarkable.
#[test]
fn subscribe_result_gap_activates_the_channel_and_reaches_no_component() {
    let mut core = active_core_with(vec![sub_binding(), ephemeral_binding("protobar", "alt")]);
    register(&mut core, "protobar", Millis(5));
    let effects = core.on_input(
        Input::TextFrame(subscribe_result_gap(
            "ephemeral:demo",
            SubscribeOutcome::Ok,
            GapReason::EpochChanged,
        )),
        Millis(7),
    );
    // Liveness re-arm and nothing else: the channel activates, no frame is sent,
    // and no activation is minted for a gap.
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_007)))]);
    assert!(core.take_ready_activation(TEST_WALL_MS).is_none());
}

/// Every gap reason takes the same silent path — the kernel never reads which
/// one it was to decide what a component sees.
#[test]
fn every_gap_reason_reaches_no_component() {
    for reason in [GapReason::EpochChanged, GapReason::BeyondRetained] {
        let mut core = active_core_with(vec![sub_binding()]);
        register(&mut core, "protobar", Millis(5));
        let effects = core.on_input(
            Input::TextFrame(subscribe_result_gap(
                "ephemeral:demo",
                SubscribeOutcome::Ok,
                reason,
            )),
            Millis(6),
        );
        assert_eq!(
            effects,
            vec![Effect::SetWakeup(Some(Millis(60_006)))],
            "{reason:?}"
        );
        assert!(
            core.take_ready_activation(TEST_WALL_MS).is_none(),
            "{reason:?}"
        );
    }
}

#[test]
fn durable_subscribe_activates_and_delivers() {
    // A durable channel is supported: Ok activates it exactly like an
    // ephemeral one, and durable positions fan out to the bound port.
    let mut core = active_core_with(vec![durable_binding("durabar", "in")]);
    let sub = register(&mut core, "durabar", Millis(5));
    // A fresh registration presents no resume cursor, for every wire class: the
    // server replays the retained window.
    assert_eq!(
        sub,
        vec![Effect::SendFrame(ClientFrame::Subscribe {
            channel: "brenn:events".into(),
            instance: "durabar".into(),

            resume: None,
        })]
    );
    let ack = core.on_input(
        Input::TextFrame(subscribe_result_for(
            "brenn:events",
            "durabar",
            SubscribeOutcome::Ok,
        )),
        Millis(6),
    );
    assert_eq!(ack, vec![Effect::SetWakeup(Some(Millis(60_006)))]);
    let envelope = sample_envelope("durable-1");
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_for("brenn:events", "durabar", &envelope, 5)),
        Millis(7),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_007)))]);
    // A durable delivery reaches the component exactly as an ephemeral one does:
    // through its position on the store, into a window. That parity is the maxim.
    let ready = take_one(&mut core);
    assert_eq!(split(window(&ready.activation, "in")).1, vec!["durable-1"]);
}

#[test]
fn durable_reconnect_resumes_with_the_stored_cursor_and_a_gap_stops_there() {
    // A reconnect echoes the stored opaque cursor verbatim. A gap the server
    // reports on the resumed SubscribeResult is real loss information — and it
    // stops at the resume layer: the kernel's answer to it is the resubscribe it
    // just made, and the component sees a first-window-after-resubscribe, which
    // is unremarkable by contract.
    let mut core = active_core_with(vec![durable_binding("durabar", "in")]);
    register(&mut core, "durabar", Millis(5));
    // Ack the fresh subscribe, then deliver so a resume cursor is stored.
    core.on_input(
        Input::TextFrame(subscribe_result_for(
            "brenn:events",
            "durabar",
            SubscribeOutcome::Ok,
        )),
        Millis(6),
    );
    core.on_input(
        Input::TextFrame(deliver_frame_cursor(
            "brenn:events",
            "durabar",
            &sample_envelope("v"),
            5,
            wire_cursor("dur-5"),
            0,
        )),
        Millis(7),
    );
    // Drain and complete "v" before the blip, as the driver does on every
    // delivery: its pending obligation is consumed, so the only thing left to
    // observe after the reconnect is the gap.
    let ready = take_one(&mut core);
    assert_eq!(split(window(&ready.activation, "in")).1, vec!["v"]);
    complete(&mut core, "durabar", ActivationOutcome::Ok, ready.buffer);
    // Blip and reconnect: the survivor resumes echoing the stored cursor.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    );
    core.on_input(Input::Tick, Millis(3_008));
    core.on_input(Input::Opened, Millis(3_009));
    let welcome = core.on_input(
        Input::TextFrame(welcome_frame(
            vec![durable_binding("durabar", "in")],
            vec![],
        )),
        Millis(3_010),
    );
    assert!(
        welcome.contains(&Effect::SendFrame(ClientFrame::Subscribe {
            channel: "brenn:events".into(),
            instance: "durabar".into(),

            resume: Some(wire_cursor("dur-5")),
        })),
        "reconnect resumes echoing the stored cursor: {welcome:?}"
    );
    // The gap activates the channel and reaches no component.
    let resumed = core.on_input(
        Input::TextFrame(subscribe_result_gap_for(
            "brenn:events",
            "durabar",
            SubscribeOutcome::Ok,
            GapReason::BeyondRetained,
        )),
        Millis(3_011),
    );
    assert_eq!(resumed, vec![Effect::SetWakeup(Some(Millis(63_011)))]);
    assert!(core.take_ready_activation(TEST_WALL_MS).is_none());
    // And delivery continues normally on the resumed span.
    core.on_input(
        Input::TextFrame(deliver_frame_for(
            "brenn:events",
            "durabar",
            &sample_envelope("after-gap"),
            1,
        )),
        Millis(3_012),
    );
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "in")).1,
        vec!["after-gap"],
        "the component's first window after the gap is ordinary"
    );
}

#[test]
fn durable_reregister_after_deregister_in_page_presents_no_cursor() {
    // A fresh registration always presents `resume: None`, for every wire class:
    // after the channel has activated, an in-page deregistration (which discards
    // the cursor) followed by a fresh re-registration subscribes with
    // `resume: None`.
    let mut core = active_core_with(vec![durable_binding("durabar", "in")]);
    register(&mut core, "durabar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result_for(
            "brenn:events",
            "durabar",
            SubscribeOutcome::Ok,
        )),
        Millis(6),
    );
    // Deregister the last instance: Unsubscribe, cursor discarded, wire
    // Unsubscribed.
    deregister(&mut core, "durabar", Millis(7));
    // Fresh re-registration subscribes with no resume cursor.
    let reattach = register(&mut core, "durabar", Millis(8));
    assert_eq!(
        reattach,
        vec![Effect::SendFrame(ClientFrame::Subscribe {
            channel: "brenn:events".into(),
            instance: "durabar".into(),

            resume: None,
        })]
    );
}

#[test]
fn subscribe_result_for_non_pending_channel_is_fatal() {
    // ephemeral:demo is bound but nothing registered, so it is not Pending.
    let mut core = active_core_with(vec![sub_binding()]);
    let effects = core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(5),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("non-pending"), "{detail}");
}

#[test]
fn registration_off_active_defers_the_subscribe() {
    let mut core = active_core_with(vec![sub_binding()]);
    // Drop to Backoff; bindings persist but no bus-plane frame may be sent.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    );
    // No Subscribe off Active — reconcile subscribes at the next Welcome.
    assert!(register(&mut core, "protobar", Millis(6)).is_empty());
}

/// A registration before the first `Welcome` has no bindings to resolve against,
/// so it emits nothing and waits. The `Welcome`'s own reconcile is what opens its
/// subscriptions — the same code path every later reconcile takes — and it runs
/// before `Connected`, so a correct client is reconciled before the kernel reacts.
#[test]
fn pre_welcome_registration_resolves_to_subscribe_before_connected() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    assert!(register(&mut core, "protobar", Millis(0)).is_empty());
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(2),
    );
    assert_eq!(effects.len(), 3);
    assert_eq!(effects[0], Effect::SetWakeup(Some(Millis(60_002))));
    assert_eq!(
        effects[1],
        Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: None,
        })
    );
    assert!(matches!(
        effects[2],
        Effect::EmitEvent(Event::Connected { .. })
    ));
}

/// A pre-`Welcome` registration of an instance the `Welcome` declares no
/// bindings for opens nothing and is not an error: the instance simply never
/// activates.
#[test]
fn pre_welcome_registration_of_an_unbound_instance_opens_nothing() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    assert!(register(&mut core, "ghost", Millis(0)).is_empty());
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(2),
    );
    assert_eq!(effects.len(), 2);
    assert!(matches!(
        effects[1],
        Effect::EmitEvent(Event::Connected { .. })
    ));
}

#[test]
#[should_panic(expected = "registered twice")]
fn duplicate_pre_welcome_registration_panics() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    register(&mut core, "protobar", Millis(0));
    register(&mut core, "protobar", Millis(0));
}

// ── Subscription table: deregistration direction ──────────────────────

#[test]
fn deregistering_the_last_instance_from_active_sends_unsubscribe() {
    let mut core = active_subscribed_core();
    let effects = deregister(&mut core, "protobar", Millis(7));
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Unsubscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),
        })]
    );
}

/// Two sibling instances on one channel are two subscriptions, so deregistering
/// one leaves the other's untouched — the refcount is per (instance, channel).
#[test]
fn deregistering_one_sibling_keeps_the_others_subscription() {
    let mut core = active_sibling_core(); // alice + bob, both Active on demo
    // alice leaves: her own subscription closes, bob's is not disturbed.
    let effects = deregister(&mut core, "alice", Millis(8));
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Unsubscribe {
            channel: "ephemeral:demo".into(),
            instance: "alice".into(),
        })]
    );
    // bob still delivers.
    core.on_input(
        Input::TextFrame(deliver_frame_for(
            "ephemeral:demo",
            "bob",
            &sample_envelope("still-here"),
            2,
        )),
        Millis(9),
    );
    let ready = take_one(&mut core);
    assert_eq!(ready.instance, "bob");
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["still-here"]
    );
}

#[test]
fn deregistering_while_pending_defers_unsubscribe_until_result() {
    let mut core = active_core_with(vec![sub_binding()]);
    register(&mut core, "protobar", Millis(5)); // Subscribe, Pending
    // Deregister while still Pending: nothing goes on the wire yet.
    assert!(deregister(&mut core, "protobar", Millis(6)).is_empty());
    // The ack arrives at refcount 0 → the deferred Unsubscribe is sent
    // (after the liveness re-arm).
    let effects = core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(7),
    );
    assert_eq!(
        effects,
        vec![
            Effect::SetWakeup(Some(Millis(60_007))),
            Effect::SendFrame(ClientFrame::Unsubscribe {
                channel: "ephemeral:demo".into(),
                instance: "protobar".into(),
            }),
        ]
    );
}

#[test]
fn reregistering_while_pending_cancels_the_deferred_unsubscribe() {
    let mut core = active_core_with(vec![sub_binding()]);
    register(&mut core, "protobar", Millis(5)); // Subscribe, Pending
    assert!(deregister(&mut core, "protobar", Millis(6)).is_empty()); // deferred
    // Re-register before the ack: no new Subscribe, one is already in flight.
    assert!(register(&mut core, "protobar", Millis(7)).is_empty());
    // Ack now finds refcount 1 → Active, no deferred Unsubscribe.
    let effects = core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(8),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_008)))]);
}

#[test]
fn deregistering_off_active_sends_nothing() {
    let mut core = active_core_with(vec![sub_binding()]);
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(5),
    ); // → Backoff
    register(&mut core, "protobar", Millis(6)); // referenced, no Subscribe
    // The channel never subscribed (Unsubscribed): deregistering sends nothing.
    assert!(deregister(&mut core, "protobar", Millis(7)).is_empty());
}

#[test]
fn deregistering_a_previously_active_channel_while_disconnected_sends_nothing() {
    // A channel that reached wire `Active`, then lost the transport, must not
    // emit an `Unsubscribe` when its last reference is released while
    // disconnected: there is no live connection to carry a bus-plane frame, and
    // the server never saw this connection subscribe to it.
    let mut core = active_subscribed_core(); // ephemeral:demo wire Active
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    ); // → Backoff, bus plane reset
    assert!(
        deregister(&mut core, "protobar", Millis(9)).is_empty(),
        "no Unsubscribe off a disconnected transport"
    );
    // The wire state is clean for the next Welcome's reconcile: reconnecting and
    // re-registering subscribes fresh with no leftover Active state.
    core.on_input(Input::Tick, Millis(3_008)); // backoff deadline → reconnect
    core.on_input(Input::Opened, Millis(3_009));
    core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3_010),
    );
    let effects = register(&mut core, "protobar", Millis(3_011));
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: None,
        })]
    );
}

#[test]
fn reregistering_after_unsubscribe_subscribes_fresh() {
    let mut core = active_subscribed_core();
    deregister(&mut core, "protobar", Millis(7)); // Unsubscribe, wire Unsubscribed
    // A fresh registration re-opens the subscription.
    let effects = register(&mut core, "protobar", Millis(8));
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: None,
        })]
    );
}

#[test]
fn deregistering_a_pre_welcome_registration_opens_nothing_at_welcome() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    register(&mut core, "protobar", Millis(0)); // parked pre-Welcome
    assert!(deregister(&mut core, "protobar", Millis(1)).is_empty());
    core.on_input(Input::Opened, Millis(2));
    // The instance is gone before the bindings arrived, so the Welcome's
    // reconcile has nothing to open; only Connected is emitted.
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3),
    );
    assert_eq!(effects.len(), 2);
    assert!(matches!(effects[0], Effect::SetWakeup(_)));
    assert!(matches!(
        effects[1],
        Effect::EmitEvent(Event::Connected { .. })
    ));
}

#[test]
#[should_panic(expected = "deregistration of unregistered instance")]
fn deregistering_an_unregistered_instance_panics() {
    let mut core = active_core_with(vec![sub_binding()]);
    deregister(&mut core, "nobody", Millis(5));
}

#[test]
fn commands_after_fatal_are_absorbed() {
    let mut core = active_core();
    let effects = core.on_input(Input::BinaryFrame, Millis(10)); // → Fatal
    assert_fatal_shape(&effects);
    assert_post_terminal_register_absorbed(&mut core, Millis(11));
}

#[test]
fn welcome_with_unsupported_output_binding_scheme_is_fatal() {
    // The bad-scheme binding rides the `outputs` table, not `subscriptions`.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let bad = OutputBinding {
        channel: "weird:demo".into(),
        instance: "protobar".into(),
        port: "out".into(),
        urgency: Urgency::Normal,
        fill_mt: TEST_FILL_MT,
        capacity_mt: TEST_CAPACITY_MT,
    };
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![bad])),
        Millis(2),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("unsupported scheme"), "{detail}");
}

#[test]
fn bus_plane_frame_while_awaiting_is_fatal() {
    // A bus-plane frame (SubscribeResult) as the first server frame, before
    // any Welcome, is fatal — no subscription can exist yet.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let effects = core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(2),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("expected Welcome"), "{detail}");
}

#[test]
fn welcome_with_zero_heartbeat_is_fatal() {
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    core.on_input(Input::Opened, Millis(1));
    let welcome = serde_json::to_string(&ServerFrame::Welcome {
        surface: "deskbar".into(),
        participant_id: "surface:deskbar".into(),
        heartbeat_secs: 0,
        max_body_bytes: 65_536,
        alert_granted: false,
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
    let effects = core.on_input(Input::TextFrame(welcome), Millis(2));
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("heartbeat_secs is zero"), "{detail}");
}

/// Two instances bound to one channel are two subscriptions on the wire: the
/// core sends a `Subscribe` per instance, not one per channel. A channel-keyed
/// refcount table would send exactly one and the second instance would never
/// have a server-side window at all.
#[test]
fn sibling_instances_on_one_channel_each_subscribe() {
    let mut core = active_core_with(vec![
        durable_binding("agenda-alice", "in"),
        durable_binding("agenda-bob", "in"),
    ]);
    let a = register(&mut core, "agenda-alice", Millis(5));
    let b = register(&mut core, "agenda-bob", Millis(6));

    let subscribed = |effects: &[Effect], instance: &str| {
        effects.contains(&Effect::SendFrame(ClientFrame::Subscribe {
            channel: "brenn:events".into(),
            instance: instance.into(),

            resume: None,
        }))
    };
    assert!(subscribed(&a, "agenda-alice"), "alice subscribes: {a:?}");
    assert!(
        subscribed(&b, "agenda-bob"),
        "bob's registration opens his own subscription rather than riding alice's: {b:?}"
    );
}

/// A `Deliver` to one instance's subscription reaches that instance and **only**
/// it. Keyed by channel alone, bob would receive alice's copy too — and since the
/// server sends each instance its own copy, every message would be delivered
/// twice to everyone.
#[test]
fn a_deliver_reaches_only_its_own_instance() {
    let mut core = active_core_with(vec![
        durable_binding("agenda-alice", "in"),
        durable_binding("agenda-bob", "in"),
    ]);
    register(&mut core, "agenda-alice", Millis(5));
    register(&mut core, "agenda-bob", Millis(6));
    for instance in ["agenda-alice", "agenda-bob"] {
        core.on_input(
            Input::TextFrame(subscribe_result_for(
                "brenn:events",
                instance,
                SubscribeOutcome::Ok,
            )),
            Millis(7),
        );
    }

    core.on_input(
        Input::TextFrame(deliver_frame_for(
            "brenn:events",
            "agenda-alice",
            &sample_envelope("for-alice"),
            1,
        )),
        Millis(8),
    );
    let ready = take_one(&mut core);
    assert_eq!(
        ready.instance, "agenda-alice",
        "alice's Deliver activates alice alone"
    );
    assert_eq!(split(window(&ready.activation, "in")).1, vec!["for-alice"]);
}

/// The push depth is a per-binding operator knob, so each position reads its own
/// *binding's* depth — not one global number for the page. Two ports of one
/// instance with different declared depths is the case a global constant could
/// not express: the shallow one loses messages while the deep one does not.
#[test]
fn each_position_reads_its_own_bindings_push_depth() {
    let mut core = active_core_with(vec![
        Binding {
            push_depth: 1,
            ..sub_binding()
        },
        Binding {
            push_depth: 64,
            ..ephemeral_binding("protobar", "alt")
        },
    ]);
    register(&mut core, "protobar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    // Two deliveries on the shared channel: the depth-1 port keeps only the last
    // (drop-oldest + counted), the depth-64 port keeps both.
    for (i, body) in ["m1", "m2"].iter().enumerate() {
        core.on_input(
            Input::TextFrame(deliver_frame(
                "ephemeral:demo",
                &sample_envelope(body),
                i as u64 + 1,
            )),
            Millis(7 + i as u64),
        );
    }
    let ready = take_one(&mut core);
    let shallow = window(&ready.activation, "messages");
    assert_eq!(split(shallow).1, vec!["m2"], "depth 1 kept the newest");
    assert_eq!(shallow.dropped, 1, "and counted the one it dropped");
    let deep = window(&ready.activation, "alt");
    assert_eq!(split(deep).1, vec!["m1", "m2"], "depth 64 kept both");
    assert_eq!(deep.dropped, 0);
}

// The `Welcome` depth check's surviving arm — a `push_depth` no `usize` can
// hold — has no native test: this suite runs on a 64-bit target where every
// `u64` converts, so the arm is reachable only on wasm32. It is kept because
// that is the target it exists for.

/// `push_depth = 0` is a sampled/context-only binding on **every** ABI now: it
/// never activates its instance and never carries new envelopes, and its window
/// is pure retained context. The dialect's rejection of it — one event per
/// envelope had no way to say "see this channel but never wake me" — died with
/// the dialect.
#[test]
fn a_depth_zero_binding_is_context_only_and_never_activates() {
    let mut core = active_core_with(vec![
        Binding {
            push_depth: 0,
            retain_depth: 4,
            noise: brenn_surface_schema::NoiseLevel::Silent,
            ..sub_binding()
        },
        Binding {
            push_depth: 4,
            retain_depth: 4,
            noise: brenn_surface_schema::NoiseLevel::Silent,
            ..ephemeral_binding("protobar", "alt")
        },
    ]);
    register(&mut core, "protobar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("m1"), 1)),
        Millis(7),
    );
    // The instance activated because of `alt`, not `messages`.
    let ready = take_one(&mut core);
    let sampled = window(&ready.activation, "messages");
    assert_eq!(
        split(sampled),
        (vec!["m1"], vec![]),
        "the depth-0 port's window is pure context: new_from == len"
    );
    assert_eq!(
        sampled.new_from as usize,
        sampled.envelopes.len(),
        "pure context"
    );
    // Ring displacement is retention, not push overflow: no drop counter.
    assert_eq!(sampled.dropped, 0);
}

/// **A wire mirror's contents are the subscription's, and die with its last
/// reference.** Deregistering releases that reference, so re-registering is a
/// fresh consumer: it is primed from an empty store and owed nothing, and the
/// server's fresh-attach replay — the catch-up authority on a transportable
/// channel, whose cursors the page cannot even read — arrives entirely as new.
///
/// Keeping the old contents instead would let the store's dedup swallow that
/// replay as re-presentation, leaving the page indefinitely on history nothing
/// can ever tell it about again.
#[test]
fn releasing_a_subscriptions_last_reference_discards_its_mirror() {
    let mut core = active_core_with(vec![Binding {
        retain_depth: 4,
        ..sub_binding()
    }]);
    register(&mut core, "protobar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("m1"), 1)),
        Millis(7),
    );
    let ready = take_one(&mut core);
    complete(&mut core, "protobar", ActivationOutcome::Ok, ready.buffer);

    // Deregistering releases the instance's last reference on the subscription,
    // which discards the resume token — and the mirror with it.
    deregister(&mut core, "protobar", Millis(8));
    assert!(
        !core
            .stores
            .contains_key(&StoreKey::Wire(SubKey::for_instance(
                "protobar",
                "ephemeral:demo"
            ))),
        "the mirror outlived the subscription's last reference"
    );
    register(&mut core, "protobar", Millis(9));
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "a fresh consumer's empty mirror owes it nothing"
    );
    // Re-created at the depth the reconcile would have given it: the fold of
    // `max(push, retain)` over the subscription's bindings. A recreate that
    // guessed it would silently widen or gut the prime of the next binding to
    // attach here.
    assert_eq!(
        wire_depth(&core, "protobar", "ephemeral:demo"),
        TEST_PUSH_DEPTH.max(4)
    );
    // The fresh `Subscribe` carries no resume, so the server replays what it
    // retains. Nothing dedups it now: it is the catch-up, and it is new.
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(10),
    );
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("m1"), 1)),
        Millis(11),
    );
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["m1"],
        "the fresh-attach replay is delivered as new"
    );
}

/// Recovery from instance failure passes through the same refcount zero, so it
/// gets the same fresh-consumer treatment: a failed instance is recovered by
/// deregistering and re-registering it (a double registration panics), and
/// deregistration releases every reference it held. No path attaches a recovering
/// instance to the mirror its failed incarnation filled.
#[test]
fn recovery_after_failure_re_subscribes_from_an_empty_mirror() {
    let mut core = active_core_with(vec![Binding {
        retain_depth: 4,
        ..sub_binding()
    }]);
    register(&mut core, "protobar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("m1"), 1)),
        Millis(7),
    );
    let ready = take_one(&mut core);
    // The entry traps: the instance is terminal and holds no position, but its
    // references — and so its mirror — are still there.
    complete(
        &mut core,
        "protobar",
        ActivationOutcome::Trap("boom".into()),
        ready.buffer,
    );
    assert!(core.is_failed("protobar"));
    assert_eq!(
        wire_bodies(&core, "protobar", "ephemeral:demo"),
        vec!["m1"],
        "failure sheds positions, not retention"
    );

    deregister(&mut core, "protobar", Millis(8));
    register(&mut core, "protobar", Millis(9));
    assert!(
        wire_bodies(&core, "protobar", "ephemeral:demo").is_empty(),
        "recovery re-subscribes from an empty mirror"
    );
    assert_eq!(
        wire_depth(&core, "protobar", "ephemeral:demo"),
        TEST_PUSH_DEPTH.max(4),
        "and at the depth the reconcile would have built"
    );
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(10),
    );
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("m1"), 1)),
        Millis(11),
    );
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["m1"],
        "the recovered instance is caught up by the replay"
    );
}

/// A wire port rebound to a different channel takes the same drop-and-recreate
/// path a `local:` one does: its position on the old channel goes rather than
/// surfacing the old channel's history under the new binding, and a fresh
/// position on the new channel is filled by the fresh subscribe's replay.
#[test]
fn a_wire_port_rebound_to_another_channel_sheds_its_old_position() {
    let mut core = active_core_with(vec![sub_binding()]);
    register(&mut core, "protobar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(6),
    );
    // Queued and deliberately left unconsumed, so the rebind has something to
    // shed.
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("m1"), 1)),
        Millis(7),
    );

    // A second `Welcome` points the same port at another channel.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    );
    core.on_input(Input::Tick, Millis(3_008));
    core.on_input(Input::Opened, Millis(3_009));
    core.on_input(
        Input::TextFrame(welcome_frame(
            vec![Binding {
                channel: "ephemeral:other".into(),
                ..sub_binding()
            }],
            vec![],
        )),
        Millis(3_010),
    );
    assert!(
        core.take_ready_activation(TEST_WALL_MS).is_none(),
        "the old channel's envelope surfaced under the new binding"
    );
    assert!(
        !core.owed_anything("protobar"),
        "the rebound port starts owed nothing, awaiting the new subscribe's replay"
    );

    // And it is a live port, not a shed one: ack the new `Subscribe` and the new
    // channel's first message windows as new on it, with nothing of the old
    // channel's history anywhere in the window.
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:other", SubscribeOutcome::Ok)),
        Millis(3_011),
    );
    core.on_input(
        Input::TextFrame(deliver_frame_for(
            "ephemeral:other",
            "protobar",
            &sample_envelope("n1"),
            1,
        )),
        Millis(3_012),
    );
    let ready = take_one(&mut core);
    let view = window(&ready.activation, "messages");
    assert_eq!(split(view), (vec![], vec!["n1"]));
    assert!(
        !view.envelopes.iter().any(|e| e.body == "m1"),
        "the old channel's envelope is nowhere in the rebound port's window"
    );
}

/// Every channel address the kernel routes came from a boot-validated binding or
/// the reserved table, so one the surface cannot bind is a kernel bug rather
/// than tolerated input. The single capability-derivation point says so out loud
/// instead of guessing a retention model — routing an unbindable address as
/// either class would silently put a channel in the wrong store with the wrong
/// lifetime, and `mqtt:` is the dangerous row: durable and transportable on the
/// bus, so a capabilities-only reading would put it on the wire.
#[test]
#[should_panic(expected = "unbindable channel address")]
fn an_unbindable_address_panics_at_the_capability_derivation() {
    channel_is_transportable("mqtt:topic");
}

/// An address carrying no recognized prefix is refused by the same gate: it
/// binds to no surface either, so the kernel never reaches a capability
/// question it cannot answer.
#[test]
#[should_panic(expected = "unbindable channel address")]
fn an_unprefixed_address_panics_at_the_capability_derivation() {
    channel_is_transportable("bare-name");
}

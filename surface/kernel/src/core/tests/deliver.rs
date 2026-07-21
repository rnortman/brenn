use super::super::*;
use super::*;
use brenn_surface_proto::SubscribeOutcome;
use brenn_surface_test_fixtures::{sample_envelope, wire_cursor};

// ── Deliver fan-out ───────────────────────────────────────────────────

/// A `Deliver` emits **no** per-message effect: it re-arms liveness, fills the
/// subscription's ring and its bindings' pending queues, and marks the instance
/// pending. The batching is the delivery model — the message reaches the
/// component when the driver next drains ready activations.
#[test]
fn deliver_on_active_channel_queues_the_message_and_resets_liveness() {
    let mut core = active_subscribed_core(); // protobar registered on ephemeral:demo, Active
    let envelope = sample_envelope("hello");
    let effects = core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &envelope, 3)),
        Millis(10),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_010)))]);
    // It landed: the instance activates, and the message is new in its window.
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["hello"]
    );
}

/// The server's `dropped` count reaches the component as the window's `dropped`
/// delta — a counter, not a marker in the stream. The message that follows the
/// loss is delivered normally; nothing marks the gap in the envelope order.
#[test]
fn deliver_with_dropped_carries_the_count_on_the_window() {
    let mut core = active_subscribed_core();
    let envelope = sample_envelope("after-loss");
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_dropped("ephemeral:demo", &envelope, 9, 4)),
        Millis(10),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_010)))]);
    let ready = take_one(&mut core);
    let w = window(&ready.activation, "messages");
    assert_eq!(w.dropped, 4);
    assert_eq!(split(w).1, vec!["after-loss"], "the loss marks no envelope");
}

#[test]
fn deliver_straggler_with_dropped_emits_diagnostic_but_no_marker() {
    let mut core = active_subscribed_core();
    // Deregister the last instance: Unsubscribe, wire Unsubscribed; channel is
    // still has-been-Active for this connection.
    deregister(&mut core, "protobar", Millis(7));
    // A straggler carrying dropped>0 is discarded entirely: liveness re-arm plus
    // the diagnostic event (which carries the discarded dropped-count), and
    // nothing queued.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_dropped(
            "ephemeral:demo",
            &sample_envelope("straggler"),
            9,
            7,
        )),
        Millis(8),
    );
    assert_eq!(
        effects,
        vec![
            Effect::SetWakeup(Some(Millis(60_008))),
            Effect::EmitEvent(Event::StragglerDiscarded {
                channel: "ephemeral:demo".into(),
                seq: 9,
                dropped: 7,
            }),
        ]
    );
}

/// Two ports of one instance on one channel share a subscription and both see
/// the delivery — in the same activation, since an instance activates once with
/// every bound port windowed.
#[test]
fn deliver_reaches_every_bound_port_of_the_instance() {
    let mut core = active_core_with(vec![sub_binding(), ephemeral_binding("protobar", "alt")]);
    register(&mut core, "protobar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(7),
    );
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("broadcast"),
            1,
        )),
        Millis(10),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_010)))]);
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["broadcast"]
    );
    assert_eq!(split(window(&ready.activation, "alt")).1, vec!["broadcast"]);
}

// ── Multi-target demux (wire fan-out consolidation) ───────────────────

/// The kernel is the fan-out site: one envelope carried once on the wire lands
/// in every named subscription's queues, each at its own per-subscription state.
#[test]
fn multi_target_deliver_feeds_every_named_subscription() {
    let mut core = active_sibling_core();
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_multi(
            "ephemeral:demo",
            &sample_envelope("hello-both"),
            vec![target("alice", 1, 0), target("bob", 1, 0)],
        )),
        Millis(10),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_010)))]);
    // Both siblings activate, each seeing the envelope as new in its own window:
    // targets share a frame, never a subscription.
    let mut seen: Vec<(String, Vec<String>)> = Vec::new();
    while let Some(ready) = core.take_ready_activation() {
        let new: Vec<String> = split(window(&ready.activation, "messages"))
            .1
            .iter()
            .map(|s| s.to_string())
            .collect();
        seen.push((ready.instance, new));
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("alice".to_string(), vec!["hello-both".to_string()]),
            ("bob".to_string(), vec!["hello-both".to_string()]),
        ]
    );
}

/// Per-target `dropped` is per-subscription: a lagging sibling's loss count
/// never leaks onto a sibling that lost nothing.
#[test]
fn multi_target_deliver_keeps_dropped_per_target() {
    let mut core = active_sibling_core();
    core.on_input(
        Input::TextFrame(deliver_frame_multi(
            "ephemeral:demo",
            &sample_envelope("after-loss"),
            vec![target("alice", 1, 0), target("bob", 1, 7)],
        )),
        Millis(10),
    );
    let mut dropped: Vec<(String, u64)> = Vec::new();
    while let Some(ready) = core.take_ready_activation() {
        dropped.push((
            ready.instance,
            window(&ready.activation, "messages").dropped,
        ));
    }
    dropped.sort();
    assert_eq!(
        dropped,
        vec![("alice".to_string(), 0), ("bob".to_string(), 7)],
        "only bob's subscription lost messages, so only bob's window counts them"
    );
}

/// Each target's span seq is checked against its **own** subscription's
/// high-water. Sibling seqs are unrelated counters — alice at 1 and bob at 40
/// in one frame is ordinary, not a regression.
#[test]
fn multi_target_seqs_are_tracked_per_subscription() {
    let mut core = active_sibling_core();
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_multi(
            "ephemeral:demo",
            &sample_envelope("x"),
            vec![target("alice", 1, 0), target("bob", 40, 0)],
        )),
        Millis(10),
    );
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::EmitEvent(Event::Fatal { .. }))),
        "sibling seq counters are independent: {effects:?}"
    );
    // And each continues from its own high-water: bob at 2 would regress bob's
    // span (40) even though it advances alice's (1).
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_multi(
            "ephemeral:demo",
            &sample_envelope("y"),
            vec![target("bob", 2, 0)],
        )),
        Millis(11),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("seq regression"), "{detail}");
}

/// A frame naming one subscription twice would ask that subscription's span seq
/// to both advance and regress within one frame. A correct server cannot mint
/// it, so it is fatal rather than deduped.
#[test]
fn multi_target_deliver_with_duplicate_target_is_fatal() {
    let mut core = active_sibling_core();
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_multi(
            "ephemeral:demo",
            &sample_envelope("x"),
            vec![target("alice", 1, 0), target("alice", 2, 0)],
        )),
        Millis(10),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("twice"), "{detail}");
}

/// A `Deliver` addressed to nobody is a delivery that means nothing — a correct
/// server never writes one.
#[test]
fn deliver_with_no_targets_is_fatal() {
    let mut core = active_sibling_core();
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_multi(
            "ephemeral:demo",
            &sample_envelope("x"),
            vec![],
        )),
        Millis(10),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("no targets"), "{detail}");
}

/// The duplicate-target check must not fire across *frames*: the same
/// subscription named by two successive frames is ordinary delivery.
#[test]
fn duplicate_target_check_is_per_frame_not_per_connection() {
    let mut core = active_sibling_core();
    for seq in 1..=2 {
        let effects = core.on_input(
            Input::TextFrame(deliver_frame_multi(
                "ephemeral:demo",
                &sample_envelope("x"),
                vec![target("alice", seq, 0)],
            )),
            Millis(10 + seq),
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::EmitEvent(Event::Fatal { .. }))),
            "frame {seq}: {effects:?}"
        );
    }
}

/// A target naming a subscription the kernel does not hold is fatal exactly as
/// it is on a single-target frame — the multi-target loop adds no tolerance.
#[test]
fn multi_target_deliver_for_never_active_subscription_is_fatal() {
    let mut core = active_sibling_core();
    let effects = core.on_input(
        Input::TextFrame(deliver_frame_multi(
            "ephemeral:demo",
            &sample_envelope("x"),
            vec![target("alice", 1, 0), target("mallory", 1, 0)],
        )),
        Millis(10),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("never active"), "{detail}");
}

#[test]
fn deliver_for_never_active_channel_is_fatal() {
    // ephemeral:demo is bound but never subscribed/acked, so it has never
    // been Active on this connection: a Deliver for it is inexplicable.
    let mut core = active_core_with(vec![sub_binding()]);
    let effects = core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("x"), 1)),
        Millis(10),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("never active"), "{detail}");
}

#[test]
fn deliver_straggler_after_unsubscribe_is_discarded() {
    let mut core = active_subscribed_core();
    // Deregister the last instance: Unsubscribe, wire Unsubscribed. The channel
    // is still in the has-been-Active set for this connection.
    assert_eq!(
        deregister(&mut core, "protobar", Millis(7)),
        vec![Effect::SendFrame(ClientFrame::Unsubscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),
        })]
    );
    // A straggler Deliver arriving after the Unsubscribe is discarded: the
    // liveness re-arm plus the diagnostic event, nothing queued, not fatal.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("straggler"),
            9,
        )),
        Millis(8),
    );
    assert_eq!(
        effects,
        vec![
            Effect::SetWakeup(Some(Millis(60_008))),
            Effect::EmitEvent(Event::StragglerDiscarded {
                channel: "ephemeral:demo".into(),
                seq: 9,
                dropped: 0,
            }),
        ]
    );
}

#[test]
fn second_straggler_in_same_span_emits_no_diagnostic() {
    let mut core = active_subscribed_core();
    deregister(&mut core, "protobar", Millis(7)); // Unsubscribe, wire Unsubscribed
    // First straggler: discard + diagnostic.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("first"),
            9,
        )),
        Millis(8),
    );
    assert_eq!(
        effects,
        vec![
            Effect::SetWakeup(Some(Millis(60_008))),
            Effect::EmitEvent(Event::StragglerDiscarded {
                channel: "ephemeral:demo".into(),
                seq: 9,
                dropped: 0,
            }),
        ]
    );
    // Second straggler in the same span: discard only, diagnostic suppressed.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("second"),
            10,
        )),
        Millis(9),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_009)))]);
}

#[test]
fn straggler_diagnostic_rearms_after_reactivation() {
    let mut core = active_subscribed_core();
    deregister(&mut core, "protobar", Millis(7)); // Unsubscribe, wire Unsubscribed
    // First span's straggler: diagnostic emitted.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("span-1"),
            9,
        )),
        Millis(8),
    );
    assert_eq!(effects.len(), 2);
    // Re-register and re-activate: the channel reaches Active again, opening a
    // new span and re-arming the diagnostic.
    register(&mut core, "protobar", Millis(9));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(10),
    );
    deregister(&mut core, "protobar", Millis(11)); // Unsubscribe again, new span ends
    // A straggler in the new span reports again — the flag re-armed.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("span-2"),
            12,
        )),
        Millis(12),
    );
    assert_eq!(
        effects,
        vec![
            Effect::SetWakeup(Some(Millis(60_012))),
            Effect::EmitEvent(Event::StragglerDiscarded {
                channel: "ephemeral:demo".into(),
                seq: 12,
                dropped: 0,
            }),
        ]
    );
}

#[test]
fn deliver_straggler_while_pending_on_resubscribe_is_discarded() {
    let mut core = active_subscribed_core();
    deregister(&mut core, "protobar", Millis(7)); // Unsubscribe, wire Unsubscribed
    // Re-register: a fresh Subscribe, wire Pending. Still has-been-Active.
    assert_eq!(
        register(&mut core, "protobar", Millis(8)),
        vec![Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: None,
        })]
    );
    // A previous-span straggler arriving while Pending is discarded (server
    // FIFO guarantees the new span's replay follows its SubscribeResult):
    // liveness re-arm plus the diagnostic event.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("old-span"),
            9,
        )),
        Millis(9),
    );
    assert_eq!(
        effects,
        vec![
            Effect::SetWakeup(Some(Millis(60_009))),
            Effect::EmitEvent(Event::StragglerDiscarded {
                channel: "ephemeral:demo".into(),
                seq: 9,
                dropped: 0,
            }),
        ]
    );
    // The new span's ack activates; subsequent Delivers reach the component.
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(10),
    );
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("new-span"),
            1,
        )),
        Millis(11),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_011)))]);
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["new-span"]
    );
}

#[test]
fn deliver_after_reconnect_before_resubscribe_is_fatal() {
    // The has-been-Active set is per-connection: a channel Active on the
    // previous connection is not Active on a fresh one until re-subscribed,
    // so a Deliver landing before the new SubscribeResult is inexplicable.
    let mut core = active_subscribed_core(); // ephemeral:demo was Active
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    ); // → Backoff, set cleared
    core.on_input(Input::Tick, Millis(3_008)); // reconnect
    core.on_input(Input::Opened, Millis(3_009));
    core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3_010),
    );
    // The channel has not (yet) been Active on this fresh connection, so a
    // Deliver for it is fatal regardless of its wire state — the decision is
    // gated purely on the per-connection has-been-Active set, now empty.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("premature"),
            1,
        )),
        Millis(3_011),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("never active"), "{detail}");
}

// ── Reconnect-reconcile ───────────────────────────────────────────────

#[test]
fn reconnect_resubscribes_surviving_attached_port() {
    // A port attached across a transport blip is resubscribed from the next
    // Welcome — its wire state was reset to Unsubscribed at close, and its
    // refcount survives.
    let mut core = active_subscribed_core(); // port 1 on ephemeral:demo, Active
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    ); // → Backoff, wire reset
    core.on_input(Input::Tick, Millis(3_008)); // reconnect
    core.on_input(Input::Opened, Millis(3_009));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3_010),
    );
    // Reconcile keeps the still-bound instance, resubscribes it fresh (no resume
    // token), all before Connected.
    assert_eq!(effects.len(), 3, "{effects:?}");
    assert!(matches!(effects[0], Effect::SetWakeup(_)));
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

#[test]
fn reconnect_resubscribes_shared_channel_once() {
    // Two ports of one instance on one channel resubscribe as a single wire
    // subscription.
    let mut core = active_core_with(vec![sub_binding(), ephemeral_binding("protobar", "alt")]);
    register(&mut core, "protobar", Millis(5));
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(7),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    );
    core.on_input(Input::Tick, Millis(3_008));
    core.on_input(Input::Opened, Millis(3_009));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(
            vec![sub_binding(), ephemeral_binding("protobar", "alt")],
            vec![],
        )),
        Millis(3_010),
    );
    let subs: Vec<&Effect> = effects
        .iter()
        .filter(|e| matches!(e, Effect::SendFrame(ClientFrame::Subscribe { .. })))
        .collect();
    assert_eq!(
        subs.len(),
        1,
        "one Subscribe for the shared channel: {effects:?}"
    );
    assert_eq!(
        *subs[0],
        Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: None,
        })
    );
}

#[test]
fn reconnect_reconcile_drops_removed_binding_before_any_subscribe() {
    // The kiosk scenario (config edit + restart under auto-reconnect): a binding
    // vanishes from the new Welcome. Its queue is dropped, no Subscribe is ever
    // emitted for its channel, the survivor is resubscribed, and Connected is
    // emitted last. The instance is not failed and not deregistered — it simply
    // stops being activated on that channel.
    let mut core = active_core_with(vec![sub_binding(), other_binding()]);
    register(&mut core, "protobar", Millis(5)); // Subscribes both channels
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(7),
    );
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:other", SubscribeOutcome::Ok)),
        Millis(8),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(9),
    );
    core.on_input(Input::Tick, Millis(3_009));
    core.on_input(Input::Opened, Millis(3_010));
    // Reconnect with `other` removed from the bindings.
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3_011),
    );
    // No Subscribe for the removed channel, anywhere in the list.
    assert!(
        !effects.iter().any(|e| matches!(
            e,
            Effect::SendFrame(ClientFrame::Subscribe { channel, .. }) if channel == "ephemeral:other"
        )),
        "no Subscribe for the dropped channel: {effects:?}"
    );
    // The survivor is resubscribed fresh.
    assert!(effects.contains(&Effect::SendFrame(ClientFrame::Subscribe {
        channel: "ephemeral:demo".into(),
        instance: "protobar".into(),

        resume: None,
    })));
    // Connected is emitted only after reconcile.
    assert!(matches!(
        effects.last().unwrap(),
        Effect::EmitEvent(Event::Connected { .. })
    ));
    // The instance is still registered and still delivered on its survivor.
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(3_012),
    );
    core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("alive"),
            1,
        )),
        Millis(3_013),
    );
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["alive"]
    );
}

// ── Resume tokens (opaque cursors) ────────────────────────────────────

#[test]
fn reconnect_resumes_with_latest_cursor() {
    // Delivers accepted while Active store the channel's opaque resume cursor;
    // a later Deliver overwrites it with its own. A continuously-registered
    // instance resuming across a transport blip re-Subscribes echoing the latest
    // cursor verbatim — the kernel never interprets it.
    let mut core = active_subscribed_core(); // protobar on ephemeral:demo, Active
    core.on_input(
        Input::TextFrame(deliver_frame_cursor(
            "ephemeral:demo",
            TEST_INSTANCE,
            &sample_envelope("a"),
            3,
            wire_cursor("cur-a"),
            0,
        )),
        Millis(7),
    );
    core.on_input(
        Input::TextFrame(deliver_frame_cursor(
            "ephemeral:demo",
            TEST_INSTANCE,
            &sample_envelope("b"),
            7,
            wire_cursor("cur-b"),
            0,
        )),
        Millis(8),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(9),
    ); // blip; the port stays attached
    core.on_input(Input::Tick, Millis(3_009));
    core.on_input(Input::Opened, Millis(3_010));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3_011),
    );
    assert!(
        effects.contains(&Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: Some(wire_cursor("cur-b")),
        })),
        "resume echoes the latest accepted cursor: {effects:?}"
    );
}

#[test]
fn detach_discards_token_and_straggler_does_not_revive_it() {
    // The token lives only while a port is attached. The last detach discards
    // it, and a post-Unsubscribe straggler does not revive it: a fresh
    // re-attach of the same pair subscribes with `resume: None`.
    let mut core = active_subscribed_core(); // port 1 on ephemeral:demo, Active
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("v"), 5)),
        Millis(7),
    );
    assert_eq!(
        deregister(&mut core, "protobar", Millis(8)),
        vec![Effect::SendFrame(ClientFrame::Unsubscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),
        })]
    );
    // Straggler for the just-unsubscribed channel: discarded, token untouched.
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("late"), 9)),
        Millis(9),
    );
    let effects = register(&mut core, "protobar", Millis(10));
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
fn partial_reconcile_retains_token_for_surviving_channel() {
    // Two of the instance's ports share ephemeral:demo. Reconnect drops only one
    // of the two bindings; the channel keeps a positive refcount, so its cursor
    // survives and the channel resubscribes echoing it. (The other refcount-0
    // discard paths — full reconcile drop and refcount-zero-while-Pending —
    // share the same one-line clearing exercised by the deregister test.)
    let mut core = active_core_with(vec![sub_binding(), ephemeral_binding("protobar", "alt")]);
    register(&mut core, "protobar", Millis(5)); // one Subscribe for both bindings
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(7),
    );
    core.on_input(
        Input::TextFrame(deliver_frame_cursor(
            "ephemeral:demo",
            TEST_INSTANCE,
            &sample_envelope("v"),
            4,
            wire_cursor("cur-v"),
            0,
        )),
        Millis(8),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(9),
    );
    core.on_input(Input::Tick, Millis(3_009));
    core.on_input(Input::Opened, Millis(3_010));
    // `alt` binding removed; `messages` survives on ephemeral:demo.
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3_011),
    );
    assert!(
        effects.contains(&Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: Some(wire_cursor("cur-v")),
        })),
        "the surviving channel resumes with its retained cursor: {effects:?}"
    );
}

/// When reconcile drops a channel's last reference — the instance's binding on
/// it vanished from the new `Welcome` — the refcount hits zero and the resume
/// cursor must be discarded. Proven end to end: bring the binding back (no
/// `Deliver` repopulates the cursor), reconnect, and the resubscribe must
/// present `resume: None` rather than the stale cursor from before the drop.
///
/// The reference is per (instance, channel), not per (instance, port): moving a
/// channel between two of one instance's ports is not a drop, so the binding has
/// to leave the `Welcome` entirely to reach refcount 0.
#[test]
fn reconcile_full_drop_discards_channel_token() {
    let mut core = active_subscribed_core(); // (protobar, messages) → ephemeral:demo
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("v"), 5)),
        Millis(7),
    ); // demo cursor stored
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    );
    core.on_input(Input::Tick, Millis(3_008));
    core.on_input(Input::Opened, Millis(3_009));
    // demo leaves the bindings entirely → the instance's reference is released,
    // refcount 0, cursor discarded.
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![other_binding()], vec![])),
        Millis(3_010),
    );
    assert!(
        !effects.iter().any(|e| matches!(
            e,
            Effect::SendFrame(ClientFrame::Subscribe { channel, .. })
                if channel == "ephemeral:demo"
        )),
        "the refcount-0 dropped channel is not resubscribed: {effects:?}"
    );
    // Bring it back and reconnect. If reconcile had discarded the cursor, the
    // resubscribe presents `resume: None`; a surviving stale cursor would leak
    // here.
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(3_012),
    );
    core.on_input(Input::Tick, Millis(6_012));
    core.on_input(Input::Opened, Millis(6_013));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(6_014),
    );
    assert!(
        effects.contains(&Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: None,
        })),
        "reconcile discarded the cursor, so the resubscribe is fresh: {effects:?}"
    );
}

// ── Deliver seq invariant ─────────────────────────────────────────────

#[test]
fn deliver_seq_regression_within_span_is_fatal() {
    let mut core = active_subscribed_core(); // ephemeral:demo, Active
    // First Deliver at seq 5 is accepted (fresh span, no seed).
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("a"), 5)),
        Millis(7),
    );
    // A lower seq within the same span is a server bug → fatal.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("b"), 4)),
        Millis(8),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("seq regression"), "{detail}");
}

#[test]
fn deliver_equal_seq_within_span_is_fatal() {
    // Seq must strictly increase: a repeat of the last seq is fatal.
    let mut core = active_subscribed_core();
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("a"), 5)),
        Millis(7),
    );
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("a-again"),
            5,
        )),
        Millis(8),
    );
    let detail = assert_fatal_shape(&effects);
    assert!(detail.contains("seq regression"), "{detail}");
}

#[test]
fn fresh_span_after_reattach_accepts_lower_seq() {
    // In-connection unsubscribe + fresh re-registration (`resume: None`) re-seeds
    // the tracker empty, so the retained ring may legally re-deliver seqs at or
    // below the previous span's high-water.
    let mut core = active_subscribed_core();
    core.on_input(
        Input::TextFrame(deliver_frame("ephemeral:demo", &sample_envelope("high"), 9)),
        Millis(7),
    );
    deregister(&mut core, "protobar", Millis(8)); // Unsubscribe, token discarded
    register(&mut core, "protobar", Millis(9)); // fresh Subscribe, seed None
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(10),
    );
    // A ring-replay Deliver at seq 2 — below the old span's 9 — is legal now, and
    // reaches the component normally.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("replayed"),
            2,
        )),
        Millis(11),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(60_011)))]);
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["replayed"]
    );
}

#[test]
fn resumed_span_accepts_any_first_seq() {
    // On a resumed re-Subscribe the span tracker resets to empty (the server
    // restarts the delivery-time span seq at 1), so the first Deliver of the
    // new span is accepted whatever its seq — the class-blind model no longer
    // seeds the tracker from the presented cursor.
    let mut core = resumed_core_seeded_at(7);
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("fresh"),
            1,
        )),
        Millis(3_012),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(63_012)))]);
    // "v" was delivered pre-blip and queued but never drained into an
    // activation; the reconnect keeps the instance registered, so its pending
    // delivery obligation survives the blip and drains as new alongside the
    // post-resume "fresh". The ring dedup keeps both out of the context half.
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["v", "fresh"]
    );
}

#[test]
fn reconnect_accepts_fresh_span_replay_below_prior_high_water() {
    // The routine server-restart heal path: the client presents its stored
    // opaque cursor on reconnect (the server needs it to decide the replay),
    // but the span tracker resets to empty, so the new span's replay Delivers
    // restarting near seq 1 — below the pre-restart high-water — must be
    // delivered, never go terminal.
    let mut core = active_subscribed_core(); // protobar on ephemeral:demo, Active
    // Accept a Deliver at seq 7 → stored cursor.
    core.on_input(
        Input::TextFrame(deliver_frame_cursor(
            "ephemeral:demo",
            TEST_INSTANCE,
            &sample_envelope("pre-restart"),
            7,
            wire_cursor("cur-pre"),
            0,
        )),
        Millis(7),
    );
    core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(8),
    ); // server restart; the instance stays registered
    core.on_input(Input::Tick, Millis(3_008));
    core.on_input(Input::Opened, Millis(3_009));
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(vec![sub_binding()], vec![])),
        Millis(3_010),
    );
    // The resume echoes the stored cursor verbatim — the server needs it.
    assert!(
        effects.contains(&Effect::SendFrame(ClientFrame::Subscribe {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),

            resume: Some(wire_cursor("cur-pre")),
        })),
        "resume presents the pre-restart cursor: {effects:?}"
    );
    core.on_input(
        Input::TextFrame(subscribe_result("ephemeral:demo", SubscribeOutcome::Ok)),
        Millis(3_011),
    );
    // First replay Deliver of the new span at seq 1 (< pre-restart 7) must be
    // delivered, not fatal — the span tracker was reset on the resumed Subscribe.
    let effects = core.on_input(
        Input::TextFrame(deliver_frame(
            "ephemeral:demo",
            &sample_envelope("post-restart"),
            1,
        )),
        Millis(3_012),
    );
    assert_eq!(effects, vec![Effect::SetWakeup(Some(Millis(63_012)))]);
    // "pre-restart" was delivered and queued before the blip but never drained;
    // the instance stays registered across the reconnect, so its pending
    // obligation survives and drains as new with the post-resume "post-restart".
    // The server resumes past the stored cursor, so it never re-sends
    // "pre-restart" — one delivery, in the new half, deduped out of context.
    let ready = take_one(&mut core);
    assert_eq!(
        split(window(&ready.activation, "messages")).1,
        vec!["pre-restart", "post-restart"]
    );
}

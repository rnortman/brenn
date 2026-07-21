use super::super::*;
use super::*;
use crate::test_support::cfg;
use brenn_surface_proto::PublishOutcome;

// ── Publish path ──────────────────────────────────────────────────────

#[test]
fn publish_while_active_sends_frame_then_result_routes_back() {
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    let effects = publish(&mut core, 7, "protobar", "out", "payload", Millis(5));
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Publish {
            instance: "protobar".into(),
            port: "out".into(),
            body: "payload".into(),
            correlation: Some(7),
            subject_instance: None,
            urgency: None,
        })]
    );
    // The server's PublishResult routes back to (protobar, out) by
    // correlation, resetting liveness as any inbound text frame does.
    let effects = core.on_input(
        Input::TextFrame(publish_result_frame(Some(7), PublishOutcome::Ok)),
        Millis(6),
    );
    assert_eq!(
        effects,
        vec![
            Effect::SetWakeup(Some(Millis(60_006))),
            Effect::EmitEvent(Event::PublishResult {
                instance: "protobar".into(),
                port: "out".into(),
                correlation: 7,
                status: PublishStatus::Ok,
            }),
        ]
    );
}

#[test]
fn publish_result_wire_outcomes_map_to_status() {
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    publish(&mut core, 1, "protobar", "out", "x", Millis(5));
    let effects = core.on_input(
        Input::TextFrame(publish_result_frame(
            Some(1),
            PublishOutcome::BodyTooLarge {
                len: 1000,
                max: 500,
            },
        )),
        Millis(6),
    );
    assert!(effects.contains(&Effect::EmitEvent(Event::PublishResult {
        instance: "protobar".into(),
        port: "out".into(),
        correlation: 1,
        status: PublishStatus::BodyTooLarge {
            len: 1000,
            max: 500,
        },
    })));
}

#[test]
fn publish_to_unbound_port_rejected_locally_without_a_frame() {
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    let effects = publish(&mut core, 3, "protobar", "ghost", "payload", Millis(5));
    assert_eq!(
        effects,
        vec![Effect::EmitEvent(Event::PublishResult {
            instance: "protobar".into(),
            port: "ghost".into(),
            correlation: 3,
            status: PublishStatus::UnboundPort,
        })]
    );
    // No correlation was tracked: a later PublishResult for it is fatal.
    let effects = core.on_input(
        Input::TextFrame(publish_result_frame(Some(3), PublishOutcome::Ok)),
        Millis(6),
    );
    assert_fatal_shape(&effects);
}

#[test]
fn publish_oversized_body_rejected_locally() {
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    // welcome_frame advertises max_body_bytes = 65_536; one over the cap.
    let body = "a".repeat(65_537);
    let effects = publish(&mut core, 4, "protobar", "out", &body, Millis(5));
    assert_eq!(
        effects,
        vec![Effect::EmitEvent(Event::PublishResult {
            instance: "protobar".into(),
            port: "out".into(),
            correlation: 4,
            status: PublishStatus::BodyTooLarge {
                len: 65_537,
                max: 65_536,
            },
        })]
    );
}

#[test]
fn publish_while_not_connected_rejected_locally() {
    // A fresh core is Connecting, never Active.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    let effects = publish(&mut core, 5, "protobar", "out", "payload", Millis(1));
    assert_eq!(
        effects,
        vec![Effect::EmitEvent(Event::PublishResult {
            instance: "protobar".into(),
            port: "out".into(),
            correlation: 5,
            status: PublishStatus::NotConnected,
        })]
    );
}

#[test]
fn publish_result_routes_by_correlation_among_several() {
    let mut core = active_core_with_outputs(vec![
        output_binding("protobar", "out"),
        output_binding("protobar", "alt"),
    ]);
    publish(&mut core, 10, "protobar", "out", "a", Millis(5));
    publish(&mut core, 11, "protobar", "alt", "b", Millis(6));
    // The result for correlation 11 routes to (protobar, alt), not (out).
    let effects = core.on_input(
        Input::TextFrame(publish_result_frame(Some(11), PublishOutcome::RateLimited)),
        Millis(7),
    );
    assert!(effects.contains(&Effect::EmitEvent(Event::PublishResult {
        instance: "protobar".into(),
        port: "alt".into(),
        correlation: 11,
        status: PublishStatus::RateLimited,
    })));
}

#[test]
fn pending_publishes_fail_connection_lost_on_disconnect() {
    let mut core = active_core_with_outputs(vec![
        output_binding("protobar", "out"),
        output_binding("protobar", "alt"),
    ]);
    // Publish the higher correlation first so ascending ConnectionLost order
    // can only hold if `fail_pending_publishes` actually sorts — not merely
    // because insertion order happened to agree with it.
    publish(&mut core, 21, "protobar", "alt", "b", Millis(5));
    publish(&mut core, 20, "protobar", "out", "a", Millis(6));
    // Disconnect fails every outstanding publish with ConnectionLost, ordered
    // by correlation, before the backoff SetWakeup.
    let effects = core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(7),
    );
    // The publish results are pinned exactly; the trailing backoff wakeup is
    // jittered, so it is pulled out of the Vec equality and range-checked.
    let (results, wakeup_tail) = effects.split_at(3);
    assert_eq!(
        results,
        [
            Effect::EmitEvent(Event::Disconnected {
                reason: DisconnectReason::TransportClosed,
            }),
            Effect::EmitEvent(Event::PublishResult {
                instance: "protobar".into(),
                port: "out".into(),
                correlation: 20,
                status: PublishStatus::ConnectionLost,
            }),
            Effect::EmitEvent(Event::PublishResult {
                instance: "protobar".into(),
                port: "alt".into(),
                correlation: 21,
                status: PublishStatus::ConnectionLost,
            }),
        ]
    );
    // The wakeup is the backoff deadline (reset to 3s nominal by Welcome).
    let deadline = assert_backoff_deadline(wakeup_tail, Millis(7), 3_000);
    // The correlations are gone: a straggling PublishResult is now fatal —
    // but the connection is already down, so feed it after a fresh Welcome
    // would re-establish; here we only assert the map was drained by
    // confirming a second disconnect emits no ConnectionLost.
    let effects = core.on_input(Input::Tick, deadline);
    assert!(matches!(effects[0], Effect::Connect { .. }));
}

#[test]
#[should_panic(expected = "duplicate pending publish correlation")]
fn duplicate_pending_publish_correlation_panics() {
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    publish(&mut core, 5, "protobar", "out", "a", Millis(5));
    publish(&mut core, 5, "protobar", "out", "b", Millis(6));
}

#[test]
fn fatal_with_a_pending_publish_fails_it_connection_lost() {
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    publish(&mut core, 42, "protobar", "out", "payload", Millis(5));
    // A second Welcome while Active is a fatal protocol error unrelated to the
    // publish. go_fatal must still drain the outstanding correlation with
    // ConnectionLost, ordered ahead of the Fatal tail.
    let effects = core.on_input(
        Input::TextFrame(welcome_frame(
            vec![],
            vec![output_binding("protobar", "out")],
        )),
        Millis(6),
    );
    // go_fatal no longer emits an error-report breadcrumb (a dying connection);
    // it closes, drains the pending publish, surfaces Fatal, and disarms.
    assert_eq!(effects.len(), 4, "{effects:?}");
    assert_eq!(effects[0], Effect::CloseTransport);
    assert_eq!(
        effects[1],
        Effect::EmitEvent(Event::PublishResult {
            instance: "protobar".into(),
            port: "out".into(),
            correlation: 42,
            status: PublishStatus::ConnectionLost,
        })
    );
    assert!(matches!(effects[2], Effect::EmitEvent(Event::Fatal { .. })));
    assert_eq!(effects[3], Effect::SetWakeup(None));
}

#[test]
fn publish_after_fatal_is_answered_not_connected() {
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    // Drive the core Fatal via a second Welcome.
    core.on_input(
        Input::TextFrame(welcome_frame(
            vec![],
            vec![output_binding("protobar", "out")],
        )),
        Millis(6),
    );
    // A publish racing the fatal transition still gets exactly one result,
    // and no frame is sent.
    let effects = publish(&mut core, 9, "protobar", "out", "x", Millis(7));
    assert_eq!(
        effects,
        vec![Effect::EmitEvent(Event::PublishResult {
            instance: "protobar".into(),
            port: "out".into(),
            correlation: 9,
            status: PublishStatus::NotConnected,
        })]
    );
}

#[test]
fn stale_build_close_with_a_pending_publish_fails_it_connection_lost() {
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    publish(&mut core, 42, "protobar", "out", "payload", Millis(5));
    // The stale-build close drains the outstanding publish with
    // ConnectionLost, ordered ahead of the ReloadRequired tail.
    let effects = core.on_input(
        Input::Disconnected {
            code: Some(STALE_BUILD_CLOSE_CODE),
            reason: "server-build-99".into(),
        },
        Millis(6),
    );
    assert_eq!(
        effects,
        vec![
            Effect::EmitEvent(Event::PublishResult {
                instance: "protobar".into(),
                port: "out".into(),
                correlation: 42,
                status: PublishStatus::ConnectionLost,
            }),
            Effect::EmitEvent(Event::ReloadRequired {
                server_build: "server-build-99".into(),
            }),
            Effect::SetWakeup(None),
        ]
    );
}

// ── error-report publish ──────────────────────────────────────────────────

#[test]
fn report_port_publishes_when_floor_advertised() {
    // With the floor advertised, the reserved `#brenn`/`error-reports` port is
    // treated as bound even though it is absent from the bindings table, so a
    // publish to it is accepted and framed.
    let mut core = active_core_with_reports(vec![]);
    let effects = publish(&mut core, 7, "#brenn", "error-reports", "{}", Millis(10));
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Publish {
            instance: "#brenn".into(),
            port: "error-reports".into(),
            body: "{}".into(),
            correlation: Some(7),
            subject_instance: None,
            urgency: None,
        })]
    );
}

/// A report subject rides the frame verbatim: the core forwards it without
/// deriving or validating anything, because the authoritative declaration set is
/// server-side. This is the wire half of per-component error attribution.
#[test]
fn report_subject_rides_the_frame_verbatim() {
    let mut core = active_core_with_reports(vec![]);
    let effects = publish_with_subject(
        &mut core,
        7,
        "#brenn",
        "error-reports",
        "{}",
        Some("clock-1"),
        Millis(10),
    );
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Publish {
            instance: "#brenn".into(),
            port: "error-reports".into(),
            body: "{}".into(),
            correlation: Some(7),
            subject_instance: Some("clock-1".into()),
            urgency: None,
        })]
    );
}

#[test]
fn report_port_rejected_when_floor_absent() {
    // No floor advertised: the reserved port is unbound like any other pair, so
    // a publish to it is the ordinary unbound-port rejection.
    let mut core = active_core();
    let effects = publish(&mut core, 7, "#brenn", "error-reports", "{}", Millis(10));
    assert_eq!(
        effects,
        vec![Effect::EmitEvent(Event::PublishResult {
            instance: "#brenn".into(),
            port: "error-reports".into(),
            correlation: 7,
            status: PublishStatus::UnboundPort,
        })]
    );
}

#[test]
fn report_port_publish_result_is_swallowed() {
    // A result for the reserved port is consumed (liveness reset) but emits no
    // Event — closing the failed-report feedback loop.
    let mut core = active_core_with_reports(vec![]);
    publish(&mut core, 7, "#brenn", "error-reports", "{}", Millis(10));
    let effects = core.on_input(
        Input::TextFrame(publish_result_frame(Some(7), PublishOutcome::RateLimited)),
        Millis(11),
    );
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::EmitEvent(Event::PublishResult { .. }))),
        "reserved-port result must not surface: {effects:?}"
    );
}

#[test]
fn report_port_pending_drained_silently_on_disconnect() {
    // A report still pending on the wire when the transport drops must not
    // surface a ConnectionLost result: across the async event channel the kernel
    // could drain it after a reconnect and publish a fresh report about the
    // failed report. The correlation is drained (a later straggler is fatal), but
    // no reserved-port event is emitted; an ordinary publish still fails loudly.
    let mut core = active_core_with_reports(vec![output_binding("protobar", "out")]);
    publish(&mut core, 10, "protobar", "out", "a", Millis(5));
    publish(&mut core, 11, "#brenn", "error-reports", "{}", Millis(6));
    let effects = core.on_input(
        Input::Disconnected {
            code: None,
            reason: String::new(),
        },
        Millis(7),
    );
    let results: Vec<&Effect> = effects
        .iter()
        .filter(|e| matches!(e, Effect::EmitEvent(Event::PublishResult { .. })))
        .collect();
    assert_eq!(
        results,
        vec![&Effect::EmitEvent(Event::PublishResult {
            instance: "protobar".into(),
            port: "out".into(),
            correlation: 10,
            status: PublishStatus::ConnectionLost,
        })],
        "only the ordinary publish surfaces ConnectionLost: {effects:?}"
    );
}

#[test]
fn report_port_local_reject_emits_no_event() {
    // A reserved-port report rejected locally (an oversize body — the reports
    // fixture caps at 65_536) is swallowed exactly like its wire result: no event
    // re-enters the kernel's non-`Ok` breadcrumb path.
    let mut core = active_core_with_reports(vec![]);
    let big = "x".repeat(65_537);
    let effects = publish(&mut core, 7, "#brenn", "error-reports", &big, Millis(10));
    assert!(
        effects.is_empty(),
        "reserved-port local reject must emit nothing: {effects:?}"
    );
}

// ── alert ─────────────────────────────────────────────────────────────────

#[test]
fn alert_while_active_sends_the_frame() {
    let mut core = active_core_alert_granted();
    let effects = alert(
        &mut core,
        AlertSeverity::Warning,
        "component panic",
        "the details",
        Millis(10),
    );
    assert_eq!(
        effects,
        vec![Effect::SendFrame(ClientFrame::Alert {
            severity: AlertSeverity::Warning,
            title: "component panic".into(),
            body: "the details".into(),
        })]
    );
}

#[test]
fn alert_while_active_but_ungranted_is_dropped() {
    // The default `active_core` fixture is alert-ungranted. An `Alert` on it
    // is a grant violation the server kills the session over, so the core
    // drops it here instead of letting `ClientHandle::alert` reach the wire.
    let mut core = active_core();
    assert!(
        alert(
            &mut core,
            AlertSeverity::Warning,
            "component panic",
            "why",
            Millis(10)
        )
        .is_empty()
    );
}

#[test]
fn alert_while_not_active_is_dropped() {
    // A fresh core is still `Connecting`; alerting rides the same WS, so with
    // no live connection the alert is silently dropped, exactly like `log`.
    let (mut core, _init) = ClientCore::new(cfg(), Millis(0));
    assert!(alert(&mut core, AlertSeverity::Critical, "boom", "why", Millis(1)).is_empty());
}

#[test]
fn alert_truncates_both_fields_to_the_proto_caps() {
    let mut core = active_core_alert_granted();
    // Multibyte content so truncation must land on a UTF-8 boundary.
    let long_title = "é".repeat(MAX_ALERT_TITLE_BYTES);
    let long_body = "本".repeat(MAX_ALERT_BODY_BYTES);
    let effects = alert(
        &mut core,
        AlertSeverity::Critical,
        &long_title,
        &long_body,
        Millis(10),
    );
    match &effects[..] {
        [Effect::SendFrame(ClientFrame::Alert { title, body, .. })] => {
            assert!(title.len() <= MAX_ALERT_TITLE_BYTES, "{}", title.len());
            assert!(title.ends_with("…[truncated]"));
            assert!(body.len() <= MAX_ALERT_BODY_BYTES, "{}", body.len());
            assert!(body.ends_with("…[truncated]"));
        }
        other => panic!("expected one Alert SendFrame, got {other:?}"),
    }
}

// ── Publish urgency ───────────────────────────────────────────────────

#[test]
fn publish_with_no_urgency_sends_none_not_the_bindings_default() {
    // The core forwards the caller's `None` verbatim rather than substituting
    // the binding's advertised default. The server holds the authoritative
    // default; the client's snapshot can be stale across a reconnect, so
    // substituting here would put a stale value on the wire exactly when it
    // races a bindings change.
    let mut core =
        active_core_with_outputs(vec![output_binding_at("protobar", "out", Urgency::High)]);
    let effects = publish(&mut core, 1, "protobar", "out", "x", Millis(5));
    match &effects[..] {
        [Effect::SendFrame(ClientFrame::Publish { urgency, .. })] => {
            assert_eq!(*urgency, None, "the port default must not be echoed back");
        }
        other => panic!("expected one Publish SendFrame, got {other:?}"),
    }
}

#[test]
fn publish_with_urgency_puts_the_override_on_the_frame() {
    for level in Urgency::ALL {
        let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
        let effects = publish_at(&mut core, 1, "protobar", "out", "x", level, Millis(5));
        match &effects[..] {
            [Effect::SendFrame(ClientFrame::Publish { urgency, .. })] => {
                assert_eq!(*urgency, Some(level));
            }
            other => panic!("expected one Publish SendFrame for {level:?}, got {other:?}"),
        }
    }
}

#[test]
fn publish_urgency_is_absent_from_the_frame_json_when_unset() {
    // `skip_serializing_if` — an ordinary publish's frame carries no `urgency`
    // key at all, so absent-means-the-port's-default is what the server reads,
    // and the common frame stays the size it was.
    let mut core = active_core_with_outputs(vec![output_binding("protobar", "out")]);
    let effects = publish(&mut core, 1, "protobar", "out", "x", Millis(5));
    let [Effect::SendFrame(frame)] = &effects[..] else {
        panic!("expected one SendFrame, got {effects:?}");
    };
    let v: serde_json::Value = serde_json::to_value(frame).unwrap();
    assert!(
        v.get("urgency").is_none(),
        "urgency must be absent, not null: {v}"
    );
}

//! The fold, driven against a real [`SurfacePage`] — real stores, a real
//! subscription plane, a real confined router carrying the surface's plane policy
//! and real outboxes — so a kill really kills and a frame is the one that would go
//! on the wire.
//!
//! The answers themselves are constructed directly where a pass would hand them
//! back. That is deliberate: the passes are covered where they live, and what is
//! under test here is what each shape of answer *becomes*.

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::publish::FlushBatch;
use brenn_attach_client::router::{MessageStamp, Origin, RouteOutcome, RouteRequest};
use brenn_attach_proto::{PublishBatchOutcome, PublishOutcome, VersionRange, negotiate};
use brenn_envelope::Urgency;
use brenn_surface_contract::ActivationError;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{LOCAL_OVERLAY_STATE_CHANNEL, LOCAL_TOAST_CHANNEL, ToastBody};
use uuid::Uuid;

use crate::activation::DropAnnouncement;
use crate::flush::PlaneRefusal;
use crate::outbound::{PortPublish, TelemetryKind, resolve_output};
use crate::test_support::bindings as fixtures;
use crate::test_support::bindings::output;
use crate::test_support::pages;
use crate::test_support::pages::{BODY_CAP, PRINCIPAL, SESSION_ID};

use super::*;

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const OUT: &str = "ephemeral:site.bar.out";
const NOTES: &str = "local:app/notes";
const EPOCH: Uuid = Uuid::from_u128(0x5107);
const NOW: Millis = Millis(1_000);
const NOW_MS: u64 = 1_000;

/// `p1` and `p2` each write the one wire channel; chrome exists because every
/// surface has one. `kind` distinguishes two byte-different documents that wire the
/// page the same way.
fn doc(kind: &str) -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component_of_kind("p1", kind),
            fixtures::component("p2"),
            fixtures::component(fixtures::CHROME),
        ],
        Vec::new(),
        vec![
            output("p1", "out", OUT),
            output("p2", "out", OUT),
            output("p1", "notes", NOTES),
            output(fixtures::CHROME, "over", LOCAL_OVERLAY_STATE_CHANNEL),
        ],
        vec![
            fixtures::local(LOCAL_OVERLAY_STATE_CHANNEL, 1),
            fixtures::local(NOTES, 2),
        ],
    )
}

/// The shared attachment, with the one knob this suite varies: the alert grant, so
/// a composed `Alert` is one the peer would accept.
fn facts() -> AttachmentFacts {
    AttachmentFacts {
        alert_granted: true,
        ..pages::facts()
    }
}

fn fresh() -> SurfacePage {
    SurfacePage::new(CONFIG.to_string(), EPOCH)
}

/// A configured page: attached, `p1`/`p2`/chrome registered and scheduled, one
/// document in force.
fn page() -> SurfacePage {
    pages::configured_page(
        CONFIG,
        EPOCH,
        facts(),
        &["p1", "p2", fixtures::CHROME],
        &doc("protobar"),
        NOW,
    )
}

/// The same page with its attachment gone: the wiring stays, so a pass can still
/// compose, but nothing may reach a socket.
fn detached() -> SurfacePage {
    let mut page = page();
    page.on_detached();
    page
}

fn announcement(instance: &str) -> DropAnnouncement {
    DropAnnouncement {
        instance: instance.to_string(),
        port: "in".to_string(),
        channel: "brenn:site.bar.in".to_string(),
        dropped: 3,
    }
}

fn fold(page: &mut SurfacePage, f: impl FnOnce(&mut Reactions, &mut SurfacePage)) -> Vec<Effect> {
    let mut reactions = Reactions::new();
    f(&mut reactions, page);
    reactions.into_effects()
}

fn events(effects: &[Effect]) -> Vec<&Event> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::EmitEvent(event) => Some(event),
            _ => None,
        })
        .collect()
}

fn frames(effects: &[Effect]) -> Vec<&ClientFrame> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::SendFrame(frame) => Some(frame),
            _ => None,
        })
        .collect()
}

fn toasts(effects: &[Effect]) -> Vec<String> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::PublishControl { channel, body } => {
                assert_eq!(channel, LOCAL_TOAST_CHANNEL);
                let toast: ToastBody =
                    serde_json::from_str(body).expect("a control publish carries a toast");
                Some(toast.text)
            }
            _ => None,
        })
        .collect()
}

fn configured(first_of_attachment: bool, wiring_changed: bool) -> Configured {
    Configured {
        first_of_attachment,
        wiring_changed,
        ..Configured::default()
    }
}

#[test]
fn phase_one_subscribes_the_config_channel_and_announces_nothing() {
    let mut page = fresh();
    let effects = fold(&mut page, |r, page| {
        r.conn_event(page, ConnEvent::Attached(facts()));
    });

    assert!(events(&effects).is_empty(), "the page is not usable yet");
    let [ClientFrame::Subscribe { channel, .. }] = frames(&effects)[..] else {
        panic!("phase 1 sends exactly one Subscribe: {effects:?}");
    };
    assert_eq!(channel, CONFIG);
}

#[test]
fn a_detach_reports_the_loss_and_then_what_died_with_it() {
    let mut page = page();
    let SurfacePage {
        connect, outbound, ..
    } = &mut page;
    let bindings = connect.bindings().expect("a document is in force");
    let out = resolve_output(bindings, "p1", "out", None).expect("the fixture binds p1/out");
    outbound.publish_port(
        out,
        PortPublish {
            instance: "p1".to_string(),
            port: "out".to_string(),
            body: "{}".to_string(),
            urgency: None,
            correlation: 7,
        },
    );

    let effects = fold(&mut page, |r, page| {
        r.conn_event(
            page,
            ConnEvent::Detached {
                reason: DetachReason::LivenessTimeout,
            },
        );
    });

    match events(&effects)[..] {
        [
            Event::Disconnected { reason },
            Event::PublishResult {
                instance,
                port,
                correlation,
                status,
            },
        ] => {
            assert_eq!(*reason, DetachReason::LivenessTimeout);
            assert_eq!(
                (instance.as_str(), port.as_str(), *correlation),
                ("p1", "out", 7)
            );
            assert_eq!(*status, PublishStatus::ConnectionLost);
        }
        ref other => panic!("the loss is reported before what died with it: {other:?}"),
    }
}

/// The detach leg folds the outboxes' answer too, and the one thing that answer
/// carries is the retry deadline. Left armed against a dead socket it fires into a
/// page with no attachment, where the flush it composes has nowhere to go and the
/// fold refuses to compose it.
#[test]
fn a_detach_disarms_the_retry_deadline_a_refused_flush_armed() {
    let mut page = page();
    let SurfacePage {
        connect, outbound, ..
    } = &mut page;
    let bindings = connect.bindings().expect("a document is in force");
    let steps = outbound.flush(
        bindings,
        connect.facts(),
        "p1",
        FlushBatch {
            entries: vec![brenn_attach_proto::BatchEntry {
                channel: OUT.to_string(),
                body: "{}".to_string(),
                urgency: Urgency::Normal,
                deliver_after: None,
            }],
            ops: Vec::new(),
        },
        NOW,
    );
    let [ClientFrame::PublishBatch { correlation, .. }] = &steps.frames[..] else {
        panic!("the flush went straight out: {steps:?}");
    };
    let refused = outbound
        .on_batch_result(*correlation, PublishBatchOutcome::RateLimited, NOW)
        .expect("the correlation is outstanding");
    assert!(
        matches!(refused.steps.retry_wakeup, Some(TimerChange::Arm(_))),
        "a refused head arms the probe: {refused:?}"
    );

    let effects = fold(&mut page, |r, page| {
        r.conn_event(
            page,
            ConnEvent::Detached {
                reason: DetachReason::LivenessTimeout,
            },
        );
    });

    assert!(matches!(
        effects[0],
        Effect::EmitEvent(Event::Disconnected { .. })
    ));
    assert_eq!(
        effects.last(),
        Some(&Effect::SetRetryWakeup(TimerChange::Disarm)),
        "the deadline dies with the socket it was armed against: {effects:?}"
    );
}

#[test]
fn a_fatal_connection_event_reaches_the_platform_half() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.conn_event(
            page,
            ConnEvent::Fatal {
                detail: "unknown correlation".to_string(),
            },
        );
    });

    assert_eq!(
        events(&effects),
        vec![&Event::Fatal {
            detail: "unknown correlation".to_string()
        }]
    );
}

/// A terminal verdict ends an attachment as finally as a detach does, and the
/// page is told so: a caller still awaiting a publish is owed `ConnectionLost`
/// now rather than never, and a page that went on believing itself attached would
/// compose frames for a socket that is gone.
///
/// All three verdicts: the version mismatch and the stale-build close end an
/// attachment exactly as a diagnosed fatal does.
#[test]
fn every_terminal_verdict_leaves_the_page_detached() {
    for verdict in [
        ConnEvent::Fatal {
            detail: "unknown correlation".to_string(),
        },
        ConnEvent::Incompatible {
            ours: VersionRange { min: 1, max: 1 },
            theirs: VersionRange { min: 2, max: 3 },
        },
        ConnEvent::PeerClosedTerminal {
            code: 4001,
            reason: "build=deadbeef".to_string(),
        },
    ] {
        let mut page = page();
        send_one_publish(&mut page);
        assert!(
            page.connect.facts().is_some(),
            "the fixture page is attached"
        );

        let effects = fold(&mut page, |r, page| {
            r.conn_event(page, verdict.clone());
        });

        assert!(page.connect.facts().is_none(), "{verdict:?}");
        assert!(
            events(&effects).iter().any(|event| matches!(
                event,
                Event::PublishResult {
                    correlation: 7,
                    status: PublishStatus::ConnectionLost,
                    ..
                }
            )),
            "the caller the verdict stranded is answered: {verdict:?} {effects:?}"
        );
    }
}

/// Idempotent: a verdict reached while already detached asks for nothing beyond
/// reporting itself.
#[test]
fn a_terminal_verdict_on_an_already_detached_page_still_reports_itself() {
    let mut page = detached();
    let effects = fold(&mut page, |r, page| {
        r.conn_event(
            page,
            ConnEvent::Fatal {
                detail: "again".into(),
            },
        );
    });
    assert_eq!(
        events(&effects),
        vec![&Event::Fatal {
            detail: "again".to_string()
        }]
    );
}

/// Put one publish of `p1`'s on the wire, so a terminal verdict has a caller to
/// strand.
fn send_one_publish(page: &mut SurfacePage) {
    let SurfacePage {
        connect, outbound, ..
    } = page;
    let bindings = connect.bindings().expect("a document is in force");
    let out = resolve_output(bindings, "p1", "out", None).expect("the fixture binds p1/out");
    outbound.publish_port(
        out,
        PortPublish {
            instance: "p1".to_string(),
            port: "out".to_string(),
            body: "{}".to_string(),
            urgency: None,
            correlation: 7,
        },
    );
}

#[test]
fn an_incompatible_peer_is_terminal_but_not_fatal() {
    let ours = VersionRange { min: 1, max: 1 };
    let theirs = VersionRange { min: 2, max: 3 };
    assert_eq!(negotiate(ours, theirs), None, "the fixture ranges disagree");

    let mut page = fresh();
    let effects = fold(&mut page, |r, page| {
        r.conn_event(page, ConnEvent::Incompatible { ours, theirs });
    });

    assert_eq!(
        events(&effects),
        vec![&Event::Incompatible { ours, theirs }]
    );
}

#[test]
fn a_terminal_close_asks_for_a_reload_naming_the_peer_build() {
    let mut page = fresh();
    let effects = fold(&mut page, |r, page| {
        r.conn_event(
            page,
            ConnEvent::PeerClosedTerminal {
                code: 4001,
                reason: "build=deadbeef".to_string(),
            },
        );
    });

    assert_eq!(
        events(&effects),
        vec![&Event::ReloadRequired {
            server_build: "build=deadbeef".to_string()
        }]
    );
}

#[test]
fn phase_two_announces_the_attachment_with_its_wiring_and_its_facts() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.configured(page, configured(true, false), NOW, NOW_MS);
    });

    match events(&effects)[..] {
        [
            Event::Connected {
                bindings,
                participant_id,
                session_id,
                max_body_bytes,
                alert_granted,
            },
        ] => {
            assert_eq!(bindings, &doc("protobar"));
            assert_eq!(participant_id, PRINCIPAL);
            assert_eq!(session_id, SESSION_ID);
            assert_eq!(*max_body_bytes, BODY_CAP);
            assert!(alert_granted);
        }
        ref other => panic!("phase 2 announces exactly one Connected: {other:?}"),
    }
}

#[test]
fn a_reconnect_carrying_a_changed_document_announces_both() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.configured(page, configured(true, true), NOW, NOW_MS);
    });

    assert!(matches!(events(&effects)[0], Event::Connected { .. }));
    assert_eq!(events(&effects)[1], &Event::WiringChanged);
}

#[test]
fn a_second_document_mid_attachment_announces_only_the_change() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.configured(page, configured(false, true), NOW, NOW_MS);
    });

    assert_eq!(events(&effects), vec![&Event::WiringChanged]);
}

#[test]
fn a_byte_equal_document_mid_attachment_announces_nothing() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.configured(page, configured(false, false), NOW, NOW_MS);
    });

    assert!(effects.is_empty(), "{effects:?}");
}

#[test]
fn phase_twos_subscribes_follow_the_announcement() {
    let mut page = page();
    let subscribe = ClientFrame::Subscribe {
        channel: OUT.to_string(),
        push_depth: 1,
        retain_depth: 1,
        resume: None,
    };
    let effects = fold(&mut page, |r, page| {
        r.configured(
            page,
            Configured {
                frames: vec![subscribe.clone()],
                ..configured(true, false)
            },
            NOW,
            NOW_MS,
        );
    });

    assert!(matches!(
        effects[0],
        Effect::EmitEvent(Event::Connected { .. })
    ));
    assert_eq!(effects[1], Effect::SendFrame(subscribe));
}

#[test]
fn a_publish_answer_reaches_the_caller_that_holds_its_correlation() {
    let mut page = page();
    let effects = fold(&mut page, |r, _| {
        r.answers(vec![PublishAnswer::Port {
            instance: "p1".to_string(),
            port: "out".to_string(),
            correlation: 12,
            status: PublishStatus::RateLimited,
        }]);
    });

    assert_eq!(
        events(&effects),
        vec![&Event::PublishResult {
            instance: "p1".to_string(),
            port: "out".to_string(),
            correlation: 12,
            status: PublishStatus::RateLimited,
        }]
    );
}

#[test]
fn a_refused_telemetry_document_asks_for_nothing() {
    let mut page = page();
    let effects = fold(&mut page, |r, _| {
        r.answers(vec![PublishAnswer::TelemetryDropped {
            kind: TelemetryKind::Status,
            outcome: PublishOutcome::RateLimited,
        }]);
    });

    assert!(effects.is_empty(), "{effects:?}");
}

#[test]
fn an_announced_loss_becomes_one_alert_and_one_toast_saying_the_same_thing() {
    let mut page = page();
    let announced = announcement("p1");
    let effects = fold(&mut page, |r, page| {
        r.verdicts(
            page,
            DropVerdicts {
                announce: vec![announced.clone()],
                fatal: Vec::new(),
            },
            NOW,
            NOW_MS,
        );
    });

    let [ClientFrame::Alert { severity, body, .. }] = frames(&effects)[..] else {
        panic!("one alert: {effects:?}");
    };
    assert_eq!(*severity, brenn_attach_proto::AlertSeverity::Warning);
    assert_eq!(*body, announced.describe());
    assert_eq!(toasts(&effects), vec![announced.describe()]);
    assert!(events(&effects).is_empty(), "nothing died");
}

#[test]
fn an_alert_composed_with_no_attachment_is_dropped_and_its_toast_is_not() {
    let mut page = detached();
    let announced = announcement("p1");
    let effects = fold(&mut page, |r, page| {
        r.verdicts(
            page,
            DropVerdicts {
                announce: vec![announced.clone()],
                fatal: Vec::new(),
            },
            NOW,
            NOW_MS,
        );
    });

    assert!(frames(&effects).is_empty(), "the socket is gone");
    assert_eq!(toasts(&effects), vec![announced.describe()]);
}

#[test]
fn a_fatal_loss_takes_its_instance_terminal_and_reports_it_once() {
    let mut page = page();
    let fatal = announcement("p1");
    let effects = fold(&mut page, |r, page| {
        r.verdicts(
            page,
            DropVerdicts {
                announce: Vec::new(),
                fatal: vec![fatal.clone()],
            },
            NOW,
            NOW_MS,
        );
    });

    assert_eq!(
        events(&effects),
        vec![&Event::InstanceFailed {
            instance: "p1".to_string(),
            reason: fatal.describe(),
        }]
    );
    assert!(page.registrations.is_failed("p1"));

    // A second verdict for the same instance reports nothing further.
    let again = fold(&mut page, |r, page| {
        r.verdicts(
            page,
            DropVerdicts {
                announce: Vec::new(),
                fatal: vec![fatal.clone()],
            },
            NOW,
            NOW_MS,
        );
    });
    assert!(events(&again).is_empty(), "{again:?}");
}

/// One retirement can push retention past the positions of several instances at
/// once, and each of them was configured to die of the loss. Every kill in the set
/// is enacted, in the order the ladder answered them.
#[test]
fn every_fatal_verdict_in_one_pass_takes_its_own_instance_terminal() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.verdicts(
            page,
            DropVerdicts {
                announce: Vec::new(),
                fatal: vec![announcement("p1"), announcement("p2")],
            },
            NOW,
            NOW_MS,
        );
    });

    assert_eq!(
        events(&effects),
        vec![
            &Event::InstanceFailed {
                instance: "p1".to_string(),
                reason: announcement("p1").describe(),
            },
            &Event::InstanceFailed {
                instance: "p2".to_string(),
                reason: announcement("p2").describe(),
            },
        ]
    );
    assert!(page.registrations.is_failed("p1"));
    assert!(
        page.registrations.is_failed("p2"),
        "the second instance's configured rung says it dies too"
    );
}

/// An unmount passes through a state where the instance is deregistered but
/// still scheduled. Killing here would panic on failing an unregistered
/// instance.
#[test]
fn a_fatal_loss_naming_a_deregistered_instance_kills_nothing() {
    let mut page = page();
    page.registrations
        .deregister("p1", &mut page.stores, &mut page.subs);
    assert!(
        page.schedules.is_tracked("p1"),
        "only the registration went"
    );

    let effects = fold(&mut page, |r, page| {
        r.verdicts(
            page,
            DropVerdicts {
                announce: vec![announcement("p1")],
                fatal: vec![announcement("p1")],
            },
            NOW,
            NOW_MS,
        );
    });

    assert!(events(&effects).is_empty(), "nobody is left to fail");
    assert_eq!(
        toasts(&effects).len(),
        1,
        "the loss is still announced: {effects:?}"
    );
}

/// The other half, on its own: an instance whose scheduler state has gone while its
/// registration stands. Killing here would panic on an unscheduled instance.
#[test]
fn a_fatal_loss_naming_an_unscheduled_instance_kills_nothing() {
    let mut page = page();
    page.schedules.forget("p1");
    assert!(
        page.registrations.is_registered("p1"),
        "only the schedule went"
    );

    let effects = fold(&mut page, |r, page| {
        r.verdicts(
            page,
            DropVerdicts {
                announce: vec![announcement("p1")],
                fatal: vec![announcement("p1")],
            },
            NOW,
            NOW_MS,
        );
    });

    assert!(events(&effects).is_empty(), "nobody is left to fail");
    assert!(
        !page.registrations.is_failed("p1"),
        "and nothing was half-enacted"
    );
    assert_eq!(
        toasts(&effects).len(),
        1,
        "the loss is still announced: {effects:?}"
    );
}

/// The release leg is the ladder's, exactly as an arrival's is: a sweep that
/// evicted positions announces and kills through this fold, and not only through a
/// completion's.
#[test]
fn a_release_that_evicted_positions_announces_and_kills() {
    let mut page = page();
    let announced = announcement("p1");
    let effects = fold(&mut page, |r, page| {
        r.released(
            page,
            Released {
                channels: vec![NOTES.to_string()],
                released: 1,
                drops: DropVerdicts {
                    announce: vec![announced.clone()],
                    fatal: vec![announcement("p2")],
                },
            },
            NOW,
            NOW_MS,
        );
    });

    let [ClientFrame::Alert { body, .. }] = frames(&effects)[..] else {
        panic!("one alert for the announcement: {effects:?}");
    };
    assert_eq!(*body, announced.describe());
    assert_eq!(toasts(&effects), vec![announced.describe()]);
    assert_eq!(
        events(&effects),
        vec![&Event::InstanceFailed {
            instance: "p2".to_string(),
            reason: announcement("p2").describe(),
        }]
    );
    assert!(page.registrations.is_failed("p2"));
}

/// The invariant the fold documents, enforced rather than trusted: an `Alert` is the
/// only frame class a pass may compose while detached. Anything else composed there
/// is a bug in a pass, and swallowing it would show up as an absence — a channel
/// that never resubscribes, an outbox that believes its batch went out.
#[test]
#[should_panic(expected = "composed Subscribe with no attachment")]
fn a_subscription_frame_composed_with_no_attachment_is_a_bug() {
    let mut page = detached();
    fold(&mut page, |r, page| {
        r.inbound(
            page,
            Inbound {
                frames: vec![ClientFrame::Subscribe {
                    channel: OUT.to_string(),
                    push_depth: 1,
                    retain_depth: 1,
                    resume: None,
                }],
                ..Inbound::default()
            },
            NOW,
            NOW_MS,
        );
    });
}

#[test]
fn an_outbox_pass_sends_its_frames_toasts_each_dropped_flush_and_states_the_timer() {
    let mut page = page();
    let batch = ClientFrame::PublishBatch {
        attribution: Some("p1".to_string()),
        correlation: 3,
        publishes: Vec::new(),
        deferred_ops: Vec::new(),
    };
    let effects = fold(&mut page, |r, page| {
        r.steps(
            page,
            OutboxSteps {
                frames: vec![batch.clone()],
                dropped: vec!["p2".to_string()],
                retry_wakeup: Some(TimerChange::Arm(Millis(2_000))),
            },
        );
    });

    assert_eq!(frames(&effects), vec![&batch]);
    let [toast] = &toasts(&effects)[..] else {
        panic!("one toast per dropped flush: {effects:?}");
    };
    assert!(toast.starts_with("p2: a queued publish batch was dropped"));
    assert_eq!(
        effects.last(),
        Some(&Effect::SetRetryWakeup(TimerChange::Arm(Millis(2_000))))
    );
}

#[test]
fn an_unchanged_retry_deadline_is_left_alone() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.steps(page, OutboxSteps::default());
    });

    assert!(effects.is_empty(), "{effects:?}");
}

#[test]
fn an_err_completion_reports_the_failure_and_keeps_the_instance() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.completion(
            page,
            Completion::nothing(
                "p1".to_string(),
                ActivationOutcome::Err(ActivationError {
                    message: "bad input".to_string(),
                }),
            ),
            NOW,
            NOW_MS,
        );
    });

    assert_eq!(
        events(&effects),
        vec![&Event::ActivationFailed {
            instance: "p1".to_string(),
            message: "bad input".to_string(),
        }]
    );
    assert!(!page.registrations.is_failed("p1"));
}

#[test]
fn a_trap_completion_reports_its_instance_terminal_in_its_own_words() {
    let mut page = page();
    let killed = crate::flush::Killed {
        first: true,
        discarded: 2,
        retry_wakeup: Some(TimerChange::Disarm),
    };
    let effects = fold(&mut page, |r, page| {
        r.completion(
            page,
            Completion {
                killed: Some(killed),
                ..Completion::nothing(
                    "p1".to_string(),
                    ActivationOutcome::Trap("unreachable executed".to_string()),
                )
            },
            NOW,
            NOW_MS,
        );
    });

    assert_eq!(effects[0], Effect::SetRetryWakeup(TimerChange::Disarm));
    assert_eq!(
        events(&effects),
        vec![&Event::InstanceFailed {
            instance: "p1".to_string(),
            reason: "unreachable executed".to_string(),
        }]
    );
}

#[test]
fn an_absorbed_completion_asks_for_nothing() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.completion(
            page,
            Completion {
                absorbed: true,
                killed: None,
                ..Completion::nothing(
                    "gone".to_string(),
                    ActivationOutcome::Trap("poisoned".to_string()),
                )
            },
            NOW,
            NOW_MS,
        );
    });

    assert!(effects.is_empty(), "{effects:?}");
}

#[test]
fn a_plane_refusal_is_reported_against_its_publisher() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.completion(
            page,
            Completion {
                refusals: vec![PlaneRefusal {
                    port: "over".to_string(),
                    channel: LOCAL_OVERLAY_STATE_CHANNEL.to_string(),
                    reason: "holder is not a declared instance".to_string(),
                }],
                ..Completion::nothing(fixtures::CHROME.to_string(), ActivationOutcome::Ok(None))
            },
            NOW,
            NOW_MS,
        );
    });

    assert_eq!(
        events(&effects),
        vec![&Event::PlaneRefused {
            instance: fixtures::CHROME.to_string(),
            port: "over".to_string(),
            channel: LOCAL_OVERLAY_STATE_CHANNEL.to_string(),
            reason: "holder is not a declared instance".to_string(),
        }]
    );
}

#[test]
fn a_straggler_is_reported_as_a_diagnostic() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.inbound(
            page,
            Inbound {
                straggler: Some(crate::inbound::Straggler {
                    channel: OUT.to_string(),
                    seq: 9,
                    dropped: 2,
                }),
                ..Inbound::default()
            },
            NOW,
            NOW_MS,
        );
    });

    assert_eq!(
        events(&effects),
        vec![&Event::StragglerDiscarded {
            channel: OUT.to_string(),
            seq: 9,
            dropped: 2,
        }]
    );
}

#[test]
fn a_gap_and_a_lost_flush_ask_for_nothing() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.inbound(
            page,
            Inbound {
                gap: Some(crate::inbound::ChannelGap {
                    channel: OUT.to_string(),
                    replay_count: 4,
                    gap: brenn_attach_proto::GapInfo {
                        reason: brenn_attach_proto::GapReason::BeyondRetained,
                    },
                }),
                lost_flushes: vec![crate::outbound::LostFlush {
                    instance: "p2".to_string(),
                    batch: FlushBatch {
                        entries: Vec::new(),
                        ops: Vec::new(),
                    },
                }],
                ..Inbound::default()
            },
            NOW,
            NOW_MS,
        );
    });

    assert!(effects.is_empty(), "{effects:?}");
}

#[test]
fn a_frame_carrying_a_document_announces_the_attachment_within_the_same_turn() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.inbound(
            page,
            Inbound {
                configured: vec![configured(true, false)],
                answers: vec![PublishAnswer::Port {
                    instance: "p1".to_string(),
                    port: "out".to_string(),
                    correlation: 1,
                    status: PublishStatus::Ok,
                }],
                ..Inbound::default()
            },
            NOW,
            NOW_MS,
        );
    });

    assert!(matches!(events(&effects)[0], Event::Connected { .. }));
    assert!(matches!(
        events(&effects)[1],
        Event::PublishResult { correlation: 1, .. }
    ));
}

#[test]
fn every_document_a_frame_carried_is_folded_in_frame_order() {
    // A config pass may carry several documents, and each one's announcement is
    // its own: the attachment is announced by the first, the wiring change by
    // whichever document made one. Folding only the last would lose the
    // announcement; folding them out of order would report the older wiring as
    // the newer.
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.inbound(
            page,
            Inbound {
                configured: vec![configured(true, false), configured(false, true)],
                ..Inbound::default()
            },
            NOW,
            NOW_MS,
        );
    });

    assert!(matches!(events(&effects)[0], Event::Connected { .. }));
    assert!(matches!(events(&effects)[1], Event::WiringChanged));
    assert_eq!(events(&effects).len(), 2, "{effects:?}");
}

#[test]
fn a_turn_that_moved_no_schedule_states_no_release_deadline() {
    let mut page = page();
    let effects = fold(&mut page, |r, page| {
        r.released(page, Released::default(), NOW, NOW_MS);
        r.end_turn(page);
    });

    assert!(effects.is_empty(), "{effects:?}");
}

#[test]
fn a_turn_that_parked_something_states_the_release_deadline_once() {
    let mut page = page();
    park_notes(&mut page, NOW_MS + 5_000);

    let effects = fold(&mut page, |r, page| {
        r.end_turn(page);
        r.end_turn(page);
    });

    assert_eq!(
        effects,
        vec![Effect::SetReleaseWakeup(ReleaseTimer::Arm(NOW_MS + 5_000))],
        "the second statement is the same deadline, so it says nothing"
    );
}

#[test]
fn go_fatal_leaves_the_terminal_event_to_the_connection() {
    let mut page = page();
    let effects = fold(&mut page, |r, _| {
        r.go_fatal("the config channel replayed nothing".to_string());
    });

    assert_eq!(
        effects,
        vec![Effect::GoFatal {
            detail: "the config channel replayed nothing".to_string()
        }]
    );
    assert!(events(&effects).is_empty(), "the connection mints it");
}

/// Park one of `p1`'s messages on the page-local channel it writes, so the page
/// holds a confined release deadline.
fn park_notes(page: &mut SurfacePage, release_at: u64) {
    let outcome = page.router.route(
        &mut page.stores,
        RouteRequest {
            channel: NOTES,
            origin: Origin::Sub("p1"),
            body: "{}".to_string(),
            stamp: MessageStamp {
                message_id: Uuid::from_u128(0x900),
                publish_ts: chrono::DateTime::from_timestamp(0, 0)
                    .expect("a representable instant"),
            },
            urgency: Urgency::Normal,
            deliver_after: Some(release_at),
        },
    );
    assert!(matches!(outcome, RouteOutcome::Parked { .. }));
}

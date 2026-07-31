//! The surface's outbound side, driven against a real outbox plane and a real
//! bindings document — so what the assertions read is the frame that would go on
//! the wire and the answer a caller would get back.

use std::collections::BTreeMap;

use brenn_attach_proto::{BatchEntry, PublishBatchOutcome};
use brenn_surface_schema::bindings::{
    BINDINGS_DOCUMENT_VERSION, BindingsDocument, PlatformSection,
};
use brenn_surface_schema::telemetry::ErrorReportDocument;
use brenn_surface_schema::{Abi, ComponentEntry, LocalChannel, OutputBinding};
use uuid::Uuid;

use super::*;

const OUT: &str = "ephemeral:site.bar.out";
const SLOW: &str = "brenn:site.bar.slow";
const NOTES: &str = "local:page/notes";
const GEOMETRY: &str = "brenn:site.surface.bar.geometry";
const STATUS: &str = "brenn:site.surface.bar.status";
const ERRORS: &str = "brenn:site.surface.bar.errors";

const NOW: Millis = Millis(1_000);

fn component(instance: &str, parked_batch_depth: u64) -> ComponentEntry {
    ComponentEntry {
        instance: instance.to_string(),
        kind: "protobar".to_string(),
        abi: Abi::Dom,
        parked_batch_depth,
        config: BTreeMap::new(),
    }
}

fn output(instance: &str, port: &str, channel: &str, urgency: Urgency) -> OutputBinding {
    OutputBinding {
        channel: channel.to_string(),
        instance: instance.to_string(),
        port: port.to_string(),
        urgency,
        fill_mt: 1_000,
        capacity_mt: 4_000,
    }
}

/// Chrome plus two components. `p1` writes a wire channel at `high` and a
/// page-local one; `p2` writes a durable channel at `low` and holds a
/// single-flush outbox, which is what makes an overflow reachable in one step.
fn doc(outputs: Vec<OutputBinding>, error: Option<(&str, LogLevel)>) -> BindingsDocument {
    BindingsDocument {
        v: BINDINGS_DOCUMENT_VERSION,
        components: vec![
            component("p1", 2),
            component("p2", 1),
            component("chrome", 4),
        ],
        subscriptions: Vec::new(),
        outputs,
        local_channels: vec![LocalChannel {
            channel: NOTES.to_string(),
            ring_depth: 4,
        }],
        chrome_instance: "chrome".to_string(),
        platform: PlatformSection {
            geometry_channel: GEOMETRY.to_string(),
            status_channel: STATUS.to_string(),
            status_interval_secs: 60,
            error_channel: error.map(|(channel, _)| channel.to_string()),
            error_report_floor: error.map(|(_, floor)| floor),
            takeover_granted: false,
        },
    }
}

fn standard_outputs() -> Vec<OutputBinding> {
    vec![
        output("p1", "out", OUT, Urgency::High),
        output("p1", "notes", NOTES, Urgency::Low),
        output("p2", "out", SLOW, Urgency::Low),
    ]
}

fn applied(outputs: Vec<OutputBinding>, error: Option<(&str, LogLevel)>) -> AppliedBindings {
    AppliedBindings::apply(&doc(outputs, error).to_body()).expect("the fixture document applies")
}

/// The standard wiring, reporting at and above `warn`.
fn wiring() -> AppliedBindings {
    applied(standard_outputs(), Some((ERRORS, LogLevel::Warn)))
}

/// The standard wiring with `instance` un-wired: its component entry and its
/// output bindings gone, as a document that stops declaring it leaves them.
fn un_wired(instance: &str) -> AppliedBindings {
    let mut shrunk = doc(
        standard_outputs()
            .into_iter()
            .filter(|o| o.instance != instance)
            .collect(),
        Some((ERRORS, LogLevel::Warn)),
    );
    shrunk.components.retain(|c| c.instance != instance);
    AppliedBindings::apply(&shrunk.to_body()).expect("the shrunk fixture document applies")
}

fn port_publish(instance: &str, port: &str, body: &str, correlation: u64) -> PortPublish {
    PortPublish {
        instance: instance.to_string(),
        port: port.to_string(),
        body: body.to_string(),
        urgency: None,
        correlation,
    }
}

/// The fields of a composed `Publish`, as a tuple the assertions read straight.
fn publish_parts(frame: &ClientFrame) -> (&str, Option<&str>, &str, Urgency, Option<u64>) {
    match frame {
        ClientFrame::Publish {
            channel,
            attribution,
            body,
            urgency,
            correlation,
        } => (
            channel.as_str(),
            attribution.as_deref(),
            body.as_str(),
            *urgency,
            *correlation,
        ),
        other => panic!("expected a Publish frame, got {other:?}"),
    }
}

fn batch_correlation(frame: &ClientFrame) -> u64 {
    match frame {
        ClientFrame::PublishBatch { correlation, .. } => *correlation,
        other => panic!("expected a PublishBatch frame, got {other:?}"),
    }
}

fn entry(channel: &str, body: &str) -> BatchEntry {
    BatchEntry {
        channel: channel.to_string(),
        body: body.to_string(),
        urgency: Urgency::Normal,
        deliver_after: None,
    }
}

fn flush_of(entries: Vec<BatchEntry>) -> FlushBatch {
    FlushBatch {
        entries,
        ops: Vec::new(),
    }
}

/// An attachment's transport contract. `max_body_bytes` is what a re-validated
/// flush's entries are measured against; `max_frame_bytes` is derived from it
/// exactly as the peer derives the read cap it advertises.
fn facts(max_body_bytes: u64) -> AttachmentFacts {
    AttachmentFacts {
        version: 1,
        participant_id: "surface:bar".to_string(),
        session_id: "sess-1".to_string(),
        heartbeat_secs: 20,
        max_body_bytes,
        max_frame_bytes: brenn_attach_proto::max_client_frame_bytes(max_body_bytes as usize) as u64,
        alert_granted: false,
    }
}

// --- port resolution -------------------------------------------------------

#[test]
fn a_port_resolves_to_its_channel_and_configured_urgency() {
    let bindings = wiring();
    let resolved = resolve_output(&bindings, "p1", "out", None).expect("the port is bound");
    assert_eq!(resolved.channel, OUT);
    assert_eq!(resolved.urgency, Urgency::High);
}

#[test]
fn a_confined_port_resolves_like_any_other() {
    let bindings = wiring();
    let resolved = resolve_output(&bindings, "p1", "notes", None).expect("the port is bound");
    assert_eq!(resolved.channel, NOTES);
    assert_eq!(resolved.urgency, Urgency::Low);
}

#[test]
fn a_callers_urgency_wins_over_the_ports_default() {
    let bindings = wiring();
    let resolved =
        resolve_output(&bindings, "p1", "out", Some(Urgency::VeryLow)).expect("the port is bound");
    assert_eq!(resolved.urgency, Urgency::VeryLow);
}

#[test]
fn an_unbound_port_and_an_unknown_instance_resolve_alike() {
    let bindings = wiring();
    assert!(resolve_output(&bindings, "p1", "nope", None).is_none());
    assert!(resolve_output(&bindings, "ghost", "out", None).is_none());
}

// --- single publishes ------------------------------------------------------

#[test]
fn a_port_publish_is_channel_addressed_and_attributed_to_its_instance() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    let frame = outbound
        .publish_port(&bindings, port_publish("p1", "out", "hello", 77))
        .expect("the port is bound");
    assert_eq!(
        publish_parts(&frame),
        (OUT, Some("p1"), "hello", Urgency::High, Some(0))
    );
}

#[test]
fn an_unbound_port_composes_no_frame_and_spends_no_correlation() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    assert!(
        outbound
            .publish_port(&bindings, port_publish("p1", "nope", "hello", 1))
            .is_none()
    );
    let frame = outbound
        .publish_port(&bindings, port_publish("p1", "out", "hello", 2))
        .expect("the port is bound");
    assert_eq!(publish_parts(&frame).4, Some(0));
}

#[test]
fn a_result_answers_the_callers_own_token() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    // Two publishes, so the wire correlation (0, 1) and the caller's (77, 78)
    // cannot be confused by coincidence.
    outbound
        .publish_port(&bindings, port_publish("p1", "out", "a", 77))
        .expect("the port is bound");
    outbound
        .publish_port(&bindings, port_publish("p2", "out", "b", 78))
        .expect("the port is bound");
    let answer = outbound
        .on_publish_result(Some(1), PublishOutcome::Ok)
        .expect("a correlation this attachment sent");
    assert_eq!(
        answer,
        Some(PublishAnswer::Port {
            instance: "p2".to_string(),
            port: "out".to_string(),
            correlation: 78,
            status: PublishStatus::Ok,
        })
    );
}

#[test]
fn every_wire_outcome_maps_to_a_caller_status() {
    let bindings = wiring();
    let cases = [
        (PublishOutcome::Ok, PublishStatus::Ok),
        (PublishOutcome::RateLimited, PublishStatus::RateLimited),
        (
            PublishOutcome::BodyTooLarge { len: 9, max: 4 },
            PublishStatus::BodyTooLarge { len: 9, max: 4 },
        ),
        (PublishOutcome::Failed, PublishStatus::Failed),
    ];
    for (outcome, expected) in cases {
        let mut outbound = SurfaceOutbound::new();
        outbound
            .publish_port(&bindings, port_publish("p1", "out", "a", 5))
            .expect("the port is bound");
        let answer = outbound
            .on_publish_result(Some(0), outcome)
            .expect("a correlation this attachment sent");
        let Some(PublishAnswer::Port { status, .. }) = answer else {
            panic!("expected a port answer, got {answer:?}");
        };
        assert_eq!(status, expected);
    }
}

#[test]
fn a_result_this_attachment_never_asked_for_is_unreconcilable() {
    let mut outbound = SurfaceOutbound::new();
    assert!(
        outbound
            .on_publish_result(Some(4), PublishOutcome::Ok)
            .is_err()
    );
    assert!(
        outbound
            .on_publish_result(None, PublishOutcome::Ok)
            .is_err()
    );
}

#[test]
fn a_lost_attachment_answers_only_the_callers_publishes() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound
        .publish_port(&bindings, port_publish("p1", "out", "a", 77))
        .expect("the port is bound");
    outbound.report(
        &bindings,
        ErrorReport {
            level: LogLevel::Error,
            source: "kernel",
            message: "boom",
            subject: None,
        },
    );
    outbound.publish_telemetry(&bindings, TelemetryKind::Status, "{}".to_string());
    outbound
        .publish_port(&bindings, port_publish("p2", "out", "b", 78))
        .expect("the port is bound");

    let answers = outbound.fail_pending();
    assert_eq!(
        answers,
        vec![
            PublishAnswer::Port {
                instance: "p1".to_string(),
                port: "out".to_string(),
                correlation: 77,
                status: PublishStatus::ConnectionLost,
            },
            PublishAnswer::Port {
                instance: "p2".to_string(),
                port: "out".to_string(),
                correlation: 78,
                status: PublishStatus::ConnectionLost,
            },
        ]
    );
    // Drained, not merely reported: a second attachment cannot be answered for
    // the first's publishes.
    assert!(outbound.fail_pending().is_empty());
}

// --- error reports ---------------------------------------------------------

#[test]
fn a_report_is_published_under_the_component_it_is_about() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    let frame = outbound
        .report(
            &bindings,
            ErrorReport {
                level: LogLevel::Error,
                source: "component:protobar",
                message: "it broke",
                subject: Some("p1"),
            },
        )
        .expect("the level clears the floor");
    let (channel, attribution, body, urgency, correlation) = publish_parts(&frame);
    assert_eq!(channel, ERRORS);
    assert_eq!(attribution, Some("p1"));
    assert_eq!(urgency, Urgency::Normal);
    assert_eq!(correlation, Some(0));
    let doc: ErrorReportDocument = serde_json::from_str(body).expect("the report body parses");
    assert_eq!(doc.source, "component:protobar");
    assert_eq!(doc.message, "it broke");
    assert_eq!(doc.level, LogLevel::Error);
}

#[test]
fn a_kernel_self_report_carries_the_bare_identity() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    let frame = outbound
        .report(
            &bindings,
            ErrorReport {
                level: LogLevel::Warn,
                source: "kernel",
                message: "a breadcrumb",
                subject: None,
            },
        )
        .expect("the level clears the floor");
    assert_eq!(publish_parts(&frame).1, None);
}

#[test]
fn a_report_below_the_floor_publishes_nothing() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    assert!(
        outbound
            .report(
                &bindings,
                ErrorReport {
                    level: LogLevel::Info,
                    source: "kernel",
                    message: "chatter",
                    subject: None,
                },
            )
            .is_none()
    );
}

#[test]
fn a_surface_with_no_error_channel_publishes_no_report() {
    let bindings = applied(standard_outputs(), None);
    let mut outbound = SurfaceOutbound::new();
    assert!(
        outbound
            .report(
                &bindings,
                ErrorReport {
                    level: LogLevel::Error,
                    source: "kernel",
                    message: "boom",
                    subject: None,
                },
            )
            .is_none()
    );
}

#[test]
fn an_over_long_report_is_truncated_to_the_schemas_caps() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    let frame = outbound
        .report(
            &bindings,
            ErrorReport {
                level: LogLevel::Error,
                source: &"s".repeat(MAX_LOG_SOURCE_BYTES * 2),
                message: &"m".repeat(MAX_LOG_MESSAGE_BYTES * 2),
                subject: None,
            },
        )
        .expect("the level clears the floor");
    let doc: ErrorReportDocument =
        serde_json::from_str(publish_parts(&frame).2).expect("the report body parses");
    assert_eq!(doc.source.len(), MAX_LOG_SOURCE_BYTES);
    assert_eq!(doc.message.len(), MAX_LOG_MESSAGE_BYTES);
    assert!(doc.message.ends_with("[truncated]"));
}

#[test]
fn a_reports_outcome_is_consumed_and_dropped() {
    let bindings = wiring();
    for outcome in [PublishOutcome::Ok, PublishOutcome::Failed] {
        let mut outbound = SurfaceOutbound::new();
        outbound
            .report(
                &bindings,
                ErrorReport {
                    level: LogLevel::Error,
                    source: "kernel",
                    message: "boom",
                    subject: None,
                },
            )
            .expect("the level clears the floor");
        assert_eq!(
            outbound
                .on_publish_result(Some(0), outcome)
                .expect("a correlation this attachment sent"),
            None
        );
    }
}

// --- the surface's own documents -------------------------------------------

#[test]
fn a_telemetry_document_is_unattributed_on_the_channel_the_wiring_names() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    let geometry =
        outbound.publish_telemetry(&bindings, TelemetryKind::Geometry, "{\"g\":1}".to_string());
    let status =
        outbound.publish_telemetry(&bindings, TelemetryKind::Status, "{\"s\":1}".to_string());
    assert_eq!(
        publish_parts(&geometry),
        (GEOMETRY, None, "{\"g\":1}", Urgency::Normal, Some(0))
    );
    assert_eq!(
        publish_parts(&status),
        (STATUS, None, "{\"s\":1}", Urgency::Normal, Some(1))
    );
}

#[test]
fn an_accepted_telemetry_document_is_owed_nobody() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.publish_telemetry(&bindings, TelemetryKind::Status, "{}".to_string());
    assert_eq!(
        outbound
            .on_publish_result(Some(0), PublishOutcome::Ok)
            .expect("a correlation this attachment sent"),
        None
    );
    assert_eq!(outbound.telemetry_dropped(), 0);
}

#[test]
fn a_metered_telemetry_document_is_counted_and_never_fatal() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.publish_telemetry(&bindings, TelemetryKind::Geometry, "{}".to_string());
    outbound.publish_telemetry(&bindings, TelemetryKind::Status, "{}".to_string());
    let first = outbound
        .on_publish_result(Some(0), PublishOutcome::RateLimited)
        .expect("a correlation this attachment sent");
    assert_eq!(
        first,
        Some(PublishAnswer::TelemetryDropped {
            kind: TelemetryKind::Geometry,
            outcome: PublishOutcome::RateLimited,
        })
    );
    outbound
        .on_publish_result(Some(1), PublishOutcome::BodyTooLarge { len: 9, max: 4 })
        .expect("a correlation this attachment sent");
    assert_eq!(outbound.telemetry_dropped(), 2);
}

#[test]
fn a_telemetry_document_refused_on_a_declared_channel_is_unreconcilable() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.publish_telemetry(&bindings, TelemetryKind::Status, "{}".to_string());
    assert!(
        outbound
            .on_publish_result(Some(0), PublishOutcome::Failed)
            .is_err()
    );
}

// --- outboxes --------------------------------------------------------------

#[test]
fn an_outbox_holds_what_its_component_entry_declares() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p2", &bindings);
    // Detached, so every flush queues — and `p2` holds one.
    outbound.flush("p2", flush_of(vec![entry(SLOW, "first")]), NOW);
    let steps = outbound.flush("p2", flush_of(vec![entry(SLOW, "second")]), NOW);
    assert_eq!(steps.dropped, vec!["p2".to_string()]);
    assert_eq!(outbound.dropped_count("p2"), 1);
    assert_eq!(outbound.rate_limited_count("p2"), 0);

    // `p1`'s entry declares two, so the two depths are distinguishable: a depth
    // read off anything but the component entry would collapse them.
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.flush("p1", flush_of(vec![entry(OUT, "first")]), NOW);
    let second = outbound.flush("p1", flush_of(vec![entry(OUT, "second")]), NOW);
    assert!(second.dropped.is_empty());
    assert_eq!(outbound.dropped_count("p1"), 0);
    let third = outbound.flush("p1", flush_of(vec![entry(OUT, "third")]), NOW);
    assert_eq!(third.dropped, vec!["p1".to_string()]);
    assert_eq!(outbound.dropped_count("p1"), 1);
}

#[test]
fn an_open_outbox_keeps_its_queue_when_a_document_redeclares_its_depth() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.flush("p1", flush_of(vec![entry(OUT, "queued")]), NOW);

    // A document that declares `p1` one flush deep rather than two. Re-opening the
    // outbox to pick that up would discard exactly the queue the depth governs.
    let mut shallower = doc(standard_outputs(), Some((ERRORS, LogLevel::Warn)));
    for component in &mut shallower.components {
        if component.instance == "p1" {
            component.parked_batch_depth = 1;
        }
    }
    let shallower = AppliedBindings::apply(&shallower.to_body())
        .expect("the shallower fixture document applies");
    let lost = outbound.reconcile(&shallower, ["p1"].into_iter());
    assert!(lost.is_empty(), "the queued flush is not thrown away");

    let steps = outbound.on_attached(&shallower, &facts(1_024), NOW);
    assert!(steps.dropped.is_empty());
    assert_eq!(steps.frames.len(), 1, "it goes out at the next attachment");
}

#[test]
#[should_panic(expected = "no component entry for registered instance")]
fn registering_an_undeclared_instance_panics() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("ghost", &bindings);
}

#[test]
fn reconcile_opens_an_outbox_for_a_registration_made_before_the_first_document() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    assert!(!outbound.is_registered("p1"));
    let lost = outbound.reconcile(&bindings, ["p1", "p2"].into_iter());
    assert!(lost.is_empty());
    assert!(outbound.is_registered("p1"));
    assert!(outbound.is_registered("p2"));
}

/// The registration table keeps an instance a new document stops declaring —
/// un-wired is not deregistered — so its outbox stays open with it, and the two
/// tables cannot disagree about whether it has one.
#[test]
fn an_un_wired_instance_keeps_its_outbox_until_it_deregisters() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.flush("p1", flush_of(vec![entry(OUT, "queued")]), NOW);

    // The instance is still registered, but a new document no longer declares
    // its component.
    let lost = outbound.reconcile(&un_wired("p1"), ["p1", "p2"].into_iter());
    assert!(lost.is_empty(), "nothing died with the un-wiring");
    assert!(outbound.is_registered("p1"));
    assert!(
        outbound.is_registered("p2"),
        "and a registration made before the first document still opens one"
    );

    // A completing activation inside the window before the page reloads finds its
    // outbox, and what it queues is dropped at the next attachment because the
    // wiring no longer admits the channel it names.
    outbound.flush("p1", flush_of(vec![entry(OUT, "in flight")]), NOW);
    let steps = outbound.on_attached(&un_wired("p1"), &facts(1_024), NOW);
    assert_eq!(
        steps.dropped,
        vec!["p1".to_string(), "p1".to_string()],
        "both queued flushes name a channel the new wiring drops"
    );

    // And the unmount that follows the reload answers cleanly.
    assert!(outbound.deregister("p1").is_empty());
    assert!(!outbound.is_registered("p1"));
}

/// An instance registered before the page's first document that the document does
/// not declare never gets an outbox — and its unmount is still total.
#[test]
fn deregistering_an_instance_that_never_held_an_outbox_answers_nothing() {
    let mut outbound = SurfaceOutbound::new();
    outbound.reconcile(&un_wired("p1"), ["p1"].into_iter());
    assert!(!outbound.is_registered("p1"));
    assert!(outbound.deregister("p1").is_empty());
}

#[test]
fn reconcile_closes_the_outbox_of_an_instance_that_deregistered() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.register("p2", &bindings);
    outbound.reconcile(&bindings, ["p2"].into_iter());
    assert!(!outbound.is_registered("p1"));
    assert!(outbound.is_registered("p2"));
}

#[test]
fn a_queued_flush_goes_out_at_the_next_attachment() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    let queued = outbound.flush("p1", flush_of(vec![entry(OUT, "queued")]), NOW);
    assert!(queued.frames.is_empty());

    let steps = outbound.on_attached(&bindings, &facts(1_024), NOW);
    assert!(steps.dropped.is_empty());
    assert_eq!(steps.frames.len(), 1);
    let correlation = batch_correlation(&steps.frames[0]);
    outbound
        .on_batch_result(correlation, PublishBatchOutcome::Ok, NOW)
        .expect("a correlation this attachment sent");
}

#[test]
fn a_flush_naming_a_channel_the_new_wiring_drops_is_dropped_whole() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.flush(
        "p1",
        flush_of(vec![entry(OUT, "keep"), entry(SLOW, "not mine")]),
        NOW,
    );
    let steps = outbound.on_attached(&bindings, &facts(1_024), NOW);
    assert_eq!(steps.dropped, vec!["p1".to_string()]);
    assert!(steps.frames.is_empty());
}

#[test]
fn a_flush_over_the_new_body_cap_is_dropped_whole() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.flush("p1", flush_of(vec![entry(OUT, "0123456789")]), NOW);
    let steps = outbound.on_attached(&bindings, &facts(4), NOW);
    assert_eq!(steps.dropped, vec!["p1".to_string()]);
    assert!(steps.frames.is_empty());
}

#[test]
fn a_flush_whose_composed_frame_is_over_the_new_frame_cap_is_dropped_whole() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    // Every entry clears the body cap on its own; the composed batch does not
    // clear the frame cap the same contract derives.
    let wide = flush_of(vec![
        entry(OUT, &"a".repeat(200)),
        entry(OUT, &"b".repeat(200)),
    ]);
    let mut shrunk = facts(256);
    shrunk.max_frame_bytes = 300;
    let steps = outbound.flush("p1", wide, NOW);
    assert!(steps.frames.is_empty());
    let steps = outbound.on_attached(&bindings, &shrunk, NOW);
    assert_eq!(steps.dropped, vec!["p1".to_string()]);
    assert!(steps.frames.is_empty());
}

/// The op half of the re-validation has to admit a legitimate op, not only refuse
/// a bad one: an inverted check would drop every flush that carried a deferral
/// control, entries and all, at every reconnect.
#[test]
fn a_flushs_legitimate_control_ops_survive_re_validation() {
    let bindings = wiring();
    let batch = FlushBatch {
        entries: vec![entry(OUT, "keep")],
        ops: vec![
            batch_op(
                &bindings,
                "p1",
                "out",
                Uuid::from_u128(1),
                DeferredOpKind::Cancel,
            )
            .expect("the port is bound"),
            batch_op(
                &bindings,
                "p1",
                "out",
                Uuid::from_u128(2),
                DeferredOpKind::Edit {
                    body: None,
                    deliver_after: Some(9_000),
                },
            )
            .expect("the port is bound"),
        ],
    };
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.flush("p1", batch.clone(), NOW);

    let steps = outbound.on_attached(&bindings, &facts(1_024), NOW);
    assert!(steps.dropped.is_empty());
    match &steps.frames[..] {
        [
            ClientFrame::PublishBatch {
                publishes,
                deferred_ops,
                ..
            },
        ] => {
            assert_eq!(publishes, &batch.entries);
            assert_eq!(deferred_ops, &batch.ops);
        }
        other => panic!("expected one PublishBatch frame, got {other:?}"),
    }
}

#[test]
fn a_flushs_control_ops_are_re_checked_like_its_entries() {
    let bindings = wiring();
    let over_cap = FlushBatch {
        entries: Vec::new(),
        ops: vec![
            batch_op(
                &bindings,
                "p1",
                "out",
                Uuid::from_u128(1),
                DeferredOpKind::Edit {
                    body: Some("0123456789".to_string()),
                    deliver_after: None,
                },
            )
            .expect("the port is bound"),
        ],
    };
    let unbound_channel = FlushBatch {
        entries: Vec::new(),
        ops: vec![
            batch_op(
                &bindings,
                "p2",
                "out",
                Uuid::from_u128(2),
                DeferredOpKind::Cancel,
            )
            .expect("the port is bound"),
        ],
    };
    for batch in [over_cap, unbound_channel] {
        let mut outbound = SurfaceOutbound::new();
        outbound.register("p1", &bindings);
        outbound.flush("p1", batch, NOW);
        let steps = outbound.on_attached(&bindings, &facts(4), NOW);
        assert_eq!(steps.dropped, vec!["p1".to_string()]);
    }
}

#[test]
fn a_detach_leaves_the_queue_and_frees_the_wire() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.on_attached(&bindings, &facts(1_024), NOW);
    outbound.flush("p1", flush_of(vec![entry(OUT, "sent")]), NOW);
    outbound.on_detached();
    // The unanswered flush died with its attachment; the next one queues rather
    // than waiting behind it.
    outbound.flush("p1", flush_of(vec![entry(OUT, "queued")]), NOW);
    let steps = outbound.on_attached(&bindings, &facts(1_024), NOW);
    assert_eq!(steps.frames.len(), 1);
    assert!(steps.dropped.is_empty());
}

#[test]
fn a_metered_flush_is_re_parked_and_re_offered_on_the_tick() {
    let bindings = wiring();
    let mut outbound = SurfaceOutbound::new();
    outbound.register("p1", &bindings);
    outbound.on_attached(&bindings, &facts(1_024), NOW);
    let sent = outbound.flush("p1", flush_of(vec![entry(OUT, "sent")]), NOW);
    let correlation = batch_correlation(&sent.frames[0]);
    outbound
        .on_batch_result(correlation, PublishBatchOutcome::RateLimited, NOW)
        .expect("a correlation this attachment sent");
    assert_eq!(outbound.rate_limited_count("p1"), 1);
    let retried = outbound.on_retry_tick(NOW);
    assert_eq!(retried.frames.len(), 1);
}

// --- batch composition -----------------------------------------------------

#[test]
fn a_batch_entry_carries_the_resolved_channel_urgency_and_release_time() {
    let bindings = wiring();
    let composed = batch_entry(
        &bindings,
        "p1",
        "out",
        "body".to_string(),
        None,
        Some(4_000),
    )
    .expect("the port is bound");
    assert_eq!(
        composed,
        BatchEntry {
            channel: OUT.to_string(),
            body: "body".to_string(),
            urgency: Urgency::High,
            deliver_after: Some(4_000),
        }
    );
    let overridden = batch_entry(
        &bindings,
        "p1",
        "out",
        "body".to_string(),
        Some(Urgency::VeryLow),
        None,
    )
    .expect("the port is bound");
    assert_eq!(overridden.urgency, Urgency::VeryLow);
    assert_eq!(overridden.deliver_after, None);
}

#[test]
fn a_batch_op_addresses_its_ports_channel() {
    let bindings = wiring();
    let composed = batch_op(
        &bindings,
        "p2",
        "out",
        Uuid::from_u128(7),
        DeferredOpKind::Cancel,
    )
    .expect("the port is bound");
    assert_eq!(composed.channel, SLOW);
    assert_eq!(composed.message_id, Uuid::from_u128(7));
    assert_eq!(composed.op, DeferredOpKind::Cancel);
}

#[test]
fn an_unbound_port_composes_neither_an_entry_nor_an_op() {
    let bindings = wiring();
    assert!(batch_entry(&bindings, "p1", "nope", "b".to_string(), None, None).is_none());
    assert!(
        batch_op(
            &bindings,
            "p1",
            "nope",
            Uuid::from_u128(1),
            DeferredOpKind::Cancel,
        )
        .is_none()
    );
}

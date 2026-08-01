//! The front door, against a real page and the receiving ends of its own
//! channels.
//!
//! Two things are observable here and these tests read both: what the gate
//! answers a caller synchronously, and what lands in the channel the layer that
//! owns the page will serve. Nothing here runs a page — that is the runner's
//! suite — so the gate is refreshed from a page a fixture assembled, which is
//! exactly the read the runner performs.

use brenn_attach_client::Millis;
use brenn_attach_client::conn::AttachmentFacts;
use brenn_surface_schema::InstanceState;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::telemetry::{InstanceReport, StatusCounters};
use uuid::Uuid;

use crate::test_support::bindings as fixtures;
use crate::test_support::pages;

use super::*;

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE_OUT: &str = "brenn:site.bar.out";
const ERRORS: &str = "brenn:site.bar.errors";
const NOTES: &str = "local:app/notes";
const EPOCH: Uuid = Uuid::from_u128(0x1_11f0);
const NOW: Millis = Millis(1_000);

/// `p1` writes both classes: one transportable port and one confined one, which
/// is the whole of what the gate distinguishes.
fn doc() -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![],
        vec![
            fixtures::output("p1", "out", WIRE_OUT),
            fixtures::output("p1", "notes", NOTES),
        ],
        vec![fixtures::local(NOTES, 4)],
    )
}

/// As [`doc`], stating an error channel and the floor a report must clear.
fn doc_reporting(floor: LogLevel) -> BindingsDocument {
    let mut doc = doc();
    doc.platform.error_channel = Some(ERRORS.to_string());
    doc.platform.error_report_floor = Some(floor);
    doc
}

/// A page with `doc` in force, `p1` mounted.
fn configured(doc: &BindingsDocument) -> SurfacePage {
    pages::configured_page(CONFIG, EPOCH, pages::facts(), &["p1"], doc, NOW)
}

/// A gate refreshed from a page — the read the layer that owns the page makes.
fn gate_over(page: &SurfacePage) -> SurfaceGate {
    let mut gate = SurfaceGate::default();
    gate.refresh(page);
    gate
}

/// The front door, plus the receiving ends a test inspects instead of a runner.
struct Front {
    handle: SurfaceHandle,
    channels: FrontChannels,
    /// Held rather than dropped: dropping it is how a platform half says it has
    /// left, which is the runner's question and not this suite's.
    _events: EventStream,
}

fn front() -> Front {
    let (handle, events, channels) = new();
    Front {
        handle,
        channels,
        _events: events,
    }
}

impl Front {
    /// Refresh the shared gate from `page`, as the layer that owns it does.
    fn refresh(&self, page: &SurfacePage) {
        self.channels
            .gate
            .lock()
            .expect("the gate mutex is not poisoned")
            .refresh(page);
    }

    fn next_publish(&mut self) -> Option<PublishSlot> {
        self.channels.publish_rx.try_recv().ok()
    }

    fn next_alert(&mut self) -> Option<AlertCommand> {
        self.channels.alert_rx.try_recv().ok()
    }

    fn next_telemetry(&mut self) -> Option<TelemetryCommand> {
        self.channels.telemetry_rx.try_recv().ok()
    }

    fn next_control(&mut self) -> Option<RunnerCommand> {
        self.channels.control_rx.try_recv().ok()
    }
}

// ── the gate ──────────────────────────────────────────────────────────────

#[test]
fn a_fresh_gate_refuses_every_publish() {
    // No page has been seen: nothing is bound, nothing is configured, and the cap
    // is zero. Reachability is asked first, so that is the answer.
    let gate = SurfaceGate::default();
    assert_eq!(
        gate.check("p1", "out", "hi"),
        Err(PublishReject::NotConnected)
    );
    assert_eq!(gate.error_report_floor(), None);
}

#[test]
fn a_configured_page_admits_a_bound_port_up_to_the_cap() {
    let page = configured(&doc());
    let gate = gate_over(&page);
    assert_eq!(gate.check("p1", "out", "hi"), Ok(()));
    // The cap is the attachment's, and a body exactly at it is admitted.
    let at_cap = "x".repeat(pages::BODY_CAP as usize);
    assert_eq!(gate.check("p1", "out", &at_cap), Ok(()));
}

#[test]
fn an_unbound_pair_is_refused_whichever_half_is_wrong() {
    let page = configured(&doc());
    let gate = gate_over(&page);
    assert_eq!(
        gate.check("p1", "nope", "hi"),
        Err(PublishReject::UnboundPort)
    );
    assert_eq!(
        gate.check("p2", "out", "hi"),
        Err(PublishReject::UnboundPort)
    );
}

#[test]
fn an_oversized_body_names_its_length_and_the_cap() {
    let page = configured(&doc());
    let gate = gate_over(&page);
    let over = "x".repeat(pages::BODY_CAP as usize + 1);
    assert_eq!(
        gate.check("p1", "out", &over),
        Err(PublishReject::BodyTooLarge {
            len: pages::BODY_CAP + 1,
            max: pages::BODY_CAP,
        })
    );
}

#[test]
fn a_detached_page_still_admits_a_confined_port_but_not_its_wire_sibling() {
    // The class is the whole distinction: confined traffic never touches the wire,
    // so refusing it while the link is down would defeat the offline correctness
    // of the class before the page's router ever saw it.
    let mut page = configured(&doc());
    page.on_detached();
    let gate = gate_over(&page);
    assert_eq!(gate.check("p1", "notes", "hi"), Ok(()));
    assert_eq!(
        gate.check("p1", "out", "hi"),
        Err(PublishReject::NotConnected)
    );
}

#[test]
fn a_confined_publish_is_still_bound_and_size_checked_while_detached() {
    // Reachability is the only predicate confinement relaxes.
    let mut page = configured(&doc());
    page.on_detached();
    let gate = gate_over(&page);
    let over = "x".repeat(pages::BODY_CAP as usize + 1);
    assert_eq!(
        gate.check("p1", "notes", &over),
        Err(PublishReject::BodyTooLarge {
            len: pages::BODY_CAP + 1,
            max: pages::BODY_CAP,
        })
    );
    assert_eq!(
        gate.check("p1", "nope", "hi"),
        Err(PublishReject::NotConnected)
    );
}

#[test]
fn an_attachment_without_its_document_publishes_nothing_on_the_wire() {
    // The reconnect window: phase 1 has happened, so there is an attachment, but
    // the document the peer is judging against has not arrived. The previous
    // document's wiring is still in force — which is what keeps the confined port
    // usable — and a wire publish composed out of it is exactly what must not go.
    let mut page = configured(&doc());
    page.on_detached();
    page.on_attached(pages::facts());
    let gate = gate_over(&page);
    assert_eq!(gate.check("p1", "notes", "hi"), Ok(()));
    assert_eq!(
        gate.check("p1", "out", "hi"),
        Err(PublishReject::NotConnected)
    );
}

#[test]
fn the_cap_a_reattachment_states_replaces_the_last_ones() {
    let mut page = configured(&doc());
    page.on_detached();
    page.on_attached(AttachmentFacts {
        max_body_bytes: 4,
        ..pages::facts()
    });
    let gate = gate_over(&page);
    assert_eq!(
        gate.check("p1", "notes", "hello"),
        Err(PublishReject::BodyTooLarge { len: 5, max: 4 })
    );
}

#[test]
fn the_report_floor_is_the_wirings() {
    // A surface that declares no error channel states no floor, and the handle
    // reads exactly that to keep a report off the publish channel entirely.
    let page = configured(&doc());
    assert_eq!(gate_over(&page).error_report_floor(), None);
    let page = configured(&doc_reporting(LogLevel::Warn));
    assert_eq!(gate_over(&page).error_report_floor(), Some(LogLevel::Warn));
}

#[test]
fn a_page_with_no_wiring_binds_nothing_and_states_no_floor() {
    // A refresh against a page before its first document must not leave a previous
    // page's table standing — the gate is a snapshot, not an accumulator.
    let mut gate = gate_over(&configured(&doc_reporting(LogLevel::Warn)));
    gate.refresh(&SurfacePage::new(CONFIG.to_string(), EPOCH));
    assert_eq!(gate.error_report_floor(), None);
    assert_eq!(
        gate.check("p1", "notes", "hi"),
        Err(PublishReject::NotConnected)
    );
}

// ── the handle ────────────────────────────────────────────────────────────

#[test]
fn a_publish_carries_its_fields_and_a_correlation_of_its_own() {
    let mut front = front();
    front.refresh(&configured(&doc()));
    assert_eq!(front.handle.publish("p1", "out", "one".into()), Ok(0));
    assert_eq!(
        front
            .handle
            .publish_with_urgency("p1", "out", "two".into(), Urgency::High),
        Ok(1)
    );
    let Some(PublishSlot::Publish(first)) = front.next_publish() else {
        panic!("the first publish is queued")
    };
    assert_eq!(first.correlation, 0);
    assert_eq!(first.instance, "p1");
    assert_eq!(first.port, "out");
    assert_eq!(first.body, "one");
    // No override: the port's configured urgency is the page's to resolve.
    assert_eq!(first.urgency, None);
    let Some(PublishSlot::Publish(second)) = front.next_publish() else {
        panic!("the second publish is queued")
    };
    assert_eq!(second.correlation, 1);
    assert_eq!(second.urgency, Some(Urgency::High));
}

#[test]
fn a_refused_publish_queues_nothing_and_spends_no_correlation() {
    let mut front = front();
    front.refresh(&configured(&doc()));
    assert_eq!(
        front.handle.publish("p1", "nope", "hi".into()),
        Err(PublishReject::UnboundPort)
    );
    assert!(front.next_publish().is_none());
    // The refusal cost the caller nothing, so the next admitted publish takes the
    // first correlation.
    assert_eq!(front.handle.publish("p1", "out", "hi".into()), Ok(0));
}

#[test]
fn a_full_publish_channel_answers_busy_and_leaves_the_page_alone() {
    let mut front = front();
    front.refresh(&configured(&doc()));
    // Fill it: one component out-running its own publishes is contained, not
    // fatal, which is the whole difference from the control plane.
    let mut queued = 0;
    loop {
        match front.handle.publish("p1", "out", "hi".into()) {
            Ok(_) => queued += 1,
            Err(PublishReject::Busy) => break,
            Err(other) => panic!("unexpected refusal {other:?}"),
        }
        assert!(queued <= PUBLISH_CHANNEL_CAPACITY + 1, "the bound holds");
    }
    assert!(queued >= PUBLISH_CHANNEL_CAPACITY);
    assert!(front.next_publish().is_some());
    assert!(front.handle.publish("p1", "out", "hi".into()).is_ok());
}

#[test]
fn a_report_is_queued_only_when_it_clears_the_wirings_floor() {
    let mut front = front();
    front.refresh(&configured(&doc_reporting(LogLevel::Warn)));
    front.handle.report(LogLevel::Info, "src", "under", None);
    assert!(front.next_publish().is_none());
    front
        .handle
        .report(LogLevel::Error, "src", "over", Some("p1"));
    let Some(PublishSlot::Report(report)) = front.next_publish() else {
        panic!("the report is queued")
    };
    assert_eq!(report.level, LogLevel::Error);
    assert_eq!(report.source, "src");
    assert_eq!(report.message, "over");
    assert_eq!(report.subject.as_deref(), Some("p1"));
}

#[test]
fn a_surface_that_declares_no_error_channel_queues_no_report() {
    let mut front = front();
    front.refresh(&configured(&doc()));
    front.handle.report(LogLevel::Error, "src", "loud", None);
    assert!(front.next_publish().is_none());
}

#[test]
fn an_alert_rides_its_own_channel_and_is_dropped_when_it_is_full() {
    let mut front = front();
    front.handle.alert(AlertSeverity::Warning, "t", "b");
    let alert = front.next_alert().expect("the alert is queued");
    assert_eq!(alert.severity, AlertSeverity::Warning);
    assert_eq!(alert.title, "t");
    assert_eq!(alert.body, "b");
    // Best-effort: a component alert-loop can neither panic the page nor block.
    for _ in 0..ALERT_CHANNEL_CAPACITY * 4 {
        front.handle.alert(AlertSeverity::Critical, "t", "b");
    }
    let mut drained = 0;
    while front.next_alert().is_some() {
        drained += 1;
    }
    assert!(drained <= ALERT_CHANNEL_CAPACITY + 1);
}

#[test]
fn both_telemetry_documents_ride_the_telemetry_channel() {
    let mut front = front();
    front.handle.send_geometry(1280, 720, 2.0);
    let Some(TelemetryCommand::Geometry {
        width,
        height,
        device_pixel_ratio,
    }) = front.next_telemetry()
    else {
        panic!("the viewport is queued")
    };
    assert_eq!((width, height), (1280, 720));
    assert_eq!(device_pixel_ratio.as_f64(), Some(2.0));
    front.handle.send_status(
        vec![InstanceReport {
            instance: "p1".into(),
            kind: "protobar".into(),
            state: InstanceState::Mounted,
            ports_attached: 0,
            reason: None,
        }],
        7,
        StatusCounters::default(),
    );
    let Some(TelemetryCommand::Status {
        instances,
        uptime_secs,
        ..
    }) = front.next_telemetry()
    else {
        panic!("the status snapshot is queued")
    };
    assert_eq!(instances.len(), 1);
    assert_eq!(uptime_secs, 7);
}

#[test]
fn a_full_telemetry_channel_drops_the_document_it_cannot_hold() {
    // Best-effort, like the alert plane: a resize storm out-running the page
    // neither panics it nor blocks the observer, and the document is latest-wins
    // so the next tick states a fresh one.
    let mut front = front();
    for _ in 0..TELEMETRY_CHANNEL_CAPACITY * 4 {
        front.handle.send_geometry(1280, 720, 2.0);
    }
    let mut drained = 0;
    while front.next_telemetry().is_some() {
        drained += 1;
    }
    assert!(drained <= TELEMETRY_CHANNEL_CAPACITY + 1);
}

#[test]
fn the_best_effort_planes_are_silent_once_the_run_is_over() {
    // The closed half of the contract, and the one the browser actually reaches:
    // the resize observer and the status tick are page-lifetime listeners that
    // outlive the run, so every one of them lands on a channel with no receiver.
    // The control plane panics there and these two must not — the difference is
    // the whole reason they are separate channels.
    let mut front = front();
    front.channels.alert_rx.close();
    front.channels.telemetry_rx.close();
    front.handle.alert(AlertSeverity::Warning, "t", "b");
    front.handle.send_geometry(1280, 720, 2.0);
    front
        .handle
        .send_status(Vec::new(), 1, StatusCounters::default());
    assert!(front.next_alert().is_none());
    assert!(front.next_telemetry().is_none());
}

#[test]
fn a_device_pixel_ratio_that_is_not_a_number_never_reaches_the_page() {
    let mut front = front();
    front.handle.send_geometry(1280, 720, f64::NAN);
    front.handle.send_geometry(1280, 720, f64::INFINITY);
    assert!(front.next_telemetry().is_none());
}

#[test]
fn the_lifecycle_commands_ride_the_control_channel() {
    let mut front = front();
    front.handle.publish_control(NOTES, "{}".into());
    front.handle.deregister_activation("p1");
    front.handle.close();
    assert!(matches!(
        front.next_control(),
        Some(RunnerCommand::PublishControl { channel, .. }) if channel == NOTES
    ));
    assert!(matches!(
        front.next_control(),
        Some(RunnerCommand::DeregisterActivation { instance }) if instance == "p1"
    ));
    assert!(matches!(front.next_control(), Some(RunnerCommand::Close)));
}

#[test]
#[should_panic(expected = "the control channel is full")]
fn a_full_control_channel_is_a_kernel_bug() {
    let front = front();
    for _ in 0..CONTROL_CHANNEL_CAPACITY + 2 {
        front.handle.close();
    }
}

#[test]
#[should_panic(expected = "the run is over")]
fn a_control_command_after_the_run_is_over_panics() {
    let mut front = front();
    front.channels.control_rx.close();
    front.handle.close();
}

#[test]
#[should_panic(expected = "the run is over")]
fn a_publish_after_the_run_is_over_panics() {
    // Unlike the best-effort planes, a publish has a caller waiting on an answer
    // that can never come.
    let mut front = front();
    front.refresh(&configured(&doc()));
    front.channels.publish_rx.close();
    let _ = front.handle.publish("p1", "out", "hi".into());
}

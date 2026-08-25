//! What the platform half asks for, driven against a real [`SurfacePage`] — real
//! stores, a real subscription plane, a real confined router carrying the
//! surface's own plane policy and real outboxes — so a confined publish really
//! reaches a reader and a composed frame is the one that would go on the wire.

use brenn_attach_client::Millis;
use brenn_attach_client::conn::AttachmentFacts;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::telemetry::{GeometryDocument, Health, StatusDocument};
use brenn_surface_schema::{
    Binding, CONTROL_PLANE_VERSION, InstanceState, LOCAL_LINK_STATE_CHANNEL,
    LOCAL_OVERLAY_STATE_CHANNEL, NoiseLevel, OverlayStateBody, Urgency as SchemaUrgency,
};
use uuid::Uuid;

use crate::registry::BindingKey;
use crate::test_support::bindings as fixtures;
use crate::test_support::bindings::{output, output_at};
use crate::test_support::pages;

use super::*;

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE: &str = "ephemeral:site.bar.out";
const NOTES: &str = "local:app/notes";
const ERRORS: &str = "ephemeral:site.bar.errors";
const GEOMETRY: &str = "brenn:site.surface.bar.geometry";
const STATUS: &str = "brenn:site.surface.bar.status";
const EPOCH: Uuid = Uuid::from_u128(0x0c1d);
const NOW: Millis = Millis(1_000);

/// The standard wiring: `p1` writes one channel of each class plus the overlay
/// plane it has no business writing; `p2` reads the page-local channel and the
/// kernel's link-state plane. `NOTES` is declared at ring depth 1, which is what
/// makes a second publish evict what the first one left unread.
fn doc(reader_noise: NoiseLevel) -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component("p2"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![
            Binding {
                noise: reader_noise,
                ..fixtures::subscription("p2", "notes", NOTES, 1, 0)
            },
            fixtures::subscription("p2", "link", LOCAL_LINK_STATE_CHANNEL, 1, 1),
        ],
        vec![
            output_at("p1", "out", WIRE, SchemaUrgency::High),
            output("p1", "notes", NOTES),
            output("p1", "over", LOCAL_OVERLAY_STATE_CHANNEL),
            output(fixtures::CHROME, "over", LOCAL_OVERLAY_STATE_CHANNEL),
        ],
        vec![
            fixtures::local(NOTES, 1),
            fixtures::local(LOCAL_OVERLAY_STATE_CHANNEL, 1),
            fixtures::local(LOCAL_LINK_STATE_CHANNEL, 1),
        ],
    )
}

/// The standard wiring plus an error channel at the `warn` floor.
fn reporting_doc() -> BindingsDocument {
    let mut document = doc(NoiseLevel::Metered);
    document.platform.error_channel = Some(ERRORS.to_string());
    document.platform.error_report_floor = Some(LogLevel::Warn);
    document
}

/// A configured page under `facts`, with `document` in force and the whole cast
/// mounted.
fn page_with(facts: AttachmentFacts, document: &BindingsDocument) -> SurfacePage {
    pages::configured_page(
        CONFIG,
        EPOCH,
        facts,
        &["p1", "p2", fixtures::CHROME],
        document,
        NOW,
    )
}

/// The standard page: attached without an alert grant, the standard wiring in
/// force, every loss on the books but none of it loud.
fn page() -> SurfacePage {
    page_with(pages::facts(), &doc(NoiseLevel::Metered))
}

/// The standard page mid-reconnect: the attachment it was configured under went
/// away, a fresh one is live, and its document has not arrived. The old wiring is
/// still held — that is what the new document is compared against — and it is not
/// what the new peer is judging frames against.
fn reattached() -> SurfacePage {
    let mut page = page();
    page.on_detached();
    page.on_attached(pages::facts());
    page
}

fn stamp(seed: u128) -> MessageStamp {
    MessageStamp {
        message_id: Uuid::from_u128(seed),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
    }
}

fn port_publish(instance: &str, port: &str, body: &str) -> PortPublish {
    PortPublish {
        instance: instance.to_string(),
        port: port.to_string(),
        body: body.to_string(),
        urgency: None,
        correlation: 7,
    }
}

/// One publish command under a shared envelope identity — every fixture publish
/// that is not routed twice onto the same confined channel.
fn publish_cmd(instance: &str, port: &str, body: &str) -> Command {
    publish_stamped(instance, port, body, 0xbeef)
}

/// As [`publish_cmd`], for a test that routes two of them onto one channel: a
/// confined store dedups by message id, so a second publish under the first's
/// identity is read as a re-presentation rather than as new.
fn publish_stamped(instance: &str, port: &str, body: &str, seed: u128) -> Command {
    Command::Publish {
        publish: port_publish(instance, port, body),
        stamp: stamp(seed),
    }
}

/// A `Publish` frame's channel, attribution, body and urgency.
fn publish_parts(frame: &ClientFrame) -> (&str, Option<&str>, &str, Urgency) {
    match frame {
        ClientFrame::Publish {
            channel,
            attribution,
            body,
            urgency,
            ..
        } => (channel, attribution.as_deref(), body, *urgency),
        other => panic!("expected a Publish, got {other:?}"),
    }
}

fn one_frame(outcome: &CommandOutcome) -> &ClientFrame {
    match outcome.frames.as_slice() {
        [frame] => frame,
        frames => panic!("expected exactly one frame, got {frames:?}"),
    }
}

fn one_answer(outcome: &CommandOutcome) -> &PublishAnswer {
    match outcome.answers.as_slice() {
        [answer] => answer,
        answers => panic!("expected exactly one answer, got {answers:?}"),
    }
}

fn status(outcome: &CommandOutcome) -> PublishStatus {
    match one_answer(outcome) {
        PublishAnswer::Port { status, .. } => *status,
        other => panic!("expected a port answer, got {other:?}"),
    }
}

/// What `channel`'s store retains, oldest first.
fn retained(page: &SurfacePage, channel: &str) -> Vec<String> {
    page.stores
        .get(channel)
        .expect("the fixture hosts the channel")
        .retained()
        .map(|(envelope, _)| envelope.body.clone())
        .collect()
}

/// One overlay-plane body naming its holder.
fn overlay_body(holder: Option<&str>) -> String {
    serde_json::to_string(&OverlayStateBody {
        v: CONTROL_PLANE_VERSION,
        holder: holder.map(str::to_owned),
        since_stamp: 0,
    })
    .expect("an overlay body serializes")
}

#[test]
fn a_transportable_port_composes_a_frame_and_awaits_the_peers_answer() {
    let mut page = page();
    let outcome = on_command(&mut page, publish_cmd("p1", "out", "hello"));
    assert_eq!(
        publish_parts(one_frame(&outcome)),
        (WIRE, Some("p1"), "hello", Urgency::High)
    );
    assert!(outcome.answers.is_empty());
}

#[test]
fn a_port_the_wiring_does_not_bind_is_refused_without_a_frame() {
    let mut page = page();
    let outcome = on_command(&mut page, publish_cmd("p1", "nowhere", "hello"));
    assert!(outcome.frames.is_empty());
    assert_eq!(status(&outcome), PublishStatus::UnboundPort);
}

#[test]
fn a_refused_publish_spends_no_wire_correlation() {
    let mut page = page();
    let refused = on_command(&mut page, publish_cmd("p1", "nowhere", "hello"));
    assert!(refused.frames.is_empty());
    let outcome = on_command(&mut page, publish_cmd("p1", "out", "hello"));
    let ClientFrame::Publish { correlation, .. } = one_frame(&outcome) else {
        panic!("a bound port composes a Publish frame");
    };
    assert_eq!(*correlation, Some(0));
}

#[test]
fn a_publish_before_the_first_document_is_not_connected() {
    let mut page = pages::attached_page(CONFIG, EPOCH, pages::facts());
    let outcome = on_command(&mut page, publish_cmd("p1", "out", "hello"));
    assert_eq!(status(&outcome), PublishStatus::NotConnected);
}

#[test]
fn a_transportable_publish_with_no_attachment_is_not_connected() {
    let mut page = page();
    page.on_detached();
    let outcome = on_command(&mut page, publish_cmd("p1", "out", "hello"));
    assert!(outcome.frames.is_empty());
    assert_eq!(status(&outcome), PublishStatus::NotConnected);
}

/// The window between phase 1 and phase 2 of a *reconnect*: the page is attached
/// and still holding the previous attachment's wiring, and the peer on the other
/// end of the new socket answers a channel outside the sender's set with a
/// protocol close and a fail2ban strike against a legitimate user. So a
/// transportable publish waits for this attachment's own document, exactly as it
/// waits for the socket.
#[test]
fn a_transportable_publish_between_the_phases_of_a_reconnect_is_not_connected() {
    let mut page = reattached();
    let outcome = on_command(&mut page, publish_cmd("p1", "out", "hello"));
    assert!(outcome.frames.is_empty());
    assert_eq!(status(&outcome), PublishStatus::NotConnected);
}

/// The other half of that window: page-local work is the page's own authority and
/// answers to no peer, so it carries on against the wiring in force — the same
/// reason a confined publish survives a detach.
#[test]
fn a_confined_publish_between_the_phases_of_a_reconnect_still_commits() {
    let mut page = reattached();
    let outcome = on_command(&mut page, publish_cmd("p1", "notes", "offline"));
    assert_eq!(status(&outcome), PublishStatus::Ok);
    assert_eq!(retained(&page, NOTES), vec!["offline".to_string()]);
}

#[test]
fn a_report_between_the_phases_of_a_reconnect_is_dropped() {
    let mut page = page_with(pages::facts(), &reporting_doc());
    page.on_detached();
    page.on_attached(pages::facts());
    let outcome = on_command(
        &mut page,
        Command::Report {
            level: LogLevel::Error,
            source: "kernel".to_string(),
            message: "it broke".to_string(),
            subject: Some("p1".to_string()),
        },
    );
    assert_eq!(outcome, CommandOutcome::default());
}

#[test]
fn telemetry_between_the_phases_of_a_reconnect_is_dropped() {
    let mut page = reattached();
    let geometry = on_command(
        &mut page,
        Command::Geometry {
            width: 1_280,
            height: 720,
            device_pixel_ratio: Number::from_f64(1.0).expect("a finite ratio"),
        },
    );
    assert_eq!(geometry, CommandOutcome::default());
    let status = on_command(&mut page, status_cmd(mounted(&["p1", "p2", "chrome"], 2)));
    assert_eq!(status, CommandOutcome::default());
}

#[test]
fn a_body_over_the_attachments_cap_is_refused_before_anything_leaves() {
    // Both classes: a confined port is reachable on a detached page the ordinary
    // way is not, so the cap is the only thing left between it and the ring.
    for (port, channel) in [("out", WIRE), ("notes", NOTES)] {
        let mut page = page();
        page.on_detached();
        let body = "x".repeat(pages::BODY_CAP as usize + 1);
        let outcome = on_command(&mut page, publish_cmd("p1", port, &body));
        assert!(outcome.frames.is_empty());
        let expected = if channel == NOTES {
            PublishStatus::BodyTooLarge {
                len: pages::BODY_CAP + 1,
                max: pages::BODY_CAP,
            }
        } else {
            PublishStatus::NotConnected
        };
        assert_eq!(status(&outcome), expected, "{port} on {channel}");
        assert!(retained(&page, NOTES).is_empty(), "{port} on {channel}");
    }
    // And on an attached page the transportable port answers the cap too.
    let mut page = page();
    let body = "x".repeat(pages::BODY_CAP as usize + 1);
    let outcome = on_command(&mut page, publish_cmd("p1", "out", &body));
    assert!(outcome.frames.is_empty());
    assert_eq!(
        status(&outcome),
        PublishStatus::BodyTooLarge {
            len: pages::BODY_CAP + 1,
            max: pages::BODY_CAP,
        }
    );
}

#[test]
fn a_confined_port_commits_in_the_page_and_is_answered_at_once() {
    let mut page = page();
    let outcome = on_command(&mut page, publish_cmd("p1", "notes", "note"));
    assert!(outcome.frames.is_empty());
    assert_eq!(status(&outcome), PublishStatus::Ok);
    assert_eq!(retained(&page, NOTES), vec!["note".to_string()]);
    assert!(
        page.stores
            .get(NOTES)
            .expect("the fixture hosts the page-local channel")
            .has_deliverable(&BindingKey::new("p2", "notes"))
    );
}

#[test]
fn a_confined_port_still_publishes_while_the_link_is_down() {
    let mut page = page();
    page.on_detached();
    let outcome = on_command(&mut page, publish_cmd("p1", "notes", "offline"));
    assert_eq!(status(&outcome), PublishStatus::Ok);
    assert_eq!(retained(&page, NOTES), vec!["offline".to_string()]);
}

#[test]
fn a_confined_publish_that_evicts_a_readers_position_charges_the_ladder() {
    let mut page = page();
    on_command(&mut page, publish_stamped("p1", "notes", "first", 1));
    let outcome = on_command(&mut page, publish_stamped("p1", "notes", "second", 2));
    assert_eq!(status(&outcome), PublishStatus::Ok);
    assert!(
        outcome.drops.is_quiet(),
        "a metered binding is counted here and announced at its own next window"
    );
    assert_eq!(
        page.schedules.metered_drops("p2", "notes"),
        1,
        "the depth-1 channel evicted what p2 had not been served"
    );
    assert_eq!(page.schedules.metered_drops("p1", "notes"), 0);
}

#[test]
fn a_fatal_binding_a_publish_evicts_asks_for_a_kill_naming_its_own_instance() {
    let mut page = page_with(pages::facts(), &doc(NoiseLevel::Fatal));
    on_command(&mut page, publish_stamped("p1", "notes", "first", 1));
    let outcome = on_command(&mut page, publish_stamped("p1", "notes", "second", 2));
    let killed: Vec<&str> = outcome
        .drops
        .fatal
        .iter()
        .map(|announcement| announcement.instance.as_str())
        .collect();
    assert_eq!(
        killed,
        vec!["p2"],
        "the publisher is not the one that lost anything"
    );
}

#[test]
fn a_plane_refusal_answers_the_publisher_and_names_the_rule_it_broke() {
    let mut page = page();
    let outcome = on_command(
        &mut page,
        publish_cmd("p1", "over", &overlay_body(Some("p1"))),
    );
    assert_eq!(status(&outcome), PublishStatus::Refused);
    let refused = outcome.refusal.expect("the overlay plane refused it");
    assert_eq!(refused.instance, "p1");
    assert_eq!(refused.refusal.port, "over");
    assert_eq!(refused.refusal.channel, LOCAL_OVERLAY_STATE_CHANNEL);
    assert!(refused.refusal.reason.contains("chrome"));
    assert!(retained(&page, LOCAL_OVERLAY_STATE_CHANNEL).is_empty());
}

#[test]
fn a_report_goes_to_the_error_channel_attributed_to_its_subject() {
    let mut page = page_with(pages::facts(), &reporting_doc());
    let outcome = on_command(
        &mut page,
        Command::Report {
            level: LogLevel::Error,
            source: "component:protobar".to_string(),
            message: "it broke".to_string(),
            subject: Some("p1".to_string()),
        },
    );
    let (channel, attribution, body, _) = publish_parts(one_frame(&outcome));
    assert_eq!((channel, attribution), (ERRORS, Some("p1")));
    assert!(body.contains("it broke"));
    assert!(outcome.answers.is_empty());
}

#[test]
fn a_kernel_breadcrumb_reports_under_the_bare_identity() {
    let mut page = page_with(pages::facts(), &reporting_doc());
    let outcome = on_command(
        &mut page,
        Command::Report {
            level: LogLevel::Warn,
            source: "kernel".to_string(),
            message: "a breadcrumb".to_string(),
            subject: None,
        },
    );
    assert_eq!(publish_parts(one_frame(&outcome)).1, None);
}

#[test]
fn a_report_below_the_floor_and_one_with_no_channel_both_publish_nothing() {
    let mut reporting = page_with(pages::facts(), &reporting_doc());
    let below = on_command(
        &mut reporting,
        Command::Report {
            level: LogLevel::Info,
            source: "kernel".to_string(),
            message: "chatter".to_string(),
            subject: None,
        },
    );
    assert!(below.frames.is_empty());

    let mut silent = page();
    let unconfigured = on_command(
        &mut silent,
        Command::Report {
            level: LogLevel::Error,
            source: "kernel".to_string(),
            message: "it broke".to_string(),
            subject: None,
        },
    );
    assert!(unconfigured.frames.is_empty());
}

#[test]
fn a_report_with_no_attachment_is_dropped_rather_than_queued() {
    let mut page = page_with(pages::facts(), &reporting_doc());
    page.on_detached();
    let outcome = on_command(
        &mut page,
        Command::Report {
            level: LogLevel::Error,
            source: "kernel".to_string(),
            message: "it broke".to_string(),
            subject: None,
        },
    );
    assert_eq!(outcome, CommandOutcome::default());
}

#[test]
fn a_granted_alert_composes_the_frame_with_both_fields_capped() {
    let mut page = page_with(
        AttachmentFacts {
            alert_granted: true,
            ..pages::facts()
        },
        &doc(NoiseLevel::Metered),
    );
    let outcome = on_command(
        &mut page,
        Command::Alert {
            attribution: None,
            severity: AlertSeverity::Critical,
            title: "t".repeat(MAX_ALERT_TITLE_BYTES * 2),
            body: "b".repeat(MAX_ALERT_BODY_BYTES * 2),
        },
    );
    match one_frame(&outcome) {
        ClientFrame::Alert {
            severity,
            title,
            body,
            ..
        } => {
            assert_eq!(*severity, AlertSeverity::Critical);
            assert!(title.len() <= MAX_ALERT_TITLE_BYTES);
            assert!(body.len() <= MAX_ALERT_BODY_BYTES);
        }
        other => panic!("expected an Alert, got {other:?}"),
    }
}

#[test]
fn an_ungranted_alert_and_one_with_no_attachment_are_both_dropped() {
    let mut ungranted = page();
    let alert = || Command::Alert {
        attribution: None,
        severity: AlertSeverity::Warning,
        title: "look".to_string(),
        body: "here".to_string(),
    };
    assert_eq!(
        on_command(&mut ungranted, alert()),
        CommandOutcome::default()
    );

    let mut detached = page_with(
        AttachmentFacts {
            alert_granted: true,
            ..pages::facts()
        },
        &doc(NoiseLevel::Metered),
    );
    detached.on_detached();
    assert_eq!(
        on_command(&mut detached, alert()),
        CommandOutcome::default()
    );
}

#[test]
fn a_control_publish_reaches_every_reader_on_the_plane() {
    let mut page = page();
    let outcome = on_command(
        &mut page,
        Command::PublishControl {
            channel: LOCAL_LINK_STATE_CHANNEL.to_string(),
            body: "{\"v\":1}".to_string(),
            stamp: stamp(0x11),
        },
    );
    assert_eq!(outcome, CommandOutcome::default());
    assert_eq!(
        retained(&page, LOCAL_LINK_STATE_CHANNEL),
        vec!["{\"v\":1}".to_string()]
    );
    assert!(
        page.stores
            .get(LOCAL_LINK_STATE_CHANNEL)
            .expect("the fixture hosts the link-state plane")
            .has_deliverable(&BindingKey::new("p2", "link"))
    );
}

#[test]
fn a_control_publish_is_stated_while_the_link_is_down() {
    let mut page = page();
    page.on_detached();
    on_command(
        &mut page,
        Command::PublishControl {
            channel: LOCAL_LINK_STATE_CHANNEL.to_string(),
            body: "{\"v\":1}".to_string(),
            stamp: stamp(0x12),
        },
    );
    assert_eq!(retained(&page, LOCAL_LINK_STATE_CHANNEL).len(), 1);
}

#[test]
fn a_control_publish_before_the_first_attachment_is_dropped() {
    let mut page = SurfacePage::new(CONFIG.to_string(), EPOCH);
    let outcome = on_command(
        &mut page,
        Command::PublishControl {
            channel: LOCAL_LINK_STATE_CHANNEL.to_string(),
            body: "{\"v\":1}".to_string(),
            stamp: stamp(0x13),
        },
    );
    assert_eq!(outcome, CommandOutcome::default());
    assert!(retained(&page, LOCAL_LINK_STATE_CHANNEL).is_empty());
}

#[test]
#[should_panic(expected = "does not publish on local:brenn/overlay-state")]
fn the_kernel_publishing_on_a_component_plane_is_a_bug() {
    let mut page = page();
    on_command(
        &mut page,
        Command::PublishControl {
            channel: LOCAL_OVERLAY_STATE_CHANNEL.to_string(),
            body: overlay_body(None),
            stamp: stamp(0x14),
        },
    );
}

#[test]
fn a_viewport_reading_publishes_the_geometry_document_unattributed() {
    let mut page = page();
    let outcome = on_command(
        &mut page,
        Command::Geometry {
            width: 1_280,
            height: 720,
            device_pixel_ratio: Number::from_f64(1.5).expect("a finite ratio"),
        },
    );
    let (channel, attribution, body, _) = publish_parts(one_frame(&outcome));
    assert_eq!((channel, attribution), (GEOMETRY, None));
    let document = GeometryDocument::parse(body).expect("the composed body parses");
    assert_eq!(document.session, pages::SESSION_ID);
    assert_eq!(
        (
            document.viewport.width,
            document.viewport.height,
            document.device_pixel_ratio
        ),
        (1_280, 720, 1.5)
    );
}

#[test]
fn a_viewport_reading_outside_the_plausible_range_is_refused_not_published() {
    let mut page = page();
    let outcome = on_command(
        &mut page,
        Command::Geometry {
            width: 0,
            height: 720,
            device_pixel_ratio: Number::from_f64(1.0).expect("a finite ratio"),
        },
    );
    assert_eq!(outcome, CommandOutcome::default());
}

#[test]
fn a_viewport_reading_with_no_attachment_is_dropped() {
    let mut page = page();
    page.on_detached();
    let outcome = on_command(
        &mut page,
        Command::Geometry {
            width: 1_280,
            height: 720,
            device_pixel_ratio: Number::from_f64(1.0).expect("a finite ratio"),
        },
    );
    assert_eq!(outcome, CommandOutcome::default());
}

#[test]
fn a_status_snapshot_publishes_the_document_with_the_health_the_wiring_implies() {
    let mut page = page();
    let outcome = on_command(&mut page, status_cmd(mounted(&["p1", "p2", "chrome"], 2)));
    let (channel, attribution, body, _) = publish_parts(one_frame(&outcome));
    assert_eq!((channel, attribution), (STATUS, None));
    let document = StatusDocument::parse(body).expect("the composed body parses");
    assert_eq!(document.session, pages::SESSION_ID);
    assert_eq!(document.health, Health::Ok);
    assert_eq!(document.uptime_secs, 42);
    assert!(document.overlay.is_none());
}

#[test]
fn an_incomplete_instance_table_reads_degraded() {
    let mut page = page();
    let outcome = on_command(&mut page, status_cmd(mounted(&["p1"], 0)));
    let (_, _, body, _) = publish_parts(one_frame(&outcome));
    let document = StatusDocument::parse(body).expect("the composed body parses");
    assert_eq!(document.health, Health::Degraded);
}

#[test]
fn the_overlay_the_page_recorded_rides_the_status_document() {
    let mut page = page();
    on_command(
        &mut page,
        publish_cmd(fixtures::CHROME, "over", &overlay_body(Some("p1"))),
    );
    let outcome = on_command(&mut page, status_cmd(mounted(&["p1", "p2", "chrome"], 2)));
    let (_, _, body, _) = publish_parts(one_frame(&outcome));
    let document = StatusDocument::parse(body).expect("the composed body parses");
    assert_eq!(
        document.overlay.map(|overlay| overlay.holder),
        Some("p1".to_string())
    );
}

/// The viewport's twin. Composing one against a detached page would hand a frame
/// to the outbound layer and then trip the fold's no-attachment assert — a panic
/// where the contract says drop.
#[test]
fn a_status_snapshot_with_no_attachment_is_dropped() {
    let mut page = page();
    page.on_detached();
    let outcome = on_command(&mut page, status_cmd(mounted(&["p1", "p2", "chrome"], 2)));
    assert_eq!(outcome, CommandOutcome::default());
}

/// The count is the page's own — the reporter cannot know what the peer refused —
/// so it is stated over whatever the snapshot carried.
#[test]
fn the_refused_telemetry_count_rides_the_status_document() {
    let mut page = page();
    on_command(
        &mut page,
        Command::Geometry {
            width: 1_280,
            height: 720,
            device_pixel_ratio: Number::from_f64(1.0).expect("a finite ratio"),
        },
    );
    let answer = page
        .outbound
        .on_publish_result(Some(0), brenn_attach_proto::PublishOutcome::RateLimited)
        .expect("a metered telemetry document is an outcome, not a fatal");
    assert!(matches!(
        answer,
        Some(PublishAnswer::TelemetryDropped { .. })
    ));

    let outcome = on_command(&mut page, status_cmd(mounted(&["p1", "p2", "chrome"], 2)));
    let (_, _, body, _) = publish_parts(one_frame(&outcome));
    let document = StatusDocument::parse(body).expect("the composed body parses");
    assert_eq!(document.counters.telemetry_dropped, 1);
}

#[test]
fn a_status_report_that_contradicts_the_wiring_is_the_callers_fatal() {
    let mut page = page();
    let outcome = on_command(&mut page, status_cmd(mounted(&["stranger"], 0)));
    assert!(outcome.frames.is_empty());
    assert!(
        outcome
            .fatal
            .expect("an undeclared instance is unreconcilable")
            .contains("stranger")
    );
}

/// One status command over `instances`, at a fixed uptime and with no counters.
fn status_cmd(instances: Vec<InstanceReport>) -> Command {
    Command::Status {
        instances,
        uptime_secs: 42,
        counters: StatusCounters::default(),
    }
}

/// Each of `instances` mounted with `ports_attached` pumps. The fixture's busiest
/// component reads two channels, so two is what a healthy table reports.
fn mounted(instances: &[&str], ports_attached: u32) -> Vec<InstanceReport> {
    instances
        .iter()
        .map(|instance| InstanceReport {
            instance: (*instance).to_string(),
            kind: "protobar".to_string(),
            state: InstanceState::Mounted,
            reason: None,
            ports_attached,
        })
        .collect()
}

#[test]
fn a_close_answers_every_caller_the_attachment_stranded_and_asks_for_the_close() {
    let mut page = page();
    on_command(&mut page, publish_cmd("p1", "out", "hello"));
    let outcome = on_command(&mut page, Command::Close);
    assert!(outcome.close);
    assert_eq!(
        status(&outcome),
        PublishStatus::ConnectionLost,
        "the publish awaiting the peer's answer is answered by the close"
    );
    assert!(page.connect.facts().is_none());
}

#[test]
fn a_close_leaves_the_page_itself_standing() {
    let mut page = page();
    on_command(&mut page, publish_cmd("p1", "notes", "note"));
    on_command(&mut page, Command::Close);
    assert_eq!(retained(&page, NOTES), vec!["note".to_string()]);
    assert!(page.registrations.is_registered("p1"));
}

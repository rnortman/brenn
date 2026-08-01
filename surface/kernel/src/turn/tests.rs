//! The front door, driven against a real [`SurfacePage`] — real stores, a real
//! subscription plane, a real confined router carrying the surface's plane policy
//! and real outboxes — so a registration really subscribes and a frame is the one
//! that would go on the wire.
//!
//! What the passes below do with an input is covered where they live. What is
//! under test here is the routing: which pass one input reaches, and what the turn
//! says afterwards.

use brenn_attach_client::publish::{FlushBatch, TimerChange};
use brenn_attach_client::router::{MessageStamp, Origin, ReleaseTimer, RouteOutcome, RouteRequest};
use brenn_attach_proto::{
    BatchEntry, ClientFrame, PublishBatchOutcome, PublishOutcome, SubscribeOutcome,
};
use brenn_envelope::Urgency;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{
    Binding, CONTROL_PLANE_VERSION, InstanceReport, InstanceState, LOCAL_OVERLAY_STATE_CHANNEL,
    NoiseLevel, OverlayStateBody, StatusCounters,
};
use uuid::Uuid;

use crate::core::{ActivationOutcome, PublishBuffer, PublishStatus};
use crate::outbound::{PortPublish, resolve_output};
use crate::registry::BindingKey;
use crate::session::Event;
use crate::test_support::bindings as fixtures;
use crate::test_support::bindings::output;
use crate::test_support::pages;

use super::*;

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const OUT: &str = "ephemeral:site.bar.out";
const IN: &str = "ephemeral:site.bar.in";
const NOTES: &str = "local:app/notes";
const EPOCH: Uuid = Uuid::from_u128(0x7011);
const NOW: Millis = Millis(1_000);
const NOW_MS: u64 = 1_000;

/// `p1` reads one wire channel and writes another; `p2` and chrome exist because
/// every surface document declares its cast.
///
/// `p1` also writes the page-local `NOTES` — declared at ring depth 1, so a second
/// publish evicts what the first left unread — and the overlay plane it has no
/// business writing, which is the one refusal a single publish can produce.
/// `reader_noise` is what a loss on `NOTES` costs `p2`.
fn doc_with(reader_noise: NoiseLevel) -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component("p2"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![
            fixtures::subscription("p1", "in", IN, 2, 2),
            fixtures::subscription("p2", "in", IN, 2, 2),
            Binding {
                noise: reader_noise,
                ..fixtures::subscription("p2", "notes", NOTES, 1, 0)
            },
        ],
        vec![
            output("p1", "out", OUT),
            output("p1", "notes", NOTES),
            output("p1", "over", LOCAL_OVERLAY_STATE_CHANNEL),
        ],
        vec![
            fixtures::local(NOTES, 1),
            fixtures::local(LOCAL_OVERLAY_STATE_CHANNEL, 1),
        ],
    )
}

fn doc() -> BindingsDocument {
    doc_with(NoiseLevel::Metered)
}

fn fresh() -> SurfacePage {
    SurfacePage::new(CONFIG.to_string(), EPOCH)
}

/// A configured page: attached, `p1`/`p2`/chrome mounted, the document in force.
fn page() -> SurfacePage {
    page_with(&doc())
}

fn page_with(document: &BindingsDocument) -> SurfacePage {
    pages::configured_page(
        CONFIG,
        EPOCH,
        pages::facts(),
        &["p1", "p2", fixtures::CHROME],
        document,
        NOW,
    )
}

fn stamp(seed: u128) -> MessageStamp {
    MessageStamp {
        message_id: Uuid::from_u128(seed),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
    }
}

/// One publish command on `(instance, port)`, under its own envelope identity: a
/// confined store dedups by message id, so two publishes onto one channel must not
/// share a stamp.
fn publish_cmd(instance: &str, port: &str, body: &str, seed: u128) -> Input {
    Input::Command(Command::Publish {
        publish: PortPublish {
            instance: instance.to_string(),
            port: port.to_string(),
            body: body.to_string(),
            urgency: None,
            correlation: 7,
        },
        stamp: stamp(seed),
    })
}

/// One overlay-plane body naming its holder — a body only chrome may write.
fn overlay_body(holder: &str) -> String {
    serde_json::to_string(&OverlayStateBody {
        v: CONTROL_PLANE_VERSION,
        holder: Some(holder.to_string()),
        since_stamp: 0,
    })
    .expect("an overlay body serializes")
}

/// Park one of `p1`'s messages on the page-local channel it writes, so the page
/// holds a confined release deadline. Routed directly: what is under test is what
/// a *turn* does with the schedule, not how it got there.
fn park_notes(page: &mut SurfacePage, body: &str, release_at: u64, seed: u128) {
    let outcome = page.router.route(
        &mut page.stores,
        RouteRequest {
            channel: NOTES,
            origin: Origin::Sub("p1"),
            body: body.to_string(),
            stamp: stamp(seed),
            urgency: Urgency::Normal,
            deliver_after: Some(release_at),
        },
    );
    assert!(matches!(outcome, RouteOutcome::Parked { .. }));
}

/// Put one of `p1`'s flushes on the wire and have the peer meter it, so the
/// instance's outbox holds a blocked head the retry deadline is owed.
fn block_the_outbox(page: &mut SurfacePage) {
    let SurfacePage {
        connect, outbound, ..
    } = page;
    let bindings = connect
        .bindings()
        .expect("the fixture page has a document in force");
    let steps = outbound.flush(
        bindings,
        connect.facts(),
        "p1",
        FlushBatch {
            entries: vec![BatchEntry {
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
    outbound
        .on_batch_result(*correlation, PublishBatchOutcome::RateLimited, NOW)
        .expect("the correlation is outstanding");
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

/// One status command over `instances`, at a fixed uptime and with no counters.
fn status_cmd(instances: &[&str]) -> Input {
    Input::Command(Command::Status {
        instances: instances
            .iter()
            .map(|instance| InstanceReport {
                instance: (*instance).to_string(),
                kind: "protobar".to_string(),
                state: InstanceState::Mounted,
                reason: None,
                ports_attached: 2,
            })
            .collect(),
        uptime_secs: 42,
        counters: StatusCounters::default(),
    })
}

fn feed(page: &mut SurfacePage, input: Input) -> Vec<Effect> {
    on_input(page, input, NOW, NOW_MS)
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

/// An empty buffer, for a completion that published nothing.
fn empty_buffer() -> PublishBuffer {
    PublishBuffer::new(
        Default::default(),
        Default::default(),
        0,
        Default::default(),
    )
}

/// Put one publish of `p1`'s on the wire, so a `PublishResult` has a correlation
/// to settle. Composed through the outbound layer directly: what is under test is
/// where the *answer* lands, not how the publish was stated.
fn send_one_publish(page: &mut SurfacePage) {
    let SurfacePage {
        connect, outbound, ..
    } = page;
    let bindings = connect
        .bindings()
        .expect("the fixture page has a document in force");
    let out = resolve_output(bindings, "p1", "out", None).expect("the fixture binds p1/out");
    outbound.publish_port(
        out,
        PortPublish {
            instance: "p1".to_string(),
            port: "out".to_string(),
            body: "hi".to_string(),
            urgency: None,
            correlation: 7,
        },
    );
}

// ---------------------------------------------------------------------------
// The routing table
// ---------------------------------------------------------------------------

#[test]
fn a_connection_event_reaches_the_fold() {
    let mut page = page();
    let effects = feed(
        &mut page,
        Input::Conn(ConnEvent::Fatal {
            detail: "unreconcilable".to_string(),
        }),
    );
    assert!(matches!(events(&effects).as_slice(), [Event::Fatal { .. }]));
}

#[test]
fn an_attachment_coming_up_subscribes_the_config_channel_and_announces_nothing() {
    let mut page = fresh();
    let effects = feed(&mut page, Input::Conn(ConnEvent::Attached(pages::facts())));
    assert!(events(&effects).is_empty());
    assert!(matches!(
        frames(&effects).as_slice(),
        [
            ClientFrame::Subscribe {
                channel,
                resume: None,
                ..
            },
        ] if channel == CONFIG
    ));
}

#[test]
fn a_routed_frame_reaches_the_inbound_pass() {
    let mut page = page();
    send_one_publish(&mut page);
    let effects = feed(
        &mut page,
        Input::Frame(ServerFrame::PublishResult {
            correlation: Some(0),
            outcome: PublishOutcome::Ok,
        }),
    );
    assert!(matches!(
        events(&effects).as_slice(),
        [Event::PublishResult {
            correlation: 7,
            status: PublishStatus::Ok,
            ..
        }]
    ));
}

#[test]
fn a_subscribe_result_settles_the_channel_it_names() {
    let mut page = page();
    let effects = feed(
        &mut page,
        Input::Frame(ServerFrame::SubscribeResult {
            channel: IN.to_string(),
            outcome: SubscribeOutcome::Ok,
            replay_count: 0,
            gap: None,
        }),
    );
    assert!(effects.is_empty());
    assert!(page.subs.is_active(IN));
}

#[test]
fn a_command_reaches_its_pass_and_answers_the_caller_that_asked() {
    let mut page = page();
    let effects = feed(
        &mut page,
        Input::Command(Command::Publish {
            publish: PortPublish {
                instance: "p1".to_string(),
                port: "nowhere".to_string(),
                body: "hi".to_string(),
                urgency: None,
                correlation: 3,
            },
            stamp: MessageStamp {
                message_id: Uuid::from_u128(0x9),
                publish_ts: chrono::DateTime::from_timestamp(0, 0)
                    .expect("a representable instant"),
            },
        }),
    );
    assert!(matches!(
        events(&effects).as_slice(),
        [Event::PublishResult {
            correlation: 3,
            status: PublishStatus::UnboundPort,
            ..
        }]
    ));
}

#[test]
fn a_close_is_asked_for_before_the_callers_it_stranded_are_answered() {
    let mut page = page();
    send_one_publish(&mut page);
    let effects = feed(&mut page, Input::Command(Command::Close));
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::Close,
            Effect::EmitEvent(Event::PublishResult {
                correlation: 7,
                status: PublishStatus::ConnectionLost,
                ..
            }),
            ..
        ]
    ));
}

#[test]
fn an_unreconcilable_frame_asks_the_connection_to_go_fatal() {
    let mut page = page();
    let effects = feed(
        &mut page,
        Input::Frame(ServerFrame::PublishResult {
            correlation: Some(41),
            outcome: PublishOutcome::Ok,
        }),
    );
    assert!(matches!(effects.as_slice(), [Effect::GoFatal { .. }]));
}

#[test]
fn a_host_fatal_takes_the_same_path_as_any_other() {
    let mut page = page();
    let effects = feed(
        &mut page,
        Input::HostFatal {
            detail: "clock before the epoch".to_string(),
        },
    );
    assert_eq!(
        effects,
        vec![Effect::GoFatal {
            detail: "clock before the epoch".to_string()
        }]
    );
}

#[test]
fn the_retry_deadline_reaches_the_outboxes() {
    let mut page = page();
    block_the_outbox(&mut page);
    let effects = feed(&mut page, Input::RetryDue);
    assert!(
        matches!(
            frames(&effects).as_slice(),
            [ClientFrame::PublishBatch { attribution, .. }] if attribution.as_deref() == Some("p1")
        ),
        "the blocked head is offered again: {effects:?}"
    );
}

#[test]
fn a_retry_fire_with_nothing_blocked_produces_nothing() {
    let mut page = page();
    let effects = feed(&mut page, Input::RetryDue);
    assert!(effects.is_empty());
}

/// The release pass, positively: what is due at the fire enters retention, wakes
/// the readers of its channel, and charges whoever it evicted — a release is an
/// ordinary arrival.
#[test]
fn a_release_fire_takes_what_is_due_into_retention_and_charges_the_ladder() {
    let mut page = page();
    // One message already retained and unread by `p2`, and one parked behind it:
    // releasing the second is what pushes the first past `p2`'s position.
    feed(&mut page, publish_cmd("p1", "notes", "first", 0x901));
    park_notes(&mut page, "second", NOW_MS + 5_000, 0x902);

    let effects = on_input(&mut page, Input::ReleaseDue, NOW, NOW_MS + 5_000);

    assert_eq!(
        retained(&page, NOTES),
        vec!["second".to_string()],
        "the depth-1 channel keeps the newest of the two it released"
    );
    assert!(
        page.stores
            .get(NOTES)
            .expect("the fixture hosts the page-local channel")
            .has_deliverable(&BindingKey::new("p2", "notes"))
    );
    assert_eq!(
        page.schedules.metered_drops("p2", "notes"),
        1,
        "the release evicted what p2 had not been served: {effects:?}"
    );
}

/// The clock the release is judged against is the one read *at the fire*, not the
/// deadline that armed it: a timer that fires early releases nothing, and says so
/// by leaving the deadline stated.
#[test]
fn a_release_fire_before_anything_is_due_releases_nothing() {
    let mut page = page();
    park_notes(&mut page, "later", NOW_MS + 5_000, 0x903);

    let effects = on_input(&mut page, Input::ReleaseDue, NOW, NOW_MS + 1);

    assert!(retained(&page, NOTES).is_empty());
    assert_eq!(
        effects,
        vec![Effect::SetReleaseWakeup(ReleaseTimer::Arm(NOW_MS + 5_000))]
    );
}

#[test]
fn a_release_fire_with_nothing_due_is_not_an_error() {
    let mut page = page();
    let effects = feed(&mut page, Input::ReleaseDue);
    assert!(effects.is_empty());
}

/// The restatement every turn ends with, whatever the input was. Asserted
/// positively, because its absence is exactly what a turn that moved no schedule
/// also produces — so only a turn that *did* move one tells the two apart.
#[test]
fn every_turn_ends_by_stating_the_release_deadline() {
    let mut page = page();
    park_notes(&mut page, "later", NOW_MS + 5_000, 0x904);

    // An input whose own pass touches no schedule at all: the deadline is stated
    // by the fold that closes the turn, not by the pass that ran in it.
    let effects = feed(&mut page, Input::RetryDue);

    assert_eq!(
        effects,
        vec![Effect::SetReleaseWakeup(ReleaseTimer::Arm(NOW_MS + 5_000))]
    );
}

#[test]
fn a_turn_that_moves_no_schedule_states_no_release_deadline() {
    let mut page = page();
    let effects = feed(&mut page, Input::RetryDue);
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::SetReleaseWakeup(_)))
    );
}

/// The refusal reaches the log before the bare status reaches the publisher —
/// ordering the fold documents and the only thing that says which rule was broken,
/// since a plane's status enum carries no reason.
#[test]
fn a_refused_confined_publish_is_diagnosed_before_its_publisher_is_answered() {
    let mut page = page();
    let effects = feed(
        &mut page,
        publish_cmd("p1", "over", &overlay_body("p1"), 0x905),
    );

    match events(&effects).as_slice() {
        [
            Event::PlaneRefused {
                instance,
                port,
                channel,
                reason,
            },
            Event::PublishResult {
                status: PublishStatus::Refused,
                ..
            },
        ] => {
            assert_eq!((instance.as_str(), port.as_str()), ("p1", "over"));
            assert_eq!(channel, LOCAL_OVERLAY_STATE_CHANNEL);
            assert!(reason.contains("chrome"), "{reason}");
        }
        other => panic!("the reason is reported ahead of the status: {other:?}"),
    }
}

/// A status document that contradicts the wiring it was assembled from is this
/// build disagreeing with itself, so the fold takes the attachment fatal rather
/// than dropping the diagnosis on the floor.
#[test]
fn a_status_that_contradicts_the_wiring_takes_the_attachment_fatal() {
    let mut page = page();
    let effects = feed(&mut page, status_cmd(&["stranger"]));
    let [Effect::GoFatal { detail }] = effects.as_slice() else {
        panic!("the contradiction is the caller's fatal: {effects:?}");
    };
    assert!(detail.contains("stranger"), "{detail}");
}

/// A `fatal`-rung loss a command's own publish caused: the kill announcement is
/// not the end of it — the fold enacts the kill and says so, on the page as a
/// toast and to the operator as an alert.
#[test]
fn a_fatal_loss_a_command_caused_kills_its_instance_and_says_so() {
    let mut page = page_with(&doc_with(NoiseLevel::Fatal));
    feed(&mut page, publish_cmd("p1", "notes", "first", 0x906));
    let effects = feed(&mut page, publish_cmd("p1", "notes", "second", 0x907));

    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::SendFrame(ClientFrame::Alert { .. }))),
        "the operator is paged: {effects:?}"
    );
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::PublishControl { .. })),
        "and the page says it too: {effects:?}"
    );
    assert!(
        events(&effects).iter().any(
            |event| matches!(event, Event::InstanceFailed { instance, .. } if instance == "p2")
        ),
        "the victim is the reader that lost the message, not the publisher: {effects:?}"
    );
    assert!(page.registrations.is_failed("p2"));
}

/// The outboxes' answer to a close: the retry deadline dies with the wire it was
/// armed against, rather than firing into a page with no attachment.
#[test]
fn a_close_disarms_the_retry_deadline_a_blocked_head_armed() {
    let mut page = page();
    block_the_outbox(&mut page);
    let effects = feed(&mut page, Input::Command(Command::Close));
    assert!(
        effects.contains(&Effect::SetRetryWakeup(TimerChange::Disarm)),
        "{effects:?}"
    );
}

#[test]
fn a_completion_for_an_unmounted_instance_is_absorbed() {
    let mut page = page();
    let effects = feed(
        &mut page,
        Input::ActivationDone(Completed {
            instance: "gone".to_string(),
            generation: 0,
            outcome: ActivationOutcome::Ok,
            buffer: empty_buffer(),
            stamps: Vec::new(),
        }),
    );
    assert!(effects.is_empty());
}

#[test]
fn a_trapped_completion_takes_its_instance_terminal() {
    let mut page = page();
    let generation = page
        .registrations
        .generation("p1")
        .expect("the fixture mounted p1");
    let effects = feed(
        &mut page,
        Input::ActivationDone(Completed {
            instance: "p1".to_string(),
            generation,
            outcome: ActivationOutcome::Trap("it panicked".to_string()),
            buffer: empty_buffer(),
            stamps: Vec::new(),
        }),
    );
    assert!(matches!(
        events(&effects).as_slice(),
        [Event::InstanceFailed { instance, .. }] if instance == "p1"
    ));
    assert!(page.registrations.is_failed("p1"));
}

// ---------------------------------------------------------------------------
// The activation entries' lifecycle
// ---------------------------------------------------------------------------

#[test]
fn a_registration_before_the_first_document_subscribes_nothing() {
    let mut page = fresh();
    let effects = feed(
        &mut page,
        Input::ActivationRegistered {
            instance: "p1".to_string(),
        },
    );
    assert!(effects.is_empty());
    assert!(page.registrations.is_registered("p1"));
    assert!(page.schedules.is_tracked("p1"));
    assert!(!page.outbound.is_registered("p1"));
}

#[test]
fn a_registration_under_a_document_subscribes_and_opens_an_outbox() {
    let mut page = pages::attached_page(CONFIG, EPOCH, pages::facts());
    page.apply_config(&doc().to_body(), NOW)
        .expect("the fixture document applies");
    let effects = feed(
        &mut page,
        Input::ActivationRegistered {
            instance: "p1".to_string(),
        },
    );
    assert!(matches!(
        frames(&effects).as_slice(),
        [ClientFrame::Subscribe { channel, .. }] if channel == IN
    ));
    assert!(page.outbound.is_registered("p1"));
    assert!(page.schedules.is_tracked("p1"));
}

#[test]
fn an_instance_no_document_declares_gets_no_outbox() {
    let mut page = page();
    let effects = feed(
        &mut page,
        Input::ActivationRegistered {
            instance: "stranger".to_string(),
        },
    );
    assert!(effects.is_empty());
    assert!(page.registrations.is_registered("stranger"));
    assert!(!page.outbound.is_registered("stranger"));
}

#[test]
fn an_unmount_unsubscribes_and_closes_the_outbox() {
    let mut page = page();
    // The subscription has to be acknowledged before an `Unsubscribe` is
    // composable: the plane defers one for a `Subscribe` the peer has not answered
    // yet.
    page.subs
        .on_subscribe_result(IN, SubscribeOutcome::Ok, 0, None)
        .expect("the fixture's one application channel is pending");
    feed(
        &mut page,
        Input::ActivationDeregistered {
            instance: "p2".to_string(),
        },
    );
    let effects = feed(
        &mut page,
        Input::ActivationDeregistered {
            instance: "p1".to_string(),
        },
    );
    assert!(matches!(
        frames(&effects).as_slice(),
        [ClientFrame::Unsubscribe { channel }] if channel == IN
    ));
    assert!(!page.registrations.is_registered("p1"));
    assert!(!page.schedules.is_tracked("p1"));
    assert!(!page.outbound.is_registered("p1"));
}

#[test]
fn an_unmount_leaves_a_subscription_a_sibling_still_holds() {
    let mut page = page();
    page.subs
        .on_subscribe_result(IN, SubscribeOutcome::Ok, 0, None)
        .expect("the fixture's one application channel is pending");
    let effects = feed(
        &mut page,
        Input::ActivationDeregistered {
            instance: "p2".to_string(),
        },
    );
    assert!(frames(&effects).is_empty());
    assert!(page.subs.is_active(IN));
}

#[test]
#[should_panic(expected = "registered twice")]
fn registering_one_instance_twice_is_a_bug() {
    let mut page = page();
    feed(
        &mut page,
        Input::ActivationRegistered {
            instance: "p1".to_string(),
        },
    );
}

#[test]
#[should_panic(expected = "deregistration of unregistered instance")]
fn unmounting_an_instance_that_never_registered_is_a_bug() {
    let mut page = page();
    feed(
        &mut page,
        Input::ActivationDeregistered {
            instance: "stranger".to_string(),
        },
    );
}

//! The outward passes, driven through a real [`SurfacePage`] — real stores, a real
//! confined router carrying the surface's own plane policy, a real subscription
//! plane and real outboxes — so an assertion reads the frame that would go on the
//! wire and the window the next activation would actually be assembled from.

use std::collections::HashMap;

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::publish::TimerChange;
use brenn_attach_client::router::{Origin, ReleaseTimer, RouteOutcome, RouteRequest};
use brenn_attach_proto::PublishBatchOutcome;
use brenn_envelope::Urgency;
use brenn_surface_contract::ActivationError;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{
    Binding, LOCAL_OVERLAY_STATE_CHANNEL, NoiseLevel, ToastBody, Urgency as SchemaUrgency,
};
use uuid::Uuid;

use crate::activation::DropAnnouncement;
use crate::publish_buffer::OutputSpec;
use crate::registry::BindingKey;
use crate::test_support::bindings as fixtures;
use crate::test_support::bindings::{output, output_at};
use crate::test_support::pages;
use crate::test_support::pages::BODY_CAP;

use super::*;

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE: &str = "ephemeral:site.bar.out";
const NOTES: &str = "local:app/notes";
/// A second page-local channel, sorting after [`NOTES`] — the two-channel fixture's
/// other plane, and what makes the sweep's address order observable.
const OTHER: &str = "local:app/other";
const EPOCH: Uuid = Uuid::from_u128(0x5107);
const NOW: Millis = Millis(1_000);
/// The wall clock every pass here is judged at. The fixture's stamps sit at the
/// Unix epoch, so any release time above zero is still ahead of its mint.
const NOW_MS: u64 = 1_000;

/// The knobs the fixture document varies. Everything else about it is constant.
struct W {
    /// `p2/notes`' loudness rung — what a loss on the one binding that reads
    /// anything asks the caller for.
    noise: NoiseLevel,
    /// The page-local channel's declared ring depth, which is its store's depth:
    /// `1` is what makes one publish evict what the previous one left unread.
    notes_depth: u64,
    /// `p2/notes`' push depth. Below `notes_depth` it is what makes an assembly's
    /// own window step over still-retained messages.
    push: u64,
}

impl Default for W {
    fn default() -> Self {
        Self {
            noise: NoiseLevel::Metered,
            notes_depth: 3,
            push: 1,
        }
    }
}

/// `p1` writes one channel of each class and never reads anything, so it is never
/// ready; `p2` reads the page-local channel `p1` writes and also writes the wire
/// channel, so a publish of `p1`'s makes a *sibling* ready and can overflow that
/// sibling's position.
fn doc(w: W) -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component("p2"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![Binding {
            noise: w.noise,
            ..fixtures::subscription("p2", "notes", NOTES, w.push, 0)
        }],
        vec![
            output_at("p1", "out", WIRE, SchemaUrgency::High),
            output("p1", "notes", NOTES),
            output("p2", "out", WIRE),
            output(fixtures::CHROME, "over", LOCAL_OVERLAY_STATE_CHANNEL),
        ],
        vec![
            fixtures::local(NOTES, w.notes_depth),
            fixtures::local(LOCAL_OVERLAY_STATE_CHANNEL, 1),
        ],
    )
}

/// A configured page: attached, `p1`/`p2`/chrome registered and scheduled, one
/// document in force.
fn page(w: W) -> SurfacePage {
    pages::configured_page(
        CONFIG,
        EPOCH,
        pages::facts(),
        &["p1", "p2", fixtures::CHROME],
        &doc(w),
        NOW,
    )
}

fn standard() -> SurfacePage {
    page(W::default())
}

/// A second page-local channel with a reader of its own, so one release fire has
/// two channels to sweep. Both readers are at the `fatal` rung on a depth-1
/// channel, so a release that lands on an unread position kills.
///
/// `other_reader` is who reads the second channel: a sibling, which makes one fire
/// evict two *different* instances' positions, or `p2` again, which makes it evict
/// two positions of one instance.
fn two_plane_doc(other_reader: &str) -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component("p2"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![
            Binding {
                noise: NoiseLevel::Fatal,
                ..fixtures::subscription("p2", "notes", NOTES, 1, 0)
            },
            Binding {
                noise: NoiseLevel::Fatal,
                ..fixtures::subscription(other_reader, "other", OTHER, 1, 0)
            },
        ],
        vec![
            output("p1", "notes", NOTES),
            output("p1", "other", OTHER),
            output(fixtures::CHROME, "over", LOCAL_OVERLAY_STATE_CHANNEL),
        ],
        vec![
            fixtures::local(NOTES, 1),
            fixtures::local(OTHER, 1),
            fixtures::local(LOCAL_OVERLAY_STATE_CHANNEL, 1),
        ],
    )
}

fn two_plane_page(other_reader: &str) -> SurfacePage {
    pages::configured_page(
        CONFIG,
        EPOCH,
        pages::facts(),
        &["p1", "p2", fixtures::CHROME],
        &two_plane_doc(other_reader),
        NOW,
    )
}

/// Leave one unread message and one message due at `NOW_MS + 500` on each of the
/// two page-local channels, so one fire at that instant releases on both and each
/// release evicts the position the unread message was owed to.
fn park_on_both(page: &mut SurfacePage) {
    route_on(page, NOTES, 0xa1, "unread");
    route_on(page, OTHER, 0xa2, "unread");
    park_on(page, NOTES, 0xb1, "later", NOW_MS + 500);
    park_on(page, OTHER, 0xb2, "later", NOW_MS + 500);
}

fn stamp(seed: u128) -> MessageStamp {
    MessageStamp {
        message_id: Uuid::from_u128(seed),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
    }
}

fn stamps(count: usize) -> Vec<MessageStamp> {
    (0..count).map(|i| stamp(0x100 + i as u128)).collect()
}

/// Put one message of `p1`'s on the page-local channel, the way a flush of its
/// would: minted, retained, and every reader on the channel woken.
fn route(page: &mut SurfacePage, seed: u128, body: &str) {
    route_on(page, NOTES, seed, body);
}

/// As [`route`], on any of the page's confined channels.
fn route_on(page: &mut SurfacePage, channel: &str, seed: u128, body: &str) {
    let outcome = page.router.route(
        &mut page.stores,
        RouteRequest {
            channel,
            origin: Origin::Sub("p1"),
            body: body.to_string(),
            stamp: stamp(seed),
            urgency: Urgency::Normal,
            deliver_after: None,
        },
    );
    assert!(matches!(outcome, RouteOutcome::Routed { .. }));
}

/// Park one message of `p1`'s on the page-local channel, answering its identity.
fn park(page: &mut SurfacePage, seed: u128, body: &str, release_at: u64) -> Uuid {
    park_on(page, NOTES, seed, body, release_at)
}

/// As [`park`], on any of the page's confined channels.
fn park_on(page: &mut SurfacePage, channel: &str, seed: u128, body: &str, release_at: u64) -> Uuid {
    let outcome = page.router.route(
        &mut page.stores,
        RouteRequest {
            channel,
            origin: Origin::Sub("p1"),
            body: body.to_string(),
            stamp: stamp(seed),
            urgency: Urgency::Normal,
            deliver_after: Some(release_at),
        },
    );
    assert!(matches!(outcome, RouteOutcome::Parked { .. }));
    stamp(seed).message_id
}

/// A buffer seeded for `instance`'s own output ports at the page's own body cap,
/// with budgets wide enough that nothing here is refused at buffer time.
fn buffer(page: &SurfacePage, instance: &str) -> PublishBuffer {
    let bindings = page
        .bindings()
        .expect("the fixture has a document in force");
    let mut outputs = HashMap::new();
    let mut sink_mt = HashMap::new();
    for binding in bindings.outputs_of(instance) {
        outputs.insert(
            binding.port.clone(),
            OutputSpec {
                channel: binding.channel.clone(),
                default_urgency: binding.urgency,
            },
        );
        sink_mt.insert(binding.port.clone(), 1_000_000);
    }
    PublishBuffer::new(
        outputs,
        bindings.declared_out_ports(instance),
        sink_mt,
        page.body_cap,
        HashMap::new(),
    )
}

/// The completion of an activation assembled for `instance` as the page holds it
/// *now* — the registration a dispatch at this moment would have stamped on it.
fn done(
    page: &SurfacePage,
    instance: &str,
    outcome: ActivationOutcome,
    buffer: PublishBuffer,
) -> Completed {
    let stamps = stamps(buffer.len());
    Completed {
        instance: instance.to_string(),
        generation: page
            .registrations
            .generation(instance)
            .expect("the fixture registered the instance"),
        outcome,
        buffer,
        stamps,
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

/// One `PublishBatch` frame's correlation.
fn correlation_of(frame: &ClientFrame) -> u64 {
    match frame {
        ClientFrame::PublishBatch { correlation, .. } => *correlation,
        other => panic!("expected a PublishBatch, got {other:?}"),
    }
}

fn toast_text(publish: &ControlPublish) -> String {
    assert_eq!(publish.channel, LOCAL_TOAST_CHANNEL);
    serde_json::from_str::<ToastBody>(&publish.body)
        .expect("the composed toast body parses")
        .text
}

#[test]
fn a_page_owed_nothing_dispatches_nothing() {
    let mut page = standard();

    assert_eq!(ready(&page), None);
    assert!(dispatch(&mut page, NOW_MS).is_none());
}

#[test]
fn a_reader_owed_a_message_is_assembled_and_left_in_flight() {
    let mut page = standard();
    route(&mut page, 0xa1, "hello");

    assert_eq!(ready(&page), Some("p2"));
    let activation = dispatch(&mut page, NOW_MS).expect("p2 is owed a message");

    assert_eq!(activation.instance, "p2");
    let window = &activation.activation.ports[0];
    assert_eq!(window.port, "notes");
    assert_eq!(
        window
            .envelopes
            .iter()
            .map(|e| brenn_surface_test_fixtures::parse_envelope(e).body)
            .collect::<Vec<_>>(),
        vec!["hello".to_string()]
    );
    assert_eq!(activation.activation.now, Some(NOW_MS));
    assert!(activation.drops.is_quiet());
    assert!(
        dispatch(&mut page, NOW_MS).is_none(),
        "the instance is in flight, and nothing else reads anything"
    );
}

#[test]
fn asking_who_is_ready_does_not_advance_the_rotation() {
    let mut page = standard();
    route(&mut page, 0xa1, "hello");

    assert_eq!(ready(&page), Some("p2"));
    assert_eq!(
        ready(&page),
        Some("p2"),
        "only an assembly advances the dispatch cursor"
    );
}

#[test]
fn an_assembled_buffer_enforces_the_attachments_body_cap() {
    let mut page = standard();
    route(&mut page, 0xa1, "hello");
    let mut activation = dispatch(&mut page, NOW_MS).expect("p2 is owed a message");

    assert!(
        activation
            .buffer
            .publish("out", "x".repeat(BODY_CAP as usize + 1))
            .is_err()
    );
    assert!(
        activation
            .buffer
            .publish("out", "small".to_string())
            .is_ok()
    );
}

/// A page keeps activating the components reading its confined planes while the
/// link is down — chrome drawing the reconnect banner is the case that matters —
/// so the body-size contract a component was given must not change with the link.
#[test]
fn the_body_cap_survives_a_detach() {
    let mut page = standard();
    page.on_detached();
    route(&mut page, 0xa1, "hello");

    let mut activation = dispatch(&mut page, NOW_MS).expect("a confined reader is owed a message");

    assert_eq!(page.body_cap, BODY_CAP);
    assert!(
        activation
            .buffer
            .publish("out", "x".repeat(BODY_CAP as usize + 1))
            .is_err()
    );
}

/// Retained is not frozen: the cap is the **most recent** attachment's, so a
/// reconnect stating a different one replaces it. Judging a later publish against a
/// dead attachment's cap refuses bodies this peer would take, or — the dangerous
/// direction — buffers one this peer answers `BodyTooLarge` for.
#[test]
fn a_reattachment_replaces_the_body_cap() {
    let mut page = standard();
    page.on_detached();
    page.on_attached(AttachmentFacts {
        max_body_bytes: BODY_CAP / 2,
        ..pages::facts()
    });

    assert_eq!(page.body_cap, BODY_CAP / 2);
    route(&mut page, 0xa1, "hello");
    let mut activation = dispatch(&mut page, NOW_MS).expect("p2 is owed a message");
    assert!(
        activation
            .buffer
            .publish("out", "x".repeat(BODY_CAP as usize / 2 + 1))
            .is_err(),
        "a body the attachment that went away would have taken"
    );
    assert!(
        activation
            .buffer
            .publish("out", "small".to_string())
            .is_ok()
    );
}

/// The window steps over what its own push depth cannot carry, which is a loss on
/// the binding — and at the `fatal` rung the caller is asked to kill the instance
/// rather than run it.
#[test]
fn a_fatal_rung_at_assembly_is_answered_rather_than_enacted() {
    let mut page = page(W {
        noise: NoiseLevel::Fatal,
        ..W::default()
    });
    route(&mut page, 0xa1, "one");
    route(&mut page, 0xa2, "two");
    route(&mut page, 0xa3, "three");

    let activation = dispatch(&mut page, NOW_MS).expect("p2 is owed messages");

    let [fatal] = &activation.drops.fatal[..] else {
        panic!("the fatal rung fired once: {:?}", activation.drops)
    };
    assert_eq!(fatal.instance, "p2");
    assert_eq!(fatal.port, "notes");
    assert_eq!(fatal.dropped, 2, "the two the window could not carry");
    assert!(
        !page.registrations.is_failed("p2"),
        "the verdict is data; the kill is the caller's"
    );
    assert_eq!(page.schedules.metered_drops("p2", "notes"), 2);
}

#[test]
fn an_ok_completion_commits_both_classes() {
    let mut page = standard();
    let mut buffer = buffer(&page, "p1");
    buffer
        .publish("out", "to the peer".to_string())
        .expect("bound");
    buffer
        .publish("notes", "to the page".to_string())
        .expect("bound");

    let completed = done(&page, "p1", ActivationOutcome::Ok(None), buffer);
    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);

    assert_eq!(completion.steps.frames.len(), 1, "one batch frame");
    assert!(completion.refusals.is_empty());
    assert!(completion.killed.is_none());
    assert!(!completion.absorbed);
    assert_eq!(
        retained(&page, NOTES),
        vec!["to the page".to_string()],
        "the confined half committed here and now"
    );
    assert!(!page.schedules.in_flight("p1"));
}

#[test]
fn an_err_completion_discards_the_work_and_counts_the_failure() {
    let mut page = standard();
    let mut buffer = buffer(&page, "p1");
    buffer
        .publish("out", "never sent".to_string())
        .expect("bound");
    buffer
        .publish("notes", "never routed".to_string())
        .expect("bound");

    let completed = done(
        &page,
        "p1",
        ActivationOutcome::Err(ActivationError {
            message: "no".to_string(),
        }),
        buffer,
    );

    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);

    assert_eq!(
        completion,
        Completion::nothing(
            "p1".to_string(),
            ActivationOutcome::Err(ActivationError {
                message: "no".to_string(),
            }),
        )
    );
    assert!(retained(&page, NOTES).is_empty());
    assert_eq!(page.schedules.entry_err_activations("p1"), 1);
    assert!(!page.schedules.in_flight("p1"));
}

#[test]
fn a_trapped_completion_takes_the_instance_terminal() {
    let mut page = standard();
    let buffer = buffer(&page, "p1");

    let completed = done(
        &page,
        "p1",
        ActivationOutcome::Trap("boom".to_string()),
        buffer,
    );

    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);

    let killed = completion.killed.expect("the trap killed it");
    assert!(killed.first);
    assert!(page.registrations.is_failed("p1"));
    assert!(!page.schedules.in_flight("p1"));
}

/// An instance can leave while its activation is still running, and what it wrote
/// then has nowhere to land — no positions, no outbox, no budget to return to.
#[test]
fn a_completion_for_a_deregistered_instance_is_absorbed() {
    let mut page = standard();
    let mut buffer = buffer(&page, "p1");
    buffer
        .publish("notes", "orphan".to_string())
        .expect("bound");
    let completed = done(&page, "p1", ActivationOutcome::Ok(None), buffer);
    page.registrations
        .deregister("p1", &mut page.stores, &mut page.subs);

    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);

    assert!(completion.absorbed);
    assert_eq!(
        completion,
        Completion {
            absorbed: true,
            ..Completion::nothing("p1".to_string(), ActivationOutcome::Ok(None))
        }
    );
    assert!(retained(&page, NOTES).is_empty());
}

/// The successor of an instance that left and came back under the same id is a
/// different component: its own positions, its own budgets, its own activations.
/// The predecessor's completion is matched on the registration it was assembled
/// under, so it lands on neither — committing it here would flush a dead
/// component's buffer under the successor's attribution and clear an in-flight
/// marker naming the successor's own running activation.
#[test]
fn a_completion_for_a_reregistered_instance_is_absorbed() {
    let mut page = standard();
    let mut buffer = buffer(&page, "p1");
    buffer
        .publish("notes", "from the previous mount".to_string())
        .expect("bound");
    let completed = done(&page, "p1", ActivationOutcome::Ok(None), buffer);
    page.registrations
        .deregister("p1", &mut page.stores, &mut page.subs);
    page.registrations.register(
        "p1",
        page.connect.bindings(),
        &mut page.stores,
        &mut page.subs,
    );
    assert!(page.registrations.is_registered("p1"));
    assert!(!page.registrations.is_failed("p1"));

    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);

    assert!(completion.absorbed);
    assert!(
        retained(&page, NOTES).is_empty(),
        "the predecessor's publish reaches nothing under the successor"
    );
}

/// The `fatal` rung is charged at an arrival, at a depth shrink and at a sibling's
/// append — none of which is the assembly, so a kill can land on an instance whose
/// activation is already running. What it wrote must not commit afterwards: the
/// kill's own account of the flush is that it is gone, and the platform half has
/// already been told the instance failed.
#[test]
fn a_completion_for_an_instance_killed_mid_flight_is_absorbed() {
    let mut page = standard();
    let mut buffer = buffer(&page, "p1");
    buffer
        .publish("notes", "confined".to_string())
        .expect("bound");
    buffer
        .publish("out", "on the wire".to_string())
        .expect("bound");
    let killed = kill(&mut page, "p1", NOW, NOW_MS);
    assert!(killed.first);
    assert!(
        page.registrations.is_registered("p1"),
        "a kill fails the instance without deregistering it"
    );

    let completed = done(&page, "p1", ActivationOutcome::Ok(None), buffer);
    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);

    assert!(completion.absorbed);
    assert!(
        completion.steps.frames.is_empty(),
        "the batch of a terminal instance never reaches the wire"
    );
    assert!(
        retained(&page, NOTES).is_empty(),
        "and its confined publish wakes no sibling"
    );
}

/// One instance's publish can evict a *sibling's* unread position, which is why an
/// announcement names its own instance rather than the publisher's.
///
/// The rung is `fatal` because a retirement announces only there — every softer
/// rung counts here and names the whole delta at the binding's next window, so an
/// `alarm` eviction is silent in this answer by design.
#[test]
fn a_confined_append_charges_the_position_it_evicted() {
    let mut page = page(W {
        noise: NoiseLevel::Fatal,
        notes_depth: 1,
        push: 1,
    });
    route(&mut page, 0xa1, "unread");
    let mut buffer = buffer(&page, "p1");
    buffer
        .publish("notes", "evicts it".to_string())
        .expect("bound");

    let completed = done(&page, "p1", ActivationOutcome::Ok(None), buffer);
    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);

    let [fatal] = &completion.drops.fatal[..] else {
        panic!("the fatal rung fired once: {:?}", completion.drops)
    };
    assert_eq!(
        fatal.instance, "p2",
        "the sibling whose position it was, not the publisher"
    );
    assert_eq!(fatal.port, "notes");
    assert_eq!(fatal.dropped, 1);
    assert_eq!(page.schedules.metered_drops("p2", "notes"), 1);
}

/// A softer rung counts the same eviction where it happened and announces nothing
/// there — the delta reaches the operator at the binding's next window.
#[test]
fn a_softer_rung_counts_an_eviction_without_announcing_it() {
    let mut page = page(W {
        noise: NoiseLevel::Alarm,
        notes_depth: 1,
        push: 1,
    });
    route(&mut page, 0xa1, "unread");
    let mut buffer = buffer(&page, "p1");
    buffer
        .publish("notes", "evicts it".to_string())
        .expect("bound");

    let completed = done(&page, "p1", ActivationOutcome::Ok(None), buffer);
    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);

    assert!(completion.drops.is_quiet());
    assert_eq!(page.schedules.metered_drops("p2", "notes"), 1);
    let announced = dispatch(&mut page, NOW_MS).expect("the arrival made p2 ready");
    assert_eq!(announced.drops.announce.len(), 1);
    assert_eq!(announced.drops.announce[0].instance, "p2");
    assert_eq!(announced.drops.announce[0].dropped, 1);
}

/// `p2` both reads the page-local channel and writes the wire, so one kill has
/// both halves to take: the position it was owed a message on, and the flush its
/// last activation left queued.
#[test]
fn a_kill_strips_positions_and_discards_a_queued_flush() {
    let mut page = standard();
    page.on_detached();
    let mut buffer = buffer(&page, "p2");
    buffer.publish("out", "queued".to_string()).expect("bound");
    let completed = done(&page, "p2", ActivationOutcome::Ok(None), buffer);
    on_activation_done(&mut page, completed, NOW, NOW_MS);
    route(&mut page, 0xa1, "unread");
    assert!(has_position(&page, "p2"), "the fixture owes it a message");

    let killed = kill(&mut page, "p2", NOW, NOW_MS);

    assert!(killed.first);
    assert_eq!(killed.discarded, 1);
    assert!(page.registrations.is_failed("p2"));
    assert!(
        !has_position(&page, "p2"),
        "its positions went with the kill"
    );
}

/// Whether `instance`'s `notes` binding is owed something the page-local channel
/// still holds.
fn has_position(page: &SurfacePage, instance: &str) -> bool {
    page.stores
        .get(NOTES)
        .expect("the fixture hosts the page-local channel")
        .has_deliverable(&BindingKey::new(instance, "notes"))
}

#[test]
fn killing_an_already_terminal_instance_reports_nothing_further() {
    let mut page = standard();
    assert!(kill(&mut page, "p2", NOW, NOW_MS).first);

    let again = kill(&mut page, "p2", NOW, NOW_MS);

    assert!(!again.first);
    assert_eq!(again.discarded, 0);
    assert_eq!(again.retry_wakeup, None);
}

#[test]
fn a_release_puts_a_parked_message_in_retention_and_wakes_its_reader() {
    let mut page = standard();
    park(&mut page, 0xb1, "later", NOW_MS + 500);
    assert_eq!(ready(&page), None, "a parked message is not on the channel");

    let released = on_release_due(&mut page, NOW_MS + 500);

    assert_eq!(released.channels, vec![NOTES.to_string()]);
    assert_eq!(released.released, 1);
    assert!(released.drops.is_quiet());
    assert_eq!(retained(&page, NOTES), vec!["later".to_string()]);
    assert_eq!(
        ready(&page),
        Some("p2"),
        "the release is an ordinary arrival on the channel"
    );
}

#[test]
fn a_fire_with_nothing_due_releases_nothing() {
    let mut page = standard();
    park(&mut page, 0xb1, "later", NOW_MS + 500);

    assert_eq!(on_release_due(&mut page, NOW_MS), Released::default());
    assert!(retained(&page, NOTES).is_empty());
}

/// A release is an ordinary arrival, so it is as accountable a cause of loss as an
/// immediate publish — and it charges at the same rung, in the same place.
#[test]
fn a_release_that_evicts_charges_the_ladder() {
    let mut page = page(W {
        noise: NoiseLevel::Fatal,
        notes_depth: 1,
        push: 1,
    });
    route(&mut page, 0xa1, "unread");
    park(&mut page, 0xb1, "later", NOW_MS + 500);

    let released = on_release_due(&mut page, NOW_MS + 500);

    assert_eq!(released.released, 1);
    let [fatal] = &released.drops.fatal[..] else {
        panic!("the fatal rung fired once: {:?}", released.drops)
    };
    assert_eq!(fatal.instance, "p2");
    assert_eq!(fatal.dropped, 1);
    assert_eq!(page.schedules.metered_drops("p2", "notes"), 1);
}

/// One fire sweeps every channel that came due, not just the first: the answer
/// names them in address order, sums what entered retention across all of them, and
/// merges every channel's ladder verdicts into one set.
#[test]
fn one_fire_releases_on_every_channel_that_came_due() {
    let mut page = two_plane_page("p1");
    park_on_both(&mut page);

    let released = on_release_due(&mut page, NOW_MS + 500);

    assert_eq!(
        released.channels,
        vec![NOTES.to_string(), OTHER.to_string()],
        "swept in address order"
    );
    assert_eq!(released.released, 2, "one message on each");
    assert_eq!(retained(&page, NOTES), vec!["later".to_string()]);
    assert_eq!(retained(&page, OTHER), vec!["later".to_string()]);
    let killed: Vec<&str> = released
        .drops
        .fatal
        .iter()
        .map(|verdict| verdict.instance.as_str())
        .collect();
    assert_eq!(
        killed,
        vec!["p2", "p1"],
        "each channel's own reader, in the order they were swept"
    );
}

/// The merge is per instance at the release site too: an instance whose positions
/// on two channels both overflowed in one fire dies once, and both losses are still
/// on its books.
#[test]
fn one_fire_overflowing_two_positions_of_one_instance_kills_it_once() {
    let mut page = two_plane_page("p2");
    park_on_both(&mut page);

    let released = on_release_due(&mut page, NOW_MS + 500);

    assert_eq!(released.channels.len(), 2);
    assert_eq!(released.released, 2);
    let [fatal] = &released.drops.fatal[..] else {
        panic!("one kill for the one instance: {:?}", released.drops)
    };
    assert_eq!(fatal.instance, "p2");
    assert_eq!(page.schedules.metered_drops("p2", "notes"), 1);
    assert_eq!(
        page.schedules.metered_drops("p2", "other"),
        1,
        "the second binding's loss is counted even though only one kill goes out"
    );
}

#[test]
fn the_release_deadline_is_stated_only_when_it_moves() {
    let mut page = standard();
    assert_eq!(release_wakeup(&mut page), None, "nothing is parked");

    park(&mut page, 0xb1, "later", NOW_MS + 500);
    assert_eq!(
        release_wakeup(&mut page),
        Some(ReleaseTimer::Arm(NOW_MS + 500))
    );
    assert_eq!(
        release_wakeup(&mut page),
        None,
        "the armed deadline is still the right one"
    );

    park(&mut page, 0xb2, "sooner", NOW_MS + 100);
    assert_eq!(
        release_wakeup(&mut page),
        Some(ReleaseTimer::Arm(NOW_MS + 100))
    );

    on_release_due(&mut page, NOW_MS + 1_000);
    assert_eq!(release_wakeup(&mut page), Some(ReleaseTimer::Disarm));
}

#[test]
fn the_retry_tick_offers_a_refused_head_again() {
    let mut page = standard();
    let mut buffer = buffer(&page, "p1");
    buffer.publish("out", "metered".to_string()).expect("bound");
    let completed = done(&page, "p1", ActivationOutcome::Ok(None), buffer);
    let completion = on_activation_done(&mut page, completed, NOW, NOW_MS);
    let correlation = correlation_of(&completion.steps.frames[0]);
    let refused = page
        .outbound
        .on_batch_result(correlation, PublishBatchOutcome::RateLimited, NOW)
        .expect("the correlation is outstanding");
    assert_eq!(
        refused.steps.retry_wakeup,
        Some(TimerChange::Arm(Millis(NOW.0 + 1_000))),
        "a refused head arms the probe"
    );

    let steps = on_retry_tick(&mut page, Millis(NOW.0 + 1_000));

    assert_eq!(steps.frames.len(), 1, "the head is offered once more");
    assert_eq!(
        correlation_of(&steps.frames[0]),
        correlation + 1,
        "a fresh correlation for a fresh attempt"
    );
}

#[test]
fn an_idle_page_has_no_retry_to_offer() {
    let mut page = standard();

    let steps = on_retry_tick(&mut page, NOW);

    assert!(steps.frames.is_empty());
    assert!(steps.dropped.is_empty());
}

fn announcement(instance: &str, dropped: u64) -> DropAnnouncement {
    DropAnnouncement {
        instance: instance.to_string(),
        port: "notes".to_string(),
        channel: NOTES.to_string(),
        dropped,
    }
}

#[test]
fn an_announcement_becomes_one_alert_and_one_toast_with_one_sentence() {
    let verdicts = DropVerdicts {
        announce: vec![announcement("p2", 3)],
        fatal: Vec::new(),
    };

    let notices = drop_notices(&verdicts);

    assert_eq!(notices.alerts.len(), 1);
    assert_eq!(notices.toasts.len(), 1);
    let sentence = announcement("p2", 3).describe();
    match &notices.alerts[0] {
        ClientFrame::Alert {
            attribution,
            severity,
            title,
            body,
        } => {
            assert_eq!(
                *attribution, None,
                "the page reports its own loss; the overflowing instance states nothing"
            );
            assert_eq!(*severity, AlertSeverity::Warning);
            assert_eq!(title, "surface input overflow on p2");
            assert_eq!(body, &sentence);
        }
        other => panic!("expected an Alert, got {other:?}"),
    }
    assert_eq!(
        toast_text(&notices.toasts[0]),
        sentence,
        "the operator reads the same words wherever the loss reaches them"
    );
}

#[test]
fn an_over_long_announcement_is_truncated_to_the_alert_caps() {
    let long = "i".repeat(MAX_ALERT_BODY_BYTES);
    let verdicts = DropVerdicts {
        announce: vec![announcement(&long, 1)],
        fatal: Vec::new(),
    };

    let notices = drop_notices(&verdicts);

    match &notices.alerts[0] {
        ClientFrame::Alert { title, body, .. } => {
            assert!(title.len() <= MAX_ALERT_TITLE_BYTES);
            assert!(body.len() <= MAX_ALERT_BODY_BYTES);
            assert!(title.ends_with("…[truncated]"));
            assert!(body.ends_with("…[truncated]"));
        }
        other => panic!("expected an Alert, got {other:?}"),
    }
}

/// A `fatal` verdict with no delta to announce says nothing here: the kill it asks
/// for carries its own account.
#[test]
fn a_fatal_verdict_alone_says_nothing() {
    let verdicts = DropVerdicts {
        announce: Vec::new(),
        fatal: vec![announcement("p2", 0)],
    };

    assert_eq!(drop_notices(&verdicts), DropNotices::default());
}

#[test]
fn every_dropped_queued_flush_becomes_one_toast() {
    let dropped = vec!["p1".to_string(), "p1".to_string(), "p2".to_string()];

    let toasts = parked_drop_notices(&dropped);

    assert_eq!(toasts.len(), 3);
    assert!(toast_text(&toasts[0]).starts_with("p1: a queued publish batch was dropped"));
    assert!(toast_text(&toasts[2]).starts_with("p2: a queued publish batch was dropped"));
}

#[test]
fn nothing_dropped_says_nothing() {
    assert!(parked_drop_notices(&[]).is_empty());
}

/// The dispatch question is asked of the *positions*, so an instance whose binding
/// holds one is ready and the publisher whose append created it is not — `p1` writes
/// the page-local channel and reads nothing on it, so it holds no position there
/// however many messages of its own it puts on the channel.
#[test]
fn readiness_is_a_question_about_one_bindings_position() {
    let mut page = standard();
    route(&mut page, 0xa1, "hello");

    assert!(has_position(&page, "p2"));
    assert!(
        !has_position(&page, "p1"),
        "the publisher reads nothing on the channel it wrote"
    );
    assert_eq!(ready(&page), Some("p2"));

    // Serve the one position and complete it, which both empties the channel's
    // deliverable and advances the rotation past `p2`: neither leaves `p1` ready.
    dispatch(&mut page, NOW_MS).expect("p2 is owed the message");
    let empty = buffer(&page, "p2");
    let completed = done(&page, "p2", ActivationOutcome::Ok(None), empty);
    on_activation_done(&mut page, completed, NOW, NOW_MS);
    assert_eq!(
        ready(&page),
        None,
        "p1's own append never made p1 ready, whichever instance the rotation resumes at"
    );
}

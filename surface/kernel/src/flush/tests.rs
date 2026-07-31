//! What an activation's completion commits, driven against real stores, a real
//! confined router with the surface's own plane policy, a real outbox plane and a
//! real publish buffer — so the assertions read the frame that would go on the
//! wire and the message a page-local reader would actually be owed.

use std::collections::HashMap;

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::publish::TimerChange;
use brenn_attach_client::router::{RouteOutcome, RouteRequest};
use brenn_attach_client::subs::Subscriptions;
use brenn_attach_proto::{ClientFrame, DeferredOpKind};
use brenn_envelope::Urgency;
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{
    Binding, LOCAL_OVERLAY_STATE_CHANNEL, NoiseLevel, Urgency as SchemaUrgency,
};
use uuid::Uuid;

use crate::activation::DropAnnouncement;
use crate::planes::SurfacePlanes;
use crate::publish_buffer::OutputSpec;
use crate::registry::{BindingKey, new_stores, reconcile_stores};
use crate::test_support::bindings as fixtures;
use crate::test_support::bindings::{output, output_at};

use super::*;

const WIRE: &str = "ephemeral:site.bar.out";
const NOTES: &str = "local:app/notes";
const EPOCH: Uuid = Uuid::from_u128(0x5107);
const PRINCIPAL: &str = "surface:bar";
const NOW: Millis = Millis(1_000);
/// The wall clock every completion here is judged at. Ahead of nothing in
/// particular: the fixture's stamps sit at the Unix epoch, so any release time
/// above zero is still ahead of its mint.
const NOW_MS: u64 = 1_000;

/// The standard wiring: `p1` writes one channel of each class plus the overlay
/// plane it has no business writing; `p2` reads the page-local channel through one
/// port and writes the wire channel, so a publish of `p1`'s can overflow a
/// *sibling's* position.
///
/// `NOTES` is declared at ring depth 1, which is what makes one publish evict what
/// the previous one left unread.
fn doc(reader_noise: NoiseLevel) -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component("p2"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![Binding {
            noise: reader_noise,
            ..fixtures::subscription("p2", "notes", NOTES, 1, 0)
        }],
        vec![
            output_at("p1", "out", WIRE, SchemaUrgency::High),
            output_at("p1", "notes", NOTES, SchemaUrgency::Low),
            output("p1", "over", LOCAL_OVERLAY_STATE_CHANNEL),
            output_at("p2", "out", WIRE, SchemaUrgency::Low),
            output(fixtures::CHROME, "over", LOCAL_OVERLAY_STATE_CHANNEL),
        ],
        vec![
            fixtures::local(NOTES, 1),
            fixtures::local(LOCAL_OVERLAY_STATE_CHANNEL, 1),
        ],
    )
}

/// A second page-local channel, for the document that rebinds `p1`'s confined
/// output away from the one its buffer captured.
const OTHER_NOTES: &str = "local:app/other";

/// The standard wiring with `p1`'s confined output rebound to `OTHER_NOTES`, and
/// that channel declared so a flush that re-resolved the port could actually route
/// there.
fn rebound_notes() -> AppliedBindings {
    let mut document = doc(NoiseLevel::Metered);
    for output in &mut document.outputs {
        if output.instance == "p1" && output.port == "notes" {
            output.channel = OTHER_NOTES.to_string();
        }
    }
    document
        .local_channels
        .push(fixtures::local(OTHER_NOTES, 1));
    AppliedBindings::apply(&document.to_body()).expect("the rebound fixture document applies")
}

/// The standard wiring with `p1`'s transportable output gone, as a document that
/// stops binding the port leaves it.
fn without_p1s_wire_output() -> AppliedBindings {
    let mut document = doc(NoiseLevel::Metered);
    document
        .outputs
        .retain(|output| !(output.instance == "p1" && output.port == "out"));
    AppliedBindings::apply(&document.to_body()).expect("the un-wired fixture document applies")
}

fn facts() -> AttachmentFacts {
    AttachmentFacts {
        version: 1,
        participant_id: PRINCIPAL.to_string(),
        session_id: "s-1".to_string(),
        heartbeat_secs: 20,
        max_body_bytes: 4_096,
        max_frame_bytes: 65_536,
        alert_granted: false,
    }
}

/// A page with the wiring in force, `p1` and `p2` registered, scheduled and
/// holding outboxes, and a confined router that has its identity.
struct Page {
    bindings: AppliedBindings,
    /// The attachment's contract, absent on a detached page.
    facts: Option<AttachmentFacts>,
    stores: SurfaceStores,
    registrations: Registrations,
    schedules: Schedules,
    router: LocalRouter<SurfacePlanes>,
    outbound: SurfaceOutbound,
}

impl Page {
    fn new(reader_noise: NoiseLevel, attached: bool) -> Self {
        let bindings = AppliedBindings::apply(&doc(reader_noise).to_body())
            .expect("the fixture document applies");
        let mut stores = new_stores(EPOCH);
        let mut subs = Subscriptions::new();
        subs.go_live();
        reconcile_stores(&bindings, &mut stores);
        let mut registrations = Registrations::new();
        let mut schedules = Schedules::new();
        let mut outbound = SurfaceOutbound::new();
        let mut planes = SurfacePlanes::new();
        planes.apply(&bindings);
        let mut router = LocalRouter::new(planes);
        router.set_principal(PRINCIPAL.to_string());
        for instance in ["p1", "p2", "chrome"] {
            registrations.register(instance, Some(&bindings), &mut stores, &mut subs);
            schedules.track(instance);
        }
        outbound.reconcile(&bindings, ["p1", "p2", "chrome"].into_iter());
        if attached {
            outbound.on_attached(&bindings, &facts(), NOW);
        }
        Self {
            bindings,
            facts: attached.then(facts),
            stores,
            registrations,
            schedules,
            router,
            outbound,
        }
    }

    /// The standard page, attached, with a `metered` reader.
    fn standard() -> Self {
        Self::new(NoiseLevel::Metered, true)
    }

    fn ctx(&mut self) -> FlushCtx<'_, SurfacePlanes> {
        FlushCtx {
            bindings: &self.bindings,
            facts: self.facts.as_ref(),
            stores: &mut self.stores,
            router: &mut self.router,
            outbound: &mut self.outbound,
            registrations: &mut self.registrations,
            schedules: &mut self.schedules,
            now_ms: NOW_MS,
            now: NOW,
        }
    }

    /// A buffer seeded for `instance`'s own output ports, with budgets wide enough
    /// that nothing here is refused at buffer time, and `deferred` as the schedule
    /// its activation was shown.
    fn buffer(&self, instance: &str, deferred: HashMap<String, Vec<Uuid>>) -> PublishBuffer {
        let mut outputs = HashMap::new();
        let mut sink_mt = HashMap::new();
        for binding in self.bindings.outputs_of(instance) {
            outputs.insert(
                binding.port.clone(),
                OutputSpec {
                    channel: binding.channel.clone(),
                    default_urgency: binding.urgency,
                },
            );
            sink_mt.insert(binding.port.clone(), 1_000_000);
        }
        PublishBuffer::new(outputs, sink_mt, 4_096, deferred)
    }

    /// Park one message of `instance`'s on `NOTES`, answering its identity.
    fn park(&mut self, instance: &str, body: &str, release_at: u64) -> Uuid {
        let stamp = stamp(0xbeef);
        let outcome = self.router.route(
            &mut self.stores,
            RouteRequest {
                channel: NOTES,
                origin: Origin::Sub(instance),
                body: body.to_string(),
                stamp,
                urgency: Urgency::Normal,
                deliver_after: Some(release_at),
            },
        );
        assert!(matches!(outcome, RouteOutcome::Parked { .. }));
        stamp.message_id
    }

    fn owed(&self, instance: &str, port: &str) -> bool {
        self.stores
            .get(NOTES)
            .expect("the fixture hosts the page-local channel")
            .has_deliverable(&BindingKey::new(instance, port))
    }

    /// What `channel`'s store retains, oldest first.
    fn retained(&self, channel: &str) -> Vec<String> {
        self.stores
            .get(channel)
            .expect("the fixture hosts the channel")
            .retained()
            .map(|(envelope, _)| envelope.body.clone())
            .collect()
    }

    fn parked_bodies(&self, instance: &str) -> Vec<String> {
        self.router
            .parked_for(&self.stores, NOTES, Origin::Sub(instance), NOW_MS)
            .into_iter()
            .map(|entry| entry.body)
            .collect()
    }
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

/// One `PublishBatch` frame's entries and ops.
fn batch_parts(frame: &ClientFrame) -> (&[BatchEntry], &[BatchDeferredOp]) {
    match frame {
        ClientFrame::PublishBatch {
            publishes,
            deferred_ops,
            ..
        } => (publishes, deferred_ops),
        other => panic!("expected a PublishBatch, got {other:?}"),
    }
}

// --- the transportable half --------------------------------------------------

#[test]
fn a_transportable_entry_carries_its_captured_channel_and_resolved_urgency() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("out", "hello".to_string()).expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    let (entries, ops) = batch_parts(&report.steps.frames[0]);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].channel, WIRE);
    assert_eq!(entries[0].body, "hello");
    assert_eq!(
        entries[0].urgency,
        Urgency::High,
        "the port's configured default, resolved at buffer time and stated concretely"
    );
    assert_eq!(entries[0].deliver_after, None);
    assert!(ops.is_empty());
    assert!(report.drops.is_quiet());
    assert!(report.refusals.is_empty());
}

#[test]
fn a_deferred_transportable_entry_states_its_release_time_verbatim() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer
        .publish_deferred("out", "later".to_string(), 9_000)
        .expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    let (entries, _) = batch_parts(&report.steps.frames[0]);
    assert_eq!(
        entries[0].deliver_after,
        Some(9_000),
        "the peer holds this channel's retention, so it decides park-vs-immediate"
    );
}

#[test]
fn a_transportable_op_rides_the_batch_frame() {
    let mut page = Page::standard();
    let parked = Uuid::from_u128(0x0ff);
    let mut buffer = page.buffer("p1", HashMap::from([("out".to_string(), vec![parked])]));
    buffer.defer_cancel("out", 0).expect("in the window");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(0));

    let (entries, ops) = batch_parts(&report.steps.frames[0]);
    assert!(
        entries.is_empty(),
        "a flush carrying only ops is still a flush"
    );
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].channel, WIRE);
    assert_eq!(ops[0].message_id, parked);
    assert_eq!(ops[0].op, DeferredOpKind::Cancel);
}

#[test]
fn a_transportable_edit_carries_its_replacement_body_and_release_time() {
    let mut page = Page::standard();
    let parked = Uuid::from_u128(0x0ff);
    let mut buffer = page.buffer("p1", HashMap::from([("out".to_string(), vec![parked])]));
    buffer
        .defer_edit("out", 0, Some("rewritten".to_string()), Some(9_000))
        .expect("in the window");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(0));

    let (_, ops) = batch_parts(&report.steps.frames[0]);
    assert_eq!(ops[0].message_id, parked);
    assert_eq!(
        ops[0].op,
        DeferredOpKind::Edit {
            body: Some("rewritten".to_string()),
            deliver_after: Some(9_000),
        },
        "both halves of the edit reach the frame, and in the right fields"
    );
}

/// A flush the wiring in force no longer admits is dropped where it is offered. The
/// peer answers a channel outside the sender's set with a protocol close and a
/// fail2ban signal, which is not a price worth paying for honestly replaying what an
/// earlier document authorized.
#[test]
fn a_flush_the_wiring_no_longer_admits_is_refused_rather_than_sent() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer
        .publish("out", "authorized then".to_string())
        .expect("bound");
    // The activation was still running when a second document stopped binding the
    // port its buffer had already resolved.
    page.bindings = without_p1s_wire_output();

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    assert!(report.steps.frames.is_empty());
    assert_eq!(report.steps.dropped, vec!["p1".to_string()]);
    assert_eq!(page.outbound.dropped_count("p1"), 1);
}

#[test]
fn a_flush_while_detached_queues_and_sends_nothing() {
    let mut page = Page::new(NoiseLevel::Metered, false);
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("out", "hello".to_string()).expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));
    assert!(report.steps.frames.is_empty());
    assert!(report.steps.dropped.is_empty());

    let steps = page.outbound.on_attached(&page.bindings, &facts(), NOW);
    assert_eq!(
        batch_parts(&steps.frames[0]).0[0].body,
        "hello",
        "the queued flush goes out when the attachment comes up"
    );
}

#[test]
fn a_buffer_with_nothing_in_it_offers_no_flush() {
    let mut page = Page::standard();
    let buffer = page.buffer("p1", HashMap::new());

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(0));

    assert_eq!(report.steps, OutboxSteps::default());
    assert!(!page.schedules.in_flight("p1"), "the activation completed");
}

// --- the confined half -------------------------------------------------------

#[test]
fn a_confined_entry_reaches_the_channels_readers_and_no_frame() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer
        .publish("notes", "page-local".to_string())
        .expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    assert!(
        report.steps.frames.is_empty(),
        "a confined publish never touches the wire"
    );
    assert!(
        page.owed("p2", "notes"),
        "the append is the delivery: every reader bound to the channel is owed it"
    );
}

#[test]
fn both_classes_from_one_activation_commit_at_their_own_authority() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("notes", "here".to_string()).expect("bound");
    buffer.publish("out", "there".to_string()).expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(2));

    let (entries, _) = batch_parts(&report.steps.frames[0]);
    assert_eq!(
        entries.iter().map(|e| e.body.as_str()).collect::<Vec<_>>(),
        vec!["there"],
        "only the transportable entry rides the frame"
    );
    assert!(page.owed("p2", "notes"));
}

#[test]
fn each_class_commits_its_entries_in_call_order() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("out", "first".to_string()).expect("bound");
    buffer.publish("out", "second".to_string()).expect("bound");
    buffer.publish("notes", "then".to_string()).expect("bound");
    buffer
        .publish("notes", "and then".to_string())
        .expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(4));

    let (entries, _) = batch_parts(&report.steps.frames[0]);
    assert_eq!(
        entries.iter().map(|e| e.body.as_str()).collect::<Vec<_>>(),
        vec!["first", "second"],
        "the frame carries its entries in the order the component wrote them"
    );
    assert_eq!(
        page.retained(NOTES),
        vec!["and then".to_string()],
        "the depth-1 channel retains the last append, so the appends went in order too"
    );
}

/// The headline invariant: every buffered entry carries the channel its port
/// resolved to at buffer time, and the flush writes that rather than resolving the
/// port again. A flush that re-resolved would land an ok'd publish on a channel a
/// *later* document authorized — the confused-deputy write the model forbids.
#[test]
fn a_buffered_publish_routes_to_the_channel_that_authorized_it() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer
        .publish("notes", "captured".to_string())
        .expect("bound");
    // A second document rebinds the port, and its channel has a store — so a flush
    // that re-resolved would route there successfully rather than failing loudly.
    page.bindings = rebound_notes();
    reconcile_stores(&page.bindings, &mut page.stores);

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    assert!(report.refusals.is_empty());
    assert_eq!(page.retained(NOTES), vec!["captured".to_string()]);
    assert!(
        page.retained(OTHER_NOTES).is_empty(),
        "the resolution that authorized the publish is the one that routed it"
    );
}

#[test]
fn a_deferred_confined_publish_parks_and_reaches_nobody() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer
        .publish_deferred("notes", "soon".to_string(), 9_000)
        .expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    assert!(report.steps.frames.is_empty());
    assert!(
        !page.owed("p2", "notes"),
        "a parked message is not on the channel"
    );
    assert_eq!(page.parked_bodies("p1"), vec!["soon".to_string()]);
}

#[test]
fn a_full_deferred_set_drops_the_schedule_and_counts_it() {
    let mut page = Page::standard();
    // The set's cap is the channel's ring depth, which the fixture declares as one.
    page.park("p1", "first", 9_000);
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer
        .publish_deferred("notes", "second".to_string(), 9_000)
        .expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    assert!(
        report.refusals.is_empty(),
        "a full set is not a plane refusal"
    );
    assert_eq!(page.parked_bodies("p1"), vec!["first".to_string()]);
    assert_eq!(page.schedules.deferred_dropped("p1"), 1);
}

#[test]
fn a_confined_op_reaches_the_instances_own_parked_message() {
    let mut page = Page::standard();
    let parked = page.park("p1", "first", 9_000);
    let mut buffer = page.buffer("p1", HashMap::from([("notes".to_string(), vec![parked])]));
    buffer
        .defer_edit("notes", 0, Some("rewritten".to_string()), None)
        .expect("in the window");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(0));

    assert!(report.steps.frames.is_empty(), "a confined op applies here");
    assert_eq!(page.parked_bodies("p1"), vec!["rewritten".to_string()]);
}

/// Control ops apply ahead of the same activation's publishes: an op names a
/// message an *earlier* activation parked, and applying it first keeps this
/// activation's own publishes out of its way. `NOTES` caps its deferred set at one,
/// so the two orders are distinguishable — publishes-first would drop the new
/// schedule at the cap and then cancel the old one, losing both.
#[test]
fn a_control_op_applies_ahead_of_the_same_activations_publishes() {
    let mut page = Page::standard();
    let parked = page.park("p1", "first", 9_000);
    let mut buffer = page.buffer("p1", HashMap::from([("notes".to_string(), vec![parked])]));
    buffer.defer_cancel("notes", 0).expect("in the window");
    buffer
        .publish_deferred("notes", "next".to_string(), 9_000)
        .expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    assert!(report.refusals.is_empty());
    assert_eq!(page.parked_bodies("p1"), vec!["next".to_string()]);
    assert_eq!(
        page.schedules.deferred_dropped("p1"),
        0,
        "the cancel freed the slot the publish then took"
    );
    assert_eq!(page.schedules.deferred_races("p1"), 0);
}

#[test]
fn a_confined_op_naming_a_released_message_is_counted_as_a_race() {
    let mut page = Page::standard();
    let mut buffer = page.buffer(
        "p1",
        HashMap::from([("notes".to_string(), vec![Uuid::from_u128(0xdead)])]),
    );
    buffer.defer_cancel("notes", 0).expect("in the window");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(0));

    assert!(
        report.refusals.is_empty(),
        "the race is benign, not a refusal"
    );
    assert_eq!(page.schedules.deferred_races("p1"), 1);
}

#[test]
fn a_plane_refusing_a_body_reports_its_reason_and_routes_nothing() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer
        .publish(
            "over",
            r#"{"v":1,"holder":"p1","since_stamp":0}"#.to_string(),
        )
        .expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));

    assert_eq!(report.refusals.len(), 1);
    assert_eq!(report.refusals[0].port, "over");
    assert_eq!(report.refusals[0].channel, LOCAL_OVERLAY_STATE_CHANNEL);
    assert!(
        report.refusals[0]
            .reason
            .contains("only the surface's chrome instance"),
        "the plane's own words: {}",
        report.refusals[0].reason
    );
    assert!(
        page.router.policy().overlay().is_none(),
        "a refused body was never observed"
    );
}

#[test]
fn a_refused_confined_edit_changes_nothing_and_reports_its_reason() {
    let mut page = Page::standard();
    let stamp = stamp(0xfeed);
    let parked = match page.router.route(
        &mut page.stores,
        RouteRequest {
            channel: LOCAL_OVERLAY_STATE_CHANNEL,
            origin: Origin::Sub("chrome"),
            body: r#"{"v":1,"holder":"p1","since_stamp":0}"#.to_string(),
            stamp,
            urgency: Urgency::Normal,
            deliver_after: Some(9_000),
        },
    ) {
        RouteOutcome::Parked { .. } => stamp.message_id,
        _ => panic!("the fixture's release time is ahead of its mint, so it parks"),
    };
    // Chrome's own edit, refused because the replacement body names a holder the
    // wiring does not declare.
    let mut buffer = page.buffer(
        "chrome",
        HashMap::from([("over".to_string(), vec![parked])]),
    );
    buffer
        .defer_edit(
            "over",
            0,
            Some(r#"{"v":1,"holder":"ghost","since_stamp":1}"#.to_string()),
            None,
        )
        .expect("in the window");

    let report = flush_ok(&mut page.ctx(), "chrome", buffer, stamps(0));

    assert_eq!(report.refusals.len(), 1);
    assert!(
        report.refusals[0]
            .reason
            .contains("not a declared instance")
    );
    assert_eq!(
        page.router
            .parked_for(
                &page.stores,
                LOCAL_OVERLAY_STATE_CHANNEL,
                Origin::Sub("chrome"),
                NOW_MS
            )
            .into_iter()
            .map(|entry| entry.body)
            .collect::<Vec<_>>(),
        vec![r#"{"v":1,"holder":"p1","since_stamp":0}"#.to_string()],
        "a refused edit leaves the parked message alone"
    );
}

#[test]
fn a_confined_publish_that_evicts_a_siblings_position_charges_the_ladder() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("notes", "one".to_string()).expect("bound");
    buffer.publish("notes", "two".to_string()).expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(2));

    assert!(
        report.drops.is_quiet(),
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
fn a_fatal_binding_evicted_by_a_publish_asks_for_a_kill_naming_its_own_instance() {
    let mut page = Page::new(NoiseLevel::Fatal, true);
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("notes", "one".to_string()).expect("bound");
    buffer.publish("notes", "two".to_string()).expect("bound");

    let report = flush_ok(&mut page.ctx(), "p1", buffer, stamps(2));

    assert_eq!(
        report.drops.fatal,
        Some(DropAnnouncement {
            instance: "p2".to_string(),
            port: "notes".to_string(),
            channel: NOTES.to_string(),
            dropped: 1,
        }),
        "the publisher is p1; the instance to kill is the reader that lost the message"
    );
    assert!(
        report
            .drops
            .fatal
            .expect("just asserted")
            .describe()
            .starts_with("p2: dropped 1"),
        "the kill reason names the instance the announcement carries"
    );
}

#[test]
fn an_err_discards_the_work_and_counts_the_failure() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("out", "hello".to_string()).expect("bound");
    buffer.publish("notes", "here".to_string()).expect("bound");

    discard_err(&mut page.ctx(), "p1", buffer);

    assert!(!page.schedules.in_flight("p1"));
    assert_eq!(page.schedules.activation_failures("p1"), 1);
    assert!(
        !page.owed("p2", "notes"),
        "nothing left the failed activation"
    );
}

#[test]
fn a_kill_strips_positions_discards_the_queue_and_reports_once() {
    let mut page = Page::new(NoiseLevel::Metered, false);
    for body in ["one", "two"] {
        let mut buffer = page.buffer("p2", HashMap::new());
        buffer.publish("out", body.to_string()).expect("bound");
        flush_ok(&mut page.ctx(), "p2", buffer, stamps(1));
    }
    // Something p2 is owed, so the position being stripped is observable.
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer
        .publish("notes", "unread".to_string())
        .expect("bound");
    flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));
    assert!(page.owed("p2", "notes"));

    let killed = kill(&mut page.ctx(), "p2");

    assert!(killed.first);
    assert_eq!(killed.discarded, 2, "both queued flushes died with it");
    assert_eq!(
        killed.retry_wakeup, None,
        "a detached page never armed the retry"
    );
    assert!(page.registrations.is_failed("p2"));
    assert!(
        !page.owed("p2", "notes"),
        "nothing is delivered to a terminal instance"
    );
    assert!(!page.schedules.in_flight("p2"));

    let again = kill(&mut page.ctx(), "p2");
    assert!(!again.first, "a failure is reported once");
    assert_eq!(again.discarded, 0);
}

#[test]
fn a_kill_that_unblocks_the_last_queue_disarms_the_retry() {
    let mut page = Page::standard();
    let mut first = page.buffer("p2", HashMap::new());
    first.publish("out", "one".to_string()).expect("bound");
    let sent = flush_ok(&mut page.ctx(), "p2", first, stamps(1));
    let correlation = match &sent.steps.frames[0] {
        ClientFrame::PublishBatch { correlation, .. } => *correlation,
        other => panic!("expected a PublishBatch, got {other:?}"),
    };
    // A refusal puts the head back on a free wire, which is the one state the
    // retry timer exists for.
    page.outbound
        .on_batch_result(
            correlation,
            brenn_attach_proto::PublishBatchOutcome::RateLimited,
            NOW,
        )
        .expect("a correlation this page sent");

    let killed = kill(&mut page.ctx(), "p2");

    assert_eq!(killed.discarded, 1);
    assert_eq!(killed.retry_wakeup, Some(TimerChange::Disarm));
}

#[test]
#[should_panic(expected = "one envelope stamp per buffered publish")]
fn a_stamp_count_short_of_the_publishes_panics() {
    let mut page = Page::standard();
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("out", "hello".to_string()).expect("bound");
    flush_ok(&mut page.ctx(), "p1", buffer, stamps(0));
}

#[test]
#[should_panic(expected = "before the page had an identity")]
fn a_confined_publish_before_the_page_has_an_identity_panics() {
    let mut page = Page::standard();
    let mut planes = SurfacePlanes::new();
    planes.apply(&page.bindings);
    page.router = LocalRouter::new(planes);
    let mut buffer = page.buffer("p1", HashMap::new());
    buffer.publish("notes", "here".to_string()).expect("bound");
    flush_ok(&mut page.ctx(), "p1", buffer, stamps(1));
}

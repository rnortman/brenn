//! The two phases of an attachment, driven through the page's own methods against
//! real stores, a real subscription plane, a real confined router carrying the
//! surface's plane policy and real outboxes — so the assertions read the frames
//! that would go on the wire and the state the next activation would be assembled
//! from.

use brenn_attach_client::publish::FlushBatch;
use brenn_attach_client::router::{
    GuardedBody, MessageStamp, Origin, PlanePolicy, RouteOutcome, RouteRequest,
};
use brenn_attach_proto::{
    BatchEntry, DeferredViewEntry, GapInfo, GapReason, SubscribeOutcome, Urgency as WireUrgency,
};
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{Binding, LOCAL_OVERLAY_STATE_CHANNEL, NoiseLevel};

use crate::activation::DropAnnouncement;

use crate::core::PublishStatus;
use crate::outbound::{PortPublish, resolve_output};
use crate::registry::BindingKey;
use crate::test_support::bindings as fixtures;
use crate::test_support::bindings::output;
use crate::test_support::pages;
use crate::test_support::pages::PRINCIPAL;

use super::*;

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE: &str = "brenn:site.bar.in";
const OTHER_WIRE: &str = "brenn:site.bar.other";
const OUT: &str = "ephemeral:site.bar.out";
const NOTES: &str = "local:app/notes";
const EPOCH: Uuid = Uuid::from_u128(0x5107);
const NOW: Millis = Millis(1_000);

/// The knobs the fixture document varies. Everything else about it is constant, so
/// two documents differing in one of these differ in exactly one way.
struct W {
    /// Distinguishes two byte-different documents that wire the page identically
    /// in every way the page acts on.
    kind: &'static str,
    /// `p1/in`'s push depth on `WIRE`. Lowering it shrinks the channel's store.
    push: u64,
    /// `p1/in`'s loudness rung, which decides what a retirement out from under its
    /// position asks the caller for.
    noise: NoiseLevel,
    /// Whether `WIRE` is bound at all — false is the document that drops it.
    wire: bool,
    /// Whether `p1/in` reads `OTHER_WIRE` instead of `WIRE` — the document that
    /// closes one subscription and opens another in one pass.
    alt_wire: bool,
    /// Whether the page-local `NOTES` channel is declared and written.
    notes: bool,
}

impl Default for W {
    fn default() -> Self {
        Self {
            kind: "protobar",
            push: 4,
            noise: NoiseLevel::Metered,
            wire: true,
            alt_wire: false,
            notes: true,
        }
    }
}

/// `p1` reads one wire channel and writes one of each class; chrome exists because
/// every surface has one.
fn doc(w: W) -> BindingsDocument {
    let mut subscriptions = Vec::new();
    if w.wire {
        let channel = if w.alt_wire { OTHER_WIRE } else { WIRE };
        subscriptions.push(Binding {
            noise: w.noise,
            ..fixtures::subscription("p1", "in", channel, w.push, 2)
        });
    }
    let mut outputs = vec![
        output("p1", "out", OUT),
        output(fixtures::CHROME, "over", LOCAL_OVERLAY_STATE_CHANNEL),
    ];
    let mut local_channels = vec![fixtures::local(LOCAL_OVERLAY_STATE_CHANNEL, 1)];
    if w.notes {
        outputs.push(output("p1", "notes", NOTES));
        local_channels.push(fixtures::local(NOTES, 2));
    }
    fixtures::doc(
        vec![
            fixtures::component_of_kind("p1", w.kind),
            fixtures::component_of_kind(fixtures::CHROME, "chrome"),
        ],
        subscriptions,
        outputs,
        local_channels,
    )
}

fn body(w: W) -> String {
    doc(w).to_body()
}

fn ack(replay_count: u32, gap: Option<GapInfo>) -> SubscribeAck {
    SubscribeAck {
        frames: Vec::new(),
        live: true,
        replay_count,
        gap,
    }
}

fn page() -> SurfacePage {
    SurfacePage::new(CONFIG.to_string(), EPOCH)
}

/// Phase 1 plus the config channel's acknowledgement — the state phase 2 runs
/// from. Answers the frames phase 1 sent.
fn attach(page: &mut SurfacePage) -> Vec<ClientFrame> {
    let frames = page.on_attached(pages::facts());
    page.subs
        .on_subscribe_result(CONFIG, SubscribeOutcome::Ok, 1, None)
        .expect("the config channel is pending");
    frames
}

/// A page with `p1` registered and scheduled, one document in force, and the
/// config channel acknowledged.
fn configured(w: W) -> SurfacePage {
    let mut page = page();
    attach(&mut page);
    page.registrations
        .register("p1", None, &mut page.stores, &mut page.subs);
    page.schedules.track("p1");
    let applied = page
        .apply_config(&body(w), NOW)
        .expect("the fixture document applies");
    ack_subscribes(&mut page, &applied.frames);
    page
}

/// Answer every `Subscribe` in `frames`, so the channels are `Active` and a later
/// release emits its `Unsubscribe` rather than deferring it.
fn ack_subscribes(page: &mut SurfacePage, frames: &[ClientFrame]) {
    for frame in frames {
        if let ClientFrame::Subscribe { channel, .. } = frame {
            page.subs
                .on_subscribe_result(channel, SubscribeOutcome::Ok, 0, None)
                .expect("the fixture answers a pending channel");
        }
    }
}

fn subscribed(frames: &[ClientFrame]) -> Vec<(&str, u64, u64)> {
    frames
        .iter()
        .filter_map(|f| match f {
            ClientFrame::Subscribe {
                channel,
                push_depth,
                retain_depth,
                ..
            } => Some((channel.as_str(), *push_depth, *retain_depth)),
            _ => None,
        })
        .collect()
}

fn unsubscribed(frames: &[ClientFrame]) -> Vec<&str> {
    frames
        .iter()
        .filter_map(|f| match f {
            ClientFrame::Unsubscribe { channel } => Some(channel.as_str()),
            _ => None,
        })
        .collect()
}

fn env(channel: &str, body: &str) -> MessageEnvelope {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&(channel, body), &mut hasher);
    MessageEnvelope {
        message_id: Uuid::from_u128(u128::from(std::hash::Hasher::finish(&hasher))),
        source: "test".into(),
        channel: channel.into(),
        sender: format!("{PRINCIPAL}#p1"),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
        body: body.into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Brenn,
    }
}

/// One flush of `p1`'s, small enough that no cap refuses it.
fn flush_batch() -> FlushBatch {
    FlushBatch {
        entries: vec![BatchEntry {
            channel: OUT.to_string(),
            body: r#"{"n":1}"#.to_string(),
            urgency: WireUrgency::Normal,
            deliver_after: None,
        }],
        ops: Vec::new(),
    }
}

/// Offer one flush of `p1`'s against the wiring in force and the attachment's own
/// contract, as a completing activation does.
fn queue_flush(page: &mut SurfacePage) -> OutboxSteps<String> {
    let SurfacePage {
        connect, outbound, ..
    } = page;
    let wiring = connect.bindings().expect("the document is in force");
    outbound.flush(wiring, connect.facts(), "p1", flush_batch(), NOW)
}

/// Park one message of `sender`'s on `NOTES`, well ahead of its mint.
fn park(page: &mut SurfacePage, sender: &str) {
    let stamp = MessageStamp {
        message_id: Uuid::from_u128(0xbeef),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
    };
    let outcome = page.router.route(
        &mut page.stores,
        RouteRequest {
            channel: NOTES,
            origin: Origin::Sub(sender),
            body: "{}".to_string(),
            stamp,
            urgency: Urgency::Normal,
            deliver_after: Some(9_000),
        },
    );
    assert!(
        matches!(outcome, RouteOutcome::Parked { .. }),
        "the fixture's release time is ahead of its mint, so it parks"
    );
}

#[test]
fn a_fresh_page_holds_the_reserved_planes_and_nothing_else() {
    let page = page();
    assert!(page.bindings().is_none());
    assert!(page.registrations.is_empty());
    assert!(page.subs.held_channels().is_empty());
    // The reserved planes are contract-declared, so they exist before any peer has
    // spoken; nothing else does.
    let channels: Vec<&str> = page.stores.channels().collect();
    assert!(channels.iter().all(|c| c.starts_with("local:brenn/")));
    assert!(!channels.is_empty());
}

#[test]
fn phase_one_subscribes_the_config_channel_and_nothing_else() {
    let mut page = page();
    let frames = page.on_attached(pages::facts());
    assert_eq!(subscribed(&frames), vec![(CONFIG, 1, 1)]);
    assert_eq!(frames.len(), 1);
}

#[test]
fn phase_one_takes_the_attachment_identity() {
    let mut page = page();
    assert_eq!(page.router.principal(), None);
    page.on_attached(pages::facts());
    assert_eq!(page.router.principal(), Some(PRINCIPAL));
}

#[test]
fn phase_one_clears_the_deferred_view_mirror() {
    let mut page = page();
    page.views.on_view(
        OUT.to_string(),
        Some("p1".to_string()),
        vec![DeferredViewEntry {
            message_id: Uuid::from_u128(1),
            body: "{}".to_string(),
            deliver_after: 9_000,
        }],
    );
    page.on_attached(pages::facts());
    // The peer re-seeds only the nonempty sets behind its `Welcome`, so a retained
    // mirror would show a schedule that released while the page was away.
    assert!(page.views.is_empty());
}

#[test]
fn the_config_ack_is_the_connect_states_answer() {
    let mut page = page();
    page.on_attached(pages::facts());
    assert!(page.on_config_ack(&ack(1, None)).is_ok());
    assert!(page.on_config_ack(&ack(0, None)).is_err());
    assert!(
        page.on_config_ack(&ack(
            1,
            Some(GapInfo {
                reason: GapReason::EpochChanged,
            })
        ))
        .is_err()
    );
}

#[test]
fn phase_two_puts_the_wiring_in_force_and_subscribes_what_it_binds() {
    let mut page = page();
    attach(&mut page);
    page.registrations
        .register("p1", None, &mut page.stores, &mut page.subs);
    let applied = page
        .apply_config(&body(W::default()), NOW)
        .expect("the fixture document applies");
    assert!(applied.first_of_attachment);
    // Nothing was in force before it, so there is nothing for it to differ from.
    assert!(!applied.wiring_changed);
    assert!(page.bindings().is_some());
    // max(4, 2) folded across the one binding on it.
    assert_eq!(page.stores.get(WIRE).expect("wire store").depth(), 4);
    assert_eq!(subscribed(&applied.frames), vec![(WIRE, 4, 2)]);
    assert_eq!(
        page.stores
            .get(WIRE)
            .expect("wire store")
            .readers()
            .cloned()
            .collect::<Vec<_>>(),
        vec![BindingKey::new("p1", "in")]
    );
}

#[test]
fn phase_two_hands_the_wiring_to_the_plane_policy() {
    let page = configured(W::default());
    // The overlay guard's holder rule is answerable only against a wiring: a
    // declared holder passes, an undeclared one is refused.
    let carried = page.router.policy().guard(
        LOCAL_OVERLAY_STATE_CHANNEL,
        Origin::Sub("chrome"),
        r#"{"v":1,"holder":"p1","since_stamp":0}"#.to_string(),
    );
    assert!(matches!(carried, GuardedBody::Carry(_)));
    let refused = page.router.policy().guard(
        LOCAL_OVERLAY_STATE_CHANNEL,
        Origin::Sub("chrome"),
        r#"{"v":1,"holder":"ghost","since_stamp":0}"#.to_string(),
    );
    assert!(matches!(refused, GuardedBody::Refused(_)));
}

#[test]
fn a_registration_made_before_the_first_document_gets_its_outbox() {
    let page = configured(W::default());
    assert!(page.outbound.is_registered("p1"));
}

#[test]
fn phase_two_closes_the_outbox_of_an_instance_that_deregistered() {
    let mut page = configured(W::default());
    queue_flush(&mut page);
    // Deregistered under the wire, with its flush still queued behind the one it
    // has outstanding.
    queue_flush(&mut page);
    page.registrations
        .deregister("p1", &mut page.stores, &mut page.subs);
    let applied = page
        .apply_config(&body(W::default()), NOW)
        .expect("the same document applies again");
    assert!(!page.outbound.is_registered("p1"));
    let [lost] = &applied.lost_flushes[..] else {
        panic!("one flush died with the outbox: {:?}", applied.lost_flushes)
    };
    assert_eq!(
        lost.instance, "p1",
        "the loss names whose committed writes vanished"
    );
}

#[test]
fn a_flush_queued_before_the_attachment_goes_out_at_phase_two() {
    let mut page = configured(W::default());
    let detached = page.on_detached();
    assert!(detached.steps.frames.is_empty());
    // Queued with no wire under it.
    let steps = queue_flush(&mut page);
    assert!(steps.frames.is_empty());
    attach(&mut page);
    let applied = page
        .apply_config(&body(W::default()), NOW)
        .expect("the same document applies again");
    assert!(matches!(
        applied.steps.frames.as_slice(),
        [ClientFrame::PublishBatch { .. }]
    ));
    assert!(applied.steps.dropped.is_empty());
}

#[test]
fn a_byte_equal_document_across_a_reconnect_changes_nothing() {
    let mut page = configured(W::default());
    page.on_detached();
    attach(&mut page);
    let applied = page
        .apply_config(&body(W::default()), NOW)
        .expect("the same document applies again");
    assert!(applied.first_of_attachment);
    assert!(!applied.wiring_changed);
}

#[test]
fn a_changed_document_reports_changed_wiring() {
    let mut page = configured(W::default());
    page.on_detached();
    attach(&mut page);
    let applied = page
        .apply_config(
            &body(W {
                kind: "protobaz",
                ..W::default()
            }),
            NOW,
        )
        .expect("the second document applies");
    assert!(applied.wiring_changed);
}

#[test]
fn a_second_document_mid_attachment_reconciles_against_it() {
    let mut page = configured(W::default());
    assert_eq!(page.subs.refcount(WIRE), 1);
    let applied = page
        .apply_config(
            &body(W {
                wire: false,
                ..W::default()
            }),
            NOW,
        )
        .expect("the second document applies");
    // Not phase 2 proper, and still reconciled: the channel it stopped binding is
    // closed and its store is gone.
    assert!(!applied.first_of_attachment);
    assert!(applied.wiring_changed);
    assert_eq!(unsubscribed(&applied.frames), vec![WIRE]);
    assert_eq!(page.subs.refcount(WIRE), 0);
    assert!(page.stores.get(WIRE).is_none());
}

#[test]
fn a_survivor_whose_subscription_died_with_the_connection_is_resubscribed() {
    let mut page = configured(W::default());
    page.on_detached();
    // The reference survived the detach, so the reconcile acquires nothing — the
    // resubscribe pass is the only thing that can put this channel back on the
    // wire.
    assert_eq!(page.subs.refcount(WIRE), 1);
    attach(&mut page);
    let applied = page
        .apply_config(&body(W::default()), NOW)
        .expect("the same document applies again");
    assert_eq!(subscribed(&applied.frames), vec![(WIRE, 4, 2)]);
}

#[test]
fn a_shrunk_depth_charges_the_lagging_position() {
    let mut page = configured(W::default());
    let store = page.stores.get_mut(WIRE).expect("wire store");
    for n in 0..4 {
        store.insert(env(WIRE, &format!("m{n}")));
    }
    let applied = page
        .apply_config(
            &body(W {
                push: 1,
                ..W::default()
            }),
            NOW,
        )
        .expect("the shallower document applies");
    // An operator lowering a depth is as accountable a cause of loss as a burst:
    // the retirement is charged at the shrink, and the binding's noise is
    // `Metered`, so the ladder stays quiet about it. Four messages against a fold
    // that goes from `max(4,2)` to `max(1,2)` retires exactly the two oldest, and
    // the position had been served none of them.
    assert_eq!(page.schedules.metered_drops("p1", "in"), 2);
    assert!(applied.drops.is_quiet());
}

/// The loud half of the same shrink, which is enforceable only through what
/// `apply_config` hands back: the page folds the ladder's verdicts into
/// `Configured::drops`, and the reconcile still completes around them.
#[test]
fn a_shrink_that_retires_a_fatal_bindings_position_asks_for_the_kill() {
    let loud = W {
        noise: NoiseLevel::Fatal,
        ..W::default()
    };
    let mut page = configured(loud);
    let store = page.stores.get_mut(WIRE).expect("wire store");
    for n in 0..4 {
        store.insert(env(WIRE, &format!("m{n}")));
    }
    let applied = page
        .apply_config(
            &body(W {
                noise: NoiseLevel::Fatal,
                push: 1,
                ..W::default()
            }),
            NOW,
        )
        .expect("the shallower document applies");

    let announcement = DropAnnouncement {
        instance: "p1".to_string(),
        port: "in".to_string(),
        channel: WIRE.to_string(),
        dropped: 2,
    };
    assert_eq!(
        applied.drops.fatal,
        vec![announcement.clone()],
        "the kill ends the instance, so a fatal rung announces at the retirement"
    );
    assert_eq!(applied.drops.announce, vec![announcement]);
    assert_eq!(
        page.schedules.metered_drops("p1", "in"),
        2,
        "the rungs are cumulative"
    );
    // And the pass ran to the end around it.
    assert_eq!(subscribed(&applied.frames), vec![(WIRE, 1, 2)]);
    assert_eq!(page.stores.get(WIRE).expect("wire store").depth(), 2);
}

/// `Configured::frames` is in order — the subscriptions this document closes, then
/// the ones it opens. Close-before-open is what keeps one channel from carrying two
/// statements at the peer mid-swap, so the interleaving is read off the frames
/// themselves rather than through the filtering helpers.
#[test]
fn phase_two_closes_a_subscription_before_it_opens_its_replacement() {
    let mut page = configured(W::default());
    let applied = page
        .apply_config(
            &body(W {
                alt_wire: true,
                ..W::default()
            }),
            NOW,
        )
        .expect("the rebinding document applies");
    match applied.frames.as_slice() {
        [
            ClientFrame::Unsubscribe { channel },
            ClientFrame::Subscribe {
                channel: opened,
                push_depth,
                retain_depth,
                ..
            },
        ] => {
            assert_eq!(channel, WIRE);
            assert_eq!(opened, OTHER_WIRE);
            assert_eq!((*push_depth, *retain_depth), (4, 2));
        }
        other => panic!("expected the close then the open, got {other:?}"),
    }
}

#[test]
fn a_dropped_confined_store_charges_its_owner_the_schedules_it_lost() {
    let mut page = configured(W::default());
    park(&mut page, "p1");
    assert_eq!(page.schedules.deferred_dropped("p1"), 0);
    page.apply_config(
        &body(W {
            notes: false,
            ..W::default()
        }),
        NOW,
    )
    .expect("the document without the page-local channel applies");
    assert!(page.stores.get(NOTES).is_none());
    // A dropped schedule is the only account of a timer the component believes it
    // set, so it lands on that component's counter.
    assert_eq!(page.schedules.deferred_dropped("p1"), 1);
}

#[test]
fn a_schedule_no_registered_instance_answers_for_is_left_uncounted() {
    let mut page = configured(W::default());
    park(&mut page, "ghost");
    page.apply_config(
        &body(W {
            notes: false,
            ..W::default()
        }),
        NOW,
    )
    .expect("the document without the page-local channel applies");
    // Nobody's counter to put it on, and nothing to panic about: the schedules are
    // as lost either way.
    assert_eq!(page.schedules.deferred_dropped("p1"), 0);
    assert_eq!(page.schedules.deferred_dropped("ghost"), 0);
}

#[test]
fn a_detach_answers_the_publishes_the_connection_carried() {
    let mut page = configured(W::default());
    let SurfacePage {
        connect, outbound, ..
    } = &mut page;
    let wiring = connect.bindings().expect("the document is in force");
    let out = resolve_output(wiring, "p1", "out", None).expect("p1 binds the port");
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
    let detached = page.on_detached();
    assert_eq!(
        detached.answers,
        vec![PublishAnswer::Port {
            instance: "p1".to_string(),
            port: "out".to_string(),
            correlation: 7,
            status: PublishStatus::ConnectionLost,
        }]
    );
}

#[test]
fn a_detach_keeps_everything_that_belongs_to_the_page() {
    let mut page = configured(W::default());
    page.stores
        .get_mut(WIRE)
        .expect("wire store")
        .insert(env(WIRE, "m0"));
    page.on_detached();
    assert!(page.bindings().is_some());
    assert!(page.registrations.is_registered("p1"));
    assert_eq!(page.router.principal(), Some(PRINCIPAL));
    // The reference and the retained message are the page's, not the
    // connection's; the wire subscription is the connection's and is gone.
    assert_eq!(page.subs.refcount(WIRE), 1);
    assert!(!page.subs.is_active(WIRE));
    assert_eq!(
        page.stores
            .get(WIRE)
            .expect("wire store")
            .retained()
            .count(),
        1
    );
}

#[test]
fn a_detach_with_no_attachment_behind_it_is_tolerated() {
    let mut page = page();
    let detached = page.on_detached();
    assert_eq!(detached, Detached::default());
}

#[test]
fn an_unusable_document_is_refused_whole() {
    let mut page = page();
    attach(&mut page);
    let before: Vec<String> = page.stores.channels().map(str::to_string).collect();
    let err = page
        .apply_config("{\"v\":99}", NOW)
        .expect_err("a document from another version is unusable");
    assert!(err.contains("unusable"));
    assert!(page.bindings().is_none());
    assert_eq!(
        page.stores.channels().collect::<Vec<_>>(),
        before.iter().map(String::as_str).collect::<Vec<_>>()
    );
}

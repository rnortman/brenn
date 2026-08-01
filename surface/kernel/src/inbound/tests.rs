//! The five routed server frames, driven through [`on_server_frame`] against a
//! real page — real stores, a real subscription plane, real outboxes and the
//! surface's own plane policy — so each assertion reads either the state the next
//! activation would be assembled from or the frames that would go on the wire.

use brenn_attach_client::publish::{FlushBatch, TimerChange};
use brenn_attach_client::subs::SubscriptionDepths;
use brenn_attach_proto::{
    BatchEntry, Cursor, DeferredViewEntry, GapReason, Urgency as WireUrgency,
};
use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency};
use brenn_surface_schema::bindings::BindingsDocument;
use brenn_surface_schema::{Binding, NoiseLevel};
use uuid::Uuid;

use crate::activation::DropAnnouncement;
use crate::outbound::PublishStatus;
use crate::outbound::{PortPublish, resolve_output};
use crate::registry::BindingKey;
use crate::test_support::bindings as fixtures;
use crate::test_support::pages;
use crate::test_support::pages::PRINCIPAL;

use super::*;

const CONFIG: &str = "ephemeral:site.surface.bar.bindings";
const WIRE: &str = "brenn:site.bar.in";
/// A second wire channel, for the document that adds one to what is already in
/// force.
const WIRE_TWO: &str = "brenn:site.bar.in2";
const OUT: &str = "ephemeral:site.bar.out";
const EPOCH: Uuid = Uuid::from_u128(0x1_11b0);
const NOW: Millis = Millis(1_000);

/// The knobs the fixture document varies: `p1/in`'s window on `WIRE`, and what
/// its loudness rung asks of a loss.
struct W {
    push: u64,
    retain: u64,
    noise: NoiseLevel,
}

impl Default for W {
    fn default() -> Self {
        Self {
            push: 4,
            retain: 2,
            noise: NoiseLevel::Metered,
        }
    }
}

/// `p1` reads one wire channel and writes one that crosses the wire; chrome exists
/// because every surface has one.
fn doc(w: W) -> BindingsDocument {
    fixtures::doc(
        vec![
            fixtures::component("p1"),
            fixtures::component(fixtures::CHROME),
        ],
        vec![Binding {
            noise: w.noise,
            ..fixtures::subscription("p1", "in", WIRE, w.push, w.retain)
        }],
        vec![fixtures::output("p1", "out", OUT)],
        Vec::new(),
    )
}

fn body(w: W) -> String {
    doc(w).to_body()
}

fn cursor(token: &str) -> Cursor {
    serde_json::from_value(serde_json::Value::String(token.to_string()))
        .expect("a cursor is a JSON string")
}

fn subscribe_result(channel: &str, replay_count: u32, gap: Option<GapInfo>) -> ServerFrame {
    ServerFrame::SubscribeResult {
        channel: channel.to_string(),
        outcome: SubscribeOutcome::Ok,
        replay_count,
        gap,
    }
}

/// A one-row delivery pass — a live row, which is what most of this suite feeds.
fn deliver(channel: &str, body: &str, seq: u64, dropped: u64) -> ServerFrame {
    deliver_pass(channel, &[(body, seq, dropped)])
}

/// A whole delivery pass in one frame: one row per `(body, seq, dropped)`, in
/// frame order.
fn deliver_pass(channel: &str, rows: &[(&str, u64, u64)]) -> ServerFrame {
    ServerFrame::Deliver {
        channel: channel.to_string(),
        rows: rows
            .iter()
            .map(|&(body, seq, dropped)| DeliverRow {
                envelope: env(channel, body),
                seq,
                cursor: cursor(&format!("c{seq}")),
                dropped,
            })
            .collect(),
    }
}

/// The one document a config pass carried, asserting it carried exactly one.
fn only_configured(inbound: Inbound) -> crate::page::Configured {
    let mut configured = inbound.configured;
    assert_eq!(configured.len(), 1, "the pass carried one document");
    configured.remove(0)
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

fn page() -> SurfacePage {
    SurfacePage::new(CONFIG.to_string(), EPOCH)
}

fn route(page: &mut SurfacePage, frame: ServerFrame) -> Inbound {
    on_server_frame(page, frame, NOW).expect("the fixture frame is reconcilable")
}

/// Phase 1 and the config channel's acknowledgement, with `p1` registered and
/// scheduled — the state phase 2 runs from.
fn attached() -> SurfacePage {
    let mut page = page();
    page.on_attached(pages::facts());
    // Acknowledged through the routing under test rather than the plane directly:
    // the config channel's own ack is one of this module's answers.
    route(&mut page, subscribe_result(CONFIG, 1, None));
    pages::mount(&mut page, &["p1"]);
    page
}

/// A page with one document in force and every subscription it opened
/// acknowledged.
fn configured(w: W) -> SurfacePage {
    let mut page = attached();
    let inbound = route(&mut page, deliver(CONFIG, &body(w), 1, 0));
    let configured = only_configured(inbound);
    for frame in &configured.frames {
        if let ClientFrame::Subscribe { channel, .. } = frame {
            route(&mut page, subscribe_result(channel, 0, None));
        }
    }
    page
}

fn subscribed(frames: &[ClientFrame]) -> Vec<&str> {
    frames
        .iter()
        .filter_map(|f| match f {
            ClientFrame::Subscribe { channel, .. } => Some(channel.as_str()),
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

/// One flush of `p1`'s, small enough that no cap refuses it.
fn flush_batch(n: u64) -> FlushBatch {
    FlushBatch {
        entries: vec![BatchEntry {
            channel: OUT.to_string(),
            body: format!(r#"{{"n":{n}}}"#),
            urgency: WireUrgency::Normal,
            deliver_after: None,
        }],
        ops: Vec::new(),
    }
}

/// Offer one flush of `p1`'s, as a completing activation does.
fn queue_flush(page: &mut SurfacePage, n: u64) -> Vec<ClientFrame> {
    let SurfacePage {
        connect, outbound, ..
    } = page;
    let wiring = connect.bindings().expect("the document is in force");
    outbound
        .flush(wiring, connect.facts(), "p1", flush_batch(n), NOW)
        .frames
}

fn batch_correlation(frames: &[ClientFrame]) -> u64 {
    frames
        .iter()
        .find_map(|f| match f {
            ClientFrame::PublishBatch { correlation, .. } => Some(*correlation),
            _ => None,
        })
        .expect("a flush on a free wire composes its batch frame")
}

/// Serve `p1/in`'s position, which is where the loss the peer reported reaches the
/// reader that missed it.
fn serve(page: &mut SurfacePage, w: W) -> brenn_attach_client::store::ServedWindow {
    page.stores
        .get_mut(WIRE)
        .expect("the document names the channel")
        .serve(
            &BindingKey::new("p1", "in"),
            SubscriptionDepths {
                push_depth: w.push,
                retain_depth: w.retain,
            },
        )
        .expect("a push-enabled position holding messages is served")
}

/// A config pass carrying two documents applies both, in frame order, and each
/// one reconciles against what the one before it left.
///
/// Reachable the moment the config channel's rung is anything but `1/1/1`, and
/// the page's whole wiring comes from that channel — a second document that
/// re-opened what the first opened, or announced the attachment a second time,
/// would mis-wire the page in the way that is hardest to diagnose.
#[test]
fn a_config_pass_applies_every_document_it_carried_in_order() {
    let mut page = attached();
    let narrow = body(W::default());
    let widened = {
        let mut document = doc(W::default());
        document
            .subscriptions
            .push(fixtures::subscription("p1", "in2", WIRE_TWO, 4, 2));
        document.to_body()
    };

    let inbound = route(
        &mut page,
        deliver_pass(CONFIG, &[(&narrow, 1, 0), (&widened, 2, 0)]),
    );

    let [first, second] = &inbound.configured[..] else {
        panic!("the pass carried two documents: {inbound:?}")
    };
    assert!(
        first.first_of_attachment,
        "phase 2 proper is the first document of the attachment"
    );
    assert!(
        !second.first_of_attachment,
        "the attachment is announced once, not once per document"
    );
    assert_eq!(subscribed(&first.frames), vec![WIRE]);
    assert_eq!(
        subscribed(&second.frames),
        vec![WIRE_TWO],
        "the second document opens only what it added: the first's subscription \
         stands"
    );
    assert!(unsubscribed(&second.frames).is_empty(), "{second:?}");
    // The wiring in force is the newest document's, and its stores exist.
    assert!(page.stores.get(WIRE_TWO).is_some());
    assert!(page.stores.get(WIRE).is_some());
}

#[test]
fn a_subscribe_result_puts_the_channel_on_the_wire() {
    let mut page = attached();
    let configured = only_configured(route(&mut page, deliver(CONFIG, &body(W::default()), 1, 0)));
    assert_eq!(subscribed(&configured.frames), vec![WIRE]);
    assert!(!page.subs.is_active(WIRE));
    let inbound = route(&mut page, subscribe_result(WIRE, 0, None));
    assert!(page.subs.is_active(WIRE));
    assert_eq!(inbound, Inbound::default());
}

#[test]
fn a_subscribe_result_for_a_channel_nothing_asked_for_is_fatal() {
    let mut page = attached();
    let err = on_server_frame(&mut page, subscribe_result(WIRE, 0, None), NOW)
        .expect_err("the page is waiting on no subscribe for that channel");
    assert!(err.contains(WIRE));
}

#[test]
fn the_config_channel_retaining_no_document_is_fatal() {
    let mut page = page();
    page.on_attached(pages::facts());
    let err = on_server_frame(&mut page, subscribe_result(CONFIG, 0, None), NOW)
        .expect_err("the peer publishes the document before it accepts a connection");
    assert!(err.contains("retains no bindings document"));
}

#[test]
fn a_gap_on_the_config_channel_is_fatal() {
    let mut page = page();
    page.on_attached(pages::facts());
    let gap = GapInfo {
        reason: GapReason::EpochChanged,
    };
    let err = on_server_frame(&mut page, subscribe_result(CONFIG, 1, Some(gap)), NOW)
        .expect_err("the config subscription presents no resume claim to gap");
    assert!(err.contains("resume claim"));
}

#[test]
fn a_gap_on_an_application_channel_is_reported_and_not_fatal() {
    let mut page = attached();
    route(&mut page, deliver(CONFIG, &body(W::default()), 1, 0));
    let gap = GapInfo {
        reason: GapReason::BeyondRetained,
    };
    let inbound = route(&mut page, subscribe_result(WIRE, 2, Some(gap)));
    assert_eq!(
        inbound.gap,
        Some(ChannelGap {
            channel: WIRE.to_string(),
            replay_count: 2,
            gap,
        })
    );
    // The page's answer to a gap is the subscribe it already made: the channel is
    // live and the replay that follows flows through the ordinary path.
    assert!(page.subs.is_active(WIRE));
}

#[test]
fn a_deferred_unsubscribe_rides_the_results_frames() {
    let mut page = attached();
    route(&mut page, deliver(CONFIG, &body(W::default()), 1, 0));
    // The only holder went away while the `Subscribe` was unanswered, so the
    // `Unsubscribe` waits for the acknowledgement.
    page.registrations
        .deregister("p1", &mut page.stores, &mut page.subs);
    let inbound = route(&mut page, subscribe_result(WIRE, 0, None));
    assert_eq!(unsubscribed(&inbound.frames), vec![WIRE]);
    assert!(!page.subs.is_active(WIRE));
}

#[test]
fn a_delivery_lands_in_the_channels_store() {
    let mut page = configured(W::default());
    let inbound = route(&mut page, deliver(WIRE, "m0", 1, 0));
    assert_eq!(inbound, Inbound::default());
    let store = page.stores.get(WIRE).expect("the wire store");
    assert_eq!(store.retained().count(), 1);
}

/// **A batched replay is one delivery point.** Sixteen retained rows arrive as
/// one pass, the whole pass lands before anything windows it, and the reader's
/// one window presents `min(push_depth, 16)` of them as new with the rest as
/// context. At `push_depth = 1` that is the one new message the binding asked
/// for — not sixteen arrivals of one new message each.
#[test]
fn a_batched_replay_is_one_window_with_a_push_capped_new_slice() {
    let w = || W {
        push: 1,
        retain: 16,
        ..W::default()
    };
    let mut page = configured(w());
    let bodies: Vec<String> = (1..=16).map(|n| format!("m{n}")).collect();
    let rows: Vec<(&str, u64, u64)> = bodies
        .iter()
        .enumerate()
        .map(|(i, body)| (body.as_str(), i as u64 + 1, 0))
        .collect();

    let inbound = route(&mut page, deliver_pass(WIRE, &rows));

    assert_eq!(
        inbound,
        Inbound::default(),
        "arrival moves no position, so the pass charges nothing"
    );
    let window = serve(&mut page, w());
    assert_eq!(window.envelopes.len(), 16, "the whole pass is retained");
    assert_eq!(
        window.new_from, 15,
        "one new — the push cap — and fifteen rows of context behind it"
    );
    assert_eq!(
        window.dropped, 0,
        "overrunning the push depth into context is not a drop"
    );
    assert_eq!(
        page.schedules.metered_drops("p1", "in"),
        0,
        "nothing was evicted from under the position"
    );
}

/// The companion, kept deliberately: distinct live publishes arrive as distinct
/// frames and stay distinct delivery points. Only a server-side catch-up pass
/// coalesces.
#[test]
fn two_separate_frames_remain_two_windows() {
    let w = || W {
        push: 1,
        retain: 16,
        ..W::default()
    };
    let mut page = configured(w());

    route(&mut page, deliver(WIRE, "m1", 1, 0));
    let first = serve(&mut page, w());
    route(&mut page, deliver(WIRE, "m2", 2, 0));
    let second = serve(&mut page, w());

    assert_eq!(first.envelopes.len() - first.new_from, 1);
    assert_eq!(first.envelopes[first.new_from].body, "m1");
    assert_eq!(second.envelopes.len() - second.new_from, 1);
    assert_eq!(second.envelopes[second.new_from].body, "m2");
}

/// The loss a pass reports is the peer's figure for the whole pass, carried on
/// the row that follows it, and it reaches the position that missed it whole.
///
/// Pinned where the figure is *consumed*: the head row's count is what
/// `count_server_drops` charges, and reading it off any other row — the last, for
/// symmetry with the cursor two lines below it — would report nothing on exactly
/// the path a catch-up after an outage takes.
#[test]
fn the_loss_a_pass_reports_reaches_the_position_that_missed_it() {
    let mut page = configured(W::default());

    let inbound = route(
        &mut page,
        deliver_pass(WIRE, &[("m0", 1, 5), ("m1", 2, 0), ("m2", 3, 0)]),
    );

    assert!(
        inbound.drops.is_quiet(),
        "the loss happened upstream, so the page charges nothing of its own"
    );
    let window = serve(&mut page, W::default());
    assert_eq!(window.dropped, 5);
}

#[test]
fn a_re_presented_delivery_is_retained_once() {
    let mut page = configured(W::default());
    route(&mut page, deliver(WIRE, "m0", 1, 0));
    // A resubscribed channel replays into a store the page kept, so the same
    // message legitimately arrives twice.
    route(&mut page, deliver(WIRE, "m0", 2, 0));
    assert_eq!(
        page.stores
            .get(WIRE)
            .expect("the wire store")
            .retained()
            .count(),
        1
    );
}

#[test]
fn the_loss_the_peer_reports_reaches_the_position_that_missed_it() {
    let mut page = configured(W::default());
    let inbound = route(&mut page, deliver(WIRE, "m0", 1, 3));
    // Nothing the page can see happened here: the messages were gone upstream, so
    // the ladder is walked at the window that reports them.
    assert!(inbound.drops.is_quiet());
    let window = serve(&mut page, W::default());
    assert_eq!(window.dropped, 3);
}

#[test]
fn an_arrival_that_outruns_a_position_is_charged_at_the_arrival() {
    let mut page = configured(W::default());
    // The store is `max(4, 2)` deep and the position has been served nothing, so
    // the fifth arrival evicts a message it was owed.
    for seq in 1..=5 {
        let inbound = route(&mut page, deliver(WIRE, &format!("m{seq}"), seq, 0));
        assert!(inbound.drops.is_quiet(), "the binding is metered");
    }
    assert_eq!(page.schedules.metered_drops("p1", "in"), 1);
}

#[test]
fn an_arrival_that_outruns_a_fatal_bindings_position_asks_for_the_kill() {
    let loud = || W {
        noise: NoiseLevel::Fatal,
        ..W::default()
    };
    let mut page = configured(loud());
    let mut verdicts = DropVerdicts::default();
    for seq in 1..=5 {
        verdicts.merge(route(&mut page, deliver(WIRE, &format!("m{seq}"), seq, 0)).drops);
    }
    let announcement = DropAnnouncement {
        instance: "p1".to_string(),
        port: "in".to_string(),
        channel: WIRE.to_string(),
        dropped: 1,
    };
    // The kill ends the instance, so the `fatal` rung announces where the loss
    // happened rather than waiting for a window that may never come.
    assert_eq!(verdicts.fatal, vec![announcement.clone()]);
    assert_eq!(verdicts.announce, vec![announcement]);
    assert_eq!(page.schedules.metered_drops("p1", "in"), 1);
}

#[test]
fn a_delivery_on_a_channel_never_active_is_fatal() {
    let mut page = configured(W::default());
    let err = on_server_frame(
        &mut page,
        deliver("brenn:site.bar.nowhere", "m0", 1, 0),
        NOW,
    )
    .expect_err("the page never had that channel open");
    assert!(err.contains("never active"));
}

#[test]
fn a_span_sequence_that_does_not_advance_is_fatal() {
    let mut page = configured(W::default());
    route(&mut page, deliver(WIRE, "m0", 7, 0));
    let err = on_server_frame(&mut page, deliver(WIRE, "m1", 7, 0), NOW)
        .expect_err("the peer assigns seq strictly increasing per span");
    assert!(err.contains("regression"));
}

#[test]
fn a_straggler_is_discarded_and_reported_once_per_span() {
    let mut page = configured(W::default());
    // Every holder released: the subscription closed, and a delivery already on
    // the wire crosses the `Unsubscribe`.
    page.registrations
        .deregister("p1", &mut page.stores, &mut page.subs);
    let first = route(&mut page, deliver(WIRE, "m0", 1, 2));
    assert_eq!(
        first.straggler,
        Some(Straggler {
            channel: WIRE.to_string(),
            seq: 1,
            dropped: 2,
        })
    );
    let second = route(&mut page, deliver(WIRE, "m1", 2, 0));
    assert_eq!(
        second.straggler, None,
        "one report per span, not per straggler"
    );
    // A straggler advances nothing and is routed nowhere: the channel is still
    // named by the document, so its store is still there, and neither delivery
    // reached it.
    assert_eq!(
        page.stores
            .get(WIRE)
            .expect("the document still names the channel")
            .retained()
            .count(),
        0
    );
}

/// Batching widens the straggler race — a whole multi-row drain can be in flight
/// across an `Unsubscribe` — so a straggling pass is discarded as a unit,
/// quietly: one report, no fatal, nothing retained, and the span left where the
/// `Unsubscribe` left it.
#[test]
fn a_straggling_pass_is_discarded_whole_and_quietly() {
    let mut page = configured(W::default());
    page.registrations
        .deregister("p1", &mut page.stores, &mut page.subs);

    let inbound = route(
        &mut page,
        deliver_pass(WIRE, &[("m0", 1, 2), ("m1", 2, 0), ("m2", 3, 0)]),
    );

    assert_eq!(
        inbound.straggler,
        Some(Straggler {
            channel: WIRE.to_string(),
            seq: 1,
            dropped: 2,
        }),
        "one report for the whole pass, naming where it started"
    );
    assert_eq!(
        page.stores
            .get(WIRE)
            .expect("the document still names the channel")
            .retained()
            .count(),
        0,
        "no row of a discarded pass reaches the store"
    );
}

#[test]
fn the_config_channels_delivery_is_phase_two() {
    let mut page = attached();
    let inbound = route(&mut page, deliver(CONFIG, &body(W::default()), 1, 0));
    let configured = only_configured(inbound);
    assert!(configured.first_of_attachment);
    assert!(page.bindings().is_some());
    assert_eq!(subscribed(&configured.frames), vec![WIRE]);
    // The wiring is not a message anybody reads, so nothing retains it.
    assert!(page.stores.get(CONFIG).is_none());
}

#[test]
fn an_unusable_config_document_is_fatal() {
    let mut page = attached();
    let err = on_server_frame(&mut page, deliver(CONFIG, r#"{"v":99}"#, 1, 0), NOW)
        .expect_err("a document from another version cannot be applied");
    assert!(err.contains("unusable"));
    assert!(page.bindings().is_none());
}

#[test]
fn a_publish_result_answers_its_caller() {
    let mut page = configured(W::default());
    let SurfacePage {
        connect, outbound, ..
    } = &mut page;
    let wiring = connect.bindings().expect("the document is in force");
    let out = resolve_output(wiring, "p1", "out", None).expect("p1 binds the port");
    let frame = outbound.publish_port(
        out,
        PortPublish {
            instance: "p1".to_string(),
            port: "out".to_string(),
            body: "{}".to_string(),
            urgency: None,
            correlation: 7,
        },
    );
    let ClientFrame::Publish { correlation, .. } = frame else {
        panic!("a port publish composes a Publish frame");
    };
    let inbound = route(
        &mut page,
        ServerFrame::PublishResult {
            correlation,
            outcome: PublishOutcome::RateLimited,
        },
    );
    assert_eq!(
        inbound.answers,
        vec![PublishAnswer::Port {
            instance: "p1".to_string(),
            port: "out".to_string(),
            // The caller's own token, not the wire correlation.
            correlation: 7,
            status: PublishStatus::RateLimited,
        }]
    );
}

#[test]
fn a_publish_result_for_a_correlation_nothing_sent_is_fatal() {
    let mut page = configured(W::default());
    let err = on_server_frame(
        &mut page,
        ServerFrame::PublishResult {
            correlation: Some(99),
            outcome: PublishOutcome::Ok,
        },
        NOW,
    )
    .expect_err("the correlation space is the page's own");
    assert!(err.contains("99"));
}

#[test]
fn an_applied_flush_frees_the_wire_for_what_queued_behind_it() {
    let mut page = configured(W::default());
    let frames = queue_flush(&mut page, 1);
    let correlation = batch_correlation(&frames);
    // Behind the unanswered frame.
    assert!(queue_flush(&mut page, 2).is_empty());
    let inbound = route(
        &mut page,
        ServerFrame::PublishBatchResult {
            correlation,
            outcome: PublishBatchOutcome::Ok,
        },
    );
    assert_eq!(batch_correlation(&inbound.steps.frames), correlation + 1);
    assert!(inbound.steps.dropped.is_empty());
    assert!(inbound.lost_flushes.is_empty());
}

#[test]
fn a_refused_flush_is_reparked_and_arms_the_retry() {
    let mut page = configured(W::default());
    let correlation = batch_correlation(&queue_flush(&mut page, 1));
    let inbound = route(
        &mut page,
        ServerFrame::PublishBatchResult {
            correlation,
            outcome: PublishBatchOutcome::RateLimited,
        },
    );
    // Not discarded and not resent: the activation returned ok, so the flush goes
    // back to the head of its outbox and the timer offers it again.
    assert!(inbound.steps.frames.is_empty());
    assert!(inbound.steps.dropped.is_empty());
    assert!(matches!(
        inbound.steps.retry_wakeup,
        Some(TimerChange::Arm(_))
    ));
    assert_eq!(page.outbound.rate_limited_count("p1"), 1);
}

#[test]
fn a_refusal_answered_after_the_outbox_closed_is_reported_lost() {
    let mut page = configured(W::default());
    let correlation = batch_correlation(&queue_flush(&mut page, 1));
    page.outbound.deregister("p1");
    let inbound = route(
        &mut page,
        ServerFrame::PublishBatchResult {
            correlation,
            outcome: PublishBatchOutcome::RateLimited,
        },
    );
    assert_eq!(
        inbound.lost_flushes,
        vec![LostFlush {
            instance: "p1".to_string(),
            batch: flush_batch(1),
        }]
    );
}

#[test]
fn a_batch_result_for_a_correlation_nothing_sent_is_fatal() {
    let mut page = configured(W::default());
    let err = on_server_frame(
        &mut page,
        ServerFrame::PublishBatchResult {
            correlation: 42,
            outcome: PublishBatchOutcome::Ok,
        },
        NOW,
    )
    .expect_err("the batch correlation space is the client crate's own");
    assert!(err.contains("42"));
}

#[test]
fn a_deferred_view_replaces_the_mirror_and_answers_nothing() {
    let mut page = configured(W::default());
    let entry = DeferredViewEntry {
        message_id: Uuid::from_u128(0xbeef),
        body: "{}".to_string(),
        deliver_after: 9_000,
    };
    let inbound = route(
        &mut page,
        ServerFrame::DeferredView {
            channel: OUT.to_string(),
            attribution: Some("p1".to_string()),
            entries: vec![entry.clone()],
        },
    );
    assert_eq!(inbound, Inbound::default());
    assert_eq!(page.views.get(OUT, Some("p1")), [entry]);
    // A full snapshot, so an empty one is the legitimate way to say the set is
    // empty.
    route(
        &mut page,
        ServerFrame::DeferredView {
            channel: OUT.to_string(),
            attribution: Some("p1".to_string()),
            entries: Vec::new(),
        },
    );
    assert!(page.views.get(OUT, Some("p1")).is_empty());
}

#[test]
#[should_panic(expected = "the connection consumes Heartbeat")]
fn a_frame_the_connection_owns_never_reaches_the_page() {
    let mut page = configured(W::default());
    let _ = on_server_frame(&mut page, ServerFrame::Heartbeat, NOW);
}

//! Subscription-plane tests. Everything is driven through the plane's own
//! methods against wire values, so the whole state machine is exercised without
//! a socket or a store — and without naming any application concept, which is
//! the crate's purity proof restated as a test property.

use super::*;

use brenn_attach_proto::GapReason;

const CHANNEL: &str = "brenn:app.events";
const OTHER: &str = "ephemeral:app.signals";

fn depths(push_depth: u64, retain_depth: u64) -> SubscriptionDepths {
    SubscriptionDepths {
        push_depth,
        retain_depth,
    }
}

fn cursor(token: &str) -> Cursor {
    serde_json::from_value(serde_json::Value::String(token.to_string()))
        .expect("cursor from a JSON string")
}

fn subscribe(
    channel: &str,
    push_depth: u64,
    retain_depth: u64,
    resume: Option<&str>,
) -> ClientFrame {
    ClientFrame::Subscribe {
        channel: channel.to_string(),
        push_depth,
        retain_depth,
        resume: resume.map(cursor),
    }
}

/// A plane with one live attachment and `channel` subscribed and acknowledged.
fn attached_with(channel: &str) -> Subscriptions {
    let mut subs = Subscriptions::new();
    assert!(subs.on_attached().is_empty());
    subs.acquire(channel, depths(5, 10), ResumePolicy::Resume);
    subs.on_subscribe_result(channel, SubscribeOutcome::Ok, 0, None)
        .expect("pending channel accepts its result");
    subs
}

#[test]
fn a_first_acquisition_on_a_live_attachment_subscribes() {
    let mut subs = Subscriptions::new();
    subs.on_attached();
    let frames = subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    assert_eq!(frames, vec![subscribe(CHANNEL, 5, 10, None)]);
    assert_eq!(subs.refcount(CHANNEL), 1);
    // Not live until the peer acknowledges it.
    assert!(!subs.is_active(CHANNEL));
}

#[test]
fn a_channel_answers_the_depths_it_was_acquired_with() {
    let mut subs = attached_with(CHANNEL);
    assert_eq!(subs.depths(CHANNEL), Some(depths(5, 10)));
    assert_eq!(
        subs.depths(OTHER),
        None,
        "a channel the attacher holds no entry for states nothing"
    );
    subs.release(CHANNEL);
    subs.acquire(CHANNEL, depths(1, 1), ResumePolicy::Resume);
    assert_eq!(
        subs.depths(CHANNEL),
        Some(depths(1, 1)),
        "a subscription stated afresh answers the fresh statement"
    );
}

#[test]
fn further_acquisitions_share_the_one_wire_subscription() {
    let mut subs = attached_with(CHANNEL);
    assert!(
        subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume)
            .is_empty()
    );
    assert!(
        subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume)
            .is_empty()
    );
    assert_eq!(subs.refcount(CHANNEL), 3);
    assert_eq!(subs.held_channels(), vec![CHANNEL]);
}

#[test]
fn an_acquisition_before_the_attachment_is_recorded_and_subscribed_at_attach() {
    let mut subs = Subscriptions::new();
    assert!(
        subs.acquire(CHANNEL, depths(1, 1), ResumePolicy::Resume)
            .is_empty()
    );
    assert_eq!(subs.on_attached(), vec![subscribe(CHANNEL, 1, 1, None)]);
}

#[test]
#[should_panic(expected = "re-acquired with a different subscription")]
fn re_acquiring_a_channel_at_a_different_depth_panics() {
    let mut subs = attached_with(CHANNEL);
    subs.acquire(CHANNEL, depths(5, 11), ResumePolicy::Resume);
}

#[test]
#[should_panic(expected = "re-acquired with a different subscription")]
fn re_acquiring_a_channel_under_another_resume_policy_panics() {
    let mut subs = attached_with(CHANNEL);
    subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Cursorless);
}

#[test]
#[should_panic(expected = "confined and never crosses the wire")]
fn acquiring_a_confined_channel_panics() {
    let mut subs = Subscriptions::new();
    subs.acquire("local:brenn/theme", depths(1, 1), ResumePolicy::Resume);
}

#[test]
#[should_panic(expected = "states no depth on either knob")]
fn acquiring_at_zero_depth_on_both_knobs_panics() {
    let mut subs = Subscriptions::new();
    subs.acquire(CHANNEL, depths(0, 0), ResumePolicy::Resume);
}

#[test]
#[should_panic(expected = "release of unsubscribed channel")]
fn releasing_an_unheld_channel_panics() {
    let mut subs = Subscriptions::new();
    subs.release(CHANNEL);
}

#[test]
#[should_panic(expected = "refcount underflow")]
fn releasing_past_zero_panics() {
    let mut subs = attached_with(CHANNEL);
    subs.release(CHANNEL);
    subs.release(CHANNEL);
}

#[test]
fn the_last_release_unsubscribes() {
    let mut subs = attached_with(CHANNEL);
    subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    assert!(subs.release(CHANNEL).is_empty());
    assert_eq!(subs.release(CHANNEL), vec![unsubscribe_frame(CHANNEL)]);
    assert!(!subs.is_active(CHANNEL));
    assert!(subs.held_channels().is_empty());
}

#[test]
fn a_release_while_the_subscribe_is_in_flight_defers_the_unsubscribe() {
    let mut subs = Subscriptions::new();
    subs.on_attached();
    subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    // The peer has not acknowledged yet, so there is nothing it would accept an
    // `Unsubscribe` for.
    assert!(subs.release(CHANNEL).is_empty());
    let ack = subs
        .on_subscribe_result(CHANNEL, SubscribeOutcome::Ok, 3, None)
        .expect("the result answers a pending channel");
    assert_eq!(ack.frames, vec![unsubscribe_frame(CHANNEL)]);
    assert!(!ack.live);
    assert!(!subs.is_active(CHANNEL));
}

#[test]
fn a_fresh_acquisition_after_a_full_release_resumes_from_nothing() {
    let mut subs = attached_with(CHANNEL);
    subs.on_deliver(CHANNEL, 1, cursor("c1"), 0)
        .expect("live subscription accepts a delivery");
    subs.release(CHANNEL);
    // The cursor went with the last subscriber: this attach takes the retained
    // tail rather than resuming past it.
    let frames = subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    assert_eq!(frames, vec![subscribe(CHANNEL, 5, 10, None)]);
}

/// The kept entry of a fully released channel carries straggler tolerance, not a
/// subscription — so an embedder that rebinds the channel's readers states a new
/// fold, and the fresh `Subscribe` carries it.
#[test]
fn a_fresh_acquisition_after_a_full_release_may_state_a_different_subscription() {
    let mut subs = attached_with(CHANNEL);
    subs.on_deliver(CHANNEL, 1, cursor("c1"), 0)
        .expect("live subscription accepts a delivery");
    subs.release(CHANNEL);
    assert_eq!(
        subs.acquire(CHANNEL, depths(2, 3), ResumePolicy::Cursorless),
        vec![subscribe(CHANNEL, 2, 3, None)]
    );
    assert!(
        subs.acquire(CHANNEL, depths(2, 3), ResumePolicy::Cursorless)
            .is_empty()
    );
    assert_eq!(subs.refcount(CHANNEL), 2);
}

/// A channel nobody holds may be stated afresh even while its `Subscribe` is
/// unanswered — the embedder whose fold is itself delivered state can be handed a
/// new one before the old one's subscribes come back. The peer is told when its
/// answer arrives: the acknowledged subscription is closed and the new statement
/// sent in its place.
#[test]
fn a_statement_replaced_while_the_subscribe_is_in_flight_is_enacted_at_its_result() {
    let mut subs = Subscriptions::new();
    subs.on_attached();
    assert_eq!(
        subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume),
        vec![subscribe(CHANNEL, 5, 10, None)]
    );
    subs.release(CHANNEL);
    // Restated at refcount zero with the old `Subscribe` still in flight: no
    // frame yet, because the peer has not finished answering the old one.
    assert!(
        subs.acquire(CHANNEL, depths(2, 3), ResumePolicy::Resume)
            .is_empty()
    );
    assert_eq!(subs.depths(CHANNEL), Some(depths(2, 3)));

    let ack = subs
        .on_subscribe_result(CHANNEL, SubscribeOutcome::Ok, 1, None)
        .expect("the result answers a pending channel");
    assert_eq!(
        ack.frames,
        vec![unsubscribe_frame(CHANNEL), subscribe(CHANNEL, 2, 3, None)]
    );
    assert!(!ack.live, "the restated subscription is not open yet");
    assert!(!subs.is_active(CHANNEL));

    // A delivery from the span that just closed is a straggler, not a fatal.
    assert_eq!(
        subs.on_deliver(CHANNEL, 1, cursor("c1"), 0)
            .expect("a straggler is tolerated"),
        DeliverDisposition::Discard { first: true }
    );

    let ack = subs
        .on_subscribe_result(CHANNEL, SubscribeOutcome::Ok, 1, None)
        .expect("the restated subscribe is pending");
    assert!(ack.frames.is_empty());
    assert!(ack.live);
    assert!(subs.is_active(CHANNEL));
    assert_eq!(subs.refcount(CHANNEL), 1);
}

/// The one-statement rule still holds for a channel somebody holds: two live
/// local subscribers cannot describe one wire subscription two ways.
#[test]
#[should_panic(expected = "re-acquired with a different subscription")]
fn re_acquiring_a_held_channel_whose_subscribe_is_in_flight_at_a_different_depth_panics() {
    let mut subs = Subscriptions::new();
    subs.on_attached();
    subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    subs.acquire(CHANNEL, depths(2, 3), ResumePolicy::Resume);
}

#[test]
fn going_live_subscribes_nothing_until_the_survivors_are_asked_for() {
    let mut subs = attached_with(CHANNEL);
    subs.on_detached();
    // The embedder whose subscribable set is state the attachment delivers goes
    // live first and resubscribes later, so it can reconcile in between.
    subs.go_live();
    // A channel acquired while live is subscribed at once, survivors or not.
    assert_eq!(
        subs.acquire(OTHER, depths(1, 1), ResumePolicy::Cursorless),
        vec![subscribe(OTHER, 1, 1, None)]
    );
    assert_eq!(
        subs.resubscribe_survivors(),
        vec![subscribe(CHANNEL, 5, 10, None)]
    );
    // Once open, a second ask leaves it alone.
    assert!(subs.resubscribe_survivors().is_empty());
}

#[test]
fn a_reattach_resubscribes_survivors_with_their_cursors() {
    let mut subs = attached_with(CHANNEL);
    subs.acquire(OTHER, depths(2, 2), ResumePolicy::Resume);
    subs.on_subscribe_result(OTHER, SubscribeOutcome::Ok, 0, None)
        .expect("pending");
    subs.on_deliver(CHANNEL, 7, cursor("c7"), 0)
        .expect("accept");
    subs.on_detached();
    assert!(!subs.is_active(CHANNEL));
    // Address order, each with whatever it last accepted.
    assert_eq!(
        subs.on_attached(),
        vec![
            subscribe(CHANNEL, 5, 10, Some("c7")),
            subscribe(OTHER, 2, 2, None),
        ]
    );
}

#[test]
fn a_cursorless_channel_never_presents_a_resume_claim() {
    let mut subs = Subscriptions::new();
    subs.on_attached();
    subs.acquire(CHANNEL, depths(1, 1), ResumePolicy::Cursorless);
    subs.on_subscribe_result(CHANNEL, SubscribeOutcome::Ok, 1, None)
        .expect("pending");
    subs.on_deliver(CHANNEL, 1, cursor("c1"), 0)
        .expect("accept");
    subs.on_detached();
    assert_eq!(subs.on_attached(), vec![subscribe(CHANNEL, 1, 1, None)]);
}

#[test]
fn a_detach_forgets_channels_nobody_holds() {
    let mut subs = attached_with(CHANNEL);
    subs.release(CHANNEL);
    subs.on_detached();
    assert!(subs.on_attached().is_empty());
    assert_eq!(subs.refcount(CHANNEL), 0);
}

#[test]
fn a_subscribe_result_reports_the_replay_count_and_gap_without_interpreting_them() {
    let mut subs = Subscriptions::new();
    subs.on_attached();
    subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    let gap = GapInfo {
        reason: GapReason::BeyondRetained,
    };
    let ack = subs
        .on_subscribe_result(CHANNEL, SubscribeOutcome::Ok, 4, Some(gap))
        .expect("pending");
    assert_eq!(ack.replay_count, 4);
    assert_eq!(ack.gap, Some(gap));
    assert!(ack.live);
    assert!(ack.frames.is_empty());
    assert!(subs.is_active(CHANNEL));
}

#[test]
fn a_subscribe_result_for_a_channel_not_pending_is_fatal() {
    let mut subs = attached_with(CHANNEL);
    let err = subs
        .on_subscribe_result(CHANNEL, SubscribeOutcome::Ok, 0, None)
        .expect_err("a second result answers nothing");
    assert!(err.contains("not pending"), "{err}");
    let err = subs
        .on_subscribe_result(OTHER, SubscribeOutcome::Ok, 0, None)
        .expect_err("a result for an unknown channel answers nothing");
    assert!(err.contains(OTHER), "{err}");
}

#[test]
fn a_delivery_advances_the_span_and_reports_its_drops() {
    let mut subs = attached_with(CHANNEL);
    assert_eq!(
        subs.on_deliver(CHANNEL, 1, cursor("c1"), 0),
        Ok(DeliverDisposition::Accept { dropped: 0 })
    );
    assert_eq!(
        subs.on_deliver(CHANNEL, 9, cursor("c9"), 4),
        Ok(DeliverDisposition::Accept { dropped: 4 })
    );
}

#[test]
fn a_seq_that_does_not_advance_the_span_is_fatal() {
    let mut subs = attached_with(CHANNEL);
    subs.on_deliver(CHANNEL, 5, cursor("c5"), 0)
        .expect("accept");
    let err = subs
        .on_deliver(CHANNEL, 5, cursor("c5-again"), 0)
        .expect_err("a repeated seq is a peer bug");
    assert!(err.contains("seq regression"), "{err}");
}

#[test]
fn a_span_restarts_at_each_subscribe() {
    let mut subs = attached_with(CHANNEL);
    subs.on_deliver(CHANNEL, 9, cursor("c9"), 0)
        .expect("accept");
    subs.on_detached();
    subs.on_attached();
    subs.on_subscribe_result(CHANNEL, SubscribeOutcome::Ok, 1, None)
        .expect("pending");
    // The peer's counter restarts at 1 on the new span, which the old
    // high-water would otherwise read as a regression.
    assert_eq!(
        subs.on_deliver(CHANNEL, 1, cursor("d1"), 0),
        Ok(DeliverDisposition::Accept { dropped: 0 })
    );
}

#[test]
fn a_delivery_on_a_channel_never_active_is_fatal() {
    let mut subs = Subscriptions::new();
    subs.on_attached();
    subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    let err = subs
        .on_deliver(CHANNEL, 1, cursor("c1"), 0)
        .expect_err("replay cannot precede the result that opens the span");
    assert!(err.contains("never active"), "{err}");
}

#[test]
fn a_straggler_after_an_unsubscribe_is_discarded_and_reported_once() {
    let mut subs = attached_with(CHANNEL);
    subs.on_deliver(CHANNEL, 1, cursor("c1"), 0)
        .expect("accept");
    subs.release(CHANNEL);
    assert_eq!(
        subs.on_deliver(CHANNEL, 2, cursor("c2"), 0),
        Ok(DeliverDisposition::Discard { first: true })
    );
    assert_eq!(
        subs.on_deliver(CHANNEL, 3, cursor("c3"), 7),
        Ok(DeliverDisposition::Discard { first: false })
    );
    // The discards advanced nothing: a re-acquisition resumes from the cursor
    // the last *accepted* delivery left, not from a discarded one — and here
    // the release dropped even that.
    assert_eq!(
        subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume),
        vec![subscribe(CHANNEL, 5, 10, None)]
    );
}

#[test]
fn the_straggler_report_re_arms_on_the_next_span() {
    let mut subs = attached_with(CHANNEL);
    subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    // One subscriber left, so the release below only closes the wire
    // subscription once the second goes.
    subs.release(CHANNEL);
    subs.release(CHANNEL);
    assert_eq!(
        subs.on_deliver(CHANNEL, 2, cursor("c2"), 0),
        Ok(DeliverDisposition::Discard { first: true })
    );
    subs.acquire(CHANNEL, depths(5, 10), ResumePolicy::Resume);
    subs.on_subscribe_result(CHANNEL, SubscribeOutcome::Ok, 0, None)
        .expect("pending");
    subs.release(CHANNEL);
    assert_eq!(
        subs.on_deliver(CHANNEL, 1, cursor("d1"), 0),
        Ok(DeliverDisposition::Discard { first: true })
    );
}

#[test]
fn a_detach_ends_straggler_tolerance() {
    let mut subs = attached_with(CHANNEL);
    subs.on_detached();
    subs.on_attached();
    // The new attachment has no span open on the channel yet, so a delivery on
    // it is inexplicable rather than a leftover.
    let err = subs
        .on_deliver(CHANNEL, 1, cursor("c1"), 0)
        .expect_err("stragglers cannot cross a connection");
    assert!(err.contains("never active"), "{err}");
}

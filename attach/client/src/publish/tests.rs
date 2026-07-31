//! The attacher-side publish plane: correlation custody, the outbox's send-or-
//! park decision and its overflow, the refusal re-park, and the parked-set
//! mirror.
//!
//! The registrant key is a plain string the test hands over and the tag on a
//! single publish is whatever the test wants routed back — nothing here names a
//! component, instance, port or pixel, which is the whole of what this layer
//! knows about the attacher above it.

use super::*;

use brenn_attach_proto::DeferredOpKind;
use uuid::Uuid;

const CHANNEL: &str = "ephemeral:demo";
const OTHER: &str = "brenn:other";

fn now() -> Millis {
    Millis(1_000)
}

fn request(body: &str) -> PublishRequest {
    PublishRequest {
        channel: CHANNEL.to_string(),
        attribution: Some("alpha".to_string()),
        body: body.to_string(),
        urgency: Urgency::Normal,
    }
}

fn entry(body: &str) -> BatchEntry {
    BatchEntry {
        channel: CHANNEL.to_string(),
        body: body.to_string(),
        urgency: Urgency::Normal,
        deliver_after: None,
    }
}

fn batch(bodies: &[&str]) -> FlushBatch {
    FlushBatch {
        entries: bodies.iter().map(|body| entry(body)).collect(),
        ops: Vec::new(),
    }
}

fn cancel_op(message_id: Uuid) -> BatchDeferredOp {
    BatchDeferredOp {
        channel: CHANNEL.to_string(),
        message_id,
        op: DeferredOpKind::Cancel,
    }
}

fn alpha() -> String {
    "alpha".to_string()
}

/// An outboxes collection with one live registrant at `depth`, already attached.
fn attached(depth: u64) -> Outboxes<String> {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), Some(alpha()), depth);
    let steps = outboxes.on_attached(now(), |_, _| true);
    assert!(steps.frames.is_empty(), "nothing was queued to send");
    outboxes
}

/// One `PublishBatch` frame's correlation, entry bodies, and op count.
fn batch_frame(frame: &ClientFrame) -> (u64, Vec<&str>, usize) {
    match frame {
        ClientFrame::PublishBatch {
            correlation,
            publishes,
            deferred_ops,
            ..
        } => (
            *correlation,
            publishes.iter().map(|e| e.body.as_str()).collect(),
            deferred_ops.len(),
        ),
        other => panic!("expected a PublishBatch, got {other:?}"),
    }
}

fn bodies_of(frame: &ClientFrame) -> Vec<&str> {
    batch_frame(frame).1
}

fn correlation_of(frame: &ClientFrame) -> u64 {
    batch_frame(frame).0
}

fn view_entry(deliver_after: u64) -> DeferredViewEntry {
    DeferredViewEntry {
        message_id: Uuid::from_u128(u128::from(deliver_after)),
        body: format!("parked at {deliver_after}"),
        deliver_after,
    }
}

// --- single publishes -------------------------------------------------------

#[test]
fn a_publish_frame_carries_the_request_verbatim() {
    let mut pending = PendingPublishes::new();
    let frame = pending.send(7, "caller", request("hi"));
    assert_eq!(
        frame,
        ClientFrame::Publish {
            channel: CHANNEL.to_string(),
            attribution: Some(alpha()),
            body: "hi".to_string(),
            urgency: Urgency::Normal,
            correlation: Some(7),
        }
    );
    assert_eq!(pending.len(), 1);
}

#[test]
fn a_fire_and_forget_publish_asks_for_no_answer() {
    match publish_frame(request("hi"), None) {
        ClientFrame::Publish { correlation, .. } => assert_eq!(correlation, None),
        other => panic!("expected a Publish, got {other:?}"),
    }
}

#[test]
fn a_result_answers_the_tag_its_publish_was_sent_with() {
    let mut pending = PendingPublishes::new();
    pending.send(1, "first", request("a"));
    pending.send(2, "second", request("b"));
    assert_eq!(pending.on_result(Some(2)), Ok("second"));
    assert_eq!(pending.len(), 1, "the other publish is still outstanding");
}

#[test]
fn a_result_with_no_correlation_is_unreconcilable() {
    let mut pending: PendingPublishes<&str> = PendingPublishes::new();
    assert!(pending.on_result(None).is_err());
}

#[test]
fn a_correlation_settles_once() {
    let mut pending = PendingPublishes::new();
    pending.send(1, "first", request("a"));
    assert!(pending.on_result(Some(1)).is_ok());
    assert!(pending.on_result(Some(1)).is_err());
}

#[test]
#[should_panic(expected = "duplicate pending publish correlation")]
fn a_reused_correlation_is_an_embedder_bug() {
    let mut pending = PendingPublishes::new();
    pending.send(1, "first", request("a"));
    pending.send(1, "second", request("b"));
}

#[test]
fn failing_the_outstanding_publishes_drains_them_in_correlation_order() {
    let mut pending = PendingPublishes::new();
    pending.send(3, "third", request("c"));
    pending.send(1, "first", request("a"));
    pending.send(2, "second", request("b"));
    assert_eq!(
        pending.fail_all(),
        vec![(1, "first"), (2, "second"), (3, "third")]
    );
    assert!(pending.is_empty());
}

// --- the outbox's send-or-park decision -------------------------------------

#[test]
fn a_flush_on_a_free_wire_goes_straight_out() {
    let mut outboxes = attached(4);
    let steps = outboxes.flush(&alpha(), batch(&["one"]), now());
    assert_eq!(steps.frames.len(), 1);
    assert_eq!(bodies_of(&steps.frames[0]), vec!["one"]);
    assert!(steps.dropped.is_empty());
    assert_eq!(
        steps.retry_wakeup, None,
        "nothing is queued, so the retry timer does not move"
    );
    assert_eq!(outboxes.parked_len(&alpha()), 0);
}

#[test]
fn a_frame_carries_the_registrations_attribution_and_both_lists() {
    let mut outboxes = attached(4);
    let flush = FlushBatch {
        entries: vec![entry("one")],
        ops: vec![cancel_op(Uuid::from_u128(9))],
    };
    let steps = outboxes.flush(&alpha(), flush, now());
    match &steps.frames[0] {
        ClientFrame::PublishBatch {
            attribution,
            publishes,
            deferred_ops,
            ..
        } => {
            assert_eq!(attribution.as_deref(), Some("alpha"));
            assert_eq!(publishes.len(), 1);
            assert_eq!(deferred_ops.len(), 1);
        }
        other => panic!("expected a PublishBatch, got {other:?}"),
    }
}

#[test]
fn a_flush_while_detached_queues_and_leaves_at_the_next_attach() {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), None, 4);
    let steps = outboxes.flush(&alpha(), batch(&["one"]), now());
    assert!(steps.frames.is_empty());
    assert_eq!(
        steps.retry_wakeup, None,
        "a detached attacher has no wire to retry against"
    );
    assert_eq!(outboxes.parked_len(&alpha()), 1);

    let steps = outboxes.on_attached(now(), |_, _| true);
    assert_eq!(bodies_of(&steps.frames[0]), vec!["one"]);
    assert_eq!(
        steps.retry_wakeup, None,
        "the head left, so nothing is blocked"
    );
}

#[test]
fn a_second_flush_queues_behind_the_unanswered_first() {
    let mut outboxes = attached(4);
    let first = outboxes.flush(&alpha(), batch(&["one"]), now());
    let steps = outboxes.flush(&alpha(), batch(&["two"]), now());
    assert!(steps.frames.is_empty(), "one flush on the wire at a time");
    assert_eq!(outboxes.parked_len(&alpha()), 1);
    assert_eq!(
        steps.retry_wakeup, None,
        "the unanswered flush's own result will pump this one; a timer would only \
         wake to do nothing"
    );

    let answer = outboxes
        .on_batch_result(
            correlation_of(&first.frames[0]),
            PublishBatchOutcome::Ok,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(bodies_of(&answer.steps.frames[0]), vec!["two"]);
    assert_eq!(
        answer.steps.retry_wakeup, None,
        "the result pumped the queue, and no timer was ever armed for it"
    );
}

#[test]
fn registrants_are_independent() {
    let mut outboxes = attached(4);
    outboxes.register("beta".to_string(), Some("beta".to_string()), 4);
    outboxes.flush(&alpha(), batch(&["a1"]), now());
    let steps = outboxes.flush(&"beta".to_string(), batch(&["b1"]), now());
    assert_eq!(
        bodies_of(&steps.frames[0]),
        vec!["b1"],
        "beta's wire is its own"
    );
}

// --- overflow ---------------------------------------------------------------

#[test]
fn the_outbox_drops_oldest_at_its_cap_and_counts_it() {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), None, 2);
    for body in ["one", "two"] {
        let steps = outboxes.flush(&alpha(), batch(&[body]), now());
        assert!(steps.dropped.is_empty());
    }
    let steps = outboxes.flush(&alpha(), batch(&["three"]), now());
    assert_eq!(steps.dropped, vec![alpha()]);
    assert_eq!(outboxes.dropped_count(&alpha()), 1);
    assert_eq!(outboxes.parked_len(&alpha()), 2);

    let steps = outboxes.on_attached(now(), |_, _| true);
    assert_eq!(
        bodies_of(&steps.frames[0]),
        vec!["two"],
        "the oldest went, the order of the rest held"
    );
}

#[test]
#[should_panic(expected = "an outbox depth of zero")]
fn a_zero_depth_outbox_is_an_embedder_bug() {
    Outboxes::new().register(alpha(), None, 0);
}

#[test]
#[should_panic(expected = "already has an outbox")]
fn registering_a_key_twice_is_an_embedder_bug() {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), None, 2);
    outboxes.register(alpha(), None, 2);
}

#[test]
#[should_panic(expected = "neither entries nor ops")]
fn an_empty_flush_is_not_a_batch() {
    let mut outboxes = attached(2);
    outboxes.flush(
        &alpha(),
        FlushBatch {
            entries: Vec::new(),
            ops: Vec::new(),
        },
        now(),
    );
}

// --- refusal ----------------------------------------------------------------

#[test]
fn a_refused_flush_goes_back_to_the_head_and_retries() {
    let mut outboxes = attached(4);
    let first = outboxes.flush(&alpha(), batch(&["one"]), now());
    outboxes.flush(&alpha(), batch(&["two"]), now());

    let answer = outboxes
        .on_batch_result(
            correlation_of(&first.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert!(
        answer.steps.frames.is_empty(),
        "a refused head is retried on the timer, not resent immediately"
    );
    assert_eq!(answer.registrant, alpha());
    assert_eq!(answer.lost, None);
    assert_eq!(outboxes.rate_limited_count(&alpha()), 1);
    assert_eq!(outboxes.parked_len(&alpha()), 2);

    let steps = outboxes.on_retry_tick(Millis(2_000));
    assert_eq!(
        bodies_of(&steps.frames[0]),
        vec!["one"],
        "the refused flush is still the oldest un-applied one"
    );
    assert_eq!(
        steps.retry_wakeup,
        Some(TimerChange::Disarm),
        "the head is on the wire again, and its result is what pumps what is behind it"
    );
}

/// A head the peer keeps refusing keeps the timer armed: each refusal puts it
/// back on a free wire, which is the state only a tick can act on.
#[test]
fn a_head_refused_again_re_arms_the_timer() {
    let mut outboxes = attached(4);
    let first = outboxes.flush(&alpha(), batch(&["one"]), now());
    outboxes
        .on_batch_result(
            correlation_of(&first.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    // The refused head goes back out on the tick and is refused again, so the
    // outbox is blocked once more when the next tick is decided.
    let steps = outboxes.on_retry_tick(Millis(2_000));
    let answer = outboxes
        .on_batch_result(
            correlation_of(&steps.frames[0]),
            PublishBatchOutcome::RateLimited,
            Millis(2_500),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(
        answer.steps.retry_wakeup,
        Some(TimerChange::Arm(Millis(2_500 + RETRY_INTERVAL_MS))),
        "a head nobody else will pump keeps the timer armed"
    );
}

/// The refusal is what arms the timer: the head is back at the front of an outbox
/// with a free wire, which is the one state a tick can act on.
#[test]
fn a_refusal_arms_the_retry_timer_a_queued_flush_does_not() {
    let mut outboxes = attached(4);
    let first = outboxes.flush(&alpha(), batch(&["one"]), now());
    let queued = outboxes.flush(&alpha(), batch(&["two"]), now());
    assert_eq!(queued.retry_wakeup, None);

    let answer = outboxes
        .on_batch_result(
            correlation_of(&first.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(
        answer.steps.retry_wakeup,
        Some(TimerChange::Arm(Millis(now().0 + RETRY_INTERVAL_MS)))
    );
}

#[test]
fn a_head_the_peer_keeps_refusing_converges_on_counted_drops() {
    let mut outboxes = attached(1);
    let first = outboxes.flush(&alpha(), batch(&["one"]), now());
    // A second flush arrives while the first is unanswered, filling the queue to
    // its cap of one.
    outboxes.flush(&alpha(), batch(&["two"]), now());
    let answer = outboxes
        .on_batch_result(
            correlation_of(&first.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(
        answer.steps.dropped,
        vec![alpha()],
        "the re-parked head is itself the overflow drop"
    );
    assert_eq!(outboxes.dropped_count(&alpha()), 1);
    assert_eq!(outboxes.parked_len(&alpha()), 1);
}

#[test]
fn a_result_for_an_unknown_batch_correlation_is_unreconcilable() {
    let mut outboxes = attached(4);
    assert!(
        outboxes
            .on_batch_result(41, PublishBatchOutcome::Ok, now())
            .is_err()
    );
}

#[test]
fn a_refusal_after_deregistration_hands_the_lost_flush_back() {
    let mut outboxes = attached(4);
    let sent = outboxes.flush(&alpha(), batch(&["one"]), now());
    let correlation = correlation_of(&sent.frames[0]);
    assert!(
        outboxes.deregister(&alpha()).is_empty(),
        "nothing was queued behind it"
    );

    let answer = outboxes
        .on_batch_result(correlation, PublishBatchOutcome::RateLimited, now())
        .expect("the correlation survives its registrant");
    assert_eq!(answer.lost, Some(batch(&["one"])));
    assert!(!outboxes.is_registered(&alpha()));
}

#[test]
fn an_ok_after_deregistration_loses_nothing() {
    let mut outboxes = attached(4);
    let sent = outboxes.flush(&alpha(), batch(&["one"]), now());
    let correlation = correlation_of(&sent.frames[0]);
    outboxes.deregister(&alpha());
    let answer = outboxes
        .on_batch_result(correlation, PublishBatchOutcome::Ok, now())
        .expect("the correlation survives its registrant");
    assert_eq!(
        answer.lost, None,
        "the peer applied it before the registrant went"
    );
}

/// A restarting registrant reuses its key, so the answer to the previous
/// incarnation's flush finds a live outbox under that name. It belongs to
/// neither: the successor never sent it.
#[test]
fn a_refusal_for_a_replaced_registration_is_lost_not_applied_to_its_successor() {
    let mut outboxes = attached(4);
    let sent = outboxes.flush(&alpha(), batch(&["old"]), now());
    let stale = correlation_of(&sent.frames[0]);
    outboxes.deregister(&alpha());
    outboxes.register(alpha(), Some("alpha-restarted".to_string()), 4);
    let fresh = outboxes.flush(&alpha(), batch(&["new"]), now());
    assert_eq!(
        bodies_of(&fresh.frames[0]),
        vec!["new"],
        "the successor's own flush goes straight out on its free wire"
    );

    let answer = outboxes
        .on_batch_result(stale, PublishBatchOutcome::RateLimited, now())
        .expect("the correlation survives its registrant");
    assert_eq!(
        answer.lost,
        Some(batch(&["old"])),
        "the dead registration's entries have no outbox left to wait in"
    );
    assert_eq!(
        outboxes.parked_len(&alpha()),
        0,
        "nothing was re-parked ahead of the successor's own queue"
    );
    assert_eq!(outboxes.rate_limited_count(&alpha()), 0);
    assert!(
        outboxes
            .flush(&alpha(), batch(&["later"]), now())
            .frames
            .is_empty(),
        "the successor's wire is still busy with the flush it actually sent"
    );
}

/// The `Ok` half of the same race: nothing to hand back, and nothing to clear
/// on an outbox that never sent it.
#[test]
fn an_ok_for_a_replaced_registration_leaves_its_successor_alone() {
    let mut outboxes = attached(4);
    let sent = outboxes.flush(&alpha(), batch(&["old"]), now());
    let stale = correlation_of(&sent.frames[0]);
    outboxes.deregister(&alpha());
    outboxes.register(alpha(), Some(alpha()), 4);
    outboxes.flush(&alpha(), batch(&["new"]), now());
    outboxes.flush(&alpha(), batch(&["queued"]), now());

    let answer = outboxes
        .on_batch_result(stale, PublishBatchOutcome::Ok, now())
        .expect("the correlation survives its registrant");
    assert_eq!(answer.lost, None);
    assert!(
        answer.steps.frames.is_empty(),
        "a stale answer pumps nothing: the successor's own flush is unanswered"
    );
    assert_eq!(outboxes.parked_len(&alpha()), 1);
}

#[test]
#[should_panic(expected = "deregister of an unregistered registrant")]
fn deregistering_a_registrant_that_holds_no_outbox_panics() {
    let mut outboxes = attached(4);
    outboxes.deregister(&alpha());
    outboxes.deregister(&alpha());
}

#[test]
fn deregistration_hands_back_the_flushes_nobody_is_left_to_send() {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), None, 4);
    outboxes.flush(&alpha(), batch(&["one"]), now());
    outboxes.flush(&alpha(), batch(&["two"]), now());
    assert_eq!(
        outboxes.deregister(&alpha()),
        vec![batch(&["one"]), batch(&["two"])]
    );
}

/// What an embedder re-checking a queued flush against an attachment's frame cap
/// reads: the whole composed frame, never less than what goes out.
#[test]
fn a_flush_measures_the_frame_it_composes_to() {
    let batch = batch(&["one", "two"]);
    let measured = batch.frame_bytes(Some("alpha"));

    let mut outboxes = attached(4);
    let steps = outboxes.flush(&alpha(), batch, now());
    let sent = serde_json::to_string(&steps.frames[0])
        .expect("a PublishBatch frame serializes")
        .len();
    assert!(
        measured >= sent,
        "measured {measured} understates the {sent}-byte frame"
    );
    // The widest correlation the plane can mint is what the slack is: 20 digits
    // against the one this frame carries.
    assert!(measured - sent <= 20);
}

/// The lookups borrow, so an embedder holding `&str` addresses a `String`-keyed
/// plane without composing an owned key per call.
#[test]
fn a_string_keyed_plane_answers_a_borrowed_key() {
    let mut outboxes = attached(1);
    outboxes.flush("alpha", batch(&["one"]), now());
    outboxes.flush("alpha", batch(&["two"]), now());
    assert!(outboxes.is_registered("alpha"));
    assert_eq!(outboxes.parked_len("alpha"), 1);
    assert_eq!(outboxes.dropped_count("alpha"), 0);
    assert_eq!(outboxes.rate_limited_count("alpha"), 0);
    assert_eq!(outboxes.deregister("alpha"), vec![batch(&["two"])]);
}

#[test]
fn the_open_outboxes_are_answered_in_key_order() {
    let mut outboxes = Outboxes::new();
    outboxes.register("zulu".to_string(), None, 4);
    outboxes.register(alpha(), None, 4);
    assert_eq!(
        outboxes.registrants().cloned().collect::<Vec<String>>(),
        vec![alpha(), "zulu".to_string()]
    );
    outboxes.deregister(&alpha());
    assert_eq!(
        outboxes.registrants().cloned().collect::<Vec<String>>(),
        vec!["zulu".to_string()]
    );
}

// --- the connection under the outboxes --------------------------------------

#[test]
fn a_detach_frees_the_wire_without_resending_the_unanswered_flush() {
    let mut outboxes = attached(4);
    let sent = outboxes.flush(&alpha(), batch(&["one"]), now());
    let correlation = correlation_of(&sent.frames[0]);
    let steps = outboxes.on_detached();
    assert!(steps.frames.is_empty());
    assert_eq!(
        outboxes.parked_len(&alpha()),
        0,
        "a sent flush is not requeued"
    );
    assert!(
        outboxes
            .on_batch_result(correlation, PublishBatchOutcome::Ok, now())
            .is_err(),
        "the batch the previous connection carried is unanswerable"
    );

    let steps = outboxes.on_attached(now(), |_, _| true);
    assert!(
        steps.frames.is_empty(),
        "a flush the peer may have applied is never resent"
    );
}

#[test]
fn a_detach_disarms_the_retry_timer() {
    let mut outboxes = attached(4);
    let sent = outboxes.flush(&alpha(), batch(&["one"]), now());
    let blocked = outboxes
        .on_batch_result(
            correlation_of(&sent.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert!(blocked.steps.retry_wakeup.is_some());
    assert_eq!(
        outboxes.on_detached().retry_wakeup,
        Some(TimerChange::Disarm)
    );
    assert_eq!(
        outboxes.on_detached().retry_wakeup,
        None,
        "an already-disarmed timer is not disarmed again"
    );
}

#[test]
fn an_attach_re_validates_every_queued_flush_against_the_new_contract() {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), None, 4);
    outboxes.flush(&alpha(), batch(&["keep"]), now());
    outboxes.flush(
        &alpha(),
        FlushBatch {
            entries: vec![BatchEntry {
                channel: OTHER.to_string(),
                body: "drop".to_string(),
                urgency: Urgency::Normal,
                deliver_after: None,
            }],
            ops: Vec::new(),
        },
        now(),
    );
    outboxes.flush(&alpha(), batch(&["also-keep"]), now());

    let steps = outboxes.on_attached(now(), |_, flush| {
        flush.entries.iter().all(|e| e.channel == CHANNEL)
    });
    assert_eq!(steps.dropped, vec![alpha()]);
    assert_eq!(outboxes.dropped_count(&alpha()), 1);
    assert_eq!(bodies_of(&steps.frames[0]), vec!["keep"]);
    assert_eq!(
        outboxes.parked_len(&alpha()),
        1,
        "the survivor behind the head waits its turn"
    );
}

#[test]
fn one_head_per_registrant_leaves_on_a_retry_tick() {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), None, 4);
    outboxes.register("beta".to_string(), None, 4);
    for body in ["one", "two"] {
        outboxes.flush(&alpha(), batch(&[body]), now());
        outboxes.flush(&"beta".to_string(), batch(&[body]), now());
    }
    let steps = outboxes.on_attached(now(), |_, _| true);
    assert_eq!(steps.frames.len(), 2, "one head each, in registrant order");
    assert_eq!(bodies_of(&steps.frames[0]), vec!["one"]);

    let steps = outboxes.on_retry_tick(Millis(2_000));
    assert!(
        steps.frames.is_empty(),
        "each registrant's second flush waits on its first being answered"
    );
}

/// An armed timer is left alone, never re-armed: a fresh deadline on every
/// unrelated input would push a blocked head's retry out for as long as the
/// traffic that blocked it lasts.
#[test]
fn traffic_while_a_head_is_blocked_leaves_the_armed_timer_alone() {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), None, 4);
    outboxes.register("beta".to_string(), None, 4);
    let sent = outboxes.on_attached(now(), |_, _| true);
    assert!(sent.frames.is_empty());

    let alphas = outboxes.flush(&alpha(), batch(&["one"]), now());
    let armed = outboxes
        .on_batch_result(
            correlation_of(&alphas.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(
        armed.steps.retry_wakeup,
        Some(TimerChange::Arm(Millis(now().0 + RETRY_INTERVAL_MS))),
        "alpha's head is blocked"
    );

    assert_eq!(
        outboxes
            .flush(&"beta".to_string(), batch(&["b1"]), Millis(1_500))
            .retry_wakeup,
        None,
        "a sibling's flush does not move alpha's deadline"
    );
    let betas = outboxes.flush(&"beta".to_string(), batch(&["b2"]), Millis(1_600));
    assert!(betas.frames.is_empty(), "beta's second flush queues");
    assert_eq!(betas.retry_wakeup, None);

    let tick = outboxes.on_retry_tick(Millis(now().0 + RETRY_INTERVAL_MS));
    assert_eq!(
        bodies_of(&tick.frames[0]),
        vec!["one"],
        "the original deadline still fires on alpha's head"
    );
}

/// The same rule from the other side: a sibling's result, which does move
/// frames, still leaves the blocked registrant's armed deadline where it was.
#[test]
fn a_siblings_result_leaves_a_blocked_registrants_deadline_where_it_was() {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), None, 4);
    outboxes.register("beta".to_string(), None, 4);
    outboxes.flush(&alpha(), batch(&["one"]), now());
    outboxes.flush(&"beta".to_string(), batch(&["b1"]), now());
    outboxes.flush(&"beta".to_string(), batch(&["b2"]), now());
    let heads = outboxes.on_attached(now(), |_, _| true);
    assert_eq!(heads.frames.len(), 2, "one head each");

    let armed = outboxes
        .on_batch_result(
            correlation_of(&heads.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(
        armed.steps.retry_wakeup,
        Some(TimerChange::Arm(Millis(now().0 + RETRY_INTERVAL_MS)))
    );

    let sibling = outboxes
        .on_batch_result(
            correlation_of(&heads.frames[1]),
            PublishBatchOutcome::Ok,
            Millis(1_500),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(bodies_of(&sibling.steps.frames[0]), vec!["b2"]);
    assert_eq!(
        sibling.steps.retry_wakeup, None,
        "alpha is still blocked, and its deadline is not renegotiated"
    );
}

#[test]
fn an_idle_attacher_arms_no_retry_timer() {
    let mut outboxes = attached(4);
    assert_eq!(outboxes.on_retry_tick(Millis(2_000)).retry_wakeup, None);
}

// --- the parked-set mirror --------------------------------------------------

#[test]
fn a_view_snapshot_replaces_what_was_held_for_its_pair() {
    let mut views = DeferredViews::new();
    views.on_view(
        CHANNEL.to_string(),
        Some(alpha()),
        vec![view_entry(10), view_entry(20)],
    );
    assert_eq!(views.get(CHANNEL, Some("alpha")).len(), 2);
    views.on_view(CHANNEL.to_string(), Some(alpha()), vec![view_entry(20)]);
    assert_eq!(
        views.get(CHANNEL, Some("alpha")),
        &[view_entry(20)],
        "the snapshot is the whole set, not a delta"
    );
}

#[test]
fn views_are_scoped_by_channel_and_attribution() {
    let mut views = DeferredViews::new();
    views.on_view(CHANNEL.to_string(), Some(alpha()), vec![view_entry(10)]);
    views.on_view(CHANNEL.to_string(), None, vec![view_entry(30)]);
    views.on_view(OTHER.to_string(), Some(alpha()), vec![view_entry(40)]);
    assert_eq!(views.get(CHANNEL, Some("alpha")), &[view_entry(10)]);
    assert_eq!(
        views.get(CHANNEL, None),
        &[view_entry(30)],
        "the attacher's own parked set is nobody else's"
    );
    assert_eq!(views.get(OTHER, Some("alpha")), &[view_entry(40)]);
    assert_eq!(views.len(), 3);
}

#[test]
fn an_unmirrored_set_reads_as_empty() {
    let views = DeferredViews::new();
    assert!(views.get(CHANNEL, Some("alpha")).is_empty());
    assert!(views.is_empty());
}

#[test]
fn clearing_the_mirror_forgets_every_set() {
    let mut views = DeferredViews::new();
    views.on_view(CHANNEL.to_string(), Some(alpha()), vec![view_entry(10)]);
    views.clear();
    assert!(
        views.get(CHANNEL, Some("alpha")).is_empty(),
        "a set the peer does not re-seed is an empty set"
    );
}

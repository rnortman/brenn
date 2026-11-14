//! Flushes that name blob handles: which handles each step releases, the drops
//! at and after a detach, and an attachment whose version has no attachments.

use super::*;

use brenn_attach_proto::AttachmentName;

fn v5() -> Codec {
    Codec::negotiated(5)
}

fn v6() -> Codec {
    Codec::negotiated(6)
}

fn attachment(name: &str, handle: u64) -> PublishAttachment {
    PublishAttachment::new(
        AttachmentName::new(name).expect("a valid name"),
        BlobHandle(handle),
    )
}

/// An entry naming one attachment per handle, under the names `a0`, `a1`, ….
fn entry_naming(body: &str, handles: &[u64]) -> BatchEntry {
    BatchEntry {
        attachments: handles
            .iter()
            .enumerate()
            .map(|(i, handle)| attachment(&format!("a{i}"), *handle))
            .collect(),
        ..entry(body)
    }
}

/// A flush of one entry naming `handles`.
fn naming(body: &str, handles: &[u64]) -> FlushBatch {
    FlushBatch {
        entries: vec![entry_naming(body, handles)],
        ops: Vec::new(),
    }
}

fn handles(raw: &[u64]) -> Vec<BlobHandle> {
    raw.iter().copied().map(BlobHandle).collect()
}

/// One live registrant at `depth` on a v6 attachment.
fn attached_v6(depth: u64) -> Outboxes<String> {
    let mut outboxes = Outboxes::new();
    outboxes.register(alpha(), Some(alpha()), depth);
    let steps = outboxes.on_attached(v6(), BODY_CAP, now(), |_, _| true);
    assert!(steps.frames.is_empty(), "nothing was queued to send");
    outboxes
}

#[test]
fn a_flushs_handles_are_its_entries_attachments_in_order() {
    let flush = FlushBatch {
        entries: vec![
            entry_naming("one", &[3, 1]),
            entry("plain"),
            entry_naming("two", &[3]),
        ],
        ops: vec![cancel_op(Uuid::from_u128(1))],
    };
    assert_eq!(flush.handles().collect::<Vec<_>>(), handles(&[3, 1, 3]));
    assert_eq!(batch(&["plain"]).handles().count(), 0);
}

#[test]
fn an_applied_flush_releases_its_handles() {
    let mut outboxes = attached_v6(4);
    let sent = outboxes.flush(&alpha(), naming("one", &[1, 2]), now());
    assert!(
        sent.released.is_empty(),
        "a flush on the wire releases nothing"
    );
    outboxes.flush(&alpha(), naming("two", &[3]), now());

    let answer = outboxes
        .on_batch_result(
            correlation_of(&sent.frames[0]),
            PublishBatchOutcome::Ok,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(answer.steps.released, handles(&[1, 2]));
    assert_eq!(
        bodies_of(&answer.steps.frames[0]),
        vec!["two"],
        "the next flush goes out, and is not released by going out"
    );
}

#[test]
fn a_flush_handed_back_lost_releases_its_handles() {
    for refusal in TERMINAL_REFUSALS {
        let mut outboxes = attached_v6(4);
        let sent = outboxes.flush(&alpha(), naming("one", &[5]), now());
        let answer = outboxes
            .on_batch_result(correlation_of(&sent.frames[0]), refusal, now())
            .expect("a correlation this attachment sent");
        assert_eq!(answer.lost, Some(naming("one", &[5])), "{refusal:?}");
        assert_eq!(answer.steps.released, handles(&[5]), "{refusal:?}");
    }
}

/// The sender deregistered under its outstanding flush: whatever the answer, the
/// flush is finished with.
#[test]
fn a_dead_registrations_answer_releases_its_handles() {
    for outcome in [
        PublishBatchOutcome::Ok,
        PublishBatchOutcome::RateLimited,
        PublishBatchOutcome::AttachmentNotHeld,
    ] {
        let mut outboxes = attached_v6(4);
        let sent = outboxes.flush(&alpha(), naming("one", &[7]), now());
        outboxes.deregister(&alpha());
        let answer = outboxes
            .on_batch_result(correlation_of(&sent.frames[0]), outcome, now())
            .expect("the correlation survives its registrant");
        assert_eq!(answer.steps.released, handles(&[7]), "{outcome:?}");
    }
}

#[test]
fn a_refused_flush_after_a_discard_releases_its_handles() {
    let mut outboxes = attached_v6(4);
    let sent = outboxes.flush(&alpha(), naming("one", &[2]), now());
    outboxes.discard_parked(&alpha(), now());
    let answer = outboxes
        .on_batch_result(
            correlation_of(&sent.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("the outstanding correlation is still known");
    assert_eq!(answer.lost, Some(naming("one", &[2])));
    assert_eq!(answer.steps.released, handles(&[2]));
}

#[test]
fn a_flush_dropped_at_the_cap_releases_its_handles() {
    let mut outboxes = attached_v6(1);
    let sent = outboxes.flush(&alpha(), naming("one", &[1]), now());
    let parked = outboxes.flush(&alpha(), naming("two", &[2]), now());
    assert!(
        parked.released.is_empty(),
        "a parked flush releases nothing"
    );

    let overflow = outboxes.flush(&alpha(), naming("three", &[3]), now());
    assert_eq!(overflow.dropped, vec![alpha()]);
    assert_eq!(overflow.released, handles(&[2]), "the oldest parked flush");

    let answer = outboxes
        .on_batch_result(
            correlation_of(&sent.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(answer.steps.dropped, vec![alpha()]);
    assert_eq!(
        answer.steps.released,
        handles(&[1]),
        "the re-parked head is itself the overflow drop"
    );
}

#[test]
fn a_rate_limited_flush_re_parked_releases_nothing() {
    let mut outboxes = attached_v6(4);
    let sent = outboxes.flush(&alpha(), naming("one", &[1]), now());
    let answer = outboxes
        .on_batch_result(
            correlation_of(&sent.frames[0]),
            PublishBatchOutcome::RateLimited,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert!(answer.steps.released.is_empty());
    assert_eq!(outboxes.parked_len(&alpha()), 1);

    let retried = outboxes.on_retry_tick(Millis(2_000));
    assert!(retried.released.is_empty(), "offered again, not finished");
    let applied = outboxes
        .on_batch_result(
            correlation_of(&retried.frames[0]),
            PublishBatchOutcome::Ok,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(applied.steps.released, handles(&[1]));
}

#[test]
fn a_discarded_queue_releases_every_handle_it_named() {
    let mut outboxes = attached_v6(4);
    outboxes.flush(&alpha(), naming("one", &[1]), now());
    outboxes.flush(&alpha(), naming("two", &[2, 3]), now());
    outboxes.flush(&alpha(), batch(&["plain"]), now());

    let discarded = outboxes.discard_parked(&alpha(), now());
    assert_eq!(discarded.flushes, 2);
    assert_eq!(
        discarded.released,
        handles(&[2, 3]),
        "the flush on the wire is not discarded, so its handle is not released"
    );
}

#[test]
fn a_handle_named_twice_is_released_twice() {
    let mut outboxes = attached_v6(4);
    let flush = FlushBatch {
        entries: vec![entry_naming("one", &[4]), entry_naming("two", &[4])],
        ops: Vec::new(),
    };
    let first = outboxes.flush(&alpha(), flush, now());
    outboxes.flush(&alpha(), naming("three", &[4]), now());

    let answer = outboxes
        .on_batch_result(
            correlation_of(&first.frames[0]),
            PublishBatchOutcome::Ok,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(answer.steps.released, handles(&[4, 4]));
    let second = outboxes
        .on_batch_result(
            correlation_of(&answer.steps.frames[0]),
            PublishBatchOutcome::Ok,
            now(),
        )
        .expect("a correlation this attachment sent");
    assert_eq!(second.steps.released, handles(&[4]));
}

/// A parked flush naming a handle can never be applied after a detach, so it
/// is dropped and counted. One in flight may have been applied, so it is
/// forgotten like every in-flight batch. A flush naming none waits for the next
/// attach.
#[test]
fn a_detach_drops_a_parked_flush_naming_a_handle_and_forgets_one_in_flight() {
    let mut outboxes = attached_v6(4);
    outboxes.register("beta".to_string(), None, 4);
    let in_flight = outboxes.flush(&alpha(), naming("one", &[1]), now());
    outboxes.flush(&alpha(), batch(&["plain"]), now());
    outboxes.flush(&alpha(), naming("two", &[2]), now());
    outboxes.flush("beta", batch(&["beta plain"]), now());

    let steps = outboxes.on_detached();
    assert_eq!(
        steps.dropped,
        vec![alpha()],
        "only the parked flush naming a handle"
    );
    assert!(
        steps.released.is_empty(),
        "every handle died with the attachment"
    );
    assert_eq!(outboxes.dropped_count(&alpha()), 1);
    assert_eq!(outboxes.parked_len(&alpha()), 1);
    assert!(
        outboxes
            .on_batch_result(
                correlation_of(&in_flight.frames[0]),
                PublishBatchOutcome::Ok,
                now()
            )
            .is_err(),
        "the in-flight flush's correlation died with the connection"
    );

    let reattached = outboxes.on_attached(v6(), BODY_CAP, now(), |_, _| true);
    assert_eq!(
        reattached
            .frames
            .iter()
            .map(|frame| bodies_of(frame))
            .collect::<Vec<_>>(),
        vec![vec!["plain"]],
        "alpha's wire is free and the forgotten flush is not sent again; \
         beta's went out before"
    );
}

#[test]
fn a_detach_forgets_a_dead_registrations_in_flight_flush() {
    let mut outboxes = attached_v6(4);
    let sent = outboxes.flush(&alpha(), naming("one", &[1]), now());
    outboxes.deregister(&alpha());
    outboxes.register(alpha(), Some(alpha()), 4);

    let steps = outboxes.on_detached();
    assert!(steps.dropped.is_empty());
    assert!(steps.released.is_empty());
    assert_eq!(
        outboxes.dropped_count(&alpha()),
        0,
        "the successor lost nothing"
    );
    assert!(
        outboxes
            .on_batch_result(
                correlation_of(&sent.frames[0]),
                PublishBatchOutcome::Ok,
                now()
            )
            .is_err(),
        "the old correlation died with the connection"
    );
}

#[test]
fn a_flush_naming_a_handle_while_detached_is_dropped_and_releases_its_handles() {
    let mut outboxes: Outboxes<String> = Outboxes::new();
    outboxes.register(alpha(), Some(alpha()), 4);
    outboxes.flush(&alpha(), batch(&["queued"]), now());

    let steps = outboxes.flush(&alpha(), naming("one", &[1, 2]), now());
    assert!(steps.frames.is_empty());
    assert_eq!(steps.dropped, vec![alpha()]);
    assert_eq!(steps.released, handles(&[1, 2]));
    assert_eq!(outboxes.dropped_count(&alpha()), 1);
    assert_eq!(outboxes.parked_len(&alpha()), 1, "the drop parks nothing");

    let plain = outboxes.flush(&alpha(), batch(&["plain"]), now());
    assert!(plain.dropped.is_empty() && plain.frames.is_empty());
    assert_eq!(
        outboxes.parked_len(&alpha()),
        2,
        "a flush naming no handle still queues"
    );

    let attached = outboxes.on_attached(v6(), BODY_CAP, now(), |_, _| true);
    assert!(attached.dropped.is_empty());
    assert_eq!(
        attached
            .frames
            .iter()
            .map(|frame| bodies_of(frame))
            .collect::<Vec<_>>(),
        vec![vec!["queued"]],
        "the queue drains in order and nothing of the dropped flush is sent"
    );
}

/// The embedder asks the attachment's facts before it hands a flush over; one
/// naming a handle on an attachment whose version has no attachments is its
/// bug and panics in the batch legality check.
#[test]
#[should_panic(expected = "this version cannot carry publish attachments")]
fn a_flush_naming_a_handle_on_a_live_v5_attachment_panics() {
    let mut outboxes: Outboxes<String> = Outboxes::new();
    outboxes.register(alpha(), Some(alpha()), 4);
    outboxes.on_attached(v5(), BODY_CAP, now(), |_, _| true);
    outboxes.flush(&alpha(), naming("one", &[1]), now());
}

/// The re-validation at attach drops a flush with attachments under a version
/// that has none, as it drops one with a body the version cannot write. No such
/// flush is parked at an attach today — the detach before it dropped every flush
/// naming a handle, and `flush` accepts none while detached — so the rule is
/// asked directly.
#[test]
fn a_flush_with_attachments_is_not_carried_by_v4_or_v5() {
    let flush = naming("one", &[1]);
    assert!(!flush.carried_by(v5()));
    assert!(!flush.carried_by(v4()));
    assert!(flush.carried_by(v6()));
    assert!(batch(&["plain"]).carried_by(v5()));
}

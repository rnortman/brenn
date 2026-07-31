//! The attacher-side store: retention, reader positions, windows, and the two
//! sources of loss it accounts for.
//!
//! Nothing here names a component, instance, port or pixel — the subscriber key
//! is whatever the test hands it, which is the whole of what this layer knows
//! about its readers.

use super::*;

use brenn_envelope::{ChannelScheme, Urgency};

const CHANNEL: &str = "ephemeral:demo";

fn epoch() -> Uuid {
    Uuid::from_u128(0x5107)
}

/// An envelope whose identity follows its body: same body, same message.
/// Identity is the subject of the store's dedup and of the window's context/new
/// split, so a fixture that pinned one id would collapse every message into one.
fn env(body: &str) -> MessageEnvelope {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(body, &mut hasher);
    MessageEnvelope {
        message_id: Uuid::from_u128(u128::from(std::hash::Hasher::finish(&hasher))),
        source: "test".into(),
        channel: CHANNEL.into(),
        sender: "attacher:test".into(),
        publish_ts: chrono::DateTime::from_timestamp(0, 0).expect("a representable instant"),
        body: body.into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Ephemeral,
    }
}

fn store(depth: u64) -> ChannelStore<String> {
    ChannelStore::new(epoch(), depth)
}

fn fill(store: &mut ChannelStore<String>, bodies: &[&str]) {
    for body in bodies {
        store.insert(env(body));
    }
}

fn depths(push_depth: u64, retain_depth: u64) -> SubscriptionDepths {
    SubscriptionDepths {
        push_depth,
        retain_depth,
    }
}

fn bodies(window: &ServedWindow) -> Vec<&str> {
    window.envelopes.iter().map(|e| e.body.as_str()).collect()
}

fn new_bodies(window: &ServedWindow) -> Vec<&str> {
    window.envelopes[window.new_from..]
        .iter()
        .map(|e| e.body.as_str())
        .collect()
}

fn serve(store: &mut ChannelStore<String>, reader: &str, push: u64, retain: u64) -> ServedWindow {
    store
        .serve(&reader.to_string(), depths(push, retain))
        .expect("the case attached this reader")
}

#[test]
fn insert_is_idempotent_by_message_id() {
    let mut s = store(8);
    s.insert(env("a"));
    s.insert(env("a"));
    assert_eq!(
        s.retained()
            .map(|(e, seq)| (e.body.as_str(), seq))
            .collect::<Vec<_>>(),
        vec![("a", 1)],
        "a re-presented message is not retained twice"
    );
}

/// The dedup is a scan of the retained window, so a message re-presented after
/// eviction is all the store can call fresh.
#[test]
fn a_message_re_presented_after_eviction_is_taken_as_fresh() {
    let mut s = store(1);
    fill(&mut s, &["a", "b", "a"]);
    assert_eq!(
        s.retained()
            .map(|(e, seq)| (e.body.as_str(), seq))
            .collect::<Vec<_>>(),
        vec![("a", 3)]
    );
}

/// A locally minted envelope is retained and delivered exactly like one off the
/// wire: the embedder's own publish reaches its own readers.
#[test]
fn append_minted_retains_and_delivers_in_order() {
    let mut s = store(8);
    s.attach("reader".to_string(), 4);
    assert!(s.append_minted(env("a")).is_empty(), "nothing was evicted");
    assert!(s.append_minted(env("b")).is_empty());
    assert_eq!(
        s.retained()
            .map(|(e, seq)| (e.body.as_str(), seq))
            .collect::<Vec<_>>(),
        vec![("a", 1), ("b", 2)]
    );
    assert!(s.has_deliverable(&"reader".to_string()));
    let window = serve(&mut s, "reader", 4, 0);
    assert_eq!(new_bodies(&window), vec!["a", "b"]);
    assert_eq!(window.dropped, 0);
}

/// The eviction a minted entry causes is reported the same way an arrival's is —
/// the loudness ladder is charged whichever side the message came from.
#[test]
fn append_minted_reports_the_readers_it_pushed_retention_past() {
    let mut s = store(1);
    s.attach("reader".to_string(), 4);
    assert!(s.append_minted(env("a")).is_empty());
    let overflow = s.append_minted(env("b"));
    assert_eq!(overflow.len(), 1);
    assert_eq!(overflow[0].subscriber, "reader".to_string());
    assert_eq!(overflow[0].evicted, 1);
}

#[test]
#[should_panic(expected = "is already retained")]
fn append_minted_refuses_a_message_the_store_already_holds() {
    let mut s = store(8);
    s.insert(env("a"));
    s.append_minted(env("a"));
}

#[test]
fn retained_reads_the_whole_window_oldest_first() {
    let mut s = store(2);
    fill(&mut s, &["a", "b", "c"]);
    assert_eq!(
        s.retained()
            .map(|(e, seq)| (e.body.as_str(), seq))
            .collect::<Vec<_>>(),
        vec![("b", 2), ("c", 3)]
    );
}

#[test]
fn a_fresh_position_is_primed_from_the_capped_retained_tail() {
    let mut s = store(8);
    fill(&mut s, &["a", "b", "c"]);
    assert_eq!(s.attach("reader".to_string(), 2), Attached::Created);
    let window = serve(&mut s, "reader", 2, 0);
    assert_eq!(new_bodies(&window), vec!["b", "c"]);
    assert_eq!(window.dropped, 0, "priming charges nothing");
}

/// Assembly and advance are one operation: what a serve handed over is behind
/// the reader's position before it runs, so the next serve is pure context.
#[test]
fn a_serve_advances_past_the_window_it_handed_over() {
    let mut s = store(8);
    s.attach("reader".to_string(), 4);
    fill(&mut s, &["a", "b"]);
    assert_eq!(new_bodies(&serve(&mut s, "reader", 4, 0)), vec!["a", "b"]);

    let again = serve(&mut s, "reader", 4, 4);
    assert_eq!(bodies(&again), vec!["a", "b"], "still readable as context");
    assert!(new_bodies(&again).is_empty());
    assert_eq!(again.dropped, 0);
    assert!(!s.has_deliverable(&"reader".to_string()));
}

/// Retention wider than the push depth back-fills as context, so a burst larger
/// than what the reader can be handed as new is not counted lost.
#[test]
fn retention_above_the_push_depth_is_served_as_context() {
    let mut s = store(8);
    s.attach("reader".to_string(), 1);
    fill(&mut s, &["a", "b", "c"]);
    let window = serve(&mut s, "reader", 1, 4);
    assert_eq!(bodies(&window), vec!["a", "b", "c"]);
    assert_eq!(new_bodies(&window), vec!["c"]);
    assert_eq!(window.dropped, 0);
}

/// The property the cursor model exists to provide, at the attacher's grain:
/// one store, two readers, their own depths and their own lag.
#[test]
fn two_readers_share_one_store_at_their_own_depths() {
    let mut s = store(4);
    s.attach("deep".to_string(), 4);
    s.attach("shallow".to_string(), 1);
    fill(&mut s, &["a", "b", "c"]);

    let deep = serve(&mut s, "deep", 4, 0);
    assert_eq!(new_bodies(&deep), vec!["a", "b", "c"]);
    assert_eq!(deep.dropped, 0);

    let shallow = serve(&mut s, "shallow", 1, 0);
    assert_eq!(new_bodies(&shallow), vec!["c"]);
    assert_eq!(shallow.dropped, 2, "its own window, its own loss");
}

#[test]
fn a_sampled_reader_holds_no_position_and_is_served_pure_context() {
    let mut s = store(8);
    assert_eq!(s.attach("sampler".to_string(), 0), Attached::Existing);
    fill(&mut s, &["a", "b"]);
    let window = serve(&mut s, "sampler", 0, 2);
    assert_eq!(bodies(&window), vec!["a", "b"]);
    assert!(new_bodies(&window).is_empty());
    assert_eq!(window.dropped, 0);
    assert_eq!(s.readers().count(), 0);
}

#[test]
fn a_push_enabled_read_without_a_position_has_no_window() {
    let mut s = store(8);
    fill(&mut s, &["a"]);
    assert!(s.serve(&"stranger".to_string(), depths(1, 0)).is_none());
}

#[test]
fn readers_names_every_position_held() {
    let mut s = store(8);
    s.attach("one".to_string(), 1);
    s.attach("two".to_string(), 1);
    s.attach("sampler".to_string(), 0);
    let mut held: Vec<&str> = s.readers().map(String::as_str).collect();
    held.sort_unstable();
    assert_eq!(held, vec!["one", "two"]);
}

#[test]
fn any_deliverable_answers_only_for_the_readers_the_predicate_names() {
    let mut s = store(8);
    s.attach("one".to_string(), 1);
    s.attach("two".to_string(), 1);
    fill(&mut s, &["a"]);
    serve(&mut s, "one", 1, 0);
    assert!(!s.any_deliverable(|r| r == "one"), "caught up");
    assert!(s.any_deliverable(|r| r == "two"));
}

#[test]
fn detach_drops_the_position_and_leaves_retention() {
    let mut s = store(8);
    s.attach("reader".to_string(), 4);
    fill(&mut s, &["a"]);
    s.detach(&"reader".to_string());
    assert!(!s.has_deliverable(&"reader".to_string()));
    assert_eq!(s.retained().count(), 1);
}

// ── The two sources of loss ───────────────────────────────────────────────

/// Loss the eviction already reported is announced to the reader but not
/// counted again; loss still inside the window has been reported by nothing, so
/// it is both.
#[test]
fn a_serve_counts_only_the_loss_no_eviction_reported() {
    let mut evicted = store(2);
    evicted.attach("reader".to_string(), 4);
    let mut charged = 0;
    for body in ["a", "b", "c", "d"] {
        charged += evicted
            .insert(env(body))
            .iter()
            .map(|e| e.evicted)
            .sum::<u64>();
    }
    assert_eq!(charged, 2, "the appends reported a and b as they retired");
    let window = serve(&mut evicted, "reader", 4, 0);
    assert_eq!(new_bodies(&window), vec!["c", "d"]);
    assert_eq!(window.dropped, 2, "the reader is still told the truth");
    assert_eq!(window.counted, 0, "the evictions already charged it");

    let mut retained = store(8);
    retained.attach("reader".to_string(), 1);
    fill(&mut retained, &["a", "b", "c"]);
    let window = serve(&mut retained, "reader", 1, 0);
    assert_eq!(window.dropped, 2);
    assert_eq!(
        window.counted, 2,
        "nothing retired them; this serve charges"
    );
}

/// One `dropped` count off the wire is the whole channel's loss: every reader
/// on it missed exactly those messages, because the position the drop happened
/// against is the attachment's single subscription.
#[test]
fn peer_reported_drops_charge_every_position_in_full() {
    let mut s = store(8);
    s.attach("one".to_string(), 4);
    s.attach("two".to_string(), 4);
    s.attach("sampler".to_string(), 0);
    s.count_server_drops(3);
    fill(&mut s, &["a"]);

    let one = serve(&mut s, "one", 4, 0);
    assert_eq!((one.dropped, one.counted), (3, 3));
    let two = serve(&mut s, "two", 4, 0);
    assert_eq!((two.dropped, two.counted), (3, 3));
    let sampler = serve(&mut s, "sampler", 0, 4);
    assert_eq!(
        (sampler.dropped, sampler.counted),
        (0, 0),
        "a sampled reader is never delivered to, so never reported against"
    );
}

#[test]
fn peer_reported_drops_are_drained_by_the_serve_that_reports_them() {
    let mut s = store(8);
    s.attach("reader".to_string(), 4);
    s.count_server_drops(2);
    assert_eq!(serve(&mut s, "reader", 4, 0).dropped, 2);
    assert_eq!(serve(&mut s, "reader", 4, 0).dropped, 0);
}

/// Both figures on one serve, so a reader that lost messages on both sides of
/// the wire hears one number.
#[test]
fn peer_reported_and_local_loss_are_summed_into_one_report() {
    let mut s = store(8);
    s.attach("reader".to_string(), 1);
    fill(&mut s, &["a", "b", "c"]);
    s.count_server_drops(4);
    let window = serve(&mut s, "reader", 1, 0);
    assert_eq!(new_bodies(&window), vec!["c"]);
    assert_eq!(window.dropped, 6);
    assert_eq!(window.counted, 6);
}

#[test]
fn a_detached_reader_takes_its_undelivered_peer_drops_with_it() {
    let mut s = store(8);
    s.attach("reader".to_string(), 4);
    s.count_server_drops(5);
    s.detach(&"reader".to_string());
    s.attach("reader".to_string(), 4);
    assert_eq!(serve(&mut s, "reader", 4, 0).dropped, 0);
}

#[test]
fn a_depth_shrink_reports_what_it_retired_and_a_grow_reports_nothing() {
    let mut s = store(4);
    s.attach("lagging".to_string(), 4);
    fill(&mut s, &["a", "b", "c", "d"]);

    let retired: Vec<(String, u64)> = s
        .retune(1)
        .into_iter()
        .map(|e| (e.subscriber, e.evicted))
        .collect();
    assert_eq!(retired, vec![("lagging".to_string(), 3)]);
    assert_eq!(s.depth(), 1);
    assert!(s.retune(8).is_empty());
}

// ── The collection ────────────────────────────────────────────────────────

#[test]
fn ensure_creates_at_the_collections_epoch_and_retunes_in_place() {
    let mut stores: ChannelStores<String> = ChannelStores::new(epoch());
    assert!(stores.ensure(CHANNEL, 4).is_empty());
    let store = stores.get_mut(CHANNEL).expect("just created");
    assert_eq!(store.epoch(), epoch());
    store.attach("reader".to_string(), 4);
    store.insert(env("a"));

    assert!(stores.ensure(CHANNEL, 8).is_empty());
    let store = stores.get(CHANNEL).expect("still held");
    assert_eq!(store.depth(), 8);
    assert_eq!(
        store.retained().count(),
        1,
        "a retune keeps the contents and the positions"
    );
    assert!(store.has_deliverable(&"reader".to_string()));
}

#[test]
fn ensure_reports_a_shrink_that_retired_a_readers_messages() {
    let mut stores: ChannelStores<String> = ChannelStores::new(epoch());
    stores.ensure(CHANNEL, 4);
    let store = stores.get_mut(CHANNEL).expect("just created");
    store.attach("reader".to_string(), 4);
    for body in ["a", "b", "c"] {
        store.insert(env(body));
    }
    let retired: Vec<(String, u64)> = stores
        .ensure(CHANNEL, 1)
        .into_iter()
        .map(|e| (e.subscriber, e.evicted))
        .collect();
    assert_eq!(retired, vec![("reader".to_string(), 2)]);
}

#[test]
fn retain_hands_back_the_stores_it_dropped_in_address_order() {
    let mut stores: ChannelStores<String> = ChannelStores::new(epoch());
    for channel in ["ephemeral:c", "ephemeral:a", "ephemeral:b"] {
        stores.ensure(channel, 2);
    }
    let dropped: Vec<String> = stores
        .retain(|channel| channel == "ephemeral:b")
        .into_iter()
        .map(|(channel, _)| channel)
        .collect();
    assert_eq!(dropped, vec!["ephemeral:a", "ephemeral:c"]);
    assert_eq!(stores.channels().collect::<Vec<_>>(), vec!["ephemeral:b"]);
}

#[test]
fn detach_matching_drops_a_registrants_positions_across_every_channel() {
    let mut stores: ChannelStores<(String, String)> = ChannelStores::new(epoch());
    for channel in ["ephemeral:a", "ephemeral:b"] {
        stores.ensure(channel, 4);
        let store = stores.get_mut(channel).expect("just created");
        store.attach(("gone".to_string(), "in".to_string()), 4);
        store.attach(("stays".to_string(), "in".to_string()), 4);
    }
    stores.detach_matching(|(registrant, _)| registrant == "gone");
    for channel in ["ephemeral:a", "ephemeral:b"] {
        let store = stores.get(channel).expect("still held");
        assert_eq!(
            store.readers().cloned().collect::<Vec<_>>(),
            vec![("stays".to_string(), "in".to_string())],
            "the store itself is untouched"
        );
    }
}

#[test]
fn the_collections_wake_question_spans_its_channels() {
    let mut stores: ChannelStores<String> = ChannelStores::new(epoch());
    for channel in ["ephemeral:a", "ephemeral:b"] {
        stores.ensure(channel, 4);
        stores
            .get_mut(channel)
            .expect("just created")
            .attach("reader".to_string(), 4);
    }
    assert!(!stores.any_deliverable(|r| r == "reader"));
    stores
        .get_mut("ephemeral:b")
        .expect("just created")
        .insert(env("a"));
    assert!(stores.any_deliverable(|r| r == "reader"));
    assert!(!stores.any_deliverable(|r| r == "stranger"));
}

// ── Deferral ──────────────────────────────────────────────────────────────

const ALICE: &str = "attacher:test#alice";
const BOB: &str = "attacher:test#bob";

fn park(store: &mut ChannelStore<String>, sender: &str, body: &str, release_at: ReleaseTime) {
    store
        .park(sender, env(body), release_at)
        .expect("the case parks inside the cap");
}

fn parked_bodies(store: &ChannelStore<String>, sender: &str, now: ReleaseTime) -> Vec<String> {
    store
        .deferred_for_sender(sender, now)
        .map(|e| e.message.body.clone())
        .collect()
}

/// The whole point of parking: the message is nowhere a reader can reach until
/// its time comes.
#[test]
fn a_parked_message_is_in_no_window_and_wakes_nobody() {
    let mut s = store(4);
    s.attach("reader".to_string(), 4);
    park(&mut s, ALICE, "later", 5_000);
    assert!(!s.has_deliverable(&"reader".to_string()));
    assert_eq!(s.retained().count(), 0);
    assert_eq!(s.next_release(), Some(5_000));
}

/// A release is an arrival: fresh tail seq, ordinary charges, every reader owed
/// it.
#[test]
fn a_release_enters_retention_as_an_ordinary_arrival() {
    let mut s = store(4);
    s.attach("reader".to_string(), 4);
    fill(&mut s, &["first"]);
    park(&mut s, ALICE, "second", 5_000);
    assert!(s.release_due(4_999).released.is_empty(), "not due yet");
    let report = s.release_due(5_000);
    assert_eq!(report.released.len(), 1);
    assert_eq!(report.released[0].message.body, "second");
    assert_eq!(
        report.released[0].seq, 2,
        "the tail seq, as any arrival takes"
    );
    assert_eq!(
        bodies(&serve(&mut s, "reader", 4, 4)),
        ["first", "second"],
        "release order behind what was already retained"
    );
    assert_eq!(s.next_release(), None);
}

#[test]
fn a_release_that_evicts_charges_the_positions_it_outran() {
    let mut s = store(1);
    s.attach("reader".to_string(), 1);
    fill(&mut s, &["first"]);
    park(&mut s, ALICE, "second", 5_000);
    let report = s.release_due(5_000);
    assert_eq!(report.overflow.len(), 1);
    assert_eq!(report.overflow[0].subscriber, "reader");
    assert_eq!(report.overflow[0].evicted, 1);
}

/// Soonest first, and an already-due deadline reports itself rather than being
/// skipped — a sweep that computed its wait from a fresher instant still hears
/// about what matured in between.
#[test]
fn next_release_is_the_soonest_deadline_due_or_not() {
    let mut s = store(4);
    park(&mut s, ALICE, "late", 9_000);
    park(&mut s, ALICE, "early", 5_000);
    assert_eq!(s.next_release(), Some(5_000));
    assert_eq!(s.release_due(6_000).released.len(), 1);
    assert_eq!(s.next_release(), Some(9_000));
}

/// The sender filter is the whole authorization story, and the cutoff is
/// `release_at > now`: an entry whose time has come is out of the view before
/// the sweep takes it, since there is nothing left to cancel or edit.
#[test]
fn a_senders_view_holds_its_own_still_parked_messages_only() {
    let mut s = store(4);
    park(&mut s, ALICE, "mine-late", 9_000);
    park(&mut s, BOB, "not-mine", 5_000);
    park(&mut s, ALICE, "mine-due", 5_000);
    assert_eq!(parked_bodies(&s, ALICE, 0), ["mine-due", "mine-late"]);
    assert_eq!(parked_bodies(&s, ALICE, 5_000), ["mine-late"]);
    assert_eq!(parked_bodies(&s, BOB, 0), ["not-mine"]);
    assert_eq!(
        parked_bodies(&s, "attacher:test#nobody", 0),
        Vec::<String>::new()
    );
}

/// The one read that spans senders, for a store about to be discarded: whoever
/// set a schedule is owed an account of it.
#[test]
fn the_channel_wide_reads_span_senders() {
    let mut s = store(4);
    park(&mut s, ALICE, "mine", 9_000);
    park(&mut s, BOB, "theirs", 5_000);
    assert_eq!(s.parked_senders().collect::<Vec<_>>(), [BOB, ALICE]);
    assert_eq!(
        s.parked()
            .map(|(e, at)| (e.body.as_str(), at))
            .collect::<Vec<_>>(),
        [("theirs", 5_000), ("mine", 9_000)]
    );
}

/// Refused, never drop-oldest: silently cancelling work already scheduled is
/// worse than refusing to schedule more.
#[test]
fn a_full_deferred_set_refuses_the_park_and_names_its_cap() {
    let mut s = store(2);
    park(&mut s, ALICE, "one", 5_000);
    park(&mut s, ALICE, "two", 5_000);
    let refused = s.park(ALICE, env("three"), 5_000);
    assert_eq!(refused, Err(QuotaExceeded { cap: 2 }));
    assert_eq!(parked_bodies(&s, ALICE, 0), ["one", "two"]);
}

#[test]
fn a_cancel_unparks_the_entry_and_nothing_is_ever_delivered() {
    let mut s = store(4);
    park(&mut s, ALICE, "regret", 5_000);
    let id = s.parked().next().expect("parked").0.message_id;
    assert_eq!(
        s.apply_defer_op(ALICE, id, DeferOp::Cancel, 0),
        DeferOpOutcome::Applied
    );
    assert_eq!(s.next_release(), None);
    assert!(s.release_due(9_000).released.is_empty());
}

#[test]
fn an_edit_rewrites_the_body_the_release_time_or_both() {
    let mut s = store(4);
    park(&mut s, ALICE, "draft", 5_000);
    let id = s.parked().next().expect("parked").0.message_id;
    assert_eq!(
        s.apply_defer_op(
            ALICE,
            id,
            DeferOp::Edit {
                body: Some("final".to_string()),
                deliver_after: Some(9_000),
            },
            0,
        ),
        DeferOpOutcome::Applied
    );
    assert_eq!(s.next_release(), Some(9_000));
    let released = s.release_due(9_000).released;
    assert_eq!(released[0].message.body, "final");
    assert_eq!(
        released[0].message.message_id, id,
        "an edit keeps the identity the caller named it by"
    );
}

/// A reschedule states no body, so what was parked is what releases — at the new
/// time.
#[test]
fn an_edit_that_states_no_body_moves_the_release_time_alone() {
    let mut s = store(4);
    park(&mut s, ALICE, "draft", 5_000);
    let id = s.parked().next().expect("parked").0.message_id;
    assert_eq!(
        s.apply_defer_op(
            ALICE,
            id,
            DeferOp::Edit {
                body: None,
                deliver_after: Some(9_000),
            },
            0,
        ),
        DeferOpOutcome::Applied
    );
    assert_eq!(s.next_release(), Some(9_000));
    assert!(
        s.release_due(5_000).released.is_empty(),
        "the original release time is no longer owed"
    );
    let released = s.release_due(9_000).released;
    assert_eq!(released[0].message.body, "draft");
}

/// A rewrite states no time, so the schedule the publisher set stands.
#[test]
fn an_edit_that_states_no_time_rewrites_the_body_alone() {
    let mut s = store(4);
    park(&mut s, ALICE, "draft", 5_000);
    let id = s.parked().next().expect("parked").0.message_id;
    assert_eq!(
        s.apply_defer_op(
            ALICE,
            id,
            DeferOp::Edit {
                body: Some("final".to_string()),
                deliver_after: None,
            },
            0,
        ),
        DeferOpOutcome::Applied
    );
    assert_eq!(s.next_release(), Some(5_000));
    assert!(
        s.release_due(4_999).released.is_empty(),
        "the release time was not pulled forward"
    );
    let released = s.release_due(5_000).released;
    assert_eq!(released[0].message.body, "final");
}

/// The two failures, which mean opposite things: the release race any publisher
/// can lose, and a view the caller built against the wrong sender.
#[test]
fn the_two_op_failures_are_distinguished() {
    let mut s = store(4);
    park(&mut s, ALICE, "gone", 5_000);
    let id = s.parked().next().expect("parked").0.message_id;
    assert_eq!(
        s.apply_defer_op(BOB, id, DeferOp::Cancel, 0),
        DeferOpOutcome::WrongSender {
            owner: ALICE.to_string()
        }
    );
    s.release_due(5_000);
    assert_eq!(
        s.apply_defer_op(ALICE, id, DeferOp::Cancel, 0),
        DeferOpOutcome::NotParked
    );
}

/// The window between a release time arriving and the sweep taking the entry: the
/// message is due, so it is out of the sender's view and out of an op's reach —
/// the answer the peer gives for the same op on a channel that crosses the wire.
#[test]
fn an_op_on_a_due_but_unswept_message_reaches_nothing() {
    let mut s = store(4);
    park(&mut s, ALICE, "due", 5_000);
    let id = s.parked().next().expect("parked").0.message_id;
    assert_eq!(
        parked_bodies(&s, ALICE, 5_000),
        Vec::<String>::new(),
        "the view already excluded it"
    );
    assert_eq!(
        s.apply_defer_op(ALICE, id, DeferOp::Cancel, 5_000),
        DeferOpOutcome::NotParked
    );
    let released = s.release_due(5_000).released;
    assert_eq!(
        released[0].message.body, "due",
        "the sweep still owes it, cancel or no cancel"
    );
}

#[test]
fn the_collections_release_deadline_is_the_soonest_across_its_channels() {
    let mut stores: ChannelStores<String> = ChannelStores::new(epoch());
    for channel in ["local:a", "local:b"] {
        stores.ensure(channel, 4);
    }
    assert_eq!(stores.next_release(), None, "nothing is parked anywhere");
    park(
        stores.get_mut("local:b").expect("hosted"),
        ALICE,
        "late",
        9_000,
    );
    park(
        stores.get_mut("local:a").expect("hosted"),
        ALICE,
        "early",
        5_000,
    );
    assert_eq!(stores.next_release(), Some(5_000));
}

/// Only the channels with something due, in address order.
#[test]
fn the_collections_sweep_names_the_due_channels_in_address_order() {
    let mut stores: ChannelStores<String> = ChannelStores::new(epoch());
    for channel in ["local:a", "local:b", "local:c"] {
        stores.ensure(channel, 4);
    }
    park(
        stores.get_mut("local:c").expect("hosted"),
        ALICE,
        "c",
        5_000,
    );
    park(
        stores.get_mut("local:a").expect("hosted"),
        ALICE,
        "a",
        5_000,
    );
    park(
        stores.get_mut("local:b").expect("hosted"),
        ALICE,
        "b",
        9_000,
    );
    let swept = stores.release_due(5_000);
    assert_eq!(
        swept
            .iter()
            .map(|(channel, report)| (channel.as_str(), report.released.len()))
            .collect::<Vec<_>>(),
        [("local:a", 1), ("local:c", 1)],
        "a channel with nothing due is absent, not present and empty"
    );
    assert_eq!(stores.next_release(), Some(9_000));
}

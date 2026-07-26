//! One battery run against every [`RetentionStore`] implementation.
//!
//! The point of the trait is that a caller cannot tell which store it holds, so
//! the tests that matter most are the ones neither implementation gets its own
//! copy of. Store-specific behaviour (restart survival, epoch gaps, cursor
//! accounting) is tested next to its implementation; what lives here is the
//! shared contract.
//!
//! The one exception is the last section: the durable store keeps a second,
//! per-subscriber grain (push rows) that the shared contract cannot see, and a
//! regression there is silent non-delivery. Those checks read the tables
//! directly and live here because they use these fixtures.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use brenn_envelope::{ChannelScheme, Urgency};
use brenn_queue::{GapReason, ReplayDecision};

use crate::db::init_db_memory;
use crate::messaging::ParticipantId;
use crate::messaging::config::{Depth, Sink};
use crate::messaging::db::{
    bus_gc_evict_channel, load_subscriber_cursor, upsert_channels, utc_to_ns,
};
use crate::messaging::store::{
    AdvanceOutcome, Attached, DbStore, DeferralOutcome, MessageSeq, NewMessage, OverflowEvent,
    Priming, PushRetireParams, ResumeCursor, RetentionStore, RingStore, StoreReplay,
    SubscriberWindow, TargetResolver,
};
use crate::messaging::testutils::test_channel_entry;
use crate::messaging::{
    MessagingDirectory, SubscriberEntry, SubscriberRegistration, WakeEconomics,
};

/// Retain depth every case uses unless it is testing the cap itself.
const DEPTH: u64 = 8;

const ATTACH_DEPTH: Depth = Depth::Bounded(DEPTH);

/// Build both stores for one channel at `retain_depth`, so a case body runs
/// twice without knowing which it holds.
async fn stores(retain_depth: u64) -> Vec<Arc<dyn RetentionStore>> {
    stores_subscribed(retain_depth, vec![]).await
}

/// Both stores for a channel carrying `subscribers` — the set a release pass
/// resolves as its targets on the durable side, and the set the ring's cursors
/// stand in for on the other.
async fn stores_subscribed(
    retain_depth: u64,
    subscribers: Vec<SubscriberEntry>,
) -> Vec<Arc<dyn RetentionStore>> {
    let (db_store, _) =
        durable_store_at_subscribed(Depth::Bounded(retain_depth), subscribers).await;
    vec![
        Arc::new(db_store),
        Arc::new(RingStore::new(
            Uuid::new_v4(),
            "ephemeral:parity",
            Depth::Bounded(retain_depth),
        )),
    ]
}

/// The durable store alone, with the database behind it, for the cases that
/// assert on the push-row grain the trait does not expose.
async fn durable_store(retain_depth: u64) -> (DbStore, crate::db::Db) {
    durable_store_at(Depth::Bounded(retain_depth)).await
}

/// [`durable_store`] at an arbitrary retain depth, for the cases that exercise
/// `Unbounded` — a legitimate durable choice with its own query shapes.
async fn durable_store_at(retain_depth: Depth) -> (DbStore, crate::db::Db) {
    durable_store_at_subscribed(retain_depth, vec![]).await
}

/// [`durable_store`] whose channel carries `subscribers`, each registered with
/// a channel-wide delivery policy — what the store's own release-target
/// resolution reads. Wake economics follow the production rule: a subscriber
/// entry carrying a `wake_min` is `UrgencyGated`, one without it is `Eager`.
async fn durable_store_at_subscribed(
    retain_depth: Depth,
    subscribers: Vec<SubscriberEntry>,
) -> (DbStore, crate::db::Db) {
    durable_store_at_subscribed_under(
        retain_depth,
        subscribers,
        crate::access::acl::ChannelMatcher::Prefix(String::new()),
    )
    .await
}

/// [`durable_store_at_subscribed`] whose subscribers' policies cover only the
/// channels `matcher` names — the delivery-time ACL gate a release pass runs.
async fn durable_store_at_subscribed_under(
    retain_depth: Depth,
    subscribers: Vec<SubscriberEntry>,
    matcher: crate::access::acl::ChannelMatcher,
) -> (DbStore, crate::db::Db) {
    let db = init_db_memory();
    let entry = test_channel_entry("parity", subscribers.clone());
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let policy = Arc::new(crate::messaging::test_support::brenn_delivery_policy(
        matcher,
    ));
    let registrations = subscribers
        .iter()
        .map(|sub| {
            (
                sub.kind.clone(),
                SubscriberRegistration {
                    policy: policy.clone(),
                    wake: match sub.wake_min {
                        Some(_) => WakeEconomics::UrgencyGated,
                        None => WakeEconomics::Eager,
                    },
                },
            )
        })
        .collect();
    let resolver = Arc::new(TargetResolver::new(
        Arc::new(MessagingDirectory::with_entries(vec![entry.clone()])),
        Arc::new(indexmap::IndexMap::new()),
        registrations,
    ));
    let store = DbStore::new(
        db.clone(),
        entry.uuid,
        entry.address.clone(),
        retain_depth,
        resolver,
    );
    (store, db)
}

/// A WASM consumer on the parity channel, as the directory carries it: the
/// subscriber a commit or a release resolves and mints a claim for.
fn wasm_target(slug: &str, wake_min: Option<crate::messaging::WakeMin>) -> SubscriberEntry {
    wasm_target_at(slug, DEPTH, wake_min)
}

/// The same subscriber holding at most `push_depth` undelivered records — a
/// commit past that retires its oldest claim and reports the drop.
fn wasm_target_at(
    slug: &str,
    push_depth: u64,
    wake_min: Option<crate::messaging::WakeMin>,
) -> SubscriberEntry {
    SubscriberEntry {
        kind: crate::messaging::SubscriberEntryKind::Wasm(slug.to_string()),
        push_depth: Depth::Bounded(push_depth),
        retain_depth: Depth::Bounded(DEPTH),
        noise: crate::messaging::config::NoiseLevel::Silent,
        wake_min,
    }
}

/// Both stores for a channel carrying one WASM consumer — the commonest shape:
/// a case that publishes and reads back one subscriber's delivery state.
async fn stores_for_proc(retain_depth: u64) -> Vec<Arc<dyn RetentionStore>> {
    stores_subscribed(retain_depth, vec![wasm_target("proc", None)]).await
}

/// The durable store alone, its channel carrying one WASM consumer at
/// `push_depth`.
async fn durable_store_for(slug: &str, push_depth: u64) -> (DbStore, crate::db::Db) {
    durable_store_at_subscribed(
        Depth::Bounded(DEPTH),
        vec![wasm_target_at(slug, push_depth, None)],
    )
    .await
}

fn message(sender: &str, body: &str) -> NewMessage {
    NewMessage {
        source: "node".to_string(),
        sender: sender.to_string(),
        body: body.to_string(),
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Brenn,
        reply_to_uuid: None,
        delivery_deadline: None,
        publish_ts_ns: utc_to_ns(Utc::now()),
    }
}

/// A ring-backed store stamps its own channel address on the envelope, so the
/// scheme a case passes in must match the store it is running against.
fn message_for(store: &dyn RetentionStore, sender: &str, body: &str) -> NewMessage {
    let mut msg = message(sender, body);
    if !store.capabilities().durable {
        msg.envelope_type = ChannelScheme::Ephemeral;
    }
    msg
}

/// The instant every case passes as the deferred surface's `now`.
///
/// Chosen freely, not anchored to the wall clock: no store reads a clock, so a
/// case's `now` is the only one that decides what is parked. On a whole second
/// because that is the granularity the durable side persists.
fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).expect("representable")
}

/// A release time a minute past [`now`], on a whole second: the durable store
/// persists timestamps at second granularity, so sub-second release times are
/// not a property either store promises.
fn soon() -> DateTime<Utc> {
    now() + Duration::seconds(60)
}

async fn retained_bodies(store: &dyn RetentionStore) -> Vec<String> {
    store
        .retained_tail(Depth::Unbounded)
        .await
        .iter()
        .map(|e| e.body.clone())
        .collect()
}

async fn deferred_bodies(store: &dyn RetentionStore, sender: &str) -> Vec<String> {
    store
        .deferred_for_sender(sender, now())
        .await
        .iter()
        .map(|d| d.envelope.body.clone())
        .collect()
}

// ── Identity ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn address_and_capabilities_agree_with_the_scheme() {
    for store in stores(DEPTH).await {
        let (scheme, _) = ChannelScheme::split(store.address()).expect("canonical address");
        assert_eq!(
            store.capabilities(),
            scheme.capabilities().expect("pub/sub scheme"),
            "{}",
            store.address()
        );
    }
}

// ── Retention ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn appended_messages_are_retained_oldest_first() {
    for store in stores(DEPTH).await {
        for body in ["a", "b", "c"] {
            let msg = message_for(&*store, "alice", body);
            store.append(msg).await;
        }
        assert_eq!(
            retained_bodies(&*store).await,
            vec!["a", "b", "c"],
            "{}",
            store.address()
        );
    }
}

#[tokio::test]
async fn retained_tail_is_capped_by_the_requested_limit() {
    for store in stores(DEPTH).await {
        for body in ["a", "b", "c"] {
            let msg = message_for(&*store, "alice", body);
            store.append(msg).await;
        }
        let tail: Vec<String> = store
            .retained_tail(Depth::Bounded(2))
            .await
            .iter()
            .map(|e| e.body.clone())
            .collect();
        assert_eq!(tail, vec!["b", "c"], "{}", store.address());
    }
}

#[tokio::test]
async fn append_reports_the_uuid_the_envelope_carries() {
    for store in stores(DEPTH).await {
        let msg = message_for(&*store, "alice", "a");
        let committed = store.append(msg).await.committed;
        let retained = store.retained_tail(Depth::Unbounded).await;
        assert_eq!(
            retained[0].message_id,
            committed.message_uuid,
            "{}",
            store.address()
        );
    }
}

// ── Deferral ──────────────────────────────────────────────────────────────

/// The load-bearing invariant of `deliver_after`: a parked message is in no
/// retention read until it releases, on every scheme.
#[tokio::test]
async fn parked_messages_are_not_observable_before_release() {
    for store in stores(DEPTH).await {
        let release_at = soon();
        let msg = message_for(&*store, "alice", "later");
        store.park(msg, release_at).await.expect("within cap");

        assert!(
            retained_bodies(&*store).await.is_empty(),
            "{}",
            store.address()
        );
        assert_eq!(store.deferred_len().await, 1, "{}", store.address());
        assert_eq!(
            store.next_release().await.map(|t| t.timestamp_millis()),
            Some(release_at.timestamp_millis()),
            "{}",
            store.address()
        );
        assert!(
            store
                .release_due(release_at - Duration::seconds(1))
                .await
                .released
                .is_empty(),
            "{}",
            store.address()
        );
    }
}

#[tokio::test]
async fn released_messages_enter_retention_and_are_reported() {
    for store in stores(DEPTH).await {
        let release_at = soon();
        let msg = message_for(&*store, "alice", "later");
        let parked = store.park(msg, release_at).await.expect("within cap");

        let released = store.release_due(release_at).await.released;
        assert_eq!(released.len(), 1, "{}", store.address());
        assert_eq!(
            released[0].envelope.message_id,
            parked.message_uuid,
            "{}",
            store.address()
        );
        assert_eq!(
            retained_bodies(&*store).await,
            vec!["later"],
            "{}",
            store.address()
        );
        assert_eq!(store.deferred_len().await, 0, "{}", store.address());
        assert!(store.next_release().await.is_none(), "{}", store.address());
    }
}

/// A message parked with nobody to deliver it to still enters retention at its
/// release time, so it is still part of the released batch the caller accounts
/// for. Reporting only the messages that had targets would make the two stores
/// disagree about what a release did.
#[tokio::test]
async fn release_reports_messages_parked_without_targets() {
    for store in stores(DEPTH).await {
        let release_at = soon();
        let lonely = store
            .park(message_for(&*store, "alice", "lonely"), release_at)
            .await
            .expect("within cap");
        store
            .park(message_for(&*store, "alice", "watched"), release_at)
            .await
            .expect("within cap");

        let released = store.release_due(release_at).await.released;
        let bodies: Vec<String> = released.iter().map(|r| r.envelope.body.clone()).collect();
        assert_eq!(bodies, vec!["lonely", "watched"], "{}", store.address());
        assert!(
            released[0].envelope.message_id.eq(&lonely.message_uuid),
            "{}",
            store.address()
        );
        assert!(released[0].target_records.is_empty(), "{}", store.address());
        assert_eq!(
            retained_bodies(&*store).await,
            vec!["lonely", "watched"],
            "{}",
            store.address()
        );
    }
}

#[tokio::test]
async fn deferred_view_is_sender_scoped_and_release_ordered() {
    for store in stores(DEPTH).await {
        let base = soon();
        for (sender, body, offset) in [
            ("alice", "a-late", 30),
            ("bob", "b", 20),
            ("alice", "a-soon", 10),
        ] {
            let msg = message_for(&*store, sender, body);
            store
                .park(msg, base + Duration::seconds(offset))
                .await
                .expect("within cap");
        }

        assert_eq!(
            deferred_bodies(&*store, "alice").await,
            vec!["a-soon", "a-late"],
            "{}",
            store.address()
        );
        assert_eq!(
            deferred_bodies(&*store, "bob").await,
            vec!["b"],
            "{}",
            store.address()
        );
        assert_eq!(store.deferred_len().await, 3, "{}", store.address());
    }
}

/// The cap is the channel's `retain_depth`, shared across senders — a channel
/// holds at most as much parked future as retained past.
#[tokio::test]
async fn deferred_cap_is_channel_wide_and_shared_across_senders() {
    for store in stores(2).await {
        for sender in ["alice", "bob"] {
            let msg = message_for(&*store, sender, "parked");
            store.park(msg, soon()).await.expect("within cap");
        }
        let over = message_for(&*store, "alice", "over");
        let err = store.park(over, soon()).await.expect_err("cap reached");
        assert_eq!(err.cap, 2, "{}", store.address());
        assert_eq!(store.deferred_len().await, 2, "{}", store.address());
    }
}

/// The cap counts what is held, not what is still in the future. Between a
/// message's release time and the pass that acts on it, its slot is still
/// occupied — on both stores, or a park admitted on one class is refused on the
/// other exactly when a lagging release loop makes it matter.
#[tokio::test]
async fn a_matured_but_unreleased_message_still_holds_its_cap_slot() {
    for store in stores(1).await {
        let release_at = soon();
        store
            .park(message_for(&*store, "alice", "parked"), release_at)
            .await
            .expect("within cap");

        // Time passes, but nothing calls `release_due`.
        let after = release_at + Duration::seconds(1);
        assert_eq!(store.deferred_len().await, 1, "{}", store.address());
        assert!(
            store.deferred_for_sender("alice", after).await.is_empty(),
            "a matured message is out of the sender view: {}",
            store.address()
        );
        let err = store
            .park(
                message_for(&*store, "alice", "over"),
                after + Duration::seconds(60),
            )
            .await
            .expect_err("the matured message still holds the only slot");
        assert_eq!(err.cap, 1, "{}", store.address());

        // Releasing it is what frees the slot, on both stores.
        assert_eq!(
            store.release_due(after).await.released.len(),
            1,
            "{}",
            store.address()
        );
        assert_eq!(store.deferred_len().await, 0, "{}", store.address());
        store
            .park(
                message_for(&*store, "alice", "next"),
                after + Duration::seconds(60),
            )
            .await
            .expect("slot freed by the release");
    }
}

/// A deadline already in the past is still reported. A release loop that
/// computes its sleep after its release pass must be told about the entry that
/// matured in between, or nothing ever wakes it for that entry.
#[tokio::test]
async fn next_release_reports_a_deadline_already_past() {
    for store in stores(DEPTH).await {
        let release_at = soon();
        store
            .park(message_for(&*store, "alice", "overdue"), release_at)
            .await
            .expect("within cap");

        // The entry matured long ago and no pass has taken it; the deadline is
        // reported all the same, so a loop waking on it releases immediately.
        assert_eq!(
            store.next_release().await.map(|t| t.timestamp_millis()),
            Some(release_at.timestamp_millis()),
            "{}",
            store.address()
        );
        store.release_due(release_at + Duration::seconds(5)).await;
        assert!(store.next_release().await.is_none(), "{}", store.address());
    }
}

#[tokio::test]
async fn zero_retain_depth_parks_nothing() {
    for store in stores(0).await {
        let msg = message_for(&*store, "alice", "a");
        let err = store.park(msg, soon()).await.expect_err("cap of zero");
        assert_eq!(err.cap, 0, "{}", store.address());
    }
}

#[tokio::test]
async fn cancel_removes_the_parked_message_entirely() {
    for store in stores(DEPTH).await {
        let release_at = soon();
        let msg = message_for(&*store, "alice", "a");
        let parked = store.park(msg, release_at).await.expect("within cap");

        assert_eq!(
            store
                .cancel_deferred("alice", parked.message_uuid, now())
                .await,
            DeferralOutcome::Applied,
            "{}",
            store.address()
        );
        assert_eq!(store.deferred_len().await, 0, "{}", store.address());
        // A cancelled message must not resurface as retained ambience when its
        // release time passes.
        assert!(
            store.release_due(release_at).await.released.is_empty(),
            "{}",
            store.address()
        );
        assert!(
            retained_bodies(&*store).await.is_empty(),
            "{}",
            store.address()
        );
    }
}

/// A message that released between the view the caller acted on and the call is
/// a reportable no-op, not a failure.
#[tokio::test]
async fn cancel_after_release_is_a_no_op() {
    for store in stores(DEPTH).await {
        let release_at = soon();
        let msg = message_for(&*store, "alice", "a");
        let parked = store.park(msg, release_at).await.expect("within cap");
        store.release_due(release_at).await;

        assert_eq!(
            store
                .cancel_deferred("alice", parked.message_uuid, now())
                .await,
            DeferralOutcome::NotDeferred,
            "{}",
            store.address()
        );
    }
}

#[tokio::test]
async fn cancel_of_an_unknown_message_is_a_no_op() {
    for store in stores(DEPTH).await {
        assert_eq!(
            store.cancel_deferred("alice", Uuid::new_v4(), now()).await,
            DeferralOutcome::NotDeferred,
            "{}",
            store.address()
        );
    }
}

#[tokio::test]
async fn edit_replaces_the_body_and_reschedules() {
    for store in stores(DEPTH).await {
        let base = soon();
        let msg = message_for(&*store, "alice", "original");
        let parked = store
            .park(msg, base + Duration::seconds(30))
            .await
            .expect("within cap");
        let moved_to = base + Duration::seconds(5);

        assert_eq!(
            store
                .edit_deferred(
                    "alice",
                    parked.message_uuid,
                    Some("edited".to_string()),
                    Some(moved_to),
                    now(),
                )
                .await,
            DeferralOutcome::Applied,
            "{}",
            store.address()
        );
        assert_eq!(
            deferred_bodies(&*store, "alice").await,
            vec!["edited"],
            "{}",
            store.address()
        );
        assert_eq!(
            store.next_release().await.map(|t| t.timestamp_millis()),
            Some(moved_to.timestamp_millis()),
            "{}",
            store.address()
        );
        assert_eq!(
            store.release_due(moved_to).await.released.len(),
            1,
            "{}",
            store.address()
        );
    }
}

/// The mirror of rescheduling earlier, and the half that pins the write rather
/// than the ordering: a message moved *later* must not release at the time it
/// was originally parked for.
#[tokio::test]
async fn edit_can_push_a_release_further_out() {
    for store in stores(DEPTH).await {
        let originally = soon();
        let msg = message_for(&*store, "alice", "a");
        let parked = store.park(msg, originally).await.expect("within cap");
        let moved_to = originally + Duration::seconds(60);

        assert_eq!(
            store
                .edit_deferred("alice", parked.message_uuid, None, Some(moved_to), now())
                .await,
            DeferralOutcome::Applied,
            "{}",
            store.address()
        );
        assert!(
            store.release_due(originally).await.released.is_empty(),
            "the original release time must no longer be due: {}",
            store.address()
        );
        assert_eq!(store.deferred_len().await, 1, "{}", store.address());
        assert_eq!(
            store.release_due(moved_to).await.released.len(),
            1,
            "{}",
            store.address()
        );
    }
}

#[tokio::test]
async fn edit_after_release_is_a_no_op() {
    for store in stores(DEPTH).await {
        let release_at = soon();
        let msg = message_for(&*store, "alice", "a");
        let parked = store.park(msg, release_at).await.expect("within cap");
        store.release_due(release_at).await;

        assert_eq!(
            store
                .edit_deferred(
                    "alice",
                    parked.message_uuid,
                    Some("edited".into()),
                    None,
                    now()
                )
                .await,
            DeferralOutcome::NotDeferred,
            "{}",
            store.address()
        );
    }
}

/// Structural authorization: a component only ever names ids its own
/// sender-scoped view gave it, so reaching another sender's message means the
/// scoping was bypassed.
async fn cancel_across_senders(index: usize) {
    let store = stores(DEPTH).await.remove(index);
    let msg = message_for(&*store, "bob", "b");
    let parked = store.park(msg, soon()).await.expect("within cap");
    store
        .cancel_deferred("alice", parked.message_uuid, now())
        .await;
}

/// Two cases rather than a loop because the assertion is a panic.
#[tokio::test]
#[should_panic(expected = "a sender-scoped view was bypassed")]
async fn touching_another_senders_parked_message_panics_on_db() {
    cancel_across_senders(0).await;
}

#[tokio::test]
#[should_panic(expected = "a sender-scoped view was bypassed")]
async fn touching_another_senders_parked_message_panics_on_ring() {
    cancel_across_senders(1).await;
}

// ── Wake source ─────────────────────────────────────────────────────────────
//
// Each store registers subscribers through its own mechanism (push rows vs
// cursors), but both must answer the trait contract identically.

fn wasm_sub(slug: &str) -> ParticipantId {
    ParticipantId::for_wasm(slug)
}

/// Just the identities from the owed walk, for rows that assert on who is owed
/// rather than on how loud their backlog is.
async fn owed_ids(store: &dyn RetentionStore) -> Vec<ParticipantId> {
    store
        .deliverable_subscribers()
        .await
        .into_iter()
        .map(|owed| owed.subscriber)
        .collect()
}

/// Retained priming on a fresh queue owes the channel's retained tail as
/// NEW — both stores agree.
#[tokio::test]
async fn attach_with_retained_priming_seeds_the_tail_as_owed() {
    for store in stores(DEPTH).await {
        for body in ["a", "b", "c"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        let sub = wasm_sub("proc");
        let attached = store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;
        assert_eq!(attached, Attached::Created, "{}", store.address());
        assert!(store.has_deliverable(&sub).await, "{}", store.address());
        assert_eq!(
            owed_ids(store.as_ref()).await,
            vec![sub],
            "{}",
            store.address()
        );
    }
}

/// Head priming owes nothing at attach: the tail already retained is context,
/// not new, so a fresh head-primed queue is owed only what publishes next.
#[tokio::test]
async fn attach_with_head_priming_owes_nothing() {
    for store in stores(DEPTH).await {
        for body in ["a", "b", "c"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        let sub = wasm_sub("proc");
        let attached = store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
            .await;
        assert_eq!(attached, Attached::Created, "{}", store.address());
        assert!(!store.has_deliverable(&sub).await, "{}", store.address());
    }
}

/// A durable queue survives the restart, and the cursor row is what says so: an
/// attach that finds one is `Existing` and re-primes nothing, however retained
/// the priming asks for. The subscriber below caught up before the "restart",
/// so it stays caught up.
#[tokio::test]
async fn durable_attach_does_not_reprime_a_surviving_queue() {
    let (store, _db) = durable_store(DEPTH).await;
    let sub = wasm_sub("proc");
    RetentionStore::attach(&store, &sub, "proc", ATTACH_DEPTH, Priming::Retained).await;
    for body in ["a", "b"] {
        store.append(message("alice", body)).await;
    }
    serve(&store, &sub, DEPTH, 0).await;

    let attached =
        RetentionStore::attach(&store, &sub, "proc", ATTACH_DEPTH, Priming::Retained).await;
    assert_eq!(attached, Attached::Existing);
    assert!(!store.has_deliverable(&sub).await);
}

/// Priming positions the new cursor, and the depth it primes at decides how far
/// back the retained case reaches: two messages for a depth of two, and the
/// whole retained set for an unbounded one — the system-subscriber shape, where
/// a bound quietly taken as one would start the queue's life having skipped its
/// backlog.
#[tokio::test]
async fn durable_attach_creates_a_cursor_at_the_primed_position() {
    for (priming, depth, expected) in [
        (Priming::Head, Depth::Bounded(2), 4),
        (Priming::Retained, Depth::Bounded(2), 2),
        (Priming::Retained, Depth::Unbounded, 1),
    ] {
        let (store, db) = durable_store(DEPTH).await;
        for body in ["a", "b", "c"] {
            store.append(message("alice", body)).await;
        }
        let sub = wasm_sub("proc");
        RetentionStore::attach(&store, &sub, "proc", depth, priming).await;

        let conn = db.lock().await;
        let row = load_subscriber_cursor(&conn, store.channel_uuid(), &sub).expect("cursor row");
        assert_eq!(row.next_owed_seq, expected, "{priming:?} at {depth:?}");
        assert_eq!(row.app_slug, "proc");
        assert_eq!(row.push_depth, depth);
    }
}

/// Two subscribers of different kinds sharing one app slug hold two positions.
/// The cursor is keyed by the kind-prefixed participant, not by the
/// `(channel, app_slug)` pair the rest of the messaging schema keys on, so a
/// component and an app that happen to be named the same neither inherit each
/// other's position nor move it.
#[tokio::test]
async fn subscribers_of_different_kinds_sharing_a_slug_hold_independent_cursors() {
    let (store, db) = durable_store(DEPTH).await;
    for body in ["a", "b", "c"] {
        store.append(message("alice", body)).await;
    }
    let component = wasm_sub("proc");
    let conversation = ParticipantId::for_conversation(7);

    RetentionStore::attach(
        &store,
        &component,
        "proc",
        Depth::Bounded(2),
        Priming::Retained,
    )
    .await;
    let attached =
        RetentionStore::attach(&store, &conversation, "proc", ATTACH_DEPTH, Priming::Head).await;
    assert_eq!(
        attached,
        Attached::Created,
        "the component's position is not the conversation's"
    );

    {
        let conn = db.lock().await;
        let channel = store.channel_uuid();
        assert_eq!(
            load_subscriber_cursor(&conn, channel, &component)
                .expect("component cursor")
                .next_owed_seq,
            2,
            "primed over the retained tail at its own depth"
        );
        assert_eq!(
            load_subscriber_cursor(&conn, channel, &conversation)
                .expect("conversation cursor")
                .next_owed_seq,
            4,
            "primed at head"
        );
    }

    let (new, _) = serve(&store, &component, DEPTH, 0).await;
    assert_eq!(new, vec!["b", "c"]);

    let conn = db.lock().await;
    assert_eq!(
        load_subscriber_cursor(&conn, store.channel_uuid(), &conversation)
            .expect("conversation cursor")
            .next_owed_seq,
        4,
        "the component's advance moved only the component"
    );
}

/// An unbounded attach caches unbounded. Collapsing it to a count would put a
/// bound nobody asked for in the row, and eviction reporting reads that cache in
/// exactly the window between attach and first read where nothing has retuned it
/// yet.
#[tokio::test]
async fn a_durable_unbounded_attach_caches_unbounded() {
    let (store, db) = durable_store(DEPTH).await;
    let sub = wasm_sub("proc");
    RetentionStore::attach(&store, &sub, "proc", Depth::Unbounded, Priming::Head).await;

    let conn = db.lock().await;
    let row = load_subscriber_cursor(&conn, store.channel_uuid(), &sub).expect("cursor row");
    assert_eq!(row.push_depth, Depth::Unbounded);
}

#[tokio::test]
async fn durable_reattach_keeps_the_position_and_detach_removes_it() {
    let (store, db) = durable_store(DEPTH).await;
    for body in ["a", "b", "c"] {
        store.append(message("alice", body)).await;
    }
    let sub = wasm_sub("proc");
    RetentionStore::attach(&store, &sub, "proc", Depth::Bounded(1), Priming::Retained).await;
    RetentionStore::attach(&store, &sub, "proc", Depth::Bounded(4), Priming::Head).await;
    {
        let conn = db.lock().await;
        let row = load_subscriber_cursor(&conn, store.channel_uuid(), &sub).expect("cursor row");
        assert_eq!(row.next_owed_seq, 3, "the position it already held");
        assert_eq!(row.push_depth, Depth::Bounded(4), "the depth cache retunes");
    }

    store.detach(&sub).await;
    let conn = db.lock().await;
    assert!(load_subscriber_cursor(&conn, store.channel_uuid(), &sub).is_none());
}

/// A sampled attach creates no queue on either class and takes away any the
/// subscriber held before the demotion, so a caller cannot tell the two stores
/// apart by what it gets back: `Existing` — there is nothing new — and nothing
/// deliverable, whatever the channel is carrying.
#[tokio::test]
async fn a_sampled_attach_creates_no_queue_on_either_class() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }

        let attached = store
            .attach(&sub, "proc", Depth::Bounded(0), Priming::Retained)
            .await;
        assert_eq!(attached, Attached::Existing, "{}", store.address());
        assert!(!store.has_deliverable(&sub).await, "{}", store.address());
        assert!(
            store.deliverable_subscribers().await.is_empty(),
            "{}",
            store.address()
        );

        let attached = store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;
        assert_eq!(
            attached,
            Attached::Created,
            "the promotion is where the queue begins: {}",
            store.address()
        );
        assert!(store.has_deliverable(&sub).await, "{}", store.address());

        let attached = store
            .attach(&sub, "proc", Depth::Bounded(0), Priming::Head)
            .await;
        assert_eq!(attached, Attached::Existing, "{}", store.address());
        assert!(
            !store.has_deliverable(&sub).await,
            "the demotion took the position with it: {}",
            store.address()
        );
        assert!(
            store.deliverable_subscribers().await.is_empty(),
            "{}",
            store.address()
        );
    }
}

#[tokio::test]
async fn a_sampled_durable_attach_holds_no_cursor() {
    let (store, db) = durable_store(DEPTH).await;
    store.append(message("alice", "a")).await;
    let sub = wasm_sub("proc");

    RetentionStore::attach(&store, &sub, "proc", Depth::Bounded(0), Priming::Retained).await;
    {
        let conn = db.lock().await;
        assert!(load_subscriber_cursor(&conn, store.channel_uuid(), &sub).is_none());
    }

    RetentionStore::attach(&store, &sub, "proc", Depth::Bounded(4), Priming::Head).await;
    {
        let conn = db.lock().await;
        assert!(load_subscriber_cursor(&conn, store.channel_uuid(), &sub).is_some());
    }

    RetentionStore::attach(&store, &sub, "proc", Depth::Bounded(0), Priming::Head).await;
    let conn = db.lock().await;
    assert!(
        load_subscriber_cursor(&conn, store.channel_uuid(), &sub).is_none(),
        "the demotion removes the position"
    );
}

/// The window read is where the caller's depth reaches the store: the row's
/// copy follows the argument, so the next read of the same cursor cannot cut a
/// window at a depth nobody asked for.
#[tokio::test]
async fn a_durable_window_retunes_the_stored_depth() {
    let (store, db) = durable_store(DEPTH).await;
    let sub = wasm_sub("proc");
    RetentionStore::attach(&store, &sub, "proc", Depth::Bounded(2), Priming::Head).await;

    RetentionStore::window(&store, &sub, Depth::Bounded(5), Depth::Bounded(0)).await;

    let conn = db.lock().await;
    let row = load_subscriber_cursor(&conn, store.channel_uuid(), &sub).expect("cursor row");
    assert_eq!(row.push_depth, Depth::Bounded(5));
    assert_eq!(row.next_owed_seq, 1, "a read moves nothing");
}

/// A position retention has outrun owes nothing that can still be served, so it
/// wakes nobody. Reading the wake question off the channel's high-water instead
/// of its retained rows would keep naming this subscriber on every tick, with
/// no window able to satisfy the wake it caused.
///
/// Durable-only: the ring answers the same question from an empty ring, which
/// its own cursor tests pin.
#[tokio::test]
async fn a_position_below_an_emptied_retention_is_not_deliverable() {
    let (store, db) = durable_store_for("proc", DEPTH).await;
    let sub = wasm_sub("proc");
    RetentionStore::attach(&store, &sub, "proc", ATTACH_DEPTH, Priming::Head).await;
    store.append(message("alice", "a")).await;
    assert!(store.has_deliverable(&sub).await);

    {
        let conn = db.lock().await;
        let eviction = bus_gc_evict_channel(
            &conn,
            store.channel_uuid(),
            store.address(),
            ChannelScheme::Brenn,
            0,
            Sink::Drop,
            None,
        );
        assert_eq!(
            eviction.messages_evicted, 1,
            "the channel retains nothing now"
        );
    }

    assert!(!store.has_deliverable(&sub).await);
    assert!(store.deliverable_subscribers().await.is_empty());
}

#[tokio::test]
async fn deliverable_subscribers_and_has_deliverable_agree_across_stores() {
    let (db_store, _db) = durable_store_for("proc", DEPTH).await;
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(DEPTH));

    RetentionStore::attach(
        &db_store,
        &wasm_sub("proc"),
        "proc",
        Depth::Bounded(4),
        Priming::Head,
    )
    .await;
    db_store.append(message("alice", "a")).await;
    ring.attach(&wasm_sub("proc"), "proc", 4, Priming::Head);
    RetentionStore::append(&ring, message_for(&ring, "alice", "a")).await;

    for store in [
        &db_store as &dyn RetentionStore,
        &ring as &dyn RetentionStore,
    ] {
        assert!(
            store.has_deliverable(&wasm_sub("proc")).await,
            "{}",
            store.address()
        );
        assert_eq!(
            owed_ids(store).await,
            vec![wasm_sub("proc")],
            "{}",
            store.address()
        );
        assert!(
            !store.has_deliverable(&wasm_sub("ghost")).await,
            "{}",
            store.address()
        );
    }

    serve(&db_store, &wasm_sub("proc"), DEPTH, 0).await;
    serve(&ring, &wasm_sub("proc"), DEPTH, 0).await;

    for store in [
        &db_store as &dyn RetentionStore,
        &ring as &dyn RetentionStore,
    ] {
        assert!(
            !store.has_deliverable(&wasm_sub("proc")).await,
            "{}",
            store.address()
        );
        assert!(
            store.deliverable_subscribers().await.is_empty(),
            "{}",
            store.address()
        );
    }
}

/// The owed walk reports the loudest urgency a subscriber has not seen, on both
/// classes — the figure the wake pass gates an urgency-gated subscriber on. It
/// is read from the unseen suffix, not from the channel, so a loud message the
/// subscriber has already passed does not keep waking it: what the walk answers
/// changes as the position moves, without anything being stored per message.
#[tokio::test]
async fn the_owed_walk_reports_the_loudest_unseen_urgency() {
    for store in stores_for_proc(DEPTH).await {
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
            .await;

        for (body, urgency) in [
            ("shout", Urgency::High),
            ("chat", Urgency::Low),
            ("chat again", Urgency::Low),
        ] {
            let mut msg = message_for(store.as_ref(), "alice", body);
            msg.urgency = urgency;
            store.append(msg).await;
        }
        let owed = store.deliverable_subscribers().await;
        assert_eq!(
            owed.iter()
                .map(|o| o.max_unseen_urgency)
                .collect::<Vec<_>>(),
            vec![Urgency::High],
            "{}",
            store.address()
        );

        // Pass the loud one; the quiet remainder is what is left to decide on.
        store.advance(&sub, MessageSeq(1), MessageSeq(1)).await;
        let owed = store.deliverable_subscribers().await;
        assert_eq!(
            owed.iter()
                .map(|o| o.max_unseen_urgency)
                .collect::<Vec<_>>(),
            vec![Urgency::Low],
            "{}",
            store.address()
        );
    }
}

/// Every level in turn is the loudest thing a position has not seen. The
/// durable side ranks urgency in hand-written SQL, so the walk is driven off
/// `Urgency::ALL` rather than the two levels one case happens to use: a level
/// added to the enum and not to that mapping fails here instead of quietly
/// ranking below the levels the SQL does know.
#[tokio::test]
async fn the_owed_walk_ranks_every_urgency_level() {
    for urgency in Urgency::ALL {
        for store in stores_for_proc(DEPTH).await {
            let sub = wasm_sub("proc");
            store
                .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
                .await;
            let mut msg = message_for(store.as_ref(), "alice", "one");
            msg.urgency = urgency;
            store.append(msg).await;
            assert_eq!(
                store
                    .deliverable_subscribers()
                    .await
                    .iter()
                    .map(|owed| owed.max_unseen_urgency)
                    .collect::<Vec<_>>(),
                vec![urgency],
                "{} at {urgency:?}",
                store.address()
            );
        }
    }
}

/// The ring answers the same question from a table of suffix maxima indexed by
/// where the position lands in what is *still retained*, so the cases that
/// matter are the ends of that table: a position retention has already outrun
/// reads the loudest survivor, and one sitting just above the loudest survivor
/// reads the quieter suffix over it — down to the last retained seq.
#[tokio::test]
async fn the_ring_ranks_the_unseen_suffix_of_what_survived() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(3));
    let sub = wasm_sub("proc");
    ring.attach(&sub, "proc", DEPTH, Priming::Head);
    for (body, urgency) in [
        ("a", Urgency::High),
        ("b", Urgency::High),
        ("c", Urgency::High),
        ("d", Urgency::Low),
        ("e", Urgency::VeryLow),
    ] {
        let mut msg = message_for(&ring, "alice", body);
        msg.urgency = urgency;
        RetentionStore::append(&ring, msg).await;
    }

    let loudest = |ring: &RingStore| {
        ring.deliverable_subscribers()
            .into_iter()
            .map(|owed| owed.max_unseen_urgency)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        loudest(&ring),
        vec![Urgency::High],
        "the position is below the frontier, so the whole retained window is unseen"
    );

    // Past the loudest survivor — which is also the oldest retained entry.
    RetentionStore::advance(&ring, &sub, MessageSeq(3), MessageSeq(3)).await;
    assert_eq!(
        loudest(&ring),
        vec![Urgency::Low],
        "a loud message the position has passed does not keep waking it"
    );

    RetentionStore::advance(&ring, &sub, MessageSeq(4), MessageSeq(4)).await;
    assert_eq!(
        loudest(&ring),
        vec![Urgency::VeryLow],
        "at the last retained seq the suffix is that one entry"
    );

    RetentionStore::advance(&ring, &sub, MessageSeq(5), MessageSeq(5)).await;
    assert!(
        loudest(&ring).is_empty(),
        "caught up: nothing unseen, so nothing to rank"
    );
}

/// A sampled subscriber holds no position, so nothing is ever owed to it and the
/// walk never names it — on either class, and whatever the channel carries.
#[tokio::test]
async fn a_sampled_subscriber_is_never_owed_anything() {
    for store in stores_for_proc(DEPTH).await {
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", Depth::Bounded(0), Priming::Head)
            .await;
        store
            .append(message_for(store.as_ref(), "alice", "loud"))
            .await;
        assert!(
            store.deliverable_subscribers().await.is_empty(),
            "{}",
            store.address()
        );
    }
}

#[tokio::test]
async fn detach_tears_down_a_subscribers_delivery_state() {
    for store in stores(DEPTH).await {
        for body in ["a", "b", "c"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;
        assert!(store.has_deliverable(&sub).await, "{}", store.address());

        // Detaching an unknown subscriber removes nothing and leaves the real
        // one owed.
        store.detach(&wasm_sub("ghost")).await;
        assert!(store.has_deliverable(&sub).await, "{}", store.address());

        store.detach(&sub).await;
        assert!(!store.has_deliverable(&sub).await, "{}", store.address());
        assert!(
            store.deliverable_subscribers().await.is_empty(),
            "{}",
            store.address()
        );

        // The messages are still retained for whoever attaches next.
        assert_eq!(
            retained_bodies(store.as_ref()).await,
            vec!["a", "b", "c"],
            "{}",
            store.address()
        );
    }
}

// ── The window and the advance ──────────────────────────────────────────────

/// Bodies of a window's new portion, oldest first.
fn new_bodies(window: &SubscriberWindow) -> Vec<String> {
    window
        .new_entries()
        .iter()
        .map(|(_, envelope)| envelope.body.clone())
        .collect()
}

/// Bodies of the whole window — context then new — oldest first.
fn window_bodies(window: &SubscriberWindow) -> Vec<String> {
    window
        .entries
        .iter()
        .map(|(_, envelope)| envelope.body.clone())
        .collect()
}

/// Read the window and advance over it, as a push consumer does: what it was
/// handed, and what the advance found it had never been handed.
async fn serve(
    store: &dyn RetentionStore,
    sub: &ParticipantId,
    push_limit: u64,
    retain_limit: u64,
) -> (Vec<String>, AdvanceOutcome) {
    let window = store
        .window(
            sub,
            Depth::Bounded(push_limit),
            Depth::Bounded(retain_limit),
        )
        .await;
    let new = new_bodies(&window);
    let advance = match window.advance_span() {
        Some((through, seen_floor)) => store.advance(sub, through, seen_floor).await,
        None => AdvanceOutcome::default(),
    };
    (new, advance)
}

/// A retained-primed queue is owed the tail, and the window hands it over oldest
/// first with nothing reported.
#[tokio::test]
async fn a_window_serves_the_owed_tail_oldest_first() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b", "c"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;

        let (new, advance) = serve(store.as_ref(), &sub, DEPTH, 0).await;
        assert_eq!(new, vec!["a", "b", "c"], "{}", store.address());
        assert_eq!(advance.dropped, 0, "{}", store.address());
        assert_eq!(advance.noise_charge, 0, "{}", store.address());
    }
}

/// The push limit is a drop-oldest cut on both classes: the window serves the
/// newest, and the advance over it reports what it skipped. Nothing is held back
/// for a next read.
#[tokio::test]
async fn the_push_limit_serves_the_newest_and_reports_the_rest() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b", "c", "d"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;

        let (new, advance) = serve(store.as_ref(), &sub, 2, 0).await;
        assert_eq!(
            new,
            vec!["c", "d"],
            "newest window wins: {}",
            store.address()
        );
        assert_eq!(
            advance.dropped,
            2,
            "the two the cut skipped: {}",
            store.address()
        );

        let (again, _) = serve(store.as_ref(), &sub, 2, 0).await;
        assert!(
            again.is_empty(),
            "nothing was held back for the next read: {}",
            store.address()
        );
    }
}

/// `retain_limit` above `push_limit` widens the window without widening what is
/// new. The extra unseen entries are served as context — delivered, so the
/// advance charges nothing for them.
#[tokio::test]
async fn unseen_context_inside_the_window_is_not_a_drop() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b", "c", "d"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;

        let window = store
            .window(&sub, Depth::Bounded(1), Depth::Bounded(4))
            .await;
        assert_eq!(
            window_bodies(&window),
            vec!["a", "b", "c", "d"],
            "{}",
            store.address()
        );
        assert_eq!(new_bodies(&window), vec!["d"], "{}", store.address());

        let (through, seen_floor) = window.advance_span().expect("the window served entries");
        let advance = store.advance(&sub, through, seen_floor).await;
        assert_eq!(
            advance.dropped,
            0,
            "every unseen entry was served: {}",
            store.address()
        );
    }
}

/// A sampled subscriber (`push_limit = 0`) is never delivered to: its window is
/// all context, whatever the channel holds.
#[tokio::test]
async fn a_sampled_window_is_all_context() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;

        let window = store
            .window(&sub, Depth::Bounded(0), Depth::Bounded(4))
            .await;
        assert_eq!(
            window_bodies(&window),
            vec!["a", "b"],
            "{}",
            store.address()
        );
        assert!(window.new_entries().is_empty(), "{}", store.address());
        assert_eq!(window.new_from, 2, "{}", store.address());
    }
}

/// The window is one span of `max(push_limit, retain_limit)`, not a context read
/// glued to a delivery read: a `P = 2`, `R = 4` port over a longer retained
/// history gets four entries, of which two are new — never `R + P` of them.
#[tokio::test]
async fn the_window_is_capped_by_the_larger_limit_not_their_sum() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;

        let window = store
            .window(&sub, Depth::Bounded(2), Depth::Bounded(4))
            .await;
        assert_eq!(
            window_bodies(&window),
            vec!["e", "f", "g", "h"],
            "the newest max(2, 4): {}",
            store.address()
        );
        assert_eq!(new_bodies(&window), vec!["g", "h"], "{}", store.address());
    }
}

/// An unbounded push limit — the system-subscriber shape — makes every unseen
/// retained message new, and the advance over that window reports nothing: an
/// unbounded window clamps nothing away.
#[tokio::test]
async fn an_unbounded_push_limit_serves_every_unseen_entry() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b", "c", "d", "e"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;

        let window = store
            .window(&sub, Depth::Unbounded, Depth::Bounded(0))
            .await;
        assert_eq!(
            new_bodies(&window),
            vec!["a", "b", "c", "d", "e"],
            "{}",
            store.address()
        );

        let (through, seen_floor) = window.advance_span().expect("the window served entries");
        let advance = store.advance(&sub, through, seen_floor).await;
        assert_eq!(advance.dropped, 0, "{}", store.address());
        assert_eq!(advance.noise_charge, 0, "{}", store.address());
    }
}

/// An unbounded retain limit widens the window to the whole retained set without
/// widening what is new: the push limit alone decides the boundary.
#[tokio::test]
async fn an_unbounded_retain_limit_widens_the_window_not_the_new_set() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b", "c", "d", "e"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;

        let window = store
            .window(&sub, Depth::Bounded(1), Depth::Unbounded)
            .await;
        assert_eq!(
            window_bodies(&window),
            vec!["a", "b", "c", "d", "e"],
            "{}",
            store.address()
        );
        assert_eq!(new_bodies(&window), vec!["e"], "{}", store.address());

        let (through, seen_floor) = window.advance_span().expect("the window served entries");
        let advance = store.advance(&sub, through, seen_floor).await;
        assert_eq!(
            advance.dropped,
            0,
            "every unseen entry was served, as context or as new: {}",
            store.address()
        );
    }
}

/// A sampled subscriber holds no position, so its window offers nothing to
/// advance over: the generic window→advance pattern cannot move or charge one.
#[tokio::test]
async fn a_sampled_window_offers_no_advance() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        for body in ["a", "b"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Retained)
            .await;

        let window = store
            .window(&sub, Depth::Bounded(0), Depth::Bounded(4))
            .await;
        assert!(!window.entries.is_empty(), "{}", store.address());
        assert!(
            window.advance_span().is_none(),
            "a sampled window advances nothing: {}",
            store.address()
        );

        let (_, advance) = serve(store.as_ref(), &sub, 0, 4).await;
        assert_eq!(advance, AdvanceOutcome::default(), "{}", store.address());
    }
}

/// Advancing a sampled subscriber anyway is a wiring bug, not a tolerated
/// no-op: a sampled attach leaves no position on either class, and the store
/// dies rather than move one that does not exist.
#[tokio::test]
#[should_panic(expected = "has no queue for subscriber")]
async fn advancing_a_sampled_ring_subscriber_panics() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(DEPTH));
    let sub = wasm_sub("sampled");
    RetentionStore::attach(&ring, &sub, "sampled", Depth::Bounded(0), Priming::Head).await;
    RetentionStore::append(&ring, message_for(&ring, "alice", "a")).await;
    RetentionStore::advance(&ring, &sub, MessageSeq(1), MessageSeq(1)).await;
}

#[tokio::test]
#[should_panic(expected = "to advance")]
async fn advancing_a_sampled_durable_subscriber_panics() {
    let (store, _db) = durable_store(DEPTH).await;
    let sub = wasm_sub("sampled");
    RetentionStore::attach(&store, &sub, "sampled", Depth::Bounded(0), Priming::Head).await;
    store.append(message("alice", "a")).await;
    RetentionStore::advance(&store, &sub, MessageSeq(1), MessageSeq(1)).await;
}

/// The same contract for a subscriber that never attached at all: with no
/// position to move, the durable store dies rather than retire the claims below
/// a cursor that was never there.
#[tokio::test]
#[should_panic(expected = "to advance")]
async fn advancing_an_unattached_durable_subscriber_panics() {
    let (store, _db) = durable_store(DEPTH).await;
    store.append(message("alice", "a")).await;
    RetentionStore::advance(&store, &wasm_sub("stranger"), MessageSeq(1), MessageSeq(1)).await;
}

/// Reading a window owed nothing is empty, not an error, and there is nothing to
/// advance over.
#[tokio::test]
async fn an_idle_queue_serves_an_empty_window() {
    for store in stores(DEPTH).await {
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
            .await;

        let window = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(0))
            .await;
        assert!(window.entries.is_empty(), "{}", store.address());
        assert!(window.advance_span().is_none(), "{}", store.address());
    }
}

/// The read moves nothing: two windows in a row are the same window, and the
/// subscriber is still owed what it has not advanced over.
#[tokio::test]
async fn a_window_read_moves_nothing() {
    for store in stores_for_proc(DEPTH).await {
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
            .await;
        for body in ["a", "b", "c"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }

        let first = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(0))
            .await;
        assert_eq!(
            new_bodies(&first),
            vec!["a", "b", "c"],
            "{}",
            store.address()
        );
        let second = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(0))
            .await;
        assert_eq!(
            new_bodies(&second),
            new_bodies(&first),
            "{}",
            store.address()
        );
        assert!(store.has_deliverable(&sub).await, "{}", store.address());
    }
}

/// The activation trigger gate is sound on both classes: whenever a store says
/// a subscriber is owed nothing, the window it would serve carries nothing new.
/// A `false` hiding new messages would silently prevent an activation.
#[tokio::test]
async fn nothing_deliverable_means_nothing_new_in_the_window() {
    for store in stores_for_proc(DEPTH).await {
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
            .await;

        // Idle: attached, nothing published.
        assert!(!store.has_deliverable(&sub).await, "{}", store.address());
        let window = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(DEPTH))
            .await;
        assert!(window.new_entries().is_empty(), "{}", store.address());

        // Owed.
        for body in ["a", "b"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }
        assert!(store.has_deliverable(&sub).await, "{}", store.address());

        // Caught up: advanced over everything the window served.
        let window = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(DEPTH))
            .await;
        let (through, seen_floor) = window.advance_span().expect("the window served entries");
        store.advance(&sub, through, seen_floor).await;
        assert!(!store.has_deliverable(&sub).await, "{}", store.address());
        let window = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(DEPTH))
            .await;
        assert!(
            window.new_entries().is_empty(),
            "caught up but the window still holds new entries: {}",
            store.address()
        );
        assert_eq!(
            window_bodies(&window),
            vec!["a", "b"],
            "the context is still served: {}",
            store.address()
        );
    }
}

/// A delivery that got partway through advances only over what the far end took,
/// and the remainder is served again — the property that lets a store serve a
/// consumer whose delivery can fail halfway.
#[tokio::test]
async fn advancing_over_a_prefix_leaves_the_remainder_owed() {
    for store in stores_for_proc(DEPTH).await {
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
            .await;
        for body in ["a", "b", "c"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }

        let window = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(0))
            .await;
        let accepted = &window.new_entries()[..2];
        let advance = store.advance(&sub, accepted[1].0, accepted[0].0).await;
        assert_eq!(advance.dropped, 0, "{}", store.address());

        let (new, _) = serve(store.as_ref(), &sub, DEPTH, 0).await;
        assert_eq!(
            new,
            vec!["c"],
            "only the unaccepted remainder is still owed: {}",
            store.address()
        );
    }
}

/// A delivery that failed outright advances nothing and keeps every obligation.
#[tokio::test]
async fn advancing_nothing_keeps_every_obligation() {
    for store in stores_for_proc(DEPTH).await {
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
            .await;
        store
            .append(message_for(store.as_ref(), "alice", "a"))
            .await;

        let window = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(0))
            .await;
        let floor = window.entries[0].0;
        // Advancing to just below the floor is the "accepted nothing" advance.
        let advance = store.advance(&sub, MessageSeq(floor.0 - 1), floor).await;
        assert_eq!(advance.dropped, 0, "{}", store.address());

        let (new, _) = serve(store.as_ref(), &sub, DEPTH, 0).await;
        assert_eq!(new, vec!["a"], "{}", store.address());
    }
}

/// Re-advancing over a window already passed reports nothing a second time: the
/// figure is a subtraction against a position that has already moved.
#[tokio::test]
async fn advance_is_idempotent() {
    for store in stores_for_proc(DEPTH).await {
        let sub = wasm_sub("proc");
        store
            .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
            .await;
        for body in ["a", "b"] {
            let msg = message_for(store.as_ref(), "alice", body);
            store.append(msg).await;
        }

        let window = store
            .window(&sub, Depth::Bounded(DEPTH), Depth::Bounded(0))
            .await;
        let (through, seen_floor) = window.advance_span().expect("entries were served");
        store.advance(&sub, through, seen_floor).await;
        let again = store.advance(&sub, through, seen_floor).await;
        assert_eq!(again.dropped, 0, "{}", store.address());
        assert_eq!(again.noise_charge, 0, "{}", store.address());
    }
}

/// A sampled subscriber holds no position on either class, so its window is a
/// pure channel read: the ambience, none of it new, whether or not the store
/// ever heard of it.
#[tokio::test]
async fn a_durable_sampled_window_is_all_context() {
    let (store, _db) = durable_store_for("proc", DEPTH).await;
    store.append(message("alice", "a")).await;
    let window = store
        .window(&wasm_sub("ghost"), Depth::Bounded(0), Depth::Bounded(DEPTH))
        .await;
    assert!(window.new_entries().is_empty());
    assert_eq!(window_bodies(&window), vec!["a"], "the ambience is served");
    assert_eq!(window.new_from, 1, "and all of it is context");
}

/// Both classes keep an explicit position per push-enabled subscriber, so a
/// push window over a queue that was never created is a wiring bug rather than
/// an empty window.
#[tokio::test]
#[should_panic(expected = "a push-enabled window over a queue that was never created")]
async fn a_durable_window_panics_for_an_unattached_subscriber() {
    let (store, _db) = durable_store_for("proc", DEPTH).await;
    store.append(message("alice", "a")).await;
    RetentionStore::window(
        &store,
        &wasm_sub("ghost"),
        Depth::Bounded(DEPTH),
        Depth::Bounded(0),
    )
    .await;
}

/// The ring keeps an explicit cursor per subscriber, so reading without one is a
/// wiring bug rather than an empty window.
#[tokio::test]
#[should_panic(expected = "has no queue for subscriber")]
async fn a_ring_window_panics_for_an_unattached_subscriber() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(DEPTH));
    RetentionStore::window(
        &ring,
        &wasm_sub("ghost"),
        Depth::Bounded(DEPTH),
        Depth::Bounded(0),
    )
    .await;
}

/// The ring twin of the durable clamp: an eviction retires a delivery obligation,
/// so it is reported by the append that retired it. Neither store makes a
/// subscriber read before its losses are accountable — which is what lets an
/// absent consumer's overflow escalate at all.
#[tokio::test]
async fn ring_eviction_reports_the_loss_without_a_read() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(2));
    let sub = wasm_sub("absent");
    ring.attach(&sub, "absent", 8, Priming::Head);
    for body in ["a", "b"] {
        assert!(
            RetentionStore::append(&ring, message_for(&ring, "alice", body))
                .await
                .overflow
                .is_empty(),
            "nothing evicted yet"
        );
    }

    let evicting = RetentionStore::append(&ring, message_for(&ring, "alice", "c")).await;
    assert_eq!(
        evicting.overflow,
        vec![OverflowEvent {
            subscriber: sub.clone(),
            dropped: 1,
            app_slug: Some("absent".to_string()),
        }]
    );
}

/// The durable twin of the row above: the GC pass that outruns a position
/// reports against it by name, with no read from the subscriber and no cursor
/// movement — and reports each evicted seq exactly once, so a wedged subscriber
/// escalates at the pass's cadence rather than accumulating a re-report per
/// pass.
#[tokio::test]
async fn durable_eviction_reports_the_loss_once_per_evicted_span() {
    let (store, db) = durable_store_for("proc", DEPTH).await;
    let sub = wasm_sub("proc");
    store
        .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
        .await;
    for body in ["a", "b", "c", "d"] {
        RetentionStore::append(&store, message_for(&store, "alice", body)).await;
    }

    let evict = |frontier: u64| {
        let db = db.clone();
        let store = &store;
        async move {
            let conn = db.lock().await;
            bus_gc_evict_channel(
                &conn,
                store.channel_uuid(),
                store.address(),
                ChannelScheme::Brenn,
                frontier,
                Sink::Drop,
                None,
            )
        }
    };

    let first = evict(3).await;
    assert_eq!(first.messages_evicted, 1);
    assert_eq!(
        first.overflow,
        vec![OverflowEvent {
            subscriber: sub.clone(),
            dropped: 1,
            app_slug: Some("proc".to_string()),
        }],
        "the app slug rides the cursor row, so the report is attributable"
    );

    let second = evict(2).await;
    assert_eq!(second.messages_evicted, 1);
    assert_eq!(
        second.overflow,
        vec![OverflowEvent {
            subscriber: sub.clone(),
            dropped: 1,
            app_slug: Some("proc".to_string()),
        }],
        "the second pass reports only its own span, never the first pass's"
    );

    let third = evict(2).await;
    assert_eq!(third.messages_evicted, 0);
    assert!(
        third.overflow.is_empty(),
        "a pass that evicts nothing reports nothing, however far the cursor lags"
    );
}

/// The two eviction reporters agree on the figure for the same history. They
/// report at different moments — the ring at the displacing append, the durable
/// store at the GC pass that outruns the position — but the same four messages
/// over a two-deep retention cost a never-running subscriber the same two seqs
/// on either class. Deposition, not delivery, is what a drop is.
#[tokio::test]
async fn eviction_reporting_agrees_across_the_classes() {
    let sub = wasm_sub("absent");
    const HISTORY: [&str; 4] = ["a", "b", "c", "d"];
    const RETAINED: u64 = 2;

    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(RETAINED));
    ring.attach(&sub, "absent", DEPTH, Priming::Head);
    let mut ring_reported = 0;
    for body in HISTORY {
        ring_reported += RetentionStore::append(&ring, message_for(&ring, "alice", body))
            .await
            .overflow
            .iter()
            .map(|e| e.dropped)
            .sum::<u64>();
    }

    let (store, db) = durable_store_for("absent", DEPTH).await;
    store
        .attach(&sub, "absent", ATTACH_DEPTH, Priming::Head)
        .await;
    for body in HISTORY {
        RetentionStore::append(&store, message_for(&store, "alice", body)).await;
    }
    let durable_reported = {
        let conn = db.lock().await;
        bus_gc_evict_channel(
            &conn,
            store.channel_uuid(),
            store.address(),
            ChannelScheme::Brenn,
            RETAINED,
            Sink::Drop,
            None,
        )
        .overflow
        .iter()
        .map(|e| e.dropped)
        .sum::<u64>()
    };

    assert_eq!(ring_reported, 2);
    assert_eq!(durable_reported, ring_reported);
}

/// A caught-up subscriber is not reported against by an eviction: its position
/// is above the span the pass removed, and a sampled subscriber holds no
/// position for a pass to find at all.
#[tokio::test]
async fn durable_eviction_reports_only_positions_it_outran() {
    let (store, db) = durable_store_for("proc", DEPTH).await;
    let caught_up = wasm_sub("proc");
    let sampled = wasm_sub("sampled");
    store
        .attach(&caught_up, "proc", ATTACH_DEPTH, Priming::Head)
        .await;
    store
        .attach(&sampled, "sampled", Depth::Bounded(0), Priming::Head)
        .await;
    for body in ["a", "b", "c"] {
        RetentionStore::append(&store, message_for(&store, "alice", body)).await;
    }
    serve(&store, &caught_up, DEPTH, 0).await;

    let conn = db.lock().await;
    let eviction = bus_gc_evict_channel(
        &conn,
        store.channel_uuid(),
        store.address(),
        ChannelScheme::Brenn,
        1,
        Sink::Drop,
        None,
    );
    assert_eq!(eviction.messages_evicted, 2);
    assert!(eviction.overflow.is_empty());
}

/// The durable advance reports a loss whatever retired the claims that named
/// it: the figure is the distance between the cursor and the window's own
/// floor, so two messages evicted out from under a lagging subscriber are two
/// drops, exactly as the ring reports for the same history.
///
/// The ladder is charged nothing here, and that is the frontier bound doing its
/// job: everything below the retention frontier was reported by the eviction
/// that retired it, so the advance that passes it does not enact it a second
/// time.
#[tokio::test]
async fn durable_advance_reports_a_loss_gc_already_retired() {
    let (store, db) = durable_store_for("proc", DEPTH).await;
    let sub = wasm_sub("proc");
    store
        .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
        .await;
    for body in ["a", "b", "c", "d"] {
        let msg = message_for(&store, "alice", body);
        RetentionStore::append(&store, msg).await;
    }

    // Retention outruns the subscriber: the two oldest go, and their claims with
    // them.
    {
        let conn = db.lock().await;
        let eviction = bus_gc_evict_channel(
            &conn,
            store.channel_uuid(),
            store.address(),
            ChannelScheme::Brenn,
            2,
            Sink::Drop,
            None,
        );
        assert_eq!(eviction.messages_evicted, 2);
        assert_eq!(
            eviction.push_rows_retired, 2,
            "the lagging subscriber's claims go too"
        );
        assert_eq!(
            eviction.overflow.first().map(|e| e.dropped),
            Some(2),
            "the pass reports the whole span it took from this position"
        );
    }

    let (new, advance) = serve(&store, &sub, DEPTH, 0).await;
    assert_eq!(new, vec!["c", "d"], "only what survived is servable");
    assert_eq!(
        advance.dropped, 2,
        "the two seqs no window ever served it, by subtraction"
    );
    assert_eq!(
        advance.noise_charge, 0,
        "the eviction that retired them reports them; the advance does not re-enact it"
    );
}

/// A loss straddling the retention frontier splits between the two reporters:
/// the part the eviction pass already reported is not charged again, and the
/// part it never touched — unseen but still retained, clamped away by the push
/// limit — is. Both terms of the bound are load-bearing here, because the
/// cursor sits above the frontier rather than at it: charging from the frontier
/// alone would re-enact what the eviction pass already escalated.
#[tokio::test]
async fn durable_advance_charges_only_the_still_retained_half_of_a_loss() {
    let (store, db) = durable_store_for("proc", DEPTH).await;
    let sub = wasm_sub("proc");
    store
        .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
        .await;

    for body in ["a", "b"] {
        RetentionStore::append(&store, message_for(&store, "alice", body)).await;
    }
    let (new, _) = serve(&store, &sub, DEPTH, 0).await;
    assert_eq!(new, vec!["a", "b"], "the cursor is at seq 3 now");

    for body in ["c", "d", "e"] {
        RetentionStore::append(&store, message_for(&store, "alice", body)).await;
    }
    {
        let conn = db.lock().await;
        let eviction = bus_gc_evict_channel(
            &conn,
            store.channel_uuid(),
            store.address(),
            ChannelScheme::Brenn,
            2,
            Sink::Drop,
            None,
        );
        assert_eq!(
            eviction.messages_evicted, 3,
            "the frontier lands above the cursor, at seq 4"
        );
        assert_eq!(
            eviction.overflow.first().map(|e| e.dropped),
            Some(1),
            "only seq 3 was both unseen and evicted"
        );
    }

    // The push limit serves only the newest, so seq 4 is unseen, still
    // retained, and never served: the advance's own charge.
    let (new, advance) = serve(&store, &sub, 1, 0).await;
    assert_eq!(new, vec!["e"]);
    assert_eq!(advance.dropped, 2, "seqs 3 and 4 were never served");
    assert_eq!(
        advance.noise_charge, 1,
        "seq 3 went with the eviction that reported it; only seq 4 is charged here"
    );
}

/// The one thing that perforates retention today: a message spared by a
/// tentative delivery hold, older than the ones the same pass deletes. It keeps
/// the frontier at its own seq, so the pass's frontier pair understates what it
/// removed, and a window wide enough to still serve the spared message starts
/// its advance below the gap — so neither reporter charges the seqs in between.
///
/// Positions and bodies are untouched by this: what is served is exactly what
/// survives, and the cursor moves exactly over it. Only the ladder's figure is
/// short. Pinned so the accounting is a known quantity while the hold exists;
/// deleting the tentative lifecycle restores density and makes both subtractions
/// exact again, and this row goes with it.
#[tokio::test]
async fn a_tentative_hold_below_an_evicted_span_shortens_the_report() {
    let (store, db) = durable_store_for("proc", DEPTH).await;
    let sub = wasm_sub("proc");
    store
        .attach(&sub, "proc", ATTACH_DEPTH, Priming::Head)
        .await;
    for body in ["a", "b", "c", "d"] {
        RetentionStore::append(&store, message_for(&store, "alice", body)).await;
    }

    // Hold the oldest message's delivery below water, as an unconfirmed surface
    // delivery does. Whose row carries the flag does not matter to eviction —
    // the flag alone spares the message.
    {
        let conn = db.lock().await;
        conn.execute(
            "UPDATE messaging_pending_pushes SET confirm_pending = 1
             WHERE message_id = (SELECT id FROM messaging_messages
                                 ORDER BY retained_seq ASC LIMIT 1)",
            [],
        )
        .expect("hold the oldest delivery");
    }

    let eviction = {
        let conn = db.lock().await;
        bus_gc_evict_channel(
            &conn,
            store.channel_uuid(),
            store.address(),
            ChannelScheme::Brenn,
            1,
            Sink::Drop,
            None,
        )
    };
    assert_eq!(
        eviction.messages_evicted, 2,
        "seqs 2 and 3 go; the held seq 1 stays, out of retention order"
    );
    assert!(
        eviction.overflow.is_empty(),
        "the held message keeps the frontier at seq 1, so the pair spans nothing"
    );

    let (new, advance) = serve(&store, &sub, DEPTH, 0).await;
    assert_eq!(
        new,
        vec!["a", "d"],
        "the held message is still retained, so the window still serves it"
    );
    assert_eq!(
        advance.dropped, 0,
        "the window's floor is the held seq, so the subtraction sees no gap"
    );
    assert_eq!(advance.noise_charge, 0, "and the ladder is charged nothing");
}

#[tokio::test]
async fn a_parked_message_owes_no_subscriber_until_release() {
    for store in stores(DEPTH).await {
        store
            .park(message_for(&*store, "alice", "later"), soon())
            .await
            .expect("within cap");
        assert!(
            store.deliverable_subscribers().await.is_empty(),
            "parked message must owe no one: {}",
            store.address()
        );
        assert!(
            !store.has_deliverable(&wasm_sub("proc")).await,
            "{}",
            store.address()
        );
    }
}

/// Targeting happens at release, not at park: a subscriber that attached while
/// the message waited is owed it.
#[tokio::test]
async fn a_subscriber_attached_mid_park_receives_the_message_at_release() {
    for store in stores_for_proc(DEPTH).await {
        let release_at = soon();
        store
            .park(message_for(&*store, "alice", "later"), release_at)
            .await
            .expect("within cap");

        // Attaches after the park, so nothing about it was knowable then.
        let latecomer = wasm_sub("proc");
        store
            .attach(&latecomer, "proc", ATTACH_DEPTH, Priming::Head)
            .await;

        store.release_due(release_at).await;

        let (new, _) = serve(store.as_ref(), &latecomer, DEPTH, 0).await;
        assert_eq!(
            new,
            vec!["later"],
            "attached mid-park and owed the release: {}",
            store.address()
        );
    }
}

/// A claim minted before the message was parked does not survive the park: an
/// edit that re-parks a live message hands targeting back to the release, so a
/// subscriber that is no longer a target is not delivered to.
///
/// The departed subscriber's push window must lose the claim with it, and lose
/// it *uncharged*: the claim left because it was never owed, not because its
/// subscriber fell behind. A window still counting it would retire the next live
/// claim early, and charging it would report a loss that never happened.
#[tokio::test]
async fn a_claim_predating_the_park_is_replaced_at_release() {
    let (store, db) =
        durable_store_at_subscribed(Depth::Bounded(DEPTH), vec![wasm_target("proc", None)]).await;
    store
        .attach(&wasm_sub("proc"), "proc", ATTACH_DEPTH, Priming::Head)
        .await;
    let departed = wasm_sub("gone");
    let params = PushRetireParams {
        app_slug: "gone",
        subscriber: &departed,
        push_depth: Depth::Bounded(1),
    };

    // A live publish, plus the claim a subscriber held on it back when it was
    // still a target — the state an edit re-parks the message underneath.
    let committed = store.append(message("alice", "a")).await.committed;
    let stale = seed_claim(&store, &db, &departed, "gone", committed.message_uuid).await;
    // Through the publish path's accounting, so the release finds a real window
    // entry to forget rather than an empty map.
    {
        let conn = db.lock().await;
        assert!(
            store
                .record_push_and_check_overflow(&params, stale, &conn)
                .is_none(),
            "the first claim fits a depth-1 window"
        );
    }
    let release_at = soon();
    {
        let conn = db.lock().await;
        conn.execute(
            "UPDATE messaging_messages SET deliver_after = ?2, retained_seq = NULL \
             WHERE uuid = ?1",
            rusqlite::params![
                committed.message_uuid.as_bytes().to_vec(),
                crate::db::format_ts_for_db(release_at)
            ],
        )
        .expect("re-park the live message");
    }

    store.release_due(release_at).await;

    let claimants: Vec<String> = {
        let conn = db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT target_subscriber FROM messaging_pending_pushes \
                 WHERE delivered_at IS NULL",
            )
            .expect("prepare");
        stmt.query_map([], |row| row.get(0))
            .expect("query")
            .map(|r| r.expect("read subscriber"))
            .collect()
    };
    assert_eq!(
        claimants,
        vec![wasm_sub("proc").as_str().to_string()],
        "the release-time target set is the whole target set"
    );
    assert!(
        !store.has_deliverable(&departed).await,
        "a subscriber that is no longer a target keeps no claim"
    );

    let (new, _) = serve(&store, &wasm_sub("proc"), DEPTH, 0).await;
    assert_eq!(new, vec!["a"]);

    let next = store.append(message("alice", "b")).await.committed;
    let fresh = seed_claim(&store, &db, &departed, "gone", next.message_uuid).await;
    let conn = db.lock().await;
    assert!(
        store
            .record_push_and_check_overflow(&params, fresh, &conn)
            .is_none(),
        "a window still counting the withdrawn claim would retire this live one"
    );
}

/// Mint an undelivered claim on `message_uuid` for a subscriber the channel does
/// not register — the residue a departed subscription leaves behind, which the
/// commit path itself can no longer produce now that a store resolves its own
/// targets. Returns the claim id.
async fn seed_claim(
    store: &DbStore,
    db: &crate::db::Db,
    subscriber: &ParticipantId,
    app_slug: &str,
    message_uuid: Uuid,
) -> i64 {
    let conn = db.lock().await;
    let message_id: i64 = conn
        .query_row(
            "SELECT id FROM messaging_messages WHERE uuid = ?1",
            rusqlite::params![message_uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .expect("the committed message row");
    crate::messaging::db::seed_pending_pushes_for_messages(
        &conn,
        &[message_id],
        subscriber,
        app_slug,
    );
    crate::messaging::db::load_pending_pushes_for_channel(&conn, subscriber, store.channel_uuid())
        .into_iter()
        .map(|row| row.push_id)
        .next_back()
        .expect("the seeded claim")
}

#[tokio::test]
async fn a_released_durable_push_makes_its_target_owed() {
    let (store, _db) =
        durable_store_at_subscribed(Depth::Bounded(DEPTH), vec![wasm_target("proc", None)]).await;
    store
        .attach(&wasm_sub("proc"), "proc", ATTACH_DEPTH, Priming::Head)
        .await;
    let release_at = soon();
    store
        .park(message("alice", "later"), release_at)
        .await
        .expect("within cap");
    assert!(store.deliverable_subscribers().await.is_empty());

    store.release_due(release_at).await;
    assert_eq!(owed_ids(&store).await, vec![wasm_sub("proc")]);
    assert!(store.has_deliverable(&wasm_sub("proc")).await);
}

/// A commit names its own targets from the channel's registrations, so a
/// publisher cannot decide who a message is owed to — nor forget someone who
/// subscribed since it resolved a list of its own.
#[tokio::test]
async fn a_commit_owes_the_message_to_the_channels_registered_subscribers() {
    for store in stores_for_proc(DEPTH).await {
        // The ring's registration is its attached cursor; the durable store's is
        // the directory subscriber the fixture registered. Neither is told by
        // this caller who to deliver to.
        store
            .attach(&wasm_sub("proc"), "proc", ATTACH_DEPTH, Priming::Head)
            .await;
        store.append(message_for(&*store, "alice", "a")).await;

        assert_eq!(
            owed_ids(store.as_ref()).await,
            vec![wasm_sub("proc")],
            "{}",
            store.address()
        );
    }
}

/// The delivery-time ACL gate runs inside the commit, on the same registrations
/// the release pass reads. A subscriber whose policy no longer covers the
/// channel is minted no claim, so a revoked subscription stops receiving at the
/// publish that follows the revocation.
#[tokio::test]
async fn a_commit_skips_a_subscriber_whose_policy_no_longer_covers_the_channel() {
    let (store, _db) = durable_store_at_subscribed_under(
        Depth::Bounded(DEPTH),
        vec![wasm_target("proc", None)],
        crate::access::acl::ChannelMatcher::Exact("brenn:elsewhere".to_string()),
    )
    .await;

    let committed = store.append(message("alice", "a")).await.committed;
    assert!(
        committed.target_records.is_empty(),
        "a subscriber the ACL no longer covers is minted no claim"
    );
    assert!(store.deliverable_subscribers().await.is_empty());
    assert_eq!(
        retained_bodies(&store).await,
        vec!["a"],
        "the message still enters retention"
    );
}

/// The release pass runs the delivery-time ACL gate, because it resolves its
/// targets when it releases rather than reading a list the park recorded. A
/// subscriber whose policy stopped covering the channel while its message
/// waited is not delivered to.
#[tokio::test]
async fn release_skips_a_subscriber_whose_policy_no_longer_covers_the_channel() {
    let (store, _db) = durable_store_at_subscribed_under(
        Depth::Bounded(DEPTH),
        vec![wasm_target("proc", None)],
        crate::access::acl::ChannelMatcher::Exact("brenn:elsewhere".to_string()),
    )
    .await;
    let release_at = soon();
    store
        .park(message("alice", "later"), release_at)
        .await
        .expect("within cap");

    let released = store.release_due(release_at).await.released;
    assert_eq!(released.len(), 1, "the message still enters retention");
    assert!(
        released[0].target_records.is_empty(),
        "a subscriber the ACL no longer covers is minted no claim"
    );
    assert!(store.deliverable_subscribers().await.is_empty());
}

// ── The durable store's second grain ──────────────────────────────────────
//
// Every shared-contract read above joins through `messaging_messages`, so a
// regression in the push-row half of a deferral write passes the battery and
// then loses a delivery in production. These cases count the rows.

/// Undelivered push rows on this channel, whatever their release state.
async fn push_row_count(db: &crate::db::Db) -> i64 {
    let conn = db.lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_pending_pushes WHERE delivered_at IS NULL",
        [],
        |row| row.get(0),
    )
    .expect("count pending pushes")
}

/// A parked message holds no delivery records, and cancelling it means none are
/// ever minted. A cancelled message that still released would be delivered to
/// whoever is attached then — exactly what its sender cancelled.
#[tokio::test]
async fn a_cancelled_parked_message_mints_no_claim_at_release() {
    let (store, db) =
        durable_store_at_subscribed(Depth::Bounded(DEPTH), vec![wasm_target("proc", None)]).await;
    let release_at = soon();
    let cancelled = store
        .park(message("alice", "a"), release_at)
        .await
        .expect("within cap");
    // A second parked message keeps the count from being trivially zero.
    store
        .park(message("alice", "b"), release_at)
        .await
        .expect("within cap");
    assert_eq!(
        push_row_count(&db).await,
        0,
        "a parked message is owed to nobody, so it holds no claim"
    );

    assert_eq!(
        store
            .cancel_deferred("alice", cancelled.message_uuid, now())
            .await,
        DeferralOutcome::Applied
    );
    let released = store.release_due(release_at).await.released;
    assert_eq!(released.len(), 1, "only the surviving message releases");
    assert_eq!(
        push_row_count(&db).await,
        1,
        "the surviving message mints one claim; the cancelled one none"
    );
}

/// A release target carries a wake *threshold*, not a resolved decision, and the
/// store applies it per message: one release pass carries whatever urgencies were
/// parked. A claim minted eager for a below-threshold message wakes a phone for
/// traffic the threshold exists to keep quiet; one minted non-eager for a
/// qualifying message waits for the next poll instead of dispatching.
#[tokio::test]
async fn a_release_target_threshold_gates_the_minted_wake_per_message() {
    let (store, db) = durable_store_at_subscribed(
        Depth::Bounded(DEPTH),
        vec![wasm_target("proc", Some(crate::messaging::WakeMin::Normal))],
    )
    .await;
    let release_at = soon();
    for (body, urgency) in [("quiet", Urgency::Low), ("loud", Urgency::Normal)] {
        let mut msg = message("alice", body);
        msg.urgency = urgency;
        store.park(msg, release_at).await.expect("within cap");
    }

    store.release_due(release_at).await;

    let wakes: Vec<(String, bool)> = {
        let conn = db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT m.body, pp.eager_wake FROM messaging_pending_pushes pp \
                 JOIN messaging_messages m ON m.id = pp.message_id ORDER BY pp.id",
            )
            .expect("prepare");
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query")
            .map(|r| r.expect("read claim"))
            .collect()
    };
    assert_eq!(
        wakes,
        vec![("quiet".to_string(), false), ("loud".to_string(), true)],
        "the threshold decides per message, not per batch"
    );
}

/// Detaching drops the subscriber's in-memory push window along with its DB
/// rows. A window left behind holds push ids the detach already deleted, so the
/// next attach starts against a full window of phantoms and retires live claims.
#[tokio::test]
async fn detach_clears_the_durable_push_window() {
    let (store, db) = durable_store_for("proc", 2).await;
    let sub = wasm_sub("proc");

    // Two claims fill the depth-2 window, each offered to it by its own commit.
    for body in ["a", "b"] {
        let outcome = store.append(message("alice", body)).await;
        assert!(
            outcome.overflow.is_empty(),
            "a full-but-not-over window retires nothing"
        );
    }

    store.detach(&sub).await;
    assert!(
        store.push_windows_is_empty(),
        "detach must drop the subscriber's window, not only its DB rows"
    );

    // Re-attached, the window starts empty again: two fresh claims fit, so
    // nothing is retired and both push rows survive.
    for body in ["c", "d"] {
        let outcome = store.append(message("alice", body)).await;
        assert!(
            outcome.overflow.is_empty(),
            "a re-attached subscriber must not inherit the detached window"
        );
    }
    assert_eq!(
        push_row_count(&db).await,
        2,
        "both post-detach claims are live"
    );
}

/// Rescheduling moves the release, and the claim is minted at the moved-to
/// instant. A reschedule that left the delivery behind would release the
/// message with nothing to wake, so the rescheduled timer would never fire.
#[tokio::test]
async fn edit_reschedules_the_durable_release() {
    let (store, _db) =
        durable_store_at_subscribed(Depth::Bounded(DEPTH), vec![wasm_target("proc", None)]).await;
    let base = soon();
    let parked = store
        .park(message("alice", "a"), base + Duration::seconds(60))
        .await
        .expect("within cap");
    let moved_to = base + Duration::seconds(5);

    assert_eq!(
        store
            .edit_deferred("alice", parked.message_uuid, None, Some(moved_to), now())
            .await,
        DeferralOutcome::Applied
    );
    let released = store.release_due(moved_to).await.released;
    assert_eq!(released.len(), 1);
    assert_eq!(
        released[0].target_records.len(),
        1,
        "the claim must be minted with the message it delivers"
    );
}

// ── Resume replay ───────────────────────────────────────────────────────────

/// Append `bodies` in order, returning each committed message's dense retention
/// sequence so a case can build the store's resume cursor from it.
async fn append_all(store: &dyn RetentionStore, bodies: &[&str]) -> Vec<u64> {
    let mut seqs = Vec::new();
    for body in bodies {
        let committed = store.append(message_for(store, "s1", body)).await;
        seqs.push(committed.committed.seq.0);
    }
    seqs
}

fn replay_bodies(replay: &StoreReplay) -> Vec<String> {
    replay
        .messages
        .iter()
        .map(|e| e.message.body.clone())
        .collect()
}

/// A cursor factory bound to the store's own epoch.
async fn cursor_of(store: &dyn RetentionStore) -> impl Fn(u64) -> ResumeCursor {
    let epoch = store.replay_from(None, Depth::Unbounded).await.epoch;
    move |seq| ResumeCursor { epoch, seq }
}

/// The core resume decisions: fresh replays the whole window, a cursor at the
/// newest message is up to date, a trailing cursor is owed exactly the suffix
/// after it, and a cursor past the newest is a resume-ahead gap.
async fn replay_scenarios(store: Arc<dyn RetentionStore>) {
    let seqs = append_all(&*store, &["m1", "m2", "m3", "m4"]).await;
    let cursor = cursor_of(&*store).await;
    let addr = store.address().to_string();

    let fresh = store.replay_from(None, Depth::Unbounded).await;
    assert_eq!(fresh.decision, ReplayDecision::Fresh, "{addr}");
    assert_eq!(replay_bodies(&fresh), ["m1", "m2", "m3", "m4"], "{addr}");

    let up_to_date = store
        .replay_from(Some(cursor(seqs[3])), Depth::Unbounded)
        .await;
    assert_eq!(up_to_date.decision, ReplayDecision::UpToDate, "{addr}");
    assert!(up_to_date.messages.is_empty(), "{addr}");

    let exact = store
        .replay_from(Some(cursor(seqs[1])), Depth::Unbounded)
        .await;
    assert_eq!(exact.decision, ReplayDecision::Exact, "{addr}");
    assert_eq!(replay_bodies(&exact), ["m3", "m4"], "{addr}");

    let ahead = store
        .replay_from(Some(cursor(seqs[3] + 5)), Depth::Unbounded)
        .await;
    assert_eq!(
        ahead.decision,
        ReplayDecision::Gap(GapReason::ResumeAhead),
        "{addr}"
    );
    assert_eq!(replay_bodies(&ahead), ["m1", "m2", "m3", "m4"], "{addr}");
}

/// A cursor the retained window has dropped out from under (depth 2, four
/// messages) is a `BeyondRetained` gap: the whole surviving window plus the
/// typed signal.
async fn replay_gap_on_overflow(store: Arc<dyn RetentionStore>) {
    let seqs = append_all(&*store, &["m1", "m2", "m3", "m4"]).await;
    let cursor = cursor_of(&*store).await;
    let addr = store.address().to_string();

    let gap = store
        .replay_from(Some(cursor(seqs[0])), Depth::Unbounded)
        .await;
    assert_eq!(
        gap.decision,
        ReplayDecision::Gap(GapReason::BeyondRetained),
        "{addr}"
    );
    assert_eq!(replay_bodies(&gap), ["m3", "m4"], "{addr}");
}

/// A consumer whose window is narrower than the channel's gets the newest
/// `limit` messages, and an owed suffix that no longer fits comes back as the
/// loss it is rather than as a silently short `Exact`.
async fn replay_honors_a_narrower_consumer_window(store: Arc<dyn RetentionStore>) {
    let seqs = append_all(&*store, &["m1", "m2", "m3", "m4"]).await;
    let cursor = cursor_of(&*store).await;
    let addr = store.address().to_string();

    let fresh = store.replay_from(None, Depth::Bounded(2)).await;
    assert_eq!(fresh.decision, ReplayDecision::Fresh, "{addr}");
    assert_eq!(replay_bodies(&fresh), ["m3", "m4"], "{addr}");

    // Owed three, window carries two: the oldest is dropped, so the answer is a
    // gap rather than an exact suffix.
    let clamped = store
        .replay_from(Some(cursor(seqs[0])), Depth::Bounded(2))
        .await;
    assert_eq!(
        clamped.decision,
        ReplayDecision::Gap(GapReason::BeyondRetained),
        "{addr}"
    );
    assert_eq!(replay_bodies(&clamped), ["m3", "m4"], "{addr}");

    // Owed two, window carries two: nothing is lost, so the suffix stays exact.
    let fitting = store
        .replay_from(Some(cursor(seqs[1])), Depth::Bounded(2))
        .await;
    assert_eq!(fitting.decision, ReplayDecision::Exact, "{addr}");
    assert_eq!(replay_bodies(&fitting), ["m3", "m4"], "{addr}");

    // Nothing owed: a limit narrows a window, it does not manufacture one.
    let up_to_date = store
        .replay_from(Some(cursor(seqs[3])), Depth::Bounded(1))
        .await;
    assert_eq!(up_to_date.decision, ReplayDecision::UpToDate, "{addr}");
    assert!(up_to_date.messages.is_empty(), "{addr}");
}

#[tokio::test]
async fn ring_replay_from_honors_a_narrower_consumer_window() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(DEPTH));
    replay_honors_a_narrower_consumer_window(Arc::new(ring)).await;
}

#[tokio::test]
async fn durable_replay_from_honors_a_narrower_consumer_window() {
    let (db_store, _db) = durable_store(DEPTH).await;
    replay_honors_a_narrower_consumer_window(Arc::new(db_store)).await;
}

#[tokio::test]
async fn ring_replay_from_covers_fresh_exact_uptodate_and_resume_ahead() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(DEPTH));
    replay_scenarios(Arc::new(ring)).await;
}

#[tokio::test]
async fn durable_replay_from_covers_fresh_exact_uptodate_and_resume_ahead() {
    let (db_store, _db) = durable_store(DEPTH).await;
    replay_scenarios(Arc::new(db_store)).await;
}

#[tokio::test]
async fn ring_replay_from_reports_gap_when_the_window_dropped_the_cursor() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(2));
    replay_gap_on_overflow(Arc::new(ring)).await;
}

#[tokio::test]
async fn durable_replay_from_reports_gap_when_the_window_dropped_the_cursor() {
    let (db_store, _db) = durable_store(2).await;
    replay_gap_on_overflow(Arc::new(db_store)).await;
}

/// A parked message is in no store's resume window before release, and once
/// released it is owed to a cursor at the pre-park head as ordinary NEW — the
/// released row sorts newest on both stores (the ring by construction, the
/// durable store by assigning its seq at release). This is the escalated loss
/// case the durable seq model exists to close.
async fn replay_parked_then_released(store: Arc<dyn RetentionStore>) {
    let seqs = append_all(&*store, &["m1", "m2", "m3", "m4"]).await;
    let cursor = cursor_of(&*store).await;
    let addr = store.address().to_string();

    store
        .park(message_for(&*store, "s1", "parked"), soon())
        .await
        .expect("park within cap");

    // Parked message is not owed: a cursor at the newest retained is up to date.
    let before = store
        .replay_from(Some(cursor(seqs[3])), Depth::Unbounded)
        .await;
    assert_eq!(before.decision, ReplayDecision::UpToDate, "{addr}");
    assert!(before.messages.is_empty(), "{addr}");

    store.release_due(soon()).await;

    // After release the same cursor is owed exactly the released message.
    let after = store
        .replay_from(Some(cursor(seqs[3])), Depth::Unbounded)
        .await;
    assert_eq!(after.decision, ReplayDecision::Exact, "{addr}");
    assert_eq!(replay_bodies(&after), ["parked"], "{addr}");
}

#[tokio::test]
async fn ring_replay_from_ignores_a_parked_message_and_serves_it_after_release() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(DEPTH));
    replay_parked_then_released(Arc::new(ring)).await;
}

#[tokio::test]
async fn durable_replay_from_ignores_a_parked_message_and_serves_it_after_release() {
    let (db_store, _db) = durable_store(DEPTH).await;
    replay_parked_then_released(Arc::new(db_store)).await;
}

/// The escalated loss case in its sharpest form: park A, publish B, resume at
/// B's seq (up to date — A holds no retention position), release A, resume again
/// ⇒ owed exactly A. Both stores agree because A's retention sequence is
/// assigned at release, above every trailing cursor.
async fn replay_escalated_loss(store: Arc<dyn RetentionStore>) {
    let cursor = cursor_of(&*store).await;
    let addr = store.address().to_string();

    store
        .park(message_for(&*store, "s1", "A"), soon())
        .await
        .expect("park within cap");
    let seqs = append_all(&*store, &["B"]).await;

    let at_b = store
        .replay_from(Some(cursor(seqs[0])), Depth::Unbounded)
        .await;
    assert_eq!(at_b.decision, ReplayDecision::UpToDate, "{addr}");

    let released = store.release_due(soon()).await.released;
    assert_eq!(released.len(), 1, "{addr}");

    let owed = store
        .replay_from(Some(cursor(seqs[0])), Depth::Unbounded)
        .await;
    assert_eq!(owed.decision, ReplayDecision::Exact, "{addr}");
    assert_eq!(replay_bodies(&owed), ["A"], "{addr}");
    // The sequence the release reported is the one replay answers with, so a
    // consumer that minted its cursor from the live release lands on the same
    // position a resume would.
    assert_eq!(
        owed.messages[0].seq, released[0].seq.0,
        "release must report the retention sequence replay uses: {addr}"
    );
}

#[tokio::test]
async fn ring_replay_owes_a_late_released_message() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(DEPTH));
    replay_escalated_loss(Arc::new(ring)).await;
}

#[tokio::test]
async fn durable_replay_owes_a_late_released_message() {
    let (db_store, _db) = durable_store(DEPTH).await;
    replay_escalated_loss(Arc::new(db_store)).await;
}

/// After a late release, the retained tail *and* a `Fresh` replay order the
/// released message last on both stores — retention order, not publish order.
/// The two are served by different reads, so both are pinned: a consumer mints
/// its next cursor from the last message of the replay window, and a window in
/// publish order would hand it the wrong one.
async fn retained_tail_orders_late_release_last(store: Arc<dyn RetentionStore>) {
    let addr = store.address().to_string();
    store
        .park(message_for(&*store, "s1", "late"), soon())
        .await
        .expect("park within cap");
    append_all(&*store, &["a", "b"]).await;
    store.release_due(soon()).await;
    assert_eq!(retained_bodies(&*store).await, ["a", "b", "late"], "{addr}");

    let fresh = store.replay_from(None, Depth::Unbounded).await;
    assert_eq!(fresh.decision, ReplayDecision::Fresh, "{addr}");
    assert_eq!(replay_bodies(&fresh), ["a", "b", "late"], "{addr}");
    let highest = fresh
        .messages
        .iter()
        .map(|m| m.seq)
        .max()
        .expect("window is non-empty");
    assert_eq!(
        fresh.messages.last().expect("window is non-empty").seq,
        highest,
        "the released message must carry the window's highest seq: {addr}"
    );
}

#[tokio::test]
async fn ring_retained_tail_orders_late_release_last() {
    let ring = RingStore::new(Uuid::new_v4(), "ephemeral:parity", Depth::Bounded(DEPTH));
    retained_tail_orders_late_release_last(Arc::new(ring)).await;
}

#[tokio::test]
async fn durable_retained_tail_orders_late_release_last() {
    let (db_store, _db) = durable_store(DEPTH).await;
    retained_tail_orders_late_release_last(Arc::new(db_store)).await;
}

/// The durable Exact branch serves only rows in retention: a parked row whose
/// release time is already in the wall-clock past but which the release pass has
/// not swept (its `retained_seq` is still null) is excluded, matching the ring
/// that never holds a parked message.
#[tokio::test]
async fn durable_replay_exact_excludes_a_due_but_unreleased_parked_row() {
    let (store, _db) = durable_store(DEPTH).await;
    let seqs = append_all(&store, &["m1", "m2"]).await;
    let cursor = cursor_of(&store).await;

    // Release time in the wall-clock past, but never released: the strict
    // seq-keyed loader must not leak it.
    let past = DateTime::from_timestamp(1_000_000_000, 0).expect("representable");
    store
        .park(message("s1", "due-unreleased"), past)
        .await
        .expect("park within cap");

    let exact = store
        .replay_from(Some(cursor(seqs[0])), Depth::Unbounded)
        .await;
    assert_eq!(exact.decision, ReplayDecision::Exact);
    assert_eq!(replay_bodies(&exact), ["m2"]);
}

/// A foreign epoch is answered `EpochChanged` with the full window on the durable
/// store, the same way the ring answers a restart — the persisted per-channel
/// epoch makes the condition reachable on both.
#[tokio::test]
async fn durable_replay_reports_epoch_changed_for_a_foreign_epoch() {
    let (store, _db) = durable_store(DEPTH).await;
    append_all(&store, &["m1", "m2"]).await;

    let foreign = ResumeCursor {
        epoch: Uuid::new_v4(),
        seq: 1,
    };
    let replay = store.replay_from(Some(foreign), Depth::Unbounded).await;
    assert_eq!(
        replay.decision,
        ReplayDecision::Gap(GapReason::EpochChanged)
    );
    assert_eq!(replay_bodies(&replay), ["m1", "m2"]);
}

/// An all-parked channel has an empty retained window and a persisted high-water
/// of 0: a cursor at 0 is up to date (nothing newer was ever assigned), one
/// above it is a resume-ahead gap. (The fully-evicted case — a cursor *below* a
/// positive high-water over an emptied window — is covered by
/// `durable_empty_window_after_eviction_discriminates_by_high_water`, which uses
/// the seq-keyed reaper to empty a window without dropping the high-water.)
#[tokio::test]
async fn durable_empty_window_is_decided_by_the_persisted_high_water() {
    let (store, _db) = durable_store(DEPTH).await;
    let cursor = cursor_of(&store).await;
    store
        .park(message("s1", "parked"), soon())
        .await
        .expect("park within cap");

    let up_to_date = store.replay_from(Some(cursor(0)), Depth::Unbounded).await;
    assert_eq!(up_to_date.decision, ReplayDecision::UpToDate);
    assert!(up_to_date.messages.is_empty());

    let ahead = store.replay_from(Some(cursor(5)), Depth::Unbounded).await;
    assert_eq!(ahead.decision, ReplayDecision::Gap(GapReason::ResumeAhead));
}

/// After the seq-keyed reaper empties a fully-retained window (frontier 0), the
/// channel row keeps its persisted high-water, so resume against the empty window
/// is still fully discriminable: a cursor at the high-water is up to date, one
/// below it is `BeyondRetained` (everything it was owed was evicted), one above
/// it is `ResumeAhead`. This is the fully-evicted case the all-parked test cannot
/// reach without GC.
#[tokio::test]
async fn durable_empty_window_after_eviction_discriminates_by_high_water() {
    let db = init_db_memory();
    let entry = test_channel_entry("evicted", vec![]);
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let store = DbStore::new(
        db.clone(),
        entry.uuid,
        entry.address.clone(),
        Depth::Bounded(DEPTH),
        Arc::new(TargetResolver::unsubscribed()),
    );
    let cursor = cursor_of(&store).await;

    // seqs 1..4, high-water 4.
    append_all(&store, &["m1", "m2", "m3", "m4"]).await;

    // Evict the whole retained window. The channel row's last_retained_seq stays
    // at 4 — GC removes rows, never the high-water.
    {
        let conn = db.lock().await;
        let eviction = bus_gc_evict_channel(
            &conn,
            entry.uuid,
            &entry.address,
            ChannelScheme::Brenn,
            0,
            Sink::Drop,
            None,
        );
        assert_eq!(
            eviction.messages_evicted, 4,
            "all four retained rows evicted"
        );
    }

    let at_hw = store.replay_from(Some(cursor(4)), Depth::Unbounded).await;
    assert_eq!(at_hw.decision, ReplayDecision::UpToDate);
    assert!(at_hw.messages.is_empty());

    let below = store.replay_from(Some(cursor(2)), Depth::Unbounded).await;
    assert_eq!(
        below.decision,
        ReplayDecision::Gap(GapReason::BeyondRetained)
    );

    let above = store.replay_from(Some(cursor(9)), Depth::Unbounded).await;
    assert_eq!(above.decision, ReplayDecision::Gap(GapReason::ResumeAhead));
}

/// Two durable channels sharing one DB interleave rowids (and a parked row
/// consumes one between them); a trailing cursor with every owed row still
/// retained resolves `Exact`, never a spurious `BeyondRetained` — the dense
/// per-channel seq is immune to the sparse shared rowid space.
#[tokio::test]
async fn durable_replay_is_immune_to_sparse_rowids() {
    let db = init_db_memory();
    let e1 = test_channel_entry("parity1", vec![]);
    let e2 = test_channel_entry("parity2", vec![]);
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &[e1.clone(), e2.clone()]);
    }
    let s1 = DbStore::new(
        db.clone(),
        e1.uuid,
        e1.address.clone(),
        Depth::Bounded(DEPTH),
        Arc::new(TargetResolver::unsubscribed()),
    );
    let s2 = DbStore::new(
        db.clone(),
        e2.uuid,
        e2.address.clone(),
        Depth::Bounded(DEPTH),
        Arc::new(TargetResolver::unsubscribed()),
    );
    let cursor = cursor_of(&s1).await;

    s1.append(message("s", "a1")).await; // ch1 seq 1
    s2.append(message("s", "x")).await;
    s1.park(message("s", "p"), soon()).await.expect("park"); // rowid, no seq
    s1.append(message("s", "a2")).await; // ch1 seq 2
    s2.append(message("s", "y")).await;
    s1.append(message("s", "a3")).await; // ch1 seq 3

    let replay = s1.replay_from(Some(cursor(1)), Depth::Unbounded).await;
    assert_eq!(replay.decision, ReplayDecision::Exact);
    assert_eq!(replay_bodies(&replay), ["a2", "a3"]);
}

/// An `Unbounded` durable retain depth is a legitimate config and takes its own
/// query shapes (no row cap on the window reads). The resume decisions must be
/// the same ones a bounded channel gives.
#[tokio::test]
async fn durable_replay_from_is_exact_on_an_unbounded_retain_depth() {
    let (store, _db) = durable_store_at(Depth::Unbounded).await;
    let seqs = append_all(&store, &["m1", "m2", "m3", "m4"]).await;
    let cursor = cursor_of(&store).await;

    let fresh = store.replay_from(None, Depth::Unbounded).await;
    assert_eq!(fresh.decision, ReplayDecision::Fresh);
    assert_eq!(replay_bodies(&fresh), ["m1", "m2", "m3", "m4"]);

    // The window edge: a cursor at the oldest retained message is owed the rest.
    let from_edge = store
        .replay_from(Some(cursor(seqs[0])), Depth::Unbounded)
        .await;
    assert_eq!(from_edge.decision, ReplayDecision::Exact);
    assert_eq!(replay_bodies(&from_edge), ["m2", "m3", "m4"]);

    let up_to_date = store
        .replay_from(Some(cursor(seqs[3])), Depth::Unbounded)
        .await;
    assert_eq!(up_to_date.decision, ReplayDecision::UpToDate);
}

/// Eviction keeps any message a tentative (`confirm_pending`) push protects, so
/// the surviving retained set is not always a contiguous suffix: an old
/// protected row can outlive the band above it. A cursor sitting at that row is
/// owed the evicted band, and answering `Exact` would report perfect continuity
/// while silently dropping it.
#[tokio::test]
async fn durable_replay_reports_a_gap_when_eviction_left_a_hole() {
    let (store, db) = durable_store_for("proc", DEPTH).await;
    let cursor = cursor_of(&store).await;

    let protected = store
        .append(message("s1", "m1"))
        .await
        .committed
        .target_records[0]
        .0;
    let seqs = append_all(&store, &["m2", "m3", "m4"]).await;

    {
        let conn = db.lock().await;
        // The claim on seq 1 goes tentative, which pins its message row through
        // eviction; the frontier of 1 makes everything but seq 4 eligible.
        conn.execute(
            "UPDATE messaging_pending_pushes SET confirm_pending = 1 WHERE id = ?1",
            rusqlite::params![protected],
        )
        .expect("stamp the claim tentative");
        let eviction = bus_gc_evict_channel(
            &conn,
            store.channel_uuid(),
            store.address(),
            ChannelScheme::Brenn,
            1,
            Sink::Drop,
            None,
        );
        assert_eq!(
            eviction.messages_evicted, 2,
            "seqs 2 and 3 are evicted; 1 is protected, 4 kept"
        );
    }

    // Cursor at the protected row: seqs 2 and 3 are gone, so it is not an exact
    // suffix even though the surviving window reaches back past the cursor.
    let holed = store.replay_from(Some(cursor(1)), Depth::Unbounded).await;
    assert_eq!(
        holed.decision,
        ReplayDecision::Gap(GapReason::BeyondRetained),
        "an evicted band behind a protected row is a gap, not an exact suffix"
    );

    // A cursor above the hole is still an exact suffix.
    let clean = store
        .replay_from(Some(cursor(seqs[1])), Depth::Unbounded)
        .await;
    assert_eq!(clean.decision, ReplayDecision::Exact);
    assert_eq!(replay_bodies(&clean), ["m4"]);
}

// ── Drop accounting ───────────────────────────────────────────────────────

/// A durable push-window overflow reports the loss at the moment the claim is
/// retired — the commit that displaced it — with no reference to the
/// subscription's noise level, the same unconditional accounting the ring
/// cursor does. The noise ladder decides how loud the drop is, never whether it
/// happened. The commit names the loser in its overflow, so a subscriber that
/// never reads still escalates.
#[tokio::test]
async fn durable_overflow_reports_the_drop_at_retirement() {
    let (store, _db) = durable_store_for("slow", 1).await;
    let sub = wasm_sub("slow");

    let mut retired = Vec::new();
    for body in ["a", "b", "c"] {
        let outcome = store.append(message("alice", body)).await;
        retired.extend(outcome.overflow);
    }

    assert_eq!(
        retired.len(),
        2,
        "a depth-1 window retires the two older claims"
    );
    assert!(
        retired
            .iter()
            .all(|e| e.subscriber == sub && e.dropped == 1),
        "each commit names the subscriber whose claim it displaced"
    );
    assert!(
        retired
            .iter()
            .all(|e| e.app_slug.as_deref() == Some("slow")),
        "the event names the app the retired claim was written under — the only \
         route from a conversation participant back to its registration"
    );
}

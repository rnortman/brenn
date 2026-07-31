//! The confined router: identity minting, the plane-policy seam, and the single
//! append that is the delivery.
//!
//! Nothing here names a component, instance, port or pixel. The planes are the
//! test's own, the reader key is a string, and the policy is injected — which is
//! the whole of what this layer knows about the channels it routes.

use super::*;

use crate::store::ChannelStores;
use crate::subs::SubscriptionDepths;

const PLANE: &str = "local:demo/plane";
const OTHER: &str = "local:demo/other";
const PRINCIPAL: &str = "attacher:demo";

/// A policy that answers from data the test sets, and records what it saw.
#[derive(Default)]
struct TestPlanes {
    /// Channels no sub-identity may write.
    attacher_only: Vec<String>,
    /// Refuse every body, with this reason.
    refuse: Option<String>,
    /// Rewrite every body to name its origin — the stamping a plane whose
    /// consumer trusts an identity in the payload performs.
    stamp_origin: bool,
    observed: Vec<MessageEnvelope>,
}

impl PlanePolicy for TestPlanes {
    fn admits(&self, channel: &str, origin: Origin<'_>) -> bool {
        !(matches!(origin, Origin::Sub(_)) && self.attacher_only.iter().any(|c| c == channel))
    }

    fn guard(&self, _channel: &str, origin: Origin<'_>, body: String) -> GuardedBody {
        if let Some(reason) = &self.refuse {
            return GuardedBody::Refused(reason.clone());
        }
        if self.stamp_origin {
            let who = match origin {
                Origin::Attacher => "attacher".to_string(),
                Origin::Sub(sub) => sub.to_string(),
            };
            return GuardedBody::Carry(format!("{body}|{who}"));
        }
        GuardedBody::Carry(body)
    }

    fn observe(&mut self, envelope: &MessageEnvelope) {
        self.observed.push(envelope.clone());
    }
}

/// A policy that states only the one rule the trait has no default for, so the
/// defaults themselves are what the routing runs on.
struct DefaultPlanes;

impl PlanePolicy for DefaultPlanes {
    fn admits(&self, _channel: &str, _origin: Origin<'_>) -> bool {
        true
    }
}

fn stamp(n: u128) -> MessageStamp {
    MessageStamp {
        message_id: Uuid::from_u128(n),
        publish_ts: DateTime::from_timestamp_millis(1_700_000_000_000).expect("a representable ts"),
    }
}

fn req<'a>(channel: &'a str, origin: Origin<'a>, body: &str, n: u128) -> RouteRequest<'a> {
    RouteRequest {
        channel,
        origin,
        body: body.to_string(),
        stamp: stamp(n),
        urgency: Urgency::Normal,
        deliver_after: None,
    }
}

/// The same publish, scheduled: `at` is a wall-clock instant in the currency
/// every release time is stated in.
fn scheduled<'a>(
    channel: &'a str,
    origin: Origin<'a>,
    body: &str,
    n: u128,
    at: ReleaseTime,
) -> RouteRequest<'a> {
    RouteRequest {
        deliver_after: Some(at),
        ..req(channel, origin, body, n)
    }
}

fn stores(depth: u64) -> ChannelStores<String> {
    let mut stores = ChannelStores::new(Uuid::from_u128(0xE90C));
    stores.ensure(PLANE, depth);
    stores.ensure(OTHER, depth);
    stores
}

fn router(policy: TestPlanes) -> LocalRouter<TestPlanes> {
    let mut router = LocalRouter::new(policy);
    router.set_principal(PRINCIPAL.to_string());
    router
}

fn retained(stores: &ChannelStores<String>, channel: &str) -> Vec<MessageEnvelope> {
    stores
        .get(channel)
        .expect("the fixture hosts this channel")
        .retained()
        .map(|(envelope, _)| envelope.clone())
        .collect()
}

fn routed<K>(outcome: RouteOutcome<K>) -> Vec<CursorOverflow<K>> {
    match outcome {
        RouteOutcome::Routed { overflow } => overflow,
        RouteOutcome::Parked { release_at } => panic!("parked until {release_at}"),
        RouteOutcome::ScheduleDropped { cap } => panic!("schedule dropped at cap {cap}"),
        RouteOutcome::Refused { reason } => panic!("refused: {reason}"),
        RouteOutcome::NoIdentity => panic!("no identity"),
    }
}

#[test]
fn the_attacher_publishes_under_its_bare_principal() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    routed(router.route(&mut stores, req(PLANE, Origin::Attacher, "state", 1)));
    let held = retained(&stores, PLANE);
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].sender, PRINCIPAL);
    assert_eq!(held[0].body, "state");
    assert_eq!(held[0].channel, PLANE);
}

#[test]
fn a_sub_identity_publishes_under_the_composed_sender() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    routed(router.route(&mut stores, req(PLANE, Origin::Sub("widget-7"), "state", 1)));
    assert_eq!(retained(&stores, PLANE)[0].sender, "attacher:demo#widget-7");
}

/// The fields a publisher could otherwise forge, and the one it states. `source`
/// is the attacher (no peer produced this), the schedule and the gesture are
/// absent, and the class says confined.
#[test]
fn the_minted_envelope_states_the_attacher_and_asserts_nothing_else() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    let mut request = req(PLANE, Origin::Sub("widget-7"), "state", 42);
    request.urgency = Urgency::Low;
    routed(router.route(&mut stores, request));
    let envelope = retained(&stores, PLANE).remove(0);
    assert_eq!(envelope.source, PRINCIPAL);
    assert_eq!(envelope.message_id, Uuid::from_u128(42));
    assert_eq!(envelope.publish_ts, stamp(42).publish_ts);
    assert_eq!(envelope.urgency, Urgency::Low);
    assert_eq!(envelope.envelope_type, ChannelScheme::Local);
    assert_eq!(envelope.reply_to, None);
    assert_eq!(envelope.delivery_deadline, None);
    assert_eq!(envelope.deliver_after, None);
    assert_eq!(envelope.impetus, None);
}

#[test]
fn a_guards_rewrite_is_what_lands_on_the_channel() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes {
        stamp_origin: true,
        ..TestPlanes::default()
    });
    routed(router.route(&mut stores, req(PLANE, Origin::Sub("widget-7"), "claim", 1)));
    assert_eq!(retained(&stores, PLANE)[0].body, "claim|widget-7");
    // And the observer saw the message as its readers will, not as its publisher
    // wrote it.
    assert_eq!(router.policy().observed[0].body, "claim|widget-7");
}

#[test]
fn a_refused_body_reaches_no_reader_and_carries_the_guards_reason() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes {
        refuse: Some("only chrome may say this".to_string()),
        ..TestPlanes::default()
    });
    let outcome = router.route(&mut stores, req(PLANE, Origin::Sub("widget-7"), "lie", 1));
    let RouteOutcome::Refused { reason } = outcome else {
        panic!("a guarded plane refuses")
    };
    assert_eq!(reason, "only chrome may say this");
    assert!(retained(&stores, PLANE).is_empty());
    assert!(router.policy().observed.is_empty());
}

#[test]
fn the_default_policy_carries_every_body_through() {
    let mut stores = stores(4);
    let mut router = LocalRouter::new(DefaultPlanes);
    router.set_principal(PRINCIPAL.to_string());
    routed(router.route(&mut stores, req(PLANE, Origin::Attacher, "verbatim", 1)));
    assert_eq!(retained(&stores, PLANE)[0].body, "verbatim");
}

/// The fan-out: one append, and every reader on the channel is owed it. There is
/// no subscription to scope a confined publish to.
#[test]
fn one_append_wakes_every_reader_on_the_channel() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    let store = stores.get_mut(PLANE).expect("hosted");
    store.attach("reader-a".to_string(), 2);
    store.attach("reader-b".to_string(), 2);
    stores
        .get_mut(OTHER)
        .expect("hosted")
        .attach("reader-c".to_string(), 2);
    routed(router.route(&mut stores, req(PLANE, Origin::Attacher, "wake", 1)));
    let store = stores.get(PLANE).expect("hosted");
    assert!(store.has_deliverable(&"reader-a".to_string()));
    assert!(store.has_deliverable(&"reader-b".to_string()));
    assert!(
        !stores
            .get(OTHER)
            .expect("hosted")
            .has_deliverable(&"reader-c".to_string()),
        "a publish on one confined channel owes another's readers nothing"
    );
}

#[test]
fn an_append_hands_back_what_it_pushed_retention_past() {
    let mut stores = stores(1);
    let mut router = router(TestPlanes::default());
    stores
        .get_mut(PLANE)
        .expect("hosted")
        .attach("reader-a".to_string(), 1);
    let none = routed(router.route(&mut stores, req(PLANE, Origin::Attacher, "first", 1)));
    assert!(none.is_empty(), "nothing was outrun yet");
    let overflow = routed(router.route(&mut stores, req(PLANE, Origin::Attacher, "second", 2)));
    assert_eq!(overflow.len(), 1);
    assert_eq!(overflow[0].subscriber, "reader-a");
    assert_eq!(overflow[0].evicted, 1);
}

/// A reader that keeps up sees both, in the order they were routed — the store's
/// dense per-channel seq, not the wall clock either of them carries.
#[test]
fn readers_are_served_confined_messages_in_route_order() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    stores
        .get_mut(PLANE)
        .expect("hosted")
        .attach("reader-a".to_string(), 2);
    for (n, body) in [(1u128, "first"), (2, "second")] {
        routed(router.route(&mut stores, req(PLANE, Origin::Attacher, body, n)));
    }
    let window = stores
        .get_mut(PLANE)
        .expect("hosted")
        .serve(
            &"reader-a".to_string(),
            SubscriptionDepths {
                push_depth: 2,
                retain_depth: 2,
            },
        )
        .expect("a reader holding a position is served");
    let bodies: Vec<&str> = window.envelopes.iter().map(|e| e.body.as_str()).collect();
    assert_eq!(bodies, ["first", "second"]);
    assert_eq!(window.new_from, 0);
}

#[test]
fn nothing_routes_before_the_attachment_has_an_identity() {
    let mut stores = stores(4);
    let mut router = LocalRouter::new(TestPlanes::default());
    let outcome = router.route(&mut stores, req(PLANE, Origin::Attacher, "early", 1));
    assert!(matches!(outcome, RouteOutcome::NoIdentity));
    assert!(retained(&stores, PLANE).is_empty());
    assert_eq!(router.principal(), None);
    router.set_principal(PRINCIPAL.to_string());
    routed(router.route(&mut stores, req(PLANE, Origin::Attacher, "early", 1)));
    assert_eq!(retained(&stores, PLANE).len(), 1);
}

#[test]
#[should_panic(expected = "not a confined channel")]
fn a_transportable_channel_is_not_the_routers_to_append_to() {
    let mut stores = ChannelStores::<String>::new(Uuid::from_u128(0xE90C));
    stores.ensure("ephemeral:demo", 4);
    let mut router = router(TestPlanes::default());
    router.route(
        &mut stores,
        req("ephemeral:demo", Origin::Attacher, "wire", 1),
    );
}

#[test]
#[should_panic(expected = "does not publish on")]
fn an_origin_the_plane_does_not_admit_is_an_embedder_bug() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes {
        attacher_only: vec![PLANE.to_string()],
        ..TestPlanes::default()
    });
    router.route(&mut stores, req(PLANE, Origin::Sub("widget-7"), "state", 1));
}

#[test]
#[should_panic(expected = "no store hosts")]
fn a_confined_channel_with_no_store_is_an_embedder_bug() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    router.route(
        &mut stores,
        req("local:demo/unhosted", Origin::Attacher, "state", 1),
    );
}

#[test]
#[should_panic(expected = "empty principal")]
fn an_empty_principal_is_no_identity_at_all() {
    LocalRouter::new(TestPlanes::default()).set_principal(String::new());
}

// ── Deferral ──────────────────────────────────────────────────────────────

/// The wall-clock instant every fixture stamp carries, in the currency a release
/// time is stated in.
const NOW: ReleaseTime = 1_700_000_000_000;
const SOON: ReleaseTime = NOW + 60_000;
const LATER: ReleaseTime = NOW + 120_000;

fn parked_until<K>(outcome: RouteOutcome<K>) -> ReleaseTime {
    match outcome {
        RouteOutcome::Parked { release_at } => release_at,
        RouteOutcome::Routed { .. } => panic!("routed immediately"),
        RouteOutcome::ScheduleDropped { cap } => panic!("schedule dropped at cap {cap}"),
        RouteOutcome::Refused { reason } => panic!("refused: {reason}"),
        RouteOutcome::NoIdentity => panic!("no identity"),
    }
}

fn schedule_of(
    router: &LocalRouter<TestPlanes>,
    stores: &ChannelStores<String>,
    origin: Origin<'_>,
) -> Vec<(String, ReleaseTime)> {
    router
        .parked_for(stores, PLANE, origin, 0)
        .into_iter()
        .map(|entry| (entry.body, entry.deliver_after))
        .collect()
}

/// A parked message is not on the channel: nothing retained, nobody woken, and
/// the plane has observed nothing, because no reader could have read it.
#[test]
fn a_schedule_ahead_of_the_mint_parks_instead_of_reaching_anyone() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    stores
        .get_mut(PLANE)
        .expect("hosted")
        .attach("reader-a".to_string(), 2);
    let at = parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Sub("widget-7"), "later", 1, SOON),
    ));
    assert_eq!(at, SOON);
    assert!(retained(&stores, PLANE).is_empty());
    assert!(
        !stores
            .get(PLANE)
            .expect("hosted")
            .has_deliverable(&"reader-a".to_string())
    );
    assert!(router.policy().observed.is_empty());
    assert_eq!(stores.next_release(), Some(SOON));
}

/// The contract every host in this system gives a release time in the past —
/// and the boundary is the mint itself, not a moment after it.
#[test]
fn a_release_time_at_or_behind_the_mint_publishes_immediately() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    routed(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "at-the-mint", 1, NOW),
    ));
    routed(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "behind-it", 2, NOW - 1),
    ));
    assert_eq!(retained(&stores, PLANE).len(), 2);
    assert_eq!(stores.next_release(), None);
}

/// Both grains park; the sender is the router's, as it is for an immediate
/// publish, and each origin sees only its own schedule.
#[test]
fn each_origin_parks_under_its_own_identity_and_sees_only_that() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "mine", 1, LATER),
    ));
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Sub("widget-7"), "theirs", 2, SOON),
    ));
    assert_eq!(
        schedule_of(&router, &stores, Origin::Attacher),
        [("mine".to_string(), LATER)]
    );
    assert_eq!(
        schedule_of(&router, &stores, Origin::Sub("widget-7")),
        [("theirs".to_string(), SOON)]
    );
    assert_eq!(
        stores
            .get(PLANE)
            .expect("hosted")
            .parked()
            .map(|(e, _)| e.sender.clone())
            .collect::<Vec<_>>(),
        ["attacher:demo#widget-7", PRINCIPAL],
        "release order, each under the sender the router derived"
    );
}

/// The entry the caller acts on later: the identity it names a message by, the
/// body it wrote, and the time it set.
#[test]
fn a_schedule_reads_back_in_the_shape_the_peer_answers_for_a_wire_channel() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Sub("widget-7"), "body", 42, SOON),
    ));
    let entries = router.parked_for(&stores, PLANE, Origin::Sub("widget-7"), 0);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].message_id, Uuid::from_u128(42));
    assert_eq!(entries[0].body, "body");
    assert_eq!(entries[0].deliver_after, SOON);
    assert!(
        router
            .parked_for(&stores, PLANE, Origin::Sub("widget-7"), SOON)
            .is_empty(),
        "an entry whose time has come is out of the view before the sweep takes it"
    );
}

#[test]
fn a_full_deferred_set_drops_the_schedule_and_names_the_cap() {
    let mut stores = stores(1);
    let mut router = router(TestPlanes::default());
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "first", 1, SOON),
    ));
    let outcome = router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "second", 2, SOON),
    );
    assert!(matches!(outcome, RouteOutcome::ScheduleDropped { cap: 1 }));
    assert_eq!(
        schedule_of(&router, &stores, Origin::Attacher),
        [("first".to_string(), SOON)],
        "the work already scheduled is what a full set protects"
    );
}

/// A release is an arrival: every reader on the channel is owed it, and it is
/// observed here — the first moment one could see it.
#[test]
fn a_release_reaches_every_reader_and_is_observed_at_that_moment() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    stores
        .get_mut(PLANE)
        .expect("hosted")
        .attach("reader-a".to_string(), 2);
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "due", 1, SOON),
    ));
    assert!(router.release_due(&mut stores, SOON - 1).is_empty());
    let swept = router.release_due(&mut stores, SOON);
    assert_eq!(swept.len(), 1);
    assert_eq!(swept[0].channel, PLANE);
    assert_eq!(swept[0].released.len(), 1);
    assert_eq!(swept[0].released[0].body, "due");
    assert_eq!(swept[0].released[0].sender, PRINCIPAL);
    assert!(swept[0].overflow.is_empty());
    assert_eq!(retained(&stores, PLANE)[0].body, "due");
    assert!(
        stores
            .get(PLANE)
            .expect("hosted")
            .has_deliverable(&"reader-a".to_string())
    );
    assert_eq!(router.policy().observed.len(), 1);
    assert_eq!(router.policy().observed[0].body, "due");
}

/// Only the due channels, in address order, and a release charges retention
/// exactly as an immediate publish does.
#[test]
fn the_sweep_names_the_due_channels_and_hands_back_what_they_retired() {
    let mut stores = stores(1);
    let mut router = router(TestPlanes::default());
    stores
        .get_mut(OTHER)
        .expect("hosted")
        .attach("reader-c".to_string(), 1);
    routed(router.route(&mut stores, req(OTHER, Origin::Attacher, "already", 1)));
    parked_until(router.route(
        &mut stores,
        scheduled(OTHER, Origin::Attacher, "due", 2, SOON),
    ));
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "not-yet", 3, LATER),
    ));
    let swept = router.release_due(&mut stores, SOON);
    assert_eq!(
        swept.iter().map(|s| s.channel.as_str()).collect::<Vec<_>>(),
        [OTHER],
        "a channel with nothing due is absent from the answer"
    );
    assert_eq!(swept[0].overflow.len(), 1);
    assert_eq!(swept[0].overflow[0].subscriber, "reader-c");
    assert_eq!(swept[0].overflow[0].evicted, 1);
}

/// The timer speaks on the transitions only: armed when the deadline moves,
/// silent when it does not, disarmed when nothing is parked anywhere.
#[test]
fn the_release_timer_is_stated_only_when_the_deadline_moves() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    assert_eq!(
        router.release_wakeup(&stores),
        None,
        "nothing parked, nothing armed"
    );
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "late", 1, LATER),
    ));
    assert_eq!(
        router.release_wakeup(&stores),
        Some(ReleaseTimer::Arm(LATER))
    );
    assert_eq!(router.release_wakeup(&stores), None);
    parked_until(router.route(
        &mut stores,
        scheduled(OTHER, Origin::Attacher, "sooner", 2, SOON),
    ));
    assert_eq!(
        router.release_wakeup(&stores),
        Some(ReleaseTimer::Arm(SOON)),
        "a sooner park on another channel moves the one deadline"
    );
    router.release_due(&mut stores, SOON);
    assert_eq!(
        router.release_wakeup(&stores),
        Some(ReleaseTimer::Arm(LATER))
    );
    router.release_due(&mut stores, LATER);
    assert_eq!(router.release_wakeup(&stores), Some(ReleaseTimer::Disarm));
    assert_eq!(router.release_wakeup(&stores), None);
}

#[test]
fn a_cancel_takes_the_message_off_the_schedule_for_good() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Sub("widget-7"), "regret", 7, SOON),
    ));
    let answer = router.apply_op(
        &mut stores,
        DeferOpRequest {
            channel: PLANE,
            origin: Origin::Sub("widget-7"),
            message_id: Uuid::from_u128(7),
            op: DeferOp::Cancel,
            now: NOW,
        },
    );
    assert_eq!(answer, DeferOpAnswer::Applied);
    assert!(schedule_of(&router, &stores, Origin::Sub("widget-7")).is_empty());
    assert!(router.release_due(&mut stores, LATER).is_empty());
    assert!(retained(&stores, PLANE).is_empty());
}

/// An edit is a second way to state a body on a plane, so it runs the same
/// guard — and what the guard made of it is what releases.
#[test]
fn an_edit_runs_the_planes_guard_and_its_rewrite_is_what_releases() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes {
        stamp_origin: true,
        ..TestPlanes::default()
    });
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Sub("widget-7"), "draft", 7, LATER),
    ));
    let answer = router.apply_op(
        &mut stores,
        DeferOpRequest {
            channel: PLANE,
            origin: Origin::Sub("widget-7"),
            message_id: Uuid::from_u128(7),
            op: DeferOp::Edit {
                body: Some("final".to_string()),
                deliver_after: Some(SOON),
            },
            now: NOW,
        },
    );
    assert_eq!(answer, DeferOpAnswer::Applied);
    assert_eq!(
        schedule_of(&router, &stores, Origin::Sub("widget-7")),
        [("final|widget-7".to_string(), SOON)],
        "the guard's rewrite, at the moved deadline"
    );
    let swept = router.release_due(&mut stores, SOON);
    assert_eq!(swept[0].released[0].body, "final|widget-7");
}

#[test]
fn a_refused_edit_changes_neither_body_nor_schedule() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Sub("widget-7"), "draft", 7, LATER),
    ));
    router.policy_mut().refuse = Some("only chrome may say this".to_string());
    let answer = router.apply_op(
        &mut stores,
        DeferOpRequest {
            channel: PLANE,
            origin: Origin::Sub("widget-7"),
            message_id: Uuid::from_u128(7),
            op: DeferOp::Edit {
                body: Some("lie".to_string()),
                deliver_after: Some(SOON),
            },
            now: NOW,
        },
    );
    assert_eq!(
        answer,
        DeferOpAnswer::Refused {
            reason: "only chrome may say this".to_string()
        }
    );
    assert_eq!(
        schedule_of(&router, &stores, Origin::Sub("widget-7")),
        [("draft".to_string(), LATER)]
    );
}

/// The race any publisher can lose: the message released between the schedule it
/// read and the op it sent.
#[test]
fn an_op_naming_a_released_message_is_the_benign_race() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "gone", 7, SOON),
    ));
    router.release_due(&mut stores, SOON);
    let answer = router.apply_op(
        &mut stores,
        DeferOpRequest {
            channel: PLANE,
            origin: Origin::Attacher,
            message_id: Uuid::from_u128(7),
            op: DeferOp::Cancel,
            now: SOON,
        },
    );
    assert_eq!(answer, DeferOpAnswer::NotParked);
}

/// The same race one turn earlier: the release time has arrived but the embedder
/// has not swept yet. The message is still physically parked, and it is still
/// beyond an op's reach — the schedule stopped showing it at its release time,
/// and the peer answers the same for a channel that crosses the wire.
#[test]
fn an_op_on_a_due_but_unswept_message_is_the_same_benign_race() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Attacher, "due", 7, SOON),
    ));
    assert!(
        router
            .parked_for(&stores, PLANE, Origin::Attacher, SOON)
            .is_empty(),
        "at its release time the schedule no longer shows it"
    );
    let answer = router.apply_op(
        &mut stores,
        DeferOpRequest {
            channel: PLANE,
            origin: Origin::Attacher,
            message_id: Uuid::from_u128(7),
            op: DeferOp::Cancel,
            now: SOON,
        },
    );
    assert_eq!(answer, DeferOpAnswer::NotParked);
    let swept = router.release_due(&mut stores, SOON);
    assert_eq!(
        swept[0].released[0].body, "due",
        "the sweep still owes it, cancel or no cancel"
    );
}

/// Before the first `Welcome` there is no identity to have parked under, so
/// there is nothing to read and nothing to act on — and neither answer is a
/// panic, since an embedder may legitimately try early.
#[test]
fn nothing_is_scheduled_before_the_attachment_has_an_identity() {
    let mut stores = stores(4);
    let mut router = LocalRouter::new(TestPlanes::default());
    assert!(
        router
            .parked_for(&stores, PLANE, Origin::Attacher, 0)
            .is_empty()
    );
    let answer = router.apply_op(
        &mut stores,
        DeferOpRequest {
            channel: PLANE,
            origin: Origin::Attacher,
            message_id: Uuid::from_u128(7),
            op: DeferOp::Cancel,
            now: NOW,
        },
    );
    assert_eq!(answer, DeferOpAnswer::NotParked);
}

#[test]
#[should_panic(expected = "does not own")]
fn an_op_against_another_senders_schedule_is_an_embedder_bug() {
    let mut stores = stores(4);
    let mut router = router(TestPlanes::default());
    parked_until(router.route(
        &mut stores,
        scheduled(PLANE, Origin::Sub("widget-7"), "theirs", 7, SOON),
    ));
    router.apply_op(
        &mut stores,
        DeferOpRequest {
            channel: PLANE,
            origin: Origin::Attacher,
            message_id: Uuid::from_u128(7),
            op: DeferOp::Cancel,
            now: NOW,
        },
    );
}

#[test]
#[should_panic(expected = "not a confined channel")]
fn a_transportable_channels_schedule_is_not_the_routers_to_answer() {
    let mut stores = ChannelStores::<String>::new(Uuid::from_u128(0xE90C));
    stores.ensure("ephemeral:demo", 4);
    let router = router(TestPlanes::default());
    router.parked_for(&stores, "ephemeral:demo", Origin::Attacher, 0);
}

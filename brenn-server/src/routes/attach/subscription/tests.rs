//! Subscription-plane tests, driven against a real in-memory `Messenger` and a
//! stub profile.
//!
//! The profile stub is what makes these transport tests rather than surface
//! tests: the plane is exercised through the seam a route supplies, so nothing
//! here names a component, a port, or an instance.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use brenn_attach_proto::{GapReason as ProtoGapReason, ServerFrame, SubscribeOutcome};
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::db::Db;
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::store::{ResumeCursor, StoreRetained};
use brenn_lib::messaging::{
    GapReason as BusGapReason, Replay, SubscriberEntry, SubscriberEntryKind,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::*;
use crate::routes::attach::profile::SubscriptionFacts;
use crate::routes::attach::session::AttachSessionCtx;
use crate::test_support::attach::{AttachCtxBuilder, TestProfile, one_channel_messenger};

/// The one channel every fixture declares, scheme-qualified and bare.
const ADDR: &str = "brenn:durable-demo";
const BARE: &str = "durable-demo";

/// A channel the fixture's profile never admits — the unsubscribable case.
const UNBOUND_ADDR: &str = "brenn:nonesuch";

/// An attachment whose profile admits [`ADDR`] at the stated fold, backed by a
/// real `Messenger` over one durable channel. Returns the context, the
/// outbound-frame receiver, and the channel uuid (to seed rows against).
async fn attach_at(
    db: &Db,
    push_depth: u64,
    retain_depth: u64,
) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>, Uuid) {
    attach_with_floor(db, push_depth, retain_depth, true).await
}

/// The same attachment with the delivery floor denying [`ADDR`] — the policy
/// holds the transport grant but no matcher covering the channel, so the profile
/// still admits the subscription and every send path must refuse it.
async fn attach_denied(db: &Db) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>, Uuid) {
    attach_with_floor(db, 8, 8, false).await
}

/// [`attach_at`], with the resolved policy's coverage of [`ADDR`] as a
/// parameter.
async fn attach_with_floor(
    db: &Db,
    push_depth: u64,
    retain_depth: u64,
    granted: bool,
) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>, Uuid) {
    let (messenger, channel_uuid) = one_channel_messenger(db, BARE).await;

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    if granted {
        policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact(BARE.to_string())];
    }

    let profile = TestProfile {
        subscribable: HashMap::from([(
            ADDR.to_string(),
            SubscriptionFacts {
                push_depth,
                retain_depth,
            },
        )]),
        subscribe_burst: 3,
        ..TestProfile::new()
    };

    let (ctx, rx) = AttachCtxBuilder::new(profile)
        .messenger(messenger)
        .policy(policy)
        .build();
    (ctx, rx, channel_uuid)
}

/// The common fold: both knobs at 8, wide enough that no test trips the clamp
/// unless it means to.
async fn attach(db: &Db) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>, Uuid) {
    attach_at(db, 8, 8).await
}

/// The mutable half of one connection — the state the plane threads as `&mut`,
/// bundled so a test drives a handler in one line instead of eight arguments.
struct Wire {
    active: ActiveChannels,
    cursors: WireCursors,
    counters: SessionCounters,
    /// The registry-shared active set the router fan-out reads.
    shared: Arc<Mutex<HashSet<String>>>,
}

impl Wire {
    fn new(ctx: &AttachSessionCtx) -> Self {
        let shared = Arc::new(Mutex::new(HashSet::new()));
        Self {
            active: ActiveChannels::new(shared.clone()),
            cursors: WireCursors::new(ctx.messenger.store_incarnation()),
            counters: SessionCounters::default(),
            shared,
        }
    }

    async fn subscribe(
        &mut self,
        ctx: &AttachSessionCtx,
        channel: &str,
        push_depth: u64,
        retain_depth: u64,
        resume: Option<Cursor>,
    ) -> FrameOutcome {
        handle_subscribe(
            ctx,
            &mut self.active,
            &mut self.cursors,
            SubscribeRequest {
                channel,
                push_depth,
                retain_depth,
                resume,
            },
            &mut self.counters,
        )
        .await
    }

    /// Subscribe [`ADDR`] at one depth on both knobs, with no resume — the shape
    /// every test that is not about admission opens with.
    async fn open(&mut self, ctx: &AttachSessionCtx, depth: u64) -> FrameOutcome {
        self.subscribe(ctx, ADDR, depth, depth, None).await
    }

    fn unsubscribe(&mut self, ctx: &AttachSessionCtx, channel: &str) -> FrameOutcome {
        handle_unsubscribe(ctx, &mut self.active, &mut self.cursors, channel)
    }

    async fn push(&mut self, ctx: &AttachSessionCtx, pushes: Vec<SessionPush>) -> FrameOutcome {
        send_session_pushes(
            ctx,
            &self.active,
            &mut self.cursors,
            pushes,
            &mut self.counters,
        )
        .await
    }

    async fn drain(&mut self, ctx: &AttachSessionCtx) -> FrameOutcome {
        drain_all(ctx, &self.active, &mut self.cursors, &mut self.counters).await
    }

    /// The channel's connection-resident position, which every delivery decision
    /// turns on.
    fn position(&self, channel: &str) -> Option<u64> {
        self.cursors.pos_of(channel).map(|cursor| cursor.seq)
    }
}

/// Insert one message on `channel_uuid`.
async fn seed(db: &Db, channel_uuid: Uuid, body: &str, ts_ns: i64) {
    use brenn_lib::messaging::db::insert_message;
    use brenn_lib::messaging::{ChannelScheme, Urgency};
    let conn = db.lock().await;
    insert_message(
        &conn,
        channel_uuid,
        "test",
        "sender",
        body,
        Urgency::Normal,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        None,
        ts_ns,
    );
}

/// Insert one message per body at ascending timestamps, starting after `already`
/// rows are on the channel.
async fn seed_from(db: &Db, channel_uuid: Uuid, already: i64, bodies: &[&str]) {
    for (i, body) in bodies.iter().enumerate() {
        seed(db, channel_uuid, body, 100 * (already + i as i64 + 1)).await;
    }
}

/// [`seed_from`] onto an empty channel.
async fn seed_all(db: &Db, channel_uuid: Uuid, bodies: &[&str]) {
    seed_from(db, channel_uuid, 0, bodies).await;
}

/// The channel's whole retained window, for building live deliveries out of real
/// envelopes rather than hand-assembled ones.
async fn retained(ctx: &AttachSessionCtx) -> Vec<StoreRetained> {
    ctx.messenger
        .store_for_address(ADDR)
        .replay_from(None, Depth::Bounded(64))
        .await
        .messages
}

/// The epoch the channel's store is numbering in — what a hand-minted resume
/// cursor must carry to be anything but a foreign-epoch gap.
async fn epoch_of(ctx: &AttachSessionCtx) -> Uuid {
    ctx.messenger
        .store_for_address(ADDR)
        .replay_from(None, Depth::Bounded(1))
        .await
        .epoch
}

/// One live push of the retained row at `seq`.
fn live(window: &[StoreRetained], seq: u64) -> SessionPush {
    let row = window
        .iter()
        .find(|r| r.seq == seq)
        .unwrap_or_else(|| panic!("retained window holds no row at seq {seq}"));
    SessionPush::Live(LiveDelivery {
        envelope: row.message.clone(),
        retained_seq: row.seq,
    })
}

/// One `Deliver` row's wire facts: the channel, its span seq, the retention
/// position inside its opaque cursor, and the drop count.
fn rows_of(frame: &ServerFrame) -> Vec<(String, u64, u64, u64)> {
    match frame {
        ServerFrame::Deliver { channel, rows } => rows
            .iter()
            .map(|row| {
                let state = cursor::parse(&row.cursor).expect("a minted cursor parses");
                (channel.clone(), row.seq, state.resume.seq, row.dropped)
            })
            .collect(),
        other => panic!("expected a Deliver, got {other:?}"),
    }
}

/// The wire facts of a pass this test expects to hold exactly one row.
fn deliver(frame: &ServerFrame) -> (String, u64, u64, u64) {
    let mut rows = rows_of(frame);
    assert_eq!(rows.len(), 1, "expected a one-row pass, got {rows:?}");
    rows.remove(0)
}

/// The `(replay_count, gap)` of a `SubscribeResult`, asserting its outcome and
/// channel along the way.
fn subscribe_result(frame: &ServerFrame) -> (u32, Option<ProtoGapReason>) {
    match frame {
        ServerFrame::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => {
            assert_eq!(channel, ADDR);
            assert!(matches!(outcome, SubscribeOutcome::Ok));
            (*replay_count, gap.map(|g| g.reason))
        }
        other => panic!("expected a SubscribeResult, got {other:?}"),
    }
}

/// Drain every frame the handlers enqueued.
fn frames(rx: &mut mpsc::Receiver<ServerFrame>) -> Vec<ServerFrame> {
    let mut out = Vec::new();
    while let Ok(frame) = rx.try_recv() {
        out.push(frame);
    }
    out
}

/// A channel the profile does not admit is a violation, and the violation says
/// nothing about whether the channel exists — unknown and undeclared are one
/// answer.
#[tokio::test]
async fn subscribe_to_an_unsubscribable_channel_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, _uuid) = attach(&db).await;
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, UNBOUND_ADDR, 1, 1, None).await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("an unsubscribable channel must violate");
    };
    assert!(detail.contains("unsubscribable"), "detail: {detail}");
    assert!(!wire.active.is_active(UNBOUND_ADDR));
    assert!(frames(&mut rx).is_empty(), "no frame is written");
}

/// One subscription per channel per attachment: a second `Subscribe` on a live
/// channel is a client bug, not a re-anchor.
#[tokio::test]
async fn duplicate_subscribe_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx, _uuid) = attach(&db).await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(wire.open(&ctx, 4).await, FrameOutcome::Continue));
    assert!(
        matches!(wire.open(&ctx, 4).await, FrameOutcome::Violation(_)),
        "a duplicate Subscribe must violate"
    );
}

/// A fresh subscribe answers `SubscribeResult{Ok}`, replays the retained window
/// oldest-first behind it **in one frame**, and activates both the local mirror
/// and the registry-shared set the router reads.
///
/// One frame is the whole point: the attach is one delivery point, so the
/// attacher windows the replay once and caps its new slice at the binding's
/// `push_depth` rather than seeing every retained row as its own arrival.
#[tokio::test]
async fn a_fresh_subscribe_replays_the_window_and_activates() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one", "two"]).await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(wire.open(&ctx, 8).await, FrameOutcome::Continue));

    let out = frames(&mut rx);
    assert_eq!(out.len(), 2, "the result plus one replay frame");
    assert_eq!(subscribe_result(&out[0]), (2, None));
    // Span seqs start at 1 and increase across the pass; the cursor carries each
    // row's own retention position.
    assert_eq!(
        rows_of(&out[1]),
        vec![(ADDR.to_string(), 1, 1, 0), (ADDR.to_string(), 2, 2, 0),]
    );

    assert!(wire.active.is_active(ADDR));
    assert!(
        wire.shared.lock().unwrap().contains(ADDR),
        "the router sees the activation through the shared set"
    );
}

/// An empty replay is the result alone, never an empty pass.
///
/// The most common attach shape in a fresh deployment, and the client's answer to
/// a `Deliver` with no rows in it is a fatal protocol violation — so the guard
/// that writes nothing is load-bearing, not defensive.
#[tokio::test]
async fn a_subscribe_to_an_empty_channel_writes_no_pass() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, _uuid) = attach(&db).await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(wire.open(&ctx, 8).await, FrameOutcome::Continue));

    let out = frames(&mut rx);
    assert_eq!(out.len(), 1, "the result alone: {out:?}");
    assert_eq!(subscribe_result(&out[0]), (0, None));
    assert!(wire.active.is_active(ADDR));
}

/// A wide window is still one frame: the count of rows grows, the count of
/// delivery points does not.
#[tokio::test]
async fn a_sixteen_row_replay_is_one_frame() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_at(&db, 1, 16).await;
    let bodies: Vec<String> = (1..=16).map(|n| format!("m{n}")).collect();
    let refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
    seed_all(&db, uuid, &refs).await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(
        wire.subscribe(&ctx, ADDR, 1, 16, None).await,
        FrameOutcome::Continue
    ));

    let out = frames(&mut rx);
    assert_eq!(out.len(), 2, "the result plus one replay frame");
    assert_eq!(subscribe_result(&out[0]), (16, None));
    assert_eq!(
        rows_of(&out[1]),
        (1..=16u64)
            .map(|n| (ADDR.to_string(), n, n, 0))
            .collect::<Vec<_>>(),
        "sixteen rows, oldest first, seqs 1..=16 — in one frame"
    );
}

/// The client states both knobs and the server clamps each to the boot fold. A
/// client asking for more than the operator declared gets the operator's answer.
#[tokio::test]
async fn a_wide_claim_clamps_to_the_boot_fold() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_at(&db, 2, 2).await;
    seed_all(&db, uuid, &["one", "two", "three"]).await;
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ADDR, 999, 999, None).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(
        wire.active.facts(ADDR),
        Some(SubscriptionFacts {
            push_depth: 2,
            retain_depth: 2,
        }),
        "the connection holds the clamped facts, not the client's claim"
    );
    let out = frames(&mut rx);
    assert_eq!(subscribe_result(&out[0]).0, 2, "clamped to the boot fold");
    assert_eq!(out.len(), 2);
    assert_eq!(rows_of(&out[1]).len(), 2);
}

/// The other direction is the client's own business: a narrower claim than the
/// fold is honoured as stated.
#[tokio::test]
async fn a_narrow_claim_is_honoured_as_stated() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_at(&db, 2, 2).await;
    seed_all(&db, uuid, &["one", "two"]).await;
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ADDR, 1, 1, None).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(
        wire.active.facts(ADDR),
        Some(SubscriptionFacts {
            push_depth: 1,
            retain_depth: 1,
        })
    );
    assert_eq!(
        frames(&mut rx).len(),
        2,
        "one replay frame behind the result"
    );
}

/// A resume cursor replays the suffix above the echoed position as one frame,
/// with no gap when retention covers it, and anchors the span there rather than
/// at 0.
#[tokio::test]
async fn a_covered_resume_replays_the_suffix_with_no_gap() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one", "two", "three"]).await;
    let echoed = cursor::mint(
        ctx.messenger.store_incarnation(),
        ADDR,
        ResumeCursor {
            epoch: epoch_of(&ctx).await,
            seq: 1,
        },
    );
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ADDR, 8, 8, Some(echoed)).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(
        subscribe_result(&out[0]),
        (2, None),
        "the two rows above the echoed position, no gap"
    );
    assert_eq!(out.len(), 2, "the result plus one resume-suffix frame");
    assert_eq!(
        rows_of(&out[1]),
        vec![(ADDR.to_string(), 1, 2, 0), (ADDR.to_string(), 2, 3, 0)]
    );
}

/// A resume the retained window cannot cover is answered `BeyondRetained`.
#[tokio::test]
async fn a_truncated_resume_gaps_beyond_retained() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_at(&db, 2, 2).await;
    seed_all(&db, uuid, &["one", "two", "three", "four"]).await;
    let echoed = cursor::mint(
        ctx.messenger.store_incarnation(),
        ADDR,
        ResumeCursor {
            epoch: epoch_of(&ctx).await,
            seq: 1,
        },
    );
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ADDR, 2, 2, Some(echoed)).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(
        subscribe_result(&frames(&mut rx)[0]).1,
        Some(ProtoGapReason::BeyondRetained)
    );
}

/// A cursor minted under a boot this store never counted (a restore that rolled
/// positions back) is conforming: it is answered as a fresh attach with an
/// `EpochChanged` gap, never a kill.
#[tokio::test]
async fn a_cursor_from_an_uncounted_boot_is_a_fresh_attach() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one", "two"]).await;
    let echoed = cursor::mint(
        ctx.messenger.store_incarnation() + 1,
        ADDR,
        ResumeCursor {
            epoch: epoch_of(&ctx).await,
            seq: 1,
        },
    );
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ADDR, 8, 8, Some(echoed)).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(
        subscribe_result(&out[0]),
        (2, Some(ProtoGapReason::EpochChanged)),
        "the whole window, as for a fresh attach, behind an epoch gap"
    );
    // Anchored at 0, so the first replayed row is position 1 again.
    assert_eq!(
        rows_of(&out[1]),
        vec![(ADDR.to_string(), 1, 1, 0), (ADDR.to_string(), 2, 2, 0)]
    );
}

/// An unparseable cursor cannot come from a conforming attacher — this server
/// minted every cursor it will ever be shown — so it kills the connection, with a
/// sanitized cause on the security line.
#[tokio::test]
async fn an_unparseable_resume_cursor_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, _uuid) = attach(&db).await;
    let bogus: Cursor =
        serde_json::from_value(serde_json::Value::String("not-a-cursor".into())).unwrap();
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ADDR, 8, 8, Some(bogus)).await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("an unparseable cursor must violate");
    };
    assert!(detail.contains("unparseable resume cursor"), "{detail}");
    assert!(
        !wire.active.is_active(ADDR),
        "the parse gate runs before activation"
    );
    assert!(frames(&mut rx).is_empty());
}

/// A cursor this server minted, echoed on a different channel, is a violation
/// rather than a position. The store cannot catch it — every ring store in the
/// process numbers under one shared epoch — so the cursor names its own channel
/// and the check happens here.
#[tokio::test]
async fn a_resume_cursor_from_another_channel_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one", "two"]).await;
    let foreign = cursor::mint(
        ctx.messenger.store_incarnation(),
        UNBOUND_ADDR,
        ResumeCursor {
            epoch: epoch_of(&ctx).await,
            seq: 1,
        },
    );
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ADDR, 8, 8, Some(foreign)).await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("a cursor minted for another channel must violate");
    };
    assert!(detail.contains("minted for another channel"), "{detail}");
    assert!(
        !detail.contains(UNBOUND_ADDR),
        "the cursor's own channel is not echoed back: {detail}"
    );
    assert!(
        !wire.active.is_active(ADDR),
        "the cursor gate runs before activation"
    );
    assert!(frames(&mut rx).is_empty());
}

/// A subscription that neither wakes nor sees is meaningless and would wedge
/// the channel — its replay clamp is empty, so the position could never
/// advance past a gap.
#[tokio::test]
async fn a_subscribe_with_no_window_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ADDR, 0, 0, None).await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("a windowless Subscribe must violate");
    };
    assert!(
        detail.contains("neither a push nor a retain window"),
        "{detail}"
    );
    assert!(!wire.active.is_active(ADDR));
    assert!(frames(&mut rx).is_empty());
}

/// The same rule reads the *clamped* pair, not the claim: a stated pair whose
/// intersection with the boot fold is empty is as windowless as a stated 0/0,
/// and a conforming attacher echoes depths that cannot produce it.
#[tokio::test]
async fn a_claim_that_clamps_to_no_window_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    // A context feed: the operator declared no push window at all.
    let (ctx, _rx, uuid) = attach_at(&db, 0, 2).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);

    // Claiming a push window the fold zeroes and no retain window at all leaves
    // nothing on either knob.
    let outcome = wire.subscribe(&ctx, ADDR, 4, 0, None).await;

    assert!(
        matches!(outcome, FrameOutcome::Violation(_)),
        "a claim clamping to 0/0 must violate"
    );
    assert!(!wire.active.is_active(ADDR));
}

/// The store's decisions map onto the wire's answers once, here — including the
/// anchor each one implies.
#[test]
fn the_resume_mapping_answers_every_decision_once() {
    let epoch = Uuid::from_u128(0x99);
    let cases = [
        (Replay::Fresh, None, 0u64),
        (Replay::UpToDate, None, 7),
        (Replay::Exact, None, 7),
        (
            Replay::Gap(BusGapReason::BeyondRetained),
            Some(ProtoGapReason::BeyondRetained),
            0,
        ),
        (
            Replay::Gap(BusGapReason::EpochChanged),
            Some(ProtoGapReason::EpochChanged),
            0,
        ),
        // A position above anything the channel assigned is answered as a fresh
        // attach with an epoch gap, never a kill.
        (
            Replay::Gap(BusGapReason::ResumeAhead),
            Some(ProtoGapReason::EpochChanged),
            0,
        ),
    ];
    for (decision, want_gap, want_anchor) in cases {
        let (gap, anchor) = resume_answer(decision, Some(7), ADDR, epoch);
        assert_eq!(anchor, want_anchor, "anchor for {decision:?}");
        assert_eq!(gap.map(|g| g.reason), want_gap, "gap for {decision:?}");
    }
    // An unechoed position anchors at 0 wherever the decision keeps it.
    assert_eq!(resume_answer(Replay::UpToDate, None, ADDR, epoch).1, 0);
}

/// Unsubscribe clears both active sets and the channel's wire state; a second one
/// has nothing to remove and is a violation.
#[tokio::test]
async fn unsubscribe_clears_wire_state_and_a_second_one_violates() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 8).await;
    let _ = frames(&mut rx);
    assert!(wire.position(ADDR).is_some());

    let outcome = wire.unsubscribe(&ctx, ADDR);

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert!(!wire.active.is_active(ADDR));
    assert!(
        wire.shared.lock().unwrap().is_empty(),
        "the router sees it go"
    );
    assert!(wire.position(ADDR).is_none(), "the span is gone with it");
    assert!(frames(&mut rx).is_empty(), "unsubscribe is fire-and-forget");
    assert!(
        matches!(wire.unsubscribe(&ctx, ADDR), FrameOutcome::Violation(_)),
        "unsubscribing what is not active must violate"
    );
}

/// The bucket admits exactly the profile's burst back to back, then treats the
/// next frame as fail2ban signal rather than dropping it silently.
#[tokio::test]
async fn the_subscribe_bucket_admits_the_profile_burst_then_violates() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx, _uuid) = attach(&db).await;
    let mut bucket = subscribe_bucket(ctx.profile.as_ref());

    for i in 0..ctx.profile.subscribe_burst() {
        assert!(
            charge_subscribe_token(&ctx, &mut bucket).is_ok(),
            "frame {i} is within the burst"
        );
    }

    let Err(FrameOutcome::Violation(detail)) = charge_subscribe_token(&ctx, &mut bucket) else {
        panic!("beyond-burst subscribe churn must violate");
    };
    assert!(detail.contains("rate exceeded"), "{detail}");
}

/// The three live-delivery decisions, against one connection's position: a
/// duplicate is dropped, the contiguous next position is sent, and a live row
/// above it is refused in favour of the whole suffix from retention.
#[tokio::test]
async fn a_live_row_is_decided_against_the_channel_position() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one", "two"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 8).await;
    let _ = frames(&mut rx);
    assert_eq!(wire.position(ADDR), Some(2));
    seed_from(&db, uuid, 2, &["three", "four", "five"]).await;
    let window = retained(&ctx).await;

    // At or below the position: the replay already wrote it.
    let outcome = wire.push(&ctx, vec![live(&window, 2)]).await;
    assert!(matches!(outcome, FrameOutcome::Continue));
    assert!(frames(&mut rx).is_empty(), "the duplicate is dropped");

    // Contiguous: sent as itself, continuing the span.
    let outcome = wire.push(&ctx, vec![live(&window, 3)]).await;
    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(out.len(), 1);
    assert_eq!(deliver(&out[0]), (ADDR.to_string(), 3, 3, 0));

    // Above the contiguous next position: the live copy is dropped and the
    // channel is served its whole suffix from retention instead — one drain, one
    // frame.
    let outcome = wire.push(&ctx, vec![live(&window, 5)]).await;
    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(out.len(), 1, "the drain is one frame");
    assert_eq!(
        rows_of(&out[0]),
        vec![(ADDR.to_string(), 4, 4, 0), (ADDR.to_string(), 5, 5, 0)],
        "the interior row rides along"
    );
}

/// A live row for a channel this connection does not hold is dropped, not
/// delivered: the router's fan-out and an unsubscribe can race.
#[tokio::test]
async fn a_live_row_on_an_inactive_channel_is_dropped() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let window = retained(&ctx).await;
    let mut wire = Wire::new(&ctx);

    let outcome = wire.push(&ctx, vec![live(&window, 1)]).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert!(frames(&mut rx).is_empty());
}

/// Rows go first as one pass, then the views in arrival order — a view must not
/// break up the sequencing pass the rows share.
#[tokio::test]
async fn a_turn_writes_the_rows_first_then_the_views() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 8).await;
    let _ = frames(&mut rx);
    seed_from(&db, uuid, 1, &["two"]).await;
    let window = retained(&ctx).await;
    let view = SessionPush::DeferredView(DeferredViewPush {
        channel: ADDR.to_string(),
        attribution: Some("clock".to_string()),
        entries: Vec::new(),
    });

    let outcome = wire.push(&ctx, vec![view, live(&window, 2)]).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(out.len(), 2);
    assert_eq!(deliver(&out[0]), (ADDR.to_string(), 2, 2, 0));
    match &out[1] {
        ServerFrame::DeferredView {
            channel,
            attribution,
            entries,
        } => {
            assert_eq!(channel, ADDR);
            assert_eq!(attribution.as_deref(), Some("clock"));
            assert!(entries.is_empty());
        }
        other => panic!("expected a DeferredView, got {other:?}"),
    }
}

/// A drain whose retention no longer covers the span above the position serves
/// the suffix and reports the lost interior as `dropped` on the first delivery
/// that follows it — once, not on every row.
#[tokio::test]
async fn a_drain_reports_the_lost_interior_span_once() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_at(&db, 2, 2).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 2).await;
    let _ = frames(&mut rx);
    assert_eq!(wire.position(ADDR), Some(1));
    // Four more rows, but the subscription's clamp reads only the newest two, so
    // rows 2 and 3 are interior loss.
    seed_from(&db, uuid, 1, &["two", "three", "four", "five"]).await;

    let outcome = wire.drain(&ctx).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(out.len(), 1, "the clamped suffix, in one frame");
    assert_eq!(
        rows_of(&out[0]),
        vec![(ADDR.to_string(), 2, 4, 2), (ADDR.to_string(), 3, 5, 0)],
        "the loss rides the first row only"
    );
}

/// A context feed — a subscription whose push fold is 0 — has no push window to
/// overflow, so nothing may be reported as dropped on it however far its
/// retention read fell behind.
#[tokio::test]
async fn a_context_feed_reports_no_drops() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_at(&db, 0, 2).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.subscribe(&ctx, ADDR, 0, 2, None).await;
    let _ = frames(&mut rx);
    seed_from(&db, uuid, 1, &["two", "three", "four", "five"]).await;

    let outcome = wire.drain(&ctx).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(out.len(), 1);
    let rows = rows_of(&out[0]);
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row.3, 0, "a context feed has no overflow to report");
    }
}

/// A drain that finds nothing above the position writes nothing — the position is
/// the whole delivery state, so a drain racing the live path is a no-op.
#[tokio::test]
async fn a_drain_with_nothing_above_the_position_is_silent() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 8).await;
    let _ = frames(&mut rx);

    let outcome = wire.drain(&ctx).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert!(frames(&mut rx).is_empty());
}

/// A row released below the current position advances the wire span seq but
/// neither regresses the minted cursor nor moves the position — so the next
/// reconnect resumes from the true position, not a below-water floor that would
/// replay already-seen rows.
#[test]
fn next_below_the_position_holds_the_cursor_but_advances_the_span_seq() {
    let mut cursors = WireCursors::new(0);
    cursors.start_span(ADDR, Uuid::from_u128(0x1234), 5);

    let (seq, cursor) = cursors.next(ADDR, 9);
    assert_eq!(seq, 1);
    assert_eq!(cursor::parse(&cursor).unwrap().resume.seq, 9);

    let (seq, cursor) = cursors.next(ADDR, 3);
    assert_eq!(seq, 2, "the span seq advances for every delivery");
    assert_eq!(
        cursor::parse(&cursor).unwrap().resume.seq,
        9,
        "the position does not regress below what was already written"
    );
    assert_eq!(cursors.pos_of(ADDR).unwrap().seq, 9);
}

/// Every channel on one connection stamps the one boot incarnation, and each
/// position carries the epoch the store answered in.
#[test]
fn a_position_carries_its_epoch_and_the_connection_incarnation() {
    let mut cursors = WireCursors::new(7);
    let epoch = Uuid::from_u128(0xfeed);
    cursors.start_span(ADDR, epoch, 0);
    cursors.start_span(UNBOUND_ADDR, epoch, 0);

    let (_, first) = cursors.next(ADDR, 1);
    let (_, second) = cursors.next(UNBOUND_ADDR, 1);

    for cursor in [first, second] {
        let state = cursor::parse(&cursor).unwrap();
        assert_eq!(state.incarnation, 7);
        assert_eq!(state.resume.epoch, epoch);
    }
}

/// The whole rendered violation detail, not a substring of it: the
/// `attacher … account …:` prefix is the stable shape an operator's fail2ban
/// expression keys on, so renaming a word in it must fail here.
#[tokio::test]
async fn a_violation_detail_carries_the_attacher_account_prefix() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx, _uuid) = attach(&db).await;
    let mut wire = Wire::new(&ctx);

    let FrameOutcome::Violation(detail) = wire.subscribe(&ctx, UNBOUND_ADDR, 1, 1, None).await
    else {
        panic!("an unsubscribable channel must violate");
    };

    assert_eq!(
        detail,
        "attacher surface:deskbar account dev: Subscribe to unsubscribable channel brenn:nonesuch",
    );
}

/// The delivery floor is the last check before the socket, and a denial answers
/// `Ok` with an empty replay rather than an error: the subscription is admitted
/// (the profile said so) and simply carries nothing.
#[tokio::test]
async fn a_denied_floor_subscribes_with_no_replay() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_denied(&db).await;
    seed_all(&db, uuid, &["one", "two"]).await;
    let mut wire = Wire::new(&ctx);

    let outcome = wire.open(&ctx, 8).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(out.len(), 1, "the result alone; no row reaches the socket");
    assert_eq!(subscribe_result(&out[0]), (0, None));
    assert!(
        wire.active.is_active(ADDR),
        "the subscription is active; it is the send that is refused"
    );
}

/// The same denial on the drain path: nothing is written and the position does
/// not move, so a policy that later grants the channel still owes the whole
/// suffix.
#[tokio::test]
async fn a_denied_floor_drains_nothing_and_holds_the_position() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_denied(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 8).await;
    let _ = frames(&mut rx);
    assert_eq!(wire.position(ADDR), Some(0));
    seed_from(&db, uuid, 1, &["two", "three"]).await;

    let outcome = wire.drain(&ctx).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert!(frames(&mut rx).is_empty());
    assert_eq!(wire.position(ADDR), Some(0), "the position is unmoved");
}

/// And on the live path, which is the third of the three sends: a denied channel
/// is skipped whether or not its row happened to arrive contiguously.
#[tokio::test]
async fn a_denied_floor_drops_a_live_row() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach_denied(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let window = retained(&ctx).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 8).await;
    let _ = frames(&mut rx);

    let outcome = wire.push(&ctx, vec![live(&window, 1)]).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert!(frames(&mut rx).is_empty());
    assert_eq!(wire.position(ADDR), Some(0));
}

/// A writer that has gone away is a `Disconnect`, never a violation: the socket
/// died, which is not a security event. One test per send path, because each is
/// its own hand-written match on the outcome.
#[tokio::test]
async fn a_dead_writer_disconnects_the_subscribe_path() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    drop(rx);

    assert!(matches!(wire.open(&ctx, 8).await, FrameOutcome::Disconnect));
}

#[tokio::test]
async fn a_dead_writer_disconnects_the_drain_path() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 8).await;
    let _ = frames(&mut rx);
    seed_from(&db, uuid, 1, &["two"]).await;
    drop(rx);

    assert!(matches!(wire.drain(&ctx).await, FrameOutcome::Disconnect));
}

#[tokio::test]
async fn a_dead_writer_disconnects_the_live_path() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one"]).await;
    let mut wire = Wire::new(&ctx);
    let _ = wire.open(&ctx, 8).await;
    let _ = frames(&mut rx);
    seed_from(&db, uuid, 1, &["two"]).await;
    let window = retained(&ctx).await;
    drop(rx);

    let outcome = wire.push(&ctx, vec![live(&window, 2)]).await;

    assert!(matches!(outcome, FrameOutcome::Disconnect));
}

#[tokio::test]
async fn a_dead_writer_disconnects_the_deferred_view_path() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, rx, _uuid) = attach(&db).await;
    let mut wire = Wire::new(&ctx);
    drop(rx);
    let view = SessionPush::DeferredView(DeferredViewPush {
        channel: ADDR.to_string(),
        attribution: Some("clock".to_string()),
        entries: Vec::new(),
    });

    assert!(matches!(
        wire.push(&ctx, vec![view]).await,
        FrameOutcome::Disconnect
    ));
}

// ---------------------------------------------------------------------------
// Class parity
// ---------------------------------------------------------------------------

/// The ephemeral twin of [`ADDR`], scheme-qualified and bare.
const EPH_ADDR: &str = "ephemeral:ephemeral-demo";
const EPH_BARE: &str = "ephemeral-demo";

/// The ephemeral ring's own retention. Sized well above anything the parity
/// script asks for, so what clamps a replay is the subscription's depth on both
/// classes rather than the ring on one of them.
const EPH_RING_DEPTH: u64 = 64;

/// The two channel classes. Durable and ephemeral differ in what a restart
/// destroys and in nothing a session can observe, which is the property this
/// section exists to pin: every class-conditional decision the plane makes —
/// cursor minting against the store's epoch, gap on a hole past retention,
/// foreign-epoch resume, the drain span the clamp leaves — is the same decision
/// on both.
#[derive(Clone, Copy, Debug)]
enum Class {
    Durable,
    Ephemeral,
}

impl Class {
    fn addr(self) -> &'static str {
        match self {
            Class::Durable => ADDR,
            Class::Ephemeral => EPH_ADDR,
        }
    }
}

/// One attachment over one channel of the given class, with the profile
/// admitting it at depth 8 both ways and the policy covering it.
struct ParityRig {
    ctx: AttachSessionCtx,
    rx: mpsc::Receiver<ServerFrame>,
    class: Class,
    channel_uuid: Uuid,
    db: Db,
    /// Rows committed so far, for the durable seeder's ascending timestamps.
    committed: i64,
}

async fn parity_rig(db: &Db, class: Class) -> ParityRig {
    let (messenger, channel_uuid, policy) = match class {
        Class::Durable => {
            let (messenger, uuid) = one_channel_messenger(db, BARE).await;
            let mut policy = AppPolicy::default();
            policy.grants.insert(AppCapability::MessagingSubscribe);
            policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact(BARE.to_string())];
            (messenger, uuid, policy)
        }
        Class::Ephemeral => {
            let (messenger, uuid) = crate::test_support::attach::one_ephemeral_channel_messenger(
                db,
                EPH_BARE,
                EPH_RING_DEPTH,
            );
            let mut policy = AppPolicy::default();
            policy.grants.insert(AppCapability::EphemeralSubscribe);
            policy.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(EPH_BARE.to_string())];
            (messenger, uuid, policy)
        }
    };

    let profile = TestProfile {
        subscribable: HashMap::from([(
            class.addr().to_string(),
            SubscriptionFacts {
                push_depth: 8,
                retain_depth: 8,
            },
        )]),
        subscribe_burst: 8,
        ..TestProfile::new()
    };
    let (ctx, rx) = AttachCtxBuilder::new(profile)
        .messenger(messenger)
        .policy(policy)
        .build();
    ParityRig {
        ctx,
        rx,
        class,
        channel_uuid,
        db: db.clone(),
        committed: 0,
    }
}

/// Land one message on the rig's channel without feeding it to the session.
///
/// The two arms are the two substrates — a durable channel's retention is the
/// database's, an ephemeral channel's is a ring — which is exactly the
/// difference the script exists to prove a session cannot see.
async fn parity_commit(rig: &mut ParityRig, body: &str) {
    rig.committed += 1;
    match rig.class {
        Class::Durable => seed(&rig.db, rig.channel_uuid, body, 100 * rig.committed).await,
        Class::Ephemeral => {
            let sender = brenn_lib::messaging::ParticipantId::for_surface("seed");
            let mut policy = AppPolicy::default();
            policy.grants.insert(AppCapability::EphemeralPublish);
            policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact(EPH_BARE.to_string())];
            let dest = rig
                .ctx
                .messenger
                .resolve_prepaid(&sender, &policy, EPH_ADDR);
            let _ = rig
                .ctx
                .messenger
                .publish_prepaid(
                    &dest,
                    brenn_lib::messaging::PrepaidEntry {
                        body,
                        urgency: brenn_lib::messaging::Urgency::Normal,
                        publish_ts: chrono::Utc::now(),
                    },
                )
                .await;
        }
    }
}

/// The channel's retained window, for building a live push out of a real
/// envelope rather than a hand-assembled one.
async fn parity_window(rig: &ParityRig) -> Vec<StoreRetained> {
    rig.ctx
        .messenger
        .store_for_address(rig.class.addr())
        .replay_from(None, Depth::Bounded(64))
        .await
        .messages
}

/// One frame reduced to what the script pins — one transcript line per row, so
/// the script reads a stream of deliveries rather than a frame layout:
/// everything except the values a class is entitled to differ in — the address,
/// the epoch its seqs are numbered in, and the store incarnation stamped beside
/// them.
fn parity_lines(frame: &ServerFrame) -> Vec<String> {
    match frame {
        ServerFrame::SubscribeResult {
            outcome,
            replay_count,
            gap,
            ..
        } => vec![format!(
            "subscribe_result outcome={outcome:?} replay={replay_count} gap={:?}",
            gap.map(|g| g.reason)
        )],
        ServerFrame::Deliver { rows, .. } => rows
            .iter()
            .map(|row| {
                let state = cursor::parse(&row.cursor).expect("a server-minted cursor parses");
                // Both numbers, because they are different numbers and both must
                // match across classes: `span` is the per-subscribe wire counter
                // the attacher orders on, `pos` the retention position the cursor
                // carries.
                format!(
                    "deliver body={} span={} pos={} dropped={}",
                    row.envelope.body, row.seq, state.resume.seq, row.dropped
                )
            })
            .collect(),
        other => panic!("the parity script expects no {other:?}"),
    }
}

/// Drain what the handlers enqueued into the transcript, returning the cursor the
/// last delivered row among them carried.
fn parity_record(rig: &mut ParityRig, transcript: &mut Vec<String>) -> Option<Cursor> {
    let mut held = None;
    for frame in frames(&mut rig.rx) {
        if let ServerFrame::Deliver { rows, .. } = &frame
            && let Some(last) = rows.last()
        {
            held = Some(last.cursor.clone());
        }
        transcript.extend(parity_lines(&frame));
    }
    held
}

/// Drive the whole scenario against one class and return its transcript.
async fn run_parity_script(class: Class) -> Vec<String> {
    let db = brenn_lib::db::init_db_memory();
    let mut rig = parity_rig(&db, class).await;
    let addr = class.addr();
    let mut transcript: Vec<String> = Vec::new();

    // 1. Two rows land with nobody attached; a fresh cursorless subscribe
    //    replays them.
    parity_commit(&mut rig, "m1").await;
    parity_commit(&mut rig, "m2").await;
    let mut wire = Wire::new(&rig.ctx);
    assert!(matches!(
        wire.subscribe(&rig.ctx, addr, 8, 8, None).await,
        FrameOutcome::Continue
    ));
    let mut held = parity_record(&mut rig, &mut transcript);

    // 2. A live row on the attached subscription, contiguous with its position.
    parity_commit(&mut rig, "m3").await;
    let window = parity_window(&rig).await;
    let push = live(&window, 3);
    assert!(matches!(
        wire.push(&rig.ctx, vec![push]).await,
        FrameOutcome::Continue
    ));
    held = parity_record(&mut rig, &mut transcript).or(held);

    // 3. Nothing it already holds is served again.
    assert!(matches!(wire.drain(&rig.ctx).await, FrameOutcome::Continue));
    assert!(
        parity_record(&mut rig, &mut transcript).is_none(),
        "the drain owed nothing"
    );
    transcript.push("quiet".to_string());

    // 4. A fresh connection echoing the cursor the attacher holds: caught up.
    let mut wire = Wire::new(&rig.ctx);
    assert!(matches!(
        wire.subscribe(&rig.ctx, addr, 8, 8, held.clone()).await,
        FrameOutcome::Continue
    ));
    held = parity_record(&mut rig, &mut transcript).or(held);

    // 5. A row lands while detached; the next connection resumes exactly onto
    //    it. A new subscribe opens a new span, so the span counter restarts at 1
    //    while the position carries on — the two numbers part company here, and
    //    must part company identically on both classes.
    parity_commit(&mut rig, "m4").await;
    let mut wire = Wire::new(&rig.ctx);
    assert!(matches!(
        wire.subscribe(&rig.ctx, addr, 8, 8, held.clone()).await,
        FrameOutcome::Continue
    ));
    held = parity_record(&mut rig, &mut transcript).or(held);

    // 6. A resume above everything the channel ever assigned: answered as a
    //    fresh attach under a gap, on both classes — never as a violation.
    let echoed = cursor::parse(&held.expect("the script received a cursor")).expect("parses");
    let ahead = cursor::mint(
        echoed.incarnation,
        addr,
        ResumeCursor {
            epoch: echoed.resume.epoch,
            seq: echoed.resume.seq + 500,
        },
    );
    let mut wire = Wire::new(&rig.ctx);
    assert!(matches!(
        wire.subscribe(&rig.ctx, addr, 8, 8, Some(ahead)).await,
        FrameOutcome::Continue
    ));
    let held = parity_record(&mut rig, &mut transcript);

    // 7. A cursor stamped with an incarnation the store never reached — what a
    //    backup restore leaves an attacher holding. The same fresh attach, which
    //    pins that the cursors this connection minted carry the store's real
    //    incarnation on both classes.
    let echoed = cursor::parse(&held.expect("step 6 delivered a cursor")).expect("parses");
    let stale = cursor::mint(echoed.incarnation + 1, addr, echoed.resume);
    let mut wire = Wire::new(&rig.ctx);
    assert!(matches!(
        wire.subscribe(&rig.ctx, addr, 8, 8, Some(stale)).await,
        FrameOutcome::Continue
    ));
    parity_record(&mut rig, &mut transcript);

    transcript
}

/// The transcript both classes must produce, spelled out so this pins the
/// behavior rather than only pinning the two classes to each other.
const PARITY_TRANSCRIPT: &[&str] = &[
    // 1. Fresh cursorless subscribe over a two-row window.
    "subscribe_result outcome=Ok replay=2 gap=None",
    "deliver body=m1 span=1 pos=1 dropped=0",
    "deliver body=m2 span=2 pos=2 dropped=0",
    // 2. The live row.
    "deliver body=m3 span=3 pos=3 dropped=0",
    // 3. At most once.
    "quiet",
    // 4. Resume at the held cursor: up to date, nothing owed.
    "subscribe_result outcome=Ok replay=0 gap=None",
    // 5. Resume onto the one row missed while detached.
    "subscribe_result outcome=Ok replay=1 gap=None",
    "deliver body=m4 span=1 pos=4 dropped=0",
    // 6. Resume ahead: a fresh attach under EpochChanged over the whole window.
    "subscribe_result outcome=Ok replay=4 gap=Some(EpochChanged)",
    "deliver body=m1 span=1 pos=1 dropped=0",
    "deliver body=m2 span=2 pos=2 dropped=0",
    "deliver body=m3 span=3 pos=3 dropped=0",
    "deliver body=m4 span=4 pos=4 dropped=0",
    // 7. A stale incarnation: the same fresh attach.
    "subscribe_result outcome=Ok replay=4 gap=Some(EpochChanged)",
    "deliver body=m1 span=1 pos=1 dropped=0",
    "deliver body=m2 span=2 pos=2 dropped=0",
    "deliver body=m3 span=3 pos=3 dropped=0",
    "deliver body=m4 span=4 pos=4 dropped=0",
];

/// **The maxim's pin.** A durable channel and an ephemeral one are one channel
/// from a session's point of view; any future re-divergence fails here rather
/// than reaching production with the suite green.
#[tokio::test]
async fn the_two_channel_classes_produce_one_transcript() {
    let durable = run_parity_script(Class::Durable).await;
    let ephemeral = run_parity_script(Class::Ephemeral).await;

    assert_eq!(
        durable, ephemeral,
        "the session answered a durable channel differently from an ephemeral one"
    );
    assert_eq!(
        durable,
        PARITY_TRANSCRIPT
            .iter()
            .map(|s| (*s).to_string())
            .collect::<Vec<_>>(),
        "the transcript both classes agree on is not the one the design specifies"
    );
}

// ---------------------------------------------------------------------------
// Runtime-provisioned channels: the cap, the absent-channel answer, and the
// directory entry a profile with no boot-declared bindings has to mint.
// ---------------------------------------------------------------------------

/// A second channel the profile admits and the directory does not hold — the
/// granted-but-absent case a runtime-provisioning route races.
const ABSENT_ADDR: &str = "brenn:vanished";

/// An attachment over one real channel whose profile also admits
/// [`ABSENT_ADDR`], with the subscription cap and the minted entry as
/// parameters.
///
/// The messenger holds exactly one channel, so the profile admitting two is the
/// whole fixture: everything a route with runtime-provisioned channels does
/// differently happens between those two answers.
async fn attach_runtime(
    db: &Db,
    max_active_subscriptions: usize,
    runtime_entry: Option<SubscriberEntry>,
    standing: Depth,
) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>, Uuid) {
    let (messenger, channel_uuid) =
        crate::test_support::attach::one_channel_messenger_at_standing(db, BARE, standing).await;

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy.acls.brenn_subscribe = vec![
        ChannelMatcher::Exact(BARE.to_string()),
        ChannelMatcher::Exact("vanished".to_string()),
    ];

    let facts = SubscriptionFacts {
        push_depth: 8,
        retain_depth: 8,
    };
    let profile = TestProfile {
        subscribable: HashMap::from([(ADDR.to_string(), facts), (ABSENT_ADDR.to_string(), facts)]),
        subscribe_burst: 16,
        max_active_subscriptions,
        runtime_entry,
        ..TestProfile::new()
    };

    let (ctx, rx) = AttachCtxBuilder::new(profile)
        .messenger(messenger)
        .policy(policy)
        .build();
    (ctx, rx, channel_uuid)
}

/// The entry a runtime-provisioning profile mints, at the depths its ACL
/// ceilings state.
fn minted_entry(push_depth: u64, retain_depth: u64) -> SubscriberEntry {
    SubscriberEntry {
        kind: SubscriberEntryKind::Remote("pod-kitchen".to_string()),
        push_depth: Depth::Bounded(push_depth),
        retain_depth: Depth::Bounded(retain_depth),
        noise: brenn_lib::messaging::config::NoiseLevel::Metered,
        wake_min: None,
    }
}

/// The channel's subscriber entries, as `(slug, push, retain)`.
fn directory_subscribers(ctx: &AttachSessionCtx, address: &str) -> Vec<(String, Depth, Depth)> {
    ctx.messenger
        .directory()
        .resolve(address)
        .expect("the fixture channel is in the directory")
        .subscribers
        .iter()
        .map(|s| (s.kind.slug().to_string(), s.push_depth, s.retain_depth))
        .collect()
}

/// **A granted channel that is not there is an answer, not a kill.** The
/// operator's own grant is what authorizes the disclosure; outside the grants
/// the address never gets this far. Nothing opens: no span, no active
/// subscription, no replay.
#[tokio::test]
async fn a_granted_but_absent_channel_answers_unavailable() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, _uuid) = attach_runtime(&db, usize::MAX, None, Depth::Unbounded).await;
    let mut wire = Wire::new(&ctx);

    let outcome = wire.subscribe(&ctx, ABSENT_ADDR, 4, 4, None).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    let written = frames(&mut rx);
    assert_eq!(written.len(), 1, "one frame, and it is the refusal");
    match &written[0] {
        ServerFrame::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => {
            assert_eq!(channel, ABSENT_ADDR);
            assert!(matches!(outcome, SubscribeOutcome::Unavailable));
            assert_eq!(*replay_count, 0);
            assert!(gap.is_none(), "nothing was resumed, so nothing gapped");
        }
        other => panic!("expected a SubscribeResult, got {other:?}"),
    }
    assert!(!wire.active.is_active(ABSENT_ADDR), "nothing was opened");
    assert_eq!(wire.position(ABSENT_ADDR), None, "no span was anchored");
    assert!(
        wire.shared.lock().unwrap().is_empty(),
        "the router was never told to queue for it"
    );
}

/// **A malformed cursor still kills, absent channel or not.** The existence
/// answer is the last thing a Subscribe can earn: every violation the frame
/// carries is decided first, so a hostile client cannot launder one behind a
/// channel it knows is gone.
#[tokio::test]
async fn a_violation_outranks_the_unavailable_answer() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, _uuid) = attach_runtime(&db, usize::MAX, None, Depth::Unbounded).await;
    let mut wire = Wire::new(&ctx);

    let junk: Cursor = serde_json::from_value(serde_json::Value::String("not-a-cursor".into()))
        .expect("a cursor is a transparent string");
    let outcome = wire.subscribe(&ctx, ABSENT_ADDR, 4, 4, Some(junk)).await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("an unparseable cursor must violate whatever the channel's state");
    };
    assert!(
        detail.contains("unparseable resume cursor"),
        "detail: {detail}"
    );
    assert!(frames(&mut rx).is_empty(), "no frame is written");
}

/// **The subscription cap is a violation, and it names no channel.** A
/// prefix-granted attacher's active-subscription state is otherwise bounded only
/// by how many channels its ACL ever matches; the cap is the bound, and a
/// correct attacher never reaches it.
#[tokio::test]
async fn subscribing_beyond_the_cap_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, _uuid) = attach_runtime(&db, 1, None, Depth::Unbounded).await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(wire.open(&ctx, 4).await, FrameOutcome::Continue));
    let _ = frames(&mut rx);

    let outcome = wire.subscribe(&ctx, ABSENT_ADDR, 4, 4, None).await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("the second subscription is over a cap of one");
    };
    assert!(
        detail.contains("active-subscription cap"),
        "detail: {detail}"
    );
    assert!(
        !detail.contains("vanished"),
        "the cap is a fact about the attacher, not the channel: {detail}"
    );
    assert!(frames(&mut rx).is_empty(), "no frame is written");

    // The cap counts what is held, so releasing one frees the slot.
    assert!(matches!(
        wire.unsubscribe(&ctx, ADDR),
        FrameOutcome::Continue
    ));
    assert!(matches!(
        wire.subscribe(&ctx, ABSENT_ADDR, 4, 4, None).await,
        FrameOutcome::Continue
    ));
}

/// **The first subscribe mints the directory entry the fan-out reads.** A
/// profile with no boot-declared bindings has nothing to fold from, so without
/// this the subscription is legal and receives nothing forever.
#[tokio::test]
async fn a_first_subscribe_mints_the_profiles_directory_entry() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx, _uuid) =
        attach_runtime(&db, usize::MAX, Some(minted_entry(8, 64)), Depth::Unbounded).await;
    let mut wire = Wire::new(&ctx);

    assert!(directory_subscribers(&ctx, ADDR).is_empty());
    assert!(matches!(wire.open(&ctx, 2).await, FrameOutcome::Continue));

    assert_eq!(
        directory_subscribers(&ctx, ADDR),
        vec![(
            "pod-kitchen".to_string(),
            Depth::Bounded(8),
            Depth::Bounded(64)
        )],
        "the entry carries the profile's ceilings, not the depths the client stated",
    );
}

/// **The entry is cut to what the channel retains.** `reap_frontier` asserts
/// every subscriber depth is at or below the channel's standing retention and
/// the reaper trusts the frontier, so an entry above it would panic the process
/// on the next sweep.
#[tokio::test]
async fn the_minted_entry_is_clamped_to_the_channels_standing_retention() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx, _uuid) = attach_runtime(
        &db,
        usize::MAX,
        Some(minted_entry(8, 64)),
        Depth::Bounded(4),
    )
    .await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(wire.open(&ctx, 2).await, FrameOutcome::Continue));

    assert_eq!(
        directory_subscribers(&ctx, ADDR),
        vec![(
            "pod-kitchen".to_string(),
            Depth::Bounded(4),
            Depth::Bounded(4)
        )],
    );
    ctx.messenger
        .directory()
        .resolve(ADDR)
        .expect("the fixture channel is in the directory")
        .reap_frontier();
}

/// **Re-subscribing mints nothing new.** Another session of the same attacher
/// may already hold the channel, and a second entry would be a second
/// server-side push window feeding the same principal. Unsubscribe leaves the
/// entry alone for the same reason.
#[tokio::test]
async fn re_subscribing_neither_duplicates_the_entry_nor_removes_it() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx, _uuid) =
        attach_runtime(&db, usize::MAX, Some(minted_entry(8, 8)), Depth::Unbounded).await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(wire.open(&ctx, 2).await, FrameOutcome::Continue));
    assert!(matches!(
        wire.unsubscribe(&ctx, ADDR),
        FrameOutcome::Continue
    ));
    assert_eq!(
        directory_subscribers(&ctx, ADDR).len(),
        1,
        "the entry outlives the subscription that minted it",
    );

    assert!(matches!(wire.open(&ctx, 2).await, FrameOutcome::Continue));
    assert_eq!(
        directory_subscribers(&ctx, ADDR),
        vec![(
            "pod-kitchen".to_string(),
            Depth::Bounded(8),
            Depth::Bounded(8)
        )],
        "a re-subscribe replaces rather than appends",
    );
}

/// **A profile whose entries are boot-declared mints none.** The hook is the
/// runtime-provisioning route's, and every other route's directory is untouched
/// by a subscribe.
#[tokio::test]
async fn a_profile_with_boot_declared_entries_touches_the_directory_not_at_all() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx, _uuid) = attach_runtime(&db, usize::MAX, None, Depth::Unbounded).await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(wire.open(&ctx, 2).await, FrameOutcome::Continue));

    assert!(directory_subscribers(&ctx, ADDR).is_empty());
}

//! Subscription-plane tests, driven against a real in-memory `Messenger` and a
//! stub profile.
//!
//! The profile stub is what makes these transport tests rather than surface
//! tests: the plane is exercised through the seam a route supplies, so nothing
//! here names a component, a port, or an instance.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};

use brenn_attach_proto::{GapReason as ProtoGapReason, ServerFrame, SubscribeOutcome};
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::db::Db;
use brenn_lib::messaging::config::{
    ChannelConfigRaw, Depth, MessagingGlobalConfig, build_channel_entries,
};
use brenn_lib::messaging::store::{ResumeCursor, StoreRetained};
use brenn_lib::messaging::{
    GapReason as BusGapReason, MessagingDirectory, Messenger, ParticipantId, Replay, WakeRouter,
    query::NoopWakeRouter,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::*;
use crate::routes::attach::profile::{AttachProfile, DeferredTarget, SubscriptionFacts};
use crate::routes::attach::registry::SessionCaps;
use crate::routes::attach::session::AttachSessionCtx;

/// The one channel every fixture declares, scheme-qualified and bare.
const ADDR: &str = "brenn:durable-demo";
const BARE: &str = "durable-demo";

/// A channel the fixture's profile never admits — the unsubscribable case.
const UNBOUND_ADDR: &str = "brenn:nonesuch";

/// The stub route seam: a bare identity, and whatever channels the test wants
/// subscribable at whatever fold.
struct TestProfile {
    attacher: ParticipantId,
    subscribable: HashMap<String, SubscriptionFacts>,
    burst: u32,
}

impl AttachProfile for TestProfile {
    fn attacher(&self) -> &ParticipantId {
        &self.attacher
    }

    fn subscribable(&self, channel: &str) -> Option<SubscriptionFacts> {
        self.subscribable.get(channel).copied()
    }

    fn publishable(&self, _attribution: Option<&str>, _channel: &str) -> bool {
        false
    }

    fn admit_attribution(&self, attribution: Option<&str>) -> Option<ParticipantId> {
        match attribution {
            None => Some(self.attacher.clone()),
            Some(_) => None,
        }
    }

    fn send_budget_scope(&self) -> &str {
        "deskbar"
    }

    fn deferred_view_targets(&self) -> &[DeferredTarget] {
        &[]
    }

    fn subscribe_burst(&self) -> u32 {
        self.burst
    }

    fn session_caps(&self) -> SessionCaps {
        SessionCaps::UNCAPPED
    }
}

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
    let raw = ChannelConfigRaw {
        send_rate: None,
        uuid: Some(Uuid::new_v4().to_string()),
        address: Some(BARE.to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Unbounded),
        retain_depth: Some(Depth::Unbounded),
        standing_retain_depth: Some(Depth::Unbounded),
        noise: None,
        sink: None,
        wake_min: None,
    };
    let entry = build_channel_entries(&[raw], &MessagingGlobalConfig::default())
        .pop()
        .expect("one channel entry");
    let channel_uuid = entry.uuid;
    {
        let conn = db.lock().await;
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![entry])),
        Arc::from("test-origin"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    if granted {
        policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact(BARE.to_string())];
    }

    let profile = TestProfile {
        attacher: ParticipantId::for_surface("deskbar"),
        subscribable: HashMap::from([(
            ADDR.to_string(),
            SubscriptionFacts {
                push_depth,
                retain_depth,
            },
        )]),
        burst: 3,
    };

    let (tx, rx) = mpsc::channel::<ServerFrame>(64);
    let ctx = AttachSessionCtx {
        profile: Arc::new(profile),
        messenger,
        policy: Arc::new(policy),
        session_id: Uuid::nil(),
        account: "dev".to_string(),
        ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        tx,
    };
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

/// A `Deliver`'s wire facts: the channel, its span seq, the retention position
/// inside its opaque cursor, and the drop count.
fn deliver(frame: &ServerFrame) -> (String, u64, u64, u64) {
    match frame {
        ServerFrame::Deliver {
            channel,
            seq,
            cursor,
            dropped,
            ..
        } => {
            let state = cursor::parse(cursor).expect("a minted cursor parses");
            (channel.clone(), *seq, state.resume.seq, *dropped)
        }
        other => panic!("expected a Deliver, got {other:?}"),
    }
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
/// oldest-first behind it, and activates both the local mirror and the
/// registry-shared set the router reads.
#[tokio::test]
async fn a_fresh_subscribe_replays_the_window_and_activates() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx, uuid) = attach(&db).await;
    seed_all(&db, uuid, &["one", "two"]).await;
    let mut wire = Wire::new(&ctx);

    assert!(matches!(wire.open(&ctx, 8).await, FrameOutcome::Continue));

    let out = frames(&mut rx);
    assert_eq!(out.len(), 3, "the result plus its two replay rows");
    assert_eq!(subscribe_result(&out[0]), (2, None));
    // Span seqs start at 1 and increase; the cursor carries each row's own
    // retention position.
    assert_eq!(deliver(&out[1]), (ADDR.to_string(), 1, 1, 0));
    assert_eq!(deliver(&out[2]), (ADDR.to_string(), 2, 2, 0));

    assert!(wire.active.is_active(ADDR));
    assert!(
        wire.shared.lock().unwrap().contains(ADDR),
        "the router sees the activation through the shared set"
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
    assert_eq!(out.len(), 3);
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
    assert_eq!(frames(&mut rx).len(), 2, "one row behind the result");
}

/// A resume cursor replays the suffix above the echoed position, with no gap when
/// retention covers it, and anchors the span there rather than at 0.
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
    assert_eq!(deliver(&out[1]), (ADDR.to_string(), 1, 2, 0));
    assert_eq!(deliver(&out[2]), (ADDR.to_string(), 2, 3, 0));
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
    assert_eq!(deliver(&out[1]), (ADDR.to_string(), 1, 1, 0));
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
    // channel is served its whole suffix from retention instead.
    let outcome = wire.push(&ctx, vec![live(&window, 5)]).await;
    assert!(matches!(outcome, FrameOutcome::Continue));
    let out = frames(&mut rx);
    assert_eq!(out.len(), 2, "the interior row rides along");
    assert_eq!(deliver(&out[0]), (ADDR.to_string(), 4, 4, 0));
    assert_eq!(deliver(&out[1]), (ADDR.to_string(), 5, 5, 0));
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
        attribution: "clock".to_string(),
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
    assert_eq!(out.len(), 2, "the clamped suffix");
    assert_eq!(deliver(&out[0]), (ADDR.to_string(), 2, 4, 2));
    assert_eq!(
        deliver(&out[1]),
        (ADDR.to_string(), 3, 5, 0),
        "the loss rides the first delivery only"
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
    assert_eq!(out.len(), 2);
    for frame in &out {
        assert_eq!(
            deliver(frame).3,
            0,
            "a context feed has no overflow to report"
        );
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
        attribution: "clock".to_string(),
        entries: Vec::new(),
    });

    assert!(matches!(
        wire.push(&ctx, vec![view]).await,
        FrameOutcome::Disconnect
    ));
}

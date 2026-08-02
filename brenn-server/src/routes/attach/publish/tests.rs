//! Publish-plane tests, driven against a real in-memory `Messenger` and a stub
//! profile.
//!
//! The profile stub is what makes these transport tests rather than surface
//! tests: authority arrives through the seam a route supplies, so nothing here
//! names a component, a port, or an instance — only an attribution string and a
//! channel address.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use brenn_attach_proto::{PublishOutcome, ServerFrame, Urgency};
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::db::Db;
use brenn_lib::messaging::config::{
    ChannelConfigRaw, Depth, MessagingGlobalConfig, SurfaceSendBudget, build_channel_entries,
};
use brenn_lib::messaging::testutils::surface_registrations;
use brenn_lib::messaging::{
    MessagingDirectory, Messenger, ParticipantId, SurfaceBatchPublish, WakeRouter,
    query::NoopWakeRouter,
};
use chrono::Utc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::*;
use crate::routes::attach::registry::{
    AttachSessionHandle, PUSH_QUEUE_FRAMES, SessionCaps, SessionPush,
};
use crate::routes::attach::session::AttachSessionCtx;
use crate::test_support::attach::{AttachCtxBuilder, TestProfile};

/// The attacher every fixture stands up.
const ATTACHER: &str = "deskbar";
/// The sub-identity the fixture declares.
const SUB: &str = "protobar";

/// The ordinary output channel, scheme-qualified and bare.
const OUT: &str = "brenn:demo-out";
const OUT_BARE: &str = "demo-out";
/// The diagnostics channel, scheme-qualified and bare.
const ERRORS: &str = "brenn:demo-errors";
const ERRORS_BARE: &str = "demo-errors";
/// An ephemeral channel, so a batch can straddle the substrate split.
const EPH: &str = "ephemeral:demo-eph";
const EPH_BARE: &str = "demo-eph";

/// Bodies over this answer `BodyTooLarge`. Small enough that a test writes the
/// oversize case as a literal.
const MAX_BODY: usize = 64;

/// The per-connection session id every fixture attaches with — a fixed,
/// recognizable value, so an audit line naming it is unambiguous in a log
/// assertion.
const SESSION_ID: Uuid = Uuid::from_u128(0x5e5_5104);

/// The fixture's authority shape: the sub-identity publishes onto `OUT`, `EPH`
/// and `ERRORS`, the bare identity onto `ERRORS` only, and `ERRORS` carries the
/// diagnostics posture.
fn profile() -> TestProfile {
    TestProfile {
        publishable: HashMap::from([
            (
                Some(SUB.to_string()),
                HashSet::from([OUT.to_string(), EPH.to_string(), ERRORS.to_string()]),
            ),
            (None, HashSet::from([ERRORS.to_string()])),
        ]),
        declared: HashSet::from([SUB.to_string()]),
        diagnostic: Some(ERRORS.to_string()),
        ..TestProfile::new()
    }
}

/// One `[[channel]]` block. A durable channel names its DB row by uuid; a
/// non-durable one derives its identity from its address and must not state one.
fn channel_raw(address: &str) -> ChannelConfigRaw {
    let durable = !address.starts_with("ephemeral:");
    ChannelConfigRaw {
        send_rate: None,
        uuid: durable.then(|| Uuid::new_v4().to_string()),
        address: Some(address.to_string()),
        address_prefix: None,
        description: None,
        // Non-durable retention is process memory, so both of its depths must
        // carry a number, and its retention is the ceiling on its push window.
        // Wide enough that no test here is about either.
        push_depth: Some(if durable {
            Depth::Unbounded
        } else {
            Depth::Bounded(64)
        }),
        retain_depth: Some(if durable {
            Depth::Unbounded
        } else {
            Depth::Bounded(64)
        }),
        // The durable reaper's frontier; a non-durable channel's retention is
        // `retain_depth` alone and stating one is refused.
        standing_retain_depth: durable.then_some(Depth::Unbounded),
        noise: None,
        sink: None,
        wake_min: None,
    }
}

/// An attachment whose profile is `profile`, backed by a real `Messenger` over
/// the two fixture channels. `granted` controls whether the publish ACL covers
/// them at all — the false case is how an invariant-excluded refusal
/// (`AclDenied`) is provoked. `burst` sizes both send budgets.
async fn attach_with(
    db: &Db,
    profile: TestProfile,
    granted: bool,
    burst: u32,
) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>) {
    attach_with_body_cap(db, profile, granted, burst, MAX_BODY).await
}

/// [`attach_with`], with the transport's own body cap as a parameter — for the
/// tests that must set it above the bus's or above what a whole batch charges.
async fn attach_with_body_cap(
    db: &Db,
    profile: TestProfile,
    granted: bool,
    burst: u32,
    max_body_bytes: usize,
) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>) {
    let entries = build_channel_entries(
        &[
            channel_raw(OUT_BARE),
            channel_raw(ERRORS_BARE),
            channel_raw(EPH),
        ],
        &MessagingGlobalConfig::default(),
    );
    {
        let conn = db.lock().await;
        brenn_lib::messaging::db::upsert_channels(&conn, &entries);
    }
    // A non-durable channel's retention is a process-memory ring, registered
    // beside the directory entry that names it.
    let nondurable: Vec<_> = entries
        .iter()
        .filter(|entry| !entry.capabilities().durable)
        .cloned()
        .collect();

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy.grants.insert(AppCapability::EphemeralPublish);
    if granted {
        policy.acls.brenn_publish = vec![
            ChannelMatcher::Exact(OUT_BARE.to_string()),
            ChannelMatcher::Exact(ERRORS_BARE.to_string()),
        ];
        policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact(EPH_BARE.to_string())];
    }
    let budget = SurfaceSendBudget {
        burst,
        refill: Duration::from_secs(3600),
    };
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(entries)),
        Arc::from("test-origin"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(Arc::new(brenn_lib::messaging::store::RingStores::build(
        &nondurable,
    )))
    .with_subscriber_registrations(surface_registrations(HashMap::from([(
        ATTACHER.to_string(),
        policy.clone(),
    )])))
    .with_surface_send_budgets([(
        ATTACHER.to_string(),
        vec![(None, budget), (Some(SUB.to_string()), budget)],
    )]);

    AttachCtxBuilder::new(profile)
        .messenger(messenger)
        .policy(policy)
        .max_body_bytes(max_body_bytes)
        .session_id(SESSION_ID)
        .build()
}

/// The common attachment: everything granted, a budget wide enough that no test
/// trips it unless it means to.
async fn attach(db: &Db) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>) {
    attach_with(db, profile(), true, 64).await
}

/// A bucket wide enough that only the tests aiming at it deny.
fn wide_bucket() -> TokenBucket {
    TokenBucket::new(64, Duration::from_secs(1), 64)
}

fn request<'a>(
    channel: &'a str,
    attribution: Option<&'a str>,
    body: &'a str,
) -> PublishRequest<'a> {
    PublishRequest {
        channel,
        attribution,
        body,
        urgency: Urgency::Normal,
        correlation: Some(7),
    }
}

/// The `PublishResult` outcome the handler enqueued, asserting the correlation
/// rode back with it.
fn published_outcome(rx: &mut mpsc::Receiver<ServerFrame>) -> PublishOutcome {
    match rx.try_recv().expect("a PublishResult frame") {
        ServerFrame::PublishResult {
            correlation,
            outcome,
        } => {
            assert_eq!(
                correlation,
                Some(7),
                "the correlation rides the answer back"
            );
            outcome
        }
        other => panic!("expected PublishResult, got {other:?}"),
    }
}

// ── Authority ────────────────────────────────────────────────────────────────

/// An attribution the profile does not declare is a violation, never a silent
/// demotion to the bare identity — demoting is how a non-conforming client would
/// launder a sub-identity's traffic onto the attacher's own budget.
#[tokio::test]
async fn an_undeclared_attribution_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some("nobody"), "{}"),
        &mut counters,
    )
    .await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("an undeclared attribution must kill the connection");
    };
    assert!(detail.contains("undeclared attribution"), "{detail}");
    assert!(detail.contains("nobody"), "the detail names it: {detail}");
    assert!(rx.try_recv().is_err(), "no frame answers a violation");
    assert_eq!(counters.publishes, 0);
}

/// A channel outside the sender's own set is a violation, whoever else may
/// write it: authority is per attribution, and unknown is indistinguishable from
/// unauthorized (no existence oracle).
#[tokio::test]
async fn a_channel_outside_the_senders_set_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    // The bare identity may write ERRORS but not OUT, which the sub-identity
    // may.
    let outcome = handle_publish(&ctx, &mut bucket, request(OUT, None, "{}"), &mut counters).await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("a channel outside the sender's set must kill the connection");
    };
    assert!(detail.contains("unpublishable channel"), "{detail}");
}

/// The happy path: the message reaches the bus under the sub-identity's own
/// sender, and both halves of the counter move.
#[tokio::test]
async fn a_publish_lands_under_its_attributions_sender() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), r#"{"hello":1}"#),
        &mut counters,
    )
    .await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::Ok);
    assert_eq!(counters.publishes, 1);
    assert_eq!(
        counters
            .by_attribution
            .get(SUB)
            .map(|column| column.publishes),
        Some(1),
        "the attributed column moves with the total"
    );

    let retained = ctx
        .messenger
        .store_for_address(OUT)
        .replay_from(None, Depth::Bounded(8))
        .await;
    let sender = &retained.messages.first().expect("one row").message.sender;
    assert_eq!(
        sender.as_str(),
        ParticipantId::for_surface_component(ATTACHER, SUB).as_str(),
        "the envelope's sender is minted from the attribution, not echoed from the frame"
    );
}

/// The attacher's own publish carries the bare identity and has no attributed
/// column — the breakdown decomposes only the attributable part of the total.
#[tokio::test]
async fn the_attachers_own_publish_is_unattributed() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(ERRORS, None, "{}"),
        &mut counters,
    )
    .await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::Ok);
    assert_eq!(counters.publishes, 1);
    assert!(counters.by_attribution.is_empty());

    let retained = ctx
        .messenger
        .store_for_address(ERRORS)
        .replay_from(None, Depth::Bounded(8))
        .await;
    assert_eq!(
        retained.messages.first().expect("one row").message.sender,
        ParticipantId::for_surface(ATTACHER).as_str(),
    );
}

/// A publishable set that names a page-local address is a broken route: local
/// traffic never crosses the wire, so the assert dies rather than routing it.
#[tokio::test]
#[should_panic(expected = "is page-local")]
async fn a_page_local_channel_in_a_publishable_set_panics() {
    let db = brenn_lib::db::init_db_memory();
    let mut profile = profile();
    profile
        .publishable
        .get_mut(&Some(SUB.to_string()))
        .expect("the declared sub-identity")
        .insert("local:brenn/toast".to_string());
    let (ctx, _rx) = attach_with(&db, profile, true, 64).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let _ = handle_publish(
        &ctx,
        &mut bucket,
        request("local:brenn/toast", Some(SUB), "{}"),
        &mut counters,
    )
    .await;
}

/// An oversized body is answered, not punished — and spends no rate token, so a
/// correct-but-buggy sender gets feedback without losing its allowance.
#[tokio::test]
async fn an_oversized_body_answers_rather_than_kills() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = TokenBucket::new(2, Duration::from_secs(3600), 2);
    let mut counters = SessionCounters::default();
    let big = "x".repeat(MAX_BODY + 1);

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), &big),
        &mut counters,
    )
    .await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(
        published_outcome(&mut rx),
        PublishOutcome::BodyTooLarge {
            len: (MAX_BODY + 1) as u64,
            max: MAX_BODY as u64,
        }
    );
    assert_eq!(counters.publish_body_too_large, 1);
    // The bucket is untouched: a doomed publish spends no token.
    assert!(matches!(
        bucket.try_consume(),
        TokenBucketOutcome::Granted | TokenBucketOutcome::GrantedAfterSuppression { .. }
    ));
}

/// Repeated oversized bodies escalate: the answer path spends no token, so
/// without a ceiling an attacher could flood the (body cap, frame cap] window
/// unthrottled.
#[tokio::test]
async fn persistent_oversized_bodies_escalate_to_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();
    let big = "x".repeat(MAX_BODY + 1);

    for _ in 1..BODY_TOO_LARGE_VIOLATION_THRESHOLD {
        assert!(matches!(
            handle_publish(
                &ctx,
                &mut bucket,
                request(OUT, Some(SUB), &big),
                &mut counters
            )
            .await,
            FrameOutcome::Continue
        ));
    }
    let FrameOutcome::Violation(detail) = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), &big),
        &mut counters,
    )
    .await
    else {
        panic!("the Nth oversized body must escalate");
    };
    assert!(detail.contains("persistent oversized"), "{detail}");
}

/// The connection bucket answers `RateLimited` and counts the denial against the
/// sub-identity that made it — never a kill, since a legitimate retry loop
/// reaches it.
#[tokio::test]
async fn an_exhausted_connection_bucket_answers_rate_limited() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = TokenBucket::new(1, Duration::from_secs(3600), 1);
    let mut counters = SessionCounters::default();

    for _ in 0..2 {
        assert!(matches!(
            handle_publish(
                &ctx,
                &mut bucket,
                request(OUT, Some(SUB), "{}"),
                &mut counters
            )
            .await,
            FrameOutcome::Continue
        ));
    }

    assert_eq!(published_outcome(&mut rx), PublishOutcome::Ok);
    assert_eq!(published_outcome(&mut rx), PublishOutcome::RateLimited);
    assert_eq!(counters.publish_rate_limited, 1);
    assert_eq!(
        counters
            .by_attribution
            .get(SUB)
            .map(|column| column.publish_rate_limited),
        Some(1),
    );
}

/// A drained send budget is the same answer from the other tier: the bus's own
/// refusal maps to `RateLimited` and never kills.
#[tokio::test]
async fn an_exhausted_send_budget_answers_rate_limited() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach_with(&db, profile(), true, 1).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    for _ in 0..2 {
        assert!(matches!(
            handle_publish(
                &ctx,
                &mut bucket,
                request(OUT, Some(SUB), "{}"),
                &mut counters
            )
            .await,
            FrameOutcome::Continue
        ));
    }

    assert_eq!(published_outcome(&mut rx), PublishOutcome::Ok);
    assert_eq!(published_outcome(&mut rx), PublishOutcome::RateLimited);
    assert_eq!(counters.publish_rate_limited, 1);
}

/// A refusal the boot invariants exclude, on a diagnostics channel: reported and
/// answered `Failed`, never fatal. Killing the server over its own diagnostics,
/// on an attacker-sendable frame path, inverts priorities.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_diagnostic_channel_refusal_is_reported_not_fatal() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach_with(&db, profile(), false, 64).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(ERRORS, None, r#"{"message":"boom"}"#),
        &mut counters,
    )
    .await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::Failed);
    assert!(
        logs_contain("boom"),
        "the report survives as a log line when the publish did not"
    );
    assert!(logs_contain("preserved in this log line only"));
}

/// A *successful* diagnostic publish leaves the audit record that is the only
/// link between a report on the bus and the authenticated account that sent it:
/// the envelope carries the attacher principal, and neither it nor the report
/// body carries the account or the session.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_diagnostic_publish_success_is_audited() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(ERRORS, None, r#"{"message":"CANARY"}"#),
        &mut counters,
    )
    .await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::Ok);
    assert!(logs_contain("attach diagnostic report published"));
    assert!(logs_contain("account=dev"), "the account is correlated");
    assert!(
        logs_contain(&SESSION_ID.to_string()),
        "the session is correlated"
    );
    assert!(
        !logs_contain("CANARY"),
        "the audit record carries no report content"
    );
}

/// An ordinary publish leaves no audit record: the correlation exists for the
/// diagnostics path, whose bodies an operator reads out of band, and not for
/// every message a component sends.
#[tokio::test]
#[tracing_test::traced_test]
async fn an_ordinary_publish_success_is_not_audited() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), "{}"),
        &mut counters,
    )
    .await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::Ok);
    assert!(!logs_contain("attach diagnostic report published"));
}

/// A body the transport admitted and the bus refused is a wiring bug between two
/// caps that derive from one number. It is answered like any oversized body, but
/// counted apart: folding it into the transport-reject count would let an
/// internal disagreement drive the escalation that exists to punish clients.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_cap_disagreement_is_counted_apart_from_client_oversize() {
    let db = brenn_lib::db::init_db_memory();
    let bus_max = MessagingGlobalConfig::default().max_body_bytes;
    let (ctx, mut rx) = attach_with_body_cap(&db, profile(), true, 64, bus_max * 2).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();
    let body = "x".repeat(bus_max + 1);

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), &body),
        &mut counters,
    )
    .await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(
        published_outcome(&mut rx),
        PublishOutcome::BodyTooLarge {
            len: (bus_max + 1) as u64,
            max: bus_max as u64,
        }
    );
    assert_eq!(counters.publish_body_cap_disagreement, 1);
    assert_eq!(
        counters.publish_body_too_large, 0,
        "an internal disagreement must not drive the client-oversize escalation"
    );
    assert!(logs_contain("body-size caps disagree"));
}

/// The same refusal on an invariant channel kills the process: boot proved the
/// channel reachable and policy-covered, so a refusal says the server disagrees
/// with itself.
#[tokio::test]
#[should_panic(expected = "broken boot invariant")]
async fn an_invariant_channel_refusal_panics() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach_with(&db, profile(), false, 64).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let _ = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), "{}"),
        &mut counters,
    )
    .await;
}

// ── The atomic batch flush ───────────────────────────────────────────────────

fn entry(channel: &str, body: &str, deliver_after: Option<u64>) -> BatchEntry {
    BatchEntry {
        channel: channel.to_string(),
        body: body.to_string(),
        urgency: Urgency::Normal,
        deliver_after,
    }
}

fn cancel(channel: &str, message_id: Uuid) -> BatchDeferredOp {
    BatchDeferredOp {
        channel: channel.to_string(),
        message_id,
        op: DeferredOpKind::Cancel,
    }
}

fn batch<'a>(
    attribution: Option<&'a str>,
    publishes: &'a [BatchEntry],
    deferred_ops: &'a [BatchDeferredOp],
) -> PublishBatchRequest<'a> {
    PublishBatchRequest {
        attribution,
        correlation: 11,
        publishes,
        deferred_ops,
    }
}

/// The `PublishBatchResult` outcome the handler enqueued, asserting the
/// correlation rode back with it.
fn batch_outcome(rx: &mut mpsc::Receiver<ServerFrame>) -> PublishBatchOutcome {
    match rx.try_recv().expect("a PublishBatchResult frame") {
        ServerFrame::PublishBatchResult {
            correlation,
            outcome,
        } => {
            assert_eq!(correlation, 11, "the correlation rides the answer back");
            outcome
        }
        other => panic!("expected PublishBatchResult, got {other:?}"),
    }
}

/// Every retained body on `channel`, oldest first.
async fn bodies(ctx: &AttachSessionCtx, channel: &str) -> Vec<String> {
    ctx.messenger
        .store_for_address(channel)
        .replay_from(None, Depth::Bounded(512))
        .await
        .messages
        .into_iter()
        .map(|row| row.message.body.clone())
        .collect()
}

/// The whole flush lands in call order under the sender the attribution mints,
/// and the stamps are strictly increasing so call order survives the class
/// boundary.
#[tokio::test]
async fn a_batch_lands_in_call_order_with_strictly_increasing_stamps() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let publishes = [
        entry(OUT, r#"{"n":1}"#, None),
        entry(OUT, r#"{"n":2}"#, None),
        entry(OUT, r#"{"n":3}"#, None),
    ];

    let outcome =
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    assert_eq!(counters.publishes, 3);
    assert_eq!(
        counters.by_attribution.get(SUB).map(|c| c.publishes),
        Some(3),
    );

    let retained = ctx
        .messenger
        .store_for_address(OUT)
        .replay_from(None, Depth::Bounded(8))
        .await
        .messages;
    let stamps: Vec<_> = retained.iter().map(|row| row.message.publish_ts).collect();
    assert_eq!(
        retained
            .iter()
            .map(|row| row.message.body.as_str())
            .collect::<Vec<_>>(),
        vec![r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#],
    );
    assert!(
        stamps.windows(2).all(|pair| pair[0] < pair[1]),
        "stamps are strictly increasing: {stamps:?}"
    );
    assert!(
        retained.iter().all(|row| row.message.sender
            == ParticipantId::for_surface_component(ATTACHER, SUB).as_str()),
        "every entry is stamped with the attribution's sender"
    );
}

/// An attribution the profile does not declare kills the connection before any
/// entry is looked at — the single-publish rule, on the atomic path.
#[tokio::test]
async fn an_undeclared_attribution_kills_the_batch() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let publishes = [entry(OUT, "{}", None)];

    let outcome =
        handle_publish_batch(&ctx, batch(Some("nobody"), &publishes, &[]), &mut counters).await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("an undeclared attribution must kill the connection");
    };
    assert!(detail.contains("undeclared attribution"), "{detail}");
    assert!(rx.try_recv().is_err(), "no frame answers a violation");
    assert!(bodies(&ctx, OUT).await.is_empty());
}

/// The shape gates, each of which a conforming attacher enforces at buffer time:
/// an empty flush is never sent, and neither list nor the total body size ever
/// passes its per-activation cap.
#[tokio::test]
async fn the_batch_shape_gates_are_violations() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;

    let over_count: Vec<BatchEntry> = (0..MAX_PUBLISHES_PER_ACTIVATION + 1)
        .map(|_| entry(OUT, "{}", None))
        .collect();
    let over_ops: Vec<BatchDeferredOp> = (0..MAX_PUBLISHES_PER_ACTIVATION + 1)
        .map(|_| cancel(OUT, Uuid::new_v4()))
        .collect();
    // Wide enough that the byte cap trips inside the count cap. The batch
    // legality law is deliberately not what answers here: the shape gates run
    // first, so the work the resolvers would do is bounded before it starts.
    let wide = MAX_PUBLISH_BYTES_PER_ACTIVATION / MAX_PUBLISHES_PER_ACTIVATION + 1;
    let over_bytes: Vec<BatchEntry> = (0..MAX_PUBLISHES_PER_ACTIVATION)
        .map(|_| entry(OUT, &"x".repeat(wide), None))
        .collect();
    // Edit bodies count against the same cap: each one rewrites a parked row, so
    // a frame of max-size edits is the same work behind the same one-token draw.
    let over_edit_bytes: Vec<BatchDeferredOp> = (0..MAX_PUBLISHES_PER_ACTIVATION)
        .map(|_| BatchDeferredOp {
            channel: OUT.to_string(),
            message_id: Uuid::new_v4(),
            op: DeferredOpKind::Edit {
                body: Some("x".repeat(wide)),
                deliver_after: None,
            },
        })
        .collect();

    let cases: Vec<(&str, Vec<BatchEntry>, Vec<BatchDeferredOp>)> = vec![
        ("empty PublishBatch", Vec::new(), Vec::new()),
        ("over the", over_count, Vec::new()),
        ("control ops, over the", Vec::new(), over_ops),
        ("body bytes, over the", over_bytes, Vec::new()),
        ("body bytes, over the", Vec::new(), over_edit_bytes),
    ];
    for (expected, publishes, ops) in cases {
        let mut counters = SessionCounters::default();
        let outcome =
            handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &ops), &mut counters).await;
        let FrameOutcome::Violation(detail) = outcome else {
            panic!("a malformed batch shape must kill the connection: {expected}");
        };
        assert!(detail.contains(expected), "{detail}");
    }
    assert!(bodies(&ctx, OUT).await.is_empty(), "nothing was applied");
}

/// The wire's own legality law, which is tighter than the per-activation caps
/// and is the one the read cap is derived from: a batch whose channels and
/// bodies together outspend the attachment's body cap is a frame no conforming
/// attacher composes.
#[tokio::test]
async fn an_illegal_batch_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    // Each entry is inside the body cap on its own; together they are not.
    let publishes: Vec<BatchEntry> = (0..4).map(|_| entry(OUT, r#"{"n":1}"#, None)).collect();

    let outcome =
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await;
    let FrameOutcome::Violation(detail) = outcome else {
        panic!("an illegal batch must kill the connection");
    };
    assert!(detail.contains("illegal PublishBatch"), "{detail}");
    assert!(bodies(&ctx, OUT).await.is_empty(), "nothing was applied");
}

/// The per-entry gates are violations, not outcomes — and they run before
/// anything applies, so a batch whose *last* entry is broken publishes none of
/// the good ones ahead of it.
#[tokio::test]
async fn a_broken_entry_kills_the_batch_and_applies_nothing() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let good = entry(OUT, r#"{"good":true}"#, None);
    let cases: Vec<(&str, BatchEntry)> = vec![
        // The bare identity may write ERRORS but not OUT; a channel outside the
        // sender's own set is unpublishable whoever else may write it.
        ("unpublishable channel", entry("brenn:nonesuch", "{}", None)),
        ("over the", entry(OUT, &"x".repeat(MAX_BODY + 1), None)),
        (
            "unrepresentable deliver_after",
            entry(OUT, "{}", Some(u64::MAX)),
        ),
    ];

    for (expected, broken) in cases {
        let mut counters = SessionCounters::default();
        let publishes = [good.clone(), broken];
        let outcome =
            handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await;
        let FrameOutcome::Violation(detail) = outcome else {
            panic!("a broken entry must kill the connection: {expected}");
        };
        assert!(detail.contains(expected), "{detail}");
        assert!(rx.try_recv().is_err(), "no frame answers a violation");
        assert!(
            bodies(&ctx, OUT).await.is_empty(),
            "the entry ahead of the broken one must not have applied"
        );
        assert_eq!(counters.publishes, 0);
    }
}

/// A drained send budget answers `RateLimited` — never a kill and never a
/// partial application: the attacher re-parks the flush and retries it whole, so
/// nothing may have landed.
#[tokio::test]
async fn an_exhausted_send_budget_refuses_the_whole_batch() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach_with(&db, profile(), true, 2).await;
    let mut counters = SessionCounters::default();
    let publishes = [
        entry(OUT, r#"{"n":1}"#, None),
        entry(OUT, r#"{"n":2}"#, None),
        entry(OUT, r#"{"n":3}"#, None),
    ];

    let outcome =
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::RateLimited);
    assert_eq!(
        counters.publish_rate_limited, 3,
        "the denial is counted once per entry that did not publish"
    );
    assert!(bodies(&ctx, OUT).await.is_empty());
}

/// An ops-only flush is still a frame the principal sent, so it draws a token —
/// a path that draws zero is a path a client can ride for free.
#[tokio::test]
async fn an_ops_only_batch_draws_a_token() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach_with(&db, profile(), true, 1).await;
    let mut counters = SessionCounters::default();

    // The op loses its race (nothing is parked), which is benign — but the draw
    // happened before it ran.
    let ops = [cancel(OUT, Uuid::new_v4())];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);

    let publishes = [entry(OUT, "{}", None)];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(
        batch_outcome(&mut rx),
        PublishBatchOutcome::RateLimited,
        "the ops-only flush spent the only token"
    );
}

/// A release time already past is an ordinary immediate publish, decided once
/// for the whole flush — not an error and not a park.
#[tokio::test]
async fn a_release_time_already_past_publishes_immediately() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let past = (Utc::now() - chrono::Duration::hours(1)).timestamp_millis() as u64;
    let publishes = [entry(OUT, r#"{"now":true}"#, Some(past))];

    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    assert_eq!(bodies(&ctx, OUT).await, vec![r#"{"now":true}"#.to_string()]);
}

/// A scheduled entry parks instead of committing, and the flush restates the
/// sender's mirror on the channel it parked against.
#[tokio::test]
async fn a_scheduled_entry_parks_and_restates_the_view() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let (mut sibling, _guard) = sibling_view_queue(&ctx);

    let later = (Utc::now() + chrono::Duration::hours(1)).timestamp_millis() as u64;
    let publishes = [
        entry(OUT, r#"{"now":true}"#, None),
        entry(OUT, r#"{"later":true}"#, Some(later)),
    ];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    assert_eq!(
        bodies(&ctx, OUT).await,
        vec![r#"{"now":true}"#.to_string()],
        "a parked entry holds no retention position"
    );
    // One restatement: the immediate entry owes none.
    let view = one_view(&mut sibling);
    assert_eq!(view.channel, OUT);
    assert_eq!(view.attribution.as_deref(), Some(SUB));
    assert_eq!(view.entries.len(), 1);
    assert_eq!(view.entries[0].body, r#"{"later":true}"#);
}

/// The attacher's own flush parks under its bare identity, and its mirror names
/// no attribution: that set is nobody else's.
#[tokio::test]
async fn a_bare_identity_flush_parks_under_the_attacher() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let (mut sibling, _guard) = sibling_view_queue(&ctx);

    let later = (Utc::now() + chrono::Duration::hours(1)).timestamp_millis() as u64;
    let publishes = [entry(ERRORS, r#"{"later":true}"#, Some(later))];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(None, &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    assert!(
        counters.by_attribution.is_empty(),
        "the attacher's own flush has no attributed column"
    );
    let view = one_view(&mut sibling);
    assert_eq!(view.attribution, None);
    assert_eq!(view.entries.len(), 1);
    let parked = ctx
        .messenger
        .deferred_view_for_sender(ERRORS, sender_of(None).as_str(), Utc::now())
        .await;
    assert_eq!(parked.len(), 1, "the set is the bare identity's");
}

/// A cancel drops the schedule and the flush restates the emptied view — the
/// mirror is a full replacement, so an empty set is stated, not implied.
#[tokio::test]
async fn a_cancel_op_empties_the_set_and_restates_the_view() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, OUT, r#"{"later":true}"#).await;
    let (mut sibling, _guard) = sibling_view_queue(&ctx);

    let ops = [cancel(OUT, parked_id)];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    let view = one_view(&mut sibling);
    assert_eq!(view.channel, OUT);
    assert!(view.entries.is_empty(), "the cancelled schedule is gone");
}

/// An edit rewrites the parked body in place; the restated view carries the new
/// one.
#[tokio::test]
async fn an_edit_op_rewrites_the_parked_body() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, OUT, r#"{"v":1}"#).await;

    let ops = [BatchDeferredOp {
        channel: OUT.to_string(),
        message_id: parked_id,
        op: DeferredOpKind::Edit {
            body: Some(r#"{"v":2}"#.to_string()),
            deliver_after: None,
        },
    }];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    let parked = ctx
        .messenger
        .deferred_view_for_sender(
            OUT,
            ParticipantId::for_surface_component(ATTACHER, SUB).as_str(),
            Utc::now(),
        )
        .await;
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].envelope.body, r#"{"v":2}"#);
}

/// An edit body is charged against the frame's byte budget like a published
/// one, so an oversize edit is an illegal batch — and it kills before any op
/// applies.
#[tokio::test]
async fn an_oversized_edit_body_kills_the_batch() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, OUT, r#"{"v":1}"#).await;

    let ops = [BatchDeferredOp {
        channel: OUT.to_string(),
        message_id: parked_id,
        op: DeferredOpKind::Edit {
            body: Some("x".repeat(MAX_BODY + 1)),
            deliver_after: None,
        },
    }];
    let FrameOutcome::Violation(detail) =
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await
    else {
        panic!("an oversized edit body must kill the connection");
    };
    assert!(detail.contains("illegal PublishBatch"), "{detail}");
    let parked = ctx
        .messenger
        .deferred_view_for_sender(
            OUT,
            ParticipantId::for_surface_component(ATTACHER, SUB).as_str(),
            Utc::now(),
        )
        .await;
    assert_eq!(parked[0].envelope.body, r#"{"v":1}"#, "nothing applied");
}

/// An op naming a message parked by another sender is a violation: the ids a
/// conforming attacher can name come from a sender-scoped view, so this one came
/// from outside every window it was ever offered.
#[tokio::test]
async fn an_op_on_another_senders_message_is_a_violation() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    // Parked under the sub-identity, named by the bare identity — which may write
    // ERRORS, so authority is not what refuses here.
    let parked_id = park(&ctx, ERRORS, r#"{"later":true}"#).await;

    let ops = [cancel(ERRORS, parked_id)];
    let FrameOutcome::Violation(detail) =
        handle_publish_batch(&ctx, batch(None, &[], &ops), &mut counters).await
    else {
        panic!("naming another sender's parked message must kill the connection");
    };
    assert!(detail.contains("parked by another sender"), "{detail}");
    assert!(
        detail.contains(&parked_id.to_string()),
        "the detail names the message: {detail}"
    );
}

/// An op naming a message that already released is the benign race: logged,
/// counted, and never a kill — a conforming attacher can always lose it.
#[tokio::test]
#[tracing_test::traced_test]
async fn an_op_that_lost_the_release_race_is_benign() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();

    let ops = [cancel(OUT, Uuid::new_v4())];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    assert!(logs_contain("deferred control op is a no-op"));
}

/// **The op half's gates are violations too, and they run before any op applies.**
/// Authority over a parked set is the authority to write the channel it lives on,
/// and an unrepresentable release time must not collapse into "no change". Each
/// case rides behind a legitimate op, which must therefore not have applied.
#[tokio::test]
async fn a_broken_control_op_kills_the_batch_and_applies_nothing() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let cases: Vec<(&str, BatchDeferredOp)> = vec![
        // A channel outside the sender's own set. `WrongSender` downstream is no
        // substitute: a sub-identity that legitimately parked on a channel whose
        // grant later narrowed is still the sender, so only this check refuses it.
        (
            "unpublishable channel",
            cancel("brenn:nonesuch", Uuid::nil()),
        ),
        (
            "unrepresentable deliver_after",
            BatchDeferredOp {
                channel: OUT.to_string(),
                message_id: Uuid::nil(),
                op: DeferredOpKind::Edit {
                    body: None,
                    deliver_after: Some(u64::MAX),
                },
            },
        ),
    ];

    for (expected, broken) in cases {
        let parked_id = park(&ctx, OUT, r#"{"v":1}"#).await;
        let mut counters = SessionCounters::default();
        let ops = [cancel(OUT, parked_id), broken];
        let FrameOutcome::Violation(detail) =
            handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await
        else {
            panic!("a broken control op must kill the connection: {expected}");
        };
        assert!(detail.contains(expected), "{detail}");
        assert!(rx.try_recv().is_err(), "no frame answers a violation");
        let parked = ctx
            .messenger
            .deferred_view_for_sender(OUT, sender_of(Some(SUB)).as_str(), Utc::now())
            .await;
        assert!(
            parked.iter().any(|m| m.message_uuid() == parked_id),
            "the cancel ahead of the broken op must not have applied"
        );
    }
}

/// **An op that violates after earlier ops applied still restates their
/// channels.** The connection dies, so the end-of-batch emission never runs — and
/// without this a sibling attachment keeps mirroring a schedule that no longer
/// exists, which is what provokes the cancel loop the mirror exists to prevent.
#[tokio::test]
async fn a_violating_op_restates_what_its_predecessors_already_changed() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let mine = park(&ctx, OUT, r#"{"mine":true}"#).await;
    // Parked by the bare identity, on a channel `SUB` may also write — so
    // authority is not what refuses the second op.
    let anothers = park_under(&ctx, ERRORS, None, r#"{"theirs":true}"#).await;
    let (mut sibling, _guard) = sibling_view_queue(&ctx);

    let ops = [cancel(OUT, mine), cancel(ERRORS, anothers)];
    let FrameOutcome::Violation(detail) =
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await
    else {
        panic!("naming another sender's parked message must kill the connection");
    };
    assert!(detail.contains("parked by another sender"), "{detail}");

    let view = one_view(&mut sibling);
    assert_eq!(view.channel, OUT, "the applied op's channel is restated");
    assert_eq!(view.attribution.as_deref(), Some(SUB));
    assert!(
        view.entries.is_empty(),
        "the op emptied the set, and no later change would ever correct the mirror"
    );
}

/// **An edit moves the release time, and the sweep is woken.** The release sweep
/// sleeps to the earliest deadline it last computed, so an edit that moved one
/// earlier has to wake it or the message waits out the poll interval. The
/// restated mirror carries the new time in the units the attacher's own clock
/// reads.
#[tokio::test]
async fn an_edit_op_moves_the_release_time() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, OUT, r#"{"v":1}"#).await;
    let (mut sibling, _guard) = sibling_view_queue(&ctx);

    // The park released an hour out; move it to five minutes out. Whole seconds,
    // because that is the resolution a parked row's release time is stored at.
    let earlier_ms = (Utc::now() + chrono::Duration::minutes(5)).timestamp() as u64 * 1000;
    let ops = [BatchDeferredOp {
        channel: OUT.to_string(),
        message_id: parked_id,
        op: DeferredOpKind::Edit {
            body: None,
            deliver_after: Some(earlier_ms),
        },
    }];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    let parked = ctx
        .messenger
        .deferred_view_for_sender(OUT, sender_of(Some(SUB)).as_str(), Utc::now())
        .await;
    assert_eq!(parked.len(), 1);
    assert_eq!(
        parked[0].release_at.timestamp_millis() as u64,
        earlier_ms,
        "the stored release time moved to the edit's"
    );
    assert_eq!(
        parked[0].envelope.body, r#"{"v":1}"#,
        "the body is untouched"
    );
    let view = one_view(&mut sibling);
    assert_eq!(view.entries.len(), 1);
    assert_eq!(view.entries[0].deliver_after, earlier_ms);
}

/// **One flush restates a channel once, however many of its halves touched it.**
/// The park and the op both name `OUT`, and the view is recomputed from the store
/// — so a second emission would carry the snapshot the first already did, doubled
/// at every attachment of the attacher.
#[tokio::test]
async fn one_flush_restates_a_channel_once_across_a_park_and_an_op() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, OUT, r#"{"old":true}"#).await;
    let (mut sibling, _guard) = sibling_view_queue(&ctx);

    let later = (Utc::now() + chrono::Duration::hours(2)).timestamp_millis() as u64;
    let publishes = [entry(OUT, r#"{"new":true}"#, Some(later))];
    let ops = [cancel(OUT, parked_id)];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &ops), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    let view = one_view(&mut sibling);
    assert_eq!(view.channel, OUT);
    assert_eq!(
        view.entries
            .iter()
            .map(|e| e.body.as_str())
            .collect::<Vec<_>>(),
        vec![r#"{"new":true}"#],
        "the single restatement is the truth after both halves"
    );
}

/// **Call order survives the substrate split.** The stamping pass runs before the
/// batch parts into its durable and ephemeral halves, so the delivered envelopes'
/// `publish_ts` — the ordering contract's only observable — orders the whole
/// flush, not each class within itself.
#[tokio::test]
async fn batch_stamps_order_across_the_substrate_split() {
    let db = brenn_lib::db::init_db_memory();
    // A body cap the four entries' channels and bodies fit under together: the
    // batch spends one budget across the frame, and the fixture's usual cap is
    // deliberately tiny.
    let (ctx, mut rx) = attach_with_body_cap(&db, profile(), true, 64, 256).await;
    let mut counters = SessionCounters::default();
    let publishes = [
        entry(OUT, r#"{"n":1}"#, None),
        entry(EPH, r#"{"n":2}"#, None),
        entry(OUT, r#"{"n":3}"#, None),
        entry(EPH, r#"{"n":4}"#, None),
    ];

    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));

    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    let mut stamped: Vec<(chrono::DateTime<Utc>, String)> = Vec::new();
    for channel in [OUT, EPH] {
        for row in ctx
            .messenger
            .store_for_address(channel)
            .replay_from(None, Depth::Bounded(8))
            .await
            .messages
        {
            stamped.push((row.message.publish_ts, row.message.body.clone()));
        }
    }
    assert_eq!(stamped.len(), 4, "both classes carried their half");
    stamped.sort();
    assert_eq!(
        stamped
            .iter()
            .map(|(_, body)| body.as_str())
            .collect::<Vec<_>>(),
        vec![r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#, r#"{"n":4}"#],
        "the global stamp order is the call order, across the class boundary"
    );
    assert!(
        stamped.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "stamps are strictly increasing across both classes: {stamped:?}"
    );
}

// ── The parked-set mirror ────────────────────────────────────────────────────

/// Park one message on `channel` under `SUB`'s sender, returning its id.
async fn park(ctx: &AttachSessionCtx, channel: &str, body: &str) -> Uuid {
    park_under(ctx, channel, Some(SUB), body).await
}

/// The sender one attribution parks under, as the profile mints it.
fn sender_of(attribution: Option<&str>) -> ParticipantId {
    match attribution {
        None => ParticipantId::for_surface(ATTACHER),
        Some(name) => ParticipantId::for_surface_component(ATTACHER, name),
    }
}

/// Park one message on `channel` under `attribution`'s sender, returning its id.
async fn park_under(
    ctx: &AttachSessionCtx,
    channel: &str,
    attribution: Option<&str>,
    body: &str,
) -> Uuid {
    let release = Utc::now() + chrono::Duration::hours(1);
    ctx.messenger
        .publish_batch_from_surface(
            ATTACHER,
            attribution,
            &[SurfaceBatchPublish {
                channel_address: channel,
                body,
                urgency: Urgency::Normal,
                publish_ts_ns: brenn_lib::messaging::db::utc_to_ns(Utc::now()),
                deliver_after: Some(release),
            }],
        )
        .await;
    let parked = ctx
        .messenger
        .deferred_view_for_sender(channel, sender_of(attribution).as_str(), Utc::now())
        .await;
    parked.last().expect("one parked message").message_uuid()
}

/// Register a sibling attachment of this attacher and hand back the queue its
/// deferred-view pushes land in.
fn sibling_view_queue(
    ctx: &AttachSessionCtx,
) -> (
    mpsc::Receiver<SessionPush>,
    crate::routes::attach::registry::AttachSessionGuard,
) {
    let (push_tx, push_rx) = mpsc::channel(PUSH_QUEUE_FRAMES);
    let mut handle = AttachSessionHandle::for_test("dev");
    handle.push_tx = push_tx;
    let guard = ctx
        .registry
        .try_register(
            ctx.profile.attacher().as_str(),
            handle,
            SessionCaps::UNCAPPED,
        )
        .expect("registered");
    (push_rx, guard)
}

/// The one deferred-view push in `queue`, asserting nothing else followed it.
fn one_view(
    queue: &mut mpsc::Receiver<SessionPush>,
) -> crate::routes::attach::registry::DeferredViewPush {
    let SessionPush::DeferredView(view) = queue.try_recv().expect("a view push") else {
        panic!("expected a deferred-view push");
    };
    assert!(
        queue.try_recv().is_err(),
        "one channel, one restatement per flush"
    );
    view
}

/// Seeding frames the nonempty sets and stays silent about the rest: an absent
/// frame is what tells a fresh attachment the set is empty.
#[tokio::test]
async fn seeding_frames_the_nonempty_sets_and_stays_silent_about_the_rest() {
    let db = brenn_lib::db::init_db_memory();
    let mut profile = profile();
    profile.deferred_targets = vec![
        DeferredTarget {
            channel: ERRORS.to_string(),
            attribution: Some(SUB.to_string()),
        },
        DeferredTarget {
            channel: OUT.to_string(),
            attribution: Some(SUB.to_string()),
        },
    ];
    let (ctx, mut rx) = attach_with(&db, profile, true, 64).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, OUT, r#"{"later":true}"#).await;

    assert!(matches!(
        seed_deferred_views(&ctx, &mut counters).await,
        FrameOutcome::Continue
    ));

    match rx.try_recv().expect("one DeferredView frame") {
        ServerFrame::DeferredView {
            channel,
            attribution,
            entries,
        } => {
            assert_eq!(channel, OUT);
            assert_eq!(attribution.as_deref(), Some(SUB));
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].message_id, parked_id);
            assert_eq!(entries[0].body, r#"{"later":true}"#);
            assert!(
                entries[0].deliver_after > Utc::now().timestamp_millis() as u64,
                "the release time rides the wire in epoch milliseconds"
            );
        }
        other => panic!("expected DeferredView, got {other:?}"),
    }
    assert!(
        rx.try_recv().is_err(),
        "the empty set is framed by its absence, not by an empty frame"
    );
}

/// A broadcast reaches every attachment of the attacher: the parked set belongs
/// to the sub-identity, which every attachment shares.
#[tokio::test]
async fn a_broadcast_reaches_every_attachment_of_the_attacher() {
    let db = brenn_lib::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let parked_id = park(&ctx, OUT, "{}").await;

    let mut queues = Vec::new();
    let mut guards = Vec::new();
    for account in ["alice", "bob"] {
        let (push_tx, push_rx) = mpsc::channel(PUSH_QUEUE_FRAMES);
        let mut handle = AttachSessionHandle::for_test(account);
        handle.push_tx = push_tx;
        guards.push(
            ctx.registry
                .try_register(
                    ctx.profile.attacher().as_str(),
                    handle,
                    SessionCaps::UNCAPPED,
                )
                .expect("registered"),
        );
        queues.push(push_rx);
    }

    broadcast_deferred_view(&ctx, Some(SUB), OUT, Utc::now()).await;

    for queue in &mut queues {
        let SessionPush::DeferredView(view) = queue.try_recv().expect("a view push") else {
            panic!("expected a deferred-view push");
        };
        assert_eq!(view.channel, OUT);
        assert_eq!(view.attribution.as_deref(), Some(SUB));
        assert_eq!(view.entries.len(), 1);
        assert_eq!(view.entries[0].message_id, parked_id);
    }
}

/// A parked set outside the seeding targets is reported, not repaired: the
/// entries still release, but nothing on the attacher can see or cancel them,
/// and that is an operator's decision to have made.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_parked_set_outside_the_targets_is_reported_as_orphaned() {
    let db = brenn_lib::db::init_db_memory();
    // No targets at all, so the parked set below is reachable by no frame.
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    park(&ctx, OUT, "{}").await;

    assert!(matches!(
        seed_deferred_views(&ctx, &mut counters).await,
        FrameOutcome::Continue
    ));

    assert!(rx.try_recv().is_err(), "an unreachable set frames nothing");
    assert!(logs_contain("parked messages this attachment cannot see"));
    assert!(
        logs_contain(SUB),
        "the orphan report names the sub-identity"
    );
}

/// **A seeding target the profile does not declare is the server disagreeing
/// with itself, not a client misbehaving.** The targets are the route's own boot
/// data and the attacher has sent nothing but a conforming `Hello` when seeding
/// runs, so blaming its IP with a security event would invert the fail2ban
/// signal — and limping on would be tolerating a broken invariant.
#[tokio::test]
#[should_panic(expected = "the profile does not declare")]
async fn a_seeding_target_the_profile_does_not_declare_panics() {
    let db = brenn_lib::db::init_db_memory();
    let mut profile = profile();
    profile.deferred_targets = vec![DeferredTarget {
        channel: OUT.to_string(),
        attribution: Some("ghost".to_string()),
    }];
    let (ctx, _rx) = attach_with(&db, profile, true, 64).await;
    let mut counters = SessionCounters::default();

    let _ = seed_deferred_views(&ctx, &mut counters).await;
}

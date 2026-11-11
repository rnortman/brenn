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

use brenn_attach_proto::{MessageBody, PublishOutcome, ServerFrame, Urgency};
use brenn_db::Db;
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::messaging::config::{
    AttachSendBudget, ChannelConfigRaw, Depth, MessagingGlobalConfig, build_channel_entries,
};
use brenn_lib::messaging::{AttachScope, MessagingDirectory, ParticipantId};
use brenn_messaging::testutils::surface_registrations;
use brenn_messaging::{
    AttachBatchPublish, Messenger, MissingChannelPosture, WakeRouter, attach_principal_budgets,
    query::NoopWakeRouter,
};
use chrono::Utc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::*;
use crate::registry::{AttachSessionHandle, PUSH_QUEUE_FRAMES, SessionCaps, SessionPush};
use crate::session::AttachSessionCtx;
use crate::test_support::{AttachCtxBuilder, TestProfile};

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
/// A channel declared `non_text`, which a v4 attacher may publish to.
const FRAMES: &str = "brenn:demo-frames";
const FRAMES_BARE: &str = "demo-frames";

/// Bodies over this answer `BodyTooLarge`. Small enough that a test writes the
/// oversize case as a literal.
const MAX_BODY: usize = 64;

/// The per-connection session id every fixture attaches with — a fixed,
/// recognizable value, so an audit line naming it is unambiguous in a log
/// assertion.
const SESSION_ID: Uuid = Uuid::from_u128(0x5e5_5104);

/// The fixture's authority shape: the sub-identity publishes onto `OUT`, `EPH`,
/// `FRAMES` and `ERRORS`, the bare identity onto `ERRORS` only, and `ERRORS`
/// carries the diagnostics posture.
fn profile() -> TestProfile {
    TestProfile {
        publishable: HashMap::from([
            (
                Some(SUB.to_string()),
                HashSet::from([
                    OUT.to_string(),
                    EPH.to_string(),
                    FRAMES.to_string(),
                    ERRORS.to_string(),
                ]),
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
        non_text: false,
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
    attach_at_version(db, profile, granted, burst, max_body_bytes, 4).await
}

/// [`attach_with_body_cap`], with the session's negotiated version as a
/// parameter.
async fn attach_at_version(
    db: &Db,
    profile: TestProfile,
    granted: bool,
    burst: u32,
    max_body_bytes: usize,
    version: u32,
) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>) {
    let entries = build_channel_entries(
        &[
            channel_raw(OUT_BARE),
            channel_raw(ERRORS_BARE),
            channel_raw(EPH),
            ChannelConfigRaw {
                non_text: true,
                ..channel_raw(FRAMES_BARE)
            },
        ],
        &MessagingGlobalConfig::default(),
    );
    {
        let conn = db.lock().await;
        brenn_messaging_store::db::upsert_channels(&conn, &entries);
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
            ChannelMatcher::Exact(FRAMES_BARE.to_string()),
        ];
        policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact(EPH_BARE.to_string())];
    }
    let budget = AttachSendBudget {
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
    .with_ring_stores(Arc::new(brenn_messaging_store::store::RingStores::build(
        &nondurable,
    )))
    .with_subscriber_registrations(surface_registrations(HashMap::from([(
        ATTACHER.to_string(),
        policy.clone(),
    )])))
    .with_attach_send_budgets(attach_principal_budgets(
        AttachScope::surface(ATTACHER),
        vec![(None, budget), (Some(SUB.to_string()), budget)],
    ));

    AttachCtxBuilder::new(profile)
        .messenger(messenger)
        .policy(policy)
        .max_body_bytes(max_body_bytes)
        .session_id(SESSION_ID)
        .version(version)
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
    body: &'a MessageBody,
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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some("nobody"), &"{}".into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    // The bare identity may write ERRORS but not OUT, which the sub-identity
    // may.
    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, None, &"{}".into()),
        &mut counters,
    )
    .await;

    let FrameOutcome::Violation(detail) = outcome else {
        panic!("a channel outside the sender's set must kill the connection");
    };
    assert!(detail.contains("unpublishable channel"), "{detail}");
}

/// A granted address that is not a well-formed channel name is a violation on
/// every write path.
///
/// Authority does not bound the bytes: a profile answering from prefix matchers
/// grants whatever follows the prefix, so a client can name a "channel" carrying
/// a newline. Nothing downstream would mint it, and the batch path could only
/// count it as unpublished — after carrying it into a log. Refused at the gate
/// the authority question is asked at, so the two paths agree.
#[tokio::test]
async fn a_granted_but_malformed_channel_is_a_violation_on_both_paths() {
    const FORGED: &str = "brenn:demo-out\nsender=system:root body=owned";

    let db = brenn_messaging_store::db::init_db_memory();
    let mut profile = profile();
    profile
        .publishable
        .get_mut(&Some(SUB.to_string()))
        .expect("the fixture grants the sub-identity")
        .insert(FORGED.to_string());
    let (ctx, _rx) = attach_with(&db, profile, true, 8).await;

    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();
    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(FORGED, Some(SUB), &"{}".into()),
        &mut counters,
    )
    .await;
    let FrameOutcome::Violation(detail) = outcome else {
        panic!("a malformed channel must kill the connection");
    };
    assert!(detail.contains("malformed channel"), "{detail}");
    assert!(
        !detail.contains('\n'),
        "the detail carries no raw client bytes: {detail:?}"
    );

    let mut counters = SessionCounters::default();
    let publishes = [entry(FORGED, "{}", None)];
    let outcome =
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await;
    let FrameOutcome::Violation(detail) = outcome else {
        panic!("a malformed batch entry must kill the connection");
    };
    assert!(detail.contains("malformed channel"), "{detail}");
}

/// The happy path: the message reaches the bus under the sub-identity's own
/// sender, and both halves of the counter move.
#[tokio::test]
async fn a_publish_lands_under_its_attributions_sender() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), &r#"{"hello":1}"#.into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(ERRORS, None, &"{}".into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
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
        request("local:brenn/toast", Some(SUB), &"{}".into()),
        &mut counters,
    )
    .await;
}

/// An oversized body is answered, not punished — and spends no rate token, so a
/// correct-but-buggy sender gets feedback without losing its allowance.
#[tokio::test]
async fn an_oversized_body_answers_rather_than_kills() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = TokenBucket::new(2, Duration::from_secs(3600), 2);
    let mut counters = SessionCounters::default();
    let big = MessageBody::from("x".repeat(MAX_BODY + 1));

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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();
    let big = MessageBody::from("x".repeat(MAX_BODY + 1));

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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = TokenBucket::new(1, Duration::from_secs(3600), 1);
    let mut counters = SessionCounters::default();

    for _ in 0..2 {
        assert!(matches!(
            handle_publish(
                &ctx,
                &mut bucket,
                request(OUT, Some(SUB), &"{}".into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_with(&db, profile(), true, 1).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    for _ in 0..2 {
        assert!(matches!(
            handle_publish(
                &ctx,
                &mut bucket,
                request(OUT, Some(SUB), &"{}".into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_with(&db, profile(), false, 64).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(ERRORS, None, &r#"{"message":"boom"}"#.into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(ERRORS, None, &r#"{"message":"CANARY"}"#.into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), &"{}".into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
    let bus_max = MessagingGlobalConfig::default().max_body_bytes;
    let (ctx, mut rx) = attach_with_body_cap(&db, profile(), true, 64, bus_max * 2).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();
    let body = MessageBody::from("x".repeat(bus_max + 1));

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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, _rx) = attach_with(&db, profile(), false, 64).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();

    let _ = handle_publish(
        &ctx,
        &mut bucket,
        request(OUT, Some(SUB), &"{}".into()),
        &mut counters,
    )
    .await;
}

// ── The atomic batch flush ───────────────────────────────────────────────────

fn entry(channel: &str, body: &str, deliver_after: Option<u64>) -> BatchEntry {
    BatchEntry {
        channel: channel.to_string(),
        body: body.into(),
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
        .map(|row| row.message.body.as_text().unwrap().to_owned())
        .collect()
}

/// The whole flush lands in call order under the sender the attribution mints,
/// and the stamps are strictly increasing so call order survives the class
/// boundary.
#[tokio::test]
async fn a_batch_lands_in_call_order_with_strictly_increasing_stamps() {
    let db = brenn_messaging_store::db::init_db_memory();
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
            .map(|row| row.message.body.as_text().unwrap())
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
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
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
                body: Some("x".repeat(wide).into()),
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
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
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

/// **The flush reads its posture off the profile.** A route whose channels are
/// minted at runtime answers `Race`, and an entry naming a channel that left the
/// directory is then dropped and counted rather than fatal — the rest of the
/// flush still lands. The default `Invariant` panics on the same batch, so a
/// call site that hard-coded either answer fails here.
#[tokio::test]
async fn a_vanished_target_is_dropped_when_the_profile_answers_race() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_with(
        &db,
        TestProfile {
            missing_channel_posture: MissingChannelPosture::Race,
            ..profile()
        },
        true,
        64,
    )
    .await;
    let mut counters = SessionCounters::default();

    // Deprovision one of the two targets between the attach and the flush: the
    // profile still grants it, and nothing but the directory disagrees.
    let uuid = ctx
        .messenger
        .directory()
        .resolve(OUT)
        .expect("the fixture channel is in the directory")
        .uuid;
    assert!(ctx.messenger.directory().remove_channel(&uuid));

    let publishes = [
        entry(OUT, r#"{"gone":true}"#, None),
        entry(ERRORS, r#"{"here":true}"#, None),
    ];
    let outcome =
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await;

    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    assert_eq!(
        counters.publishes, 1,
        "the vanished entry published nothing and is not counted as though it had"
    );
    assert_eq!(bodies(&ctx, ERRORS).await, vec![r#"{"here":true}"#]);
}

/// A drained send budget answers `RateLimited` — never a kill and never a
/// partial application: the attacher re-parks the flush and retries it whole, so
/// nothing may have landed.
#[tokio::test]
async fn an_exhausted_send_budget_refuses_the_whole_batch() {
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
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
    assert_eq!(view.entries[0].body, MessageBody::from(r#"{"later":true}"#));
}

/// The attacher's own flush parks under its bare identity, and its mirror names
/// no attribution: that set is nobody else's.
#[tokio::test]
async fn a_bare_identity_flush_parks_under_the_attacher() {
    let db = brenn_messaging_store::db::init_db_memory();
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
        .deferred_view_for_sender(ERRORS, sender_of(None).as_str())
        .await;
    assert_eq!(parked.len(), 1, "the set is the bare identity's");
}

/// A cancel drops the schedule and the flush restates the emptied view — the
/// mirror is a full replacement, so an empty set is stated, not implied.
#[tokio::test]
async fn a_cancel_op_empties_the_set_and_restates_the_view() {
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, OUT, r#"{"v":1}"#).await;

    let ops = [BatchDeferredOp {
        channel: OUT.to_string(),
        message_id: parked_id,
        op: DeferredOpKind::Edit {
            body: Some(r#"{"v":2}"#.into()),
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
        )
        .await;
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].envelope.body.as_text().unwrap(), r#"{"v":2}"#);
}

/// A v4 attacher may publish to a `non_text` channel, and what it parks and
/// edits there is its own frames' strings: the stored bodies are untyped text
/// and every restated view carries them as text.
#[tokio::test]
async fn a_v4_view_on_a_non_text_channel_after_a_publish_and_an_edit_is_text() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach(&db).await;
    assert!(
        ctx.messenger
            .directory()
            .resolve(FRAMES)
            .expect("the fixture declares the channel")
            .resolved_channel
            .non_text
    );
    let mut counters = SessionCounters::default();
    let (mut sibling, _guard) = sibling_view_queue(&ctx);

    let later = (Utc::now() + chrono::Duration::hours(1)).timestamp_millis() as u64;
    let publishes = [
        entry(FRAMES, r#"{"now":true}"#, None),
        entry(FRAMES, r#"{"v":1}"#, Some(later)),
    ];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    let view = one_view(&mut sibling);
    assert_eq!(view.channel, FRAMES);
    assert_eq!(view.entries.len(), 1);
    assert_eq!(view.entries[0].body, MessageBody::from(r#"{"v":1}"#));
    let parked_id = view.entries[0].message_id;

    let ops = [BatchDeferredOp {
        channel: FRAMES.to_string(),
        message_id: parked_id,
        op: DeferredOpKind::Edit {
            body: Some(r#"{"v":2}"#.into()),
            deliver_after: None,
        },
    }];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    let view = one_view(&mut sibling);
    assert_eq!(view.entries.len(), 1);
    assert_eq!(view.entries[0].message_id, parked_id);
    assert_eq!(view.entries[0].body, MessageBody::from(r#"{"v":2}"#));

    let parked = ctx
        .messenger
        .deferred_view_for_sender(FRAMES, sender_of(Some(SUB)).as_str())
        .await;
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].envelope.body.content_type(), None);
    assert_eq!(parked[0].envelope.body.as_text(), Some(r#"{"v":2}"#));
    let retained = ctx
        .messenger
        .store_for_address(FRAMES)
        .replay_from(None, Depth::Bounded(512))
        .await
        .messages;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].message.body.content_type(), None);
    assert_eq!(retained[0].message.body.as_text(), Some(r#"{"now":true}"#));
}

/// An edit body is charged against the frame's byte budget like a published
/// one, so an oversize edit is an illegal batch — and it kills before any op
/// applies.
#[tokio::test]
async fn an_oversized_edit_body_kills_the_batch() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, OUT, r#"{"v":1}"#).await;

    let ops = [BatchDeferredOp {
        channel: OUT.to_string(),
        message_id: parked_id,
        op: DeferredOpKind::Edit {
            body: Some("x".repeat(MAX_BODY + 1).into()),
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
        )
        .await;
    assert_eq!(
        parked[0].envelope.body.as_text().unwrap(),
        r#"{"v":1}"#,
        "nothing applied"
    );
}

/// An op naming a message parked by another sender is a violation: the ids a
/// conforming attacher can name come from a sender-scoped view, so this one came
/// from outside every window it was ever offered.
#[tokio::test]
async fn an_op_on_another_senders_message_is_a_violation() {
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
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
            .deferred_view_for_sender(OUT, sender_of(Some(SUB)).as_str())
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
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
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
        .deferred_view_for_sender(OUT, sender_of(Some(SUB)).as_str())
        .await;
    assert_eq!(parked.len(), 1);
    assert_eq!(
        parked[0].release_at.timestamp_millis() as u64,
        earlier_ms,
        "the stored release time moved to the edit's"
    );
    assert_eq!(
        parked[0].envelope.body.as_text(),
        Some(r#"{"v":1}"#),
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
    let db = brenn_messaging_store::db::init_db_memory();
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
            .map(|e| e.body.as_text().unwrap())
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
    let db = brenn_messaging_store::db::init_db_memory();
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
            stamped.push((
                row.message.publish_ts,
                row.message.body.as_text().unwrap().to_owned(),
            ));
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
async fn park(ctx: &AttachSessionCtx, channel: &str, body: impl Into<MessageBody>) -> Uuid {
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
    body: impl Into<MessageBody>,
) -> Uuid {
    let release = Utc::now() + chrono::Duration::hours(1);
    ctx.messenger
        .publish_batch_from_attacher(
            AttachScope::surface(ATTACHER),
            attribution,
            &[AttachBatchPublish {
                channel_address: channel,
                body: body.into(),
                urgency: Urgency::Normal,
                publish_ts_ns: brenn_messaging_store::db::utc_to_ns(Utc::now()),
                deliver_after: Some(release),
            }],
            MissingChannelPosture::Invariant,
        )
        .await;
    let parked = ctx
        .messenger
        .deferred_view_for_sender(channel, sender_of(attribution).as_str())
        .await;
    parked.last().expect("one parked message").message_uuid()
}

/// Register a sibling attachment of this attacher and hand back the queue its
/// deferred-view pushes land in.
fn sibling_view_queue(
    ctx: &AttachSessionCtx,
) -> (
    mpsc::Receiver<SessionPush>,
    crate::registry::AttachSessionGuard,
) {
    let (push_tx, push_rx) = mpsc::channel(PUSH_QUEUE_FRAMES);
    let mut handle = AttachSessionHandle::for_test("dev");
    handle.push_tx = push_tx;
    // Under the registry key, which is what the routes register under — a
    // surface's bare slug, not the participant id. Registering under the id
    // would make the broadcast find a session no production lookup can.
    let guard = ctx
        .registry
        .try_register(
            &ctx.profile.attach_scope().registry_key(),
            handle,
            SessionCaps::UNCAPPED,
        )
        .expect("registered");
    (push_rx, guard)
}

/// The one deferred-view push in `queue`, asserting nothing else followed it.
fn one_view(queue: &mut mpsc::Receiver<SessionPush>) -> crate::registry::DeferredViewPush {
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
    let db = brenn_messaging_store::db::init_db_memory();
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
            assert_eq!(entries[0].body, MessageBody::from(r#"{"later":true}"#));
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
    let db = brenn_messaging_store::db::init_db_memory();
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
                    &ctx.profile.attach_scope().registry_key(),
                    handle,
                    SessionCaps::UNCAPPED,
                )
                .expect("registered"),
        );
        queues.push(push_rx);
    }

    broadcast_deferred_view(&ctx, Some(SUB), OUT).await;

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
    let db = brenn_messaging_store::db::init_db_memory();
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
    let db = brenn_messaging_store::db::init_db_memory();
    let mut profile = profile();
    profile.deferred_targets = vec![DeferredTarget {
        channel: OUT.to_string(),
        attribution: Some("ghost".to_string()),
    }];
    let (ctx, _rx) = attach_with(&db, profile, true, 64).await;
    let mut counters = SessionCounters::default();

    let _ = seed_deferred_views(&ctx, &mut counters).await;
}

fn parked_with(body: MessageBody) -> DeferredMessage {
    DeferredMessage {
        release_at: chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
        envelope: Arc::new(brenn_envelope::testutils::fixture_envelope(
            "brenn:out",
            "parked",
            body,
        )),
    }
}

/// A view entry holds the parked body as it is, type and all: what a version
/// can write of it is decided where the frame is encoded.
#[test]
fn a_view_entry_holds_the_parked_body_as_it_is() {
    for body in [
        MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap(),
        MessageBody::from_parts(Some("image/png"), &[0x89]).unwrap(),
    ] {
        let entries = deferred_view_entries(&[parked_with(body.clone())]);
        assert_eq!(entries[0].body, body);
    }
}

/// The common attachment, negotiated at v5.
async fn attach_v5(db: &Db) -> (AttachSessionCtx, mpsc::Receiver<ServerFrame>) {
    attach_at_version(db, profile(), true, 64, MAX_BODY, 5).await
}

/// A typed body that is not text: never valid UTF-8, so nothing on the path can
/// read it as a string.
fn png(len: usize) -> MessageBody {
    MessageBody::from_parts(Some("image/png"), &vec![0xff; len]).unwrap()
}

/// A single non-text publish to a `non_text` channel is stored byte-exact with
/// its content type, and the body cap reads its raw length.
#[tokio::test]
async fn a_v5_non_text_publish_is_stored_with_its_type() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_v5(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();
    let body = png(MAX_BODY);

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(FRAMES, Some(SUB), &body),
        &mut counters,
    )
    .await;
    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::Ok);

    let over = png(MAX_BODY + 1);
    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(FRAMES, Some(SUB), &over),
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

    let retained = ctx
        .messenger
        .store_for_address(FRAMES)
        .replay_from(None, Depth::Bounded(512))
        .await
        .messages;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].message.body, body);
    assert_eq!(retained[0].message.body.content_type(), Some("image/png"));
}

/// A non-text batch entry, published now and parked for later, keeps its bytes
/// and its content type on both paths.
#[tokio::test]
async fn a_v5_non_text_batch_entry_is_stored_and_parked_with_its_type() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_v5(&db).await;
    let mut counters = SessionCounters::default();
    let now_body = png(3);
    let later_body = MessageBody::from_parts(Some("image/jpeg"), &[0x00, 0xd8, 0xff]).unwrap();
    let later = (Utc::now() + chrono::Duration::hours(1)).timestamp_millis() as u64;
    let publishes = [
        BatchEntry {
            body: now_body.clone(),
            ..entry(FRAMES, "", None)
        },
        BatchEntry {
            body: later_body.clone(),
            ..entry(FRAMES, "", Some(later))
        },
    ];

    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);

    let retained = ctx
        .messenger
        .store_for_address(FRAMES)
        .replay_from(None, Depth::Bounded(512))
        .await
        .messages;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].message.body, now_body);
    let parked = ctx
        .messenger
        .deferred_view_for_sender(FRAMES, sender_of(Some(SUB)).as_str())
        .await;
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].envelope.body, later_body);
    assert_eq!(parked[0].envelope.body.content_type(), Some("image/jpeg"));
}

/// A deferred edit carrying a non-text body replaces the parked body with it,
/// content type and all.
#[tokio::test]
async fn a_v5_non_text_edit_rewrites_the_parked_body_with_its_type() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_v5(&db).await;
    let mut counters = SessionCounters::default();
    let parked_id = park(&ctx, FRAMES, r#"{"v":1}"#).await;
    let body = png(5);

    let ops = [BatchDeferredOp {
        channel: FRAMES.to_string(),
        message_id: parked_id,
        op: DeferredOpKind::Edit {
            body: Some(body.clone()),
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
        .deferred_view_for_sender(FRAMES, sender_of(Some(SUB)).as_str())
        .await;
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].envelope.body, body);
    assert_eq!(parked[0].envelope.body.content_type(), Some("image/png"));
}

// ── The text-only refusal ────────────────────────────────────────────────────

/// A non-text publish to a text-only channel is answered `TextOnlyChannel`:
/// nothing is stored, the connection token is spent (so a client looping on the
/// mistake is metered), the counter moves and the first refusal is logged with
/// the body's summary, never its bytes.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_v5_non_text_publish_to_a_text_only_channel_is_refused() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_v5(&db).await;
    let mut bucket = TokenBucket::new(2, Duration::from_secs(3600), 2);
    let mut counters = SessionCounters::default();
    let body = png(4);

    for _ in 0..2 {
        assert!(matches!(
            handle_publish(
                &ctx,
                &mut bucket,
                request(OUT, Some(SUB), &body),
                &mut counters
            )
            .await,
            FrameOutcome::Continue
        ));
        assert_eq!(published_outcome(&mut rx), PublishOutcome::TextOnlyChannel);
    }
    assert_eq!(counters.publish_text_only_refused, 2);
    assert_eq!(counters.publishes, 0);
    assert!(bodies(&ctx, OUT).await.is_empty(), "nothing is stored");
    assert!(logs_contain("answering TextOnlyChannel"));
    assert!(logs_contain("image/png, 4 bytes"));

    // Both refusals spent a connection token.
    assert!(matches!(
        handle_publish(
            &ctx,
            &mut bucket,
            request(OUT, Some(SUB), &"{}".into()),
            &mut counters
        )
        .await,
        FrameOutcome::Continue
    ));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::RateLimited);
}

/// Typed text is text: a v5 session publishes it onto a text-only channel and
/// it is stored with its content type.
#[tokio::test]
async fn a_v5_typed_text_publish_to_a_text_only_channel_keeps_its_type() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_v5(&db).await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();
    let body = MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap();

    assert!(matches!(
        handle_publish(
            &ctx,
            &mut bucket,
            request(OUT, Some(SUB), &body),
            &mut counters
        )
        .await,
        FrameOutcome::Continue
    ));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::Ok);
    assert_eq!(counters.publish_text_only_refused, 0);
    let retained = ctx
        .messenger
        .store_for_address(OUT)
        .replay_from(None, Depth::Bounded(512))
        .await
        .messages;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].message.body, body);
    assert_eq!(
        retained[0].message.body.content_type(),
        Some("text/markdown")
    );
}

/// One refused entry refuses the whole flush: the text entry beside it is not
/// published, nothing parks, no view is restated — and the send budget is drawn
/// anyway, so the next flush finds it spent.
#[tokio::test]
async fn a_v5_batch_with_a_refused_entry_applies_nothing_and_draws_the_budget() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_at_version(&db, profile(), true, 2, MAX_BODY, 5).await;
    let mut counters = SessionCounters::default();
    let (mut sibling, _guard) = sibling_view_queue(&ctx);
    let later = (Utc::now() + chrono::Duration::hours(1)).timestamp_millis() as u64;
    let publishes = [
        entry(OUT, r#"{"n":1}"#, Some(later)),
        BatchEntry {
            body: png(2),
            ..entry(OUT, "", None)
        },
    ];

    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::TextOnlyChannel);
    assert_eq!(counters.publish_text_only_refused, 1);
    assert_eq!(counters.publishes, 0);
    assert!(bodies(&ctx, OUT).await.is_empty());
    assert!(
        ctx.messenger
            .deferred_view_for_sender(OUT, sender_of(Some(SUB)).as_str())
            .await
            .is_empty(),
        "nothing parked"
    );
    assert!(sibling.try_recv().is_err(), "no view restated");

    let publishes = [entry(OUT, "{}", None)];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &[]), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(
        batch_outcome(&mut rx),
        PublishBatchOutcome::RateLimited,
        "the refused flush drew both tokens"
    );
}

/// A refused edit refuses the flush the same way: the cancel ahead of it is
/// not applied and the parked bodies are untouched.
#[tokio::test]
async fn a_v5_batch_with_a_refused_edit_applies_nothing() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_v5(&db).await;
    let mut counters = SessionCounters::default();
    let kept = park(&ctx, OUT, r#"{"v":1}"#).await;
    let edited = park(&ctx, OUT, r#"{"v":2}"#).await;
    let (mut sibling, _guard) = sibling_view_queue(&ctx);

    let ops = [
        cancel(OUT, kept),
        BatchDeferredOp {
            channel: OUT.to_string(),
            message_id: edited,
            op: DeferredOpKind::Edit {
                body: Some(png(3)),
                deliver_after: None,
            },
        },
    ];
    assert!(matches!(
        handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::TextOnlyChannel);
    assert_eq!(counters.publish_text_only_refused, 1);
    let parked: Vec<_> = ctx
        .messenger
        .deferred_view_for_sender(OUT, sender_of(Some(SUB)).as_str())
        .await
        .into_iter()
        .map(|message| message.envelope.body.clone())
        .collect();
    assert_eq!(
        parked,
        vec![
            MessageBody::from(r#"{"v":1}"#),
            MessageBody::from(r#"{"v":2}"#)
        ]
    );
    assert!(sibling.try_recv().is_err(), "no view restated");
}

/// Under a drained budget the same refused flush answers `RateLimited`: the
/// draw comes first, as for any flush.
#[tokio::test]
async fn a_v5_refused_batch_under_a_drained_budget_answers_rate_limited() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_at_version(&db, profile(), true, 1, MAX_BODY, 5).await;
    let mut counters = SessionCounters::default();
    let refused = [BatchEntry {
        body: png(2),
        ..entry(OUT, "", None)
    }];

    for expected in [
        PublishBatchOutcome::TextOnlyChannel,
        PublishBatchOutcome::RateLimited,
    ] {
        assert!(matches!(
            handle_publish_batch(&ctx, batch(Some(SUB), &refused, &[]), &mut counters).await,
            FrameOutcome::Continue
        ));
        assert_eq!(batch_outcome(&mut rx), expected);
    }
    assert_eq!(counters.publish_text_only_refused, 1);
    assert_eq!(counters.publish_rate_limited, 1);
}

// ── A target the directory does not hold ─────────────────────────────────────

/// Take `channel` out of the directory, as a deprovision or the first half of a
/// reload's retune does; the profile still grants it.
fn deprovision(ctx: &AttachSessionCtx, channel: &str) {
    let uuid = ctx
        .messenger
        .directory()
        .resolve(channel)
        .expect("the fixture channel is in the directory")
        .uuid;
    assert!(ctx.messenger.directory().remove_channel(&uuid));
}

/// A non-text publish to a granted channel the directory does not hold is
/// answered as the bus answers an absent channel — `Failed` under the
/// diagnostic posture — and is never offered to the bus, whose own later read
/// could find an entry the door never checked.
#[tokio::test]
async fn a_v5_non_text_publish_to_an_absent_channel_is_answered_as_absent() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_at_version(
        &db,
        TestProfile {
            diagnostic: Some(FRAMES.to_string()),
            ..profile()
        },
        true,
        64,
        MAX_BODY,
        5,
    )
    .await;
    let mut bucket = wide_bucket();
    let mut counters = SessionCounters::default();
    deprovision(&ctx, FRAMES);

    let outcome = handle_publish(
        &ctx,
        &mut bucket,
        request(FRAMES, Some(SUB), &png(3)),
        &mut counters,
    )
    .await;
    assert!(matches!(outcome, FrameOutcome::Continue));
    assert_eq!(published_outcome(&mut rx), PublishOutcome::Failed);
    assert_eq!(counters.publishes, 0);
    assert_eq!(counters.publish_text_only_refused, 0);
}

/// Under `Race`, a batch's non-text entry and non-text edit on a channel the
/// door found absent are the bus's answers for a target that is gone: the entry
/// is dropped and warned about, the edit is a no-op, and the rest of the flush
/// lands.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_v5_batch_drops_non_text_bodies_for_an_absent_channel_under_race() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_at_version(
        &db,
        TestProfile {
            missing_channel_posture: MissingChannelPosture::Race,
            ..profile()
        },
        true,
        64,
        4 * MAX_BODY,
        5,
    )
    .await;
    let mut counters = SessionCounters::default();
    deprovision(&ctx, FRAMES);

    let publishes = [
        BatchEntry {
            body: png(2),
            ..entry(FRAMES, "", None)
        },
        entry(ERRORS, r#"{"here":true}"#, None),
    ];
    let ops = [BatchDeferredOp {
        channel: FRAMES.to_string(),
        message_id: Uuid::new_v4(),
        op: DeferredOpKind::Edit {
            body: Some(png(3)),
            deliver_after: None,
        },
    }];
    match handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &ops), &mut counters).await {
        FrameOutcome::Continue => {}
        FrameOutcome::Violation(detail) => panic!("the flush must not violate: {detail}"),
        FrameOutcome::Disconnect => panic!("the flush must not disconnect"),
    }
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    assert_eq!(counters.publishes, 1, "the dropped entry published nothing");
    assert_eq!(counters.publish_text_only_refused, 0);
    assert_eq!(bodies(&ctx, ERRORS).await, vec![r#"{"here":true}"#]);
    assert!(logs_contain(
        "a non-text body for a target the directory does not hold"
    ));
    assert!(logs_contain("the directory does not hold its channel"));
}

/// Under `Race`, a cancel, a text edit and a schedule aimed at a granted channel
/// the directory does not hold leave the attachment alive: the ops are lost
/// races, the schedule is a vanished entry, and no view of the absent channel
/// is restated. Text bodies, so a v4 session reaches it.
#[tokio::test]
async fn control_ops_and_schedules_on_an_absent_channel_are_no_ops_under_race() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, mut rx) = attach_with_body_cap(
        &db,
        TestProfile {
            missing_channel_posture: MissingChannelPosture::Race,
            ..profile()
        },
        true,
        64,
        4 * MAX_BODY,
    )
    .await;
    let mut counters = SessionCounters::default();
    let (mut sibling, _guard) = sibling_view_queue(&ctx);
    deprovision(&ctx, OUT);
    let later = (Utc::now() + chrono::Duration::hours(1)).timestamp_millis() as u64;

    let publishes = [
        entry(OUT, r#"{"parked":true}"#, Some(later)),
        entry(ERRORS, r#"{"here":true}"#, None),
    ];
    let ops = [
        cancel(OUT, Uuid::new_v4()),
        BatchDeferredOp {
            channel: OUT.to_string(),
            message_id: Uuid::new_v4(),
            op: DeferredOpKind::Edit {
                body: Some("{}".into()),
                deliver_after: None,
            },
        },
    ];
    match handle_publish_batch(&ctx, batch(Some(SUB), &publishes, &ops), &mut counters).await {
        FrameOutcome::Continue => {}
        FrameOutcome::Violation(detail) => panic!("the flush must not violate: {detail}"),
        FrameOutcome::Disconnect => panic!("the flush must not disconnect"),
    }
    assert_eq!(batch_outcome(&mut rx), PublishBatchOutcome::Ok);
    assert_eq!(counters.publishes, 1);
    assert_eq!(bodies(&ctx, ERRORS).await, vec![r#"{"here":true}"#]);
    assert!(sibling.try_recv().is_err(), "no view of an absent channel");
}

/// Under `Invariant` the same cancel is a broken boot invariant.
#[tokio::test]
#[should_panic(expected = "the directory does not hold")]
async fn a_control_op_on_an_absent_channel_panics_under_invariant() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (ctx, _rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    deprovision(&ctx, OUT);
    let ops = [cancel(OUT, Uuid::new_v4())];
    handle_publish_batch(&ctx, batch(Some(SUB), &[], &ops), &mut counters).await;
}

// ── Deferred views per session version ───────────────────────────────────────

/// A view of one text entry, one typed-text entry and one non-text entry.
fn mixed_view() -> crate::registry::DeferredViewPush {
    let entry = |n: u128, body: MessageBody| DeferredViewEntry {
        message_id: Uuid::from_u128(n),
        body,
        deliver_after: 1_700_000_000_000,
    };
    crate::registry::DeferredViewPush {
        channel: FRAMES.to_string(),
        attribution: Some(SUB.to_string()),
        entries: vec![
            entry(1, r#"{"v":1}"#.into()),
            entry(2, png(3)),
            entry(
                3,
                MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap(),
            ),
        ],
    }
}

/// The entries of the one `DeferredView` frame `rx` holds.
fn view_frame(rx: &mut mpsc::Receiver<ServerFrame>) -> Vec<DeferredViewEntry> {
    match rx.try_recv().expect("a DeferredView frame") {
        ServerFrame::DeferredView {
            channel,
            attribution,
            entries,
        } => {
            assert_eq!(channel, FRAMES);
            assert_eq!(attribution.as_deref(), Some(SUB));
            entries
        }
        other => panic!("expected DeferredView, got {other:?}"),
    }
}

/// A pushed view reaches a v4 session without the entries v4 cannot carry —
/// typed text stays — and a v5 session with all of them. Each omission is
/// counted; the first per view on a session is logged.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_pushed_view_is_cut_to_what_the_sessions_version_carries() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (v4, mut v4_rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    for _ in 0..2 {
        assert!(matches!(
            crate::subscription::send_deferred_view(&v4, mixed_view(), &mut counters).await,
            FrameOutcome::Continue
        ));
        let ids: Vec<_> = view_frame(&mut v4_rx)
            .iter()
            .map(|entry| entry.message_id)
            .collect();
        assert_eq!(ids, vec![Uuid::from_u128(1), Uuid::from_u128(3)]);
    }
    assert_eq!(counters.deferred_view_entries_omitted, 2);
    logs_assert(|lines| {
        match lines
            .iter()
            .filter(|line| line.contains("left out for a session"))
            .count()
        {
            1 => Ok(()),
            n => Err(format!("one omission line per view per session, got {n}")),
        }
    });

    let db = brenn_messaging_store::db::init_db_memory();
    let (v5, mut v5_rx) = attach_v5(&db).await;
    let mut counters = SessionCounters::default();
    assert!(matches!(
        crate::subscription::send_deferred_view(&v5, mixed_view(), &mut counters).await,
        FrameOutcome::Continue
    ));
    assert_eq!(view_frame(&mut v5_rx), mixed_view().entries);
    assert_eq!(counters.deferred_view_entries_omitted, 0);
}

/// A view a v4 session can see none of is still written, empty: each view
/// replaces the last, so withholding it would leave a stale mirror standing.
#[tokio::test]
async fn a_pushed_view_cut_to_nothing_is_written_empty() {
    let db = brenn_messaging_store::db::init_db_memory();
    let (v4, mut rx) = attach(&db).await;
    let mut counters = SessionCounters::default();
    let mut view = mixed_view();
    view.entries.retain(|entry| !entry.body.is_text());

    assert!(matches!(
        crate::subscription::send_deferred_view(&v4, view, &mut counters).await,
        FrameOutcome::Continue
    ));
    assert!(view_frame(&mut rx).is_empty());
}

/// Seeding cuts the same way: a v4 session is seeded with the text entries of
/// a set that also holds non-text ones, and is sent no frame for a set it can
/// see none of; a v5 session is seeded with everything.
#[tokio::test]
async fn seeding_is_cut_to_what_the_sessions_version_carries() {
    for version in [4, 5] {
        let db = brenn_messaging_store::db::init_db_memory();
        let mut profile = profile();
        profile.deferred_targets = vec![
            DeferredTarget {
                channel: FRAMES.to_string(),
                attribution: Some(SUB.to_string()),
            },
            DeferredTarget {
                channel: FRAMES.to_string(),
                attribution: None,
            },
        ];
        let (ctx, mut rx) = attach_at_version(&db, profile, true, 64, MAX_BODY, version).await;
        let mut counters = SessionCounters::default();
        let text = park(&ctx, FRAMES, r#"{"v":1}"#).await;
        let binary = park(&ctx, FRAMES, png(3)).await;
        let bare_binary = park_under(&ctx, FRAMES, None, png(5)).await;

        assert!(matches!(
            seed_deferred_views(&ctx, &mut counters).await,
            FrameOutcome::Continue
        ));

        let mut seen = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            let ServerFrame::DeferredView {
                attribution,
                entries,
                ..
            } = frame
            else {
                panic!("expected DeferredView, got {frame:?}");
            };
            seen.push((
                attribution,
                entries.iter().map(|e| e.message_id).collect::<Vec<_>>(),
            ));
        }
        seen.sort();
        let expected = match version {
            4 => vec![(Some(SUB.to_string()), vec![text])],
            _ => vec![
                (None, vec![bare_binary]),
                (Some(SUB.to_string()), vec![text, binary]),
            ],
        };
        assert_eq!(seen, expected, "at v{version}");
        assert_eq!(
            counters.deferred_view_entries_omitted,
            if version == 4 { 2 } else { 0 },
        );
    }
}

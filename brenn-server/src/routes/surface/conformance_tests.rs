//! Conformance by construction: a whole attacher that is not a browser, driven
//! against the real route over a real socket.
//!
//! `brenn-attach-conformance` is built on the attachment protocol and the
//! attacher-generic client and nothing else — no surface crate appears anywhere
//! in its dependency graph, so it cannot name a component, a port, a mount, or a
//! pixel even by accident. What it can nevertheless do here is the whole
//! attachment: negotiate, subscribe, publish, be delivered, lose its transport
//! to a severed socket, reconnect, and resume each subscription from the cursor
//! it held. That is the demonstration — the protocol carries no browser
//! assumptions, shown by an attacher with no browser to make them from.
//!
//! The surface route is the door it comes through because a route is what exists
//! to be attached to; nothing in the client knows which one it is. The same
//! attacher runs against the token-authenticated remote route in
//! `routes::remote`'s conformance suite: a different connector, and nothing
//! below it differs.
//!
//! What this suite is *not*: a second copy of the frame-semantics suites. Cursor
//! grammar, gap minting, violation handling and the publish authority matrix are
//! pinned beside the code that decides them (`brenn-attach-server`'s suites, and
//! `ws_tests.rs` for the route's own wiring). This one asks a narrower question,
//! end to end.

use brenn_attach_conformance::relay::SeverableRelay;
use brenn_attach_conformance::{
    AttachClient, ClientConfig, Credential, DeferredOpKind, Observation, PublishBatchOutcome,
    PublishRequest, ResumePolicy, SubscriptionDepths,
};
use brenn_attach_proto::{PublishOutcome, Urgency};
use brenn_messaging::testutils::ephemeral_channel_entry;
use std::time::Duration;

use brenn_surface_server::test_fixtures::{COMPONENT, EPH_ADDR, EPH_NAME, deskbar_loop};

use super::test_fixtures::{SurfaceTestHarness, surface_harness};
use crate::test_support::TEST_BUILD_ID;
use crate::test_support::http::{
    TestServer, http_base_addr, setup_authenticated_user, spawn_test_server,
};

/// The depths this suite subscribes at. Both knobs stated, as the protocol
/// requires; the peer clamps them to what its own configuration resolved.
const DEPTHS: SubscriptionDepths = SubscriptionDepths {
    push_depth: 4,
    retain_depth: 4,
};

/// Everything a run needs standing up: a booted surface, an authenticated
/// account, a live server, and a severable relay in front of it.
///
/// Held whole by each test rather than destructured, because two of the three
/// fields are alive-or-not: dropping the server stops it, and dropping the relay
/// cuts every pair it is carrying.
struct Rig {
    client: AttachClient,
    relay: SeverableRelay,
    _server: TestServer,
}

async fn build_rig(db: &brenn_db::Db, retain_depth: u64) -> Rig {
    let SurfaceTestHarness { state, .. } = surface_harness(
        db,
        deskbar_loop(vec![]),
        vec![ephemeral_channel_entry(EPH_NAME, retain_depth)],
    )
    .await;
    let (token, _) = setup_authenticated_user(db).await;
    let (base, server) = spawn_test_server(state).await;
    let relay = SeverableRelay::spawn(http_base_addr(&base)).await;
    let client = AttachClient::new(ClientConfig {
        // The served-asset build check is the surface route's own, composed into
        // the URL by whatever attaches — exactly as the page kernel composes it.
        url: format!(
            "ws://{}/surface/deskbar/ws?build={TEST_BUILD_ID}",
            relay.addr()
        ),
        credential: Credential::SessionCookie(token),
        ident: "attach-conformance".to_string(),
    });
    Rig {
        client,
        relay,
        _server: server,
    }
}

/// One publish under the component's attribution.
fn publish(body: &str) -> PublishRequest {
    PublishRequest {
        channel: EPH_ADDR.to_string(),
        attribution: Some(COMPONENT.to_string()),
        body: body.to_string(),
        urgency: Urgency::Normal,
    }
}

/// Assert the next thing the attacher observes is the loss of its transport.
async fn expect_detach(client: &mut AttachClient) {
    match client.next_observation().await {
        Observation::Detached { .. } => {}
        other => panic!("expected the severed socket to detach the attachment, got {other:?}"),
    }
}

/// **The whole attachment, from a client with no surface crate under it.**
///
/// Negotiate, subscribe, publish, be delivered the peer's stamped envelope, lose
/// the socket to a severed relay, reconnect on the backoff, and resume: the
/// resumed subscribe carries the cursor of the last delivery accepted, so the
/// peer replays nothing already seen and the next message continues the stream
/// rather than repeating it.
#[tokio::test]
async fn a_non_browser_attacher_subscribes_publishes_and_resumes_across_a_severed_socket() {
    let db = crate::test_support::init_db_memory();
    // Retained depth 4, so a *cursorless* resubscribe here would replay the
    // message already seen. That is what makes `replay_count == 0` after the
    // sever evidence that the cursor was presented and honoured.
    let mut rig = build_rig(&db, 4).await;

    let facts = rig.client.attach().await;
    assert_eq!(
        facts.participant_id, "surface:deskbar",
        "the attachment speaks as the principal, before any attribution"
    );
    assert_eq!(
        facts.version,
        brenn_attach_proto::SUPPORTED_VERSIONS.max,
        "the only version either end speaks"
    );
    assert!(
        !facts.alert_granted,
        "the fixture grants no alert plane, and a conforming attacher is told so"
    );

    let ack = rig
        .client
        .subscribe(EPH_ADDR, DEPTHS, ResumePolicy::Resume)
        .await;
    assert_eq!(ack.replay_count, 0, "nothing is retained yet");
    assert!(ack.gap.is_none());
    assert!(ack.live);

    assert_eq!(rig.client.publish(publish("one")).await, PublishOutcome::Ok);
    let first = rig.client.next_delivery(EPH_ADDR).await;
    assert_eq!(first.body, "one");
    assert_eq!(
        first.sender, "surface:deskbar#protobar",
        "the attribution mints the component's sub-identity, server-side"
    );
    assert_eq!(first.seq, 1, "the first delivery of the span");
    assert_eq!(first.dropped, 0);

    rig.relay.sever();
    expect_detach(&mut rig.client).await;
    // Nothing here reopens anything: the driver's own backoff schedule is what
    // reconnects, and this waits it out.
    let resumed = rig.client.attach().await;
    assert_eq!(resumed.participant_id, "surface:deskbar");

    let ack = rig.client.next_subscribe_ack(EPH_ADDR).await;
    assert_eq!(
        ack.replay_count, 0,
        "the resumed cursor covers the retained message, so nothing is replayed"
    );
    assert!(
        ack.gap.is_none(),
        "an in-window resume on the same epoch is not a gap"
    );

    assert_eq!(rig.client.publish(publish("two")).await, PublishOutcome::Ok);
    let second = rig.client.next_delivery(EPH_ADDR).await;
    assert_eq!(
        second.body, "two",
        "the stream continues past the resume point rather than repeating it"
    );
    assert_eq!(
        second.seq, 1,
        "a span's sequence restarts at each subscribe"
    );

    rig.client.close().await;
}

/// **The cursorless posture, which is how retained state is re-applied.**
///
/// A subscription that never presents a resume claim is answered with the
/// retained window again at every attachment — the property a channel carrying
/// state depends on, and the exact contrast with the run above across the same
/// severed socket.
#[tokio::test]
async fn a_cursorless_subscription_is_replayed_the_retained_window_at_every_attachment() {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, 4).await;

    rig.client.attach().await;
    let ack = rig
        .client
        .subscribe(EPH_ADDR, DEPTHS, ResumePolicy::Cursorless)
        .await;
    assert_eq!(ack.replay_count, 0, "nothing is retained yet");

    assert_eq!(
        rig.client.publish(publish("state")).await,
        PublishOutcome::Ok
    );
    assert_eq!(rig.client.next_delivery(EPH_ADDR).await.body, "state");

    rig.relay.sever();
    expect_detach(&mut rig.client).await;
    rig.client.attach().await;

    let ack = rig.client.next_subscribe_ack(EPH_ADDR).await;
    assert_eq!(
        ack.replay_count, 1,
        "no cursor is claimed, so the retained window comes again"
    );
    assert_eq!(
        rig.client.next_delivery(EPH_ADDR).await.body,
        "state",
        "and the retained message is what arrives"
    );

    rig.client.close().await;
}

/// How far ahead the parked tick is scheduled, and how long the run then waits
/// for it to come due. Both short, because nothing here is racing a release —
/// this rig runs no release pass at all, so once the instant passes the entry
/// stays parked for as long as the test wants it to.
const PARK_AHEAD_MS: u64 = 150;
const PAST_THE_INSTANT: Duration = Duration::from_millis(400);

/// The registrant this suite flushes under: one outbox per component instance,
/// which is the grain an activation's flush has.
const REGISTRANT: &str = COMPONENT;

/// Wall clock in the units a `deliver_after` is stated in.
fn now_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).expect("a positive epoch")
}

/// **A parked message past its release instant is shown, and is not actionable.**
///
/// The deferred window is every entry the sender parked that no release pass has
/// taken — `deliver_after` says when it is owed, not whether it is still held —
/// so an empty set means nothing is standing, at every instant. The other half of
/// that rule is the cutoff the authority keeps: an entry the view shows as
/// already due cannot be cancelled or edited, and an attacher that names one gets
/// the benign no-op rather than a severed socket.
///
/// Both halves are asserted here through the wire, from a client with no surface
/// crate under it: park a tick, let its instant pass with nothing to release it,
/// lose the socket, and read the set the peer seeds behind `Welcome`.
#[tokio::test]
async fn a_due_but_unreleased_entry_is_seeded_at_welcome_and_a_cancel_naming_it_is_a_no_op() {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, 4).await;

    rig.client.attach().await;
    rig.client
        .subscribe(EPH_ADDR, DEPTHS, ResumePolicy::Resume)
        .await;
    rig.client.register_outbox(REGISTRANT, Some(COMPONENT), 4);

    let deliver_after = now_ms() + PARK_AHEAD_MS;
    rig.client
        .flush(
            REGISTRANT,
            AttachClient::parking(EPH_ADDR, "tick", Some(deliver_after)),
        )
        .await;
    assert_eq!(
        rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok,
        "the flush applies: one entry, parked"
    );

    let parked = rig
        .client
        .next_deferred_view(EPH_ADDR, Some(COMPONENT))
        .await;
    assert_eq!(parked.len(), 1, "one message is standing on the channel");
    assert_eq!(parked[0].body, "tick");
    assert_eq!(
        parked[0].deliver_after, deliver_after,
        "the mirror states the instant the attacher asked for"
    );

    // Nothing releases it: the entry comes due where it is parked.
    tokio::time::sleep(PAST_THE_INSTANT).await;
    rig.relay.sever();
    expect_detach(&mut rig.client).await;
    rig.client.attach().await;

    let seeded = rig
        .client
        .next_deferred_view(EPH_ADDR, Some(COMPONENT))
        .await;
    assert_eq!(
        seeded, parked,
        "an entry no release pass has taken is still the sender's, past instant and all"
    );
    assert!(
        seeded[0].deliver_after < now_ms(),
        "and the instant it carries has passed, which is what makes it unactionable"
    );
    assert_eq!(
        rig.client.parked_view(EPH_ADDR, Some(COMPONENT)),
        seeded,
        "the client's own mirror is what the seeding wrote, cleared at the attach before it"
    );

    rig.client
        .flush(
            REGISTRANT,
            AttachClient::controlling(EPH_ADDR, &seeded[0], DeferredOpKind::Cancel),
        )
        .await;
    assert_eq!(
        rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok,
        "a legal frame naming a due entry is applied as a batch; the op inside it is the no-op"
    );

    let restated = rig
        .client
        .next_deferred_view(EPH_ADDR, Some(COMPONENT))
        .await;
    assert_eq!(
        restated, seeded,
        "the cancel took nothing, and the view is restated so the attacher sees that"
    );
    assert!(
        rig.client.is_attached(),
        "a no-op is not a violation: the socket is still up"
    );

    rig.client.close().await;
}

/// Pump observations until one of `registrant`'s flushes is reported dropped.
///
/// Bounded rather than looping forever: every other observation on the way is
/// legitimate, and a terminal attachment can never produce this one.
async fn expect_flush_dropped(client: &mut AttachClient, registrant: &str) {
    for _ in 0..16 {
        match client.next_observation().await {
            Observation::FlushDropped { registrant: named } if named == registrant => return,
            Observation::Terminal(cause) => {
                panic!("the attachment ended before the drop was reported: {cause:?}")
            }
            _ => {}
        }
    }
    panic!("no flush of {registrant}'s was reported dropped");
}

/// **A flush offered while there is no wire is held, not lost, and is applied
/// exactly once at the next attachment.**
///
/// The outbox plane is what makes an activation's flush atomic across an
/// outage: a component publishes when its activation ends, not when the socket
/// happens to be up. Nothing of the flush reaches the peer while the transport
/// is gone — the first thing observed after it is the reconnect — and the
/// attachment that follows carries it once, which the parked set the peer seeds
/// is the end-to-end witness of.
#[tokio::test]
async fn a_flush_offered_while_detached_is_sent_once_at_the_next_attachment() {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, 4).await;

    rig.client.attach().await;
    rig.client
        .subscribe(EPH_ADDR, DEPTHS, ResumePolicy::Resume)
        .await;
    rig.client.register_outbox(REGISTRANT, Some(COMPONENT), 4);

    rig.relay.sever();
    expect_detach(&mut rig.client).await;

    // Offered with no wire under it: the plane holds the whole flush.
    rig.client
        .flush(
            REGISTRANT,
            AttachClient::parking(EPH_ADDR, "held", Some(now_ms() + 10_000)),
        )
        .await;
    match rig.client.next_observation().await {
        Observation::Attached(_) => {}
        other => panic!("nothing may reach the peer before the reconnect, got {other:?}"),
    }

    assert_eq!(
        rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok,
        "the held flush is sent once the wire is back",
    );
    let parked = rig
        .client
        .next_deferred_view(EPH_ADDR, Some(COMPONENT))
        .await;
    assert_eq!(
        parked.iter().map(|e| e.body.as_str()).collect::<Vec<_>>(),
        ["held"],
        "applied exactly once: a re-send would park it twice",
    );

    rig.client.close().await;
}

/// **A registrant's depth is a cap on whole flushes, and reaching it is said
/// out loud.**
///
/// The plane bounds what one registrant may hold across an outage, and the
/// overflow takes the oldest flush — a drop, which is the one thing a publisher
/// is never allowed to discover by absence. The surviving flush is the newest,
/// and it is the only one the peer ever sees.
#[tokio::test]
async fn a_flush_past_a_registrants_depth_is_dropped_and_named() {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, 4).await;

    rig.client.attach().await;
    rig.client
        .subscribe(EPH_ADDR, DEPTHS, ResumePolicy::Resume)
        .await;
    rig.client.register_outbox(REGISTRANT, Some(COMPONENT), 1);

    rig.relay.sever();
    expect_detach(&mut rig.client).await;

    let deliver_after = now_ms() + 10_000;
    rig.client
        .flush(
            REGISTRANT,
            AttachClient::parking(EPH_ADDR, "first", Some(deliver_after)),
        )
        .await;
    rig.client
        .flush(
            REGISTRANT,
            AttachClient::parking(EPH_ADDR, "second", Some(deliver_after)),
        )
        .await;
    expect_flush_dropped(&mut rig.client, REGISTRANT).await;

    assert_eq!(
        rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok,
        "the flush that survived the cap is sent at the reconnect",
    );
    let parked = rig
        .client
        .next_deferred_view(EPH_ADDR, Some(COMPONENT))
        .await;
    assert_eq!(
        parked.iter().map(|e| e.body.as_str()).collect::<Vec<_>>(),
        ["second"],
        "the cap dropped the oldest flush, and only the newest reached the peer",
    );

    rig.client.close().await;
}

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
//! to be attached to; nothing in the client knows which one it is. A future
//! token-authenticated route swaps the connector and changes nothing below it.
//!
//! What this suite is *not*: a second copy of the frame-semantics suites. Cursor
//! grammar, gap minting, violation handling and the publish authority matrix are
//! pinned beside the code that decides them (`routes::attach`'s suites, and
//! `ws_tests.rs` for the route's own wiring). This one asks a narrower question,
//! end to end.

use brenn_attach_conformance::relay::SeverableRelay;
use brenn_attach_conformance::{
    AttachClient, ClientConfig, Observation, PublishRequest, ResumePolicy, SubscriptionDepths,
};
use brenn_attach_proto::{PublishOutcome, Urgency};
use brenn_lib::db;
use brenn_lib::messaging::testutils::ephemeral_channel_entry;

use super::test_fixtures::{
    COMPONENT, EPH_ADDR, EPH_NAME, SurfaceTestHarness, deskbar_loop, surface_harness,
};
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

async fn build_rig(db: &db::Db, retain_depth: u64) -> Rig {
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
        session_cookie: token,
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
    let db = db::init_db_memory();
    // Retained depth 4, so a *cursorless* resubscribe here would replay the
    // message already seen. That is what makes `replay_count == 0` after the
    // sever evidence that the cursor was presented and honoured.
    let mut rig = build_rig(&db, 4).await;

    let facts = rig.client.attach().await;
    assert_eq!(
        facts.participant_id, "surface:deskbar",
        "the attachment speaks as the principal, before any attribution"
    );
    assert_eq!(facts.version, 1, "the only version either end speaks");
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
    let db = db::init_db_memory();
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

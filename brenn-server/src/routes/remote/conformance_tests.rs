//! The second door, walked by the same attacher.
//!
//! `brenn-attach-conformance` is a whole client built on the attachment protocol
//! and the attacher-generic client and nothing else. `routes::surface`'s
//! conformance suite drives it through the browser route; this one drives it
//! through `/remote/{slug}/ws` with a bearer token in place of a session cookie.
//! Swapping the connector is the entire client-side difference, which is the
//! claim the two suites make together.
//!
//! What the run proves that the route's own frame-level suites cannot: that a
//! daemon reaches the bus through matcher-granted authority — channels the
//! operator described by prefix rather than enumerated at boot — and that the
//! machinery this route needed and the surface did not (the runtime-minted
//! directory entry, the `Unavailable` settlement, the active-subscription cap,
//! the non-fatal publish posture) behaves as a client experiences it.
//!
//! What this suite is *not*: a second copy of the auth ladder or the capacity
//! gate, which are the HTTP edge's and live in `ws_tests`; nor of the frame
//! semantics, which are the shared session's and live beside it in
//! `brenn-attach-server`.

use std::sync::Arc;
use std::time::Duration;

use brenn_attach_conformance::relay::SeverableRelay;
use brenn_attach_conformance::{
    AttachClient, BatchEntry, ClientConfig, Credential, DeferredOpKind, Delivery, FlushBatch,
    MessageBody, Observation, PublishBatchOutcome, PublishRequest, ResumePolicy,
    SubscribeSettlement, SubscriptionDepths,
};
use brenn_attach_proto::{
    AlertSeverity, PublishOutcome, SUPPORTED_VERSIONS, Urgency, VersionRange,
};
use brenn_envelope::chat::{ChatRoster, decode as chat_decode};
use std::path::Path;

use brenn_lib::access::raw::ChannelMatcherRaw;
use brenn_lib::config::remote_fleet;
use brenn_lib::messaging::AttachGrant;
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::remote::RemoteConfigRaw;
use brenn_lib::messaging::{ChannelEntry, SubscriberEntry, SubscriberEntryKind};
use brenn_messaging::PublishResult;
use brenn_messaging::testutils::{ephemeral_channel_entry, test_channel_entry};

use super::test_fixtures::{
    APP, OWNER, RemoteTestHarness, SLUG, TOKEN, remote_harness_with_channels,
};
use crate::test_support::http::{TestServer, http_base_addr, spawn_test_server};

/// The rig's `[[remote]]`: the fleet-driver shape of `test_fixtures::FLEET` with
/// publish rights added on the two outbound prefixes.
///
/// Not how an operator wires a real fleet — a driver reads `out.`/`stream.` and
/// writes `in.`/`wake.`, and the chat adapter is what answers on the outbound
/// leaves. But a conformance run has no chat adapter behind it, and the
/// alternative is asserting delivery against rows this suite reached around the
/// wire to insert. Both directions on one prefix is legal config, and it keeps
/// every fact these tests read a fact the attacher established through frames.
fn loopback(token_file: &Path) -> RemoteConfigRaw {
    RemoteConfigRaw {
        publish_acl: vec![
            ChannelMatcherRaw::Prefix("chat.app.home.out.".to_string()),
            ChannelMatcherRaw::Prefix("chat.app.home.in.".to_string()),
        ],
        ephemeral_publish_acl: vec![
            ChannelMatcherRaw::Prefix("chat.app.home.stream.".to_string()),
            ChannelMatcherRaw::Prefix("chat.app.home.wake.".to_string()),
        ],
        ..remote_fleet(token_file)
    }
}

/// Granted exactly, at 1/1: the fleet roster, a state channel.
const ROSTER_BARE: &str = "chat.app.home.roster";
const ROSTER: &str = "brenn:chat.app.home.roster";
/// Granted under the `out.` prefix: one conversation's durable outbound leaf.
const OUT_7_BARE: &str = "chat.app.home.out.7";
const OUT_7: &str = "brenn:chat.app.home.out.7";
/// Granted under the same prefix but provisioned only where a test says so — the
/// conversation born (or dying) under a live attachment.
const OUT_9_BARE: &str = "chat.app.home.out.9";
const OUT_9: &str = "brenn:chat.app.home.out.9";
/// Granted for publish only: the inbound leaf a driver sends commands on.
const IN_7_BARE: &str = "chat.app.home.in.7";
const IN_7: &str = "brenn:chat.app.home.in.7";
/// Granted under the ephemeral `stream.` prefix: the token stream.
const STREAM_7_NAME: &str = "chat.app.home.stream.7";
const STREAM_7: &str = "ephemeral:chat.app.home.stream.7";

/// What `chat.app.home.out.7` actually stands to retain, below the ACL's 64.
/// The two numbers differ so the minted entry's clamp is observable.
const OUT_STANDING: u64 = 4;

/// The depths this suite subscribes at. Both knobs stated, as the protocol
/// requires; the peer clamps them to the `[[remote]]` ceilings.
const DEPTHS: SubscriptionDepths = SubscriptionDepths {
    push_depth: 4,
    retain_depth: 4,
};

/// A durable channel that retains a bounded window — the shape a real
/// conversation leaf has, and the reason a subscriber entry minted at an ACL
/// ceiling has to be clamped before the reaper reads it.
fn durable_at_standing(bare: &str, standing: u64) -> ChannelEntry {
    let mut entry = test_channel_entry(bare, vec![]);
    entry.resolved_channel.retain_depth = Depth::Bounded(standing);
    entry.resolved_channel.standing_retain_depth = Depth::Bounded(standing);
    entry
}

/// The channels one conversation of app `home` provisions, plus the roster.
fn fleet_channels() -> Vec<ChannelEntry> {
    vec![
        test_channel_entry(ROSTER_BARE, vec![]),
        durable_at_standing(OUT_7_BARE, OUT_STANDING),
        test_channel_entry(IN_7_BARE, vec![]),
        ephemeral_channel_entry(STREAM_7_NAME, 32),
    ]
}

/// Everything a run needs standing up: a booted remote, a live server, and a
/// severable relay in front of it.
///
/// Held whole rather than destructured, because two of the fields are alive-or-
/// not: dropping the server stops it, and dropping the relay cuts every pair it
/// is carrying.
struct Rig {
    client: AttachClient,
    relay: SeverableRelay,
    harness: RemoteTestHarness,
    _server: TestServer,
}

/// Stand the rig up with its client offering `versions`.
async fn build_rig(
    db: &brenn_db::Db,
    block: impl FnOnce(&Path) -> RemoteConfigRaw,
    channels: Vec<ChannelEntry>,
    versions: VersionRange,
) -> Rig {
    let harness = remote_harness_with_channels(db, block, channels).await;
    let (base, server) = spawn_test_server(harness.state.clone()).await;
    let relay = SeverableRelay::spawn(http_base_addr(&base)).await;
    let client = client_through(&relay, versions);
    Rig {
        client,
        relay,
        harness,
        _server: server,
    }
}

/// A client of the rig's one remote, reaching the server through `relay` and
/// offering `versions`.
fn client_through(relay: &SeverableRelay, versions: VersionRange) -> AttachClient {
    AttachClient::new(ClientConfig {
        // No query string: the remote route has no served-asset build to agree
        // with, so a daemon's URL is the route and nothing else.
        url: format!("ws://{}/remote/{SLUG}/ws", relay.addr()),
        credential: Credential::Bearer(TOKEN.to_string()),
        ident: "remote-conformance".to_string(),
        versions,
    })
}

impl Rig {
    /// A second session of the same remote, through the same relay, so a
    /// sever cuts it too.
    fn sibling(&self, versions: VersionRange) -> AttachClient {
        client_through(&self.relay, versions)
    }
}

/// One publish under the remote's own identity — the only attribution it has.
fn publish(channel: &str, body: impl Into<MessageBody>) -> PublishRequest {
    PublishRequest::new(channel, body.into(), Urgency::Normal)
}

/// Assert the attacher observes the loss of its transport.
///
/// A publish still outstanding when the socket died is reported lost first: that
/// is the same event seen from the publish plane, not a different one, so it is
/// passed over rather than treated as a surprise.
async fn expect_detach(client: &mut AttachClient) {
    loop {
        match client.next_observation().await {
            Observation::Detached { .. } => return,
            Observation::PublishLost { .. } => continue,
            other => panic!("expected the attachment to be detached, got {other:?}"),
        }
    }
}

/// This remote's directory entry on `channel`, if a subscribe has minted one.
fn remote_entry(rig: &Rig, channel: &str) -> Option<SubscriberEntry> {
    rig.harness
        .directory
        .resolve(channel)?
        .subscribers
        .iter()
        .find(|sub| matches!(&sub.kind, SubscriberEntryKind::Remote(slug) if slug == SLUG))
        .cloned()
}

/// Provision `entry` under a live attachment, the way a conversation's creation
/// does: the database row and the directory, in that order.
async fn provision(rig: &Rig, db: &brenn_db::Db, entry: ChannelEntry) {
    {
        let conn = db.lock().await;
        brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    rig.harness.directory.add_channel(entry);
}

/// Assert the rig captured exactly one protocol-violation security event.
async fn expect_one_violation(rig: &Rig, context: &str) {
    let events = rig.harness.captured().await;
    assert_eq!(
        events.len(),
        1,
        "{context}: one violation earns one security event, got {events:?}"
    );
    assert!(
        events[0].0.contains("attach_protocol_violation"),
        "{context}: the event is fail2ban-grade violation signal, got source {:?}",
        events[0].0
    );
}

/// **The whole bridge, from a client with no browser under it.**
///
/// Bearer-authenticate, negotiate, subscribe an exactly-granted state channel and
/// a prefix-granted conversation leaf, publish, be delivered the peer's stamped
/// envelope, lose the socket to a severed relay, reconnect, and resume: the
/// resumed subscribe carries the cursor of the last delivery accepted, so the
/// peer replays nothing already seen.
async fn a_daemon_attaches_subscribes_publishes_and_resumes_across_a_severed_socket(
    versions: VersionRange,
) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;

    let facts = rig.client.attach().await;
    assert_eq!(
        facts.participant_id, "remote:pod-kitchen",
        "the attachment speaks as the one principal a remote has"
    );
    assert_eq!(facts.version, versions.max);
    assert!(facts.alert_granted, "the block grants the alert plane");

    let roster = rig
        .client
        .subscribe(ROSTER, DEPTHS, ResumePolicy::Cursorless)
        .await;
    assert_eq!(roster.replay_count, 0, "no snapshot has been written yet");
    let out = rig
        .client
        .subscribe(OUT_7, DEPTHS, ResumePolicy::Resume)
        .await;
    assert_eq!(out.replay_count, 0, "nothing is retained yet");
    assert!(out.gap.is_none());

    assert_eq!(
        rig.client.publish(publish(OUT_7, "one")).await,
        PublishOutcome::Ok
    );
    let first = rig.client.next_delivery(OUT_7).await;
    assert_eq!(first.text(), "one");
    assert_eq!(
        first.sender, "remote:pod-kitchen",
        "a remote publishes as itself: one principal, no sub-identity grain to mint"
    );
    assert_eq!(first.seq, 1, "the first delivery of the span");
    assert_eq!(first.dropped, 0);

    rig.relay.sever();
    expect_detach(&mut rig.client).await;
    // Nothing here reopens anything: the driver's own backoff schedule is what
    // reconnects, and this waits it out.
    let resumed = rig.client.attach().await;
    assert_eq!(resumed.participant_id, "remote:pod-kitchen");

    let out = rig.client.next_subscribe_ack(OUT_7).await;
    assert_eq!(
        out.replay_count, 0,
        "the channel retains the message and the resumed cursor covers it, so nothing replays"
    );
    assert!(
        out.gap.is_none(),
        "an in-window resume on the same epoch is not a gap"
    );

    assert_eq!(
        rig.client.publish(publish(OUT_7, "two")).await,
        PublishOutcome::Ok
    );
    let second = rig.client.next_delivery(OUT_7).await;
    assert_eq!(
        second.text(),
        "two",
        "the stream continues past the resume point rather than repeating it"
    );

    rig.client.close().await;
}

/// The app's owner, created on first ask: the app is a singleton with one
/// allowed user, so every conversation of `home` hangs off this row and a test
/// seeding a second one must not try to create the user twice.
async fn owner(db: &brenn_db::Db) -> i64 {
    let conn = db.lock().await;
    match brenn_db::auth::user::get_user_by_username(&conn, OWNER) {
        Some(user) => user.id,
        None => brenn_db::auth::user::create_user(&conn, OWNER, "$argon2id$fake"),
    }
}

/// Mint a conversation of app `home`, so the roster has something to name.
///
/// The row alone: these cases test the delivery of a server-authored snapshot,
/// and provisioning the conversation's chat family would only add channels no
/// assertion here reads. The case that does exercise provisioning goes through
/// the messenger's own minting path instead.
async fn seed_conversation(db: &brenn_db::Db) -> i64 {
    let user = owner(db).await;
    let conn = db.lock().await;
    brenn_db::conversation::create_conversation(&conn, user, APP, false)
}

/// The app's roster snapshot as the server authors it, published through the
/// same call the conversation-creation hooks make.
///
/// A refusal is surfaced here rather than being read as an absent snapshot: the
/// publish answers `Some` even when it refuses, and `None` for an app the
/// messenger does not know or an address its directory cannot resolve — so a
/// test that only checked for delivery could pass a rig that never published.
async fn publish_roster(rig: &Rig) {
    let outcome = rig
        .harness
        .state
        .messenger
        .as_ref()
        .expect("the rig boots a messenger")
        .publish_chat_roster(APP)
        .await;
    assert!(
        matches!(outcome, Some(PublishResult::Ok { .. })),
        "the server-side roster publish must have gone out, got {outcome:?}"
    );
}

/// The conversation ids a delivered roster body names.
fn roster_ids(body: &str) -> Vec<i64> {
    let roster: ChatRoster = chat_decode(body).expect("a roster body is a versioned chat message");
    roster.conversations.into_iter().map(|c| c.id).collect()
}

/// How long a roster case waits for a delivery before calling it absent.
const ROSTER_WAIT: Duration = Duration::from_secs(10);

/// The next delivery on `channel`, or a failed assertion — never an endless wait.
///
/// A delivery a regression would simply never make must surface as an ordinary
/// test failure, not a hang that outlasts the runner's patience.
async fn expect_roster_delivery(client: &mut AttachClient) -> Delivery {
    tokio::time::timeout(ROSTER_WAIT, client.next_delivery(ROSTER))
        .await
        .unwrap_or_else(|_| panic!("no roster delivery arrived within {ROSTER_WAIT:?}"))
}

/// **The pod's cold connect: what it learns before it knows anything.**
///
/// A snapshot written while no peer was attached is retained, so the roster
/// subscribe of a remote attaching afterwards replays it — which is the whole
/// mechanism by which a daemon holding a fleet-grain grant discovers the
/// exact-channel addresses it may subscribe to.
async fn a_retained_roster_snapshot_replays_to_a_freshly_attached_remote(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    let conversation = seed_conversation(&db).await;
    publish_roster(&rig).await;

    rig.client.attach().await;
    let ack = rig
        .client
        .subscribe(ROSTER, DEPTHS, ResumePolicy::Cursorless)
        .await;
    assert_eq!(
        ack.replay_count, 1,
        "the retained snapshot is what a cold-connecting peer reconciles against"
    );

    let delivery = expect_roster_delivery(&mut rig.client).await;
    assert_eq!(
        roster_ids(delivery.text()),
        vec![conversation],
        "the replayed snapshot names the app's conversation"
    );
    assert_eq!(
        delivery.sender, "system:chat-roster",
        "the roster has one writer, and it is not the attacher"
    );

    rig.client.close().await;
}

/// **The same channel while the peer is watching, twice over.**
///
/// A conversation created under a live attachment reaches the daemon as a
/// delivery on the roster it already holds — the reconcile path that keeps a
/// long-lived pod current without reattaching. The second snapshot is the half
/// that path actually lives on: a peer that only ever received the first would
/// hold the world as it stood at connect time and never learn of another
/// conversation until it reattached.
async fn a_roster_republish_reaches_a_subscribed_remote(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;

    let ack = rig
        .client
        .subscribe(ROSTER, DEPTHS, ResumePolicy::Cursorless)
        .await;
    assert_eq!(ack.replay_count, 0, "no snapshot has been written yet");

    // One publish, after the subscription exists. The roster is deduplicated
    // against the last body it published, so each snapshot below has to name a
    // conversation set the one before it did not.
    let first = seed_conversation(&db).await;
    publish_roster(&rig).await;

    let delivery = expect_roster_delivery(&mut rig.client).await;
    assert_eq!(
        roster_ids(delivery.text()),
        vec![first],
        "the live snapshot names the conversation that appeared under the attachment"
    );
    assert_eq!(delivery.seq, 1, "the first delivery of the span");
    assert_eq!(delivery.dropped, 0);

    let second = seed_conversation(&db).await;
    publish_roster(&rig).await;

    let next = expect_roster_delivery(&mut rig.client).await;
    assert_eq!(
        roster_ids(next.text()),
        vec![first, second],
        "the follow-up snapshot reaches the subscription that already took the first"
    );
    assert_eq!(next.seq, 2, "the second delivery of the same span");
    assert_eq!(next.dropped, 0);

    rig.client.close().await;
}

/// **The mint the pod actually waits on.**
///
/// The two cases above publish the snapshot themselves; this one lets the
/// messenger do it, through the path that mints an app's conversation lazily —
/// `attach_conversation`, which provisions the chat family and republishes the
/// roster. It is the seam the other cases leave open: an announce landing on an
/// address the remote's fleet grant does not cover, or a prefix derivation that
/// differs between the messenger and the harness, leaves both halves green while
/// the pod learns of no conversation it did not already know.
async fn a_conversation_the_attach_mints_reaches_a_subscribed_remote(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    let messenger = Arc::clone(
        rig.harness
            .state
            .messenger
            .as_ref()
            .expect("the rig boots a messenger"),
    );
    let user = owner(&db).await;

    rig.client.attach().await;
    let ack = rig
        .client
        .subscribe(ROSTER, DEPTHS, ResumePolicy::Cursorless)
        .await;
    assert_eq!(ack.replay_count, 0, "no snapshot has been written yet");

    // The app has never held a conversation: the attach mints one, provisions it
    // and announces it, all from inside the messenger.
    messenger
        .attach_conversation(OUT_7, APP, Depth::Bounded(8))
        .await;
    let conversation = {
        let conn = db.lock().await;
        brenn_db::conversation::get_singleton_conversation_id(&conn, user, APP)
            .expect("the attach mints the app's conversation")
    };

    let delivery = expect_roster_delivery(&mut rig.client).await;
    assert_eq!(
        roster_ids(delivery.text()),
        vec![conversation],
        "the roster the attach republished names the conversation it minted"
    );
    assert_eq!(
        delivery.sender, "system:chat-roster",
        "the announce is the server's own, whoever triggered it"
    );

    rig.client.close().await;
}

/// **Both retention classes, one attachment, one vocabulary.**
///
/// A conversation's durable outbound leaf and its ephemeral token stream differ
/// in where the rows live and in nothing the attacher says or reads: same
/// subscribe, same `Deliver` shape, same per-row cursor. The bridge carries the
/// distinction as an address prefix and no more.
async fn a_durable_leaf_and_an_ephemeral_stream_deliver_alike(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;

    for channel in [OUT_7, STREAM_7] {
        let ack = rig
            .client
            .subscribe(channel, DEPTHS, ResumePolicy::Resume)
            .await;
        assert_eq!(ack.replay_count, 0, "{channel}: nothing retained yet");
        assert_eq!(
            rig.client.publish(publish(channel, "token")).await,
            PublishOutcome::Ok
        );
        let delivery = rig.client.next_delivery(channel).await;
        assert_eq!(delivery.text(), "token");
        assert_eq!(delivery.sender, "remote:pod-kitchen");
        assert_eq!(delivery.seq, 1);
        assert_eq!(delivery.dropped, 0);
    }

    assert!(
        remote_entry(&rig, STREAM_7).is_some(),
        "an ephemeral channel's subscriber entry is minted by the same path"
    );

    rig.client.close().await;
}

/// **The directory entry no boot could have folded.**
///
/// A remote's channels are matcher-granted, so nothing at boot knows which
/// addresses it will hold; the entry that makes the fan-out find it is minted by
/// its own first subscribe, at the operator's ceilings — never the depths the
/// client stated — and clamped to what the channel actually stands to retain.
async fn the_first_subscribe_mints_the_directory_entry_at_the_clamped_profile_ceiling(
    versions: VersionRange,
) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;

    assert!(
        remote_entry(&rig, OUT_7).is_none(),
        "boot folds no entry for a remote: there was no set of addresses to fold"
    );

    rig.client
        .subscribe(ROSTER, DEPTHS, ResumePolicy::Cursorless)
        .await;
    rig.client
        .subscribe(OUT_7, DEPTHS, ResumePolicy::Resume)
        .await;

    let roster = remote_entry(&rig, ROSTER).expect("the subscribe minted the roster entry");
    assert_eq!(
        (roster.push_depth, roster.retain_depth),
        (Depth::Bounded(1), Depth::Bounded(1)),
        "the operator's 1/1 ceiling, not the 4/4 the client asked for"
    );

    let out = remote_entry(&rig, OUT_7).expect("the subscribe minted the conversation entry");
    assert_eq!(
        (out.push_depth, out.retain_depth),
        (Depth::Bounded(OUT_STANDING), Depth::Bounded(OUT_STANDING)),
        "the 8/64 ceiling clamped to the channel's standing window, which the reaper trusts"
    );

    rig.relay.sever();
    expect_detach(&mut rig.client).await;
    rig.client.attach().await;
    rig.client.next_subscribe_ack(OUT_7).await;

    assert_eq!(
        rig.harness
            .directory
            .resolve(OUT_7)
            .expect("the channel is still provisioned")
            .subscribers
            .len(),
        1,
        "a second session re-subscribing replaces its own entry rather than adding one"
    );
}

/// **The deprovision race, answered rather than punished.**
///
/// A prefix grant names conversations that come and go, so an address the
/// operator authorized and the directory does not hold is a race the daemon
/// reconciles from — non-fatal, opening nothing, and not fail2ban signal.
async fn a_granted_channel_the_directory_lacks_is_unavailable_until_it_is_provisioned(
    versions: VersionRange,
) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;

    let settlement = rig
        .client
        .subscribe_settlement(OUT_9, DEPTHS, ResumePolicy::Resume)
        .await;
    assert!(
        matches!(settlement, SubscribeSettlement::Unavailable),
        "granted by prefix and absent from the directory, got {settlement:?}"
    );
    assert!(
        !rig.client.is_subscribed(OUT_9),
        "the refusal cleared the hold, so a reattach carries no claim on the channel"
    );
    assert!(
        rig.harness.captured().await.is_empty(),
        "a race the operator's own topology produces is not a security event"
    );

    provision(&rig, &db, durable_at_standing(OUT_9_BARE, OUT_STANDING)).await;

    let ack = rig
        .client
        .subscribe(OUT_9, DEPTHS, ResumePolicy::Resume)
        .await;
    assert_eq!(
        ack.replay_count, 0,
        "a channel born a moment ago holds none"
    );
    assert_eq!(
        rig.client.publish(publish(OUT_9, "born")).await,
        PublishOutcome::Ok
    );
    assert_eq!(
        rig.client.next_delivery(OUT_9).await.text(),
        "born",
        "and it delivers like any other conversation leaf"
    );

    rig.client.close().await;
}

/// **A `non_text` channel is not served to a text-only wire, and asking is
/// not a violation.**
///
/// The daemon cannot see the declaration, so its subscribe is conforming:
/// answered `Unavailable`, opening nothing, capturing no security event. The
/// same session subscribes a text-only channel as usual and may still publish
/// text to the `non_text` one, since a declaration widens what a channel may
/// carry.
#[tokio::test]
async fn a_non_text_channel_is_unavailable_to_a_v4_remote_which_may_still_publish_text_to_it() {
    let db = crate::test_support::init_db_memory();
    let mut frames = durable_at_standing(OUT_9_BARE, OUT_STANDING);
    frames.resolved_channel.non_text = true;
    let mut channels = fleet_channels();
    channels.push(frames);
    let mut rig = build_rig(&db, loopback, channels, VersionRange::exactly(4)).await;
    assert_eq!(rig.client.attach().await.version, 4);

    let settlement = rig
        .client
        .subscribe_settlement(OUT_9, DEPTHS, ResumePolicy::Resume)
        .await;
    assert!(
        matches!(settlement, SubscribeSettlement::Unavailable),
        "a non_text channel to a v4 session, got {settlement:?}"
    );
    assert!(!rig.client.is_subscribed(OUT_9));
    assert!(
        remote_entry(&rig, OUT_9).is_none(),
        "the refusal minted no directory entry"
    );

    rig.client
        .subscribe(OUT_7, DEPTHS, ResumePolicy::Resume)
        .await;
    assert_eq!(
        rig.client.publish(publish(OUT_9, "plain text")).await,
        PublishOutcome::Ok
    );
    assert_eq!(
        rig.client.publish(publish(OUT_7, "still served")).await,
        PublishOutcome::Ok
    );
    assert_eq!(rig.client.next_delivery(OUT_7).await.text(), "still served");
    assert!(
        rig.harness.captured().await.is_empty(),
        "asking for a channel the wire cannot carry is not a security event"
    );

    rig.client.close().await;
}

/// **Outside the grants there is no oracle, only a closed socket.**
///
/// An address no matcher covers and a granted name under a scheme it was not
/// granted on answer identically: the attachment dies as a protocol violation,
/// saying nothing about whether the channel exists.
///
/// The third address the profile refuses — a confined `local:` one — cannot be
/// reached from here, which is the stronger answer: the client's own confined
/// router panics on it, so a conforming attacher never puts one on the wire for
/// the server to judge.
async fn subscribing_outside_the_grants_closes_the_attachment(versions: VersionRange) {
    for (context, channel) in [
        ("no matcher covers it", "brenn:chat.app.other.out.1"),
        (
            "granted name, wrong scheme",
            "ephemeral:chat.app.home.out.7",
        ),
    ] {
        let db = crate::test_support::init_db_memory();
        let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
        rig.client.attach().await;

        // Fire-and-forget: the answer to a violation is a closed socket, so a
        // helper that waited for a frame would pump through the reconnect and
        // earn the same close again.
        rig.client
            .send_subscribe(channel, DEPTHS, ResumePolicy::Resume)
            .await;
        expect_detach(&mut rig.client).await;
        expect_one_violation(&rig, context).await;
    }
}

/// **The cap a prefix grant makes necessary.**
///
/// With channels minted at runtime, per-session subscription state is bounded
/// only by how many addresses the ACL ever matches — unless the operator states
/// a cap, which the profile answers and the plane enforces as a violation.
async fn subscribing_beyond_the_configured_cap_closes_the_attachment(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(
        &db,
        |token| RemoteConfigRaw {
            max_subscriptions: Some(2),
            ..loopback(token)
        },
        fleet_channels(),
        versions,
    )
    .await;
    rig.client.attach().await;

    rig.client
        .subscribe(ROSTER, DEPTHS, ResumePolicy::Cursorless)
        .await;
    rig.client
        .subscribe(OUT_7, DEPTHS, ResumePolicy::Resume)
        .await;

    rig.client
        .send_subscribe(STREAM_7, DEPTHS, ResumePolicy::Resume)
        .await;
    expect_detach(&mut rig.client).await;
    expect_one_violation(&rig, "third subscription under a cap of two").await;
}

/// **One principal, and no way to claim another.**
///
/// A remote declares no sub-identities, so an attribution on a publish names
/// something no operator wrote — refused before the channel is even consulted,
/// and refused as a violation rather than an outcome.
async fn publishing_under_a_named_attribution_closes_the_attachment(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;

    rig.client
        .send_publish(PublishRequest {
            channel: IN_7.to_string(),
            attribution: Some("brain".to_string()),
            body: "who said that".into(),
            urgency: Urgency::Normal,
        })
        .await;
    expect_detach(&mut rig.client).await;
    expect_one_violation(&rig, "a publish claiming a sub-identity").await;
}

/// **A publish into a channel that just died is an outcome, not a death.**
///
/// The surface answers `Invariant` here because its channels are boot-declared;
/// a remote's are provisioned at runtime, so the same event is the ordinary race
/// the profile's `Diagnostic` posture exists for. The attachment survives it and
/// keeps publishing elsewhere.
async fn publishing_into_a_deprovisioned_channel_is_a_failed_outcome(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;
    rig.client
        .subscribe(OUT_7, DEPTHS, ResumePolicy::Resume)
        .await;

    let uuid = rig
        .harness
        .directory
        .resolve(OUT_7)
        .expect("provisioned by the rig")
        .uuid;
    assert!(rig.harness.directory.remove_channel(&uuid));

    assert_eq!(
        rig.client.publish(publish(OUT_7, "into the void")).await,
        PublishOutcome::Failed,
        "the conversation vanished under a publish already in flight"
    );
    assert_eq!(
        rig.client.publish(publish(IN_7, "still here")).await,
        PublishOutcome::Ok,
        "and the attachment is unharmed"
    );
    assert!(
        rig.harness.captured().await.is_empty(),
        "a deprovision race is not fail2ban signal"
    );

    rig.client.close().await;
}

/// **A control op on a channel that just died is a lost race, not a death.**
///
/// The daemon parks a message and is shown it; the channel is deprovisioned;
/// the daemon cancels what it was shown. The server cannot find the channel,
/// answers the flush, and the attachment and the server both carry on.
async fn cancelling_into_a_deprovisioned_channel_is_a_no_op(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;

    rig.client.register_outbox("schedule", None, 4);
    rig.client
        .flush(
            "schedule",
            FlushBatch {
                entries: vec![
                    BatchEntry::new(OUT_7, "later".into(), Urgency::Normal)
                        .deliver_after(a_minute_from_now()),
                ],
                ops: Vec::new(),
            },
        )
        .await;
    assert_eq!(
        rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok
    );
    let shown = rig.client.next_deferred_view(OUT_7, None).await;
    assert_eq!(shown.len(), 1, "the daemon is shown what it parked");

    let uuid = rig
        .harness
        .directory
        .resolve(OUT_7)
        .expect("provisioned by the rig")
        .uuid;
    assert!(rig.harness.directory.remove_channel(&uuid));

    rig.client
        .flush(
            "schedule",
            AttachClient::controlling(OUT_7, &shown[0], DeferredOpKind::Cancel),
        )
        .await;
    assert_eq!(
        rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok
    );
    assert_eq!(
        rig.client.publish(publish(IN_7, "still here")).await,
        PublishOutcome::Ok,
        "the attachment is unharmed"
    );
    assert!(
        rig.harness.captured().await.is_empty(),
        "a deprovision race is not fail2ban signal"
    );

    rig.client.close().await;
}

/// **The alert plane, which touches no channel.**
///
/// A granted alert reaches the process dispatcher attributed to the remote's
/// principal — the point being that paging works even when the bus it would
/// report on does not.
async fn a_granted_alert_reaches_the_dispatcher_attributed_to_the_remote(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;

    rig.client
        .send_alert(
            None,
            AlertSeverity::Warning,
            "mic array offline",
            "XVF3800 gone",
        )
        .await;
    // The session reads frames in order, so an answered publish is the
    // happens-after edge that makes the alert readable without a sleep.
    assert_eq!(
        rig.client.publish(publish(IN_7, "ping")).await,
        PublishOutcome::Ok
    );

    let events = rig.harness.captured().await;
    assert_eq!(events.len(), 1, "one alert, got {events:?}");
    assert!(
        events[0]
            .0
            .contains("Attacher remote:pod-kitchen: mic array offline"),
        "the dispatcher's source names who paged, got {:?}",
        events[0].0
    );
    assert!(
        events[0].1.contains("attacher=remote:pod-kitchen"),
        "and the body carries the server-attested attribution, got {:?}",
        events[0].1
    );

    rig.client.close().await;
}

/// **Deny-by-default, all the way down.**
///
/// An ungranted attacher is told so in its `Welcome`; alerting anyway is a
/// conforming client's bug and the peer treats it as one.
async fn alerting_without_the_grant_closes_the_attachment(versions: VersionRange) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(
        &db,
        |token| RemoteConfigRaw {
            grants: vec![
                AttachGrant::Subscribe,
                AttachGrant::Publish,
                AttachGrant::EphemeralSubscribe,
                AttachGrant::EphemeralPublish,
            ],
            ..loopback(token)
        },
        fleet_channels(),
        versions,
    )
    .await;

    let facts = rig.client.attach().await;
    assert!(
        !facts.alert_granted,
        "the grant is advertised, so a conforming client suppresses its own alerts"
    );

    rig.client
        .send_alert(None, AlertSeverity::Critical, "paging anyway", "")
        .await;
    expect_detach(&mut rig.client).await;
    expect_one_violation(&rig, "an alert from an ungranted attacher").await;
}

/// **Attribution is the surface's word, and it crosses the wire.**
///
/// A remote declares no sub-identities, so any attribution it writes names
/// nobody — a violation, judged on the string the peer actually read off the
/// frame. That is the only whole-socket witness that `attribution` survives
/// serialization at all: were it dropped in transit, this alert would arrive as
/// the platform's own, dispatch, and leave the attachment open.
async fn an_attributed_alert_from_an_attacher_with_no_sub_identities_closes_the_attachment(
    versions: VersionRange,
) {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), versions).await;
    rig.client.attach().await;

    rig.client
        .send_alert(
            Some("mic-watch"),
            AlertSeverity::Warning,
            "speaking for someone",
            "",
        )
        .await;
    expect_detach(&mut rig.client).await;
    let events = rig.harness.captured().await;
    assert_eq!(
        events.len(),
        1,
        "one violation, and no dispatched alert, got {events:?}"
    );
    assert!(
        events[0].0.contains("attach_protocol_violation"),
        "the event is fail2ban-grade violation signal, got source {:?}",
        events[0].0
    );
    assert!(
        events[0].1.contains("mic-watch"),
        "the violation names the attribution the peer read, got {:?}",
        events[0].1
    );
}

// ---------------------------------------------------------------------------
// Bodies as bytes: what a v5 attachment carries and a v4 one does not
// ---------------------------------------------------------------------------

/// A body no text carrier can hold: JPEG's start-of-image marker followed by
/// bytes that are not UTF-8.
fn jpeg_body() -> MessageBody {
    MessageBody::from_parts(
        Some("image/jpeg"),
        &[0xff, 0xd8, 0xff, 0xe0, 0x00, 0x80, 0xfe],
    )
    .expect("a binary content type over bytes")
}

/// `entry`, declared `non_text`.
fn non_text(mut entry: ChannelEntry) -> ChannelEntry {
    entry.resolved_channel.non_text = true;
    entry
}

/// The fleet's channels plus `out.9` as a durable `non_text` conversation leaf.
fn channels_with_non_text_out_9() -> Vec<ChannelEntry> {
    let mut channels = fleet_channels();
    channels.push(non_text(durable_at_standing(OUT_9_BARE, OUT_STANDING)));
    channels
}

/// Now plus a minute, in epoch milliseconds: a release no case outlives.
fn a_minute_from_now() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).expect("a positive epoch") + 60_000
}

/// **Bytes in, the same bytes out, and again after the socket dies.**
///
/// A v5 attacher publishes a body that is not UTF-8, under a content type, to
/// a durable `non_text` channel. A second v5 session of the same remote is
/// delivered it byte for byte with its type, live; and, holding a cursorless
/// subscription, is replayed it from the channel's window after the relay
/// severs both transports.
#[tokio::test]
async fn a_v5_non_text_body_reaches_another_v5_attacher_byte_exact_live_and_on_replay() {
    let db = crate::test_support::init_db_memory();
    let five = VersionRange::exactly(5);
    let mut rig = build_rig(&db, loopback, channels_with_non_text_out_9(), five).await;
    let mut reader = rig.sibling(five);
    assert_eq!(rig.client.attach().await.version, 5);
    assert_eq!(reader.attach().await.version, 5);

    let ack = reader
        .subscribe(OUT_9, DEPTHS, ResumePolicy::Cursorless)
        .await;
    assert_eq!(ack.replay_count, 0, "nothing is retained yet");

    let body = jpeg_body();
    assert_eq!(
        rig.client.publish(publish(OUT_9, body.clone())).await,
        PublishOutcome::Ok
    );
    let live = reader.next_delivery(OUT_9).await;
    assert_eq!(live.body, body, "delivered live, byte for byte");
    assert_eq!(live.body.content_type(), Some("image/jpeg"));
    assert_eq!(live.body.as_bytes(), body.as_bytes());
    assert_eq!(live.sender, "remote:pod-kitchen");

    rig.relay.sever();
    expect_detach(&mut rig.client).await;
    expect_detach(&mut reader).await;
    rig.client.attach().await;
    reader.attach().await;

    let ack = reader.next_subscribe_ack(OUT_9).await;
    assert_eq!(
        ack.replay_count, 1,
        "a cursorless resubscribe is replayed the window"
    );
    let replayed = reader.next_delivery(OUT_9).await;
    assert_eq!(replayed.body, body, "and the replay is byte-exact too");
    assert!(
        rig.harness.captured().await.is_empty(),
        "nothing here is a security event"
    );

    rig.client.close().await;
    reader.close().await;
}

/// **Typed text keeps its type on v5 and is plain text on v4.**
///
/// The channel is text-only: typed text is text, so a v5 attacher may put it
/// there. A v5 subscriber reads the type; a v4 subscriber of the same remote
/// reads the same text with no type, which is all v4 can say.
#[tokio::test]
async fn typed_text_arrives_typed_at_v5_and_as_its_text_at_v4() {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), VersionRange::exactly(5)).await;
    let mut old = rig.sibling(VersionRange::exactly(4));
    assert_eq!(rig.client.attach().await.version, 5);
    assert_eq!(old.attach().await.version, 4);

    rig.client
        .subscribe(OUT_7, DEPTHS, ResumePolicy::Resume)
        .await;
    old.subscribe(OUT_7, DEPTHS, ResumePolicy::Resume).await;

    let typed = MessageBody::from_parts(Some("text/markdown"), b"# kitchen\n\nlights on")
        .expect("text under a text type");
    assert_eq!(
        rig.client.publish(publish(OUT_7, typed.clone())).await,
        PublishOutcome::Ok,
        "typed text is text, so a text-only channel takes it"
    );

    let at_v5 = rig.client.next_delivery(OUT_7).await;
    assert_eq!(at_v5.body, typed, "v5 is delivered the type with the text");
    let at_v4 = old.next_delivery(OUT_7).await;
    assert_eq!(
        at_v4.body,
        MessageBody::text("# kitchen\n\nlights on"),
        "v4 is delivered the text alone"
    );
    assert_eq!(at_v4.body.content_type(), None);

    rig.client.close().await;
    old.close().await;
}

/// **A non-text body offered to a text-only channel is an answer, not a
/// death.**
///
/// The attacher cannot see a channel's declaration, so the mistake is a
/// conforming one: a single publish is answered `TextOnlyChannel`, and a batch
/// holding one such entry beside a text one is answered the same with nothing
/// of it applied. The attachment carries on, and no security event is raised.
#[tokio::test]
async fn a_v5_non_text_publish_to_a_text_only_channel_is_refused_single_and_batched() {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(&db, loopback, fleet_channels(), VersionRange::exactly(5)).await;
    rig.client.attach().await;
    rig.client
        .subscribe(OUT_7, DEPTHS, ResumePolicy::Resume)
        .await;

    assert_eq!(
        rig.client.publish(publish(OUT_7, jpeg_body())).await,
        PublishOutcome::TextOnlyChannel
    );

    rig.client.register_outbox("frames", None, 4);
    rig.client
        .flush(
            "frames",
            FlushBatch {
                entries: vec![
                    BatchEntry::new(OUT_7, "caption".into(), Urgency::Normal),
                    BatchEntry::new(OUT_7, jpeg_body(), Urgency::Normal),
                ],
                ops: Vec::new(),
            },
        )
        .await;
    assert_eq!(
        rig.client.next_batch_outcome().await,
        PublishBatchOutcome::TextOnlyChannel
    );

    assert_eq!(
        rig.client.publish(publish(OUT_7, "after")).await,
        PublishOutcome::Ok,
        "the attachment is unharmed"
    );
    assert_eq!(
        rig.client.next_delivery(OUT_7).await.text(),
        "after",
        "the first delivery is the text published after: neither refusal stored anything, \
         and the batch's text entry went with its refused sibling"
    );
    assert!(
        rig.harness.captured().await.is_empty(),
        "a refused body is an outcome, not fail2ban signal"
    );

    rig.client.close().await;
}

/// **A sibling on v4 is never shown what it cannot hold.**
///
/// One remote, a v5 and a v4 session. The v5 session parks a non-text entry
/// and a text one; its own mirror holds both. The v4 session mirrors the same
/// parked set, and is sent it without the entry v4 cannot represent.
#[tokio::test]
async fn a_v5_sessions_parked_non_text_entry_is_in_its_own_view_and_not_in_a_v4_siblings() {
    let db = crate::test_support::init_db_memory();
    let mut rig = build_rig(
        &db,
        loopback,
        channels_with_non_text_out_9(),
        VersionRange::exactly(5),
    )
    .await;
    let mut old = rig.sibling(VersionRange::exactly(4));
    assert_eq!(rig.client.attach().await.version, 5);
    assert_eq!(old.attach().await.version, 4);

    let release = a_minute_from_now();
    rig.client.register_outbox("schedule", None, 4);
    rig.client
        .flush(
            "schedule",
            FlushBatch {
                entries: vec![
                    BatchEntry::new(OUT_9, jpeg_body(), Urgency::Normal).deliver_after(release),
                    BatchEntry::new(OUT_9, "held".into(), Urgency::Normal).deliver_after(release),
                ],
                ops: Vec::new(),
            },
        )
        .await;
    assert_eq!(
        rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok
    );

    let own = rig.client.next_deferred_view(OUT_9, None).await;
    let mut own_bodies: Vec<MessageBody> = own.into_iter().map(|e| e.body).collect();
    own_bodies.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert_eq!(
        own_bodies,
        vec![MessageBody::text("held"), jpeg_body()],
        "the v5 session's own mirror holds both parked entries"
    );

    let sibling = old.next_deferred_view(OUT_9, None).await;
    assert_eq!(
        sibling.iter().map(|e| e.body.clone()).collect::<Vec<_>>(),
        vec![MessageBody::text("held")],
        "the v4 session is sent the text entry and not the one it cannot carry"
    );
    assert!(old.is_attached(), "the cut is quiet: the v4 session lives");
    assert!(rig.harness.captured().await.is_empty());

    rig.client.close().await;
    old.close().await;
}

// ---------------------------------------------------------------------------
// Every case above that a v4 daemon can run, run at both offers
// ---------------------------------------------------------------------------

/// Each case, twice: once from a daemon offering exactly v4 — a daemon built
/// before v5, which must see what it always saw — and once from one offering
/// every supported version.
macro_rules! at_each_offer {
    ($($case:ident),* $(,)?) => {
        $(
            mod $case {
                use super::*;

                #[tokio::test]
                async fn offering_v4() {
                    super::$case(VersionRange::exactly(4)).await;
                }

                #[tokio::test]
                async fn offering_every_supported_version() {
                    super::$case(SUPPORTED_VERSIONS).await;
                }
            }
        )*
    };
}

at_each_offer!(
    a_daemon_attaches_subscribes_publishes_and_resumes_across_a_severed_socket,
    a_retained_roster_snapshot_replays_to_a_freshly_attached_remote,
    a_roster_republish_reaches_a_subscribed_remote,
    a_conversation_the_attach_mints_reaches_a_subscribed_remote,
    a_durable_leaf_and_an_ephemeral_stream_deliver_alike,
    the_first_subscribe_mints_the_directory_entry_at_the_clamped_profile_ceiling,
    a_granted_channel_the_directory_lacks_is_unavailable_until_it_is_provisioned,
    subscribing_outside_the_grants_closes_the_attachment,
    subscribing_beyond_the_configured_cap_closes_the_attachment,
    publishing_under_a_named_attribution_closes_the_attachment,
    publishing_into_a_deprovisioned_channel_is_a_failed_outcome,
    cancelling_into_a_deprovisioned_channel_is_a_no_op,
    a_granted_alert_reaches_the_dispatcher_attributed_to_the_remote,
    alerting_without_the_grant_closes_the_attachment,
    an_attributed_alert_from_an_attacher_with_no_sub_identities_closes_the_attachment,
);

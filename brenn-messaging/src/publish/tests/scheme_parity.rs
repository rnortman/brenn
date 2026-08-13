//! One publish ladder, every pub/sub scheme: the gates a publish passes, and the
//! outcomes it can produce, do not depend on the target's delivery class.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::config::{Depth, MessagingGlobalConfig, ResolvedMessagingConfig, SendRate};
use crate::db::init_db_memory;
use crate::store::RingStores;
use crate::testutils::{ephemeral_channel_entry, local_channel_entry, test_channel_entry};
use crate::{MessagingDirectory, WakeRouter, canonical_address, db::upsert_channels};
use brenn_lib::access::AppCapability;
use brenn_lib::access::acl::ChannelMatcher;
use indexmap::IndexMap;
use std::sync::Arc;

const SOURCE: &str = "test-source";
const DURABLE: &str = "durable-chan";
/// A second durable channel, retention-bounded so its deferred cap is reachable
/// — the fixture's main durable channel is `Unbounded` on both.
const DURABLE_CAPPED: &str = "durable-capped";
const CAPPED_DEPTH: u64 = 2;
const CONVERSATION: i64 = 1;

/// The ring store behind a non-durable channel of the parity fixture.
fn ring_store(m: &Messenger, address: &str) -> Arc<crate::store::RingStore> {
    m.ring_stores()
        .get_by_address(address)
        .unwrap_or_else(|| panic!("{address} is a registered non-durable channel"))
        .clone()
}

/// The one sender used across the ladder tests: covered by every scheme's
/// publish ACL, so a denial in these tests is always the gate under test.
fn publisher(slug: &str) -> brenn_lib::config::AppConfig {
    let mut cfg = test_app_config(
        slug,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec!["bob".to_string()],
    );
    cfg.policy.grants.insert(AppCapability::EphemeralPublish);
    cfg.policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Prefix(String::new()));
    cfg.policy.grants.insert(AppCapability::LocalPublish);
    cfg.policy
        .acls
        .local_publish
        .push(ChannelMatcher::Prefix(String::new()));
    cfg
}

/// A sender carrying the **`brenn:` half only**: `MessagingPublish` plus a
/// `brenn_publish` matcher covering every bare name, and nothing else. Its
/// matcher therefore covers `eph-chan` too — the wrong-grant probe for the
/// `ephemeral:` scheme.
fn brenn_only(slug: &str) -> brenn_lib::config::AppConfig {
    test_app_config(
        slug,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec!["bob".to_string()],
    )
}

/// The mirror of [`brenn_only`]: the **`ephemeral:` half only**, with a matcher
/// covering every bare name including `durable-chan`.
fn eph_only(slug: &str) -> brenn_lib::config::AppConfig {
    let mut cfg = test_app_config(slug, None, vec!["bob".to_string()]);
    cfg.policy.grants.insert(AppCapability::EphemeralPublish);
    cfg.policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Prefix(String::new()));
    cfg
}

/// A messenger holding one channel of each class — durable, transportable
/// non-durable, and confined non-durable — with its stores and bus wired the way
/// boot wires them.
async fn parity_messenger(rate: SendRate) -> Arc<Messenger> {
    let db = init_db_memory();
    let durable = test_channel_entry(DURABLE, vec![]);
    let capped = {
        let mut entry = test_channel_entry(DURABLE_CAPPED, vec![]);
        entry.resolved_channel.retain_depth = Depth::Bounded(CAPPED_DEPTH);
        entry.resolved_channel.standing_retain_depth = Depth::Bounded(CAPPED_DEPTH);
        entry
    };
    let ephemeral = ephemeral_channel_entry("eph-chan", 8);
    let local = local_channel_entry("local-chan", 8);
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'bob', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'pub-app', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
        upsert_channels(&conn, &[durable.clone(), capped.clone()]);
    }

    let mut durable = durable;
    durable.resolved_channel.send_rate = rate;
    let mut capped = capped;
    capped.resolved_channel.send_rate = rate;
    let mut ephemeral = ephemeral;
    ephemeral.resolved_channel.send_rate = rate;
    let mut local = local;
    local.resolved_channel.send_rate = rate;

    let nondurable = [ephemeral.clone(), local.clone()];
    let directory = Arc::new(MessagingDirectory::with_entries(vec![
        durable, capped, ephemeral, local,
    ]));

    let mut apps: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
    apps.insert("pub-app".to_string(), publisher("pub-app"));
    apps.insert(
        "no-grants".to_string(),
        test_app_config("no-grants", None, vec!["bob".to_string()]),
    );
    apps.insert("brenn-only".to_string(), brenn_only("brenn-only"));
    apps.insert("eph-only".to_string(), eph_only("eph-only"));

    let stores = Arc::new(RingStores::build(&nondurable));
    Messenger::new(
        db,
        directory,
        Arc::from(SOURCE),
        Arc::new(apps),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_ring_stores(stores)
}

async fn send(m: &Messenger, slug: &str, addr: &str, body: &str) -> PublishResult {
    m.publish(
        PublishOrigin::Conversation { id: CONVERSATION },
        slug,
        addr,
        body,
        Urgency::Normal,
        None,
        None,
        None,
    )
    .await
}

/// Every class's target address, for the per-scheme sweeps.
fn all_targets() -> Vec<String> {
    vec![
        canonical_address(DURABLE),
        "ephemeral:eph-chan".to_string(),
        "local:local-chan".to_string(),
    ]
}

// --- Commit ---------------------------------------------------------------

#[tokio::test]
async fn an_ephemeral_publish_lands_in_the_ring_and_fans_out() {
    let m = parity_messenger(SendRate::default()).await;
    let PublishResult::Ok { address, .. } = send(&m, "pub-app", "ephemeral:eph-chan", "hi").await
    else {
        panic!("expected Ok");
    };
    assert_eq!(address, "ephemeral:eph-chan");

    let store = ring_store(&m, "ephemeral:eph-chan");
    let retained = store.retained_tail(10);
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].envelope.body, "hi");
    assert_eq!(retained[0].envelope.sender, "app:pub-app@test-source");
    assert_eq!(ring_store(&m, "ephemeral:eph-chan").publish_count(), 1);
}

/// A `local:` publish runs the one ladder and, under a covering
/// `local_publish` ACL, commits to the channel's confined ring — never handed
/// to the bus (a confined channel has no wire).
#[tokio::test]
async fn a_local_publish_commits_to_the_confined_ring() {
    let m = parity_messenger(SendRate::default()).await;
    let result = send(&m, "pub-app", "local:local-chan", "hi").await;
    let PublishResult::Ok { address, .. } = result else {
        panic!("expected Ok, got {result:?}");
    };
    assert_eq!(address, "local:local-chan");

    let entry = m
        .directory()
        .resolve("local:local-chan")
        .expect("local channel resolves in the directory");
    let retained = m.ring_store_for(&entry).retained_tail(10);
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].envelope.body, "hi");
    assert_eq!(retained[0].envelope.sender, "app:pub-app@test-source");
    // The message landed on a channel that cannot leave the process: a confined
    // store issues no live receiver, so no serializer can ever be handed it.
    assert!(!m.ring_store_for(&entry).capabilities().transportable);
}

/// A `local:` publish from a sender without the `LocalPublish` grant is denied
/// at layer-1, exactly like every other scheme's grant gate.
#[tokio::test]
async fn a_local_publish_without_grant_is_denied() {
    let m = parity_messenger(SendRate::default()).await;
    let result = send(&m, "no-grants", "local:local-chan", "hi").await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "got {result:?}",
    );
    // A denial commits nothing: the ring must hold no message the gate refused.
    let entry = m
        .directory()
        .resolve("local:local-chan")
        .expect("local channel resolves in the directory");
    let store = m.ring_store_for(&entry);
    assert!(store.retained_tail(10).is_empty());
    assert_eq!(store.publish_count(), 0);
}

/// An unauthorized sender attaching `reply_to` to an existing non-durable
/// channel must get `MissingSender`, never an outcome about the reply target —
/// the reply_to arms (`AclDenied`/`UnknownChannel`/`MalformedAddress`) name a
/// second channel, so reaching one before the target's own grant gate would
/// confirm that the target channel exists.
#[tokio::test]
async fn a_nondurable_option_probe_never_reveals_channel_existence() {
    let m = parity_messenger(SendRate::default()).await;
    // `no-grants` holds neither MessagingPublish nor EphemeralPublish.
    let result = m
        .publish(
            PublishOrigin::Conversation { id: CONVERSATION },
            "no-grants",
            "ephemeral:eph-chan",
            "hi",
            Urgency::Normal,
            Some("brenn:answers"),
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "an out-of-ACL reply_to probe on an existing ephemeral channel must not \
         reach the reply_to arms; got {result:?}",
    );
}

/// A `brenn:` publish writes its row and reports the conversation's remaining
/// budget.
#[tokio::test]
async fn a_durable_publish_still_commits_to_the_database() {
    let m = parity_messenger(SendRate::default()).await;
    let addr = canonical_address(DURABLE);
    let result = send(&m, "pub-app", &addr, "hi").await;
    let PublishResult::Ok {
        remaining_budget, ..
    } = result
    else {
        panic!("got {result:?}");
    };
    assert_eq!(remaining_budget, Some(99));
}

/// The per-conversation send budget bounds what a conversation sends, not where
/// the bytes land — so it is drawn on every scheme.
#[tokio::test]
async fn the_conversation_budget_is_drawn_on_every_scheme() {
    let m = parity_messenger(SendRate::default()).await;
    let durable = canonical_address(DURABLE);
    for (addr, expected) in [(durable.as_str(), 99), ("ephemeral:eph-chan", 98)] {
        let result = send(&m, "pub-app", addr, "hi").await;
        let PublishResult::Ok {
            remaining_budget, ..
        } = result
        else {
            panic!("{addr}: got {result:?}");
        };
        assert_eq!(remaining_budget, Some(expected), "{addr}");
    }
}

// --- Gate parity ----------------------------------------------------------

#[tokio::test]
async fn a_malformed_address_is_rejected_on_every_scheme() {
    let m = parity_messenger(SendRate::default()).await;
    for addr in ["brenn:a b", "ephemeral:", "local:a/b"] {
        let result = send(&m, "pub-app", addr, "hi").await;
        assert!(
            matches!(result, PublishResult::MalformedAddress(ref a) if a == addr),
            "{addr}: got {result:?}",
        );
    }
    // An address with no recognized scheme at all fails the same gate.
    let result = send(&m, "pub-app", "weird:thing", "hi").await;
    assert!(
        matches!(result, PublishResult::MalformedAddress(ref a) if a == "weird:thing"),
        "got {result:?}",
    );
}

#[tokio::test]
async fn an_undeclared_channel_is_unknown_on_every_scheme() {
    let m = parity_messenger(SendRate::default()).await;
    for addr in ["brenn:nope", "ephemeral:nope", "local:nope"] {
        let result = send(&m, "pub-app", addr, "hi").await;
        assert!(
            matches!(result, PublishResult::UnknownChannel(ref a) if a == addr),
            "{addr}: got {result:?}",
        );
    }
}

/// Layer-1 is one gate whose *grant* follows the scheme: an app holding neither
/// grant is `MissingSender` everywhere.
#[tokio::test]
async fn a_grantless_sender_is_missing_on_every_scheme() {
    let m = parity_messenger(SendRate::default()).await;
    for addr in all_targets() {
        let result = send(&m, "no-grants", &addr, "hi").await;
        assert!(
            matches!(result, PublishResult::MissingSender),
            "{addr}: got {result:?}",
        );
        // An unknown slug lands on the same arm.
        let result = send(&m, "nobody", &addr, "hi").await;
        assert!(
            matches!(result, PublishResult::MissingSender),
            "{addr}: got {result:?}",
        );
    }
}

/// Layer-1 reads the **scheme's own** grant, not the union of the sender's
/// transport grants: `MessagingPublish` plus a `brenn_publish` matcher covering
/// the bare name does not authorize the `ephemeral:` form of that channel.
///
/// The mirror holds too, so neither half can be leaned on by the other. This is
/// the property surface fixtures rely on when one policy carries both halves
/// with disjoint matcher lists.
#[tokio::test]
async fn the_wrong_schemes_grant_does_not_authorize_a_publish() {
    let m = parity_messenger(SendRate::default()).await;

    // `brenn-only`'s `brenn_publish` matcher is `Prefix("")`, so it covers
    // `eph-chan` — only the missing `EphemeralPublish` grant denies this.
    let result = send(&m, "brenn-only", "ephemeral:eph-chan", "hi").await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "MessagingPublish must not admit an ephemeral: publish; got {result:?}",
    );
    assert_eq!(ring_store(&m, "ephemeral:eph-chan").publish_count(), 0);

    // Mirror: `eph-only`'s `ephemeral_publish` matcher covers `durable-chan`,
    // and only the missing `MessagingPublish` grant denies it.
    let result = send(&m, "eph-only", &canonical_address(DURABLE), "hi").await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "EphemeralPublish must not admit a brenn: publish; got {result:?}",
    );
    assert!(
        retained_bodies(&m, &canonical_address(DURABLE))
            .await
            .is_empty(),
        "a denied publish commits nothing",
    );
}

#[tokio::test]
async fn every_denial_bumps_the_one_denied_counter() {
    let m = parity_messenger(SendRate::default()).await;
    let sender = "app:pub-app@test-source";
    let _ = send(&m, "pub-app", "brenn:nope", "hi").await;
    let _ = send(&m, "pub-app", "ephemeral:nope", "hi").await;
    assert_eq!(m.publish_denied_count(sender, "unknown_channel"), 2);
    let _ = send(&m, "pub-app", "ephemeral:a b", "hi").await;
    assert_eq!(m.publish_denied_count(sender, "malformed_address"), 1);
    // A success bumps nothing.
    let _ = send(&m, "pub-app", "ephemeral:eph-chan", "hi").await;
    assert_eq!(m.publish_denied_count(sender, "unknown_channel"), 2);
    assert_eq!(m.publish_denied_count(sender, "malformed_address"), 1);
}

// --- Capability-gated option fields ---------------------------------------

/// `reply_to` and `delivery_deadline` are envelope metadata, not substrate
/// features: the ladder accepts both on every transportable scheme and the
/// consumer reads them back off the retained envelope whichever store holds it.
#[tokio::test]
async fn the_option_fields_are_carried_on_every_transportable_scheme() {
    let m = parity_messenger(SendRate::default()).await;
    let deadline = chrono::DateTime::from_timestamp(1_800_000_000, 0).expect("representable");
    let reply_to = canonical_address(DURABLE);

    for addr in [canonical_address(DURABLE), "ephemeral:eph-chan".to_string()] {
        let result = m
            .publish(
                PublishOrigin::Conversation { id: CONVERSATION },
                "pub-app",
                &addr,
                "hi",
                Urgency::Normal,
                Some(&reply_to),
                None,
                Some(deadline),
            )
            .await;
        assert!(
            matches!(result, PublishResult::Ok { .. }),
            "{addr}: got {result:?}",
        );
    }

    let ring = ring_store(&m, "ephemeral:eph-chan");
    let retained = ring.retained_tail(10);
    assert_eq!(retained.len(), 1);
    assert_eq!(
        retained[0].envelope.reply_to.as_deref(),
        Some(reply_to.as_str()),
        "the ring retains the resolved reply address"
    );
    assert_eq!(retained[0].envelope.delivery_deadline, Some(deadline));

    let durable = m
        .store_for_address(&canonical_address(DURABLE))
        .retained_tail(Depth::Bounded(10))
        .await;
    assert_eq!(durable.len(), 1);
    assert_eq!(
        durable[0].reply_to.as_deref(),
        Some(reply_to.as_str()),
        "the durable read-back joins the uuid to the same address"
    );
    assert_eq!(durable[0].delivery_deadline, Some(deadline));
}

/// A `reply_to` naming a channel outside the sender's visibility is refused for
/// the reply target, not for the carrying channel's class — the same arm on
/// every scheme.
#[tokio::test]
async fn an_unresolvable_reply_to_is_refused_on_every_scheme() {
    let m = parity_messenger(SendRate::default()).await;
    for addr in [canonical_address(DURABLE), "ephemeral:eph-chan".to_string()] {
        let result = m
            .publish(
                PublishOrigin::Conversation { id: CONVERSATION },
                "pub-app",
                &addr,
                "hi",
                Urgency::Normal,
                Some("brenn:no-such-answers"),
                None,
                None,
            )
            .await;
        assert!(
            matches!(&result, PublishResult::UnknownChannel(a) if a == "brenn:no-such-answers"),
            "{addr}: got {result:?}",
        );
    }
    // Nothing was committed on either side.
    assert_eq!(ring_store(&m, "ephemeral:eph-chan").publish_count(), 0);
    assert!(
        retained_bodies(&m, &canonical_address(DURABLE))
            .await
            .is_empty()
    );
}

// --- deliver_after on non-durable channels (park + release) ---------------

async fn publish_deferred(
    m: &Messenger,
    addr: &str,
    body: &str,
    deliver_after: chrono::DateTime<chrono::Utc>,
) -> PublishResult {
    m.publish(
        PublishOrigin::Conversation { id: CONVERSATION },
        "pub-app",
        addr,
        body,
        Urgency::Normal,
        None,
        Some(deliver_after),
        None,
    )
    .await
}

/// A future `deliver_after` on an `ephemeral:` channel parks the message: it is
/// in no read (not committed, not fanned out, not retained) until the release
/// pass moves it in, at which point it appears in retention.
#[tokio::test]
async fn a_future_deliver_after_parks_and_then_releases_on_an_ephemeral_channel() {
    let m = parity_messenger(SendRate::default()).await;
    // Truncated to ms: the ring store keeps release times as epoch-ms, so it
    // reports its release deadline at ms granularity.
    let base =
        chrono::DateTime::from_timestamp_millis(chrono::Utc::now().timestamp_millis()).unwrap();
    let release_at = base + chrono::Duration::seconds(30);

    assert!(matches!(
        publish_deferred(&m, "ephemeral:eph-chan", "later", release_at).await,
        PublishResult::Ok { .. }
    ));

    // Parked, not committed: no publish counted, nothing retained, one in the
    // deferred set.
    let store = ring_store(&m, "ephemeral:eph-chan");
    assert_eq!(ring_store(&m, "ephemeral:eph-chan").publish_count(), 0);
    assert!(store.retained_tail(10).is_empty());
    assert_eq!(store.deferred_len(), 1);
    assert_eq!(m.next_deferred_release().await, Some(release_at));

    // Not due yet — releases nothing.
    assert_eq!(
        m.release_due_messages(base + chrono::Duration::seconds(15))
            .await
            .released,
        0
    );
    assert_eq!(store.deferred_len(), 1);
    assert!(store.retained_tail(10).is_empty());

    // Due — released into retention, and the deferred set empties.
    assert_eq!(
        m.release_due_messages(base + chrono::Duration::seconds(45))
            .await
            .released,
        1
    );
    let tail = store.retained_tail(10);
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].envelope.body, "later");
    assert_eq!(store.deferred_len(), 0);
    assert_eq!(m.next_deferred_release().await, None);
}

/// A confined `local:` channel releases straight into its own retention — it
/// has no wire to fan out to. Parked directly on the store: a `local:` publish
/// through the ladder is still blocked on the separate local-publish ACL, so
/// this exercises the confined branch of the dispatcher release pass, not the
/// publish path.
#[tokio::test]
async fn the_release_pass_releases_a_confined_local_channels_parked_message() {
    use brenn_envelope::{ChannelScheme, MessageEnvelope};

    let m = parity_messenger(SendRate::default()).await;
    let base =
        chrono::DateTime::from_timestamp_millis(chrono::Utc::now().timestamp_millis()).unwrap();
    let release_at = base + chrono::Duration::seconds(30);

    let store = m
        .ring_stores()
        .get_by_address("local:local-chan")
        .unwrap()
        .clone();
    store
        .park(
            MessageEnvelope {
                message_id: uuid::Uuid::new_v4(),
                source: SOURCE.into(),
                channel: "local:local-chan".into(),
                sender: "app:pub-app@test-source".into(),
                publish_ts: base,
                body: "soon".to_string(),
                reply_to: None,
                delivery_deadline: None,
                deliver_after: None,
                impetus: None,
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Local,
            },
            release_at,
        )
        .expect("park");

    assert!(store.retained_tail(10).is_empty());
    assert_eq!(m.next_deferred_release().await, Some(release_at));

    assert_eq!(
        m.release_due_messages(base + chrono::Duration::seconds(45))
            .await
            .released,
        1
    );
    let tail = store.retained_tail(10);
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].envelope.body, "soon");
}

/// The retained window of any channel, read class-blind through its store.
async fn retained_bodies(m: &Messenger, addr: &str) -> Vec<String> {
    m.store_for_address(addr)
        .retained_tail(crate::config::Depth::Bounded(10))
        .await
        .into_iter()
        .map(|e| e.body.clone())
        .collect()
}

/// One release pass covers every class: a durable deferral and a ring deferral
/// coming due together are both moved into retention by the same sweep.
///
/// The durable channel here carries no subscribers, so its parked message has no
/// push claim to release — release is a property of the message, not of anyone
/// waiting for it, and a sweep that could only see held claims would leave such
/// a message parked forever.
#[tokio::test]
async fn one_release_pass_releases_a_durable_and_a_ring_deferral() {
    let m = parity_messenger(SendRate::default()).await;
    // Whole seconds: durable release times persist at second granularity.
    let base = chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp(), 0).unwrap();
    let release_at = base + chrono::Duration::seconds(30);
    let durable_addr = canonical_address(DURABLE);

    for addr in [durable_addr.as_str(), "ephemeral:eph-chan"] {
        assert!(matches!(
            publish_deferred(&m, addr, "later", release_at).await,
            PublishResult::Ok { .. }
        ));
        assert!(
            retained_bodies(&m, addr).await.is_empty(),
            "{addr}: a parked message is in no retention read"
        );
    }

    let sweep = m
        .release_due_messages(base + chrono::Duration::seconds(45))
        .await;
    assert_eq!(sweep.released, 2, "one pass releases both classes");
    assert_eq!(retained_bodies(&m, &durable_addr).await, vec!["later"]);
    assert_eq!(
        retained_bodies(&m, "ephemeral:eph-chan").await,
        vec!["later"]
    );
}

/// The release deadline is the earliest across every channel, whatever class
/// holds it — it is the one thing that decides how long the dispatcher may
/// sleep, so a class it could not see would oversleep that class's deferrals.
#[tokio::test]
async fn the_release_deadline_is_the_earliest_across_classes() {
    let m = parity_messenger(SendRate::default()).await;
    let base = chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp(), 0).unwrap();
    let soon = base + chrono::Duration::seconds(30);
    let late = base + chrono::Duration::seconds(600);
    let durable_addr = canonical_address(DURABLE);

    publish_deferred(&m, &durable_addr, "durable-late", late).await;
    publish_deferred(&m, "ephemeral:eph-chan", "ring-soon", soon).await;
    assert_eq!(
        m.next_deferred_release().await,
        Some(soon),
        "the ring's earlier deadline wins over the durable one"
    );

    // With the ring's deferral released, the durable one is the deadline — and
    // the sweep reports it itself, which is what lets the dispatcher ask each
    // store once per pass instead of walking them again for its sleep target.
    let sweep = m
        .release_due_messages(base + chrono::Duration::seconds(45))
        .await;
    assert_eq!(sweep.next_release, Some(late));
    assert_eq!(m.next_deferred_release().await, Some(late));

    let sweep = m.release_due_messages(late).await;
    assert_eq!(
        sweep.next_release, None,
        "the sweep that empties the last deferred set reports no next release"
    );
    assert_eq!(
        m.next_deferred_release().await,
        None,
        "nothing parked anywhere"
    );
}

/// A past-or-present `deliver_after` is not parked — it commits immediately,
/// exactly like a publish with no `deliver_after` at all.
#[tokio::test]
async fn a_past_deliver_after_commits_immediately() {
    let m = parity_messenger(SendRate::default()).await;
    let past = chrono::Utc::now() - chrono::Duration::seconds(30);

    assert!(matches!(
        publish_deferred(&m, "ephemeral:eph-chan", "now", past).await,
        PublishResult::Ok { .. }
    ));
    // Committed immediately: counted and retained, nothing parked.
    assert_eq!(ring_store(&m, "ephemeral:eph-chan").publish_count(), 1);
    let store = ring_store(&m, "ephemeral:eph-chan");
    assert_eq!(store.retained_tail(10).len(), 1);
    assert_eq!(store.deferred_len(), 0);
}

/// A released `ephemeral:` message enters retention at the release, which is
/// where every consumer of a ring channel reads it from — the same place a
/// released durable message lands, one sweep for both classes.
#[tokio::test]
async fn a_released_ephemeral_message_enters_retention_at_the_release() {
    let m = parity_messenger(SendRate::default()).await;
    let base = chrono::Utc::now();
    let release_at = base + chrono::Duration::seconds(30);

    assert!(matches!(
        publish_deferred(&m, "ephemeral:eph-chan", "later", release_at).await,
        PublishResult::Ok { .. }
    ));
    let store = ring_store(&m, "ephemeral:eph-chan");
    assert!(
        store.retained_tail(10).is_empty(),
        "a parked message is in retention for nobody"
    );

    assert_eq!(
        m.release_due_messages(base + chrono::Duration::seconds(45))
            .await
            .released,
        1
    );
    assert_eq!(
        store
            .retained_tail(10)
            .iter()
            .map(|r| r.envelope.body.as_str())
            .collect::<Vec<_>>(),
        vec!["later"],
        "the release is what puts it where a resume can see it"
    );
}

/// The deferred set is capped channel-wide by `retain_depth`; beyond it a
/// deferred publish is refused with the quota outcome, never drop-oldest.
#[tokio::test]
async fn deferred_quota_exceeded_at_the_channel_cap() {
    let m = parity_messenger(SendRate::default()).await;
    let release_at = chrono::Utc::now() + chrono::Duration::seconds(30);

    // retain_depth is 8, so the ninth park is refused.
    for i in 0..8 {
        assert!(matches!(
            publish_deferred(&m, "ephemeral:eph-chan", &format!("m{i}"), release_at).await,
            PublishResult::Ok { .. }
        ));
    }
    assert!(matches!(
        publish_deferred(&m, "ephemeral:eph-chan", "overflow", release_at).await,
        PublishResult::DeferredQuotaExceeded { cap: 8 }
    ));
    assert_eq!(ring_store(&m, "ephemeral:eph-chan").deferred_len(), 8);
}

/// The cap is the channel's, not the class's: a durable channel refuses beyond
/// its own `retain_depth` exactly as its ring-backed sibling does.
#[tokio::test]
async fn deferred_quota_exceeded_at_the_channel_cap_on_a_durable_channel() {
    let m = parity_messenger(SendRate::default()).await;
    let release_at = chrono::Utc::now() + chrono::Duration::seconds(30);
    let addr = canonical_address(DURABLE_CAPPED);

    for i in 0..CAPPED_DEPTH {
        assert!(matches!(
            publish_deferred(&m, &addr, &format!("m{i}"), release_at).await,
            PublishResult::Ok { .. }
        ));
    }
    let before = send_budget(&m).await;
    assert!(matches!(
        publish_deferred(&m, &addr, "overflow", release_at).await,
        PublishResult::DeferredQuotaExceeded { cap: CAPPED_DEPTH }
    ));
    assert_eq!(
        m.store_for_address(&addr).deferred_len().await,
        CAPPED_DEPTH,
        "the refused park added nothing"
    );
    assert_eq!(
        send_budget(&m).await,
        before,
        "a refused park costs the sender nothing — the draw is refunded"
    );
}

/// The conversation's remaining send budget.
async fn send_budget(m: &Messenger) -> u32 {
    let conn = m.db().lock().await;
    crate::db::read_send_budget(&conn, CONVERSATION).expect("the budget row exists")
}

/// A refused deferred publish on a ring-backed channel is equally free: the
/// refusal is discovered after the budget draw on every scheme.
#[tokio::test]
async fn a_refused_deferred_publish_refunds_the_send_budget() {
    let m = parity_messenger(SendRate::default()).await;
    let release_at = chrono::Utc::now() + chrono::Duration::seconds(30);

    for i in 0..8 {
        assert!(matches!(
            publish_deferred(&m, "ephemeral:eph-chan", &format!("m{i}"), release_at).await,
            PublishResult::Ok { .. }
        ));
    }
    let before = send_budget(&m).await;
    assert!(matches!(
        publish_deferred(&m, "ephemeral:eph-chan", "overflow", release_at).await,
        PublishResult::DeferredQuotaExceeded { cap: 8 }
    ));
    assert_eq!(send_budget(&m).await, before);
}

// --- Send-rate gate -------------------------------------------------------

fn tight_rate() -> SendRate {
    SendRate {
        burst: 2,
        refill_interval_secs: 1,
        refill: 1,
    }
}

/// A `brenn:` publisher is rate-gated by the per-(sender, channel) send-rate
/// gate.
#[tokio::test(start_paused = true)]
async fn the_send_rate_gate_fires_on_a_durable_channel() {
    let m = parity_messenger(tight_rate()).await;
    let addr = canonical_address(DURABLE);
    for _ in 0..2 {
        assert!(matches!(
            send(&m, "pub-app", &addr, "x").await,
            PublishResult::Ok { .. }
        ));
    }
    let result = send(&m, "pub-app", &addr, "x").await;
    assert!(matches!(result, PublishResult::RateLimited), "{result:?}");
    assert_eq!(m.publish_rate_limited_count("app:pub-app@test-source"), 1);

    // A whole refill interval returns exactly `refill` tokens.
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert!(matches!(
        send(&m, "pub-app", &addr, "x").await,
        PublishResult::Ok { .. }
    ));
    assert!(matches!(
        send(&m, "pub-app", &addr, "x").await,
        PublishResult::RateLimited
    ));
}

/// The grain is `(sender, channel)`: draining one channel's bucket leaves the
/// same sender's budget on every other channel intact.
#[tokio::test(start_paused = true)]
async fn the_send_rate_gate_is_per_channel() {
    let m = parity_messenger(tight_rate()).await;
    let addr = canonical_address(DURABLE);
    for _ in 0..2 {
        let _ = send(&m, "pub-app", &addr, "x").await;
    }
    assert!(matches!(
        send(&m, "pub-app", &addr, "x").await,
        PublishResult::RateLimited
    ));
    assert!(matches!(
        send(&m, "pub-app", "ephemeral:eph-chan", "x").await,
        PublishResult::Ok { .. }
    ));
}

/// A publish the earlier gates doom spends no rate token.
#[tokio::test(start_paused = true)]
async fn a_doomed_publish_consumes_no_rate_token() {
    let m = parity_messenger(tight_rate()).await;
    for _ in 0..10 {
        let _ = send(&m, "pub-app", "ephemeral:eph-chan", &"x".repeat(999_999)).await;
    }
    assert_eq!(m.publish_rate_limited_count("app:pub-app@test-source"), 0);
    assert!(matches!(
        send(&m, "pub-app", "ephemeral:eph-chan", "x").await,
        PublishResult::Ok { .. }
    ));
}

#[tokio::test]
async fn an_oversized_body_is_rejected_on_every_scheme() {
    let m = parity_messenger(SendRate::default()).await;
    let body = "x".repeat(m.defaults.max_body_bytes + 1);
    for addr in [canonical_address(DURABLE), "ephemeral:eph-chan".to_string()] {
        let result = send(&m, "pub-app", &addr, &body).await;
        assert!(
            matches!(result, PublishResult::BodyTooLarge { .. }),
            "{addr}: got {result:?}",
        );
    }
}

//! The mint gate and the envelope field: who may claim carried
//! user-interaction authority, what happens to a claim nobody authorized, and
//! that a claim that lands survives every path a stored message is read back on.
//!
//! No production entry wrapper exposes the field, so these drive `publish_core`
//! directly under directly-constructed policies.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::config::{Depth, MessagingGlobalConfig, ResolvedMessagingConfig};
use crate::db::init_db_memory;
use crate::db::{load_channel_retained_tail, load_envelope_by_uuid, read_send_budget};
use crate::query::MessageQuery;
use crate::store::RingStores;
use crate::testutils::{ephemeral_channel_entry, test_channel_entry};
use crate::{Impetus, MessagingDirectory, WakeRouter, db::upsert_channels};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::acl::ChannelMatcher;
use indexmap::IndexMap;
use std::sync::Arc;

const SOURCE: &str = "test-source";
const APP: &str = "pub-app";
const DURABLE_NAME: &str = "records";
const EPHEMERAL_ADDR: &str = "ephemeral:ticks";
const CONVERSATION: i64 = 1;

/// Whether the fixture's publisher holds the mint capability. The two policies
/// differ in that one grant and nothing else, so a difference in outcome is
/// attributable to the gate under test.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mint {
    Granted,
    Withheld,
}

fn publisher(mint: Mint) -> brenn_lib::config::AppConfig {
    let mut cfg = test_app_config(
        APP,
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
    cfg.policy.grants.insert(AppCapability::EphemeralSubscribe);
    cfg.policy
        .acls
        .ephemeral_subscribe
        .push(ChannelMatcher::Prefix(String::new()));
    if mint == Mint::Granted {
        cfg.policy.grants.insert(AppCapability::MintImpetus);
    }
    cfg
}

/// One durable channel and one ephemeral channel, both inside the publisher's
/// ACLs, plus the conversation row a `Conversation`-origin publish is
/// attributed to.
async fn messenger(mint: Mint) -> Arc<Messenger> {
    let db = init_db_memory();
    let durable = test_channel_entry(DURABLE_NAME, vec![]);
    let ephemeral = ephemeral_channel_entry("ticks", 8);
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
        upsert_channels(&conn, std::slice::from_ref(&durable));
    }

    let nondurable = [ephemeral.clone()];
    let directory = Arc::new(MessagingDirectory::with_entries(vec![durable, ephemeral]));

    let mut apps: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
    apps.insert(APP.to_string(), publisher(mint));

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

fn durable_addr() -> String {
    crate::canonical_address(DURABLE_NAME)
}

/// Publish as the fixture's app with an explicit impetus. The `Conversation`
/// origin is what makes the budget observable, so a refusal that drew one would
/// show up.
async fn publish_with(m: &Messenger, addr: &str, impetus: Option<Impetus>) -> crate::PublishResult {
    m.publish_core(
        crate::PublishOrigin::Conversation { id: CONVERSATION },
        PublishPrincipal::App { slug: APP },
        addr,
        "hi",
        Urgency::Low,
        None,
        None,
        None,
        impetus,
    )
    .await
}

// ── The gate ─────────────────────────────────────────────────────────────

#[tokio::test]
#[tracing_test::traced_test]
async fn a_claim_without_the_mint_grant_is_refused_whole() {
    let m = messenger(Mint::Withheld).await;
    let result = publish_with(&m, &durable_addr(), Some(Impetus::Replenish)).await;
    assert!(
        matches!(result, PublishResult::ImpetusUnauthorized),
        "expected ImpetusUnauthorized, got {result:?}"
    );

    // Nothing stored: the refusal is of the publish, not of the field.
    let conn = m.db.lock().await;
    let entry = m.directory.resolve(&durable_addr()).unwrap();
    assert!(
        load_channel_retained_tail(&conn, entry.uuid, Depth::Unbounded).is_empty(),
        "a refused mint must leave no message behind"
    );
    // No budget row at all: the draw is downstream of the gate, and a
    // `decrement_send_budget` would have seeded the row at its ceiling.
    assert_eq!(read_send_budget(&conn, CONVERSATION), None);
    drop(conn);

    // The rate token is likewise untouched: the same publish, now without the
    // claim, is admitted rather than rate-limited.
    assert!(matches!(
        publish_with(&m, &durable_addr(), None).await,
        PublishResult::Ok { .. }
    ));

    assert!(logs_contain("impetus_mint_denied"));
    assert!(logs_contain(&format!("app:{APP}@{SOURCE}")));
}

#[tokio::test]
async fn the_same_claim_lands_under_a_policy_holding_the_grant() {
    let m = messenger(Mint::Granted).await;
    let result = publish_with(&m, &durable_addr(), Some(Impetus::Replenish)).await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "expected Ok, got {result:?}"
    );
}

/// Minting and channel scope are two independent factors: holding the mint
/// grant buys no reach, and the ACL answer comes first so a claim on a channel
/// the sender cannot publish to hears about the channel, not about the mint.
#[tokio::test]
async fn minting_confers_no_channel_reach() {
    let m = messenger(Mint::Granted).await;
    let new_apps = {
        let mut apps = (*m.apps).clone();
        apps.get_mut(APP).unwrap().policy.acls.brenn_publish.clear();
        Arc::new(apps)
    };
    let mut m = m;
    Arc::get_mut(&mut m).unwrap().apps = new_apps;

    let result = publish_with(&m, &durable_addr(), Some(Impetus::Replenish)).await;
    assert!(
        matches!(result, PublishResult::AclDenied(_)),
        "expected AclDenied, got {result:?}"
    );
}

/// The layer-1 grant gate still answers first for a sender with no publish
/// authority at all — attaching impetus must not turn a `MissingSender` into a
/// different, channel-revealing answer.
#[tokio::test]
async fn a_sender_without_publish_authority_still_hears_missing_sender() {
    let m = messenger(Mint::Granted).await;
    let new_apps = {
        let mut apps = (*m.apps).clone();
        let app = apps.get_mut(APP).unwrap();
        app.policy = brenn_lib::access::AppPolicy::with_grants(&[AppCapability::MintImpetus]);
        Arc::new(apps)
    };
    let mut m = m;
    Arc::get_mut(&mut m).unwrap().apps = new_apps;

    let result = publish_with(&m, &durable_addr(), Some(Impetus::Replenish)).await;
    assert!(
        matches!(result, PublishResult::MissingSender),
        "expected MissingSender, got {result:?}"
    );
}

// ── Retention ────────────────────────────────────────────────────────────

/// Every durable read-back path decodes the column: the retained window and the
/// history query (`query::row_to_envelope`) and the by-uuid load
/// (`row_to_message_envelope`).
#[tokio::test]
async fn impetus_survives_the_durable_column_on_every_decoder() {
    let m = messenger(Mint::Granted).await;
    let addr = durable_addr();
    let message_id = match publish_with(&m, &addr, Some(Impetus::Replenish)).await {
        PublishResult::Ok { message_id, .. } => message_id,
        other => panic!("expected Ok, got {other:?}"),
    };
    // A second, impetus-free message on the same channel: the column is
    // per-row, and a decoder that hard-coded either answer would fail one half.
    assert!(matches!(
        publish_with(&m, &addr, None).await,
        PublishResult::Ok { .. }
    ));

    let entry = m.directory.resolve(&addr).unwrap();
    {
        let conn = m.db.lock().await;
        let tail = load_channel_retained_tail(&conn, entry.uuid, Depth::Unbounded);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].1.impetus, Some(Impetus::Replenish));
        assert_eq!(tail[1].1.impetus, None);

        let loaded = load_envelope_by_uuid(&conn, message_id).expect("the message is stored");
        assert_eq!(loaded.impetus, Some(Impetus::Replenish));
    }

    let history = m
        .query(&MessageQuery {
            channel: addr,
            limit: 10,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: APP.to_string(),
        })
        .await
        .expect("the publisher may read its own channel");
    assert_eq!(history.len(), 2);
    // Newest first.
    assert_eq!(history[0].impetus, None);
    assert_eq!(history[1].impetus, Some(Impetus::Replenish));
}

#[tokio::test]
async fn impetus_rides_the_ephemeral_ring() {
    let m = messenger(Mint::Granted).await;
    assert!(matches!(
        publish_with(&m, EPHEMERAL_ADDR, Some(Impetus::Replenish)).await,
        PublishResult::Ok { .. }
    ));
    let retained = m
        .ring_stores()
        .get_by_address(EPHEMERAL_ADDR)
        .expect("a registered non-durable channel")
        .retained_tail(10);
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].envelope.impetus, Some(Impetus::Replenish));
}

/// A parked message keeps its impetus: authority was established at publish, and
/// when the message is processed is the ordinary bus question.
#[tokio::test]
async fn a_parked_message_keeps_its_impetus() {
    let m = messenger(Mint::Granted).await;
    let addr = durable_addr();
    let release_at = chrono::Utc::now() + chrono::Duration::hours(1);
    let message_id = match m
        .publish_core(
            crate::PublishOrigin::Conversation { id: CONVERSATION },
            PublishPrincipal::App { slug: APP },
            &addr,
            "later",
            Urgency::Low,
            None,
            Some(release_at),
            None,
            Some(Impetus::Replenish),
        )
        .await
    {
        PublishResult::Ok { message_id, .. } => message_id,
        other => panic!("expected Ok, got {other:?}"),
    };
    let conn = m.db.lock().await;
    let parked = load_envelope_by_uuid(&conn, message_id).expect("the parked row is stored");
    assert_eq!(parked.impetus, Some(Impetus::Replenish));
}

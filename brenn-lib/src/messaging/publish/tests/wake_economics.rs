//! Per-registration wake economics: `eager_wake` on the persisted push row is
//! derived from the subscriber's declared [`WakeEconomics`], not from a global
//! `wake_min` formula.
//!
//! - `Eager` subscribers (parked WASM/system consumers, attached surface
//!   sessions) are always created eager, regardless of `wake_min` or urgency —
//!   this is the structural fix for the stranded-surface-push bug, where a
//!   below-`wake_min` publish to a live surface session wrote `eager_wake = 0`
//!   and parked invisibly.
//! - `UrgencyGated` subscribers (LLM conversations) keep the urgency gate:
//!   `eager_wake = wake_min.wakes(urgency)`, exactly as before.

use super::super::*;
use super::CountingRouter;
use crate::access::acl::ChannelMatcher;
use crate::access::{AppCapability, AppPolicy};
use crate::db::init_db_memory;
use crate::messaging::config::{
    Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig,
    ResolvedSubscription, Sink,
};
use crate::messaging::db::upsert_channels;
use crate::messaging::test_support::test_app_config;
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, ParticipantId, SubscriberEntry,
    SubscriberEntryKind, Urgency, WakeMin, WakeRouter, canonical_address,
};
use indexmap::IndexMap;
use std::sync::Arc;
use uuid::Uuid;

const PUBLISHER: &str = "test-publisher";

/// Universal system publish policy.
fn publisher_policy() -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::MessagingPublish);
    p.acls
        .brenn_publish
        .push(ChannelMatcher::Prefix(String::new()));
    p
}

/// Universal `brenn_subscribe` delivery policy so a fan-out row is not dropped at
/// the delivery-time ACL gate.
fn receiver_policy() -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::MessagingSubscribe);
    p.acls
        .brenn_subscribe
        .push(ChannelMatcher::Prefix(String::new()));
    p
}

/// Read the `eager_wake` flag of the single pending push row targeting
/// `subscriber` (asserts exactly one row exists).
async fn eager_wake_of(m: &Arc<Messenger>, subscriber: &ParticipantId) -> bool {
    let conn = m.db().lock().await;
    conn.query_row(
        "SELECT pp.eager_wake FROM messaging_pending_pushes pp \
         WHERE pp.target_subscriber = ?1",
        rusqlite::params![subscriber.as_str()],
        |r| Ok(r.get::<_, i64>(0)? != 0),
    )
    .expect("exactly one pending push row for the subscriber")
}

/// Build a `Messenger` whose one `brenn:` channel carries a single surface
/// subscriber (`Eager`), with a `High` channel-level `wake_min` (so the old
/// global formula would gate a `Low`/`VeryLow` publish to `eager_wake = 0`).
async fn build_surface_subscriber_messenger(surface_slug: &str) -> (Arc<Messenger>, String) {
    let db = init_db_memory();
    let channel_addr = canonical_address("eager-ch");
    let entry = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            // High so wakes(Low) == false under the pre-fix global formula.
            wake_min: WakeMin::High,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Surface {
                slug: surface_slug.to_string(),
                instance: None,
            },
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let mut system_policies = std::collections::HashMap::new();
    system_policies.insert(PUBLISHER.to_string(), publisher_policy());
    let mut surface_policies = std::collections::HashMap::new();
    surface_policies.insert(surface_slug.to_string(), receiver_policy());
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::system_registrations(
        system_policies,
    ))
    .with_subscriber_registrations(crate::messaging::testutils::surface_registrations(
        surface_policies,
    ));
    (messenger, channel_addr)
}

/// The stranded-surface-push fix: a below-`wake_min` publish to an `Eager`
/// (surface) subscriber still creates an eager push row — it is not stranded.
#[tokio::test]
async fn eager_surface_subscriber_woken_below_wake_min() {
    let (m, addr) = build_surface_subscriber_messenger("deskbar").await;
    let r = m
        .publish_from_system(PUBLISHER, &addr, "quiet", Urgency::Low, None)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "publish ok, got {r:?}"
    );
    assert!(
        eager_wake_of(&m, &ParticipantId::for_surface("deskbar")).await,
        "Low-urgency publish to a High-wake_min surface subscriber must still be eager \
         (the stranded-surface-push fix); pre-fix this wrote eager_wake = 0 and \
         stranded the row"
    );
}

/// `Eager` ignores urgency entirely: even the lowest urgency is delivered eager.
#[tokio::test]
async fn eager_surface_subscriber_ignores_urgency() {
    let (m, addr) = build_surface_subscriber_messenger("deskbar").await;
    let r = m
        .publish_from_system(PUBLISHER, &addr, "quietest", Urgency::VeryLow, None)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "publish ok, got {r:?}"
    );
    assert!(
        eager_wake_of(&m, &ParticipantId::for_surface("deskbar")).await,
        "an Eager subscriber never consults wake_min, so even VeryLow is eager"
    );
}

/// Build a `Messenger` whose one `brenn:` channel carries a single app-backed
/// (`UrgencyGated`) subscriber with the given subscription `wake_min`, plus a
/// system publisher. The app's singleton conversation resolves against `alice`.
async fn build_app_subscriber_messenger(sub_wake_min: WakeMin) -> (Arc<Messenger>, String, i64) {
    let db = init_db_memory();
    let conversation_id = 1;
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'alice', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
    }
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("gated-ch");
    let entry = ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: sub_wake_min,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::App("sub-app".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(sub_wake_min),
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));

    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        "sub-app".to_string(),
        test_app_config(
            "sub-app",
            Some(ResolvedMessagingConfig {
                send_budget: 10_000,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid,
                    channel_address: channel_addr.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: sub_wake_min,
                }],
            }),
            vec!["alice".to_string()],
        ),
    );
    // Grant the app a universal subscribe ACL so the delivery-time gate admits it.
    apps_raw
        .get_mut("sub-app")
        .unwrap()
        .policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Prefix(String::new()));

    let mut system_policies = std::collections::HashMap::new();
    system_policies.insert(PUBLISHER.to_string(), publisher_policy());
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(apps_raw),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::system_registrations(
        system_policies,
    ));
    (messenger, channel_addr, conversation_id)
}

/// `UrgencyGated` (app/conversation) subscribers keep the urgency gate: a
/// below-`wake_min` publish parks (`eager_wake = 0`).
#[tokio::test]
async fn urgency_gated_app_subscriber_parks_below_wake_min() {
    let (m, addr, conv) = build_app_subscriber_messenger(WakeMin::High).await;
    let r = m
        .publish_from_system(PUBLISHER, &addr, "quiet", Urgency::Low, None)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "publish ok, got {r:?}"
    );
    assert!(
        !eager_wake_of(&m, &ParticipantId::for_conversation(conv)).await,
        "a Low publish below a High wake_min for an UrgencyGated subscriber parks (eager_wake = 0)"
    );
}

/// `UrgencyGated` subscribers still wake at or above their `wake_min`.
#[tokio::test]
async fn urgency_gated_app_subscriber_wakes_at_threshold() {
    let (m, addr, conv) = build_app_subscriber_messenger(WakeMin::High).await;
    let r = m
        .publish_from_system(PUBLISHER, &addr, "loud", Urgency::High, None)
        .await;
    assert!(
        matches!(r, PublishResult::Ok { .. }),
        "publish ok, got {r:?}"
    );
    assert!(
        eager_wake_of(&m, &ParticipantId::for_conversation(conv)).await,
        "a High publish meeting a High wake_min for an UrgencyGated subscriber wakes eagerly"
    );
}

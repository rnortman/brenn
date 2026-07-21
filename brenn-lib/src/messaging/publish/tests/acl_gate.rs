//! Delivery-time ACL gate at the fan-out chokepoint (design §2.2,
//! "Enforcement point A"). `resolve_push_targets` re-authorizes **every**
//! subscriber — `App` and `Wasm`, static and dynamic — against its current
//! `AppPolicy` via `subscriber_policy` + `allows_channel_access`. A subscriber whose
//! policy no longer covers the channel is skipped: no pending-push row is
//! persisted for it. There is no static/dynamic branch — the gate is uniform.
//!
//! These tests pin the universality the requirement demands: a *static*-style
//! subscriber (one with no `DynamicSubscribe` grant — it never used the runtime
//! tool) is denied when its policy carries no covering matcher and kept when it
//! does, and the same holds for `Wasm` subscribers via `wasm_policies`.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::access::{AppCapability, AppPolicy, acl::ChannelMatcher};
use crate::db::init_db_memory;
use crate::messaging::config::{
    Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig,
    ResolvedSubscription, Sink,
};
use crate::messaging::db::upsert_channels;
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind, Urgency,
    WakeMin, WakeRouter, canonical_address,
};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// A policy authorizing brenn delivery on the test channel: `MessagingSubscribe`
/// grant + a covering `brenn_subscribe` matcher, and (when `dynamic`) the
/// `DynamicSubscribe` grant. The `_delivery` decision must **ignore**
/// `DynamicSubscribe`, so the `dynamic = false` (static-style) policy must still
/// permit delivery — that is the load-bearing universality assertion.
fn brenn_policy(covers: bool, dynamic: bool) -> AppPolicy {
    let mut p = AppPolicy::default();
    p.grants.insert(AppCapability::MessagingSubscribe);
    if dynamic {
        p.grants.insert(AppCapability::DynamicSubscribe);
    }
    if covers {
        p.acls
            .brenn_subscribe
            .push(ChannelMatcher::Exact("acl-gate-ch".to_string()));
    }
    p
}

/// Build a `Messenger` whose single `brenn:acl-gate-ch` channel has one `App`
/// subscriber (`acl-app`, singleton, user `recv`) whose policy is `app_policy`.
/// A sender app (`acl-sender`, user `sender`) exists for publish auth.
async fn build_app_gate_messenger(app_policy: AppPolicy) -> (Arc<Messenger>, Arc<CountingRouter>) {
    let db = init_db_memory();
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("acl-gate-ch");
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
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::App("acl-app".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'sender', 'h', '2024-01-01'), (2, 'recv', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'acl-sender', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let router = Arc::new(CountingRouter::default());
    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        "acl-sender".to_string(),
        test_app_config(
            "acl-sender",
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["sender".to_string()],
        ),
    );
    // Subscriber app: take the default config (singleton, user `recv`), then
    // overwrite its policy with the test's `app_policy` so the gate decision is
    // exactly what the test controls.
    let mut recv_app = test_app_config(
        "acl-app",
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![ResolvedSubscription {
                channel_uuid,
                channel_address: channel_addr.clone(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            }],
        }),
        vec!["recv".to_string()],
    );
    recv_app.policy = app_policy;
    apps_raw.insert("acl-app".to_string(), recv_app);

    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("acl-test"),
        Arc::new(apps_raw),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    (messenger, router)
}

async fn publish_to_gate(m: &Arc<Messenger>) -> PublishResult {
    m.publish(
        crate::messaging::PublishOrigin::Conversation { id: 1 },
        "acl-sender",
        &canonical_address("acl-gate-ch"),
        "msg",
        Urgency::Normal,
        None,
        None,
        None,
    )
    .await
}

/// Static-style App subscriber (no `DynamicSubscribe`) with a covering matcher is
/// **kept**: a pending-push row is persisted. Proves the gate authorizes a
/// subscriber that never touched the runtime subscribe tool.
#[tokio::test]
async fn app_subscriber_with_covering_matcher_is_kept() {
    let (m, _r) = build_app_gate_messenger(brenn_policy(true, false)).await;
    let result = publish_to_gate(&m).await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "publish must succeed, got {result:?}"
    );
    // Subscriber `acl-app` gets singleton conversation id 2 (user `recv`).
    let rows = m
        .load_pending_pushes(&ParticipantId::for_conversation(2))
        .await;
    assert_eq!(
        rows.len(),
        1,
        "covering-matcher subscriber must get a pending-push row"
    );
}

/// App subscriber whose policy carries **no** covering matcher is **denied**: no
/// pending-push row is persisted. This is the intended deny-by-default
/// consequence of universal enforcement (design §3, static-without-matcher).
#[tokio::test]
async fn app_subscriber_without_covering_matcher_is_denied() {
    let (m, _r) = build_app_gate_messenger(brenn_policy(false, false)).await;
    let result = publish_to_gate(&m).await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "publish itself still succeeds (sender authorized); the subscriber is gated, got {result:?}"
    );
    // The singleton conversation may be created during resolution, but no
    // pending-push row may be persisted for the denied subscriber.
    let rows = m
        .load_pending_pushes(&ParticipantId::for_conversation(2))
        .await;
    assert!(
        rows.is_empty(),
        "subscriber without a covering matcher must be denied at the fan-out gate, got {} row(s)",
        rows.len()
    );
}

/// A `Wasm(slug)` subscriber is gated via `wasm_policies`: kept when its policy
/// covers the channel, denied when it does not. Pins that universal enforcement
/// reaches WASM consumers (design §2.2, WASM subscribers in scope this cycle).
async fn build_wasm_gate_messenger(
    wasm_policy: Option<AppPolicy>,
) -> (Arc<Messenger>, Arc<CountingRouter>) {
    let db = init_db_memory();
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("acl-gate-ch");
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
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm("acl-wasm".to_string()),
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
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'sender', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'acl-sender', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let router = Arc::new(CountingRouter::default());
    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        "acl-sender".to_string(),
        test_app_config(
            "acl-sender",
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["sender".to_string()],
        ),
    );
    let mut wasm_policies: HashMap<String, AppPolicy> = HashMap::new();
    if let Some(p) = wasm_policy {
        wasm_policies.insert("acl-wasm".to_string(), p);
    }
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("acl-test"),
        Arc::new(apps_raw),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
        wasm_policies,
    ));
    (messenger, router)
}

#[tokio::test]
async fn wasm_subscriber_with_covering_matcher_is_kept() {
    let (m, _r) = build_wasm_gate_messenger(Some(brenn_policy(true, false))).await;
    let result = publish_to_gate(&m).await;
    assert!(matches!(result, PublishResult::Ok { .. }), "got {result:?}");
    let rows = m
        .load_pending_pushes(&ParticipantId::for_wasm("acl-wasm"))
        .await;
    assert_eq!(
        rows.len(),
        1,
        "covering-matcher Wasm subscriber must be kept"
    );
}

#[tokio::test]
async fn wasm_subscriber_without_covering_matcher_is_denied() {
    let (m, _r) = build_wasm_gate_messenger(Some(brenn_policy(false, false))).await;
    let result = publish_to_gate(&m).await;
    assert!(matches!(result, PublishResult::Ok { .. }), "got {result:?}");
    let rows = m
        .load_pending_pushes(&ParticipantId::for_wasm("acl-wasm"))
        .await;
    assert!(
        rows.is_empty(),
        "Wasm subscriber without a covering matcher must be denied, got {} row(s)",
        rows.len()
    );
}

/// A live `Wasm` subscriber with **no policy at all** in `wasm_policies` is a
/// host wiring bug; the delivery gate fails closed (deny), it does not panic.
#[tokio::test]
async fn wasm_subscriber_with_no_policy_is_denied_not_panicked() {
    let (m, _r) = build_wasm_gate_messenger(None).await;
    let result = publish_to_gate(&m).await;
    assert!(matches!(result, PublishResult::Ok { .. }), "got {result:?}");
    let rows = m
        .load_pending_pushes(&ParticipantId::for_wasm("acl-wasm"))
        .await;
    assert!(
        rows.is_empty(),
        "Wasm subscriber with no resolvable policy must fail closed (deny), got {} row(s)",
        rows.len()
    );
}

mod acl_gate;
mod dispatch_any;
mod dispatch_row;
mod ingress;
mod overflow;
mod publish_core;
mod surface;
mod system;
mod transport_ingress;
mod wake_economics;
mod wasm;

use super::*;
use crate::db::init_db_memory;
use crate::messaging::config::{
    Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig,
    ResolvedSubscription, Sink,
};
use crate::messaging::db::upsert_channels;
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind, WakeMin,
    WakeRouter, canonical_address,
};
use indexmap::IndexMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Track router calls in a thread-safe accumulator for tests.
/// `deliver_returns` controls the `deliver` return:
///   0 → `Ok(false)` (sleeping bridge),
///   1 → `Ok(true)`  (active bridge accepted),
///   2 → `Err(...)`  (bridge raced with shutdown / send failed) —
///       used by F26 tests to exercise the `dispatch_row` Err arm.
#[derive(Default)]
pub(super) struct CountingRouter {
    // Records (subscriber, formatted_envelope) per deliver() call.
    // Stores ParticipantId directly so the mock stays opaque — test assertions
    // call as_conversation_id() or compare ParticipantId directly rather than
    // baking the conversation-kind assumption into shared mock infrastructure.
    pub(super) deliveries: tokio::sync::Mutex<Vec<(ParticipantId, String)>>,
    pub(super) deliver_returns: AtomicU64,
    pub(super) eager_wakes: AtomicU64,
    pub(super) alarms: AtomicU64,
}

#[async_trait::async_trait]
impl WakeRouter for CountingRouter {
    async fn deliver(
        &self,
        _key: &crate::messaging::SubscriberEntryKind,
        subscriber: &ParticipantId,
        envelope: &crate::messaging::MessageEnvelope,
        _push_id: i64,
        _seq: i64,
    ) -> Result<bool, String> {
        use crate::messaging::format::format_messaging_event_single;
        self.deliveries
            .lock()
            .await
            .push((subscriber.clone(), format_messaging_event_single(envelope)));
        match self.deliver_returns.load(Ordering::SeqCst) {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("simulated bridge-died-mid-send".to_string()),
        }
    }
    async fn deliver_ingress(
        &self,
        _key: &crate::messaging::SubscriberEntryKind,
        subscriber: &ParticipantId,
        event: &crate::messaging::ingress::Event,
    ) -> Result<bool, String> {
        self.deliveries
            .lock()
            .await
            .push((subscriber.clone(), format!("ingress:{}", event.source)));
        match self.deliver_returns.load(Ordering::SeqCst) {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("simulated bridge-died-mid-send".to_string()),
        }
    }
    fn spawn_eager_wake(
        &self,
        _key: &crate::messaging::SubscriberEntryKind,
        _subscriber: &ParticipantId,
    ) {
        self.eager_wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn delivery_shape(
        &self,
        key: &crate::messaging::SubscriberEntryKind,
    ) -> crate::messaging::DeliveryShape {
        crate::messaging::default_delivery_shape(key)
    }

    fn alarm(&self, _channel: &str, _subscriber: &ParticipantId) {
        self.alarms.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) async fn build_messenger(
    deliver_returns: u64,
) -> (Arc<Messenger>, Uuid, i64, i64, Arc<CountingRouter>) {
    let db = init_db_memory();
    let conn = db.lock().await;
    // Users / conversations.
    conn.execute(
        "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'bob', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (2, 'alice', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'pa-bob', '2024-01-01', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (2, 2, 'active', 'pa-alice', '2024-01-01', '2024-01-01')",
        [],
    )
    .unwrap();
    let channel_uuid = Uuid::new_v4();
    let entry = ChannelEntry {
        uuid: channel_uuid,
        address: canonical_address("pa-alice"),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        // Subscribers populated below: pa-alice is push-enabled (Unbounded).
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::App("pa-alice".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    upsert_channels(&conn, std::slice::from_ref(&entry));
    drop(conn);

    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));

    // Build apps map. pa-bob = sender; pa-alice = subscriber target.
    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        "pa-bob".to_string(),
        test_app_config(
            "pa-bob",
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["bob".to_string()],
        ),
    );
    apps_raw.insert(
        "pa-alice".to_string(),
        test_app_config(
            "pa-alice",
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid,
                    channel_address: canonical_address("pa-alice"),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                }],
            }),
            vec!["alice".to_string()],
        ),
    );
    let apps = Arc::new(apps_raw);

    let router = Arc::new(CountingRouter::default());
    router
        .deliver_returns
        .store(deliver_returns, Ordering::SeqCst);
    let messenger = Messenger::new(
        db.clone(),
        directory,
        Arc::from("test-source"),
        apps,
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    (messenger, channel_uuid, 1, 2, router)
}

pub(super) fn test_app_config(
    slug: &str,
    messaging: Option<ResolvedMessagingConfig>,
    allowed_users: Vec<String>,
) -> crate::config::AppConfig {
    crate::messaging::test_support::test_app_config(slug, messaging, allowed_users)
}

//! Shared test infrastructure for the automation module tree.
//!
//! Exposes `FakeIngressRouter`, `FakeWakeRouter`, `default_app_cfg`,
//! `make_engine_full`, and `make_engine_with_apps` so `fire.rs`,
//! `loop_task.rs`, `startup.rs`, and `mod.rs` tests don't each maintain
//! separate copies. Adding a field to `AppConfig` or changing a trait
//! signature requires only one edit here.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::AutomationGlobalConfig;
use crate::{AutomationEngine, IngressRouter};
use brenn_db::Db;
use brenn_lib::config::AppConfig;
use brenn_lib::messaging::{MessageEnvelope, MessagingDirectory, MessagingGlobalConfig, Urgency};
use brenn_messaging::{Messenger, WakeRouter};
use brenn_obs::alerting::AlertDispatcher;

/// Open an in-memory database with every migration set the automation engine
/// needs: `brenn_lib`'s slice plus this crate's own tables.
///
/// Matches the production composition, so no test asserts against a schema the
/// running engine would not have.
pub(super) fn init_db_memory() -> Db {
    let conn = brenn_db::open_connection_memory();
    brenn_messaging_store::db::run_slice_migrations(&conn);
    crate::schema::run_automation_migrations(&conn);
    brenn_db::into_db(conn)
}

/// Ingress router that records submitted events for later assertion.
pub(super) struct FakeIngressRouter {
    #[allow(clippy::type_complexity)]
    pub(super) events: Mutex<Vec<(i64, String, String, String, String, Urgency)>>,
}

impl FakeIngressRouter {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
        })
    }

    pub(super) async fn events(&self) -> Vec<(i64, String, String, String, String, Urgency)> {
        self.events.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl IngressRouter for FakeIngressRouter {
    async fn submit_ingress(
        &self,
        conversation_id: i64,
        app_slug: &str,
        source: &str,
        summary: &str,
        payload: &str,
        urgency: Urgency,
    ) {
        self.events.lock().await.push((
            conversation_id,
            app_slug.to_string(),
            source.to_string(),
            summary.to_string(),
            payload.to_string(),
            urgency,
        ));
    }
}

/// Wake router stub: every conversation is inactive, deliver is a no-op.
#[derive(Default)]
pub(super) struct FakeWakeRouter;

#[async_trait::async_trait]
impl WakeRouter for FakeWakeRouter {
    async fn deliver(
        &self,
        _key: &brenn_lib::messaging::SubscriberEntryKind,
        _envelope: &std::sync::Arc<MessageEnvelope>,
        _retained_seq: i64,
    ) -> Result<bool, String> {
        Ok(false)
    }
    async fn deliver_ingress(
        &self,
        _key: &brenn_lib::messaging::SubscriberEntryKind,
        _: &brenn_lib::messaging::ParticipantId,
        _event: &brenn_messaging_store::ingress::Event,
    ) -> Result<bool, String> {
        Ok(false)
    }
    fn spawn_eager_wake(
        &self,
        _key: &brenn_lib::messaging::SubscriberEntryKind,
        _: &brenn_lib::messaging::ParticipantId,
    ) {
    }
    fn delivery_shape(
        &self,
        key: &brenn_lib::messaging::SubscriberEntryKind,
    ) -> brenn_messaging::DeliveryShape {
        brenn_messaging::default_delivery_shape(key)
    }
    fn alarm(
        &self,
        _channel: &str,
        _subscriber: &brenn_lib::messaging::ParticipantId,
        _count: u64,
    ) {
    }
}

/// Build a minimal `AppConfig` for test engines.
///
/// `slug` — app slug (also used as name).
/// `singleton` — sets `singleton` flag; most automation tests want `true`.
pub(super) fn default_app_cfg(slug: &str, singleton: bool) -> AppConfig {
    default_app_cfg_with_subscriptions(slug, singleton, vec![])
}

/// Like `default_app_cfg` but with an explicit subscription list. Use when the
/// test directory has a push-enabled subscriber entry for this app, so the app's
/// own config and the directory agree about what it subscribes to.
pub(super) fn default_app_cfg_with_subscriptions(
    slug: &str,
    singleton: bool,
    subscriptions: Vec<brenn_lib::messaging::config::ResolvedSubscription>,
) -> AppConfig {
    let messaging_cfg = brenn_lib::messaging::config::ResolvedMessagingConfig {
        send_budget: 100,
        subscriptions,
    };
    AppConfig {
        working_dir: std::path::PathBuf::from("/tmp"),
        model: String::new(),
        singleton,
        allowed_users: vec!["testuser".to_string()],
        history_replay_limit: 100,
        state_dir: std::path::PathBuf::from("/tmp"),
        messaging: Some(messaging_cfg),
        // Grant MessagingPublish + a universal brenn_publish matcher so the
        // publish gate authorizes.
        policy: brenn_lib::access::AppPolicy::messaging_sender_policy(),
        ..brenn_lib::config::test_app_config(slug)
    }
}

/// Build an `AutomationEngine` from a caller-supplied apps map.
///
/// Use this when tests need an engine with a specific set of apps (e.g.
/// startup tests that vary which apps are present vs absent). Uses noop
/// defaults for all other collaborators.
pub(super) fn make_engine_with_apps(
    db: brenn_db::Db,
    apps: Arc<indexmap::IndexMap<String, AppConfig>>,
) -> Arc<AutomationEngine> {
    let directory = Arc::new(MessagingDirectory::new());
    let messenger = Messenger::new(
        db.clone(),
        directory.clone(),
        Arc::from("brenn://test"),
        apps.clone(),
        Arc::new(FakeWakeRouter),
        MessagingGlobalConfig::default(),
    );
    let (alerts, _) = AlertDispatcher::noop();
    AutomationEngine::new(
        db,
        messenger,
        apps,
        directory,
        FakeIngressRouter::new(),
        AutomationGlobalConfig::default(),
        alerts,
    )
}

/// [`make_engine_with_apps`]'s bus-aware twin: the same engine, plus every app's
/// chat roster channel — the durable row, the directory entry, the reserved
/// writer's registration, and the chat vocabulary the address is composed from.
///
/// Without that wiring `publish_chat_roster` answers `None` at its directory
/// lookup before it reads anything, so a test driving a conversation-creating
/// path green-lights an announcement that never happens. `extra_entries` are
/// upserted and put in the directory beside the rosters, for a test whose
/// subject also needs a destination channel.
pub(super) async fn make_engine_on_the_bus(
    db: brenn_db::Db,
    apps: Arc<indexmap::IndexMap<String, AppConfig>>,
    extra_entries: Vec<brenn_lib::messaging::ChannelEntry>,
    ingress_router: Arc<dyn IngressRouter>,
) -> Arc<AutomationEngine> {
    use brenn_lib::config::LlmChatConfig;
    use brenn_lib::messaging::ChannelScheme;
    use brenn_messaging::chat_roster::{CHAT_ROSTER_COMPONENT, chat_roster_entry};
    use brenn_messaging::system::{SystemParticipantSpec, registrations_from_specs};

    let chat = LlmChatConfig::default();
    let defaults = MessagingGlobalConfig::default();
    let rosters: Vec<brenn_lib::messaging::ChannelEntry> = apps
        .keys()
        .map(|slug| chat_roster_entry(&chat, slug, &defaults))
        .collect();
    let roster_writer = SystemParticipantSpec::publish_only(
        CHAT_ROSTER_COMPONENT,
        ChannelScheme::Brenn,
        &rosters
            .iter()
            .map(|entry| {
                entry
                    .address
                    .strip_prefix(ChannelScheme::Brenn.prefix())
                    .expect("a roster address is a brenn: address")
                    .to_string()
            })
            .collect::<Vec<_>>(),
    );

    let entries: Vec<brenn_lib::messaging::ChannelEntry> =
        rosters.into_iter().chain(extra_entries).collect();
    {
        let conn = db.lock().await;
        brenn_messaging_store::db::upsert_channels(&conn, &entries);
    }

    let directory = Arc::new(MessagingDirectory::with_entries(entries));
    let messenger = Messenger::new(
        db.clone(),
        directory.clone(),
        Arc::from("brenn://test"),
        apps.clone(),
        Arc::new(FakeWakeRouter),
        defaults,
    )
    .with_subscriber_registrations(registrations_from_specs(&[roster_writer]))
    .with_llm_chat(chat);
    let (alerts, _) = AlertDispatcher::noop();
    AutomationEngine::new(
        db,
        messenger,
        apps,
        directory,
        ingress_router,
        AutomationGlobalConfig::default(),
        alerts,
    )
}

/// Every roster snapshot published for `app_slug`, oldest first.
pub(super) async fn roster_snapshots(engine: &AutomationEngine, app_slug: &str) -> Vec<String> {
    let address =
        brenn_envelope::chat::chat_roster_address(&engine.messenger.llm_chat().prefix, app_slug);
    let conn = engine.db.lock().await;
    let mut stmt = conn
        .prepare(
            "SELECT m.body FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address = ?1 ORDER BY m.id",
        )
        .expect("prepare roster scan");
    stmt.query_map([address], |row| row.get::<_, String>(0))
        .expect("query roster snapshots")
        .map(|row| row.expect("decode roster row"))
        .collect()
}

/// The `ResolvedSubscription` list "test-app" needs to agree with `directory`:
/// one per push-enabled subscriber entry naming the app, so the app's config and
/// the directory say the same thing about what it subscribes to.
pub(super) fn app_subscriptions(
    directory: &MessagingDirectory,
) -> Vec<brenn_lib::messaging::config::ResolvedSubscription> {
    directory
        .list()
        .iter()
        .flat_map(|entry| {
            entry
                .subscribers
                .iter()
                .filter(|s| s.kind.slug() == "test-app")
                .map(|s| brenn_lib::messaging::config::ResolvedSubscription {
                    channel_uuid: entry.uuid,
                    channel_address: entry.address.clone(),
                    push_depth: s.push_depth,
                    retain_depth: s.retain_depth,
                    noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                    wake_min: brenn_lib::messaging::WakeMin::Normal,
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Build an `AutomationEngine` with full control over all collaborators.
///
/// Call sites that only need defaults can pass `Arc::new(FakeWakeRouter)`,
/// `AlertDispatcher::noop().0`, and `AutomationGlobalConfig::default()`.
pub(super) fn make_engine_full(
    db: brenn_db::Db,
    directory: MessagingDirectory,
    ingress_router: Arc<dyn IngressRouter>,
    wake_router: Arc<dyn WakeRouter>,
    alerts: AlertDispatcher,
    global_cfg: AutomationGlobalConfig,
    singleton: bool,
) -> Arc<AutomationEngine> {
    let app_cfg =
        default_app_cfg_with_subscriptions("test-app", singleton, app_subscriptions(&directory));
    let mut apps = indexmap::IndexMap::new();
    apps.insert("test-app".to_string(), app_cfg);
    let apps = Arc::new(apps);

    let directory = Arc::new(directory);
    let messenger = Messenger::new(
        db.clone(),
        directory.clone(),
        Arc::from("brenn://test"),
        apps.clone(),
        wake_router,
        MessagingGlobalConfig::default(),
    );

    AutomationEngine::new(
        db,
        messenger,
        apps,
        directory,
        ingress_router,
        global_cfg,
        alerts,
    )
}

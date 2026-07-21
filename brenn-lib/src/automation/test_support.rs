//! Shared test infrastructure for the automation module tree.
//!
//! Exposes `FakeIngressRouter`, `FakeWakeRouter`, `default_app_cfg`,
//! `make_engine_full`, and `make_engine_with_apps` so `fire.rs`,
//! `loop_task.rs`, `startup.rs`, and `mod.rs` tests don't each maintain
//! separate copies. Adding a field to `AppConfig` or changing a trait
//! signature requires only one edit here.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::automation::config::AutomationGlobalConfig;
use crate::automation::{AutomationEngine, IngressRouter};
use crate::config::AppConfig;
use crate::messaging::{
    MessageEnvelope, MessagingDirectory, MessagingGlobalConfig, Messenger, Urgency, WakeRouter,
};
use crate::obs::alerting::AlertDispatcher;

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
        _key: &crate::messaging::SubscriberEntryKind,
        _: &crate::messaging::ParticipantId,
        _: &MessageEnvelope,
        _push_id: i64,
        _seq: i64,
    ) -> Result<bool, String> {
        Ok(false)
    }
    async fn deliver_ingress(
        &self,
        _key: &crate::messaging::SubscriberEntryKind,
        _: &crate::messaging::ParticipantId,
        _event: &crate::messaging::ingress::Event,
    ) -> Result<bool, String> {
        Ok(false)
    }
    fn spawn_eager_wake(
        &self,
        _key: &crate::messaging::SubscriberEntryKind,
        _: &crate::messaging::ParticipantId,
    ) {
    }
    fn delivery_shape(
        &self,
        key: &crate::messaging::SubscriberEntryKind,
    ) -> crate::messaging::DeliveryShape {
        crate::messaging::default_delivery_shape(key)
    }
    fn alarm(&self, _channel: &str, _subscriber: &crate::messaging::ParticipantId) {}
}

/// Build a minimal `AppConfig` for test engines.
///
/// `slug` — app slug (also used as name).
/// `singleton` — sets `singleton` flag; most automation tests want `true`.
pub(super) fn default_app_cfg(slug: &str, singleton: bool) -> AppConfig {
    default_app_cfg_with_subscriptions(slug, singleton, vec![])
}

/// Like `default_app_cfg` but with an explicit subscription list. Use when the
/// test directory has a push-enabled subscriber entry for this app — the
/// `resolve_push_targets` invariant requires a matching `ResolvedSubscription`.
pub(super) fn default_app_cfg_with_subscriptions(
    slug: &str,
    singleton: bool,
    subscriptions: Vec<crate::messaging::config::ResolvedSubscription>,
) -> AppConfig {
    let messaging_cfg = crate::messaging::config::ResolvedMessagingConfig {
        send_budget: 100,
        subscriptions,
    };
    AppConfig {
        slug: slug.to_string(),
        name: slug.to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: std::path::PathBuf::from("/tmp"),
        model: String::new(),
        single_instance: false,
        singleton,
        persistent: false,
        idle_timeout: None,
        compaction: None,
        idle_hook_secs: 0,
        allowed_users: vec!["testuser".to_string()],
        disabled_tools: vec![],
        mcp_servers: Default::default(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: true,
        path_mapper: crate::config::PathMapper::Identity,
        container_spawn: None,
        start_hooks: Default::default(),
        post_pull_hooks: Default::default(),
        startup_hooks: Default::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: Default::default(),
        mounts: vec![],
        history_replay_limit: 100,
        frontmatter: Default::default(),
        state_dir: std::path::PathBuf::from("/tmp"),
        messaging: Some(messaging_cfg),
        messaging_default_send_budget: 100,
        // App is a messaging sender; grant MessagingPublish + a universal
        // brenn_publish matcher so the Phase-2 Seam A publish gate authorizes.
        policy: crate::access::AppPolicy::messaging_sender_policy(),
        pwa_push: None,
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    }
}

/// Build an `AutomationEngine` from a caller-supplied apps map.
///
/// Use this when tests need an engine with a specific set of apps (e.g.
/// startup tests that vary which apps are present vs absent). Uses noop
/// defaults for all other collaborators.
pub(super) fn make_engine_with_apps(
    db: crate::db::Db,
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

/// Build an `AutomationEngine` with full control over all collaborators.
///
/// Call sites that only need defaults can pass `Arc::new(FakeWakeRouter)`,
/// `AlertDispatcher::noop().0`, and `AutomationGlobalConfig::default()`.
pub(super) fn make_engine_full(
    db: crate::db::Db,
    directory: MessagingDirectory,
    ingress_router: Arc<dyn IngressRouter>,
    wake_router: Arc<dyn WakeRouter>,
    alerts: AlertDispatcher,
    global_cfg: AutomationGlobalConfig,
    singleton: bool,
) -> Arc<AutomationEngine> {
    // Build subscriptions for "test-app" from the directory so the
    // resolve_push_targets invariant holds: every push-enabled channel subscriber
    // must have a matching ResolvedSubscription on the app.
    let subscriptions: Vec<crate::messaging::config::ResolvedSubscription> = directory
        .list()
        .iter()
        .flat_map(|entry| {
            entry
                .subscribers
                .iter()
                .filter(|s| s.kind.slug() == "test-app")
                .map(|s| crate::messaging::config::ResolvedSubscription {
                    channel_uuid: entry.uuid,
                    channel_address: entry.address.clone(),
                    push_depth: s.push_depth,
                    retain_depth: s.retain_depth,
                    noise: crate::messaging::config::NoiseLevel::Silent,
                    wake_min: crate::messaging::WakeMin::Normal,
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let app_cfg = default_app_cfg_with_subscriptions("test-app", singleton, subscriptions);
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

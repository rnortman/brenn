//! Test-only constructors and injectors for ActiveBridge: in-memory bridges
//! with mock services, mount fixtures, sync hooks, and the pending-permission
//! preseeder.

#![cfg(test)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::time::{Duration, Instant};

use brenn_lib::approval_rules::ApprovalRuleSet;
use brenn_lib::config::PathMapper;
use brenn_lib::db::Db;
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::ws_types::{ViewportClass, WsServerMessage};
use tokio::sync::oneshot;
use tokio::sync::{broadcast, watch};

use super::ActiveBridge;
use super::compaction::CompactionState;
use super::permission_sync::PendingPermission;
use super::registry::ActiveBridges;

/// Named-field configuration for `ActiveBridge::inject_for_test_full`.
///
/// All fields default to the minimal safe test configuration: no mounts,
/// no optional services, identity path mapper, non-shared, non-singleton.
/// Call sites use struct-update syntax (`TestBridgeConfig { field, ..Default::default() }`)
/// so future fields do not require a sweep of existing tests.
pub(in crate::active_bridge) struct TestBridgeConfig {
    /// Simulates a shared app slot; most tests leave this `false`.
    /// Setting `true` enables locking semantics that can cause test flakiness
    /// if multiple tests run concurrently without proper coordination.
    pub shared: bool,
    /// Caller-supplied shared registry. `None` (default) mints a fresh
    /// `ActiveBridges::new()` exactly as before. `Some(registry)` uses the
    /// caller's registry, enabling two bridges to share one `ActiveBridges`
    /// instance in a single test.
    ///
    /// `make_bridge_no_loop` resolves `None` → fresh and `Some` → caller;
    /// it still returns the resolved registry regardless of which path was taken.
    pub active_bridges: Option<ActiveBridges>,
    pub idle_timeout: Option<Duration>,
    pub singleton: bool,
    pub compaction_config: Option<brenn_lib::config::CompactionConfig>,
    pub mounts: Vec<brenn_lib::config::ResolvedMount>,
    pub repo_sync_sender: Option<crate::repo_sync::SyncTriggerSender>,
    pub idle_hook_secs: u64,
    pub path_mapper: PathMapper,
    pub messenger: Option<Arc<brenn_lib::messaging::Messenger>>,
    pub pwa_push_service: Option<Arc<dyn brenn_lib::pwa_push::PwaPushSender>>,
    /// Optional MQTT service (ingress registry + health). `None` (default) leaves
    /// `bridge.mqtt_service()` empty; `Some` injects it so `MessageChannelList`
    /// mqtt: health enrichment can be exercised.
    pub mqtt_service: Option<Arc<brenn_lib::mqtt::MqttService>>,
    /// Optional concrete MQTT event router. `None` (default) leaves
    /// `bridge.mqtt_event_router()` empty; only the runtime `mqtt:`
    /// subscribe-activation path needs it.
    pub mqtt_event_router: Option<Arc<crate::mqtt_router::MqttEventRouterImpl>>,
    /// App-level user allowlist. Empty = open app (all users visible). Non-empty = restricted.
    pub allowed_users: Vec<String>,
    /// Optional automation engine. `None` = no automation. Threaded into
    /// `inject_for_test_full` so inline `Arc::new(Self { ... })` literals in
    /// automation helpers are no longer needed.
    pub automation_engine: Option<Arc<brenn_lib::automation::AutomationEngine>>,
    /// Per-app integration instances. Empty map = no integrations enabled.
    /// Pfin tests that exercise config-dependent paths should set this.
    pub integrations: HashMap<String, std::sync::Arc<dyn brenn_lib::integration::Integration>>,
    /// First-class tool registry. `None` (default) mints an empty registry;
    /// `Some` injects one so the `registry_adapter` intercept can be exercised.
    pub tools: Option<Arc<crate::tool_registry::ToolRegistry>>,
    /// This app's resolved tool grants. Empty (default) = no registry tools
    /// granted.
    pub tool_grants: std::collections::BTreeMap<String, brenn_lib::tools::ResolvedToolGrant>,
}

impl Default for TestBridgeConfig {
    fn default() -> Self {
        Self {
            shared: false,
            active_bridges: None,
            idle_timeout: None,
            singleton: false,
            compaction_config: None,
            mounts: vec![],
            repo_sync_sender: None,
            idle_hook_secs: 0,
            path_mapper: PathMapper::Identity,
            messenger: None,
            pwa_push_service: None,
            mqtt_service: None,
            mqtt_event_router: None,
            allowed_users: vec![],
            automation_engine: None,
            integrations: HashMap::new(),
            tools: None,
            tool_grants: std::collections::BTreeMap::new(),
        }
    }
}

/// Build a test integrations map containing a pfin integration with a minimal
/// config (`command = "pf"`, empty env). Used by tests that exercise pfin
/// tool paths and need `bridge.pfin_config()` to return `Some`.
pub(in crate::active_bridge) fn pfin_test_integrations()
-> HashMap<String, std::sync::Arc<dyn brenn_lib::integration::Integration>> {
    let config_value: toml::Value =
        toml::from_str("command = \"pf\"").expect("valid pfin test toml");
    let factory = brenn_pfin::PfinFactory;
    let integration =
        brenn_lib::integration::IntegrationFactory::create(&factory, Some(&config_value));
    let mut map: HashMap<String, std::sync::Arc<dyn brenn_lib::integration::Integration>> =
        HashMap::new();
    map.insert("pfin".to_string(), integration);
    map
}

impl ActiveBridge {
    /// Test-only: overwrite `bridge.session` with `CcSession::recording_for_test()`
    /// and return the receiver that captures every `OutgoingEnvelope` sent. Access
    /// the `CcOutgoing` via `.msg`; `.ack` holds the optional flush-ack sender.
    ///
    /// `pub(crate)` so WS-layer tests outside `active_bridge` can install a recording
    /// session on a bridge retrieved from `state.active_bridges` without needing
    /// access to the `pub(in crate::active_bridge)` `install_recording_session` helper.
    pub(crate) async fn install_recording_session_for_test(
        &self,
    ) -> tokio::sync::mpsc::Receiver<brenn_cc::session::OutgoingEnvelope> {
        let (session, rx) = brenn_cc::session::CcSession::recording_for_test();
        let mut guard = self.session.lock().await;
        *guard = Some(session);
        rx
    }

    /// Test-only: insert a pending synchronous permission into the bridge's
    /// in-memory map, bypassing the live `ApprovalRequired` → CC oneshot flow.
    /// Used by WS-layer tests that need to exercise `send_pending_permissions_*`
    /// without spinning up a CC subprocess.
    ///
    /// Divergence from production: the `oneshot::Receiver` is dropped
    /// immediately, so a subsequent `handle_permission_response` on this
    /// entry would `warn!` on the dropped sender (and no CC decision would
    /// be delivered). Tests that exercise the resolve path should drive the
    /// real `SessionEvent::ApprovalRequired` flow via `test_bridge()` instead.
    pub(crate) async fn insert_pending_permission_for_test(
        &self,
        request_id: &str,
        tool_name: &str,
        tool_input: serde_json::Value,
    ) {
        let (tx, _rx) = oneshot::channel();
        let mut permissions = self.pending_permissions.lock().await;
        permissions.insert(
            request_id.to_string(),
            PendingPermission {
                tx,
                original_input: tool_input.clone(),
                tool_use_id: format!("tu_{request_id}"),
                tool_name: tool_name.to_string(),
                display_input: tool_input,
            },
        );
    }

    /// Test-only: inject state for testing event routing without spawning CC.
    pub(crate) fn inject_for_test(
        user_id: i64,
        conversation_id: i64,
        app_slug: &str,
        db: Db,
        broadcast_tx: broadcast::Sender<WsServerMessage>,
    ) -> Arc<Self> {
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            app_slug,
            db,
            broadcast_tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig::default(),
        )
    }

    pub(crate) fn inject_for_test_shared(
        user_id: i64,
        conversation_id: i64,
        app_slug: &str,
        shared: bool,
        db: Db,
        broadcast_tx: broadcast::Sender<WsServerMessage>,
    ) -> Arc<Self> {
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            app_slug,
            db,
            broadcast_tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                shared,
                ..Default::default()
            },
        )
    }

    pub(crate) fn inject_for_test_with_mounts(
        user_id: i64,
        conversation_id: i64,
        app_slug: &str,
        db: Db,
        broadcast_tx: broadcast::Sender<WsServerMessage>,
        mounts: Vec<brenn_lib::config::ResolvedMount>,
    ) -> Arc<Self> {
        Self::inject_for_test_with_mounts_and_mapper(
            user_id,
            conversation_id,
            app_slug,
            db,
            broadcast_tx,
            mounts,
            PathMapper::Identity,
        )
    }

    pub(crate) fn inject_for_test_with_mounts_and_mapper(
        user_id: i64,
        conversation_id: i64,
        app_slug: &str,
        db: Db,
        broadcast_tx: broadcast::Sender<WsServerMessage>,
        mounts: Vec<brenn_lib::config::ResolvedMount>,
        path_mapper: PathMapper,
    ) -> Arc<Self> {
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            app_slug,
            db,
            broadcast_tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                mounts,
                path_mapper,
                ..Default::default()
            },
        )
    }

    /// Test helper: bridge with mounts and a repo-sync sender so the
    /// GitRepoPull / GitRepoCommitAndPush PostToolUse tests can observe
    /// the `SyncTrigger::Push` emissions they're supposed to make.
    pub(crate) fn inject_for_test_with_mounts_and_sync(
        user_id: i64,
        conversation_id: i64,
        app_slug: &str,
        db: Db,
        broadcast_tx: broadcast::Sender<WsServerMessage>,
        mounts: Vec<brenn_lib::config::ResolvedMount>,
        repo_sync_sender: crate::repo_sync::SyncTriggerSender,
    ) -> Arc<Self> {
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            app_slug,
            db,
            broadcast_tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                mounts,
                repo_sync_sender: Some(repo_sync_sender),
                ..Default::default()
            },
        )
    }

    /// Test helper: bridge with a configured `idle_hook_secs`. Used by
    /// idle-hooks tests to drive arming / cancellation under controlled
    /// timing. `mounts` lets tests register `DirtyRepoHook` against
    /// real mounts; pass `vec![]` and rely on a fake hook otherwise.
    pub(crate) fn inject_for_test_with_idle_hook_secs(
        user_id: i64,
        conversation_id: i64,
        app_slug: &str,
        db: Db,
        broadcast_tx: broadcast::Sender<WsServerMessage>,
        idle_hook_secs: u64,
        mounts: Vec<brenn_lib::config::ResolvedMount>,
    ) -> Arc<Self> {
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            app_slug,
            db,
            broadcast_tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                idle_hook_secs,
                mounts,
                ..Default::default()
            },
        )
    }

    pub(in crate::active_bridge) fn inject_for_test_full(
        user_id: i64,
        conversation_id: i64,
        app_slug: &str,
        db: Db,
        broadcast_tx: broadcast::Sender<WsServerMessage>,
        alert_dispatcher: AlertDispatcher,
        cfg: TestBridgeConfig,
    ) -> Arc<Self> {
        let TestBridgeConfig {
            shared,
            active_bridges: active_bridges_opt,
            idle_timeout,
            singleton,
            compaction_config,
            mounts,
            repo_sync_sender,
            idle_hook_secs,
            path_mapper,
            messenger,
            pwa_push_service,
            mqtt_service,
            mqtt_event_router,
            allowed_users,
            automation_engine,
            integrations,
            tools,
            tool_grants,
        } = cfg;
        // Resolve: None → fresh per-bridge registry; Some → caller-supplied shared registry.
        let active_bridges = active_bridges_opt.unwrap_or_else(ActiveBridges::new);
        // Default: an empty first-class tool registry (no tools registered).
        let tools =
            tools.unwrap_or_else(|| Arc::new(crate::tool_registry::ToolRegistry::new(vec![])));
        let (epoch_tx, _epoch_rx) = watch::channel(0u64);
        Arc::new(Self {
            session: tokio::sync::Mutex::new(None),
            event_tx: broadcast_tx,
            alert_dispatcher,
            pending_permissions: tokio::sync::Mutex::new(HashMap::new()),
            conversation_id,
            user_id,
            app_slug: app_slug.to_string(),
            working_dir: PathBuf::from("."),
            path_mapper,
            shared: AtomicBool::new(shared),
            db,
            subscribers: tokio::sync::RwLock::new(HashMap::new()),
            drain_on_idle: AtomicBool::new(false),
            cc_idle: AtomicBool::new(true),
            idle_timeout,
            idle_shutdown: std::sync::Mutex::new(None),
            spawn_instant: Instant::now(),
            active_bridges,
            tool_registry: Arc::new(HashMap::new()),
            tools,
            tool_grants,
            server_origin: Arc::from("test-origin"),
            // Mirror the production auto-approve base exactly so fixture-based
            // tests see the same approval policy as real bridges. ExportUsage is
            // already absent from GLOBAL_EXTRA_STATIC_BASE.
            approval_rules: ApprovalRuleSet::new(
                super::bridge::GLOBAL_EXTRA_STATIC_BASE,
                &[],
                vec![],
            ),
            approval_outcomes: tokio::sync::Mutex::new(HashMap::new()),
            pending_tool_uses: tokio::sync::Mutex::new(HashMap::new()),
            handled_tool_uses: tokio::sync::Mutex::new(HashSet::new()),
            last_set_model: tokio::sync::Mutex::new(None),
            integrations,
            container_spawn: None,
            mounts,
            idle_hook_secs,
            idle_hooks: std::sync::Mutex::new(Vec::new()),
            idle_hook_timer: std::sync::Mutex::new(None),
            frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
            allowed_users,
            viewport_class: std::sync::Mutex::new(ViewportClass::Wide),
            singleton,
            compaction: tokio::sync::Mutex::new(CompactionState::default()),
            compaction_config,
            context_usage: std::sync::Mutex::new(None),
            seed_max_tokens: std::sync::Mutex::new(None),
            last_total_cost_usd: std::sync::Mutex::new(None),
            active_model_slug: std::sync::Mutex::new(None),
            cc_version: std::sync::Mutex::new(None),
            server_shutting_down: Arc::new(AtomicBool::new(false)),
            repo_sync_sender,
            messenger,
            pwa_push_service,
            mqtt_service,
            mqtt_event_router,
            automation_engine,
            usage_session_gap_secs: 1800,
            messaging_default_send_budget: 100,
            last_cost_prune_at: AtomicI64::new(0),
            last_lint_snapshot: std::sync::Mutex::new(None),
            event_loop_handle: std::sync::Mutex::new(None),
            died_handled: AtomicBool::new(false),
            event_loop_epoch: epoch_tx,
        })
    }

    /// Test bridge with both a Messenger (one brenn channel) and a PwaPushService
    /// (one subscription pre-seeded). Used to test the `MessageChannelList` merge path.
    ///
    /// App slug is "testapp". Messenger has one channel `brenn:test-channel`.
    /// PwaPushService has one subscription for user "alice" on device "laptop".
    pub(crate) async fn test_new_with_combined_services() -> Arc<Self> {
        use std::sync::Arc;

        let (tx, _rx) = broadcast::channel(16);
        let db = brenn_lib::db::init_db_memory();
        let now = brenn_lib::db::format_ts_for_db(chrono::Utc::now());

        // Seed user + conversation (for emit_tool_summary FK), plus alice + device + subscription.
        let (user_id, conversation_id) = {
            let conn = db.lock().await;
            let (uid, cid) = seed_test_user_and_conversation(&conn, &now, "testapp");
            // alice: a second user for pwa_push targets.
            seed_alice_with_subscription(&conn, &now);
            (uid, cid)
        };

        // Messenger with one channel: brenn:test-channel subscribed by "testapp".
        // Capture the UUID so we can upsert it into messaging_channels (required
        // by the FK on messaging_messages.channel_uuid) and reference it from the
        // testapp ResolvedSubscription.
        let test_channel_uuid = uuid::Uuid::new_v4();
        let test_channel_entry = brenn_lib::messaging::ChannelEntry {
            uuid: test_channel_uuid,
            address: brenn_lib::messaging::canonical_address("test-channel"),
            description: None,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                standing_retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: vec![brenn_lib::messaging::SubscriberEntry {
                kind: brenn_lib::messaging::SubscriberEntryKind::App("testapp".to_string()),
                push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                wake_min: Some(brenn_lib::messaging::WakeMin::Normal),
            }],
            transport_type: brenn_lib::messaging::ChannelScheme::Brenn,
            mount: None,
        };
        // A second registered channel that resolves in the directory but is
        // deliberately NOT covered by `testapp`'s `brenn_publish` ACL. Lets the
        // Seam-A `AclDenied` intercept test (design §2.2) exercise the
        // channel-resolves-but-not-authorized path without affecting the other
        // tests (which all target `test-channel`). No subscribers needed; the
        // publish ACL check fires before delivery resolution.
        let locked_channel_uuid = uuid::Uuid::new_v4();
        let locked_channel_entry = brenn_lib::messaging::ChannelEntry {
            uuid: locked_channel_uuid,
            address: brenn_lib::messaging::canonical_address("locked-channel"),
            description: None,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                standing_retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: brenn_lib::messaging::ChannelScheme::Brenn,
            mount: None,
        };
        {
            let conn = db.lock().await;
            brenn_lib::messaging::db::upsert_channels(
                &conn,
                std::slice::from_ref(&test_channel_entry),
            );
            brenn_lib::messaging::db::upsert_channels(
                &conn,
                std::slice::from_ref(&locked_channel_entry),
            );
        }
        let dir = brenn_lib::messaging::MessagingDirectory::with_entries(vec![
            test_channel_entry,
            locked_channel_entry,
        ]);
        // Seed "testapp" with messaging enabled so publish/cancel/edit intercept
        // tests can exercise the full happy-path through the real messenger.
        // The subscription must be present so resolve_push_targets can look up
        // the noise level (invariant: every channel subscriber has a ResolvedSubscription).
        let mut messenger_apps = indexmap::IndexMap::new();
        let mut testapp_cfg =
            crate::test_support::app_config::default_test_app_config("testapp", "testapp");
        testapp_cfg.working_dir = std::path::PathBuf::from("/tmp");
        testapp_cfg.singleton = true;
        testapp_cfg.allowed_users = vec!["test".to_string()];
        testapp_cfg.history_replay_limit = 100;
        testapp_cfg.messaging = Some(brenn_lib::messaging::ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![brenn_lib::messaging::config::ResolvedSubscription {
                channel_uuid: test_channel_uuid,
                channel_address: brenn_lib::messaging::canonical_address("test-channel"),
                push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            }],
        });
        // Grant the messaging capabilities so the intercept happy-path tests
        // publish (the Seam-A publish gate, design §2.2, requires both the
        // MessagingPublish grant and a covering brenn_publish matcher).
        testapp_cfg
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::MessagingPublish);
        testapp_cfg
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::MessagingSubscribe);
        // Layer-2 publish ACL: authorize publishing to `test-channel` so the
        // happy-path send/cancel/edit intercept tests pass the Seam-A
        // brenn_publish gate (mirrors the brenn_subscribe matcher below;
        // scoped to the exact channel the tests use).
        testapp_cfg
            .policy
            .acls
            .brenn_publish
            .push(brenn_lib::access::acl::ChannelMatcher::Exact(
                "test-channel".to_string(),
            ));
        // Phase-1 non-MQTT gate: authorize a runtime brenn: subscribe to
        // `test-channel` so the static-sub-conflict test reaches (and asserts)
        // the lib core's conflict error rather than being PolicyDenied first.
        // Scoped to the exact channel the test uses (minimal grant).
        testapp_cfg
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::DynamicSubscribe);
        testapp_cfg.policy.acls.brenn_subscribe.push(
            brenn_lib::access::acl::ChannelMatcher::Exact("test-channel".to_string()),
        );
        messenger_apps.insert("testapp".to_string(), testapp_cfg);
        let messenger = brenn_lib::messaging::Messenger::new(
            db.clone(),
            Arc::new(dir),
            Arc::from("test-source"),
            Arc::new(messenger_apps),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            brenn_lib::messaging::MessagingGlobalConfig::default(),
        );

        // PwaPushService: temp keypair file, "testapp" has pwa_push enabled.
        let tmp = tempfile::tempdir().expect("tempdir for VAPID key");
        let vapid = brenn_lib::pwa_push::vapid::load_or_generate(&tmp.path().join("vapid.json"));
        let pwa_config = brenn_lib::pwa_push::config::ResolvedPwaPushConfig {
            vapid,
            subject: "mailto:test@example.com".to_string(),
            endpoint_policy: brenn_lib::pwa_push::endpoint_validator::EndpointPolicy::new(
                vec![],
                false,
            ),
        };
        let mut apps = indexmap::IndexMap::new();
        apps.insert(
            "testapp".to_string(),
            make_test_push_app_config(tmp.path().to_path_buf(), vec![], true),
        );
        let (test_alert_dispatcher, _alert_handle) =
            brenn_lib::obs::alerting::noop_alert_dispatcher();
        let pwa_push_service = Arc::new(brenn_lib::pwa_push::PwaPushService::new(
            db.clone(),
            pwa_config,
            Arc::new(apps),
            brenn_lib::messaging::MessagingGlobalConfig::default(),
            std::sync::Arc::from("https://brenn.test"),
            test_alert_dispatcher,
        ));

        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                messenger: Some(messenger),
                pwa_push_service: Some(pwa_push_service),
                ..Default::default()
            },
        )
    }

    /// Test bridge whose Messenger directory holds one `mqtt:` ingress channel
    /// (`mqtt:home:sensors/+/temp`) and whose `MqttService` has a registered
    /// ingress supervisor handle for client `home` with that filter subscribed at
    /// QoS 2. Used to exercise the `MessageChannelList` mqtt: health enrichment
    /// (design §2.5): the listing's `MqttDetails` should be filled with
    /// `qos`/`health` from the service.
    ///
    /// App slug is "testapp", registered in the messenger with an `mqtt_subscribe`
    /// ACL matcher for `(home, sensors/+/temp)`. Since `MessageChannelList` is now
    /// app-scoped (design §2.2) and sources `mqtt:` rows from ACL matchers (not the
    /// directory), the matcher is what makes the `mqtt:home:sensors/+/temp` Pattern
    /// row appear so the enrichment path can fill its health fields.
    pub(crate) async fn test_new_with_mqtt_ingress_listing() -> Arc<Self> {
        use std::sync::Arc;

        let (tx, _rx) = broadcast::channel(16);
        let db = brenn_lib::db::init_db_memory();
        let now = brenn_lib::db::format_ts_for_db(chrono::Utc::now());
        let (user_id, conversation_id) = {
            let conn = db.lock().await;
            seed_test_user_and_conversation(&conn, &now, "testapp")
        };

        // Directory with one mqtt: ingress channel.
        let mqtt_address =
            brenn_lib::mqtt::config::parsed_address_canonical("home", "sensors/+/temp");
        let mqtt_entry = brenn_lib::messaging::ChannelEntry {
            uuid: brenn_lib::messaging::mqtt_channel_uuid_from_address(&mqtt_address),
            address: mqtt_address.clone(),
            description: None,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                standing_retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: brenn_lib::messaging::ChannelScheme::Mqtt,
            mount: None,
        };
        let dir = brenn_lib::messaging::MessagingDirectory::with_entries(vec![mqtt_entry]);
        // Register "testapp" with an mqtt_subscribe ACL matcher covering
        // (home, sensors/+/temp): list_accessible_channels (design §2.2) sources
        // mqtt: Pattern rows from these matchers, so this is what makes the
        // mqtt:home:sensors/+/temp row appear (and then get health-enriched).
        let mut messenger_apps = indexmap::IndexMap::new();
        let mut testapp_cfg =
            crate::test_support::app_config::default_test_app_config("testapp", "testapp");
        testapp_cfg
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::MqttSubscribe);
        testapp_cfg
            .policy
            .acls
            .mqtt_subscribe
            .push(brenn_lib::access::acl::MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            });
        messenger_apps.insert("testapp".to_string(), testapp_cfg);
        let messenger = brenn_lib::messaging::Messenger::new(
            db.clone(),
            Arc::new(dir),
            Arc::from("test-source"),
            Arc::new(messenger_apps),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            brenn_lib::messaging::MessagingGlobalConfig::default(),
        );

        // MqttService with a registered ingress handle for `home`; the filter is
        // subscribed at QoS 2 so the enrichment surfaces it. The handle's `client`
        // cell is `None` (no live broker in a unit test) → health "disconnected".
        let mqtt_service = brenn_lib::mqtt::MqttService::new();
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        let config = Arc::new(crate::test_support::mqtt::test_client_config("home"));
        let handle = brenn_lib::mqtt::MqttClientHandle::new(config, vec![], stop_tx);
        handle
            .add_subscription("sensors/+/temp".to_string(), 2)
            .await;
        mqtt_service.add_client(handle).await;

        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                messenger: Some(messenger),
                mqtt_service: Some(mqtt_service),
                ..Default::default()
            },
        )
    }

    /// Test bridge for the runtime `mqtt:` subscribe-activation wrapper
    /// (`crate::mqtt_subscribe`, design §2.3).
    ///
    /// Wires the three collaborators the wrapper composes, sharing one DB and one
    /// `Messenger`:
    /// - a `Messenger` whose directory starts with **one** existing `brenn:`
    ///   channel (`brenn:test-channel`, no subscribers) and an `apps` map with the
    ///   bridge's app (`testapp`) as a **singleton single-user** app (so
    ///   push-enabled subscribes pass the resolver invariants);
    /// - an `MqttService` with a registered ingress supervisor for client `home`,
    ///   default `qos = 2` and `urgency = High` (deliberately non-default so
    ///   qos-/urgency-resolution is observable), whose `client` cell is `None`
    ///   (no live broker in a unit test → SUBSCRIBE deferred);
    /// - a `MqttEventRouterImpl` wired via `set_state` with an `AppState` holding
    ///   the **same** `Messenger`, so a post-subscribe `deliver_inbound` end-to-end
    ///   proves the runtime-added `IngressRoute` is consulted (the route-presence
    ///   assertion, mirroring the `mqtt_router` add_route test).
    ///
    /// Returns the bridge; its `Messenger`/`MqttService`/router and DB are reached
    /// through the bridge accessors and `bridge.messenger().db()`.
    pub(crate) async fn test_new_for_mqtt_subscribe() -> Arc<Self> {
        Self::test_new_for_mqtt_subscribe_with_singleton(
            true,
            mqtt_subscribe_test_policy(),
            brenn_lib::messaging::config::Depth::Unbounded,
        )
        .await
    }

    /// Same wiring as [`test_new_for_mqtt_subscribe`] but with the `brenn:`
    /// `test-channel` given a **bounded** `standing_retain_depth` (2), so a dynamic
    /// subscribe requesting a deeper `retain_depth` trips the dynamic-path cap
    /// (`RetainDepthExceedsStanding`). Used to drive the intercept-site
    /// over-standing warn.
    pub(crate) async fn test_new_for_mqtt_subscribe_bounded_standing() -> Arc<Self> {
        Self::test_new_for_mqtt_subscribe_with_singleton(
            true,
            mqtt_subscribe_test_policy(),
            brenn_lib::messaging::config::Depth::Bounded(2),
        )
        .await
    }

    /// Same wiring as [`test_new_for_mqtt_subscribe`] but with `testapp` declared
    /// **non-singleton** (and two `allowed_users`, so it also fails the
    /// single-user side of the push-enabled invariant). Used to drive the
    /// intercept→core handoff for a `push_depth > 0` subscribe on a non-singleton
    /// app, which the shared resolver rejects as a tool error (design §5).
    pub(crate) async fn test_new_for_mqtt_subscribe_non_singleton() -> Arc<Self> {
        Self::test_new_for_mqtt_subscribe_with_singleton(
            false,
            mqtt_subscribe_test_policy(),
            brenn_lib::messaging::config::Depth::Unbounded,
        )
        .await
    }

    /// Same wiring as [`test_new_for_mqtt_subscribe`] but with a caller-supplied
    /// dynamic-subscribe `AppPolicy` stamped on `testapp`/`otherapp`, so a test
    /// can drive deny paths (no `DynamicSubscribe` grant, a matcher narrower than
    /// the requested filter, …).
    pub(crate) async fn test_new_for_mqtt_subscribe_with_policy(
        policy: brenn_lib::access::AppPolicy,
    ) -> Arc<Self> {
        Self::test_new_for_mqtt_subscribe_with_singleton(
            true,
            policy,
            brenn_lib::messaging::config::Depth::Unbounded,
        )
        .await
    }

    /// Shared body for the `mqtt:` subscribe-activation fixtures. When
    /// `singleton` is `true` the app is a singleton single-user app (push-enabled
    /// subscribes pass the resolver invariants); when `false` it is a non-singleton
    /// multi-user app (push-enabled subscribes are rejected by the resolver).
    /// `policy` is stamped on both `testapp` and `otherapp` (the Phase-1
    /// dynamic-subscribe gate, §6.5).
    async fn test_new_for_mqtt_subscribe_with_singleton(
        singleton: bool,
        policy: brenn_lib::access::AppPolicy,
        standing_retain_depth: brenn_lib::messaging::config::Depth,
    ) -> Arc<Self> {
        use std::sync::Arc;

        let (tx, _rx) = broadcast::channel(16);
        let db = brenn_lib::db::init_db_memory();
        let now = brenn_lib::db::format_ts_for_db(chrono::Utc::now());
        let (user_id, conversation_id) = {
            let conn = db.lock().await;
            seed_test_user_and_conversation(&conn, &now, "testapp")
        };

        // Directory: one existing brenn: channel, no subscribers.
        let brenn_entry = brenn_lib::messaging::ChannelEntry {
            uuid: uuid::Uuid::new_v4(),
            address: brenn_lib::messaging::canonical_address("test-channel"),
            description: None,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                standing_retain_depth,
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: brenn_lib::messaging::ChannelScheme::Brenn,
            mount: None,
        };
        {
            let conn = db.lock().await;
            brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&brenn_entry));
        }
        let dir = Arc::new(brenn_lib::messaging::MessagingDirectory::with_entries(
            vec![brenn_entry],
        ));

        // Apps: testapp. When `singleton`, a singleton single-user app so
        // push-enabled subscribes pass the resolver's singleton/single-user
        // invariants; otherwise a non-singleton multi-user app so push-enabled
        // subscribes are rejected by the resolver.
        let mut apps = indexmap::IndexMap::new();
        let mut testapp_cfg =
            crate::test_support::app_config::default_test_app_config("testapp", "testapp");
        testapp_cfg.singleton = singleton;
        testapp_cfg.allowed_users = if singleton {
            vec!["test".to_string()]
        } else {
            vec!["test".to_string(), "other".to_string()]
        };
        // The dynamic-subscribe gate requires each app in the fixture to carry a
        // policy authorizing its `mqtt:` subscribes. `testapp` gets the supplied
        // policy (the default `mqtt_subscribe_test_policy` admits every filter the
        // activation/intercept tests request; gate tests pass a narrower/empty
        // policy to exercise the deny paths).
        testapp_cfg.policy = policy.clone();
        let allowed_users = testapp_cfg.allowed_users.clone();
        apps.insert("testapp".to_string(), testapp_cfg);
        // A SECOND app on the same filter (the no-duplicate-route test). It too
        // issues a runtime `mqtt:` subscribe, so it carries the same policy or the
        // gate would deny it (and a missing policy for a live app would panic at
        // the enforcement site).
        let mut otherapp_cfg =
            crate::test_support::app_config::default_test_app_config("otherapp", "otherapp");
        otherapp_cfg.singleton = singleton;
        otherapp_cfg.allowed_users = allowed_users;
        otherapp_cfg.policy = policy;
        apps.insert("otherapp".to_string(), otherapp_cfg);

        let messenger = brenn_lib::messaging::Messenger::new(
            db.clone(),
            dir,
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            brenn_lib::messaging::MessagingGlobalConfig::default(),
        );

        // MqttService with a registered `home` ingress supervisor. Non-default
        // qos (2) / urgency (High) so qos-/urgency-resolution is observable; the
        // `client` cell is None (no broker) → live SUBSCRIBE deferred.
        let mqtt_service = brenn_lib::mqtt::MqttService::new();
        let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
        let mut config = crate::test_support::mqtt::test_client_config("home");
        config.urgency = brenn_lib::messaging::Urgency::High;
        config.qos = 2;
        let handle = brenn_lib::mqtt::MqttClientHandle::new(Arc::new(config), vec![], stop_tx);
        mqtt_service.add_client(handle).await;

        // Concrete router wired with an AppState holding the same Messenger, so a
        // post-subscribe deliver_inbound routes through the runtime-added route.
        let router = Arc::new(crate::mqtt_router::MqttEventRouterImpl::new());
        let mut router_state = crate::state::AppState::for_test(db.clone(), None);
        router_state.messenger = Some(messenger.clone());
        router.set_state(router_state, vec![]);

        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                messenger: Some(messenger),
                mqtt_service: Some(mqtt_service),
                mqtt_event_router: Some(router),
                ..Default::default()
            },
        )
    }

    /// Test bridge with a Messenger and a fully-wired AutomationEngine.
    ///
    /// App slug is "testapp". App has `allowed_users: ["test"]`.
    /// Messenger has one channel `brenn:test-channel` subscribed by "testapp".
    /// The automation engine is connected to the bridge so intercept tests
    /// can exercise the full create/edit/delete/list paths.
    pub(crate) async fn test_new_with_automation() -> Arc<Self> {
        Self::test_new_with_automation_config(
            brenn_lib::automation::AutomationGlobalConfig::default(),
        )
        .await
    }

    /// Like `test_new_with_automation` but with a caller-supplied
    /// `AutomationGlobalConfig` — use when a test needs a non-default cap
    /// (e.g., `max_jobs_per_app: 1`).
    ///
    /// **Note:** This bridge's `approval_rules` uses `GLOBAL_EXTRA_STATIC_BASE`
    /// (via `inject_for_test_full`), which auto-approves the four automation MCP
    /// tools (`MCP_AUTO_CREATE/LIST/EDIT/DELETE`) plus messaging/git/device tools.
    /// Automation-intercept tests assert on `AutomationHandled` verdicts, not on
    /// bridge-level approval policy, so this has no effect on those tests — but
    /// future tests that need a tool call to flow through the approval path (rather
    /// than being auto-approved) should not use this fixture for that tool.
    pub(crate) async fn test_new_with_automation_config(
        automation_cfg: brenn_lib::automation::AutomationGlobalConfig,
    ) -> Arc<Self> {
        Self::test_new_with_automation_config_and_dispatcher(
            automation_cfg,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await
    }

    /// Like `test_new_with_automation_config` but wires a caller-supplied
    /// `AlertDispatcher` as the bridge's dispatcher — pass a capturing one (see
    /// `make_capturing_alerter`) to assert on security-event alerts the automation
    /// intercept emits (e.g. create/edit address-denial signals).
    pub(crate) async fn test_new_with_automation_config_and_dispatcher(
        automation_cfg: brenn_lib::automation::AutomationGlobalConfig,
        bridge_alert_dispatcher: AlertDispatcher,
    ) -> Arc<Self> {
        use std::sync::Arc;

        let (tx, _rx) = broadcast::channel(16);
        let db = brenn_lib::db::init_db_memory();
        let now = brenn_lib::db::format_ts_for_db(chrono::Utc::now());

        let (user_id, conversation_id) = {
            let conn = db.lock().await;
            seed_test_user_and_conversation(&conn, &now, "testapp")
        };

        let dir = Arc::new(brenn_lib::messaging::MessagingDirectory::with_entries(
            vec![brenn_lib::messaging::ChannelEntry {
                uuid: uuid::Uuid::new_v4(),
                address: brenn_lib::messaging::canonical_address("test-channel"),
                description: None,
                resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                    push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                    retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                    standing_retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                    noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                    sink: brenn_lib::messaging::config::Sink::Drop,
                    wake_min: brenn_lib::messaging::WakeMin::Normal,
                },
                subscribers: vec![brenn_lib::messaging::SubscriberEntry {
                    kind: brenn_lib::messaging::SubscriberEntryKind::App("testapp".to_string()),
                    push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                    retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                    noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                    wake_min: Some(brenn_lib::messaging::WakeMin::Normal),
                }],
                transport_type: brenn_lib::messaging::ChannelScheme::Brenn,
                mount: None,
            }],
        ));

        let mut apps = indexmap::IndexMap::new();
        let state_dir = std::path::PathBuf::from(".");
        apps.insert(
            "testapp".to_string(),
            brenn_lib::config::AppConfig {
                slug: "testapp".to_string(),
                name: "testapp".to_string(),
                description: String::new(),
                icon: String::new(),
                working_dir: state_dir.clone(),
                model: "claude-sonnet".to_string(),
                single_instance: false,
                singleton: false,
                persistent: false,
                idle_timeout: None,
                compaction: None,
                idle_hook_secs: 0,
                allowed_users: vec!["test".to_string()],
                disabled_tools: vec![],
                mcp_servers: std::collections::HashMap::new(),
                multiuser: false,
                prefix_username: false,
                prefix_timestamp: false,
                prefix_device: false,
                path_mapper: PathMapper::Identity,
                container_spawn: None,
                start_hooks: Default::default(),
                post_pull_hooks: Default::default(),
                startup_hooks: Default::default(),
                cc_extra_args: vec![],
                approval_rules: vec![],
                attachment_targets: vec![],
                integrations: std::collections::HashMap::new(),
                mounts: vec![],
                history_replay_limit: 2000,
                frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
                state_dir,
                messaging: Some(brenn_lib::messaging::config::ResolvedMessagingConfig {
                    send_budget: 100,
                    subscriptions: vec![brenn_lib::messaging::config::ResolvedSubscription {
                        channel_uuid: uuid::Uuid::nil(),
                        channel_address: "brenn:test-channel".to_string(),
                        push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                        retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                        noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                        wake_min: brenn_lib::messaging::WakeMin::Normal,
                    }],
                }),
                messaging_default_send_budget: 100,
                // App is a messaging sender; grant MessagingPublish so
                // messaging_enabled() passes, plus a `brenn_publish` matcher for
                // `test-channel` so create/edit jobs targeting it pass the
                // create-time publish-ACL scope gate (scoped to the exact channel
                // the automation tests use).
                policy: {
                    let mut p = brenn_lib::access::AppPolicy::default();
                    p.grants
                        .insert(brenn_lib::access::AppCapability::MessagingPublish);
                    p.acls
                        .brenn_publish
                        .push(brenn_lib::access::acl::ChannelMatcher::Exact(
                            "test-channel".to_string(),
                        ));
                    p
                },
                pwa_push: None,
                webhook_subscriptions: vec![],
                mqtt_subscriptions: vec![],
            },
        );
        let apps_arc = Arc::new(apps);

        let messenger = brenn_lib::messaging::Messenger::new(
            db.clone(),
            dir.clone(),
            Arc::from("test-source"),
            apps_arc.clone(),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            brenn_lib::messaging::MessagingGlobalConfig::default(),
        );

        let (alert_dispatcher, _handle) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        let engine = brenn_lib::automation::AutomationEngine::new(
            db.clone(),
            messenger.clone(),
            apps_arc,
            dir,
            Arc::new(crate::test_support::NoopEventRouter)
                as Arc<dyn brenn_lib::automation::IngressRouter>,
            automation_cfg,
            alert_dispatcher,
        );

        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            bridge_alert_dispatcher,
            TestBridgeConfig {
                messenger: Some(messenger),
                automation_engine: Some(engine),
                allowed_users: vec!["test".into()],
                ..Default::default()
            },
        )
    }

    /// Minimal test bridge wrapping a caller-supplied `Messenger`.
    ///
    /// DB pre-seeded with user + conversation for "testapp".
    /// The `db` embedded in `messenger` is reused as the bridge's DB.
    pub(crate) async fn test_new_with_messenger(
        messenger: Arc<brenn_lib::messaging::Messenger>,
    ) -> Arc<Self> {
        Self::test_new_with_messenger_and_dispatcher(
            messenger,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await
    }

    /// Like `test_new_with_messenger` but wires a caller-supplied
    /// `AlertDispatcher` — pass a capturing one (see `make_capturing_alerter`) to
    /// assert on security-event alerts emitted from the intercept.
    pub(crate) async fn test_new_with_messenger_and_dispatcher(
        messenger: Arc<brenn_lib::messaging::Messenger>,
        alert_dispatcher: AlertDispatcher,
    ) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(16);
        let db = messenger.db().clone();
        let (user_id, conversation_id) = {
            let conn = db.lock().await;
            conn.execute(
                "INSERT INTO users (username, password_hash, created_at) \
                 VALUES ('test', 'x', datetime('now'))",
                [],
            )
            .expect("insert user");
            let uid: i64 = conn.last_insert_rowid();
            let cid = brenn_lib::conversation::create_conversation(&conn, uid, "testapp", false);
            (uid, cid)
        };
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            alert_dispatcher,
            TestBridgeConfig {
                messenger: Some(messenger),
                ..Default::default()
            },
        )
    }

    /// Minimal test bridge with no pwa_push service. DB pre-seeded with user + conversation.
    pub(crate) async fn test_new_for_pwa_push() -> Arc<Self> {
        Self::test_new_for_pwa_push_with_service_opt(None).await
    }

    /// Like `test_new_for_pwa_push` but injects a caller-supplied `PwaPushSender`.
    ///
    /// Used by injection tests that need to capture the `data` map passed to
    /// `svc.send()` without making real HTTP calls. Returns a bridge with
    /// `app_slug = "testapp"` and the given service wired in.
    pub(crate) async fn test_new_for_pwa_push_with_service(
        svc: Arc<dyn brenn_lib::pwa_push::PwaPushSender>,
    ) -> Arc<Self> {
        Self::test_new_for_pwa_push_with_service_opt(Some(svc)).await
    }

    async fn test_new_for_pwa_push_with_service_opt(
        svc: Option<Arc<dyn brenn_lib::pwa_push::PwaPushSender>>,
    ) -> Arc<Self> {
        let (db, tx, user_id, conversation_id) = make_minimal_test_db_and_channel().await;
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                pwa_push_service: svc,
                ..Default::default()
            },
        )
    }

    /// Like `test_new_for_pwa_push` but with a real `Messenger` whose `"testapp"`
    /// policy grants `MqttPublish` and holds an `mqtt_publish` ACL matcher for
    /// client `ha`.
    ///
    /// Use when a test needs to exercise MQTT send paths past the grant + ACL
    /// gates (e.g., testing wildcard rejection, qos validation, or the
    /// server-global "MQTT not configured on this server" gate) — the Seam-C
    /// per-client publish ACL check passes for `mqtt:ha:...` addresses so the test
    /// reaches the later validation it exercises.
    pub(crate) async fn test_new_with_mqtt_publish_acl() -> Arc<Self> {
        let (db, tx, user_id, conversation_id) = make_minimal_test_db_and_channel().await;
        let messenger = make_test_messenger_with_mqtt_publish(db.clone(), &["ha"]);
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                messenger: Some(messenger),
                ..Default::default()
            },
        )
    }

    /// Test bridge whose `"testapp"` policy does **not** grant `MqttPublish`, with
    /// an MQTT service present so the server-global readiness check passes and the
    /// shared `enforce_and_publish` (design §2.3) reaches the grant/ACL gate and
    /// returns `AclDenied`. The intercept then selects the "MQTT publish is not
    /// enabled" remedy string (grant absent). Used by the grant-absent intercept
    /// test.
    pub(crate) async fn test_new_for_mqtt_no_grant() -> Arc<Self> {
        let (db, tx, user_id, conversation_id) = make_minimal_test_db_and_channel().await;
        let messenger = make_test_messenger_no_mqtt_grant(db.clone());
        let mqtt_service = brenn_lib::mqtt::MqttService::new();
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                messenger: Some(messenger),
                mqtt_service: Some(mqtt_service),
                ..Default::default()
            },
        )
    }

    /// Test bridge for the Seam-C MQTT publish per-client ACL (design §2.4).
    ///
    /// - A real `Messenger` whose `"testapp"` policy grants `MqttPublish` and
    ///   holds an `mqtt_publish` ACL matcher for each client in `allowed_clients`.
    /// - An `MqttService` with a registered (disconnected) session for each client
    ///   in `session_clients`, so an allowed publish resolves the session handle and
    ///   reaches `publish_on_handle` (which returns `NotConnected` without a live
    ///   broker — the test only needs the ACL check to pass and the session lookup to
    ///   succeed, not a real broker ack).
    ///
    /// `allowed_clients` (the ACL) and `session_clients` (the sessions) are
    /// independent so a test can exercise "session exists but ACL denies" and the
    /// happy path separately. (An ACL-allowed client with no session is a
    /// boot-prevented invariant that panics at the publish path, so it is not a
    /// fixture case.)
    pub(crate) async fn test_new_for_mqtt_publish_acl(
        allowed_clients: &[&str],
        session_clients: &[&str],
    ) -> Arc<Self> {
        let (db, tx, user_id, conversation_id) = make_minimal_test_db_and_channel().await;
        let messenger = make_test_messenger_with_mqtt_publish(db.clone(), allowed_clients);

        let mqtt_service = brenn_lib::mqtt::MqttService::new();
        for client in session_clients {
            let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
            let config = Arc::new(crate::test_support::mqtt::test_client_config(client));
            let handle = brenn_lib::mqtt::MqttClientHandle::new(config, vec![], stop_tx);
            mqtt_service.add_client(handle).await;
        }

        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                messenger: Some(messenger),
                mqtt_service: Some(mqtt_service),
                ..Default::default()
            },
        )
    }

    /// Test bridge with a real `PwaPushService` where `"testapp"` has
    /// `allowed_users: ["bob"]` — alice is absent, so `get_target` for any
    /// alice address returns `GetTargetResult::Forbidden`. DB is pre-seeded
    /// with alice's device and subscription (same as `test_new_with_combined_services`)
    /// to prove that Forbidden fires before any subscription query, and to allow
    /// future tests that need alice's subscription to exist.
    ///
    /// Used by `pwa_push_channel_get_forbidden_returns_access_denied`.
    pub(crate) async fn test_new_with_restricted_push_access() -> Arc<Self> {
        let (tx, _rx) = tokio::sync::broadcast::channel(16);
        let db = brenn_lib::db::init_db_memory();
        let now = brenn_lib::db::format_ts_for_db(chrono::Utc::now());

        // Seed user + conversation, plus alice + device + subscription.
        let (user_id, conversation_id) = {
            let conn = db.lock().await;
            let (uid, cid) = seed_test_user_and_conversation(&conn, &now, "testapp");
            // alice: a second user; absent from allowed_users → Forbidden.
            seed_alice_with_subscription(&conn, &now);
            (uid, cid)
        };

        let tmp = tempfile::tempdir().expect("tempdir for VAPID key");
        let vapid = brenn_lib::pwa_push::vapid::load_or_generate(&tmp.path().join("vapid.json"));
        let pwa_config = brenn_lib::pwa_push::config::ResolvedPwaPushConfig {
            vapid,
            subject: "mailto:test@example.com".to_string(),
            endpoint_policy: brenn_lib::pwa_push::endpoint_validator::EndpointPolicy::new(
                vec![],
                false,
            ),
        };

        // "testapp" has allowed_users: ["bob"] — alice is absent → Forbidden.
        let mut apps = indexmap::IndexMap::new();
        apps.insert(
            "testapp".to_string(),
            make_test_push_app_config(tmp.path().to_path_buf(), vec!["bob".to_string()], true),
        );

        let (test_alert_dispatcher, _alert_handle) =
            brenn_lib::obs::alerting::noop_alert_dispatcher();
        let pwa_push_service = Arc::new(brenn_lib::pwa_push::PwaPushService::new(
            db.clone(),
            pwa_config,
            Arc::new(apps),
            brenn_lib::messaging::MessagingGlobalConfig::default(),
            std::sync::Arc::from("https://brenn.test"),
            test_alert_dispatcher,
        ));

        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                pwa_push_service: Some(pwa_push_service),
                ..Default::default()
            },
        )
    }

    /// Bridge with a real `PwaPushService` where the sender app (`"testapp"`)
    /// has push disabled (`pwa_push_enabled = false`). Used by intercept tests
    /// that drive the `PushSendResult::MissingSender` path (channel-disabled
    /// branch) through the intercept and assert the wire response shape.
    pub(crate) async fn test_new_with_push_disabled_service() -> Arc<Self> {
        let tmp = tempfile::tempdir().expect("tempdir for VAPID key");
        let vapid = brenn_lib::pwa_push::vapid::load_or_generate(&tmp.path().join("vapid.json"));
        let pwa_config = brenn_lib::pwa_push::config::ResolvedPwaPushConfig {
            vapid,
            subject: "mailto:test@example.com".to_string(),
            endpoint_policy: brenn_lib::pwa_push::endpoint_validator::EndpointPolicy::new(
                vec![],
                false,
            ),
        };

        // "testapp" has pwa_push block with enabled = false.
        let mut apps = indexmap::IndexMap::new();
        apps.insert(
            "testapp".to_string(),
            make_test_push_app_config(tmp.path().to_path_buf(), vec![], false),
        );

        let (test_alert_dispatcher, _alert_handle) =
            brenn_lib::obs::alerting::noop_alert_dispatcher();
        let pwa_push_service = Arc::new(brenn_lib::pwa_push::PwaPushService::new(
            brenn_lib::db::init_db_memory(),
            pwa_config,
            Arc::new(apps),
            brenn_lib::messaging::MessagingGlobalConfig::default(),
            std::sync::Arc::from("https://brenn.test"),
            test_alert_dispatcher,
        ));

        let (db, tx, user_id, conversation_id) = make_minimal_test_db_and_channel().await;
        Self::inject_for_test_full(
            user_id,
            conversation_id,
            "testapp",
            db,
            tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                pwa_push_service: Some(pwa_push_service),
                ..Default::default()
            },
        )
    }
}

/// Allocate an in-memory DB pre-seeded with one user (`"test"`) and one
/// conversation (`"testapp"`), plus a broadcast channel — the minimum setup
/// shared by `test_new_for_pwa_push` and `test_new_with_mqtt_publish_acl`.
///
/// Returns `(db, broadcast_tx, user_id, conversation_id)`.
async fn make_minimal_test_db_and_channel() -> (Db, broadcast::Sender<WsServerMessage>, i64, i64) {
    let (tx, _rx) = broadcast::channel(16);
    let db = brenn_lib::db::init_db_memory();
    let (user_id, conversation_id) = {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (username, password_hash, created_at) \
             VALUES ('test', 'x', datetime('now'))",
            [],
        )
        .expect("insert user");
        let uid: i64 = conn.last_insert_rowid();
        let cid = brenn_lib::conversation::create_conversation(&conn, uid, "testapp", false);
        (uid, cid)
    };
    (db, tx, user_id, conversation_id)
}

/// Build a `Messenger` (empty directory) whose `"testapp"` policy grants
/// `MqttPublish` and holds an `mqtt_publish` ACL matcher for each client in
/// `clients`. Used by MQTT-publish intercept fixtures/tests so the Seam-C
/// per-client publish ACL check (design §2.4) — which reaches the policy via
/// `messenger.app_policy("testapp")` — resolves and passes for those clients.
fn make_test_messenger_with_mqtt_publish(
    db: Db,
    clients: &[&str],
) -> Arc<brenn_lib::messaging::Messenger> {
    let mut testapp_cfg =
        crate::test_support::app_config::default_test_app_config("testapp", "testapp");
    testapp_cfg
        .policy
        .grants
        .insert(brenn_lib::access::AppCapability::MqttPublish);
    for client in clients {
        testapp_cfg
            .policy
            .acls
            .mqtt_publish
            .push(brenn_lib::access::acl::MqttClientMatcher {
                client: (*client).to_string(),
            });
    }
    let mut messenger_apps = indexmap::IndexMap::new();
    messenger_apps.insert("testapp".to_string(), testapp_cfg);
    brenn_lib::messaging::Messenger::new(
        db,
        Arc::new(brenn_lib::messaging::MessagingDirectory::with_entries(
            vec![],
        )),
        Arc::from("test-source"),
        Arc::new(messenger_apps),
        Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
            as Arc<dyn brenn_lib::messaging::WakeRouter>,
        brenn_lib::messaging::MessagingGlobalConfig::default(),
    )
}

/// Build a `Messenger` (empty directory) whose `"testapp"` policy does **not**
/// grant `MqttPublish`. Used by the MQTT-publish "grant absent" intercept test:
/// after the shared-enforcement refactor (design §2.3) the intercept reaches the
/// policy via `messenger.app_policy("testapp")` and `enforce_and_publish` returns
/// `AclDenied`; the intercept's secondary `has_grant(MqttPublish)` check (false
/// here) selects the "MQTT publish is not enabled" remedy string.
fn make_test_messenger_no_mqtt_grant(db: Db) -> Arc<brenn_lib::messaging::Messenger> {
    let testapp_cfg =
        crate::test_support::app_config::default_test_app_config("testapp", "testapp");
    let mut messenger_apps = indexmap::IndexMap::new();
    messenger_apps.insert("testapp".to_string(), testapp_cfg);
    brenn_lib::messaging::Messenger::new(
        db,
        Arc::new(brenn_lib::messaging::MessagingDirectory::with_entries(
            vec![],
        )),
        Arc::from("test-source"),
        Arc::new(messenger_apps),
        Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
            as Arc<dyn brenn_lib::messaging::WakeRouter>,
        brenn_lib::messaging::MessagingGlobalConfig::default(),
    )
}

/// Build a minimal `AppConfig` for `"testapp"` suitable for `PwaPushService` tests.
///
/// `state_dir` is used for both `working_dir` and `state_dir`. `allowed_users`
/// controls the per-app access list (pass `vec![]` for open access). `push_enabled`
/// sets `AppPwaPushBlock::enabled`.
fn make_test_push_app_config(
    state_dir: std::path::PathBuf,
    allowed_users: Vec<String>,
    push_enabled: bool,
) -> brenn_lib::config::AppConfig {
    brenn_lib::config::AppConfig {
        slug: "testapp".to_string(),
        name: "testapp".to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: state_dir.clone(),
        model: "claude-sonnet".to_string(),
        single_instance: false,
        singleton: false,
        persistent: false,
        idle_timeout: None,
        compaction: None,
        idle_hook_secs: 0,
        allowed_users,
        disabled_tools: vec![],
        mcp_servers: std::collections::HashMap::new(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: false,
        path_mapper: PathMapper::Identity,
        container_spawn: None,
        start_hooks: Default::default(),
        post_pull_hooks: Default::default(),
        startup_hooks: Default::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: std::collections::HashMap::new(),
        mounts: vec![],
        history_replay_limit: 2000,
        frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
        state_dir,
        messaging: None,
        messaging_default_send_budget: 100,
        // Grant PwaPush exactly when this fixture wants push enabled, so
        // pwa_push_enabled() reflects the intended state.
        policy: {
            let mut p = brenn_lib::access::AppPolicy::default();
            if push_enabled {
                p.grants.insert(brenn_lib::access::AppCapability::PwaPush);
            }
            p
        },
        // Push authorization is the `PwaPush` grant on `policy` above (set from
        // `push_enabled`); the block carries only delivery settings now (§2.5.1).
        pwa_push: Some(brenn_lib::pwa_push::config::AppPwaPushBlock {
            default_title: None,
        }),
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    }
}

/// Seed alice (user), a device, and a push subscription into an already-locked
/// connection. Returns `alice_id`.
///
/// Called after the `"test"` user + conversation are already inserted. `now`
/// must be a DB-formatted timestamp string (from `brenn_lib::db::format_ts_for_db`).
/// Seed the test user `'test'` and a conversation for `app_slug`, returning
/// `(user_id, conversation_id)`. Replaces the hand-rolled INSERT-user +
/// `create_conversation` block copied across the bridge test fixtures
/// (quality-6); a schema change to `users`/`conversations` now touches one place.
fn seed_test_user_and_conversation(
    conn: &rusqlite::Connection,
    now: &str,
    app_slug: &str,
) -> (i64, i64) {
    conn.execute(
        "INSERT INTO users (username, password_hash, created_at) VALUES ('test', 'x', ?1)",
        rusqlite::params![now],
    )
    .expect("insert test user");
    let uid: i64 = conn.last_insert_rowid();
    let cid = brenn_lib::conversation::create_conversation(conn, uid, app_slug, false);
    (uid, cid)
}

/// The dynamic-subscribe `AppPolicy` stamped on the `mqtt:`-subscribe fixture's
/// apps. Grants `DynamicSubscribe` + `MqttSubscribe` and one `mqtt_subscribe`
/// matcher (client `home`, filter `#`) that covers every filter the
/// activation/intercept tests request on the `home` client (`sensors/+/temp`,
/// `sensors/explicit`, `sensors/x`, `home/kitchen/state`, …), so the gate admits
/// them. The matcher is still client-scoped to `home`, so a subscribe naming any
/// other client (e.g. `nope`) is denied — the gate stays meaningful.
fn mqtt_subscribe_test_policy() -> brenn_lib::access::AppPolicy {
    crate::test_support::app_config::mqtt_acl_policy("home", "#")
}

fn seed_alice_with_subscription(conn: &rusqlite::Connection, now: &str) -> i64 {
    conn.execute(
        "INSERT INTO users (username, password_hash, created_at) VALUES ('alice', 'x', ?1)",
        rusqlite::params![now],
    )
    .expect("insert alice");
    let alice_id: i64 = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO devices (token, guessed_slug, last_seen_at, created_at) VALUES ('tok1', 'laptop', ?1, ?1)",
        rusqlite::params![now],
    )
    .expect("insert device");
    let device_id: i64 = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at) VALUES (?1, ?2, ?3, ?3)",
        rusqlite::params![device_id, alice_id, now],
    )
    .expect("insert device_users");
    brenn_lib::pwa_push::db::upsert_subscription(
        conn,
        device_id,
        alice_id,
        &brenn_lib::pwa_push::endpoint_validator::ValidatedEndpoint::for_testing(
            "https://push.example.com/sub",
        ),
        "p256dh_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "auth_AAAAAAAAAAAAAAAAAAAAAA",
    );
    alice_id
}

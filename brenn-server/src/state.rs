use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use brenn_wasm::ReplayComponent;

use brenn_lib::app::AppTool;
use brenn_lib::config::AppConfig;
use brenn_lib::db::Db;
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::ws_types::ModelInfo;
use indexmap::IndexMap;
use tokio::sync::{Mutex, RwLock, broadcast};
#[cfg(not(test))]
use tracing::info;
#[cfg(not(test))]
use tracing::warn;
use uuid::Uuid;

#[cfg(not(test))]
use crate::active_bridge::SpawnContext;
use crate::active_bridge::{ActiveBridge, ActiveBridges};
use crate::repo_sync::SyncTriggerSender;

/// Notification that a new bridge was spawned for a conversation.
/// WS connections watching the same conversation auto-attach.
#[derive(Debug, Clone)]
pub struct BridgeSpawned {
    pub conversation_id: i64,
    pub app_slug: String,
}

/// A file uploaded via POST /app/{slug}/upload, awaiting reference in a SendMessage.
#[derive(Debug, Clone)]
pub struct PendingUpload {
    pub app_slug: String,
    pub filename: String,
    pub disk_filename: String,
    pub media_type: String,
    pub size: u64,
    pub uploaded_at: tokio::time::Instant,
    pub uploader_user_id: i64,
}

/// Thread-safe registry of pending (not yet sent) uploads.
pub type PendingUploads = Arc<Mutex<HashMap<Uuid, PendingUpload>>>;

/// Application state shared across all handlers via Axum's State extractor.
#[derive(Clone)]
pub struct AppState {
    /// The build identifier this server was built from (git short SHA, semver
    /// tag, or `unknown-dev`). Threaded in from the binary crate's compile-time
    /// const via `run_server`; the WS/surface stale-client handshakes compare
    /// the client's `build` param byte-for-byte against it.
    ///
    /// Never read the build-id environment variable in this crate: the whole
    /// point of the crate split is that this library does not vary with the
    /// build id, so its test binary stays a cache hit across commits.
    pub build_id: &'static str,
    pub db: Db,
    #[cfg_attr(test, allow(dead_code))]
    pub alert_dispatcher: AlertDispatcher,
    pub active_bridges: ActiveBridges,
    /// Whether to set the Secure flag on session cookies.
    pub secure_cookies: bool,
    /// Directory for log files (including CC transcripts).
    #[cfg_attr(test, allow(dead_code))]
    pub log_dir: PathBuf,
    /// Path to the Brenn DisplayFile MCP stub script (server-global).
    #[cfg_attr(test, allow(dead_code))]
    pub mcp_script_path: PathBuf,
    /// Per-app configurations, keyed by slug. Iteration order is the
    /// declaration order of `[[app]]` blocks in `brenn.toml`.
    pub apps: Arc<IndexMap<String, AppConfig>>,
    /// Notification channel for bridge spawn events.
    /// WS connections subscribe to auto-attach when a bridge spawns
    /// for a conversation they're viewing.
    pub bridge_notify_tx: broadcast::Sender<BridgeSpawned>,
    /// Uploads awaiting reference in a SendMessage. Keyed by upload_id (UUID).
    pub pending_uploads: PendingUploads,
    /// Directory containing static frontend assets (JS, CSS, manifest).
    pub static_dir: PathBuf,
    /// Directory containing surface assets (wasm shell + component modules),
    /// served under `/surface-static`.
    pub surface_dist_dir: PathBuf,
    /// Cached available models from CC's init ack, keyed by app slug.
    /// Populated on first CC spawn per app; refreshed on subsequent spawns.
    pub cached_models: Arc<RwLock<HashMap<String, Vec<ModelInfo>>>>,
    /// Per-tool extension implementations, keyed by tool name. Looked up by
    /// ActiveBridge for custom formatting, auto-approve, and display.
    pub tool_registry: Arc<HashMap<String, Arc<dyn AppTool>>>,
    /// First-class tool registry (grant-governed tools invocable by LLM and
    /// WASM callers alike). Threaded onto each `ActiveBridge` for the LLM path.
    /// Read only on the (non-test) bridge-spawn path.
    #[cfg_attr(test, allow(dead_code))]
    pub tools: Arc<crate::tool_registry::ToolRegistry>,
    /// Origin string for tool-caller `ParticipantId`s (`app:<slug>@<origin>`).
    /// The messaging source when configured, else the server bind address.
    /// Read only on the (non-test) bridge-spawn path.
    #[cfg_attr(test, allow(dead_code))]
    pub tool_server_origin: Arc<str>,
    /// Per-conversation wake locks. Prevents concurrent `wake_conversation`
    /// calls from double-spawning CC.
    #[cfg_attr(test, allow(dead_code))]
    pub wake_locks: WakeLocks,
    /// Process-wide flag set by `shutdown_signal` when SIGTERM / SIGINT
    /// arrives. Bridges' `SessionEvent::Died` handler consults it (alongside
    /// `drain_on_idle`) to suppress the "CC session died" warning alert
    /// during intentional server teardown. Independent from the per-session
    /// `shutting_down` flag: that one gates the reader-task EOF alert in
    /// `brenn-cc`; this one gates the event-loop's Died handler.
    ///
    /// `shutdown_signal` also iterates `active_bridges` and calls
    /// `mark_shutting_down()` on each session so the reader path stays
    /// quiet too.
    pub server_shutting_down: Arc<AtomicBool>,
    /// Repo-sync trigger sender. `None` when no sync-enabled clones are
    /// configured — in that case the feature is effectively disabled and
    /// call sites that would fire a trigger (webhook endpoint, push hook,
    /// resume-poke) skip gracefully.
    ///
    /// See `docs/designs/repo-sync.md`.
    #[allow(dead_code)] // Consumers wired in Phases 2–4.
    pub repo_sync_sender: Option<SyncTriggerSender>,
    /// Messenger for the messaging MVP. `None` when no `[[channel]]`
    /// is configured (messaging effectively disabled).
    pub messenger: Option<Arc<brenn_lib::messaging::Messenger>>,
    /// PWA push service (VAPID keypair, subscription DB, HTTP client). `None` when
    /// no app has `pwa_push.enabled = true` (push effectively disabled).
    pub pwa_push: Option<Arc<dyn brenn_lib::pwa_push::PwaPushSender>>,
    /// MQTT service (per-client session supervisors, event router). `None` when no
    /// `[[mqtt_client]]` is referenced by any ingress channel or `mqtt_publish`/
    /// `mqtt_subscribe` ACL matcher (`bootstrap/mqtt::referenced_clients`).
    #[cfg_attr(test, allow(dead_code))]
    pub mqtt: Option<Arc<brenn_lib::mqtt::MqttService>>,
    /// Concrete MQTT inbound event router. `None` when MQTT is not configured.
    /// Threaded onto each spawned `ActiveBridge` so a runtime `mqtt:` dynamic
    /// subscribe can call `add_route` (design §2.3 step 6). The `Arc<dyn
    /// MqttEventRouter>` the supervisors hold exposes only `deliver_inbound`, so
    /// the concrete handle is retained here separately from `mqtt`.
    #[cfg_attr(test, allow(dead_code))]
    pub mqtt_event_router: Option<Arc<crate::mqtt_router::MqttEventRouterImpl>>,
    /// Webhook service (endpoint registry, event router). `None` when no
    /// `[[webhook_endpoint]]` is configured or no app declares any
    /// `[[app.webhook_subscription]]`.
    #[cfg_attr(test, allow(dead_code))]
    pub webhook: Option<Arc<brenn_lib::webhook::WebhookService>>,
    /// Automation engine. `None` when automation is not configured (no messenger
    /// or no apps with allowed_users).
    #[cfg_attr(test, allow(dead_code))]
    pub automation_engine: Option<Arc<brenn_lib::automation::AutomationEngine>>,
    /// Replay-protection components, keyed by webhook endpoint slug.
    /// Empty map = no endpoint is replay-protected.
    /// Populated at startup from `ResolvedWebhookEndpoint.replay_protection`.
    pub replay_components: Arc<HashMap<String, Arc<ReplayComponent>>>,
    /// Per-endpoint serialization locks for replay component calls.
    ///
    /// `ReplayComponent::check` runs in `spawn_blocking` and internally holds
    /// the SQLite `tx_active` CAS guard. Concurrent requests for the same
    /// endpoint that both call `spawn_blocking` simultaneously race on that CAS
    /// and the loser panics (→ 500). This per-endpoint `tokio::sync::Mutex`
    /// serializes the `spawn_blocking` calls so concurrent inbound requests wait
    /// rather than fail. One entry per replay-protected endpoint; empty for
    /// unbound endpoints (fast path). Keyed by endpoint slug.
    pub replay_locks: Arc<HashMap<String, Arc<Mutex<()>>>>,
    /// Usage session gap in seconds. A new usage event that arrives more than
    /// this many seconds after `last_activity_at` closes the prior session and
    /// opens a new one. Default (and test fixture value): 1800 (30 minutes).
    pub usage_session_gap_secs: u32,
    /// Boot-resolved surfaces, keyed by slug. Empty when no `[[surface]]`
    /// blocks are configured.
    pub surfaces: Arc<HashMap<String, Arc<crate::routes::surface::SurfaceRuntime>>>,
    /// Attached surface WS sessions (slug → handles). A durable push router
    /// reads this to route wakes to live connections.
    pub surface_registry: crate::routes::surface::registry::SurfaceRegistry,
    /// Idle-heartbeat interval advertised in `Welcome`. `HEARTBEAT_SECS` in
    /// production; test states set 1 for fast integration tests.
    pub surface_heartbeat_secs: u32,
    /// Test-only: bridge to return from `wake_conversation`. Consumed on first call.
    #[cfg(test)]
    pub test_wake_bridge: Arc<Mutex<Option<Arc<ActiveBridge>>>>,
}

/// Per-conversation lock map for wake_conversation concurrency control.
///
/// Lightweight: only holds entries for conversations currently being woken.
/// The lock prevents two concurrent wake_conversation calls from both spawning CC.
#[derive(Clone, Default)]
pub struct WakeLocks {
    #[cfg_attr(test, allow(dead_code))]
    inner: Arc<Mutex<HashMap<i64, Arc<Mutex<()>>>>>,
}

impl WakeLocks {
    /// Acquire the wake lock for a conversation.
    ///
    /// Returns an owned mutex guard. Entries are never removed from the map —
    /// they're `Arc<Mutex<()>>` (~64 bytes each), bounded by conversation count,
    /// and not worth the complexity of cleanup.
    #[cfg(not(test))]
    async fn lock(&self, conversation_id: i64) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut map = self.inner.lock().await;
            map.entry(conversation_id)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }
}

impl AppState {
    /// Fire-and-forget eager wake: spawn `wake_conversation` in a background task.
    /// Logs errors server-side but does not surface them to the user.
    ///
    /// `tz` is the spawning `WsConnection`'s browser-reported timezone,
    /// used to seed `GRAF_USER_TZ` in CC's environment. See
    /// `docs/designs/graf-user-tz.md`.
    #[cfg(not(test))]
    pub fn spawn_eager_wake(&self, conversation_id: i64, tz: chrono_tz::Tz) {
        let state = self.clone();
        tokio::spawn(async move {
            if let Err(e) = state.wake_conversation(conversation_id, tz).await {
                tracing::error!(conversation_id, "eager spawn failed: {e}");
            }
        });
    }

    /// Test-mode no-op: eager wake does nothing (no real CC to spawn).
    #[cfg(test)]
    pub fn spawn_eager_wake(&self, _conversation_id: i64, _tz: chrono_tz::Tz) {}

    /// Wake CC for a conversation. No-op if bridge already running.
    /// Spawns CC with `--resume` if the conversation has a prior session.
    ///
    /// Concurrency: uses a per-conversation lock to prevent double-spawn.
    /// Returns the bridge (existing or newly spawned).
    #[cfg(not(test))]
    pub async fn wake_conversation(
        &self,
        conversation_id: i64,
        tz: chrono_tz::Tz,
    ) -> Result<Arc<ActiveBridge>, String> {
        let conv = {
            let conn = self.db.lock().await;
            brenn_lib::conversation::get_conversation(&conn, conversation_id)
        };
        self.spawn_if_absent(&conv, tz).await
    }

    /// Wake CC for a conversation using a pre-loaded `Conversation` value.
    ///
    /// Identical to `wake_conversation` but skips the DB fetch — for callers
    /// (e.g. `resolve_bridge` Case 2) that already hold the row and would
    /// otherwise pay a second lock acquisition to re-fetch it.
    #[cfg(not(test))]
    pub async fn wake_with_conv(
        &self,
        conv: &brenn_lib::conversation::Conversation,
        tz: chrono_tz::Tz,
    ) -> Result<Arc<ActiveBridge>, String> {
        self.spawn_if_absent(conv, tz).await
    }

    /// Shared implementation for `wake_conversation` and `wake_with_conv`.
    ///
    /// Acquires the per-conversation wake lock (double-checked), assembles
    /// the `SpawnContext`, spawns CC, and logs start-hook warnings.
    /// The only difference between the two public entrypoints is whether the
    /// caller has already fetched the `Conversation` row from the DB.
    #[cfg(not(test))]
    async fn spawn_if_absent(
        &self,
        conv: &brenn_lib::conversation::Conversation,
        tz: chrono_tz::Tz,
    ) -> Result<Arc<ActiveBridge>, String> {
        let conversation_id = conv.id;

        // Fast path: already running (no lock needed).
        if let Some(bridge) = self.active_bridges.get(conversation_id).await {
            return Ok(bridge);
        }

        // Acquire per-conversation wake lock.
        let _guard = self.wake_locks.lock(conversation_id).await;

        // Re-check after acquiring lock (another caller may have spawned).
        if let Some(bridge) = self.active_bridges.get(conversation_id).await {
            return Ok(bridge);
        }

        let app_config = self
            .apps
            .get(&conv.app_slug)
            .ok_or_else(|| format!("unknown app: {}", conv.app_slug))?;

        let resume_id = conv.cc_session_id.clone();

        let alert_dispatcher = self
            .alert_dispatcher
            .with_field("App", &conv.app_slug)
            .with_field("User", conv.user_id.to_string())
            .with_field("Conversation", conversation_id.to_string())
            .with_field("Lifecycle", "wake");

        info!(
            conversation_id,
            app_slug = %conv.app_slug,
            has_resume_id = resume_id.is_some(),
            "waking conversation"
        );

        let (bridge, _rx, warnings, _models) = self
            .spawn_and_register_bridge(SpawnContext {
                user_id: conv.user_id,
                conversation_id,
                shared: conv.shared,
                db: self.db.clone(),
                alert_dispatcher,
                active_bridges: self.active_bridges.clone(),
                resume_session_id: resume_id,
                log_dir: &self.log_dir,
                mcp_script_path: &self.mcp_script_path,
                app_config,
                model_override: None,
                tool_registry: self.tool_registry.clone(),
                tools: self.tools.clone(),
                server_origin: self.tool_server_origin.clone(),
                server_shutting_down: self.server_shutting_down.clone(),
                user_tz: tz,
                repo_sync_sender: self.repo_sync_sender.clone(),
                messenger: self.messenger.clone(),
                pwa_push_service: self.pwa_push.clone(),
                mqtt_service: self.mqtt.clone(),
                mqtt_event_router: self.mqtt_event_router.clone(),
                automation_engine: self.automation_engine.clone(),
                usage_session_gap_secs: self.usage_session_gap_secs,
            })
            .await?;

        // Log start hook warnings (e.g. auto_pull failures). These are non-fatal
        // but should be observable server-side.
        for w in &warnings {
            warn!(conversation_id, "start hook warning: {w}");
        }

        Ok(bridge)
    }

    /// Spawn a CC subprocess, cache its models, register the bridge, and notify.
    ///
    /// Shared by `wake_conversation` (autonomous wakes) and `WsConnection::spawn_bridge`
    /// (user-triggered spawns). Returns `(bridge, initial_rx, warnings, model_infos)`.
    /// Callers that need to send `ModelsAvailable` over WS should use `model_infos`;
    /// callers that need to subscribe from the start should use `initial_rx` (the
    /// pre-created receiver that captures all broadcasts from the event loop's first
    /// message). `initial_rx` must be passed to `attach_to_bridge_with_rx`; do not
    /// discard it and call `bridge.subscribe()` instead.
    #[cfg(not(test))]
    pub async fn spawn_and_register_bridge(
        &self,
        ctx: SpawnContext<'_>,
    ) -> Result<
        (
            std::sync::Arc<ActiveBridge>,
            tokio::sync::broadcast::Receiver<brenn_lib::ws_types::WsServerMessage>,
            Vec<String>,
            Vec<ModelInfo>,
        ),
        String,
    > {
        let app_slug = ctx.app_config.slug.clone();
        let conversation_id = ctx.conversation_id;

        let (bridge, rx, warnings, models) = ActiveBridge::spawn_new(ctx).await?;

        // Convert CC ModelOption → WS ModelInfo for callers and the model cache.
        let model_infos: Vec<ModelInfo> = models
            .iter()
            .map(|m| ModelInfo {
                value: m.value.clone(),
                display_name: m.display_name.clone(),
                description: m.description.clone(),
            })
            .collect();

        // Cache models in memory and DB.
        if !model_infos.is_empty() {
            self.cached_models
                .write()
                .await
                .insert(app_slug.clone(), model_infos.clone());
            let conn = self.db.lock().await;
            brenn_lib::db::save_app_models(&conn, &app_slug, &model_infos);
        }

        // Register in active_bridges.
        self.active_bridges
            .insert(conversation_id, bridge.clone())
            .await;

        // Deliver any pending tool results that accumulated while CC was down.
        bridge.deliver_pending_results().await;

        // Notify WS connections watching this conversation.
        if self
            .bridge_notify_tx
            .send(BridgeSpawned {
                conversation_id,
                app_slug: app_slug.clone(),
            })
            .is_err()
        {
            tracing::debug!("bridge spawn notification with no listeners");
        }

        Ok((bridge, rx, warnings, model_infos))
    }

    /// Shared implementation for the two test wake stubs. Checks `active_bridges`
    /// first (fast path), then drains `test_wake_bridge` and inserts the result.
    #[cfg(test)]
    async fn test_wake_bridge_impl(
        &self,
        conversation_id: i64,
    ) -> Result<Arc<ActiveBridge>, String> {
        if let Some(bridge) = self.active_bridges.get(conversation_id).await {
            return Ok(bridge);
        }
        let bridge = self
            .test_wake_bridge
            .lock()
            .await
            .take()
            .ok_or_else(|| "no test bridge registered for conversation".to_string())?;
        self.active_bridges
            .insert(conversation_id, bridge.clone())
            .await;
        if self
            .bridge_notify_tx
            .send(BridgeSpawned {
                conversation_id,
                app_slug: bridge.app_slug.clone(),
            })
            .is_err()
        {
            tracing::debug!("bridge spawn notification with no listeners (test)");
        }
        Ok(bridge)
    }

    /// Test-mode wake_with_conv: mirrors production by using the already-held
    /// `conv` directly (fast path check + test_wake_bridge spawn), without
    /// going through `wake_conversation`'s DB re-fetch path.
    ///
    /// This structure exercises the same code shape as production
    /// (`spawn_if_absent` called with a pre-loaded `Conversation`), so a
    /// refactor that removes or changes the "conv already held" optimization
    /// will break this test stub rather than passing silently.
    #[cfg(test)]
    pub async fn wake_with_conv(
        &self,
        conv: &brenn_lib::conversation::Conversation,
        _tz: chrono_tz::Tz,
    ) -> Result<Arc<ActiveBridge>, String> {
        self.test_wake_bridge_impl(conv.id).await
    }

    /// Test-mode wake_conversation: checks active_bridges first (fast path),
    /// then falls back to `test_wake_bridge` (simulates spawning).
    /// Inserts the bridge into active_bridges and sends BridgeSpawned notification.
    #[cfg(test)]
    pub async fn wake_conversation(
        &self,
        conversation_id: i64,
        _tz: chrono_tz::Tz,
    ) -> Result<Arc<ActiveBridge>, String> {
        self.test_wake_bridge_impl(conversation_id).await
    }

    /// Submit an event for a conversation. If CC is running, delivers immediately.
    /// If CC is sleeping, queues the event and optionally wakes CC.
    ///
    /// Construct an `AppState` with test-safe defaults. Every field not related
    /// to the DB or app config is populated with the canonical test fixture values
    /// used across `routes::ws` tests, so adding a new `AppState` field only
    /// requires updating this one function.
    ///
    /// `apps` defaults to a single `"test"` app (via `test_apps()`) when callers
    /// pass `None`; pass `Some(apps)` to override.
    #[cfg(test)]
    pub(crate) fn for_test(
        db: brenn_lib::db::Db,
        apps: Option<Arc<IndexMap<String, brenn_lib::config::AppConfig>>>,
    ) -> Self {
        use tokio::sync::broadcast;
        let (alert_dispatcher, _handle) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let apps = apps.unwrap_or_else(crate::test_support::app_config::test_apps);
        AppState {
            build_id: crate::test_support::TEST_BUILD_ID,
            db,
            alert_dispatcher,
            active_bridges: ActiveBridges::new(),
            secure_cookies: false,
            log_dir: std::path::PathBuf::from("logs"),
            mcp_script_path: std::path::PathBuf::from("noop_mcp.py"),
            apps,
            bridge_notify_tx: broadcast::channel(64).0,
            pending_uploads: Default::default(),
            static_dir: std::path::PathBuf::from("frontend/dist"),
            surface_dist_dir: std::path::PathBuf::from("surface/dist"),
            cached_models: Default::default(),
            tool_registry: Default::default(),
            tools: Arc::new(crate::tool_registry::ToolRegistry::new(vec![])),
            tool_server_origin: Arc::from("test-origin"),
            wake_locks: Default::default(),
            server_shutting_down: Arc::new(AtomicBool::new(false)),
            repo_sync_sender: None,
            messenger: None,
            pwa_push: None,
            mqtt: None,
            mqtt_event_router: None,
            webhook: None,
            automation_engine: None,
            usage_session_gap_secs: 1800,
            surfaces: Arc::new(HashMap::new()),
            surface_registry: Default::default(),
            surface_heartbeat_secs: 1,
            replay_components: Arc::new(HashMap::new()),
            replay_locks: Arc::new(HashMap::new()),
            test_wake_bridge: Default::default(),
        }
    }
}

/// Adapter implementing `brenn_lib::automation::IngressRouter` over `AppState`.
///
/// Uses the same deferred-state pattern as `WakeRouterImpl`: the `AppState`
/// is not yet constructed when `AutomationEngine` is built, so we stash a
/// `OnceCell<AppState>` here and call `set_state` immediately after `AppState`
/// construction.
pub struct IngressRouterImpl {
    state: tokio::sync::OnceCell<AppState>,
}

impl IngressRouterImpl {
    pub fn new() -> Self {
        Self {
            state: tokio::sync::OnceCell::new(),
        }
    }

    /// Fill in the `AppState`. Must be called before any automation fire
    /// can reach `submit_ingress`.
    pub fn set_state(&self, state: AppState) {
        self.state
            .set(state)
            .map_err(|_| ())
            .expect("IngressRouterImpl state already set");
    }
}

#[async_trait::async_trait]
impl brenn_lib::automation::IngressRouter for IngressRouterImpl {
    async fn submit_ingress(
        &self,
        conversation_id: i64,
        app_slug: &str,
        source: &str,
        summary: &str,
        payload: &str,
        urgency: brenn_lib::messaging::Urgency,
    ) {
        let state = self
            .state
            .get()
            .expect("IngressRouterImpl state must be set before submit_ingress is called");
        let messenger = state
            .messenger
            .as_ref()
            .expect("IngressRouterImpl: messenger must be set (automation requires messaging)");
        messenger
            .submit_ingress(conversation_id, app_slug, source, summary, payload, urgency)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::broadcast;

    use super::AppState;
    use crate::active_bridge::ActiveBridge;

    /// Verify that `test_wake_bridge_impl` returns the cached `Arc<ActiveBridge>`
    /// when a bridge for the conversation is already in `active_bridges` (fast path),
    /// without consuming `test_wake_bridge`.
    #[tokio::test]
    async fn wake_conversation_fast_path_returns_cached_bridge() {
        let db = brenn_lib::db::init_db_memory();
        let state = AppState::for_test(db.clone(), None);
        let (broadcast_tx, _) = broadcast::channel(64);

        let conv_id = 7_i64;
        let bridge = ActiveBridge::inject_for_test(1, conv_id, "test", db, broadcast_tx);

        // Pre-insert the bridge so the fast path is reachable.
        state.active_bridges.insert(conv_id, bridge.clone()).await;

        let result = state
            .wake_conversation(conv_id, chrono_tz::Tz::UTC)
            .await
            .expect("wake_conversation should succeed on cached bridge");

        assert!(
            Arc::ptr_eq(&result, &bridge),
            "fast path must return the exact cached Arc, not a newly spawned bridge"
        );

        // test_wake_bridge should remain None — the fast path must not have consumed it.
        assert!(
            state.test_wake_bridge.lock().await.is_none(),
            "fast path must not drain test_wake_bridge"
        );
    }
}

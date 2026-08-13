use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use brenn_wasm::ReplayComponent;

use brenn_db::Db;
use brenn_lib::app::AppTool;
use brenn_lib::config::AppConfig;
use brenn_obs::alerting::AlertDispatcher;
use brenn_ws_types::ModelInfo;
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
use brenn_git::sync::SyncTriggerSender;

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
    pub tools: Arc<brenn_tool_registry::ToolRegistry>,
    /// Origin string for tool-caller `ParticipantId`s (`app:<slug>@<origin>`).
    /// The messaging source when configured, else the server bind address.
    /// Read only on the (non-test) bridge-spawn path.
    #[cfg_attr(test, allow(dead_code))]
    pub tool_server_origin: Arc<str>,
    /// Per-conversation wake locks. Prevents concurrent `wake_conversation`
    /// calls from double-spawning CC.
    #[cfg_attr(test, allow(dead_code))]
    pub wake_locks: WakeLocks,
    /// Per-conversation retry backoff for *failed* spawns. Declines bus-driven
    /// spawn attempts while armed; see [`SpawnBackoff`].
    #[cfg_attr(test, allow(dead_code))]
    pub spawn_backoff: SpawnBackoff,
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
    pub messenger: Option<Arc<brenn_messaging::Messenger>>,
    /// PWA push service (VAPID keypair, subscription DB, HTTP client). `None` when
    /// no app has `pwa_push.enabled = true` (push effectively disabled).
    pub pwa_push: Option<Arc<dyn brenn_pwa_push::PwaPushSender>>,
    /// MQTT service (per-client session supervisors, event router). `None` when no
    /// `[[mqtt_client]]` is referenced by any ingress channel or `mqtt_publish`/
    /// `mqtt_subscribe` ACL matcher (`bootstrap/mqtt::referenced_clients`).
    #[cfg_attr(test, allow(dead_code))]
    pub mqtt: Option<Arc<brenn_mqtt::MqttService>>,
    /// Concrete MQTT inbound event router. `None` when MQTT is not configured.
    /// Threaded onto each spawned `ActiveBridge` so a runtime `mqtt:` dynamic
    /// subscribe can call `add_route`. The `Arc<dyn
    /// MqttEventRouter>` the supervisors hold exposes only `deliver_inbound`, so
    /// the concrete handle is retained here separately from `mqtt`.
    #[cfg_attr(test, allow(dead_code))]
    pub mqtt_event_router: Option<Arc<crate::mqtt_router::MqttEventRouterImpl>>,
    /// Webhook service (endpoint registry, event router). `None` when no
    /// `[[webhook_endpoint]]` is configured or no app declares any
    /// `[[app.webhook_subscription]]`.
    #[cfg_attr(test, allow(dead_code))]
    pub webhook: Option<Arc<brenn_webhook::WebhookService>>,
    /// Automation engine. `None` when automation is not configured (no messenger
    /// or no apps with allowed_users).
    #[cfg_attr(test, allow(dead_code))]
    pub automation_engine: Option<Arc<brenn_automation::AutomationEngine>>,
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
    pub surfaces: Arc<HashMap<String, Arc<brenn_surface_server::SurfaceRuntime>>>,
    /// Boot-resolved remotes, keyed by slug. Empty when no `[[remote]]` blocks
    /// are configured.
    pub remotes: Arc<HashMap<String, Arc<brenn_remote_server::RemoteRuntime>>>,
    /// Attached attachment sessions (attacher → handles). A durable push router
    /// reads this to route wakes to live connections.
    pub attach_registry: brenn_attach_server::registry::AttachRegistry,
    /// Idle-heartbeat interval advertised in `Welcome`, shared by both attach
    /// routes. `HEARTBEAT_SECS` in production; test states set 1 for fast
    /// integration tests.
    pub attach_heartbeat_secs: u32,
    /// Test-only: bridge to return from `wake_conversation`. Consumed on first call.
    #[cfg(test)]
    pub test_wake_bridge: Arc<Mutex<Option<Arc<ActiveBridge>>>>,
    /// Test-only: the conversations whose spawn attempts the entrypoints
    /// admitted, in order. A test build has no CC to spawn, so the spawn itself
    /// is the one thing stubbed out; recording what reached it is what makes the
    /// decisions in front of it — which trigger consults the backoff, and which
    /// does not — assertable rather than a comment.
    #[cfg(test)]
    pub wake_spawns: Arc<std::sync::Mutex<Vec<(i64, BusHold)>>>,
}

/// Per-conversation lock map for wake_conversation concurrency control.
///
/// Lightweight: only holds entries for conversations currently being woken.
/// The lock prevents two concurrent wake_conversation calls from both spawning CC.
///
/// **This is the single-spawn mechanism, and the only one.** `spawn_if_absent`'s
/// fast-path check, this guard held across the whole spawn, the re-check under
/// it, and registration into `active_bridges` only after the CC handshake are
/// together the spawn state machine: spawn → active → idle/errored, never more
/// than one live spawn per conversation, with concurrent wakes during the spawn
/// window collapsing onto the lock. Do not add a pacing or cooldown layer in
/// front of it on the theory that double-spawn needs a second defense — it does
/// not, and one was removed for being exactly that. The one thing that *does*
/// belong beside it is [`SpawnBackoff`], which damps spawns that **fail**, a
/// different problem a mutex cannot solve.
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

/// Base retry window after a failed conversation spawn: one dispatcher tick,
/// which is the cadence the wake walk retries at anyway.
const SPAWN_BACKOFF_BASE: Duration = brenn_messaging::dispatcher::POLL_INTERVAL;

/// Longest retry window a failure episode reaches. Bounds retry noise while
/// keeping recovery latency operator-tolerable — a spawn that starts working
/// again is picked up within 15 minutes even with no other trigger.
const SPAWN_BACKOFF_CAP: Duration = Duration::from_secs(15 * 60);

/// Per-conversation retry backoff for conversation spawns that **fail**.
///
/// A CC spawn is slow in wall time but cheap in resources, so a healthy wake is
/// never paced: if a message is going to be processed at all, processing it now
/// is strictly better than making it wait (and the token cache is hot right
/// after a cycle). What is worth damping is the one case where retrying buys
/// nothing — a spawn that errors. Bus-driven wakes arrive at kick rate, bounded
/// only by the publish rate, so an unbounded retry of a persistently failing
/// spawn is a storm.
///
/// The state is armed **only** by a failed attempt and cleared by a successful
/// one, so a working conversation never touches it. While armed it declines
/// bus-driven attempts from every trigger — kick, tick, deadline, urgency all
/// funnel into the same eager-wake attempt and the backoff sits below them all.
/// It does **not** decline a user-initiated spawn: a human retry is
/// self-pacing, no storm arises from it, and it is the likeliest recovery probe
/// in exactly this state — and its success clears the backoff for the bus side
/// too.
///
/// Positions never move while it is armed, so nothing is lost: when a spawn
/// finally succeeds, the drain serves the whole accumulated backlog.
///
/// This lives on the spawn machinery beside [`WakeLocks`], not on the
/// `Messenger`: it is spawn policy, and the spawner is what knows a spawn
/// failed.
#[derive(Clone, Default)]
pub struct SpawnBackoff {
    inner: Arc<std::sync::Mutex<HashMap<i64, BackoffEntry>>>,
}

/// One conversation's open failure episode.
struct BackoffEntry {
    /// When the current window opened — the instant of the failure that set it.
    opened: Instant,
    /// How long that window runs. `SPAWN_BACKOFF_BASE` on the first failure,
    /// doubling per consecutive failure, capped at `SPAWN_BACKOFF_CAP`.
    window: Duration,
    /// Whether this episode has already alerted on reaching the cap. Reset only
    /// by the success that ends the episode, so a conversation that cannot
    /// spawn alerts once per episode rather than once per process — an operator
    /// must hear every episode of a conversation silently not processing.
    alerted: bool,
}

impl SpawnBackoff {
    /// Whether a bus-driven spawn for this conversation is inside the window a
    /// previous failure opened.
    pub fn declines(&self, conversation_id: i64) -> bool {
        self.declines_at(conversation_id, Instant::now())
    }

    /// Record a failed spawn attempt: open the episode or double its window.
    /// Returns `true` exactly once per episode — on the failure that first
    /// reaches the cap — which is the caller's cue to alert.
    pub fn record_failure(&self, conversation_id: i64) -> bool {
        self.record_failure_at(conversation_id, Instant::now())
    }

    /// End any open episode: a spawn succeeded, so the next failure starts over
    /// at the base window and may alert again.
    pub fn clear(&self, conversation_id: i64) {
        self.map().remove(&conversation_id);
    }

    /// The clock-taking halves of the two accessors above, so the policy is
    /// testable without waiting out a 60-second window.
    fn declines_at(&self, conversation_id: i64, now: Instant) -> bool {
        self.map()
            .get(&conversation_id)
            .is_some_and(|entry| now.duration_since(entry.opened) < entry.window)
    }

    fn record_failure_at(&self, conversation_id: i64, now: Instant) -> bool {
        let mut map = self.map();
        let entry = map.entry(conversation_id).or_insert(BackoffEntry {
            opened: now,
            // Halved so the doubling below lands the first failure on the base
            // window — one rule for every failure, rather than a first-failure
            // special case that could drift from it.
            window: SPAWN_BACKOFF_BASE / 2,
            alerted: false,
        });
        entry.opened = now;
        entry.window = (entry.window * 2).min(SPAWN_BACKOFF_CAP);
        if entry.window == SPAWN_BACKOFF_CAP && !entry.alerted {
            entry.alerted = true;
            return true;
        }
        false
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<i64, BackoffEntry>> {
        self.inner.lock().expect("spawn_backoff lock poisoned")
    }
}

/// Whether the bridge a wake brings up is left held by the bus door.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BusHold {
    /// A peer asked for this conversation by name — a command or a pre-warm on
    /// its own chat channels — so the bus door holds the bridge for its idle
    /// window.
    ///
    /// The hold is taken at the spawn rather than left to the adapter's drain
    /// because a start-up drain can legitimately find nothing to serve: a
    /// predecessor bridge's last pass may have advanced the cursor between the
    /// wake pass reading it as owed and this spawn, or the position may be gone
    /// altogether. A spawned bridge that stamps nothing takes no hold from any
    /// door and arms no timer, and nothing ever re-asks the lifetime question
    /// for it — a CC subprocess parked forever for a conversation nobody
    /// touches again.
    Held,
    /// The wake is a delivery trigger for something else — an app subscriber's
    /// backlog — whose bridge lifetime this call does not decide.
    Unheld,
}

/// One wake attempt: bring the bridge up (or find it already up), leave the bus
/// door holding it when the trigger was the conversation's own, and hand the
/// outcome to the backoff.
pub(crate) async fn run_wake_attempt(
    state: AppState,
    conversation_id: i64,
    tz: chrono_tz::Tz,
    hold: BusHold,
) {
    let outcome = state.wake_conversation(conversation_id, tz).await;
    if let (BusHold::Held, Ok(bridge)) = (hold, &outcome) {
        bridge.note_bus_activity().await;
    }
    state.record_spawn_outcome(conversation_id, outcome.map(|_| ()));
}

impl AppState {
    /// Fire-and-forget eager wake for a **user-initiated** trigger: a browser
    /// attaching, switching conversation, or replacing a CC that died under a
    /// live session. Never declined by the spawn backoff — a human retry is
    /// self-pacing, and this is the likeliest probe to find a broken spawn
    /// working again.
    ///
    /// Logs errors server-side but does not surface them to the user.
    ///
    /// `tz` is the spawning `WsConnection`'s browser-reported timezone,
    /// used to seed `GRAF_USER_TZ` in CC's environment. See
    /// `docs/designs/graf-user-tz.md`.
    pub fn spawn_eager_wake(&self, conversation_id: i64, tz: chrono_tz::Tz) {
        // The browser attaching is its own door, and it takes its own hold.
        self.spawn_wake_task(conversation_id, tz, BusHold::Unheld);
    }

    /// Fire-and-forget eager wake for a **bus-driven** trigger: the wake walk,
    /// on kick, tick, deadline, or urgency. Every one of those funnels through
    /// here, so declining here declines all of them — which is what makes the
    /// backoff a bound on the whole bus side rather than on one trigger.
    pub fn spawn_bus_wake(&self, conversation_id: i64, tz: chrono_tz::Tz) {
        self.bus_wake(conversation_id, tz, BusHold::Unheld);
    }

    /// The same bus-driven trigger for a conversation's **own** chat channels,
    /// which additionally leaves the bus door holding what it brought up.
    pub fn spawn_chat_wake(&self, conversation_id: i64, tz: chrono_tz::Tz) {
        self.bus_wake(conversation_id, tz, BusHold::Held);
    }

    /// The backoff gate both bus entrypoints share.
    fn bus_wake(&self, conversation_id: i64, tz: chrono_tz::Tz, hold: BusHold) {
        if self.spawn_backoff.declines(conversation_id) {
            // The position does not move, so nothing is lost: the next attempt
            // past the window finds the same backlog.
            tracing::debug!(conversation_id, "bus wake declined by spawn backoff");
            return;
        }
        self.spawn_wake_task(conversation_id, tz, hold);
    }

    /// The spawn attempt every entrypoint makes.
    #[cfg(not(test))]
    fn spawn_wake_task(&self, conversation_id: i64, tz: chrono_tz::Tz, hold: BusHold) {
        let state = self.clone();
        drop(tokio::spawn(run_wake_attempt(
            state,
            conversation_id,
            tz,
            hold,
        )));
    }

    /// Test-mode stand-in: record the attempt instead of making it. There is no
    /// CC to spawn, and the decisions worth pinning are the ones in front of
    /// this call, not the subprocess behind it. The hold is recorded with it —
    /// it is one of those decisions, and the only observable difference between
    /// the two bus entrypoints.
    #[cfg(test)]
    fn spawn_wake_task(&self, conversation_id: i64, _tz: chrono_tz::Tz, hold: BusHold) {
        self.wake_spawns
            .lock()
            .expect("wake_spawns lock poisoned")
            .push((conversation_id, hold));
    }

    /// The one place a finished spawn attempt's outcome reaches the backoff: a
    /// success ends any open failure episode, a failure opens or widens one, and
    /// the failure that first reaches the cap tells the operator the
    /// conversation is silently not processing.
    fn record_spawn_outcome(&self, conversation_id: i64, outcome: Result<(), String>) {
        match outcome {
            Ok(()) => self.spawn_backoff.clear(conversation_id),
            Err(e) => {
                tracing::error!(conversation_id, "eager spawn failed: {e}");
                if self.spawn_backoff.record_failure(conversation_id) {
                    self.alert_dispatcher.alert(
                        brenn_obs::alerting::AlertSeverity::Warning,
                        "Conversation spawn failing".to_string(),
                        format!(
                            "Conversation {conversation_id} has failed to spawn repeatedly and \
                             its retry backoff has reached its {} minute ceiling; it is not \
                             processing anything published to it. Last error: {e}",
                            SPAWN_BACKOFF_CAP.as_secs() / 60,
                        ),
                    );
                }
            }
        }
    }

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
            brenn_db::conversation::get_conversation(&conn, conversation_id)
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
        conv: &brenn_db::conversation::Conversation,
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
        conv: &brenn_db::conversation::Conversation,
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
            tokio::sync::broadcast::Receiver<brenn_ws_types::WsServerMessage>,
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
            brenn_db::save_app_models(&conn, &app_slug, &model_infos);
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
        conv: &brenn_db::conversation::Conversation,
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
    #[cfg(any(test, feature = "testutils"))]
    pub fn for_test(
        db: brenn_db::Db,
        apps: Option<Arc<IndexMap<String, brenn_lib::config::AppConfig>>>,
    ) -> Self {
        use tokio::sync::broadcast;
        let (alert_dispatcher, _handle) = brenn_obs::alerting::noop_alert_dispatcher();
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
            tools: Arc::new(brenn_tool_registry::ToolRegistry::new(vec![])),
            tool_server_origin: Arc::from("test-origin"),
            wake_locks: Default::default(),
            spawn_backoff: Default::default(),
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
            remotes: Arc::new(HashMap::new()),
            attach_registry: Default::default(),
            attach_heartbeat_secs: 1,
            replay_components: Arc::new(HashMap::new()),
            replay_locks: Arc::new(HashMap::new()),
            // These two fields, and the wake stubs that read them, exist only
            // in this crate's own test build.
            #[cfg(test)]
            test_wake_bridge: Default::default(),
            #[cfg(test)]
            wake_spawns: Default::default(),
        }
    }
}

/// Adapter implementing `brenn_automation::IngressRouter` over `AppState`.
///
/// Uses the same deferred-state pattern as `WakeRouterImpl`: the `AppState`
/// is not yet constructed when `AutomationEngine` is built, so we stash a
/// `OnceCell<AppState>` here and call `set_state` immediately after `AppState`
/// construction.
pub struct IngressRouterImpl {
    state: tokio::sync::OnceCell<AppState>,
}

impl Default for IngressRouterImpl {
    fn default() -> Self {
        Self::new()
    }
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
impl brenn_automation::IngressRouter for IngressRouterImpl {
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

    use super::{AppState, BusHold};
    use crate::active_bridge::ActiveBridge;

    /// Verify that `test_wake_bridge_impl` returns the cached `Arc<ActiveBridge>`
    /// when a bridge for the conversation is already in `active_bridges` (fast path),
    /// without consuming `test_wake_bridge`.
    #[tokio::test]
    async fn wake_conversation_fast_path_returns_cached_bridge() {
        let db = crate::test_support::init_db_memory();
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

    // -----------------------------------------------------------------------
    // SpawnBackoff — tested with the clock passed in, so a 60-second window
    // costs no wall time.
    // -----------------------------------------------------------------------

    use super::{SPAWN_BACKOFF_BASE, SPAWN_BACKOFF_CAP, SpawnBackoff};
    use std::time::{Duration, Instant};

    /// A healthy spawn never touches the backoff, so an untouched conversation
    /// is never declined. This is the whole reason the arming condition is a
    /// *failure* and not an attempt.
    ///
    /// Runs through the wall-clock entrypoints the spawn path itself calls, so
    /// the rows below that pass their own clock cannot be pinning a policy that
    /// nothing production-side reaches.
    #[test]
    fn an_unarmed_conversation_is_never_declined() {
        let backoff = SpawnBackoff::default();
        assert!(!backoff.declines(1));
        backoff.record_failure(1);
        assert!(backoff.declines(1), "one failure arms the base window");
    }

    /// One failure opens a window of the base length: declined inside it,
    /// admitted the moment it lapses. Nothing renews the window but another
    /// failure, so a conversation that simply stays broken is retried at the
    /// tick rate rather than at the publish rate.
    #[test]
    fn a_failed_spawn_declines_until_its_window_lapses() {
        let backoff = SpawnBackoff::default();
        let t0 = Instant::now();
        assert!(
            !backoff.record_failure_at(1, t0),
            "the base window is not the cap, so nothing alerts yet"
        );

        assert!(backoff.declines_at(1, t0 + SPAWN_BACKOFF_BASE / 2));
        assert!(!backoff.declines_at(1, t0 + SPAWN_BACKOFF_BASE));
        assert!(
            !backoff.declines_at(2, t0),
            "the backoff is per conversation — one broken spawn holds up nobody else"
        );
    }

    /// Consecutive failures double the window to the cap and no further, and the
    /// cap alert fires exactly once per episode: an operator must hear that a
    /// conversation is silently not processing, but only once per outage.
    #[test]
    fn the_window_doubles_to_the_cap_and_alerts_once_per_episode() {
        let backoff = SpawnBackoff::default();
        let t0 = Instant::now();
        let mut alerts = 0;
        let mut failures = 0;
        // Enough failures to pass the cap several times over.
        while failures < 20 {
            if backoff.record_failure_at(1, t0) {
                alerts += 1;
            }
            failures += 1;
        }
        assert_eq!(alerts, 1, "one alert per failure episode, not per failure");
        assert!(backoff.declines_at(1, t0 + SPAWN_BACKOFF_CAP - Duration::from_secs(1)));
        assert!(
            !backoff.declines_at(1, t0 + SPAWN_BACKOFF_CAP),
            "the window is capped, so recovery latency stays operator-tolerable"
        );
    }

    /// Failures until the window reaches the cap, returning how many of them
    /// alerted. Bounded: a regression that made the cap unreachable, or the
    /// episode's alert flag sticky, must fail an assertion rather than hang the
    /// suite until CI times out.
    fn failures_to_cap(backoff: &SpawnBackoff, conversation_id: i64, at: Instant) -> usize {
        let mut alerts = 0;
        for _ in 0..20 {
            if backoff.record_failure_at(conversation_id, at) {
                alerts += 1;
            }
        }
        alerts
    }

    /// A successful spawn ends the episode: the next failure starts over at the
    /// base window and may alert again. This is also how a user-initiated spawn
    /// clears the bus side — both outcomes reach the same state.
    #[test]
    fn a_successful_spawn_ends_the_episode() {
        let backoff = SpawnBackoff::default();
        let t0 = Instant::now();
        assert_eq!(failures_to_cap(&backoff, 1, t0), 1);
        backoff.clear(1);
        assert!(!backoff.declines_at(1, t0));

        assert!(
            !backoff.record_failure_at(1, t0),
            "the episode restarted at the base window"
        );
        assert!(!backoff.declines_at(1, t0 + SPAWN_BACKOFF_BASE));
        assert_eq!(
            failures_to_cap(&backoff, 1, t0),
            1,
            "the cleared episode alerts again on the next outage"
        );
    }

    // -----------------------------------------------------------------------
    // The wiring in front of the spawn: which trigger consults the backoff,
    // and what a finished attempt does to it.
    // -----------------------------------------------------------------------

    /// The conversations whose spawn attempts got through, in order.
    fn admitted(state: &AppState) -> Vec<i64> {
        state
            .wake_spawns
            .lock()
            .unwrap()
            .iter()
            .map(|(conversation_id, _)| *conversation_id)
            .collect()
    }

    /// The backoff bounds the bus side and only the bus side: an armed
    /// conversation declines the wake walk's every trigger, while a browser
    /// attach still gets its spawn — a human retry is self-pacing and is the
    /// likeliest probe to find a broken spawn working again.
    #[tokio::test]
    async fn an_armed_backoff_declines_the_bus_trigger_and_not_the_user_one() {
        let state = AppState::for_test(crate::test_support::init_db_memory(), None);
        let conv = 11_i64;

        state.spawn_bus_wake(conv, chrono_tz::Tz::UTC);
        assert_eq!(
            admitted(&state),
            vec![conv],
            "an unarmed conversation's bus wake reaches the spawn"
        );

        state.spawn_backoff.record_failure(conv);
        state.spawn_bus_wake(conv, chrono_tz::Tz::UTC);
        assert_eq!(
            admitted(&state),
            vec![conv],
            "the armed window declines the bus trigger"
        );
        state.spawn_eager_wake(conv, chrono_tz::Tz::UTC);
        assert_eq!(
            admitted(&state),
            vec![conv, conv],
            "the user trigger is never declined"
        );

        state.spawn_backoff.clear(conv);
        state.spawn_bus_wake(conv, chrono_tz::Tz::UTC);
        assert_eq!(
            admitted(&state),
            vec![conv, conv, conv],
            "the cleared episode admits the bus side again"
        );
    }

    /// The chat entrypoint shares the backoff with the rest of the bus side and
    /// differs from it in exactly one thing: what it leaves holding the bridge.
    #[tokio::test]
    async fn only_the_chat_entrypoint_asks_for_the_bus_hold() {
        let state = AppState::for_test(crate::test_support::init_db_memory(), None);
        let conv = 11_i64;

        state.spawn_chat_wake(conv, chrono_tz::Tz::UTC);
        state.spawn_bus_wake(conv, chrono_tz::Tz::UTC);
        state.spawn_eager_wake(conv, chrono_tz::Tz::UTC);
        assert_eq!(
            *state.wake_spawns.lock().unwrap(),
            vec![
                (conv, BusHold::Held),
                (conv, BusHold::Unheld),
                (conv, BusHold::Unheld),
            ],
            "a peer asking for a conversation by name is the one trigger that holds it",
        );

        state.spawn_backoff.record_failure(conv);
        state.spawn_chat_wake(conv, chrono_tz::Tz::UTC);
        assert_eq!(
            admitted(&state).len(),
            3,
            "and it is declined by the armed backoff like every other bus trigger",
        );
    }

    /// A failed attempt arms the backoff and a successful one clears it —
    /// the two halves of the episode, reached through the one function the
    /// spawn task hands its outcome to.
    #[tokio::test]
    async fn a_spawn_outcome_arms_and_clears_the_backoff() {
        let state = AppState::for_test(crate::test_support::init_db_memory(), None);
        let conv = 12_i64;

        state.record_spawn_outcome(conv, Err("no bridge".to_string()));
        assert!(
            state.spawn_backoff.declines(conv),
            "a failed attempt opens the window"
        );

        state.record_spawn_outcome(conv, Ok(()));
        assert!(
            !state.spawn_backoff.declines(conv),
            "a successful attempt ends the episode"
        );
    }

    /// Reaching the cap raises exactly one `Warning` through the
    /// `AlertDispatcher`: a conversation that cannot spawn is a conversation
    /// silently not processing, and that is the operator's only signal.
    #[tokio::test]
    async fn the_cap_alerts_once_through_the_alert_dispatcher() {
        use brenn_obs::alerting::{AlertSeverity, make_capturing_alerter_with_severity};

        let (alert_dispatcher, captured, handle) = make_capturing_alerter_with_severity();
        let mut state = AppState::for_test(crate::test_support::init_db_memory(), None);
        state.alert_dispatcher = alert_dispatcher;
        let conv = 13_i64;

        for _ in 0..20 {
            state.record_spawn_outcome(conv, Err("no bridge".to_string()));
        }
        drop(state);
        let _ = handle.await;

        let alerts = captured.lock().unwrap();
        assert_eq!(
            alerts.len(),
            1,
            "one alert per failure episode, not per failure: {alerts:?}"
        );
        assert!(matches!(alerts[0].0, AlertSeverity::Warning));
    }
}

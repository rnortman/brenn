//! The ActiveBridge struct (per-conversation CC subprocess wrapper) + constructor + small accessors that don't fit elsewhere.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::time::{Duration, Instant};

use brenn_approval_rules::ApprovalRuleSet;
use brenn_cc::session::{CcSession, SessionEvent};
use brenn_db::Db;
use brenn_db::conversation;
use brenn_lib::app::AppTool;
use brenn_lib::config::PathMapper;
use brenn_obs::alerting::AlertDispatcher;
use brenn_obs::transcript::TranscriptWriter;
use brenn_ws_types::{ViewportClass, WsServerMessage};
#[cfg(test)]
use tokio::sync::watch;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use super::cc_event_loop::cc_event_loop;
use super::cc_spawn_config::build_cc_session_config;
use super::compaction::{CompactionState, ContextUsage};
use super::lifecycle::Subscriber;
use super::mcp_constants::*;
use super::permission_sync::PendingPermission;
use super::registry::ActiveBridges;
use super::tool_summary::{ApprovalOutcome, PendingToolUse};

/// A live CC session shared across WS connections.
///
/// Created when a CC subprocess is spawned. The event loop runs as a detached
/// task that broadcasts events to all subscribers. WS handlers interact via
/// `send_message()`, `handle_permission_response()`, `handle_tool_card_response()`,
/// and `subscribe()`.
pub struct ActiveBridge {
    /// The CC session handle. Behind mutex for send coordination.
    pub(super) session: tokio::sync::Mutex<Option<CcSession>>,
    /// Broadcast channel for WS events. All attached tabs subscribe.
    pub(super) event_tx: broadcast::Sender<WsServerMessage>,
    /// Alert/security-event dispatcher. Cloned from `SpawnContext.alert_dispatcher`
    /// at construction. Used by `&self` handlers (e.g. `handle_async_tool_response`,
    /// `handle_permission_response`) to emit `SchemaViolation` security signals for
    /// browser-supplied request_ids that fail the ownership check or match nothing.
    pub(super) alert_dispatcher: AlertDispatcher,
    /// Pending synchronous permission approvals (Bash, Edit, etc.).
    /// Interactive tool requests live in the DB, not here.
    pub(super) pending_permissions: tokio::sync::Mutex<HashMap<String, PendingPermission>>,
    /// Conversation ID in the DB.
    pub conversation_id: i64,
    /// User who owns this conversation.
    pub user_id: i64,
    /// App this bridge belongs to. Immutable after creation.
    pub app_slug: String,
    /// App's working directory (host-side). Used for stable file URL computation.
    pub working_dir: PathBuf,
    /// Path mapper for translating between host and CC-visible paths.
    pub path_mapper: PathMapper,
    /// Whether this conversation is shared (multiuser). Mirrors the DB `shared` column.
    /// `AtomicBool` so it can be updated via `set_shared()` without `&mut self`.
    pub shared: AtomicBool,
    /// Database handle.
    pub(super) db: Db,
    /// Users currently subscribed to this bridge (for presence indicators).
    /// Keyed by user_id, ref-counted for multi-tab.
    pub(super) subscribers: tokio::sync::RwLock<HashMap<i64, Subscriber>>,
    /// When set, the bridge should be killed when CC becomes idle (between turns).
    /// Set by `maybe_drain` when the subscriber map empties (under subscribers lock).
    /// Cleared by `add_subscriber` (reconnection cancels drain, also under lock).
    pub(super) drain_on_idle: AtomicBool,
    /// Whether CC is idle (between turns). Set to `true` by `handle_turn_completed`,
    /// set to `false` by `send_message` (user sends a new message → CC starts working).
    /// Used by `maybe_drain` to decide kill-now vs wait-for-turn.
    pub(super) cc_idle: AtomicBool,
    /// The doors that can hold this bridge open — the browser, and the bus where
    /// the deployment has one — and what each has last seen. Every keep-alive
    /// decision is the OR over them; see `lifetime.rs`.
    pub(in crate::active_bridge) lifetime: super::lifetime::LifetimeArbiter,
    /// Cancel handle for the timer that re-asks the lifetime question when the
    /// soonest hold is due to expire. `std::sync::Mutex` because
    /// `JoinHandle::abort()` is sync.
    pub(super) lifetime_timer: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// When the CC process this bridge currently holds was spawned. Used to
    /// measure init latency and diagnose stuck spawns. `std::sync::Mutex`
    /// because a profile swap replaces the process and the figure must measure
    /// *that* process, not the bridge.
    pub(super) spawn_instant: std::sync::Mutex<Instant>,
    /// Reference to the global bridge registry, needed for self-kill on drain.
    pub(super) active_bridges: ActiveBridges,
    /// Per-tool extension implementations, keyed by tool name. Used for custom
    /// formatting, auto-approve, and display.
    pub(super) tool_registry: Arc<HashMap<String, Arc<dyn AppTool>>>,
    /// First-class tool registry (grant-governed tools). The `registry_adapter`
    /// intercept routes CC's `mcp__brenn__*` calls that resolve here through it.
    pub(super) tools: Arc<brenn_tool_registry::ToolRegistry>,
    /// This app's resolved tool grants (from `AppPolicy.tool_grants`). The
    /// authorization side of a registry tool call; joined with the descriptor's
    /// `auto_approve` in the adapter.
    pub(super) tool_grants: std::collections::BTreeMap<String, brenn_lib::tools::ResolvedToolGrant>,
    /// Origin string for this app's tool-caller `ParticipantId`
    /// (`app:<slug>@<origin>`).
    pub(super) server_origin: Arc<str>,
    /// Pattern-based auto-approval rules (global, static config, DB).
    pub(super) approval_rules: ApprovalRuleSet,
    /// Tracks how each tool invocation was approved, keyed by tool_use_id.
    /// Inserted by the Permission handler (auto-approve or manual), consumed by
    /// the ToolResult handler for enrichment (showing which rule approved).
    /// Optional — CC-internal auto-approvals won't have an entry here.
    pub(super) approval_outcomes: tokio::sync::Mutex<HashMap<String, ApprovalOutcome>>,
    /// Pending tool invocations, populated from assistant message `tool_use`
    /// content blocks. Consumed by the ToolResult handler to emit summaries.
    /// Missing from both this map AND `handled_tool_uses` = protocol violation + alert.
    pub(super) pending_tool_uses: tokio::sync::Mutex<HashMap<String, PendingToolUse>>,
    /// Tool use IDs that were already handled by a specialized path (e.g., noop
    /// tools emit summaries from PostToolUse). The ToolResult handler checks this
    /// to avoid duplicate summaries and false alerts.
    pub(super) handled_tool_uses: tokio::sync::Mutex<HashSet<String>>,
    /// Last model sent via `set_model`, to avoid redundant calls.
    pub(super) last_set_model: tokio::sync::Mutex<Option<String>>,
    /// Per-app integration instances, cloned from `app_config.integrations` at
    /// construction. Used by `pfin_config()` and similar typed-config accessors.
    pub(super) integrations:
        HashMap<String, std::sync::Arc<dyn brenn_lib::integration::Integration>>,
    /// Container spawn config for this app, if containerized. Needed to run
    /// pfin reconcile inside the container rather than on the host.
    pub(super) container_spawn: Option<brenn_lib::config::ContainerSpawnConfig>,
    /// Resolved repo mounts for this app. Used by GitRepo* virtual tools
    /// and `DirtyRepoHook` (via `mounts()`).
    pub(crate) mounts: Vec<brenn_lib::config::ResolvedMount>,
    /// Idle-hook delay in seconds. `0` disables idle hooks. Mirrors the
    /// resolved `app_config.idle_hook_secs`. Carried on the bridge so
    /// `register_idle_hook` and `maybe_arm_idle_hook_timer` don't have to
    /// thread the `AppConfig` through.
    pub(super) idle_hook_secs: u64,
    /// Registered idle hooks. Sync mutex because all touches are short
    /// non-async pushes / clones; we never hold the guard across an
    /// `.await`.
    pub(super) idle_hooks: std::sync::Mutex<Vec<Arc<dyn crate::idle_hooks::IdleHook>>>,
    /// Cancel handle for the shared idle-hook timer. `std::sync::Mutex`
    /// because `JoinHandle::abort()` is sync — same pattern as
    /// `lifetime_timer`.
    pub(super) idle_hook_timer: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Per-app frontmatter rendering rules. Read by the DisplayFile
    /// PreToolUse intercept when rendering markdown files.
    pub(super) frontmatter: brenn_lib::config::FrontmatterRenderConfig,
    /// Allowed users for this app. Empty means open to all users.
    /// Used by device tools to compute the app-visibility set.
    pub(super) allowed_users: Vec<String>,
    /// Last reported viewport class from any connected client. Used for
    /// rendering viewport-appropriate HTML in approval dialogs (e.g., batch
    /// reconcile table vs swipe view). Defaults to Wide. Updated by WS
    /// connections via `set_viewport_class()`.
    pub(super) viewport_class: std::sync::Mutex<ViewportClass>,
    /// Whether this app is singleton (one conversation per user, context grows
    /// without bound). Only singleton apps support compaction.
    pub(in crate::active_bridge) singleton: bool,
    /// LLM-initiated compaction state (phase + compaction-related flags).
    pub(in crate::active_bridge) compaction: tokio::sync::Mutex<CompactionState>,
    /// Compaction config (thresholds). `None` when compaction is not configured
    /// (non-singleton apps).
    pub(super) compaction_config: Option<brenn_lib::config::CompactionConfig>,
    /// Most recent context fill derived from the NDJSON stream. `std::sync::Mutex`
    /// because it's only accessed for quick reads/writes, never held across `.await`.
    pub(super) context_usage: std::sync::Mutex<Option<ContextUsage>>,
    /// Max context-window size used as denominator for `ContextUsage` broadcasts
    /// before the authoritative value arrives from `result.modelUsage.contextWindow`.
    /// Set at `handle_initialized` from `model_window_cache` (None on cache miss).
    /// Nulled by `update_context_from_assistant` on a genuine mid-session model
    /// switch, then re-populated by the caller from a cache lookup.
    /// Not written by `update_max_tokens_from_result_sync` — that function updates
    /// `context_usage.max_tokens` directly; subsequent broadcasts within the same
    /// session use that as the denominator, not this seed.
    pub(super) seed_max_tokens: std::sync::Mutex<Option<u64>>,
    /// Cumulative cost as of the last persisted turn. Used to compute per-turn
    /// delta. Loaded from `conversations.total_cost_usd` at bridge construction
    /// and updated after each turn before broadcasting.
    pub(super) last_total_cost_usd: std::sync::Mutex<Option<f64>>,
    /// Last top-level assistant model slug. Used to pick the right key out of
    /// `result.modelUsage`. `None` until the first non-subagent assistant message.
    pub(super) active_model_slug: std::sync::Mutex<Option<String>>,
    /// CC version captured at `handle_initialized`. Used by the version-floor
    /// check and by the `model_window_cache` upsert.
    pub(super) cc_version: std::sync::Mutex<Option<String>>,
    /// Process-wide shutdown flag (cloned from `AppState::server_shutting_down`).
    /// Checked by the `SessionEvent::Died` handler alongside `drain_on_idle` to
    /// suppress the "CC session died" Warning alert during intentional server
    /// teardown (SIGTERM from systemctl, etc.). Distinct from `drain_on_idle`
    /// (per-conversation drain) and `CcSession::shutting_down` (gates the
    /// reader-task Critical alert). All three signal "this death was expected";
    /// we just need to check all of them.
    pub(super) server_shutting_down: Arc<AtomicBool>,
    /// Repo-sync trigger sender. `None` when no sync-enabled clones are
    /// configured. Used by the `GitRepoPull` and `GitRepoCommitAndPush`
    /// PostToolUse handlers to emit a `Push` trigger immediately after a
    /// successful tool invocation, so sibling clones of the same remote
    /// see the advance within seconds instead of waiting for the next
    /// poll interval.
    pub(super) repo_sync_sender: Option<brenn_git::sync::SyncTriggerSender>,
    /// Messenger for the messaging MVP. `None` when no `[[channel]]`
    /// blocks are configured (messaging effectively disabled).
    pub(crate) messenger: Option<Arc<brenn_messaging::Messenger>>,
    /// Serializes this conversation's bus delivery: read the position, render,
    /// send, advance. Three callers reach that sequence — the startup drain, the
    /// dispatcher fan-out, and the wake walk — and the read is pure, so two of
    /// them running at once would each read the same unseen suffix and each send
    /// it. Held across the whole sequence, so the second caller reads a position
    /// that has already moved and sends nothing.
    pub(in crate::active_bridge) bus_delivery: tokio::sync::Mutex<()>,
    /// Rung when this conversation's chat command channel may have something
    /// new. The chat adapter waits on it; permits coalesce, so a burst of
    /// commands costs one drain that serves all of them.
    pub(crate) chat_commands: Arc<tokio::sync::Notify>,
    /// Rung when the chat adapter should stop. Nothing else can end it: it holds
    /// an `Arc` on this bridge and reads a broadcast this bridge owns the only
    /// sender for, so the channel it waits on cannot close while it is waiting.
    /// A permit is stored, so signalling before the adapter reaches its wait is
    /// not a race.
    pub(in crate::active_bridge) chat_shutdown: Arc<tokio::sync::Notify>,
    /// PWA push service. `None` when no app has `pwa_push.enabled = true`.
    pub(super) pwa_push_service: Option<Arc<dyn brenn_pwa_push::PwaPushSender>>,
    /// MQTT service. `None` when no `[[mqtt_client]]` is configured.
    pub(super) mqtt_service: Option<Arc<brenn_mqtt::MqttService>>,
    /// MQTT inbound event router (concrete type). `None` when MQTT is not
    /// configured. Held so a runtime `mqtt:` dynamic subscribe can add an
    /// `IngressRoute` via `add_route`; the `Arc<dyn MqttEventRouter>` clones
    /// the supervisors hold expose only `deliver_inbound`, so the concrete
    /// handle must be threaded here to reach `add_route`.
    pub(super) mqtt_event_router: Option<Arc<crate::mqtt_router::MqttEventRouterImpl>>,
    /// Automation engine. `None` when the automation subsystem is not configured
    /// (no messenger, or no apps with allowed_users).
    pub(super) automation_engine: Option<Arc<brenn_automation::AutomationEngine>>,
    /// Usage session inactivity gap in seconds. Mirrors
    /// `AppState::usage_session_gap_secs`; carried here so `handle_turn_completed`
    /// can record usage without threading the AppState through.
    pub(super) usage_session_gap_secs: u32,
    /// Per-app messaging send budget (or the global default when the app has no
    /// `[app.messaging]` block). Used by MQTT send to check/decrement the shared
    /// budget without threading `AppState` through the intercept.
    pub(super) messaging_default_send_budget: u32,
    /// Epoch second when `cost_samples::prune_before` last ran. Zero = never.
    /// Pruning is at most once per hour; the DELETE is a no-op ~99.96% of the
    /// time without this gate.
    pub(in crate::active_bridge) last_cost_prune_at: AtomicI64,
    /// Last lint-error set injected to CC. Resets to `None` on bridge creation
    /// so each new CC session gets fresh context. `None` means no inject has
    /// occurred yet.
    ///
    /// `std::sync::Mutex` is correct here: the critical section in
    /// `maybe_inject_lint_errors` performs no `.await` while the lock is held
    /// (it reads, compares, and drops before the async send).
    pub(crate) last_lint_snapshot: std::sync::Mutex<Option<Vec<brenn_ws_types::TodoLintError>>>,
    /// The CC event-loop task handle, stored so the wedge watchdog can detect a
    /// dead event loop via `is_finished()`. `std::sync::Mutex` because it is set
    /// once after the loop is spawned and only peeked thereafter (no `.await`
    /// while held). `None` until `install_event_loop_handle` runs, and in test
    /// bridges that never spawn a loop.
    pub(in crate::active_bridge) event_loop_handle:
        std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The chat bus adapter's task handle, stored for the same reason as
    /// `event_loop_handle`: the adapter panics rather than write a truncated
    /// record, and a detached task's panic unwinds nothing but itself. While
    /// this bridge lives it holds a sender on the broadcast the adapter reads,
    /// so a finished adapter means it died — which the watchdog treats as a
    /// wedge. `None` where the deployment has no bus, and in test bridges.
    pub(in crate::active_bridge) chat_adapter_handle:
        std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The Claude profile name the current CC process was spawned under.
    /// `None` for an app that declared no `claude_profiles`. In memory only:
    /// cost attribution needs the profile per *turn*, which is a different
    /// grain than a per-conversation column could hold.
    pub(in crate::active_bridge) cc_profile: std::sync::Mutex<Option<String>>,
    /// Which profile each profiled app should run under. `None` when no agent
    /// declared `claude_profiles`. Read to decide whether this bridge is stale.
    pub(in crate::active_bridge) cc_profiles: Option<Arc<brenn_cc_profile::ProfileGoal>>,
    /// How a swap reaches the world: the replacement spawn and the model cache.
    /// `None` on test bridges that never spawned a process.
    pub(in crate::active_bridge) swap_host: Option<Arc<dyn super::profile_swap::ProfileSwapHost>>,
    /// Set for the window in which a profile swap has torn the old CC process
    /// down and not yet installed its replacement. Keeps a second swap and the
    /// wedge watchdog off a bridge that is mid-swap; it says nothing about which
    /// death belongs to the swap, which is `swap_ack`'s job.
    pub(in crate::active_bridge) swapping: AtomicBool,
    /// A swap's claim on the death of the process it is retiring: installed
    /// before the teardown, taken and rung by the event loop's `Died` arm. One
    /// death consumes it, so a replacement that dies a moment later is an
    /// ordinary death and no swap can be woken by a stale signal.
    pub(in crate::active_bridge) swap_ack:
        std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// The sender half of the CC event channel this bridge's event loop reads.
    /// A swap hands it to the replacement process so the existing loop stays the
    /// consumer, and a failed swap sends a synthetic `Died` down it.
    pub(in crate::active_bridge) cc_event_tx: mpsc::Sender<SessionEvent>,
    /// Set once the clean-slate death reset has run for this bridge (from the
    /// event loop's `Died` handler or the watchdog). Lets the watchdog tell a
    /// loop that ended after cleanly processing session death from one that died
    /// with cleanup never run, and makes each wedge fire its reset exactly once.
    pub(in crate::active_bridge) died_handled: AtomicBool,
    /// Test-only synchronization epoch. Incremented by `cc_event_loop` at three
    /// signal sites: after startup drain, after each event match arm, and after
    /// post-loop teardown. Receivers exist only in test helpers (`event_fence`).
    #[cfg(test)]
    pub(in crate::active_bridge) event_loop_epoch: watch::Sender<u64>,
}

/// Context bundle for [`ActiveBridge::spawn_new`].
///
/// Bundles all spawn parameters into one named struct so call sites remain readable.
/// Caller must not hold the `active_bridges` write lock when calling `spawn_new`.
pub struct SpawnContext<'a> {
    pub user_id: i64,
    pub conversation_id: i64,
    pub shared: bool,
    pub db: Db,
    pub alert_dispatcher: AlertDispatcher,
    pub active_bridges: ActiveBridges,
    pub resume_session_id: Option<String>,
    pub log_dir: &'a Path,
    pub mcp_script_path: &'a Path,
    pub app_config: &'a brenn_lib::config::AppConfig,
    pub model_override: Option<&'a str>,
    pub tool_registry: Arc<HashMap<String, Arc<dyn AppTool>>>,
    /// First-class tool registry, for the LLM tool-call adapter.
    pub tools: Arc<brenn_tool_registry::ToolRegistry>,
    /// Origin string for this app's tool-caller `ParticipantId`.
    pub server_origin: Arc<str>,
    pub server_shutting_down: Arc<AtomicBool>,
    /// Spawning context's IANA timezone (WS connection's browser-reported zone, or
    /// UTC fallback for autonomous wakes). Seeds `GRAF_USER_TZ` in CC's environment
    /// for graf MCP tool calls. See `docs/designs/graf-user-tz.md`.
    pub user_tz: chrono_tz::Tz,
    pub repo_sync_sender: Option<brenn_git::sync::SyncTriggerSender>,
    pub messenger: Option<Arc<brenn_messaging::Messenger>>,
    pub pwa_push_service: Option<Arc<dyn brenn_pwa_push::PwaPushSender>>,
    pub mqtt_service: Option<Arc<brenn_mqtt::MqttService>>,
    /// Concrete MQTT event router handle, for runtime `add_route`.
    pub mqtt_event_router: Option<Arc<crate::mqtt_router::MqttEventRouterImpl>>,
    pub automation_engine: Option<Arc<brenn_automation::AutomationEngine>>,
    pub usage_session_gap_secs: u32,
    /// Which profile each profiled app should run under. The account this
    /// spawn runs under is this handle resolved against `app_config.slug`, and
    /// a live bridge compares against it to tell it is on the wrong one.
    pub cc_profiles: Option<Arc<brenn_cc_profile::ProfileGoal>>,
    /// The server-owned half of the swap host; `spawn_new` completes it with
    /// this bridge's transcript writer and alert dispatcher.
    pub swap_host_seed: super::profile_swap::SwapHostSeed,
}

/// The model a spawn runs, paired with the `last_set_model` seed it justifies.
///
/// The model is the caller's override, or the app's configured default. A
/// fresh spawn's `--model` flag definitely put CC on that model, so it is
/// recorded as the last asserted one and the first send dedups away. A resumed
/// session is deliberately left unseeded: whether `--resume` restores the
/// session's own model is unverified, so the first send's unconditional
/// `set_model` stays as the re-assertion.
fn spawn_model_and_seed(
    model_override: Option<&str>,
    app_model: &str,
    resuming: bool,
) -> (String, Option<String>) {
    let model = model_override
        .map(String::from)
        .unwrap_or_else(|| app_model.to_string());
    let seed = (!resuming).then(|| model.clone());
    (model, seed)
}

impl ActiveBridge {
    /// Spawn a new CC session and start the event loop task.
    ///
    /// Returns `(bridge, initial_rx)` — the bridge wrapped in an Arc and a
    /// pre-created broadcast receiver. The receiver was created before the event
    /// loop started, so it captures every broadcast from the beginning. The caller
    /// must use this receiver (not `bridge.subscribe()`) to avoid missing events.
    /// The caller is also responsible for registering the bridge in `ActiveBridges`.
    #[cfg_attr(test, allow(dead_code))]
    pub async fn spawn_new(
        ctx: SpawnContext<'_>,
    ) -> Result<
        (
            Arc<Self>,
            broadcast::Receiver<WsServerMessage>,
            Vec<String>,
            Vec<brenn_cc::session::ModelOption>,
        ),
        String,
    > {
        let SpawnContext {
            user_id,
            conversation_id,
            shared,
            db,
            alert_dispatcher,
            active_bridges,
            resume_session_id,
            log_dir,
            mcp_script_path,
            app_config,
            model_override,
            tool_registry,
            tools,
            server_origin,
            server_shutting_down,
            user_tz,
            repo_sync_sender,
            messenger,
            pwa_push_service,
            mqtt_service,
            mqtt_event_router,
            automation_engine,
            usage_session_gap_secs,
            cc_profiles,
            swap_host_seed,
        } = ctx;

        // Create event channels.
        let (cc_event_tx, cc_event_rx) = mpsc::channel(256);
        // Keep the initial receiver — it must exist before the event loop starts
        // so the spawning connection doesn't miss any broadcasts. The chat bus
        // adapter's receiver is taken here for the same reason: the conversation
        // record must start at the conversation's first event.
        let (broadcast_tx, initial_rx) = broadcast::channel(512);
        let bus_chat_rx = broadcast_tx.subscribe();

        // Derived, not carried: one expression, so no second spawn path can
        // pass a profile that disagrees with the goal handle beside it.
        let cc_profile = cc_profiles
            .as_ref()
            .and_then(|g| g.resolve(&app_config.slug));
        let container_name_suffix =
            super::cc_spawn_config::conversation_container_suffix(conversation_id);
        let transcript_name = format!("cc-{}-{container_name_suffix}.ndjson", app_config.slug);
        let mut transcript_writer = TranscriptWriter::new(log_dir, &transcript_name)
            .map_err(|e| format!("failed to create transcript writer: {e}"))?;
        transcript_writer
            .register_sighup_reopen()
            .map_err(|e| format!("failed to register transcript SIGHUP handler: {e}"))?;
        let transcript = Arc::new(transcript_writer);

        // Run start hooks for new conversations (not resume).
        let hook_warnings = if resume_session_id.is_none() {
            let hook_result = brenn_hooks::run_start_hooks(app_config, conversation_id).await?;
            hook_result.warnings
        } else {
            Vec::new()
        };

        let (model, spawn_model) = spawn_model_and_seed(
            model_override,
            &app_config.model,
            resume_session_id.is_some(),
        );

        let config = build_cc_session_config(super::cc_spawn_config::CcSpawnInputs {
            app_config,
            mcp_script_path,
            model,
            container_name_suffix,
            resume_session_id,
            transcript: transcript.clone(),
            alert_dispatcher: alert_dispatcher.clone(),
            user_tz,
            server_shutting_down: server_shutting_down.clone(),
            cc_profile: cc_profile.clone(),
        });

        let swap_shutdown = server_shutting_down.clone();
        let swap_host_profiled = cc_profile.is_some();
        let spawn_instant = Instant::now();
        info!(
            conversation_id,
            app_slug = %app_config.slug,
            "spawning CC session",
        );

        let (session, init_ack_info) = CcSession::spawn(config, cc_event_tx.clone())
            .await
            .map_err(|e| format!("CC spawn failed: {e}"))?;

        info!(
            conversation_id,
            app_slug = %app_config.slug,
            "CC spawn complete, awaiting init"
        );

        // Build approval rule set: global tools + tool registry auto-approves + config + DB rules.
        let global_extra: Vec<&str> = {
            // Belt-and-suspenders: PreToolUse already grants permission via Allow
            // for brenn noop tools, so Permission shouldn't fire. But defend in depth.
            let mut extra = GLOBAL_EXTRA_STATIC_BASE.to_vec();
            for tool in tool_registry.values() {
                if tool.auto_approve() {
                    extra.push(tool.name());
                }
            }
            extra
        };
        let db_rules = {
            let conn = db.lock().await;
            brenn_db::load_approval_rules(&conn, &app_config.slug, conversation_id)
        };
        let db_rule_tuples: Vec<(String, String)> = db_rules
            .iter()
            .map(|r| (r.tool_name.clone(), r.pattern.clone()))
            .collect();
        let approval_rules =
            ApprovalRuleSet::new(&global_extra, &app_config.approval_rules, db_rule_tuples);

        // Seed last_total_cost_usd from the existing conversation row so that
        // the first turn delta is correct even after a bridge restart.
        let initial_cost = {
            let conn = db.lock().await;
            brenn_db::conversation::get_total_cost_usd(&conn, conversation_id)
        };

        #[cfg(test)]
        let epoch_tx = {
            let (tx, _rx) = watch::channel(0u64);
            tx
        };
        // A deployment with no bus has no bus door: nothing can drive this
        // conversation over the wire, so there is no hold for it to take.
        let bus_idle_timeout = messenger
            .as_ref()
            .map(|m| Duration::from_secs(m.llm_chat().idle_timeout_secs));
        let bridge = Arc::new(Self {
            session: tokio::sync::Mutex::new(Some(session)),
            event_tx: broadcast_tx.clone(),
            // Clone for the struct field; the original is moved into
            // `cc_event_loop` below.
            alert_dispatcher: alert_dispatcher.clone(),
            pending_permissions: tokio::sync::Mutex::new(HashMap::new()),
            conversation_id,
            user_id,
            app_slug: app_config.slug.clone(),
            working_dir: app_config.working_dir.clone(),
            path_mapper: app_config.path_mapper.clone(),
            shared: AtomicBool::new(shared),
            db: db.clone(),
            subscribers: tokio::sync::RwLock::new(HashMap::new()),
            drain_on_idle: AtomicBool::new(false),
            // CC has completed its handshake (control_response received) and is
            // waiting for input. No turn is in progress. The system/init message
            // is informational metadata, not a readiness signal — see
            // docs/designs/init-not-required.md.
            cc_idle: AtomicBool::new(true),
            lifetime: super::lifetime::LifetimeArbiter::new(
                app_config.idle_timeout,
                bus_idle_timeout,
            ),
            lifetime_timer: std::sync::Mutex::new(None),
            spawn_instant: std::sync::Mutex::new(spawn_instant),
            active_bridges: active_bridges.clone(),
            tool_registry,
            tools,
            tool_grants: app_config.policy.tool_grants.clone(),
            server_origin,
            approval_rules,
            approval_outcomes: tokio::sync::Mutex::new(HashMap::new()),
            pending_tool_uses: tokio::sync::Mutex::new(HashMap::new()),
            handled_tool_uses: tokio::sync::Mutex::new(HashSet::new()),
            last_set_model: tokio::sync::Mutex::new(spawn_model),
            integrations: app_config.integrations.clone(),
            container_spawn: app_config.container_spawn.clone(),
            mounts: app_config.mounts.clone(),
            idle_hook_secs: app_config.idle_hook_secs,
            idle_hooks: std::sync::Mutex::new(Vec::new()),
            idle_hook_timer: std::sync::Mutex::new(None),
            frontmatter: app_config.frontmatter.clone(),
            allowed_users: app_config.allowed_users.clone(),
            viewport_class: std::sync::Mutex::new(ViewportClass::Wide),
            singleton: app_config.singleton,
            compaction: tokio::sync::Mutex::new(CompactionState::default()),
            compaction_config: app_config.compaction.clone(),
            context_usage: std::sync::Mutex::new(None),
            seed_max_tokens: std::sync::Mutex::new(None),
            last_total_cost_usd: std::sync::Mutex::new(initial_cost),
            active_model_slug: std::sync::Mutex::new(None),
            cc_version: std::sync::Mutex::new(None),
            server_shutting_down,
            repo_sync_sender,
            messenger,
            bus_delivery: tokio::sync::Mutex::new(()),
            chat_commands: Arc::new(tokio::sync::Notify::new()),
            chat_shutdown: Arc::new(tokio::sync::Notify::new()),
            pwa_push_service,
            mqtt_service,
            mqtt_event_router,
            automation_engine,
            usage_session_gap_secs,
            messaging_default_send_budget: app_config.messaging_send_budget(),
            last_cost_prune_at: AtomicI64::new(0),
            last_lint_snapshot: std::sync::Mutex::new(None),
            event_loop_handle: std::sync::Mutex::new(None),
            chat_adapter_handle: std::sync::Mutex::new(None),
            died_handled: AtomicBool::new(false),
            cc_profile: std::sync::Mutex::new(cc_profile.map(|p| p.name)),
            cc_profiles,
            // Only a bridge that can go stale gets one. An app with no
            // `claude_profiles` never resolves a profile, so it can never swap.
            swap_host: swap_host_profiled.then(|| {
                Arc::new(super::profile_swap::ServerSwapHost {
                    models: swap_host_seed.models,
                    app_config: app_config.clone(),
                    mcp_script_path: mcp_script_path.to_path_buf(),
                    transcript,
                    alert_dispatcher: alert_dispatcher.clone(),
                    user_tz,
                    server_shutting_down: swap_shutdown,
                }) as Arc<dyn super::profile_swap::ProfileSwapHost>
            }),
            swapping: AtomicBool::new(false),
            swap_ack: std::sync::Mutex::new(None),
            cc_event_tx,
            #[cfg(test)]
            event_loop_epoch: epoch_tx,
        });

        // Register idle hooks. `DirtyRepoHook` is registered when the app
        // declares any `[[app.repo]]` / `[[app.mount]]` entries — clones
        // are the only thing it knows how to nudge about. See
        // `docs/designs/idle-hooks.md` § "When no repos are declared".
        if !bridge.mounts.is_empty() {
            bridge.register_idle_hook(Arc::new(crate::idle_hooks::DirtyRepoHook::new()));
        }

        // Start the outbound bus leg before the event loop, so nothing it would
        // have published can be broadcast first.
        super::bus_chat::spawn_bus_chat_adapter(
            bridge.clone(),
            bus_chat_rx,
            init_ack_info.models.clone(),
        );

        // Spawn the detached event loop task and retain its handle so the wedge
        // watchdog can detect a dead loop.
        let bridge_for_loop = bridge.clone();
        let loop_handle = tokio::spawn(cc_event_loop(
            cc_event_rx,
            bridge_for_loop,
            alert_dispatcher,
        ));
        bridge.install_event_loop_handle(loop_handle);

        Ok((bridge, initial_rx, hook_warnings, init_ack_info.models))
    }

    /// Store the CC event-loop task handle so the wedge watchdog can observe
    /// whether the loop is still running. Called once, right after the loop is
    /// spawned; also used by watchdog tests to install a handle on an injected
    /// bridge.
    pub(in crate::active_bridge) fn install_event_loop_handle(
        &self,
        handle: tokio::task::JoinHandle<()>,
    ) {
        *self
            .event_loop_handle
            .lock()
            .expect("event_loop_handle lock poisoned") = Some(handle);
    }

    /// Whether the stored event-loop handle reports the loop has finished.
    /// `false` when no handle is installed (nothing to supervise yet).
    pub(in crate::active_bridge) fn event_loop_finished(&self) -> bool {
        self.event_loop_handle
            .lock()
            .expect("event_loop_handle lock poisoned")
            .as_ref()
            .is_some_and(|h| h.is_finished())
    }

    /// Store the chat bus adapter's task handle so the wedge watchdog can
    /// observe whether the adapter is still running.
    pub(in crate::active_bridge) fn install_chat_adapter_handle(
        &self,
        handle: tokio::task::JoinHandle<()>,
    ) {
        *self
            .chat_adapter_handle
            .lock()
            .expect("chat_adapter_handle lock poisoned") = Some(handle);
    }

    /// Whether the stored chat-adapter handle reports the adapter has finished.
    /// `false` when no handle is installed — a deployment without a bus runs no
    /// adapter, and there is nothing to supervise.
    pub(in crate::active_bridge) fn chat_adapter_finished(&self) -> bool {
        self.chat_adapter_handle
            .lock()
            .expect("chat_adapter_handle lock poisoned")
            .as_ref()
            .is_some_and(|h| h.is_finished())
    }

    /// Tell the chat bus adapter to finish.
    ///
    /// Every path that takes a bridge out of the registry calls this. The
    /// adapter holds an `Arc` on the bridge and waits on a broadcast whose only
    /// sender the bridge owns, so a deregistered bridge with a running adapter is
    /// a cycle: the task, the broadcast buffer, and everything else the bridge
    /// holds would live as long as the process, once per bridge ever torn down.
    ///
    /// The adapter publishes whatever the broadcast is already holding before it
    /// goes, so events produced before the teardown still reach the record;
    /// events produced after it do not, which is the honest bound when the
    /// bridge itself is gone.
    pub(in crate::active_bridge) fn stop_chat_adapter(&self) {
        self.chat_shutdown.notify_one();
    }

    /// Whether the clean-slate death reset has already run for this bridge.
    pub(in crate::active_bridge) fn died_handled(&self) -> bool {
        self.died_handled.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Helper to get the brenn username for the bridge's user.
    pub(super) async fn get_username(&self) -> String {
        let conn = self.db.lock().await;
        conversation::get_username(&conn, self.user_id).unwrap_or_else(|| {
            panic!("BUG: user_id {} not found in DB", self.user_id);
        })
    }

    /// Artifact-display roots for this bridge: thin wrapper around
    /// `artifact::mount_roots_for(&self.mounts)`. Kept as a method so call
    /// sites that hold an `ActiveBridge` don't have to reach into `mounts`.
    pub(crate) fn artifact_mount_roots(&self) -> Vec<brenn_render::artifact::MountRoot> {
        brenn_render::artifact::mount_roots_for(&self.mounts)
    }

    /// Alert/security-event dispatcher for this bridge. Used by the messaging
    /// intercept to emit app-attributed security signals.
    pub(crate) fn alert_dispatcher(&self) -> &AlertDispatcher {
        &self.alert_dispatcher
    }

    /// This bridge's app-origin attribution for a publish-denial security signal.
    /// Pairs `app_slug` with `conversation_id` in one place so the two cannot
    /// drift apart at a call site; every app-path `signal_publish_denial` caller
    /// uses this instead of hand-building `DenialOrigin::App`.
    pub(crate) fn denial_origin(&self) -> brenn_obs::security::DenialOrigin<'_> {
        brenn_obs::security::DenialOrigin::App {
            slug: &self.app_slug,
            conversation_id: self.conversation_id,
        }
    }

    /// Messenger handle for messaging tools. `None` when this server has
    /// no messaging configured.
    pub fn messenger(&self) -> Option<&Arc<brenn_messaging::Messenger>> {
        self.messenger.as_ref()
    }

    /// PWA push service handle. `None` when no app has `pwa_push.enabled`.
    pub fn pwa_push_service(&self) -> Option<&Arc<dyn brenn_pwa_push::PwaPushSender>> {
        self.pwa_push_service.as_ref()
    }

    /// Automation engine handle. `None` when automation is not configured
    /// (no messaging or no apps with allowed_users).
    pub fn automation_engine(&self) -> Option<&Arc<brenn_automation::AutomationEngine>> {
        self.automation_engine.as_ref()
    }

    /// MQTT service handle. `None` when no `[[mqtt_client]]` is configured.
    pub fn mqtt_service(&self) -> Option<&Arc<brenn_mqtt::MqttService>> {
        self.mqtt_service.as_ref()
    }

    /// Concrete MQTT event router handle. `None` when MQTT is not configured.
    /// Used by the runtime `mqtt:` dynamic-subscribe path to add an
    /// `IngressRoute` (design §2.3 step 6).
    pub fn mqtt_event_router(&self) -> Option<&Arc<crate::mqtt_router::MqttEventRouterImpl>> {
        self.mqtt_event_router.as_ref()
    }

    /// The shared DB handle. Used by callers that need to pass the `Db` to a
    /// helper which locks it internally for a short synchronous step (e.g.
    /// `mqtt::egress::enforce_and_publish`, which decrements the send budget under
    /// the lock and drops it before the broker await).
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Per-app messaging send budget (used by MQTT send + future tools that
    /// share the same budget). Falls back to the global default when the app
    /// has no `[app.messaging]` block.
    pub fn app_config_default_send_budget(&self) -> u32 {
        self.messaging_default_send_budget
    }

    /// Returns the typed config for an integration by name, or `None` if
    /// that integration is not enabled or the stored instance is not of type
    /// `T`. The `'static` bound is required by `Any::downcast_ref`.
    fn integration_config<T: 'static>(&self, name: &str) -> Option<&T> {
        self.integrations
            .get(name)
            .and_then(|i| i.as_any().downcast_ref::<T>())
    }

    /// Returns the typed pfin config for this app, or `None` when pfin is not
    /// enabled on this app.
    ///
    /// When pfin is enabled, the config is always present (the factory panics
    /// at startup on missing config), so callers that run only when pfin is
    /// enabled should use `.expect("pfin enabled ⇒ config present")`.
    pub(super) fn pfin_config(&self) -> Option<&brenn_pfin::PfinConfig> {
        self.integration_config::<brenn_pfin::PfinIntegration>(brenn_pfin::INTEGRATION_NAME)
            .map(|p| &p.config)
    }

    /// Assemble a `SubprocessExecContext` for pfin subprocess invocations.
    ///
    /// All four pfin exec sites use the same fields; this helper centralises
    /// assembly so that adding a new field to `SubprocessExecContext` only
    /// requires one update.
    pub(super) fn pfin_exec_ctx<'a>(
        &'a self,
        cfg: &'a brenn_pfin::PfinConfig,
    ) -> brenn_lib::subprocess::SubprocessExecContext<'a> {
        brenn_lib::subprocess::SubprocessExecContext {
            command: &cfg.command,
            env: &cfg.env,
            working_dir: &self.working_dir,
            container_spawn: self.container_spawn.as_ref(),
        }
    }

    // -----------------------------------------------------------------------
    // Idle hooks — see `docs/designs/idle-hooks.md`.
    // -----------------------------------------------------------------------

    /// Find the deepest `auto_pull = true` RW mount whose canonical root
    /// is an ancestor of (or equal to) the canonical parent of `host_path`.
    ///
    /// Returns `Some(slug)` for the deepest matching mount, or `None` when no
    /// auto-pull mount covers the path. Mounts that fail to canonicalize are
    /// skipped with a `warn!` log (same policy as `open_export_target`).
    ///
    /// If the target's parent does not exist (common for new export destinations),
    /// walks up to the first existing ancestor before canonicalizing — so that
    /// paths into not-yet-created subdirectories of an auto-pull mount still
    /// produce a warning.
    ///
    /// This helper is advisory only (approval-prompt annotation). It does NOT
    /// share implementation with `open_export_target`'s mount matching, which
    /// is security-critical and TOCTOU-safe via `openat2`. The annotation needs
    /// only ancestor-check semantics; coupling the two would complicate the
    /// sandbox code for no benefit.
    pub(super) fn find_auto_pull_mount(&self, host_path: &std::path::Path) -> Option<&str> {
        let parent = host_path.parent().unwrap_or(std::path::Path::new("/"));
        // Bare-filename paths (no directory component) produce an empty parent
        // string. Canonicalizing "" resolves to the process cwd, which is
        // non-deterministic and may yield a spurious match. Return None early.
        if parent.as_os_str().is_empty() {
            return None;
        }
        // Walk up to find the first existing ancestor to canonicalize. This
        // handles the common case where the export destination directory does
        // not exist yet — e.g. a new subdirectory inside an auto-pull mount.
        let mut probe = parent;
        let canon_parent = loop {
            match probe.canonicalize() {
                Ok(p) => break p,
                Err(_) => match probe.parent() {
                    Some(up) => probe = up,
                    None => {
                        warn!(
                            path = %host_path.display(),
                            "find_auto_pull_mount: could not canonicalize any ancestor of target path; no annotation"
                        );
                        return None;
                    }
                },
            }
        };

        let mut best: Option<(usize, &str)> = None; // (component_count, slug)

        for mount in &self.mounts {
            if mount.access != brenn_lib::config::AccessLevel::ReadWrite || !mount.auto_pull {
                continue;
            }
            let canon_root = match mount.host_path.canonicalize() {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        slug = %mount.slug,
                        path = %mount.host_path.display(),
                        error = %e,
                        "find_auto_pull_mount: mount host_path cannot be canonicalized; skipping"
                    );
                    continue;
                }
            };
            if canon_parent.starts_with(&canon_root) {
                let depth = canon_root.components().count();
                let is_deeper = best.is_none_or(|(prev_depth, _)| depth > prev_depth);
                if is_deeper {
                    best = Some((depth, mount.slug.as_str()));
                }
            }
        }

        best.map(|(_, slug)| slug)
    }

    /// Annotate a tool input JSON with `_git_sync_mount` when `output_file`
    /// falls inside an `auto_pull = true` RW mount.
    ///
    /// Strips any incoming `_git_sync_mount` from the CC-supplied input first
    /// so that CC cannot forge the field (e.g. to create a false-positive
    /// warning on a safe path or to suppress a warning on a synced path).
    ///
    /// If `output_file` is absent or path translation fails, the input is returned
    /// unchanged (no annotation is better than a wrong one).
    pub(super) fn annotate_git_sync(&self, mut tool_input: serde_json::Value) -> serde_json::Value {
        // Strip CC-supplied annotation fields before computing ours. This
        // ensures only server-injected values reach the formatter.
        if let Some(obj) = tool_input.as_object_mut() {
            obj.remove("_git_sync_mount");
        }
        let output_file_str = match tool_input.get("output_file").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s.to_owned(),
            _ => return tool_input,
        };
        let agent_path = std::path::PathBuf::from(&output_file_str);
        let host_path = match self.path_mapper.to_host(&agent_path) {
            Some(p) => p,
            None => return tool_input,
        };
        if let Some(slug) = self.find_auto_pull_mount(&host_path)
            && let Some(obj) = tool_input.as_object_mut()
        {
            obj.insert(
                "_git_sync_mount".to_string(),
                serde_json::Value::String(slug.to_string()),
            );
        }
        tool_input
    }

    /// Enrich a tool input with `_pending_import` from the pfin CLI.
    ///
    /// If the input already has `_pending_import`, or lacks `import_id`, skips
    /// enrichment. On fetch failure, logs a warning and returns the input
    /// unchanged (non-fatal — the display just won't have the header).
    pub(super) async fn enrich_with_import_details(
        &self,
        mut tool_input: serde_json::Value,
    ) -> serde_json::Value {
        if tool_input.get("_pending_import").is_some() {
            return tool_input;
        }
        if let Some(import_id) = tool_input.get("import_id").and_then(|v| v.as_str()) {
            let pfin_config = self
                .pfin_config()
                .expect("pfin enabled ⇒ config present; missing config is a startup bug");
            let ctx = self.pfin_exec_ctx(pfin_config);
            match brenn_pfin::fetch_import_details(import_id, &ctx).await {
                Ok(details) => {
                    tool_input
                        .as_object_mut()
                        .expect("tool_input must be an object")
                        .insert("_pending_import".to_string(), details);
                }
                Err(e) => {
                    warn!("failed to fetch import details for {import_id}: {e}");
                }
            }
        }
        tool_input
    }
}

/// Static base of the global auto-approve tool list.
///
/// This slice lists every tool that is unconditionally auto-approved at
/// bridge construction time. `MCP_EXPORT_USAGE_TOOL` is intentionally
/// absent — it must go through the CC Permission flow.
///
/// The full `global_extra` built in `spawn_bridge` appends dynamic entries
/// from the `tool_registry` (tools whose `AppTool::auto_approve()` returns
/// true). This constant covers only the hardcoded base.
pub(super) const GLOBAL_EXTRA_STATIC_BASE: &[&str] = &[
    MCP_DISPLAY_FILE_TOOL,
    MCP_PROPOSE_RECONCILIATION_TOOL,
    MCP_BATCH_RECONCILE_TOOL,
    MCP_BATCH_ASSIGN_TOOL,
    MCP_REQUEST_COMPACTION_TOOL,
    MCP_GIT_LIST_REPOS_TOOL,
    MCP_GIT_REPO_STATUS_TOOL,
    // GitRepoPull is a first-class registry tool; the registry_adapter
    // auto-approves it at PreToolUse, so it needs no Permission-layer entry.
    // Messaging tools — auto-approved (the budget bounds runaway agent loops).
    brenn_render::tools::messaging::MCP_MESSAGE_LIST_CHANNELS_TOOL,
    brenn_render::tools::messaging::MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL,
    brenn_render::tools::messaging::MCP_MESSAGE_SEND_TOOL,
    brenn_render::tools::messaging::MCP_MESSAGE_QUERY_CHANNEL_TOOL,
    // Automation tools — all four are auto-approved.
    brenn_automation::MCP_AUTO_CREATE_TOOL,
    brenn_automation::MCP_AUTO_LIST_TOOL,
    brenn_automation::MCP_AUTO_EDIT_TOOL,
    brenn_automation::MCP_AUTO_DELETE_TOOL,
    MCP_DEVICE_LIST_TOOL,
    MCP_DEVICE_GET_TOOL,
    MCP_DEVICE_ASSIGN_SLUG_TOOL,
    // GitRepoCommitAndPush and GitRepoRun are NOT auto-approved —
    // they go through the Permission flow for user approval.
    // ExportUsage is NOT auto-approved — it writes files and flows
    // through CC's standard approval mechanism.
];

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh spawn seeds `last_set_model` with the model it passed as
    /// `--model`, so the first send at that model costs no NDJSON frame.
    #[test]
    fn fresh_spawn_seeds_the_seed_with_the_spawn_model() {
        assert_eq!(
            spawn_model_and_seed(None, "sonnet", false),
            ("sonnet".to_string(), Some("sonnet".to_string()))
        );
        assert_eq!(
            spawn_model_and_seed(Some("opus"), "sonnet", false),
            ("opus".to_string(), Some("opus".to_string())),
            "the seed follows the override, not the app default"
        );
    }

    /// A resumed spawn is not seeded, so the first send re-asserts the
    /// effective model rather than deduping it away.
    #[test]
    fn resumed_spawn_leaves_the_seed_empty() {
        assert_eq!(
            spawn_model_and_seed(None, "sonnet", true),
            ("sonnet".to_string(), None)
        );
        assert_eq!(
            spawn_model_and_seed(Some("opus"), "sonnet", true),
            ("opus".to_string(), None)
        );
    }

    /// ExportUsage is absent from the production static base of global_extra.
    ///
    /// Directly scans `GLOBAL_EXTRA_STATIC_BASE` — the hardcoded base that
    /// `spawn_bridge` passes to `ApprovalRuleSet::new`. A regression that
    /// re-adds `MCP_EXPORT_USAGE_TOOL` to that slice will fail here regardless
    /// of what path the test fixture uses.
    #[test]
    fn export_usage_not_in_global_extra_static_base() {
        assert!(
            !GLOBAL_EXTRA_STATIC_BASE.contains(&MCP_EXPORT_USAGE_TOOL),
            "MCP_EXPORT_USAGE_TOOL must not appear in GLOBAL_EXTRA_STATIC_BASE; \
             ExportUsage must go through the CC Permission flow, not auto-approve"
        );
    }

    /// The concrete `MqttEventRouterImpl` threads through the test fixture onto
    /// the bridge and is reachable via `mqtt_event_router()` (the handle the
    /// runtime `mqtt:` subscribe-activation path needs for `add_route`, design
    /// §2.3 step 6). Default fixtures leave it `None`.
    #[tokio::test]
    async fn mqtt_event_router_threads_through_fixture() {
        use crate::active_bridge::test_fixtures::TestBridgeConfig;
        let db = crate::test_support::init_db_memory();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_db::auth::user::create_user(&conn, "mqttrouteruser", "$argon2id$fake");
            let cid = brenn_db::conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (tx, _rx) = tokio::sync::broadcast::channel(4);

        // Default fixture: no router wired.
        let plain = ActiveBridge::inject_for_test_full(
            user_id,
            conv_id,
            "test",
            db.clone(),
            tx.clone(),
            brenn_obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig::default(),
        );
        assert!(
            plain.mqtt_event_router().is_none(),
            "default fixture must leave mqtt_event_router None"
        );

        // Injected router resolves through the accessor (same Arc).
        let router = std::sync::Arc::new(crate::mqtt_router::MqttEventRouterImpl::new());
        let bridge = ActiveBridge::inject_for_test_full(
            user_id,
            conv_id,
            "test",
            db,
            tx,
            brenn_obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                mqtt_event_router: Some(router.clone()),
                ..Default::default()
            },
        );
        let got = bridge
            .mqtt_event_router()
            .expect("injected router must be reachable");
        assert!(
            std::sync::Arc::ptr_eq(got, &router),
            "mqtt_event_router() must return the exact injected Arc"
        );
    }

    // -----------------------------------------------------------------------
    // annotate_git_sync / find_auto_pull_mount tests
    // -----------------------------------------------------------------------

    fn mk_mount(
        slug: &str,
        host_path: std::path::PathBuf,
        access: brenn_lib::config::AccessLevel,
        auto_pull: bool,
    ) -> brenn_lib::config::ResolvedMount {
        brenn_lib::config::ResolvedMount {
            slug: slug.to_string(),
            host_path,
            container_path: None,
            access,
            auto_pull,
            is_working_dir: false,
            primary: false,
        }
    }

    async fn bridge_with_mounts(
        mounts: Vec<brenn_lib::config::ResolvedMount>,
    ) -> std::sync::Arc<ActiveBridge> {
        bridge_with_mounts_and_mapper(mounts, brenn_lib::config::PathMapper::Identity).await
    }

    async fn bridge_with_mounts_and_mapper(
        mounts: Vec<brenn_lib::config::ResolvedMount>,
        mapper: brenn_lib::config::PathMapper,
    ) -> std::sync::Arc<ActiveBridge> {
        let db = crate::test_support::init_db_memory();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_db::auth::user::create_user(&conn, "testuser2", "$argon2id$fake");
            let cid = brenn_db::conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (tx, _rx) = tokio::sync::broadcast::channel(4);
        ActiveBridge::inject_for_test_with_mounts_and_mapper(
            user_id, conv_id, "test", db, tx, mounts, mapper,
        )
    }

    /// annotate_git_sync injects _git_sync_mount when path is under an auto_pull mount.
    #[tokio::test]
    async fn annotate_git_sync_injects_field_for_auto_pull_mount() {
        let tmp = tempfile::tempdir().unwrap();
        let mount = mk_mount(
            "life",
            tmp.path().to_path_buf(),
            brenn_lib::config::AccessLevel::ReadWrite,
            true,
        );
        let bridge = bridge_with_mounts(vec![mount]).await;

        let out_path = tmp.path().join("usage.csv");
        let input = serde_json::json!({ "output_file": out_path.to_str().unwrap() });
        let result = bridge.annotate_git_sync(input);

        assert_eq!(
            result.get("_git_sync_mount").and_then(|v| v.as_str()),
            Some("life"),
            "_git_sync_mount must be the slug: {result}"
        );
    }

    /// annotate_git_sync does not inject when the matching mount has auto_pull=false.
    #[tokio::test]
    async fn annotate_git_sync_no_injection_for_non_auto_pull() {
        let tmp = tempfile::tempdir().unwrap();
        let mount = mk_mount(
            "work",
            tmp.path().to_path_buf(),
            brenn_lib::config::AccessLevel::ReadWrite,
            false, // auto_pull = false
        );
        let bridge = bridge_with_mounts(vec![mount]).await;

        let out_path = tmp.path().join("usage.csv");
        let input = serde_json::json!({ "output_file": out_path.to_str().unwrap() });
        let result = bridge.annotate_git_sync(input);

        assert!(
            result.get("_git_sync_mount").is_none(),
            "must not inject _git_sync_mount for non-auto_pull mount: {result}"
        );
    }

    /// annotate_git_sync does not inject when the path is outside all mounts.
    #[tokio::test]
    async fn annotate_git_sync_no_injection_for_path_outside_mounts() {
        let tmp_mount = tempfile::tempdir().unwrap();
        let tmp_other = tempfile::tempdir().unwrap();
        let mount = mk_mount(
            "life",
            tmp_mount.path().to_path_buf(),
            brenn_lib::config::AccessLevel::ReadWrite,
            true,
        );
        let bridge = bridge_with_mounts(vec![mount]).await;

        let out_path = tmp_other.path().join("usage.csv");
        let input = serde_json::json!({ "output_file": out_path.to_str().unwrap() });
        let result = bridge.annotate_git_sync(input);

        assert!(
            result.get("_git_sync_mount").is_none(),
            "must not inject _git_sync_mount for path outside mounts: {result}"
        );
    }

    /// annotate_git_sync returns input unchanged when output_file is absent.
    #[tokio::test]
    async fn annotate_git_sync_missing_output_file() {
        let bridge = bridge_with_mounts(vec![]).await;
        let input = serde_json::json!({ "kind": "sessions" });
        let result = bridge.annotate_git_sync(input.clone());
        assert_eq!(
            result, input,
            "must return input unchanged when output_file absent"
        );
    }

    /// annotate_git_sync injects warning even when the target directory does
    /// not exist yet. Walking up to the first existing ancestor allows new
    /// subdirectories inside an auto-pull mount to be correctly detected.
    #[tokio::test]
    async fn annotate_git_sync_nonexistent_subdir_under_auto_pull_mount() {
        let tmp = tempfile::tempdir().unwrap();
        let mount = mk_mount(
            "life",
            tmp.path().to_path_buf(),
            brenn_lib::config::AccessLevel::ReadWrite,
            true,
        );
        let bridge = bridge_with_mounts(vec![mount]).await;

        // Point at a path whose parent directory does not exist.
        let out_path = tmp.path().join("nonexistent-subdir").join("usage.csv");
        let input = serde_json::json!({ "output_file": out_path.to_str().unwrap() });
        let result = bridge.annotate_git_sync(input);

        assert_eq!(
            result.get("_git_sync_mount").and_then(|v| v.as_str()),
            Some("life"),
            "_git_sync_mount must be the slug even for nonexistent subdir: {result}"
        );
    }

    /// find_auto_pull_mount returns the deepest matching mount when two
    /// auto_pull mounts are nested (e.g. /repos and /repos/life).
    #[tokio::test]
    async fn annotate_git_sync_deepest_mount_wins() {
        let tmp = tempfile::tempdir().unwrap();
        // Shallow mount covering the whole tmp dir.
        let mount_shallow = mk_mount(
            "repos",
            tmp.path().to_path_buf(),
            brenn_lib::config::AccessLevel::ReadWrite,
            true,
        );
        // Deep mount covering a sub-directory.
        let deep_dir = tmp.path().join("life");
        std::fs::create_dir(&deep_dir).unwrap();
        let mount_deep = mk_mount(
            "life",
            deep_dir.clone(),
            brenn_lib::config::AccessLevel::ReadWrite,
            true,
        );
        let bridge = bridge_with_mounts(vec![mount_shallow, mount_deep]).await;

        let out_path = deep_dir.join("usage.csv");
        let input = serde_json::json!({ "output_file": out_path.to_str().unwrap() });
        let result = bridge.annotate_git_sync(input);

        assert_eq!(
            result.get("_git_sync_mount").and_then(|v| v.as_str()),
            Some("life"),
            "deepest matching mount slug must win: {result}"
        );
    }

    /// ReadOnly mount with auto_pull=true must not produce a warning —
    /// you cannot write there, so no git sync risk applies.
    #[tokio::test]
    async fn annotate_git_sync_no_injection_for_readonly_auto_pull_mount() {
        let tmp = tempfile::tempdir().unwrap();
        let mount = mk_mount(
            "readonly-synced",
            tmp.path().to_path_buf(),
            brenn_lib::config::AccessLevel::ReadOnly,
            true, // auto_pull = true, but ReadOnly
        );
        let bridge = bridge_with_mounts(vec![mount]).await;

        let out_path = tmp.path().join("usage.csv");
        let input = serde_json::json!({ "output_file": out_path.to_str().unwrap() });
        let result = bridge.annotate_git_sync(input);

        assert!(
            result.get("_git_sync_mount").is_none(),
            "must not inject _git_sync_mount for ReadOnly mount even with auto_pull: {result}"
        );
    }

    /// annotate_git_sync strips CC-supplied _git_sync_mount before computing
    /// the server-side annotation. A forged field cannot survive to the formatter.
    #[tokio::test]
    async fn annotate_git_sync_strips_forged_git_sync_mount() {
        let tmp = tempfile::tempdir().unwrap();
        // No auto_pull mounts — path will not match anything.
        let mount = mk_mount(
            "work",
            tmp.path().to_path_buf(),
            brenn_lib::config::AccessLevel::ReadWrite,
            false,
        );
        let bridge = bridge_with_mounts(vec![mount]).await;

        let out_path = tmp.path().join("usage.csv");
        // CC supplies a forged _git_sync_mount on a non-synced path.
        let input = serde_json::json!({
            "output_file": out_path.to_str().unwrap(),
            "_git_sync_mount": "personal-finance",
        });
        let result = bridge.annotate_git_sync(input);

        assert!(
            result.get("_git_sync_mount").is_none(),
            "forged _git_sync_mount must be stripped when path is not in auto_pull mount: {result}"
        );
    }

    /// annotate_git_sync returns no annotation when path_mapper.to_host() returns None
    /// (path outside all container mappings). The annotation must not fire on an
    /// untranslatable path — no annotation is better than a wrong one.
    #[tokio::test]
    async fn annotate_git_sync_no_injection_for_path_outside_container_mapping() {
        let tmp = tempfile::tempdir().unwrap();
        let mount = mk_mount(
            "life",
            tmp.path().to_path_buf(),
            brenn_lib::config::AccessLevel::ReadWrite,
            true,
        );
        // Container mapper covers /container/life → tmp, but the tool_input
        // references /other/path which has no mapping.
        let mapper =
            brenn_lib::config::PathMapper::container(vec![brenn_lib::config::PathMapping {
                host_root: tmp.path().to_path_buf(),
                container_root: std::path::PathBuf::from("/container/life"),
            }]);
        let bridge = bridge_with_mounts_and_mapper(vec![mount], mapper).await;

        // Path is outside all container mappings → to_host returns None.
        let input = serde_json::json!({ "output_file": "/other/path/usage.csv" });
        let result = bridge.annotate_git_sync(input);

        assert!(
            result.get("_git_sync_mount").is_none(),
            "must not inject when path translation fails: {result}"
        );
    }
}

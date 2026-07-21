//! `WsConnection` struct definition and primitive methods (send, attach, detach,
//! conversation_switched helpers, etc.).
//!
//! Also owns `SendResult` and `msg_type_name` — the return type and log helper
//! for `send_ws`, colocated here with their only production caller.

use std::sync::Arc;
use std::time::Duration;

use brenn_lib::token_bucket::{TokenBucket, TokenBucketOutcome};
use brenn_lib::ws_types::{
    PaneLayout, PermissionDecision, ToolResponseDecision, ViewportClass, WsServerMessage,
};
use tokio::sync::{broadcast, mpsc};
use tracing::warn;

use crate::active_bridge::ActiveBridge;
use crate::state::AppState;

/// Maximum number of `ClientError` messages allowed in a burst before rate-limiting kicks in.
const BURST_CAP: u32 = 10;

/// One token is refilled per this interval under sustained load.
const REFILL_INTERVAL: Duration = Duration::from_secs(60);

/// Per-connection token bucket for `ClientError` messages.
///
/// Prevents a hostile or misbehaving page from flooding the log at TCP speed.
/// State lives on `WsConnection` — each WS tab has an independent bucket.
/// Connection close destroys it; no cleanup is needed. Wraps the generic
/// [`TokenBucket`]; this type owns only the domain-specific transition logging.
pub(super) struct ClientErrorBucket {
    bucket: TokenBucket,
}

impl ClientErrorBucket {
    /// Initialize with a full complement of tokens — the connection starts with
    /// full burst capacity, not in a rate-limited state.
    pub(super) fn new() -> Self {
        Self {
            bucket: TokenBucket::new(BURST_CAP, REFILL_INTERVAL, 1),
        }
    }

    /// Attempt to consume one token. Handles all logging for rate-limit
    /// transitions internally — callers need not log anything on `false`.
    ///
    /// `client_ip` is included in rate-limit transition logs so fail2ban and
    /// on-call engineers can identify the source connection.
    pub(super) fn try_consume(&mut self, client_ip: std::net::IpAddr) -> bool {
        match self.bucket.try_consume() {
            TokenBucketOutcome::Granted => true,
            TokenBucketOutcome::GrantedAfterSuppression { suppressed } => {
                warn!(
                    client_ip = %client_ip,
                    suppressed,
                    "ClientError rate limit lifted, messages were suppressed"
                );
                true
            }
            TokenBucketOutcome::Denied { first } => {
                if first {
                    warn!(
                        client_ip = %client_ip,
                        "rate-limiting ClientError from this connection"
                    );
                }
                false
            }
        }
    }

    /// Messages denied since the current suppression window opened (0 when not
    /// suppressed).
    #[cfg(test)]
    pub(super) fn suppressed(&self) -> u64 {
        self.bucket.suppressed()
    }

    /// Whether a suppression window is currently open.
    #[cfg(test)]
    pub(super) fn in_suppression(&self) -> bool {
        self.bucket.in_suppression()
    }
}

/// Result of `send_ws` — whether the per-tab mpsc accepted the message.
///
/// Marked `#[must_use]` so every call site explicitly acknowledges whether the
/// send succeeded. Fire-and-forget sites use `let _ = self.send_ws(...)`.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SendResult {
    Ok,
    /// The mpsc buffer is full (256 capacity). The broadcast select arm handles
    /// this by triggering a deferred history reload.
    Full,
    /// The writer task exited (WS connection dead). Caller should break the loop.
    Closed,
}

/// Extract the serde tag name from a `WsServerMessage` for logging.
/// Avoids formatting the full message (which may contain large HTML payloads).
pub(super) fn msg_type_name(msg: &WsServerMessage) -> &'static str {
    match msg {
        WsServerMessage::StreamToken { .. } => "StreamToken",
        WsServerMessage::ThinkingToken { .. } => "ThinkingToken",
        WsServerMessage::AssistantMessage { .. } => "AssistantMessage",
        WsServerMessage::PermissionRequest { .. } => "PermissionRequest",
        WsServerMessage::PermissionCancelled { .. } => "PermissionCancelled",
        WsServerMessage::PermissionResolved { .. } => "PermissionResolved",
        WsServerMessage::ToolCardRequest { .. } => "ToolCardRequest",
        WsServerMessage::ToolCardResolved { .. } => "ToolCardResolved",
        WsServerMessage::Status { .. } => "Status",
        WsServerMessage::Error { .. } => "Error",
        WsServerMessage::ConversationList { .. } => "ConversationList",
        WsServerMessage::ConversationSwitched { .. } => "ConversationSwitched",
        WsServerMessage::HistoryComplete { .. } => "HistoryComplete",
        WsServerMessage::UserMessageEcho { .. } => "UserMessageEcho",
        WsServerMessage::SystemMessageBroadcast { .. } => "SystemMessageBroadcast",
        WsServerMessage::ArtifactContent { .. } => "ArtifactContent",
        WsServerMessage::ToolUseSummary { .. } => "ToolUseSummary",
        WsServerMessage::ArtifactIndex { .. } => "ArtifactIndex",
        WsServerMessage::SessionStolen { .. } => "SessionStolen",
        WsServerMessage::AppBusy { .. } => "AppBusy",
        WsServerMessage::Welcome { .. } => "Welcome",
        WsServerMessage::ModelsAvailable { .. } => "ModelsAvailable",
        WsServerMessage::PresenceUpdate { .. } => "PresenceUpdate",
        WsServerMessage::SetLayout { .. } => "SetLayout",
        WsServerMessage::PrivacyChanged { .. } => "PrivacyChanged",
        WsServerMessage::ApprovalRuleError { .. } => "ApprovalRuleError",
        WsServerMessage::TargetResult { .. } => "TargetResult",
        WsServerMessage::ContextUsage { .. } => "ContextUsage",
        WsServerMessage::PermissionMode { .. } => "PermissionMode",
        WsServerMessage::TodoState { .. } => "TodoState",
        WsServerMessage::TodoDoneResult { .. } => "TodoDoneResult",
        WsServerMessage::TodoMutationResult { .. } => "TodoMutationResult",
        WsServerMessage::HistoryPage { .. } => "HistoryPage",
        WsServerMessage::CostUsage { .. } => "CostUsage",
        WsServerMessage::PushVapidKey { .. } => "PushVapidKey",
        WsServerMessage::PushEnabled { .. } => "PushEnabled",
    }
}

/// Queued response received before the bridge was ready.
pub(super) enum QueuedResponse {
    Permission {
        request_id: String,
        decision: PermissionDecision,
    },
    ToolCard {
        request_id: String,
        decision: ToolResponseDecision,
    },
}

/// Per-tab connection state.
pub(super) struct WsConnection {
    pub(super) user_id: i64,
    /// Username resolved at connect time.
    pub(super) username: String,
    /// App slug this WS connection is scoped to.
    pub(super) app_slug: String,
    pub(super) client_ip: std::net::IpAddr,
    pub(super) current_conversation_id: Option<i64>,
    pub(super) broadcast_rx: Option<broadcast::Receiver<WsServerMessage>>,
    pub(super) ws_tx: mpsc::Sender<WsServerMessage>,
    pub(super) state: AppState,
    /// True when auto-attached to another user's single-instance bridge.
    /// Viewer-only connections can observe but not send messages or spawn CC.
    pub(super) viewer_only: bool,
    /// IANA timezone reported by the browser (e.g. "Asia/Tokyo"). Defaults to UTC.
    pub(super) timezone: chrono_tz::Tz,
    /// Viewport class taken from the WS connect URL (required; absence
    /// rejects the upgrade — see `ws_handler`). Used to emit the correct
    /// `SetLayout` BEFORE any history frame so the client can mount the
    /// right DOM shape up front.
    pub(super) viewport_class: ViewportClass,
    /// Device identity resolved at connect time. Fixed for the lifetime of
    /// this connection. Authentication without a device row is a panic
    /// (authenticated-but-no-device is a bug, not a tolerated state).
    pub(super) device_id: i64,
    /// Receives notifications when bridges spawn. Allows auto-attach when
    /// another connection spawns a bridge for this connection's conversation.
    pub(super) bridge_notify_rx: broadcast::Receiver<crate::state::BridgeSpawned>,
    /// True after `send_history` has been called for the current conversation.
    /// Cleared by `detach()`. No longer suppresses re-replay on BridgeSpawned
    /// (see `last_sent_seq`), but still used to distinguish first-connect from
    /// reconnect paths that don't call `send_history`.
    pub(super) history_sent: bool,
    /// Highest DB seq delivered to this tab via history replay or live broadcast
    /// forwarding. Used as `from_seq` for incremental re-replay on `BridgeSpawned`
    /// so drain rows written after the initial `send_history` are not lost.
    ///
    /// Not cleared by `detach()` — preserved across CC restarts for persistent apps
    /// so BridgeSpawned triggers a cheap incremental replay instead of full history.
    /// Cleared on conversation switch (alongside `oldest_loaded_seq`).
    pub(super) last_sent_seq: Option<i64>,
    /// Queued approval/tool-card responses received before the bridge was ready.
    /// Drained when the bridge finishes initializing.
    pub(super) queued_responses: Vec<QueuedResponse>,
    /// Tracks whether a replay seam was active for the current conversation.
    /// Set by `send_history`, cleared on conversation switch. Used to guard
    /// `LoadMoreHistory` — the frontend shouldn't request more history when
    /// the full history was already sent.
    pub(super) oldest_loaded_seq: Option<i64>,
    pub(super) client_error_bucket: ClientErrorBucket,
    /// Test-only: pre-built bridge returned by `spawn_bridge` instead of spawning CC.
    #[cfg(test)]
    pub(super) test_bridge: Option<Arc<ActiveBridge>>,
}

// impl WsConnection — primitive accessors and core send/attach/detach helpers
impl WsConnection {
    /// Get the app config for this connection's app slug.
    pub(super) fn app_config(&self) -> &brenn_lib::config::AppConfig {
        self.state
            .apps
            .get(&self.app_slug)
            .unwrap_or_else(|| panic!("app {:?} not found in config", self.app_slug))
    }

    /// IANA timezone string for the current connection.
    pub(super) fn timezone_str(&self) -> &str {
        self.timezone.name()
    }

    /// Effective timezone for this connection: the DB override (if active) or browser TZ.
    ///
    /// Locks the DB, loads the device_user row, and delegates to `effective_timezone`.
    /// Use only when you do not already hold the device_user row; call
    /// `brenn_lib::auth::device::effective_timezone(&du, self.timezone, Utc::now())` directly
    /// when the row is already loaded to avoid a redundant lock.
    pub(super) async fn effective_timezone(&self) -> chrono_tz::Tz {
        let conn = self.state.db.lock().await;
        let du = brenn_lib::auth::device::load_device_user(&conn, self.device_id, self.user_id);
        brenn_lib::auth::device::effective_timezone(&du, self.timezone, chrono::Utc::now())
    }

    /// "Today" in this connection's effective timezone — the authoritative value
    /// for the frontend's task-sectioning logic. Sent in every TodoState
    /// so the browser doesn't use `new Date()` (which disagrees with
    /// graf's resolved zone when they differ).
    ///
    /// Production callers that need both env and today must use
    /// `build_graf_env_and_today` to avoid two independent DB reads.
    /// This wrapper exists for call sites that need only today (and for tests).
    #[allow(dead_code)]
    pub(super) async fn today_in_connection_tz(&self) -> chrono::NaiveDate {
        chrono::Utc::now()
            .with_timezone(&self.effective_timezone().await)
            .date_naive()
    }

    /// Fetch the display slug for this connection's device + user.
    ///
    /// Re-fetches from DB on every call — intentional, not an oversight.
    /// Device renames during a session must be picked up immediately.
    /// Panics on DB miss (matches existing inline behavior).
    pub(super) async fn fetch_device_slug(&self) -> String {
        let conn = self.state.db.lock().await;
        let device = brenn_lib::auth::device::load_device(&conn, self.device_id);
        let du = brenn_lib::auth::device::load_device_user(&conn, self.device_id, self.user_id);
        du.display_slug(&device).to_string()
    }

    /// Thin wrapper over `build_graf_env_from` — see that function for docs.
    pub(super) async fn build_graf_env(&self) -> Vec<(String, String)> {
        super::usage::build_graf_env_from(self.app_config(), self.effective_timezone().await)
    }

    /// Compute both the graf environment and today's date under one DB lock.
    ///
    /// Call sites that need both (e.g. `send_todo_state`) must use this to avoid
    /// loading the device_user row twice with two independent `Utc::now()` snapshots.
    /// Two independent reads leave a narrow TOCTOU window where a TZ override write
    /// or expiry crossing between them would produce env and today derived from
    /// different effective timezones — the exact divergence this feature exists to close.
    ///
    /// One lock, one `load_device_user`, one `Utc::now()` — mirrors the discipline
    /// in `persist_and_send` (design §2.2).
    pub(super) async fn build_graf_env_and_today(
        &self,
    ) -> (Vec<(String, String)>, chrono::NaiveDate) {
        let now = chrono::Utc::now();
        let conn = self.state.db.lock().await;
        let du = brenn_lib::auth::device::load_device_user(&conn, self.device_id, self.user_id);
        let tz = brenn_lib::auth::device::effective_timezone(&du, self.timezone, now);
        let env = super::usage::build_graf_env_from(self.app_config(), tz);
        let today = now.with_timezone(&tz).date_naive();
        (env, today)
    }

    /// Build a `ConversationSwitched` message with correct `is_owner` and `shared`.
    ///
    /// When `conversation` is `None` (empty state), `is_owner` is `true` and
    /// `shared` is `false` — both vacuously (no conversation, no restriction).
    pub(super) fn conversation_switched(
        &self,
        conversation: Option<&brenn_lib::conversation::Conversation>,
        state: brenn_lib::ws_types::CcState,
    ) -> WsServerMessage {
        WsServerMessage::ConversationSwitched {
            conversation_id: conversation.map(|c| c.id),
            state,
            is_owner: conversation.is_none_or(|c| c.user_id == self.user_id),
            shared: conversation.is_some_and(|c| c.shared),
            reload: false,
        }
    }

    /// Build a `ConversationSwitched` from a bridge's fields (no `Conversation` needed).
    /// Used in connect flow and broadcast recovery where we have a bridge but no DB struct.
    pub(super) fn conversation_switched_from_bridge(
        &self,
        bridge: &ActiveBridge,
        state: brenn_lib::ws_types::CcState,
    ) -> WsServerMessage {
        self.conversation_switched_from_bridge_inner(bridge, state, false)
    }

    /// Like `conversation_switched_from_bridge`, but with `reload: true`.
    /// Used for recovery paths (broadcast lag, mpsc buffer overflow) where the
    /// frontend must clear and reload even though the conversation_id hasn't changed.
    pub(super) fn conversation_switched_reload_from_bridge(
        &self,
        bridge: &ActiveBridge,
        state: brenn_lib::ws_types::CcState,
    ) -> WsServerMessage {
        self.conversation_switched_from_bridge_inner(bridge, state, true)
    }

    fn conversation_switched_from_bridge_inner(
        &self,
        bridge: &ActiveBridge,
        state: brenn_lib::ws_types::CcState,
        reload: bool,
    ) -> WsServerMessage {
        WsServerMessage::ConversationSwitched {
            conversation_id: Some(bridge.conversation_id),
            state,
            is_owner: bridge.user_id == self.user_id,
            shared: bridge.shared.load(std::sync::atomic::Ordering::Relaxed),
            reload,
        }
    }

    /// Send the appropriate pane layout for this connection's viewport class.
    pub(super) async fn send_layout(&self) {
        let layout = match self.viewport_class {
            ViewportClass::Compact => PaneLayout::SinglePane,
            ViewportClass::Wide => PaneLayout::TwoColumn,
        };
        // Use backpressure so a saturated writer channel does not silently drop
        // the SetLayout frame, leaving the client's layout state stuck.
        // Error means the WS writer channel is closed (connection dead); already
        // logged inside send_ws_backpressure. No recovery action possible here.
        self.send_ws_backpressure(WsServerMessage::SetLayout { layout })
            .await
            .ok();
    }

    /// Attach to an active bridge's broadcast channel and register as present.
    /// Creates a new receiver via `bridge.subscribe()` — use this for attaching
    /// to an already-running bridge (auto-attach, switch conversation, etc.).
    pub(super) async fn attach_to_bridge(&mut self, bridge: &Arc<ActiveBridge>) {
        let rx = bridge.subscribe();
        self.attach_to_bridge_with_rx(bridge, rx).await;
    }

    /// Attach using a pre-created broadcast receiver. Used by `spawn_bridge`
    /// where the receiver must exist before the event loop starts.
    ///
    /// `add_subscriber` broadcasts to existing subscribers when this user first
    /// appears. The `rx` may also buffer that broadcast (it was created before
    /// `add_subscriber`), causing a harmless duplicate PresenceUpdate on this
    /// connection. PresenceUpdate is an idempotent full snapshot, so this is fine.
    /// The direct send is needed for the multi-tab case where `add_subscriber`
    /// doesn't broadcast (user already present, count goes 1→2).
    pub(super) async fn attach_to_bridge_with_rx(
        &mut self,
        bridge: &Arc<ActiveBridge>,
        rx: broadcast::Receiver<WsServerMessage>,
    ) {
        self.current_conversation_id = Some(bridge.conversation_id);
        let presence = bridge.add_subscriber(self.user_id, &self.username).await;
        self.broadcast_rx = Some(rx);
        bridge.set_viewport_class(self.viewport_class);
        // Send presence snapshot directly to this connection (not broadcast).
        let _ = self.send_ws(WsServerMessage::PresenceUpdate {
            conversation_id: bridge.conversation_id,
            users: presence,
        });
    }

    /// Check single-instance enforcement. Returns true (blocked) if another
    /// session is already running for this single-instance app.
    pub(super) async fn check_single_instance_blocked(&self) -> bool {
        let app_config = self.app_config();
        if app_config.single_instance {
            let existing = self.state.active_bridges.get_for_app(&self.app_slug).await;
            if !existing.is_empty() {
                let owner_info = format!("conversation {}", existing[0].conversation_id);
                let _ = self.send_ws(WsServerMessage::AppBusy {
                    message: format!(
                        "This app already has an active session ({owner_info}). \
                         You can force-close it to start a new one."
                    ),
                });
                return true;
            }
        }
        false
    }

    /// Detach from any current broadcast (e.g., when switching conversations).
    /// Removes subscriber presence if we're attached to a bridge.
    /// Clears `history_sent` so the next attach sends fresh history.
    pub(super) async fn detach(&mut self) {
        self.history_sent = false;
        // Clear any queued approvals from the old conversation.
        self.queued_responses.clear();
        if self.broadcast_rx.is_some() {
            // Remove presence before dropping the broadcast receiver.
            if let Some(conv_id) = self.current_conversation_id
                && let Some(bridge) = self.state.active_bridges.get(conv_id).await
            {
                bridge.remove_subscriber(self.user_id).await;
            }
            self.broadcast_rx = None;
        }
        // Don't clear current_conversation_id — we may still be viewing history.
    }

    /// Mark that history has already been sent to this connection.
    ///
    /// Sets `history_sent = true` for incremental re-replay on bridge respawn.
    /// `last_sent_seq` MUST be preserved here — do NOT clear it. The two fields
    /// are coupled: clearing `last_sent_seq` "symmetrically" would silently
    /// regress to full replay on every CC restart.
    pub(super) fn mark_history_already_sent(&mut self) {
        // Do NOT assert last_sent_seq.is_some() here: for an empty conversation
        // (no persisted rows), send_history leaves last_sent_seq = None, and
        // that is valid — an empty replay is still idempotent. The invariant
        // the doc comment describes (do NOT clear last_sent_seq) is about
        // callers of this function, not a precondition on its input.
        self.history_sent = true;
    }

    /// Extract the DB seq from a message, if it carries one.
    ///
    /// Used to update `last_sent_seq` when forwarding live broadcasts so the
    /// BridgeSpawned incremental re-replay cursor stays current.
    pub(super) fn extract_seq(msg: &WsServerMessage) -> Option<i64> {
        match msg {
            WsServerMessage::UserMessageEcho { seq, .. }
            | WsServerMessage::SystemMessageBroadcast { seq, .. }
            | WsServerMessage::AssistantMessage { seq, .. }
            | WsServerMessage::ToolUseSummary { seq, .. }
            | WsServerMessage::ArtifactContent { seq, .. }
            | WsServerMessage::TargetResult { seq, .. } => *seq,
            _ => None,
        }
    }

    /// If `cached_models` contains a non-empty entry for this connection's app,
    /// send `ModelsAvailable` to this connection.
    ///
    /// Called from the `BridgeSpawned` handler (guarded by `app_slug` match at
    /// the call site). Send is fire-and-forget: buffer-full drops `ModelsAvailable`
    /// (model picker hidden until next event or refresh); the connection is not
    /// considered dead. `Closed` means the connection is already dead and will be
    /// cleaned up at the next backpressure send. See also: `Status` send at
    /// event_loop.rs:393.
    pub(super) async fn send_models_if_app_populated(&self) {
        let models = self.state.cached_models.read().await;
        if let Some(model_infos) = models.get(&self.app_slug)
            && !model_infos.is_empty()
            && let SendResult::Closed = self.send_ws(WsServerMessage::ModelsAvailable {
                available_models: model_infos.clone(),
            })
        {
            tracing::debug!(
                app_slug = %self.app_slug,
                "ModelsAvailable: WS send channel closed (connection already dead)"
            );
        }
    }

    /// Send a message to this tab's WS sink.
    ///
    /// Returns `SendResult::Full` if the per-tab mpsc buffer is at capacity
    /// (the broadcast select arm handles this by triggering a history reload).
    /// Returns `SendResult::Closed` if the writer task has exited.
    pub(super) fn send_ws(&self, msg: WsServerMessage) -> SendResult {
        match self.ws_tx.try_send(msg) {
            Ok(()) => SendResult::Ok,
            Err(mpsc::error::TrySendError::Full(dropped)) => {
                warn!("WS send buffer full, dropped {}", msg_type_name(&dropped));
                SendResult::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("WS send channel closed");
                SendResult::Closed
            }
        }
    }

    /// Send a message to this tab's WS sink, waiting for buffer space.
    ///
    /// Unlike `send_ws` (which uses `try_send` and drops on full), this waits
    /// for a slot in the mpsc buffer. Used by `send_history` to ensure all
    /// history messages are delivered regardless of conversation size.
    ///
    /// Returns `Err(())` only when the channel is closed (WS connection dead).
    pub(super) async fn send_ws_backpressure(&self, msg: WsServerMessage) -> Result<(), ()> {
        match self.ws_tx.reserve().await {
            Ok(permit) => {
                permit.send(msg);
                Ok(())
            }
            Err(_) => {
                warn!("WS send channel closed during back-pressured send");
                Err(())
            }
        }
    }

    /// Run the on-connect setup block: select the initial conversation, send
    /// history, send todo state, and eager-spawn CC if needed.
    ///
    /// Returns `()`. If the WS channel closes mid-history-delivery, `run_setup`
    /// returns early internally; the main event loop then drains and exits when
    /// the writer task drops the channel. Post-setup work in the caller
    /// (`record_ws_connect`, main loop entry) still runs on a dead socket, which
    /// is behaviorally harmless — see `event_loop.rs`.
    pub(super) async fn run_setup(
        &mut self,
        requested_conversation_id: Option<i64>,
        requested_last_seq: Option<i64>,
    ) {
        use brenn_lib::conversation;
        use brenn_lib::ws_types::CcState;

        let (singleton, multiuser, single_instance) = {
            let ac = self.app_config();
            (ac.singleton, ac.multiuser, ac.single_instance)
        };

        // Clone state Arc so closures below don't borrow `self`.
        let state = self.state.clone();

        if singleton {
            let conv = {
                let db_conn = state.db.lock().await;
                conversation::get_or_create_singleton_conversation(
                    &db_conn,
                    self.user_id,
                    &self.app_slug,
                )
            };

            let from_seq = if requested_conversation_id == Some(conv.id) {
                requested_last_seq
            } else {
                None
            };

            if let Some(bridge) = state.active_bridges.get(conv.id).await {
                self.attach_to_bridge(&bridge).await;
                let cc_state = bridge.resolve_cc_state().await;
                let _ = self.send_ws(self.conversation_switched_from_bridge(&bridge, cc_state));
                if self
                    .send_history(bridge.conversation_id, from_seq)
                    .await
                    .is_err()
                {
                    return;
                }
                if self
                    .send_pending_permissions_backpressure(&bridge)
                    .await
                    .is_err()
                {
                    return;
                }
            } else {
                self.current_conversation_id = Some(conv.id);
                let _ = self.send_ws(self.conversation_switched(Some(&conv), CcState::Connecting));
                if self.send_history(conv.id, from_seq).await.is_err() {
                    return;
                }
            }
        } else {
            let selected = if let Some(requested_id) = requested_conversation_id {
                match self
                    .try_select_requested_conversation(requested_id, multiuser, requested_last_seq)
                    .await
                {
                    Err(()) => {
                        // WS died during history delivery — abort setup entirely.
                        // Don't proceed to conversation list / todo state.
                        return;
                    }
                    Ok(result) => result,
                }
            } else {
                false
            };

            if !selected {
                if let Some((_conv_id, bridge)) = state
                    .active_bridges
                    .get_for_user(self.user_id, &self.app_slug, multiuser)
                    .await
                {
                    self.attach_to_bridge(&bridge).await;
                    let cc_state = bridge.resolve_cc_state().await;
                    let _ = self.send_ws(self.conversation_switched_from_bridge(&bridge, cc_state));
                    if self
                        .send_history(bridge.conversation_id, None)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if self
                        .send_pending_permissions_backpressure(&bridge)
                        .await
                        .is_err()
                    {
                        return;
                    }
                } else {
                    let auto_attached = if single_instance {
                        let app_bridges = state.active_bridges.get_for_app(&self.app_slug).await;
                        if let Some(bridge) = app_bridges.first() {
                            self.attach_to_bridge(bridge).await;
                            self.viewer_only = !(multiuser
                                && bridge.shared.load(std::sync::atomic::Ordering::Relaxed));
                            let cc_state = bridge.resolve_cc_state().await;
                            let _ = self
                                .send_ws(self.conversation_switched_from_bridge(bridge, cc_state));
                            if self
                                .send_history(bridge.conversation_id, None)
                                .await
                                .is_err()
                            {
                                return;
                            }
                            if self
                                .send_pending_permissions_backpressure(bridge)
                                .await
                                .is_err()
                            {
                                return;
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if !auto_attached {
                        let active_conv = {
                            let db_conn = state.db.lock().await;
                            conversation::get_active_conversation(
                                &db_conn,
                                self.user_id,
                                &self.app_slug,
                                multiuser,
                            )
                        };
                        if let Some(conv) = active_conv {
                            self.current_conversation_id = Some(conv.id);
                            let _ = self.send_ws(
                                self.conversation_switched(Some(&conv), CcState::Connecting),
                            );
                            if self.send_history(conv.id, None).await.is_err() {
                                return;
                            }
                        } else {
                            let _ = self.send_ws(self.conversation_switched(None, CcState::Idle));
                            let _ = self.send_ws(
                                brenn_lib::ws_types::WsServerMessage::HistoryComplete {
                                    oldest_loaded_seq: None,
                                },
                            );
                            let _ =
                                self.send_ws(brenn_lib::ws_types::WsServerMessage::ArtifactIndex {
                                    files: vec![],
                                });
                        }
                    }
                }
            }

            self.send_conversation_list().await;
        }

        // Send initial todo state after conversation selection.
        if let Some(config) = brenn_graf::graf_config(self.app_config()) {
            self.send_todo_state(config).await;
        }

        // Eager-spawn CC if not already attached to a live bridge.
        if self.broadcast_rx.is_none()
            && let Some(conv_id) = self.current_conversation_id
        {
            state.spawn_eager_wake(conv_id, self.timezone);
        }
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::auth::user::create_user;
    use brenn_lib::conversation;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::ws_types::{CcState, WsServerMessage};
    use tokio::sync::{broadcast, mpsc};

    use super::super::testing::*;
    use super::*;
    use crate::active_bridge::ActiveBridge;
    use crate::state::AppState;

    #[tokio::test]
    async fn mark_history_sent_preserves_last_sent_seq() {
        let (mut conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;
        conn.last_sent_seq = Some(42);
        conn.mark_history_already_sent();
        assert!(conn.history_sent, "history_sent must be true");
        assert_eq!(
            conn.last_sent_seq,
            Some(42),
            "last_sent_seq must be preserved unchanged"
        );
    }

    #[tokio::test]
    async fn mark_history_sent_preserves_none_last_sent_seq() {
        // For an empty conversation (no persisted rows), send_history leaves
        // last_sent_seq = None. mark_history_already_sent must not overwrite that.
        let (mut conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;
        conn.last_sent_seq = None;
        conn.mark_history_already_sent();
        assert!(conn.history_sent, "history_sent must be true");
        assert_eq!(
            conn.last_sent_seq, None,
            "last_sent_seq must remain None for empty conversation"
        );
    }

    #[tokio::test]
    async fn fetch_device_slug_returns_nonempty_for_seeded_device() {
        let (conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;
        let slug = conn.fetch_device_slug().await;
        assert!(
            !slug.is_empty(),
            "fetch_device_slug must return a non-empty slug"
        );
    }

    #[tokio::test]
    async fn attach_to_bridge_sends_presence_update() {
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);
        let (ws_tx, mut ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        let (user_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            (uid, did)
        };
        let conv_id = {
            let conn = db.lock().await;
            conversation::create_conversation(&conn, user_id, TEST_APP_SLUG, false)
        };

        let bridge = ActiveBridge::inject_for_test(
            user_id,
            conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );
        state.active_bridges.insert(conv_id, bridge.clone()).await;

        let mut conn = WsConnBuilder::with_defaults(
            user_id,
            TEST_USERNAME.to_string(),
            TEST_APP_SLUG.to_string(),
            ws_tx,
            state,
            device_id,
        )
        .build();

        conn.attach_to_bridge(&bridge).await;

        // Should have sent a PresenceUpdate to the connection.
        // attach_to_bridge is awaited in-process; PresenceUpdate is the last message it emits.
        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::PresenceUpdate { .. })
        })
        .await;
        let has_presence = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::PresenceUpdate { users, .. } if users.len() == 1 && users[0].username == TEST_USERNAME
            )
        });
        assert!(
            has_presence,
            "expected PresenceUpdate after attach, got: {msgs:?}"
        );

        // current_conversation_id should be set.
        assert_eq!(conn.current_conversation_id, Some(conv_id));
    }

    #[tokio::test]
    async fn detach_removes_subscriber() {
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);
        let (ws_tx, _ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        let (user_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            (uid, did)
        };
        let conv_id = {
            let conn = db.lock().await;
            conversation::create_conversation(&conn, user_id, TEST_APP_SLUG, false)
        };

        let bridge = ActiveBridge::inject_for_test(
            user_id,
            conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );
        state.active_bridges.insert(conv_id, bridge.clone()).await;

        let mut conn = WsConnBuilder::with_defaults(
            user_id,
            TEST_USERNAME.to_string(),
            TEST_APP_SLUG.to_string(),
            ws_tx,
            state,
            device_id,
        )
        .build();

        // Attach then detach.
        conn.attach_to_bridge(&bridge).await;
        assert!(conn.broadcast_rx.is_some());

        conn.detach().await;
        assert!(conn.broadcast_rx.is_none());
        // current_conversation_id is intentionally NOT cleared by detach.
        assert_eq!(conn.current_conversation_id, Some(conv_id));
    }

    #[tokio::test]
    async fn conversation_switched_helper_own_conversation() {
        let (conn, _ws_rx, _db, _alice, _bob, conv_id) = test_multiuser_conn_for_privacy().await;

        let conv = {
            let db_conn = conn.state.db.lock().await;
            conversation::get_conversation(&db_conn, conv_id)
        };

        let msg = conn.conversation_switched(Some(&conv), CcState::Idle);
        match msg {
            WsServerMessage::ConversationSwitched {
                conversation_id,
                state,
                is_owner,
                shared,
                reload,
            } => {
                assert_eq!(conversation_id, Some(conv_id));
                assert_eq!(state, CcState::Idle);
                assert!(is_owner, "alice owns this conversation");
                assert!(shared, "conversation was created shared");
                assert!(!reload, "normal switch should not set reload");
            }
            _ => panic!("expected ConversationSwitched"),
        }
    }

    #[tokio::test]
    async fn conversation_switched_helper_other_users_conversation() {
        let (mut conn, _ws_rx, _db, _alice, bob_id, conv_id) =
            test_multiuser_conn_for_privacy().await;

        // Pretend we're bob.
        conn.user_id = bob_id;

        let conv = {
            let db_conn = conn.state.db.lock().await;
            conversation::get_conversation(&db_conn, conv_id)
        };

        let msg = conn.conversation_switched(Some(&conv), CcState::Thinking);
        match msg {
            WsServerMessage::ConversationSwitched {
                is_owner, shared, ..
            } => {
                assert!(!is_owner, "bob doesn't own this conversation");
                assert!(shared, "conversation is shared");
            }
            _ => panic!("expected ConversationSwitched"),
        }
    }

    #[tokio::test]
    async fn conversation_switched_helper_none() {
        let (conn, _ws_rx, _db, _alice, _bob, _conv_id) = test_multiuser_conn_for_privacy().await;

        let msg = conn.conversation_switched(None, CcState::Idle);
        match msg {
            WsServerMessage::ConversationSwitched {
                conversation_id,
                is_owner,
                shared,
                ..
            } => {
                assert_eq!(conversation_id, None);
                assert!(is_owner, "vacuously true when no conversation");
                assert!(!shared, "vacuously false when no conversation");
            }
            _ => panic!("expected ConversationSwitched"),
        }
    }

    // --- SendResult / buffer-full recovery tests ---

    #[tokio::test]
    async fn send_ws_returns_full_when_buffer_at_capacity() {
        let (conn, _ws_rx, _db, _uid) = test_ws_conn_with_channel(1).await;

        // First send succeeds — fills the single slot.
        let r1 = conn.send_ws(WsServerMessage::HistoryComplete {
            oldest_loaded_seq: None,
        });
        assert_eq!(r1, SendResult::Ok);

        // Second send hits a full buffer.
        let r2 = conn.send_ws(WsServerMessage::HistoryComplete {
            oldest_loaded_seq: None,
        });
        assert_eq!(r2, SendResult::Full);
    }

    #[tokio::test]
    async fn send_ws_returns_closed_when_receiver_dropped() {
        let (conn, ws_rx, _db, _uid) = test_ws_conn_with_channel(1).await;

        // Drop receiver — simulates ws_writer task exiting.
        drop(ws_rx);

        let result = conn.send_ws(WsServerMessage::HistoryComplete {
            oldest_loaded_seq: None,
        });
        assert_eq!(result, SendResult::Closed);
    }

    #[tokio::test]
    async fn send_ws_backpressure_succeeds() {
        let (conn, mut ws_rx, _db, _uid) = test_ws_conn_with_channel(1).await;

        let result = conn
            .send_ws_backpressure(WsServerMessage::HistoryComplete {
                oldest_loaded_seq: None,
            })
            .await;
        assert!(result.is_ok());

        // Verify the message was received.
        let msg = ws_rx.try_recv().expect("should have received a message");
        assert!(matches!(msg, WsServerMessage::HistoryComplete { .. }));
    }

    #[tokio::test]
    async fn send_ws_backpressure_errors_when_closed() {
        let (conn, ws_rx, _db, _uid) = test_ws_conn_with_channel(1).await;
        drop(ws_rx);

        let result = conn
            .send_ws_backpressure(WsServerMessage::HistoryComplete {
                oldest_loaded_seq: None,
            })
            .await;
        assert!(result.is_err(), "should return Err when channel is closed");
    }

    #[tokio::test]
    async fn conversation_switched_reload_from_bridge_sets_reload() {
        let (conn, _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(16);
        let bridge =
            ActiveBridge::inject_for_test(conn.user_id, conv_id, "test", db.clone(), broadcast_tx);

        let msg = conn.conversation_switched_reload_from_bridge(&bridge, CcState::Thinking);
        match msg {
            WsServerMessage::ConversationSwitched {
                reload,
                conversation_id,
                ..
            } => {
                assert!(reload, "reload variant should set reload: true");
                assert_eq!(conversation_id, Some(conv_id));
            }
            _ => panic!("expected ConversationSwitched"),
        }

        // Compare with normal variant.
        let normal_msg = conn.conversation_switched_from_bridge(&bridge, CcState::Thinking);
        match normal_msg {
            WsServerMessage::ConversationSwitched { reload, .. } => {
                assert!(!reload, "normal variant should set reload: false");
            }
            _ => panic!("expected ConversationSwitched"),
        }
    }

    // -----------------------------------------------------------------------
    // extract_seq tests
    // -----------------------------------------------------------------------

    /// Seq-bearing variant with `seq: Some(n)` must return `Some(n)`.
    #[test]
    fn extract_seq_seq_bearing_with_some_returns_some() {
        let msg = WsServerMessage::AssistantMessage {
            content: "hello".into(),
            seq: Some(42),
        };
        assert_eq!(WsConnection::extract_seq(&msg), Some(42));
    }

    /// Seq-bearing variant with `seq: None` must return `None`.
    #[test]
    fn extract_seq_seq_bearing_with_none_returns_none() {
        let msg = WsServerMessage::AssistantMessage {
            content: "hello".into(),
            seq: None,
        };
        assert_eq!(WsConnection::extract_seq(&msg), None);
    }

    /// Non-seq-bearing variant must return `None`.
    #[test]
    fn extract_seq_non_seq_variant_returns_none() {
        let msg = WsServerMessage::Status {
            state: CcState::Idle,
        };
        assert_eq!(WsConnection::extract_seq(&msg), None);
    }

    /// All six seq-bearing variant arms must return `Some(n)` for `seq: Some(n)`.
    /// Tests each arm individually so a missing arm is caught immediately.
    #[test]
    fn extract_seq_all_seq_bearing_variants_covered() {
        use brenn_lib::ws_types::SystemMessageCategory;

        let cases: Vec<WsServerMessage> = vec![
            WsServerMessage::UserMessageEcho {
                text: "hi".into(),
                username: "bob".into(),
                timestamp: "2026-01-01T00:00:00+00:00".into(),
                attachments: vec![],
                selected_tasks: vec![],
                seq: Some(1),
            },
            WsServerMessage::SystemMessageBroadcast {
                rendered_html: "<p>sys</p>".into(),
                category: SystemMessageCategory::EventDrain,
                timestamp: "2026-01-01T00:00:00+00:00".into(),
                seq: Some(2),
            },
            WsServerMessage::AssistantMessage {
                content: "hello".into(),
                seq: Some(3),
            },
            WsServerMessage::ToolUseSummary {
                tool_name: "Read".into(),
                rendered_summary: "<div></div>".into(),
                detail_html: None,
                seq: Some(4),
            },
            WsServerMessage::ArtifactContent {
                file_path: "f.md".into(),
                rendered_html: "<p></p>".into(),
                raw_content: "# x".into(),
                snapshot: None,
                seq: Some(5),
            },
            WsServerMessage::TargetResult {
                target: "build".into(),
                success: true,
                summary: "ok".into(),
                detail: None,
                files: vec![],
                seq: Some(6),
            },
        ];
        // Assert each variant individually with a named label so a wrong arm is obvious.
        let names = [
            "UserMessageEcho",
            "SystemMessageBroadcast",
            "AssistantMessage",
            "ToolUseSummary",
            "ArtifactContent",
            "TargetResult",
        ];
        let expected_seqs: [i64; 6] = [1, 2, 3, 4, 5, 6];
        for ((msg, name), expected) in cases.iter().zip(names.iter()).zip(expected_seqs.iter()) {
            assert_eq!(
                WsConnection::extract_seq(msg),
                Some(*expected),
                "{name} must return Some({expected})"
            );
        }
    }

    /// `check_single_instance_blocked` must return `false` for a single-instance app
    /// when no other session is active (empty `active_bridges`).
    #[tokio::test]
    async fn check_single_instance_blocked_empty_bridges_returns_false() {
        let (conn, _ws_rx, _, _, _) =
            test_ws_conn_with_resume_conv_and_apps(test_apps_single_instance()).await;
        let blocked = conn.check_single_instance_blocked().await;
        assert!(!blocked, "must not be blocked when active_bridges is empty");
    }

    /// `check_single_instance_blocked` must return `true` for a single-instance app
    /// when any bridge already exists for the same app slug.
    /// Note: the function filters on app slug only, not on user_id; the bridge
    /// here belongs to a different user but that distinction is not part of the contract.
    #[tokio::test]
    async fn check_single_instance_blocked_existing_bridge_returns_true() {
        let (conn, mut ws_rx, db, _, _) =
            test_ws_conn_with_resume_conv_and_apps(test_apps_single_instance()).await;

        // Insert a bridge for a different user's conversation — same app slug.
        let other_user = {
            let c = db.lock().await;
            brenn_lib::auth::user::create_user(&c, "other_user", "$argon2id$fake")
        };
        let other_conv = {
            let c = db.lock().await;
            brenn_lib::conversation::create_conversation(&c, other_user, "test", false)
        };
        let (bcast_tx, _) = broadcast::channel::<WsServerMessage>(16);
        let other_bridge =
            ActiveBridge::inject_for_test(other_user, other_conv, "test", db.clone(), bcast_tx);
        conn.state
            .active_bridges
            .insert(other_conv, other_bridge)
            .await;

        let blocked = conn.check_single_instance_blocked().await;
        assert!(blocked, "must be blocked when another bridge exists");

        // Must have sent an AppBusy message.
        let msg = ws_rx.try_recv().expect("AppBusy must be sent");
        assert!(
            matches!(msg, WsServerMessage::AppBusy { .. }),
            "expected AppBusy, got: {msg:?}"
        );
    }

    // --- run_setup tests ---

    /// Singleton app: run_setup selects (or creates) the singleton conversation
    /// and sends ConversationSwitched followed by HistoryComplete.
    #[tokio::test]
    async fn run_setup_singleton_sends_conversation_switched_and_history() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps_singleton()).await;

        // Singleton path calls get_or_create_singleton_conversation which
        // creates a conversation when none exists. Ensure one exists so we
        // can assert the correct conversation_id is reported.
        let conv_id = {
            let c = db.lock().await;
            brenn_lib::conversation::create_conversation(&c, user_id, "test", false)
        };

        conn.run_setup(None, None).await;

        // Singleton path does not send ConversationList; ArtifactIndex is the last
        // message from send_history on this path (full replay, no pending tools).
        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ArtifactIndex { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ArtifactIndex"
        );

        // Must include a ConversationSwitched for the singleton conversation.
        let has_switched = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched { conversation_id: Some(id), .. }
                if *id == conv_id
            )
        });
        assert!(
            has_switched,
            "expected ConversationSwitched for conv {conv_id}; got: {msgs:?}"
        );

        // Must include HistoryComplete (from send_history on the full-replay path).
        let has_history_complete = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        assert!(
            has_history_complete,
            "expected HistoryComplete; got: {msgs:?}"
        );
    }

    /// Non-singleton app with no active bridge and no DB conversation:
    /// run_setup falls through to the DB lookup, finds nothing, and sends
    /// ConversationSwitched(None) + HistoryComplete.
    #[tokio::test]
    async fn run_setup_non_singleton_no_bridge_no_db_conv_sends_null_switched() {
        // Default test_apps() is non-singleton with no active bridge.
        let (mut conn, mut ws_rx, _, _) = test_ws_conn_for_app(test_apps()).await;

        conn.run_setup(None, None).await;

        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ConversationList { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ConversationList"
        );

        // Must include ConversationSwitched with conversation_id: None.
        let has_null_switched = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: None,
                    ..
                }
            )
        });
        assert!(
            has_null_switched,
            "expected ConversationSwitched(None); got: {msgs:?}"
        );

        // Must include HistoryComplete.
        let has_history_complete = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        assert!(
            has_history_complete,
            "expected HistoryComplete; got: {msgs:?}"
        );
    }

    /// Non-graf app emits no TodoState on connect.
    ///
    /// The `if let Some(config) = graf_config(...)` guard prevents `send_todo_state`
    /// from being called when the selected app has no graf integration. This test
    /// pins that invariant: a future call site added without the guard would emit
    /// a TodoState for a non-graf app, which would not be caught by the type system.
    #[tokio::test]
    async fn run_setup_non_graf_app_emits_no_todo_state() {
        // test_apps() has no graf integration configured.
        let (mut conn, mut ws_rx, _, _) = test_ws_conn_for_app(test_apps()).await;

        conn.run_setup(None, None).await;

        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ConversationList { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ConversationList"
        );
        let has_todo_state = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::TodoState { .. }));
        assert!(
            !has_todo_state,
            "non-graf app must not emit TodoState on connect; got: {msgs:?}"
        );
    }

    /// WS-closed early-return: when the ws_rx is dropped before run_setup,
    /// send_history fails on the first backpressure send (HistoryComplete),
    /// and run_setup returns early without calling send_todo_state.
    /// Verified by: run_setup completes without hang or panic, and no
    /// TodoState is received (channel is closed; any send would silently fail).
    #[tokio::test]
    async fn run_setup_ws_closed_returns_early_without_hang() {
        let (mut conn, ws_rx, db, user_id) = test_ws_conn_for_app(test_apps_singleton()).await;

        // Ensure a conversation exists so the singleton path proceeds to send_history.
        {
            let c = db.lock().await;
            brenn_lib::conversation::create_conversation(&c, user_id, "test", false);
        }

        // Drop the receiver — subsequent send_ws_backpressure calls will fail.
        drop(ws_rx);

        // run_setup must return without blocking; the early-return path fires
        // when send_history's HistoryComplete backpressure send fails.
        conn.run_setup(None, None).await;
        // Reaching here confirms no deadlock or panic.
    }

    // -----------------------------------------------------------------------
    // run_setup branch tests — ws-setup-branch-tests
    // -----------------------------------------------------------------------

    /// Branch: requested-id hit — `try_select_requested_conversation` finds the
    /// conversation owned by the user and returns `true`; run_setup does not fall
    /// through to auto-attach or DB lookup.
    #[tokio::test]
    async fn run_setup_requested_id_hit_selects_conversation() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps()).await;

        // Create a conversation belonging to the test user.
        let conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, user_id, TEST_APP_SLUG, false)
        };

        // run_setup with requested_conversation_id = the user's conversation.
        conn.run_setup(Some(conv_id), None).await;

        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ConversationList { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ConversationList"
        );

        // Must emit ConversationSwitched for the requested conversation.
        let switched = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: Some(id),
                    ..
                } if *id == conv_id
            )
        });
        assert!(
            switched,
            "expected ConversationSwitched for conv {conv_id}; got: {msgs:?}"
        );

        // Must emit HistoryComplete (history was sent for the selected conversation).
        let has_history = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        assert!(has_history, "expected HistoryComplete; got: {msgs:?}");

        // Connection's current_conversation_id must be updated.
        assert_eq!(
            conn.current_conversation_id,
            Some(conv_id),
            "current_conversation_id must be set to the requested id"
        );
    }

    /// Branch: auto-attach to user's own active bridge — no `requested_conversation_id`,
    /// but `active_bridges.get_for_user` finds a live bridge for this user.
    #[tokio::test]
    async fn run_setup_auto_attach_user_bridge() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps()).await;

        // Create a conversation and a live bridge for the user.
        // complete_conversation is called to reflect a realistic production state: active
        // bridges often run for conversations that are DB-complete. The auto-attach path
        // below uses active_bridges (in-memory lookup), not DB conversation status, so
        // the DB status has no effect on this branch — unlike the DB-fallback test which
        // explicitly must NOT call complete_conversation (see run_setup_db_fallback_with_stored_conversation).
        let conv_id = {
            let c = db.lock().await;
            let cid = conversation::create_conversation(&c, user_id, TEST_APP_SLUG, false);
            conversation::complete_conversation(&c, cid, None);
            cid
        };
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);
        let bridge = ActiveBridge::inject_for_test(
            user_id,
            conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(conv_id, bridge.clone())
            .await;

        // run_setup with no requested conversation; should find the live bridge.
        conn.run_setup(None, None).await;

        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ConversationList { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ConversationList"
        );

        // Must include ConversationSwitched for the bridge's conversation.
        let switched = msgs.iter().find(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: Some(id),
                    ..
                } if *id == conv_id
            )
        });
        assert!(
            switched.is_some(),
            "expected ConversationSwitched for bridge conv {conv_id}; got: {msgs:?}"
        );

        // The bridge path derives cc_state from the live bridge; a fresh test bridge has
        // no pending permissions, is_alive()=false, and is_cc_idle()=true → Idle.
        assert!(
            matches!(
                switched.unwrap(),
                WsServerMessage::ConversationSwitched {
                    state: CcState::Idle,
                    ..
                }
            ),
            "bridge auto-attach must emit CcState::Idle for a fresh test bridge; got: {msgs:?}"
        );

        // Connection must be attached to the bridge (broadcast_rx is set).
        assert!(
            conn.broadcast_rx.is_some(),
            "connection must be attached to the bridge"
        );
    }

    /// Branch: single-instance auto-attach — `single_instance` is true and
    /// `active_bridges.get_for_app` finds a bridge for another user; this
    /// connection attaches as a viewer.
    #[tokio::test]
    async fn run_setup_single_instance_auto_attach() {
        let (mut conn, mut ws_rx, db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        // Create a bridge belonging to a different user for the same app.
        // complete_conversation reflects a realistic production state; the single-instance
        // auto-attach path uses active_bridges (in-memory), not DB conversation status.
        // Both DB operations share one lock acquisition (no need to release and re-acquire).
        let (other_user, other_conv) = {
            let c = db.lock().await;
            let uid = create_user(&c, "other_user", "$argon2id$fake");
            let cid = conversation::create_conversation(&c, uid, TEST_APP_SLUG, false);
            conversation::complete_conversation(&c, cid, None);
            (uid, cid)
        };
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);
        let other_bridge = ActiveBridge::inject_for_test(
            other_user,
            other_conv,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(other_conv, other_bridge.clone())
            .await;

        // run_setup with no requested conversation; single_instance → get_for_app fires.
        conn.run_setup(None, None).await;

        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ConversationList { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ConversationList"
        );

        // Must emit ConversationSwitched for the other user's conversation.
        let switched = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: Some(id),
                    ..
                } if *id == other_conv
            )
        });
        assert!(
            switched,
            "expected ConversationSwitched for single-instance bridge conv {other_conv}; got: {msgs:?}"
        );

        // Connection must be attached to the other bridge (broadcast_rx is set).
        assert!(
            conn.broadcast_rx.is_some(),
            "single-instance auto-attach must set broadcast_rx"
        );

        // viewer_only must be true for a non-multiuser single_instance app: the connecting
        // user is not the conversation owner, so the viewer-only flag protects them from
        // sending messages or triggering CC actions.
        assert!(
            conn.viewer_only,
            "single-instance non-multiuser attachment must set viewer_only = true"
        );
    }

    /// Branch: DB fallback — no active bridge, but `get_active_conversation` finds
    /// a stored conversation; run_setup emits ConversationSwitched with the existing
    /// conv_id (and CcState::Connecting because no bridge is live).
    #[tokio::test]
    async fn run_setup_db_fallback_with_stored_conversation() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps()).await;

        // Create an active (non-completed) conversation — get_active_conversation
        // filters for status = 'active', so we must NOT call complete_conversation.
        let conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, user_id, TEST_APP_SLUG, false)
        };

        // No active bridge is registered — all bridges absent.
        conn.run_setup(None, None).await;

        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ConversationList { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ConversationList"
        );

        // Must emit ConversationSwitched for the stored conversation.
        let switched = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: Some(id),
                    ..
                } if *id == conv_id
            )
        });
        assert!(
            switched,
            "expected ConversationSwitched for DB conv {conv_id}; got: {msgs:?}"
        );

        // Must emit HistoryComplete (history was sent for the fallback conversation).
        let has_history = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        assert!(has_history, "expected HistoryComplete; got: {msgs:?}");

        // No live bridge — broadcast_rx must remain None.
        assert!(
            conn.broadcast_rx.is_none(),
            "DB-fallback path must not set broadcast_rx"
        );
    }

    /// Branch: requested-id miss — `try_select_requested_conversation` returns false
    /// (conversation does not exist or does not belong to the user); run_setup falls
    /// through to auto-attach then DB lookup, finds nothing, and emits
    /// ConversationSwitched(None).
    #[tokio::test]
    async fn run_setup_requested_id_miss_falls_through_to_null_switched() {
        let (mut conn, mut ws_rx, _, _) = test_ws_conn_for_app(test_apps()).await;

        // Use a conversation id that has never been created — guaranteed miss.
        let nonexistent_id: i64 = 99_999_999;

        // No active bridge, no DB conversation — fall-through ends at no-conversation branch.
        conn.run_setup(Some(nonexistent_id), None).await;

        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ConversationList { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ConversationList"
        );

        // Must emit ConversationSwitched(None) — the missed id must not be selected.
        let switched_none = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: None,
                    ..
                }
            )
        });
        assert!(
            switched_none,
            "requested-id miss must fall through to ConversationSwitched(None); got: {msgs:?}"
        );

        // Must not emit ConversationSwitched for the nonexistent id.
        let switched_bad = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: Some(id),
                    ..
                } if *id == nonexistent_id
            )
        });
        assert!(
            !switched_bad,
            "must not select the nonexistent conversation; got: {msgs:?}"
        );
    }

    /// Branch: no-conversation-at-all — no active bridge and no DB conversation;
    /// run_setup emits ConversationSwitched(None) + empty HistoryComplete + empty ArtifactIndex.
    /// This is the cold-start path for a user who has never used the app.
    #[tokio::test]
    async fn run_setup_no_conversation_emits_null_switched_and_empty_history() {
        let (mut conn, mut ws_rx, _, _) = test_ws_conn_for_app(test_apps()).await;

        // No active bridge, no conversation in the DB.
        conn.run_setup(None, None).await;

        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ConversationList { .. })
        })
        .await;
        assert!(
            ws_rx.try_recv().is_err(),
            "unexpected extra message after ConversationList"
        );

        // Must emit ConversationSwitched(None).
        let switched_none = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: None,
                    ..
                }
            )
        });
        assert!(
            switched_none,
            "cold-start must emit ConversationSwitched(None); got: {msgs:?}"
        );

        // Must emit HistoryComplete with no oldest_loaded_seq (empty history).
        let has_empty_history = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::HistoryComplete {
                    oldest_loaded_seq: None
                }
            )
        });
        assert!(
            has_empty_history,
            "cold-start must emit HistoryComplete{{None}}; got: {msgs:?}"
        );

        // Must emit ArtifactIndex with no files.
        let has_empty_artifacts = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ArtifactIndex { files } if files.is_empty()));
        assert!(
            has_empty_artifacts,
            "cold-start must emit ArtifactIndex{{files: []}}; got: {msgs:?}"
        );
    }

    /// `detach()` must NOT clear `last_sent_seq`. Clearing it would silently
    /// regress incremental replay (BridgeSpawned path) to full replay on every
    /// CC restart for persistent apps. This test pins the invariant.
    #[tokio::test]
    async fn detach_preserves_last_sent_seq() {
        let (mut conn, _ws_rx, db, user_id) = test_ws_conn_with_channel(256).await;

        let conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, user_id, "test", false)
        };

        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);
        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        conn.state
            .active_bridges
            .insert(conv_id, bridge.clone())
            .await;
        conn.current_conversation_id = Some(conv_id);
        conn.last_sent_seq = Some(42);
        conn.history_sent = true;

        conn.attach_to_bridge(&bridge).await;
        assert!(conn.broadcast_rx.is_some());

        // Simulate CC crash: detach() is called when BroadcastResult::Closed.
        conn.detach().await;

        // The critical invariant: last_sent_seq must survive detach().
        // A regression that clears it would silently downgrade to full replay.
        assert_eq!(
            conn.last_sent_seq,
            Some(42),
            "detach() must NOT clear last_sent_seq — clearing it would cause full replay on CC restart"
        );
        // history_sent is cleared by detach (expected — next attach re-sends history).
        assert!(!conn.history_sent, "detach() must clear history_sent");
    }

    #[tokio::test]
    async fn send_models_sends_to_matching_app() {
        let (conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Inject cached models for the connection's app ("test").
        conn.state
            .cached_models
            .write()
            .await
            .insert("test".to_string(), test_model_infos());

        conn.send_models_if_app_populated().await;

        // send_models_if_app_populated is awaited in-process; ModelsAvailable is
        // its only message and serves as the sentinel.
        let msgs = collect_until(&mut ws_rx, |m| {
            matches!(m, WsServerMessage::ModelsAvailable { .. })
        })
        .await;
        let found = msgs.iter().find_map(|m| {
            if let WsServerMessage::ModelsAvailable { available_models } = m {
                Some(available_models)
            } else {
                None
            }
        });
        let models = found.expect("expected ModelsAvailable, got: {msgs:?}");
        assert_eq!(
            models.len(),
            2,
            "expected exactly 2 models, got: {models:?}"
        );
        assert_eq!(models[0].value, "sonnet", "first model must be sonnet");
        assert_eq!(models[1].value, "opus", "second model must be opus");
    }

    #[tokio::test]
    async fn send_models_skips_when_app_has_no_cache_entry() {
        let (conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // No entry inserted — cached_models is empty, as in "bridge not yet spawned".
        conn.send_models_if_app_populated().await;

        // send_models_if_app_populated is awaited in-process and emits nothing
        // when no cache entry exists — check immediately without a timed drain.
        assert!(
            ws_rx.try_recv().is_err(),
            "must not send ModelsAvailable when no cache entry exists"
        );
    }

    #[tokio::test]
    async fn send_models_skips_empty_cache() {
        let (conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Insert an empty model list — should be treated as "no models yet".
        conn.state
            .cached_models
            .write()
            .await
            .insert("test".to_string(), vec![]);

        conn.send_models_if_app_populated().await;

        // send_models_if_app_populated is awaited in-process and emits nothing
        // when the cached model list is empty — check immediately without a timed drain.
        assert!(
            ws_rx.try_recv().is_err(),
            "must not send ModelsAvailable when cached model list is empty"
        );
    }

    // -----------------------------------------------------------------------
    // ClientErrorBucket tests
    // -----------------------------------------------------------------------

    /// A dummy IP for unit tests — the IP value does not affect bucket logic,
    /// only log output.
    const TEST_IP: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 1));

    /// Calling `try_consume` up to `BURST_CAP` times all return `true`.
    /// The next call returns `false` (bucket exhausted).
    #[test]
    fn client_error_bucket_allows_up_to_burst_cap() {
        let mut bucket = ClientErrorBucket::new();
        for i in 0..BURST_CAP {
            assert!(
                bucket.try_consume(TEST_IP),
                "call {i} (0-indexed) should return true (within burst cap)"
            );
        }
        assert!(
            !bucket.try_consume(TEST_IP),
            "call {} (0-indexed) should return false (bucket exhausted)",
            BURST_CAP
        );
    }

    /// After exhausting the bucket, advancing time by one `REFILL_INTERVAL`
    /// causes the next `try_consume` to return `true`.
    #[tokio::test]
    async fn client_error_bucket_refills_after_interval() {
        tokio::time::pause();

        let mut bucket = ClientErrorBucket::new();

        // Exhaust all tokens.
        for _ in 0..BURST_CAP {
            bucket.try_consume(TEST_IP);
        }
        assert!(!bucket.try_consume(TEST_IP), "bucket must be exhausted");

        // Advance by exactly one refill interval.
        tokio::time::advance(REFILL_INTERVAL).await;

        assert!(
            bucket.try_consume(TEST_IP),
            "one token should be available after one refill interval"
        );
    }

    /// Waiting for many refill intervals must cap at `BURST_CAP`, not accumulate
    /// unbounded tokens.
    #[tokio::test]
    async fn client_error_bucket_caps_at_burst() {
        tokio::time::pause();

        let mut bucket = ClientErrorBucket::new();

        // Exhaust all tokens.
        for _ in 0..BURST_CAP {
            bucket.try_consume(TEST_IP);
        }
        assert!(
            !bucket.try_consume(TEST_IP),
            "precondition: bucket exhausted"
        );

        // Advance by 2× BURST_CAP intervals — would give 2×BURST_CAP tokens if uncapped.
        tokio::time::advance(REFILL_INTERVAL * (2 * BURST_CAP)).await;

        // Should be capped at BURST_CAP, not 2×BURST_CAP.
        for i in 0..BURST_CAP {
            assert!(
                bucket.try_consume(TEST_IP),
                "call {i} after long wait should return true"
            );
        }
        assert!(
            !bucket.try_consume(TEST_IP),
            "should be exhausted again after consuming exactly BURST_CAP tokens"
        );
    }

    /// Integration-level: send BURST_CAP + 5 ClientError messages through
    /// `handle_client_message`. Assert no panic, that `suppressed_count` is 5,
    /// and that `logged_suppression` is set (log-once invariant holds).
    ///
    /// Time is paused so no refill occurs between messages, making the
    /// rate-limited state structurally guaranteed rather than relying on real
    /// time not advancing a full REFILL_INTERVAL during the test.
    #[tokio::test]
    async fn dispatch_client_error_suppressed_after_burst() {
        tokio::time::pause();
        let (mut conn, _ws_rx, _db, _uid, _conv_id) =
            super::super::testing::test_ws_conn_with_resume_conv().await;

        let msg_json = r#"{"type":"ClientError","message":"test error"}"#;
        let extra = 5u32;
        let total = BURST_CAP + extra;

        for _ in 0..total {
            super::super::dispatch::handle_client_message(
                msg_json,
                &mut conn,
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            )
            .await;
        }

        assert_eq!(
            conn.client_error_bucket.suppressed(),
            u64::from(extra),
            "suppressed count must equal the number of messages sent beyond BURST_CAP"
        );
        assert!(
            conn.client_error_bucket.in_suppression(),
            "suppression window must be open after the first suppressed message"
        );
    }

    /// After suppression, a refilled token clears `suppressed_count` and
    /// resets `logged_suppression` so the next suppression window logs again.
    #[tokio::test]
    async fn dispatch_client_error_transition_out_resets_state() {
        tokio::time::pause();
        let (mut conn, _ws_rx, _db, _uid, _conv_id) =
            super::super::testing::test_ws_conn_with_resume_conv().await;

        let msg_json = r#"{"type":"ClientError","message":"test error"}"#;
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

        // Exhaust all tokens.
        for _ in 0..=BURST_CAP {
            super::super::dispatch::handle_client_message(msg_json, &mut conn, ip).await;
        }
        assert!(conn.client_error_bucket.suppressed() > 0);
        assert!(conn.client_error_bucket.in_suppression());

        // Advance time to refill one token, then consume it.
        tokio::time::advance(REFILL_INTERVAL).await;
        super::super::dispatch::handle_client_message(msg_json, &mut conn, ip).await;

        // Transition-out must reset both counters.
        assert_eq!(
            conn.client_error_bucket.suppressed(),
            0,
            "suppressed count must reset to 0 after transition out"
        );
        assert!(
            !conn.client_error_bucket.in_suppression(),
            "suppression window must be closed after transition out"
        );
    }

    // ── TZ override read-site tests: build_graf_env and today_in_connection_tz ──

    /// `build_graf_env` honours an active `tz_override` — emits the override zone,
    /// not the connection's browser TZ.
    #[tokio::test]
    async fn build_graf_env_honours_tz_override() {
        let (conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;
        // Browser TZ is UTC (set in WsConnBuilder::build).
        assert_eq!(conn.timezone, chrono_tz::Tz::UTC);

        // Write the override directly so we don't need a bridge.
        {
            let db_conn = conn.state.db.lock().await;
            brenn_lib::auth::device::set_tz_override(
                &db_conn,
                conn.device_id,
                conn.user_id,
                Some("Asia/Tokyo"),
                None,
            );
        }

        let env = conn.build_graf_env().await;
        let tz_val = env
            .iter()
            .find(|(k, _)| k == "GRAF_USER_TZ")
            .map(|(_, v)| v.as_str())
            .expect("GRAF_USER_TZ must be present");
        assert_eq!(
            tz_val, "Asia/Tokyo",
            "GRAF_USER_TZ must reflect the override, not the browser UTC"
        );
    }

    /// After clearing the override, `build_graf_env` reverts to the browser TZ.
    #[tokio::test]
    async fn build_graf_env_reverts_after_clear() {
        let (conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Set then clear.
        {
            let db_conn = conn.state.db.lock().await;
            brenn_lib::auth::device::set_tz_override(
                &db_conn,
                conn.device_id,
                conn.user_id,
                Some("Asia/Tokyo"),
                None,
            );
        }
        {
            let db_conn = conn.state.db.lock().await;
            brenn_lib::auth::device::set_tz_override(
                &db_conn,
                conn.device_id,
                conn.user_id,
                None,
                None,
            );
        }

        let env = conn.build_graf_env().await;
        let tz_val = env
            .iter()
            .find(|(k, _)| k == "GRAF_USER_TZ")
            .map(|(_, v)| v.as_str())
            .expect("GRAF_USER_TZ must be present");
        assert_eq!(
            tz_val, "UTC",
            "GRAF_USER_TZ must revert to browser UTC after clear"
        );
    }

    /// `today_in_connection_tz` returns the date in the override zone, not browser TZ.
    ///
    /// Uses two zones that can produce different dates at the test-execution instant
    /// *only* if we run exactly at the UTC midnight boundary — highly unlikely, and
    /// the test is still sound as a coverage assertion. We check that the returned
    /// date matches chrono's own computation in the override zone.
    #[tokio::test]
    async fn today_in_connection_tz_honours_tz_override() {
        let (conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Set override to Asia/Tokyo.
        {
            let db_conn = conn.state.db.lock().await;
            brenn_lib::auth::device::set_tz_override(
                &db_conn,
                conn.device_id,
                conn.user_id,
                Some("Asia/Tokyo"),
                None,
            );
        }

        // Bracket the call with before/after snapshots so the assertion accepts
        // either bound — avoids a spurious failure if the test crosses UTC midnight
        // (which is 09:00 JST; unlikely but possible on slow CI runners).
        let before = chrono::Utc::now();
        let today = conn.today_in_connection_tz().await;
        let after = chrono::Utc::now();
        let date_before = before.with_timezone(&chrono_tz::Asia::Tokyo).date_naive();
        let date_after = after.with_timezone(&chrono_tz::Asia::Tokyo).date_naive();
        assert!(
            today == date_before || today == date_after,
            "today_in_connection_tz must return today in Asia/Tokyo; \
             got {today}, before={date_before}, after={date_after}"
        );
    }

    /// With an expired override, `today_in_connection_tz` reverts to browser TZ.
    #[tokio::test]
    async fn today_in_connection_tz_uses_browser_tz_when_override_expired() {
        let (conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Set override with a past expiry.
        {
            let db_conn = conn.state.db.lock().await;
            brenn_lib::auth::device::set_tz_override(
                &db_conn,
                conn.device_id,
                conn.user_id,
                Some("Asia/Tokyo"),
                Some(1), // epoch second 1 → long past
            );
        }

        let today = conn.today_in_connection_tz().await;
        let expected = chrono::Utc::now()
            .with_timezone(&chrono_tz::Tz::UTC)
            .date_naive();
        assert_eq!(
            today, expected,
            "today_in_connection_tz must use browser TZ (UTC) when override is expired"
        );
    }
}

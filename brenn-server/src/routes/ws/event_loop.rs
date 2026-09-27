//! WebSocket event loop: `handle_ws`, `ws_writer`, `recv_broadcast`, `BroadcastResult`.

use std::net::IpAddr;

use axum::extract::ws::{Message, WebSocket};
use brenn_db::auth::session::Session;
use brenn_db::conversation;
use brenn_usage_db as usage;
use brenn_ws_types::{CcState, ViewportClass, WsServerMessage};
use futures::{SinkExt, StreamExt};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use super::connection::{SendResult, WsConnection};
use super::dispatch::handle_client_message;
use crate::state::AppState;

/// Result of trying to receive from the broadcast channel.
enum BroadcastResult {
    Message(WsServerMessage),
    Lagged(u64),
    Closed,
    NoBroadcast,
}

/// Receive from broadcast, handling the case where there's no active subscription.
async fn recv_broadcast(rx: &mut Option<broadcast::Receiver<WsServerMessage>>) -> BroadcastResult {
    let Some(rx) = rx.as_mut() else {
        // No broadcast subscription. Pend forever until cancelled by select!.
        std::future::pending::<()>().await;
        return BroadcastResult::NoBroadcast;
    };

    match rx.recv().await {
        Ok(msg) => BroadcastResult::Message(msg),
        Err(broadcast::error::RecvError::Lagged(n)) => BroadcastResult::Lagged(n),
        Err(broadcast::error::RecvError::Closed) => BroadcastResult::Closed,
    }
}

/// Write WsServerMessages to the WebSocket sink.
///
/// The channel closing is the teardown, and the sink is closed rather than
/// dropped: closing flushes a Close frame — the reply queued when the client
/// sent one, or a server-initiated one — so the peer sees the closing handshake
/// instead of a connection reset. A peer that has already gone is the ordinary
/// case here, not an error.
async fn ws_writer(
    mut sink: futures::stream::SplitSink<WebSocket, Message>,
    mut rx: mpsc::Receiver<WsServerMessage>,
) {
    while let Some(msg) = rx.recv().await {
        let json = serde_json::to_string(&msg).expect("WsServerMessage serialization");
        if let Err(e) = sink.send(Message::Text(json.into())).await {
            warn!("WS write failed: {e}");
            break;
        }
    }
    if let Err(e) = sink.close().await {
        debug!("WS close handshake did not complete: {e}");
    }
}

/// Handshake parameters extracted by `ws_handler` from Axum extractors and
/// passed to `handle_ws` as a single bundle. Eliminates the 9-argument
/// positional signature and the `#[allow(clippy::too_many_arguments)]` waiver.
pub(super) struct WsHandshake {
    pub(super) socket: WebSocket,
    pub(super) session: Session,
    pub(super) client_ip: IpAddr,
    pub(super) state: AppState,
    pub(super) app_slug: String,
    pub(super) requested_conversation_id: Option<i64>,
    pub(super) requested_last_seq: Option<i64>,
    pub(super) viewport_class: ViewportClass,
    pub(super) device_id: i64,
}

pub(super) async fn handle_ws(hs: WsHandshake) {
    let WsHandshake {
        socket,
        session,
        client_ip,
        state,
        app_slug,
        requested_conversation_id,
        requested_last_seq,
        viewport_class,
        device_id,
    } = hs;
    let (ws_sink, mut ws_stream) = socket.split();

    // Channel for server → browser messages (per-tab).
    let (ws_tx, ws_rx) = mpsc::channel::<WsServerMessage>(256);

    // Spawn the WS writer task.
    let writer_handle = tokio::spawn(ws_writer(ws_sink, ws_rx));

    let app_config = state
        .apps
        .get(&app_slug)
        .unwrap_or_else(|| panic!("app {app_slug:?} not found in config"));
    let multiuser = app_config.multiuser;

    let mut conn = WsConnection {
        user_id: session.user.id,
        username: session.user.username.clone(),
        app_slug,
        client_ip,
        current_conversation_id: None,
        broadcast_rx: None,
        ws_tx: ws_tx.clone(),
        state: state.clone(),
        viewer_only: false,
        timezone: chrono_tz::Tz::UTC,
        viewport_class,
        device_id,
        bridge_notify_rx: state.bridge_notify_tx.subscribe(),
        apps_swapped_rx: state.apps_swapped_tx.subscribe(),
        history_sent: false,
        last_sent_seq: None,
        queued_responses: Vec::new(),
        oldest_loaded_seq: None,
        client_error_bucket: super::connection::ClientErrorBucket::new(),
        #[cfg(test)]
        test_bridge: None,
    };

    // Send Welcome as the very first message — gives the frontend its identity.
    let available_models = conn.available_models().await;
    let default_model = conn.app_config().model.clone();
    let attachment_targets: Vec<brenn_ws_types::TargetInfo> = conn
        .app_config()
        .attachment_targets
        .iter()
        .map(|t| brenn_ws_types::TargetInfo {
            name: t.name.clone(),
            label: t.label.clone(),
            accept: t.accept.clone(),
            multi: t.multi,
        })
        .collect();
    let singleton = app_config.singleton;
    let _ = conn.send_ws(WsServerMessage::Welcome {
        username: session.user.username.clone(),
        user_id: session.user.id,
        multiuser,
        singleton,
        available_models,
        default_model,
        attachment_targets,
        pwa_push_enabled: app_config.pwa_push_enabled(),
    });

    // Emit current subscription state so the frontend initializes correctly
    // without waiting for an explicit subscribe/unsubscribe action.
    if app_config.pwa_push_enabled() {
        let enabled = {
            let db_conn = state.db.lock().await;
            brenn_pwa_push::db::subscription_exists(&db_conn, conn.device_id, session.user.id)
        };
        let _ = conn.send_ws(WsServerMessage::PushEnabled { enabled });
    }

    // SetLayout must precede ConversationSwitched and any history frames:
    // the frontend gates rendering on this message.
    conn.send_layout().await;

    // On connect: select the initial conversation, send history, send todo state,
    // and eager-spawn CC. Returns early internally if the WS channel closes
    // mid-delivery; in that case the main loop below drains and exits immediately.
    conn.run_setup(requested_conversation_id, requested_last_seq)
        .await;

    // Record the WS connection as a usage event.
    {
        let db_conn = conn.state.db.lock().await;
        usage::record_ws_connect(
            &db_conn,
            conn.device_id,
            conn.user_id,
            &conn.app_slug,
            conn.current_conversation_id,
            conn.state.usage_session_gap_secs,
        );
    }

    // Reload state for mpsc buffer-full recovery. When the per-tab mpsc fills up
    // during broadcast forwarding, we defer a full history reload until the buffer
    // drains (via reserve()). See docs/designs/ws-buffer-full-recovery.md.
    let mut reload_pending = false;

    // Main loop: read from WS and broadcast concurrently.
    loop {
        tokio::select! {
            // WS message from browser.
            ws_msg = ws_stream.next() => {
                match ws_msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_client_message(&text, &mut conn, client_ip).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!(user = %session.user.username, "WebSocket closed");
                        break;
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                        // Protocol-level ping/pong handled automatically by axum.
                    }
                    Some(Ok(Message::Binary(_))) => {
                        brenn_obs::security::log_and_alert_security_event(
                            &state.alert_dispatcher,
                            brenn_obs::security::SecurityEventType::SchemaViolation,
                            client_ip,
                            &format!("binary WS frame from user {}", session.user.username),
                        );
                        let db_conn = state.db.lock().await;
                        brenn_db::auth::session::delete_session(&db_conn, &session.token);
                        break;
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error: {e}");
                        break;
                    }
                }
            }
            // Broadcast event from active bridge.
            broadcast_msg = recv_broadcast(&mut conn.broadcast_rx) => {
                match broadcast_msg {
                    BroadcastResult::Message(msg) => {
                        if reload_pending {
                            // Discard broadcast messages while reload is pending —
                            // they're stale; fresh history will follow.
                        } else {
                            // Track the highest seq forwarded via live broadcast so
                            // BridgeSpawned incremental re-replay stays current.
                            // Update AFTER send_ws so last_sent_seq never advances
                            // past what the tab has actually received: if send_ws
                            // returns Full the message is dropped and reload_pending
                            // is set; last_sent_seq must not advance past the dropped
                            // message or BridgeSpawned incremental re-replay would
                            // skip it. (The reload path re-sends full history anyway,
                            // but the invariant is load-bearing for future callers.)
                            let msg_seq = WsConnection::extract_seq(&msg);
                            if let Some(msg_seq) = msg_seq {
                                // DEBUG on dedup: this is expected behavior when live
                                // broadcast and incremental re-replay overlap. WARN would
                                // be fail2ban signal per CLAUDE.md; this is not an anomaly.
                                if let Some(last) = conn.last_sent_seq
                                    && msg_seq <= last
                                {
                                    tracing::debug!(
                                        seq = msg_seq,
                                        last_sent_seq = last,
                                        "duplicate seq on live broadcast — frontend will dedup"
                                    );
                                }
                            }
                            let send_result = conn.send_ws(msg);
                            // Advance cursor only on successful enqueue.
                            if send_result != SendResult::Full
                                && let Some(msg_seq) = msg_seq
                            {
                                conn.last_sent_seq = Some(
                                    conn.last_sent_seq.map_or(msg_seq, |last| last.max(msg_seq)),
                                );
                            }
                            if send_result == SendResult::Full {
                                // Per-tab mpsc buffer overflowed. Defer a full history
                                // reload until the buffer drains.
                                warn!("per-tab mpsc buffer full, deferring history reload");
                                reload_pending = true;
                                // Re-subscribe to skip any queued broadcast messages.
                                if let Some(conv_id) = conn.current_conversation_id
                                    && let Some(bridge) = state.active_bridges.get(conv_id).await
                                {
                                    conn.broadcast_rx = Some(bridge.subscribe());
                                }
                            }
                        }
                    }
                    BroadcastResult::Lagged(n) => {
                        warn!("broadcast lagged by {n} messages, sending full history reload");
                        reload_pending = true;
                        // Re-subscribe to the broadcast channel.
                        if let Some(conv_id) = conn.current_conversation_id
                            && let Some(bridge) = state.active_bridges.get(conv_id).await
                        {
                            conn.broadcast_rx = Some(bridge.subscribe());
                        }
                    }
                    BroadcastResult::Closed => {
                        reload_pending = false;
                        if conn.on_bridge_closed().await.is_err() {
                            // WS channel closed — connection dead.
                            break;
                        }
                    }
                    BroadcastResult::NoBroadcast => {
                        // No active broadcast — shouldn't happen in select!, but safe.
                    }
                }
            }
            // Buffer drain: when a reload is pending, wait for the mpsc to have
            // space, then send ConversationSwitched(reload) + full history.
            // send_history uses back-pressure, so it always completes unless
            // the WS channel closes (connection dead).
            Ok(permit) = ws_tx.reserve(), if reload_pending => {
                drop(permit); // Free the slot — send_ws will re-acquire via try_send.
                if let Some(conv_id) = conn.current_conversation_id {
                    // Build and send the ConversationSwitched(reload: true).
                    if let Some(bridge) = state.active_bridges.get(conv_id).await {
                        let _ = conn.send_ws(conn.conversation_switched_reload_from_bridge(&bridge, CcState::Thinking));
                    } else {
                        let conv = {
                            let db_conn = state.db.lock().await;
                            conversation::get_conversation_opt(&db_conn, conv_id)
                        };
                        let (is_owner, shared) = conv
                            .map(|c| (c.user_id == conn.user_id, c.shared))
                            .unwrap_or((true, false));
                        let _ = conn.send_ws(WsServerMessage::ConversationSwitched {
                            conversation_id: Some(conv_id),
                            state: CcState::Thinking,
                            is_owner,
                            shared,
                            reload: true,
                        });
                    }
                    if conn.send_history(conv_id, None).await.is_err() {
                        // WS channel closed — connection dead.
                        break;
                    }
                }
                reload_pending = false;
            }
            // A reload installed a new agent map. Ask the connect-time
            // question again: `allowed_users` converges with the swap, and a
            // socket opened under the old document is the only place a user
            // the new one denies is still holding authority.
            swapped = conn.apps_swapped_rx.recv() => {
                if !conn.survives_apps_swap(&session.user.username, client_ip, swapped) {
                    break;
                }
            }
            // Bridge spawn notification — another connection spawned a bridge
            // for a conversation we might be viewing.
            notification = conn.bridge_notify_rx.recv() => {
                match notification {
                    Ok(crate::state::BridgeSpawned { conversation_id, app_slug }) => {
                        if conn.follow_singleton_on_spawn(conversation_id, &app_slug).await.is_err() {
                            // WS channel closed — connection dead.
                            break;
                        }
                        // Auto-attach if we're viewing this conversation but have no bridge.
                        if conn.current_conversation_id == Some(conversation_id)
                            && conn.broadcast_rx.is_none()
                            && let Some(bridge) = state.active_bridges.get(conversation_id).await
                        {
                            conn.attach_to_bridge(&bridge).await;

                            let cc_state = bridge.resolve_cc_state().await;

                            if conn.history_sent {
                                // History was already sent on this connect (eager spawn
                                // from connect/switch). Perform an incremental re-replay
                                // from `last_sent_seq` to pick up any rows written by
                                // `drain_pending_events` after the initial `send_history`
                                // call — closing the wake-spawn race.
                                //
                                // If last_sent_seq is current (live broadcasts tracked it
                                // without dropping), this returns 0 rows. If drain wrote new
                                // rows they are replayed here; the frontend deduplicates on seq.
                                // If reload_pending was set (mpsc buffer full), last_sent_seq
                                // may lag; send_history fills the gap. If last_sent_seq also fell
                                // behind the seam (>2000 messages lost), send_history emits a
                                // ConversationSwitched{reload:true} to clear the client and replay
                                // from the seam — a disruption, but correct after a buffer overflow.
                                // (See docs/adr/2026/05/06-system-message-race/ for analysis.)
                                // send_ws(Status) is fire-and-forget: a dropped Status frame
                                // on buffer-full is a UI glitch (stale spinner) not data loss.
                                // The subsequent send_history uses backpressure and handles
                                // channel-closed correctly.
                                let _ = conn.send_ws(WsServerMessage::Status { state: cc_state });
                                if conn.send_history(conversation_id, conn.last_sent_seq).await.is_err() {
                                    // WS channel closed — connection dead.
                                    break;
                                }
                            } else {
                                // External wake (event queue, another connection).
                                // Full conversation switch with history.
                                let _ = conn.send_ws(conn.conversation_switched_from_bridge(
                                    &bridge, cc_state,
                                ));
                                if conn.send_history(conversation_id, None).await.is_err() {
                                    // WS channel closed — connection dead.
                                    break;
                                }
                            }

                            // Replay any pending synchronous permissions on this
                            // late-attach path so the tab sees the dialog.
                            if conn
                                .send_pending_permissions_backpressure(&bridge)
                                .await
                                .is_err()
                            {
                                // WS channel closed — connection dead.
                                break;
                            }

                            // Drain any approval responses that arrived before the bridge was ready.
                            conn.drain_queued_responses(&bridge).await;
                        }

                        // App-scope (not conversation-scope): a tab viewing a
                        // different conversation in the same app still needs models.
                        if conn.app_slug == app_slug {
                            conn.send_models_if_app_populated().await;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Missed some notifications. Not critical — we'll catch
                        // the next one or the user will send a message directly.
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Server shutting down.
                        break;
                    }
                }
            }
        }
    }

    // Record the WS disconnect.
    {
        let db_conn = conn.state.db.lock().await;
        usage::record_ws_disconnect(
            &db_conn,
            conn.device_id,
            conn.user_id,
            &conn.app_slug,
            conn.current_conversation_id,
        );
    }

    // Cleanup: detach from bridge (removes presence subscriber, drops broadcast rx).
    // The bridge stays alive independently.
    conn.detach().await;

    // Signal the writer task to stop: it ends when every sender is gone, and
    // the connection holds one of the two. Both go here, so a teardown the
    // server initiated — a denied user after an agent-map swap — actually
    // reaches the socket: the writer returns, closes the sink, and the client's
    // stream ends. Keeping the connection alive across the await below would
    // park the writer on a channel that can never close.
    drop(conn);
    drop(ws_tx);
    if let Err(e) = writer_handle.await {
        error!("ws_writer task panicked: {e}");
    }
}

impl super::connection::WsConnection {
    /// The tab's bridge closed its broadcast: its Claude Code process exited.
    ///
    /// The socket stays open. A singleton tab first follows its app's newest
    /// conversation; a persistent app then respawns the conversation the tab
    /// is on, and a non-persistent one goes idle. Err(()) when the socket
    /// closed during a history replay.
    pub(super) async fn on_bridge_closed(&mut self) -> Result<(), ()> {
        // Detach cleans up presence (if the bridge is still in the registry).
        self.detach().await;
        let switched = self.follow_singleton().await?;
        if switched && self.broadcast_rx.is_some() {
            // The tab is on the live successor now, and ConversationSwitched carried
            // its state; there is nothing to respawn.
            return Ok(());
        }
        if self.app_config().persistent {
            // Persistent app: CC dying is abnormal. Re-spawn.
            // detach() cleared history_sent. Set it back — the user
            // already has this conversation's history in the DOM.
            // When BridgeSpawned fires, the handler should perform
            // an incremental re-replay from last_sent_seq (not full
            // replay). last_sent_seq is intentionally NOT cleared by
            // detach() so it remains valid here.
            self.mark_history_already_sent();
            let _ = self.send_ws(WsServerMessage::Status {
                state: CcState::Connecting,
            });
            let conv_id = self
                .current_conversation_id
                .expect("persistent app always has a conversation");
            self.state.spawn_eager_wake(conv_id, self.timezone);
        } else {
            // Non-persistent: CC exited normally (conversation done).
            let _ = self.send_ws(WsServerMessage::Status {
                state: CcState::Idle,
            });
        }
        Ok(())
    }

    /// A bridge spawned somewhere. A singleton tab showing another conversation
    /// of the same app re-checks which conversation is current.
    pub(super) async fn follow_singleton_on_spawn(
        &mut self,
        conversation_id: i64,
        app_slug: &str,
    ) -> Result<bool, ()> {
        if self.app_slug != app_slug || self.current_conversation_id == Some(conversation_id) {
            return Ok(false);
        }
        self.follow_singleton().await
    }

    /// Whether this connection outlives an agent-map swap.
    ///
    /// A connection is authorized once, at connect, and nothing re-asks —
    /// so this is where a user the candidate document no longer allows is
    /// severed, which is what a restart would have done to them. The answer
    /// comes off the table, which the commit has already swapped.
    ///
    /// A missed pulse (`Lagged`) is still a swap and asks the same question;
    /// a closed channel is the server going down, and the connection goes with
    /// it.
    pub(super) fn survives_apps_swap(
        &self,
        username: &str,
        client_ip: std::net::IpAddr,
        swapped: Result<(), broadcast::error::RecvError>,
    ) -> bool {
        match swapped {
            Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => {
                if self.app_config().user_has_access(username) {
                    return true;
                }
                brenn_obs::security::log_and_alert_security_event(
                    &self.state.alert_dispatcher,
                    brenn_obs::security::SecurityEventType::AuthFailure,
                    client_ip,
                    &format!(
                        "user {username} denied WS access to app {} after a reload",
                        self.app_slug
                    ),
                );
                false
            }
            Err(broadcast::error::RecvError::Closed) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use brenn_db::conversation;
    use brenn_ws_types::{CcState, PaneLayout, WsServerMessage};

    use super::super::testing::*;

    /// The swap arm: a socket whose user the candidate no longer allows is
    /// closed, and one it still allows is left alone. A missed pulse asks the
    /// same question; a closed channel is the server going down.
    #[tokio::test]
    async fn a_swap_closes_the_socket_of_a_user_the_new_map_denies() {
        use tokio::sync::broadcast::error::RecvError;

        let (conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;
        assert!(
            conn.survives_apps_swap(TEST_USERNAME, TEST_CLIENT_IP, Ok(())),
            "the booted map allows this user",
        );

        // The reload's swap, as commit performs it: a new map in the one table
        // every gate reads.
        let mut denied = (*conn.state.apps.load()).clone();
        denied[TEST_APP_SLUG].allowed_users = vec!["someone-else".to_string()];
        conn.state.apps.store(std::sync::Arc::new(denied));

        assert!(
            !conn.survives_apps_swap(TEST_USERNAME, TEST_CLIENT_IP, Ok(())),
            "a user the swapped map denies is severed rather than left holding authority",
        );
        assert!(
            !conn.survives_apps_swap(TEST_USERNAME, TEST_CLIENT_IP, Err(RecvError::Lagged(3))),
            "a missed pulse is still a swap",
        );
        assert!(
            !conn.survives_apps_swap(TEST_USERNAME, TEST_CLIENT_IP, Err(RecvError::Closed)),
            "a closed channel is the server going down",
        );
    }

    /// The same swap, for a user the candidate still names: nothing happens.
    #[tokio::test]
    async fn a_swap_leaves_an_allowed_users_socket_open() {
        let (conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;
        let mut still_allowed = (*conn.state.apps.load()).clone();
        still_allowed[TEST_APP_SLUG].allowed_users =
            vec![TEST_USERNAME.to_string(), "someone-else".to_string()];
        conn.state.apps.store(std::sync::Arc::new(still_allowed));

        assert!(conn.survives_apps_swap(TEST_USERNAME, TEST_CLIENT_IP, Ok(())));
    }

    #[tokio::test]
    async fn send_layout_on_connect_defaults_to_two_column() {
        // test_ws_conn_for_app doesn't call the Welcome sequence, so test
        // send_layout directly on a fresh connection (default viewport = Wide).
        let (conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        conn.send_layout().await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert_eq!(msgs.len(), 1);
        match &msgs[0] {
            WsServerMessage::SetLayout { layout } => {
                assert_eq!(*layout, PaneLayout::TwoColumn);
            }
            other => panic!("expected SetLayout, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_layout_is_strictly_before_any_history_frame() {
        // Protocol invariant under the mobile-startup-refresh rewrite:
        // the server must emit SetLayout before any history frame (including
        // the terminal HistoryComplete) so the client can mount the correct
        // DOM shape up front. This test drives send_layout then send_history
        // in order on an empty conversation, then asserts the ordering on
        // the wire. An empty conversation still produces a HistoryComplete,
        // which is the earliest history-stream boundary — more than enough
        // to prove SetLayout precedes the history stream.
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps()).await;

        let conv_id = {
            let db_conn = db.lock().await;
            conversation::create_conversation(&db_conn, user_id, "test", false)
        };
        conn.current_conversation_id = Some(conv_id);

        // Match the production order in handle_ws's setup block:
        // Welcome is already-sent state; SetLayout then ConversationSwitched
        // then send_history.
        conn.send_layout().await;
        let _ = conn.send_ws(conn.conversation_switched(None, CcState::Idle));
        conn.send_history(conv_id, None)
            .await
            .expect("send_history succeeded");

        let msgs = collect_messages(&mut ws_rx).await;
        let layout_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::SetLayout { .. }))
            .expect("SetLayout must be present");
        let first_history_idx = msgs
            .iter()
            .position(|m| {
                matches!(
                    m,
                    WsServerMessage::AssistantMessage { .. }
                        | WsServerMessage::UserMessageEcho { .. }
                        | WsServerMessage::SystemMessageBroadcast { .. }
                        | WsServerMessage::ToolUseSummary { .. }
                        | WsServerMessage::ArtifactContent { .. }
                        | WsServerMessage::HistoryComplete { .. }
                )
            })
            .expect("expected at least one history-payload frame");
        assert!(
            layout_idx < first_history_idx,
            "SetLayout (idx {layout_idx}) must come before the first history frame (idx {first_history_idx}); msgs: {msgs:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Bridge spawn notification
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn spawn_bridge_sends_notification() {
        let (mut conn, _ws_rx, _db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Subscribe to bridge notifications before spawning.
        let mut notify_rx = conn.state.bridge_notify_tx.subscribe();

        conn.handle_send_message("trigger spawn", vec![], None, vec![])
            .await;

        // The spawn should have sent a BridgeSpawned notification.
        let notification = notify_rx.try_recv();
        assert!(
            notification.is_ok(),
            "expected BridgeSpawned notification after spawn_bridge"
        );
        let spawned = notification.unwrap();
        // Resume reactivates the existing conversation, so the notification
        // is for the same conv_id.
        assert_eq!(
            spawned.conversation_id, conv_id,
            "notification should be for the resumed conversation"
        );
        assert_eq!(
            spawned.app_slug, TEST_APP_SLUG,
            "notification should carry the spawning app's slug"
        );
    }

    #[tokio::test]
    async fn spawn_bridge_attaches_with_initial_rx() {
        let (mut conn, _ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Before sending, no broadcast subscription.
        assert!(conn.broadcast_rx.is_none());

        conn.handle_send_message("trigger spawn", vec![], None, vec![])
            .await;

        // After sending, should be attached (broadcast_rx is Some).
        assert!(
            conn.broadcast_rx.is_some(),
            "connection should be subscribed to bridge broadcast after spawn"
        );
    }

    #[tokio::test]
    async fn spawn_bridge_registers_in_active_bridges() {
        let (mut conn, _ws_rx, _db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Before spawn, active_bridges has no entry for this conversation.
        assert!(
            conn.state.active_bridges.get(conv_id).await.is_none(),
            "active_bridges should not contain the conversation before spawn"
        );

        conn.handle_send_message("trigger spawn", vec![], None, vec![])
            .await;

        // After spawn, the bridge must be registered so other tasks can route to it.
        assert!(
            conn.state.active_bridges.get(conv_id).await.is_some(),
            "active_bridges must contain the conversation after spawn_bridge"
        );
    }

    /// On WS connect with pwa_push enabled, `Welcome.user_id` is populated and
    /// `PushEnabled` reflects the current subscription state for (device, user).
    #[tokio::test]
    async fn welcome_includes_user_id_for_sw_idb_set_maintenance() {
        let (conn, mut ws_rx, db, uid, _pwa_push) = test_ws_conn_with_pwa_push().await;

        // Simulate what handle_ws does: send Welcome with user_id, then emit
        // PushEnabled reflecting current subscription state.
        let welcome_user_id = conn.user_id;
        let _ = conn.send_ws(WsServerMessage::Welcome {
            username: "testuser".to_string(),
            user_id: welcome_user_id,
            multiuser: false,
            singleton: false,
            available_models: vec![],
            default_model: "claude-sonnet-4-5".to_string(),
            attachment_targets: vec![],
            pwa_push_enabled: true,
        });

        // No subscription yet — PushEnabled should be false.
        let no_sub = {
            let db_conn = db.lock().await;
            brenn_pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
        };
        let _ = conn.send_ws(WsServerMessage::PushEnabled { enabled: no_sub });

        let welcome_msg = ws_rx.try_recv().expect("Welcome must be sent");
        match welcome_msg {
            WsServerMessage::Welcome {
                user_id,
                pwa_push_enabled,
                ..
            } => {
                assert_eq!(
                    user_id, uid,
                    "Welcome.user_id must match the authenticated user"
                );
                assert!(
                    pwa_push_enabled,
                    "pwa_push_enabled must be true for pwa_push app"
                );
            }
            other => panic!("expected Welcome, got {other:?}"),
        }

        let push_enabled_msg = ws_rx
            .try_recv()
            .expect("PushEnabled must be sent on connect");
        assert!(
            matches!(
                push_enabled_msg,
                WsServerMessage::PushEnabled { enabled: false }
            ),
            "expected PushEnabled(false) with no subscription, got {push_enabled_msg:?}"
        );

        // Now add a subscription and simulate re-connect.
        {
            let db_conn = db.lock().await;
            brenn_pwa_push::db::upsert_subscription(
                &db_conn,
                conn.device_id,
                conn.user_id,
                &brenn_pwa_push::endpoint_validator::ValidatedEndpoint::for_testing(
                    "https://push.example.com/sub",
                ),
                &fake_p256dh(),
                &fake_auth(),
            );
        }
        let with_sub = {
            let db_conn = db.lock().await;
            brenn_pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
        };
        let _ = conn.send_ws(WsServerMessage::PushEnabled { enabled: with_sub });
        let msg = ws_rx
            .try_recv()
            .expect("PushEnabled must be sent after subscription");
        assert!(
            matches!(msg, WsServerMessage::PushEnabled { enabled: true }),
            "expected PushEnabled(true) after subscription, got {msg:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Following the singleton
    // -----------------------------------------------------------------------

    use crate::active_bridge::ActiveBridge;
    use crate::state::BusHold;

    /// A singleton tab attached to the bridge of `old`, with `new` created
    /// after it as the app's newest row. `ws_rx` is drained.
    async fn singleton_tab_on_a_superseded_conversation(
        persistent: bool,
    ) -> (
        super::super::connection::WsConnection,
        tokio::sync::mpsc::Receiver<WsServerMessage>,
        brenn_db::Db,
        i64,
        i64,
        i64,
    ) {
        let mut apps = (*test_apps_singleton()).clone();
        apps[TEST_APP_SLUG].persistent = persistent;
        let (mut conn, mut ws_rx, db, user_id) =
            test_ws_conn_for_app(std::sync::Arc::new(apps)).await;
        let old = {
            let db_conn = db.lock().await;
            conversation::create_conversation(&db_conn, user_id, TEST_APP_SLUG, false)
        };
        let old_bridge = register_bridge(&conn, &db, user_id, old).await;
        conn.attach_to_bridge(&old_bridge).await;
        let new = {
            let db_conn = db.lock().await;
            conversation::create_singleton_successor(&db_conn, user_id, TEST_APP_SLUG, "1")
        };
        collect_messages(&mut ws_rx).await;
        (conn, ws_rx, db, user_id, old, new)
    }

    /// Inject a bridge for `conv_id` and register it as live.
    async fn register_bridge(
        conn: &super::super::connection::WsConnection,
        db: &brenn_db::Db,
        user_id: i64,
        conv_id: i64,
    ) -> std::sync::Arc<ActiveBridge> {
        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<WsServerMessage>(64);
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
        bridge
    }

    fn switched_to(msgs: &[WsServerMessage], id: i64) -> Option<usize> {
        msgs.iter().position(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: Some(c),
                    reload: true,
                    ..
                } if *c == id
            )
        })
    }

    fn any_switch(msgs: &[WsServerMessage]) -> bool {
        msgs.iter()
            .any(|m| matches!(m, WsServerMessage::ConversationSwitched { .. }))
    }

    fn status_position(msgs: &[WsServerMessage], state: CcState) -> Option<usize> {
        msgs.iter()
            .position(|m| matches!(m, WsServerMessage::Status { state: s } if *s == state))
    }

    #[tokio::test]
    async fn a_closed_bridge_moves_a_persistent_singleton_tab_to_the_successor_and_wakes_it() {
        let (mut conn, mut ws_rx, _db, _user_id, _old, new) =
            singleton_tab_on_a_superseded_conversation(true).await;

        assert!(conn.on_bridge_closed().await.is_ok());

        assert_eq!(conn.current_conversation_id, Some(new));
        assert!(
            conn.broadcast_rx.is_none(),
            "no bridge is live for the successor"
        );
        let msgs = collect_messages(&mut ws_rx).await;
        let switch_idx = switched_to(&msgs, new)
            .unwrap_or_else(|| panic!("expected a reload switch to {new}; msgs: {msgs:?}"));
        let history_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::HistoryComplete { .. }))
            .unwrap_or_else(|| panic!("expected HistoryComplete; msgs: {msgs:?}"));
        assert!(
            switch_idx < history_idx,
            "the switch precedes the successor's history; msgs: {msgs:?}"
        );
        let connecting_idx = status_position(&msgs, CcState::Connecting)
            .unwrap_or_else(|| panic!("expected Status(Connecting); msgs: {msgs:?}"));
        assert!(connecting_idx > switch_idx, "msgs: {msgs:?}");
        assert_eq!(
            *conn.state.wake_spawns.lock().unwrap(),
            vec![(new, BusHold::Unheld)],
            "the respawn wakes the successor, never the superseded id",
        );
    }

    #[tokio::test]
    async fn a_closed_bridge_attaches_a_singleton_tab_to_a_live_successor() {
        let (mut conn, mut ws_rx, db, user_id, _old, new) =
            singleton_tab_on_a_superseded_conversation(true).await;
        register_bridge(&conn, &db, user_id, new).await;

        assert!(conn.on_bridge_closed().await.is_ok());

        assert_eq!(conn.current_conversation_id, Some(new));
        assert!(
            conn.broadcast_rx.is_some(),
            "attached to the live successor"
        );
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(switched_to(&msgs, new).is_some(), "msgs: {msgs:?}");
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, WsServerMessage::Status { .. })),
            "a live successor needs no Status; msgs: {msgs:?}"
        );
        assert!(conn.state.wake_spawns.lock().unwrap().is_empty());
    }

    /// The switch onto a live successor reads `shared` off the bridge, the live
    /// flag a privacy toggle updates, not off the row.
    #[tokio::test]
    async fn a_switch_onto_a_live_successor_carries_the_bridges_shared_flag() {
        let (mut conn, mut ws_rx, db, user_id, _old, new) =
            singleton_tab_on_a_superseded_conversation(true).await;
        let successor = register_bridge(&conn, &db, user_id, new).await;
        successor
            .shared
            .store(true, std::sync::atomic::Ordering::SeqCst);

        assert!(conn.on_bridge_closed().await.is_ok());

        let msgs = collect_messages(&mut ws_rx).await;
        let (is_owner, shared) = msgs
            .iter()
            .find_map(|m| match m {
                WsServerMessage::ConversationSwitched {
                    conversation_id: Some(c),
                    reload: true,
                    is_owner,
                    shared,
                    ..
                } if *c == new => Some((*is_owner, *shared)),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no reload switch to the successor; msgs: {msgs:?}"));
        assert!(shared, "shared comes from the bridge; msgs: {msgs:?}");
        assert!(is_owner, "msgs: {msgs:?}");
    }

    #[tokio::test]
    async fn a_closed_bridge_on_the_current_singleton_conversation_respawns_it() {
        let mut apps = (*test_apps_singleton()).clone();
        apps[TEST_APP_SLUG].persistent = true;
        let (mut conn, mut ws_rx, db, user_id) =
            test_ws_conn_for_app(std::sync::Arc::new(apps)).await;
        let current = {
            let db_conn = db.lock().await;
            conversation::create_conversation(&db_conn, user_id, TEST_APP_SLUG, false)
        };
        let bridge = register_bridge(&conn, &db, user_id, current).await;
        conn.attach_to_bridge(&bridge).await;
        collect_messages(&mut ws_rx).await;

        assert!(conn.on_bridge_closed().await.is_ok());

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(!any_switch(&msgs), "msgs: {msgs:?}");
        assert!(
            status_position(&msgs, CcState::Connecting).is_some(),
            "msgs: {msgs:?}"
        );
        assert_eq!(
            *conn.state.wake_spawns.lock().unwrap(),
            vec![(current, BusHold::Unheld)],
        );
        assert_eq!(conn.current_conversation_id, Some(current));
    }

    #[tokio::test]
    async fn a_closed_bridge_on_a_non_persistent_singleton_tab_switches_and_goes_idle() {
        let (mut conn, mut ws_rx, _db, _user_id, _old, new) =
            singleton_tab_on_a_superseded_conversation(false).await;

        assert!(conn.on_bridge_closed().await.is_ok());

        assert_eq!(conn.current_conversation_id, Some(new));
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(switched_to(&msgs, new).is_some(), "msgs: {msgs:?}");
        assert!(
            matches!(
                msgs.last(),
                Some(WsServerMessage::Status {
                    state: CcState::Idle
                })
            ),
            "the last message is Status(Idle); msgs: {msgs:?}"
        );
        assert!(conn.state.wake_spawns.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_successor_spawn_switches_a_tab_watching_an_idle_superseded_conversation() {
        let (mut conn, mut ws_rx, db, user_id, old, new) =
            singleton_tab_on_a_superseded_conversation(false).await;
        conn.detach().await;
        assert_eq!(conn.current_conversation_id, Some(old));
        register_bridge(&conn, &db, user_id, new).await;

        assert_eq!(
            conn.follow_singleton_on_spawn(new, TEST_APP_SLUG).await,
            Ok(true)
        );

        assert_eq!(conn.current_conversation_id, Some(new));
        assert!(conn.broadcast_rx.is_some());
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(switched_to(&msgs, new).is_some(), "msgs: {msgs:?}");
    }

    #[tokio::test]
    async fn a_full_send_buffer_does_not_drop_the_reload_switch() {
        let (mut conn, mut ws_rx, _db, _user_id, old, new) =
            singleton_tab_on_a_superseded_conversation(false).await;
        conn.detach().await;
        assert_eq!(conn.current_conversation_id, Some(old));

        let mut filled = 0;
        while conn.send_ws(WsServerMessage::Status {
            state: CcState::Idle,
        }) == super::super::connection::SendResult::Ok
        {
            filled += 1;
        }
        assert_eq!(
            conn.send_ws(WsServerMessage::Status {
                state: CcState::Idle,
            }),
            super::super::connection::SendResult::Full
        );
        assert_eq!(filled, 256);

        let drain = async {
            let mut received = Vec::new();
            while let Some(msg) = ws_rx.recv().await {
                let done = matches!(msg, WsServerMessage::HistoryComplete { .. });
                received.push(msg);
                if done {
                    while let Ok(msg) = ws_rx.try_recv() {
                        received.push(msg);
                    }
                    break;
                }
            }
            received
        };
        let (result, msgs) = tokio::join!(conn.follow_singleton(), drain);

        assert_eq!(result, Ok(true));
        assert_eq!(conn.current_conversation_id, Some(new));
        let switch_idx = switched_to(&msgs, new)
            .unwrap_or_else(|| panic!("expected a reload switch to {new}; msgs: {msgs:?}"));
        let history_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::HistoryComplete { .. }))
            .unwrap_or_else(|| panic!("expected HistoryComplete; msgs: {msgs:?}"));
        assert!(
            switch_idx < history_idx,
            "the switch precedes the successor's history; msgs: {msgs:?}"
        );
        assert!(
            switch_idx >= filled,
            "the switch follows every filler frame; msgs: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn a_spawn_of_a_superseded_conversation_leaves_the_tab_on_the_newest() {
        let (mut conn, mut ws_rx, _db, _user_id, old, new) =
            singleton_tab_on_a_superseded_conversation(false).await;
        conn.detach().await;
        conn.current_conversation_id = Some(new);

        assert_eq!(
            conn.follow_singleton_on_spawn(old, TEST_APP_SLUG).await,
            Ok(false)
        );

        assert_eq!(conn.current_conversation_id, Some(new));
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(msgs.is_empty(), "msgs: {msgs:?}");
    }

    #[tokio::test]
    async fn a_spawn_in_another_app_is_ignored() {
        let (mut conn, mut ws_rx, _db, _user_id, old, new) =
            singleton_tab_on_a_superseded_conversation(false).await;

        assert_eq!(
            conn.follow_singleton_on_spawn(new, "other-app").await,
            Ok(false)
        );

        assert_eq!(conn.current_conversation_id, Some(old));
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(msgs.is_empty(), "msgs: {msgs:?}");
    }

    #[tokio::test]
    async fn follow_singleton_is_inert_for_a_non_singleton_app() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps()).await;
        let older = {
            let db_conn = db.lock().await;
            let older = conversation::create_conversation(&db_conn, user_id, TEST_APP_SLUG, false);
            conversation::create_conversation(&db_conn, user_id, TEST_APP_SLUG, false);
            older
        };
        conn.current_conversation_id = Some(older);

        assert_eq!(conn.follow_singleton().await, Ok(false));

        assert_eq!(conn.current_conversation_id, Some(older));
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(msgs.is_empty(), "msgs: {msgs:?}");
    }
}

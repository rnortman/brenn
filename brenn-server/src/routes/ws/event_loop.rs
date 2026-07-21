//! WebSocket event loop: `handle_ws`, `ws_writer`, `recv_broadcast`, `BroadcastResult`.

use std::net::IpAddr;

use axum::extract::ws::{Message, WebSocket};
use brenn_lib::auth::session::Session;
use brenn_lib::conversation;
use brenn_lib::usage::{self as usage};
use brenn_lib::ws_types::{CcState, ViewportClass, WsServerMessage};
use futures::{SinkExt, StreamExt};
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

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
        history_sent: false,
        last_sent_seq: None,
        queued_responses: Vec::new(),
        oldest_loaded_seq: None,
        client_error_bucket: super::connection::ClientErrorBucket::new(),
        #[cfg(test)]
        test_bridge: None,
    };

    // Send Welcome as the very first message — gives the frontend its identity.
    let available_models = {
        let models = conn.state.cached_models.read().await;
        models.get(&conn.app_slug).cloned().unwrap_or_default()
    };
    let default_model = conn.app_config().model.clone();
    let attachment_targets: Vec<brenn_lib::ws_types::TargetInfo> = conn
        .app_config()
        .attachment_targets
        .iter()
        .map(|t| brenn_lib::ws_types::TargetInfo {
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
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, session.user.id)
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
                        brenn_lib::obs::security::log_and_alert_security_event(
                            &state.alert_dispatcher,
                            brenn_lib::obs::security::SecurityEventType::SchemaViolation,
                            client_ip,
                            &format!("binary WS frame from user {}", session.user.username),
                        );
                        let db_conn = state.db.lock().await;
                        brenn_lib::auth::session::delete_session(&db_conn, &session.token);
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
                        // Bridge's broadcast sender was dropped (CC exited).
                        // Don't break — keep WS open for user to start a new conversation.
                        // Detach cleans up presence (if bridge is still in registry).
                        reload_pending = false;
                        conn.detach().await;
                        if conn.app_config().persistent {
                            // Persistent app: CC dying is abnormal. Re-spawn.
                            // detach() cleared history_sent. Set it back — the user
                            // already has this conversation's history in the DOM.
                            // When BridgeSpawned fires, the handler should perform
                            // an incremental re-replay from last_sent_seq (not full
                            // replay). last_sent_seq is intentionally NOT cleared by
                            // detach() so it remains valid here.
                            conn.mark_history_already_sent();
                            let _ = conn.send_ws(WsServerMessage::Status {
                                state: CcState::Connecting,
                            });
                            let conv_id = conn
                                .current_conversation_id
                                .expect("persistent app always has a conversation");
                            state.spawn_eager_wake(conv_id, conn.timezone);
                        } else {
                            // Non-persistent: CC exited normally (conversation done).
                            let _ = conn.send_ws(WsServerMessage::Status {
                                state: CcState::Idle,
                            });
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
            // Bridge spawn notification — another connection spawned a bridge
            // for a conversation we might be viewing.
            notification = conn.bridge_notify_rx.recv() => {
                match notification {
                    Ok(crate::state::BridgeSpawned { conversation_id, app_slug }) => {
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

    drop(ws_tx); // Signal the writer task to stop.
    if let Err(e) = writer_handle.await {
        error!("ws_writer task panicked: {e}");
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::conversation;
    use brenn_lib::ws_types::{CcState, PaneLayout, WsServerMessage};

    use super::super::testing::*;

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
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
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
            brenn_lib::pwa_push::db::upsert_subscription(
                &db_conn,
                conn.device_id,
                conn.user_id,
                &brenn_lib::pwa_push::endpoint_validator::ValidatedEndpoint::for_testing(
                    "https://push.example.com/sub",
                ),
                &fake_p256dh(),
                &fake_auth(),
            );
        }
        let with_sub = {
            let db_conn = db.lock().await;
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
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
}

//! `handle_switch_conversation`, `handle_new_conversation`, `handle_steal_app`,
//! `handle_set_conversation_privacy`, `try_select_requested_conversation`,
//! `handle_reconnect`, `handle_list_conversations`.

use brenn_lib::conversation;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::{CcState, WsServerMessage};
use tracing::{info, warn};

use super::connection::WsConnection;

// impl WsConnection — conversation lifecycle (switch, new, steal, privacy, reconnect, list)
impl WsConnection {
    /// Handle SwitchConversation.
    pub(super) async fn handle_switch_conversation(&mut self, conversation_id: i64) {
        let multiuser = self.app_config().multiuser;

        // Authorization check.
        let conv = {
            let conn = self.state.db.lock().await;
            conversation::get_conversation_opt(&conn, conversation_id)
        };

        let conv = match conv {
            Some(c) if c.app_slug != self.app_slug => {
                // Wrong app — treat as not found.
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Conversation not found".to_string(),
                });
                return;
            }
            Some(c) if conversation::can_access_conversation(self.user_id, &c, multiuser) => c,
            Some(c) => {
                log_and_alert_security_event(
                    &self.state.alert_dispatcher,
                    SecurityEventType::SchemaViolation,
                    self.client_ip,
                    &format!(
                        "user {} tried to access conversation {} owned by user {}",
                        self.user_id, conversation_id, c.user_id
                    ),
                );
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Not authorized".to_string(),
                });
                return;
            }
            None => {
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Conversation not found".to_string(),
                });
                return;
            }
        };

        // Detach from current bridge.
        self.detach().await;
        self.current_conversation_id = Some(conversation_id);
        self.viewer_only = false;

        // Check if there's a live bridge for this conversation.
        let (cc_state, bridge_for_replay) =
            if let Some(bridge) = self.state.active_bridges.get(conversation_id).await {
                self.attach_to_bridge(&bridge).await;
                let state = bridge.resolve_cc_state().await;
                (state, Some(bridge))
            } else {
                (CcState::Connecting, None)
            };

        let _ = self.send_ws(self.conversation_switched(Some(&conv), cc_state));

        // Send history. Back-pressured — Err means WS is dead.
        if self.send_history(conversation_id, None).await.is_err() {
            return;
        }
        // Replay any pending synchronous permissions so the user sees the
        // dialog after a switch-back to a permission-blocked conversation.
        if let Some(bridge) = bridge_for_replay
            && self
                .send_pending_permissions_backpressure(&bridge)
                .await
                .is_err()
        {
            return;
        }

        // Send fresh todo state so the sidebar is authoritative immediately
        // after switching, without waiting for the next mutation.
        // Guard on graf_config: switching to a non-graf app is legitimate and
        // should silently skip rather than reject; only send if graf is configured.
        if let Some(config) = brenn_graf::graf_config(self.app_config()) {
            self.send_todo_state(config).await;
        }

        // Update conversation list for sidebar.
        self.send_conversation_list().await;

        // If we're not attached to a live bridge, eager-spawn CC.
        if self.broadcast_rx.is_none()
            && let Some(conv_id) = self.current_conversation_id
        {
            self.state.spawn_eager_wake(conv_id, self.timezone);
        }
    }

    /// Handle NewConversation.
    ///
    /// Eagerly creates the conversation (or reuses an empty one) and, for
    /// persistent apps, eagerly spawns CC so it's ready when the user types.
    pub(super) async fn handle_new_conversation(&mut self) {
        self.detach().await;
        self.viewer_only = false;
        self.oldest_loaded_seq = None;
        self.last_sent_seq = None;
        self.history_sent = false;

        let shared = self.app_config().multiuser;
        let persistent = self.app_config().persistent;

        // Reuse an empty conversation if one exists (avoids accumulating
        // title-less conversations from repeated "New Conversation" clicks).
        let reusable = {
            let conn = self.state.db.lock().await;
            conversation::find_empty_conversation(&conn, self.user_id, &self.app_slug)
        };
        let conv_id = match reusable {
            Some(id) if self.state.active_bridges.get(id).await.is_none() => id,
            _ => {
                let conn = self.state.db.lock().await;
                conversation::create_conversation(&conn, self.user_id, &self.app_slug, shared)
            }
        };
        self.current_conversation_id = Some(conv_id);

        let conv = {
            let conn = self.state.db.lock().await;
            conversation::get_conversation(&conn, conv_id)
        };

        let initial_state = if persistent {
            CcState::Connecting
        } else {
            CcState::Idle
        };
        let _ = self.send_ws(self.conversation_switched(Some(&conv), initial_state));
        let _ = self.send_ws(WsServerMessage::HistoryComplete {
            oldest_loaded_seq: None,
        });
        let _ = self.send_ws(WsServerMessage::ArtifactIndex { files: vec![] });
        self.send_conversation_list().await;

        // Eager-spawn only for persistent apps. For non-persistent apps,
        // the user hasn't typed anything yet — no reason to burn a CC
        // process. handle_send_message will spawn on first message.
        if persistent {
            self.state.spawn_eager_wake(conv_id, self.timezone);
        }
    }

    /// Handle StealApp: kill all existing bridges for this single-instance app.
    pub(super) async fn handle_steal_app(&mut self) {
        let app_config = self
            .state
            .apps
            .get(&self.app_slug)
            .unwrap_or_else(|| panic!("app {:?} not found in config", self.app_slug));

        if !app_config.single_instance {
            warn!(app = %self.app_slug, "StealApp for non-single-instance app");
            let _ = self.send_ws(WsServerMessage::Error {
                message: "This app does not use single-instance mode".to_string(),
            });
            return;
        }

        let bridges = self.state.active_bridges.get_for_app(&self.app_slug).await;
        if bridges.is_empty() {
            let _ = self.send_ws(WsServerMessage::Error {
                message: "No active session to steal".to_string(),
            });
            return;
        }

        info!(
            app = %self.app_slug,
            bridge_count = bridges.len(),
            "stealing app session",
        );

        for bridge in &bridges {
            // Notify all tabs attached to this bridge before killing.
            bridge.broadcast(WsServerMessage::SessionStolen {
                message: "Session terminated — another user started a new session".to_string(),
            });
            // Kill CC subprocess and remove from registry.
            bridge.kill_session(&self.state.active_bridges).await;
        }

        // Detach from current bridge if we were attached to one of the killed ones.
        if let Some(conv_id) = self.current_conversation_id
            && bridges.iter().any(|b| b.conversation_id == conv_id)
        {
            self.detach().await;
            self.current_conversation_id = None;
        }
        self.viewer_only = false;
        self.oldest_loaded_seq = None;
        self.last_sent_seq = None;

        // Put the user in a clean state, ready to start a new conversation.
        let _ = self.send_ws(self.conversation_switched(None, CcState::Idle));
        let _ = self.send_ws(WsServerMessage::HistoryComplete {
            oldest_loaded_seq: None,
        });
        let _ = self.send_ws(WsServerMessage::ArtifactIndex { files: vec![] });
    }

    /// Handle SetConversationPrivacy: owner toggles shared/private on a conversation.
    pub(super) async fn handle_set_conversation_privacy(
        &mut self,
        conversation_id: i64,
        shared: bool,
    ) {
        let app_config = self.app_config();
        if !app_config.multiuser {
            log_and_alert_security_event(
                &self.state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                self.client_ip,
                &format!(
                    "user {} sent SetConversationPrivacy on non-multiuser app {}",
                    self.user_id, self.app_slug
                ),
            );
            let _ = self.send_ws(WsServerMessage::Error {
                message: "Privacy toggle is only available in multiuser apps".to_string(),
            });
            return;
        }

        let conv = {
            let conn = self.state.db.lock().await;
            conversation::get_conversation_opt(&conn, conversation_id)
        };
        let conv = match conv {
            Some(c) if c.app_slug != self.app_slug => {
                // Wrong app — treat as not found.
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Conversation not found".to_string(),
                });
                return;
            }
            Some(c) => c,
            None => {
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Conversation not found".to_string(),
                });
                return;
            }
        };

        // Only the conversation owner can change privacy.
        // The UI doesn't show the toggle for non-owners, so this is suspicious.
        if conv.user_id != self.user_id {
            log_and_alert_security_event(
                &self.state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                self.client_ip,
                &format!(
                    "user {} tried to change privacy on conversation {} owned by user {}",
                    self.user_id, conversation_id, conv.user_id
                ),
            );
            let _ = self.send_ws(WsServerMessage::Error {
                message: "Only the conversation owner can change privacy".to_string(),
            });
            return;
        }

        // Update DB.
        {
            let conn = self.state.db.lock().await;
            conversation::set_conversation_shared(&conn, conversation_id, shared);
        }

        // Update ActiveBridge if one exists, and broadcast to other subscribers.
        if let Some(bridge) = self.state.active_bridges.get(conversation_id).await {
            bridge.set_shared(shared);
            bridge.broadcast(WsServerMessage::PrivacyChanged {
                conversation_id,
                shared,
            });
        }

        // Always send PrivacyChanged directly to the requester. The broadcast
        // above only reaches other subscribers (or nobody if no bridge exists).
        // The requester needs it to update their toggle UI immediately.
        let _ = self.send_ws(WsServerMessage::PrivacyChanged {
            conversation_id,
            shared,
        });

        // Refresh conversation list for the requester.
        self.send_conversation_list().await;
    }

    /// Try to select a specific conversation requested via `?conv=ID`.
    /// Returns true if the conversation was found and loaded, false if it should
    /// fall through to auto-selection (sends an error message on auth/not-found failures).
    ///
    /// `last_seq` enables incremental reconnect: when `Some(n)`, only messages
    /// with `seq > n` are replayed (validated server-side by `build_history`).
    /// Returns `Ok(true)` if the conversation was selected and the WS is alive,
    /// `Ok(false)` if the conversation could not be selected (wrong app, wrong
    /// user, not found), or `Err(())` if the conversation was selected but the
    /// WS died during history delivery.
    pub(super) async fn try_select_requested_conversation(
        &mut self,
        conversation_id: i64,
        multiuser: bool,
        last_seq: Option<i64>,
    ) -> Result<bool, ()> {
        let conv = {
            let db_conn = self.state.db.lock().await;
            conversation::get_conversation_opt(&db_conn, conversation_id)
        };
        match conv {
            Some(c) if c.app_slug != self.app_slug => {
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Conversation not found".to_string(),
                });
                Ok(false)
            }
            Some(c) if conversation::can_access_conversation(self.user_id, &c, multiuser) => {
                self.current_conversation_id = Some(c.id);
                self.viewer_only = false;

                let (cc_state, bridge_for_replay) =
                    if let Some(bridge) = self.state.active_bridges.get(conversation_id).await {
                        self.attach_to_bridge(&bridge).await;
                        let state = bridge.resolve_cc_state().await;
                        (state, Some(bridge))
                    } else {
                        (CcState::Connecting, None)
                    };

                let _ = self.send_ws(self.conversation_switched(Some(&c), cc_state));
                // Back-pressured — Err means WS is dead.
                self.send_history(c.id, last_seq).await.map_err(|_| ())?;
                if let Some(bridge) = bridge_for_replay {
                    self.send_pending_permissions_backpressure(&bridge)
                        .await
                        .map_err(|_| ())?;
                }
                Ok(true)
            }
            Some(c) => {
                log_and_alert_security_event(
                    &self.state.alert_dispatcher,
                    SecurityEventType::SchemaViolation,
                    self.client_ip,
                    &format!(
                        "user {} tried to access conversation {} owned by user {} via ?conv",
                        self.user_id, conversation_id, c.user_id
                    ),
                );
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Not authorized".to_string(),
                });
                Ok(false)
            }
            None => {
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Conversation not found".to_string(),
                });
                Ok(false)
            }
        }
    }

    /// Handle Reconnect. If we are already attached to the target
    /// conversation, treat as history-resync only (no detach, no
    /// re-attach); otherwise delegate to `handle_switch_conversation`.
    ///
    /// The early-return path skips the authorization check: we remain
    /// attached to a bridge that was already authorized via
    /// `attach_to_bridge`, so a same-conversation Reconnect cannot
    /// bypass any check the original attach didn't.
    pub(super) async fn handle_reconnect(&mut self, conversation_id: i64, last_seq: Option<i64>) {
        if self.current_conversation_id == Some(conversation_id) && self.broadcast_rx.is_some() {
            // `kill_session` removes the bridge from the registry before
            // the broadcast `Sender` drops, so there is a real window in
            // which `broadcast_rx` is still live but `active_bridges.get`
            // returns None. Return silently and let the imminent
            // `BroadcastResult::Closed` (handled in the select loop) run
            // the existing recovery path.
            let Some(bridge) = self.state.active_bridges.get(conversation_id).await else {
                warn!(
                    conversation_id,
                    "Reconnect: bridge missing while broadcast_rx is live (kill_session race)"
                );
                return;
            };
            if self.send_history(conversation_id, last_seq).await.is_err() {
                return;
            }
            if self
                .send_pending_permissions_backpressure(&bridge)
                .await
                .is_err()
            {
                return;
            }
            return;
        }
        self.handle_switch_conversation(conversation_id).await;
    }

    /// Handle ListConversations.
    pub(super) async fn handle_list_conversations(&self) {
        self.send_conversation_list().await;
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::auth::user::create_user;
    use brenn_lib::conversation::{self as conversation, ConversationStatus};
    use brenn_lib::db::init_db_memory;
    use brenn_lib::ws_types::{CcState, PaneLayout, ViewportClass, WsServerMessage};
    use tokio::sync::{broadcast, mpsc};

    use super::super::dispatch::handle_client_message;
    use super::super::testing::*;
    use crate::active_bridge::ActiveBridge;
    use crate::state::AppState;

    #[tokio::test]
    async fn resume_sets_title_when_missing() {
        let (mut conn, _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_send_message("What is the meaning of life?", vec![], None, vec![])
            .await;

        let db_conn = db.lock().await;
        let conv = conversation::get_conversation(&db_conn, conv_id);
        assert_eq!(conv.title.as_deref(), Some("What is the meaning of life?"));
    }

    #[tokio::test]
    async fn resume_preserves_existing_title() {
        let (mut conn, _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Set a title before resuming.
        {
            let db_conn = db.lock().await;
            conversation::set_title(&db_conn, conv_id, "Existing title");
        }

        conn.handle_send_message("Follow-up question", vec![], None, vec![])
            .await;

        let db_conn = db.lock().await;
        let conv = conversation::get_conversation(&db_conn, conv_id);
        assert_eq!(conv.title.as_deref(), Some("Existing title"));
    }

    #[tokio::test]
    async fn resume_reactivates_completed_conversation() {
        let (mut conn, _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Verify it's completed before resume.
        {
            let db_conn = db.lock().await;
            let conv = conversation::get_conversation(&db_conn, conv_id);
            assert_eq!(conv.status, ConversationStatus::Completed);
        }

        conn.handle_send_message("Resume this", vec![], None, vec![])
            .await;

        let db_conn = db.lock().await;
        let conv = conversation::get_conversation(&db_conn, conv_id);
        assert_eq!(conv.status, ConversationStatus::Active);
    }

    #[tokio::test]
    async fn resume_sends_conversation_switched() {
        let (mut conn, mut ws_rx, _db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_send_message("Hello again", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        // First message should be ConversationSwitched.
        let switched = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::ConversationSwitched { .. }));
        assert!(
            switched.is_some(),
            "expected ConversationSwitched, got: {msgs:?}"
        );
        match switched.unwrap() {
            WsServerMessage::ConversationSwitched {
                conversation_id,
                state,
                ..
            } => {
                assert_eq!(*conversation_id, Some(conv_id));
                assert_eq!(*state, CcState::Thinking);
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn resume_sends_conversation_list() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_send_message("Hello again", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        let has_list = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ConversationList { .. }));
        assert!(
            has_list,
            "expected ConversationList in messages, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn resume_message_ordering() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_send_message("Hello", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        // Expected order: ConversationSwitched, ConversationList, UserMessageEcho.
        // (UserMessageEcho comes from persist_and_send → broadcast_user_echo.)
        let switched_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::ConversationSwitched { .. }));
        let list_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::ConversationList { .. }));

        assert!(
            switched_idx.is_some() && list_idx.is_some(),
            "missing expected messages: {msgs:?}"
        );
        assert!(
            switched_idx.unwrap() < list_idx.unwrap(),
            "ConversationSwitched should come before ConversationList: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn resume_unauthorized_conversation_sends_error() {
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);
        let (ws_tx, mut ws_rx) = mpsc::channel(256);

        // Create two users; conversation belongs to user A.
        let (_user_a, conv_id) = {
            let conn = db.lock().await;
            let uid_a = create_user(&conn, "alice", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid_a, "test", false);
            conversation::complete_conversation(&conn, cid, None);
            (uid_a, cid)
        };
        let (user_b, device_id_b) = {
            let conn = db.lock().await;
            let uid_b = create_user(&conn, "bob", "$argon2id$fake");
            let did = create_test_device(&conn, uid_b);
            (uid_b, did)
        };

        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);
        let test_bridge =
            ActiveBridge::inject_for_test(user_b, conv_id, "test", db.clone(), broadcast_tx);

        // User B tries to send to user A's conversation.
        let mut conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            test_bridge: Some(test_bridge),
            ..WsConnBuilder::with_defaults(
                user_b,
                TEST_USERNAME.to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id_b,
            )
        }
        .build();

        conn.handle_send_message("Sneaky message", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        let has_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message == "Not authorized"),
        );
        assert!(has_error, "expected Error 'Not authorized', got: {msgs:?}");
    }

    #[tokio::test]
    async fn new_conversation_sends_switched_history_complete_and_list() {
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);
        let (ws_tx, mut ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        // Create user and a dummy conversation for the test bridge to reference.
        // handle_send_message will create a *different* conversation (the "new" one),
        // but the bridge needs a valid conversation_id for persist_user_message_with_attachments.
        let (user_id, dummy_conv_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            let cid = conversation::create_conversation(&conn, uid, TEST_APP_SLUG, false);
            (uid, cid, did)
        };

        let test_bridge = ActiveBridge::inject_for_test(
            user_id,
            dummy_conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );

        // Register the bridge for wake_conversation (called by handle_send_message).
        *state.test_wake_bridge.lock().await = Some(test_bridge.clone());

        let mut conn = WsConnBuilder {
            // current_conversation_id: None (default) — new conversation path.
            test_bridge: Some(test_bridge),
            ..WsConnBuilder::with_defaults(
                user_id,
                TEST_USERNAME.to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        conn.handle_send_message("First message", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        // New conversation path: ConversationSwitched, HistoryComplete, ArtifactIndex, ConversationList.
        let switched_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::ConversationSwitched { .. }));
        let history_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        let artifact_index_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::ArtifactIndex { .. }));
        let list_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::ConversationList { .. }));

        assert!(
            switched_idx.is_some(),
            "missing ConversationSwitched: {msgs:?}"
        );
        assert!(history_idx.is_some(), "missing HistoryComplete: {msgs:?}");
        assert!(
            artifact_index_idx.is_some(),
            "missing ArtifactIndex: {msgs:?}"
        );
        assert!(list_idx.is_some(), "missing ConversationList: {msgs:?}");
        assert!(
            switched_idx.unwrap() < history_idx.unwrap(),
            "ConversationSwitched should come before HistoryComplete: {msgs:?}"
        );
        assert!(
            history_idx.unwrap() < artifact_index_idx.unwrap(),
            "HistoryComplete should come before ArtifactIndex: {msgs:?}"
        );
        assert!(
            artifact_index_idx.unwrap() < list_idx.unwrap(),
            "ArtifactIndex should come before ConversationList: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn stale_conversation_id_creates_new_conversation() {
        // When current_conversation_id points to a non-existent conversation
        // (no DB record), the code falls through to create a new conversation.
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);
        let (ws_tx, mut ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        // Create user and a dummy conversation for the test bridge.
        let (user_id, dummy_conv_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            let cid = conversation::create_conversation(&conn, uid, TEST_APP_SLUG, false);
            (uid, cid, did)
        };

        let test_bridge = ActiveBridge::inject_for_test(
            user_id,
            dummy_conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );

        // Register bridge for wake_conversation (the stale conv_id falls through
        // to new-conversation path which calls wake_conversation).
        *state.test_wake_bridge.lock().await = Some(test_bridge.clone());

        let mut conn = WsConnBuilder {
            current_conversation_id: Some(99999), // Non-existent conversation.
            test_bridge: Some(test_bridge),
            ..WsConnBuilder::with_defaults(
                user_id,
                TEST_USERNAME.to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        conn.handle_send_message("Message to nowhere", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        // Should fall through to new-conversation path.
        let has_switched = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ConversationSwitched { .. }));
        assert!(
            has_switched,
            "expected new conversation (ConversationSwitched), got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn steal_app_non_single_instance_returns_error() {
        let (mut conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        conn.handle_steal_app().await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs.iter().any(|m| matches!(m, WsServerMessage::Error { message } if message.contains("single-instance")));
        assert!(
            has_error,
            "expected error about single-instance, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn steal_app_no_active_session_returns_error() {
        let (mut conn, mut ws_rx, _db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        conn.handle_steal_app().await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs.iter().any(|m| matches!(m, WsServerMessage::Error { message } if message.contains("No active session")));
        assert!(
            has_error,
            "expected 'No active session' error, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn steal_app_kills_bridges_and_clears_state() {
        let (mut conn, mut ws_rx, db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        // Create another user's bridge for the same app.
        let other_user_id = {
            let c = db.lock().await;
            create_user(&c, "otheruser", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, other_user_id, "test", false)
        };
        let (broadcast_tx, mut other_rx) = broadcast::channel(64);
        let other_bridge = ActiveBridge::inject_for_test(
            other_user_id,
            other_conv_id,
            "test",
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(other_conv_id, other_bridge.clone())
            .await;

        // Attach our connection to the other user's bridge (simulating auto-attach).
        conn.attach_to_bridge(&other_bridge).await;

        // Drain the PresenceUpdate from attach.
        let presence = other_rx.try_recv();
        assert!(
            matches!(&presence, Ok(WsServerMessage::PresenceUpdate { .. })),
            "expected PresenceUpdate after attach, got: {presence:?}"
        );

        // Steal.
        conn.handle_steal_app().await;

        // Other bridge should have received SessionStolen.
        let stolen = other_rx.try_recv();
        assert!(
            matches!(&stolen, Ok(WsServerMessage::SessionStolen { .. })),
            "expected SessionStolen on killed bridge, got: {stolen:?}"
        );

        // Bridge should be removed from registry.
        assert!(
            conn.state.active_bridges.get(other_conv_id).await.is_none(),
            "bridge should be removed from registry after steal"
        );

        // Our connection should be detached.
        assert!(conn.current_conversation_id.is_none());

        // We should receive ConversationSwitched(None) + HistoryComplete + ArtifactIndex.
        let msgs = collect_messages(&mut ws_rx).await;
        let has_switched_none = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    conversation_id: None,
                    ..
                }
            )
        });
        let has_history_complete = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        let has_artifact_index = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ArtifactIndex { .. }));
        assert!(
            has_switched_none,
            "expected ConversationSwitched(None), got: {msgs:?}"
        );
        assert!(
            has_history_complete,
            "expected HistoryComplete, got: {msgs:?}"
        );
        assert!(has_artifact_index, "expected ArtifactIndex, got: {msgs:?}");
    }

    #[tokio::test]
    async fn new_conversation_clears_viewer_only() {
        let (mut conn, mut _ws_rx, db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        // Simulate being in viewer-only mode.
        let other_user_id = {
            let c = db.lock().await;
            create_user(&c, "otheruser", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, other_user_id, "test", false)
        };
        let (broadcast_tx, _) = broadcast::channel(64);
        let other_bridge = ActiveBridge::inject_for_test(
            other_user_id,
            other_conv_id,
            "test",
            db.clone(),
            broadcast_tx,
        );
        conn.attach_to_bridge(&other_bridge).await;
        conn.viewer_only = true;

        // Start a new conversation.
        conn.handle_new_conversation().await;

        assert!(
            !conn.viewer_only,
            "NewConversation should clear viewer_only"
        );
        assert!(
            conn.current_conversation_id.is_some(),
            "NewConversation should eagerly create a conversation"
        );
    }

    #[tokio::test]
    async fn switch_conversation_clears_viewer_only() {
        let (mut conn, mut _ws_rx, db, user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        // Create a conversation owned by this user.
        let own_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, user_id, "test", false)
        };

        // Simulate being in viewer-only mode.
        conn.viewer_only = true;
        conn.current_conversation_id = Some(999); // fake other user's conv

        // Switch to own conversation.
        conn.handle_switch_conversation(own_conv_id).await;

        assert!(
            !conn.viewer_only,
            "SwitchConversation should clear viewer_only"
        );
        assert_eq!(conn.current_conversation_id, Some(own_conv_id));
    }

    // -----------------------------------------------------------------------
    // Singleton app tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn singleton_new_conversation_rejected() {
        let (mut conn, mut ws_rx, _db, _user_id) =
            test_ws_conn_for_app(test_apps_singleton()).await;
        let ip = conn.client_ip;

        handle_client_message(r#"{"type":"NewConversation"}"#, &mut conn, ip).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            has_error,
            "expected Error for NewConversation in singleton app: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn singleton_switch_conversation_rejected() {
        let (mut conn, mut ws_rx, _db, _user_id) =
            test_ws_conn_for_app(test_apps_singleton()).await;
        let ip = conn.client_ip;

        handle_client_message(
            r#"{"type":"SwitchConversation","conversation_id":999}"#,
            &mut conn,
            ip,
        )
        .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            has_error,
            "expected Error for SwitchConversation in singleton app: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn singleton_list_conversations_is_noop() {
        let (mut conn, mut ws_rx, _db, _user_id) =
            test_ws_conn_for_app(test_apps_singleton()).await;
        let ip = conn.client_ip;

        handle_client_message(r#"{"type":"ListConversations"}"#, &mut conn, ip).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_list = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ConversationList { .. }));
        assert!(
            !has_list,
            "singleton app should not send ConversationList: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn singleton_reconnect_wrong_id_rejected() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps_singleton()).await;
        let ip = conn.client_ip;

        // Create the singleton conversation and set it as current.
        let conv_id = {
            let c = db.lock().await;
            conversation::get_or_create_singleton_conversation(&c, user_id, "test").id
        };
        conn.current_conversation_id = Some(conv_id);

        // Reconnect with a different conversation ID.
        let wrong_id = conv_id + 1;
        handle_client_message(
            &format!(r#"{{"type":"Reconnect","conversation_id":{wrong_id},"last_seq":null}}"#),
            &mut conn,
            ip,
        )
        .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            has_error,
            "expected Error for Reconnect to wrong conversation in singleton app: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn singleton_reconnect_correct_id_allowed() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps_singleton()).await;
        let ip = conn.client_ip;

        // Create the singleton conversation and set it as current.
        let conv_id = {
            let c = db.lock().await;
            conversation::get_or_create_singleton_conversation(&c, user_id, "test").id
        };
        conn.current_conversation_id = Some(conv_id);

        // Reconnect with the correct conversation ID — should not produce an Error.
        handle_client_message(
            &format!(r#"{{"type":"Reconnect","conversation_id":{conv_id},"last_seq":null}}"#),
            &mut conn,
            ip,
        )
        .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            !has_error,
            "Reconnect to correct conversation should not error in singleton app: {msgs:?}"
        );
    }

    /// A Reconnect to the conversation we're already attached to must
    /// not detach the bridge or emit a `ConversationSwitched`.
    #[tokio::test]
    async fn reconnect_to_same_conversation_does_not_detach() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps_singleton()).await;
        let ip = conn.client_ip;

        // Create the singleton conversation.
        let conv_id = {
            let c = db.lock().await;
            conversation::get_or_create_singleton_conversation(&c, user_id, "test").id
        };

        // Inject a bridge and attach. The default WsConnection has
        // `broadcast_rx: None`, which would bypass the early-return; we
        // must attach explicitly to exercise it.
        let (broadcast_tx, _broadcast_rx_other) = broadcast::channel(64);
        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        conn.state
            .active_bridges
            .insert(conv_id, bridge.clone())
            .await;
        conn.attach_to_bridge(&bridge).await;

        // Drain the initial PresenceUpdate from the attach.
        let _setup_msgs = collect_messages(&mut ws_rx).await;

        // Send Reconnect to the same conversation we're attached to.
        handle_client_message(
            &format!(r#"{{"type":"Reconnect","conversation_id":{conv_id},"last_seq":null}}"#),
            &mut conn,
            ip,
        )
        .await;

        let msgs = collect_messages(&mut ws_rx).await;

        // Bridge is still in the registry — no kill_session / detach ran.
        assert!(
            conn.state.active_bridges.get(conv_id).await.is_some(),
            "bridge should still be registered after Reconnect-to-self: {msgs:?}"
        );

        // No ConversationSwitched on same-conversation Reconnect.
        let has_conv_switched = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ConversationSwitched { .. }));
        assert!(
            !has_conv_switched,
            "Reconnect-to-self must not emit ConversationSwitched, got: {msgs:?}"
        );

        // No PresenceUpdate from this Reconnect — no add_subscriber was
        // called; subscriber count is unchanged.
        let presence_count = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::PresenceUpdate { .. }))
            .count();
        assert_eq!(
            presence_count, 0,
            "Reconnect-to-self must not emit PresenceUpdate, got: {msgs:?}"
        );

        // History resync ran — HistoryComplete is the visible signal.
        let has_history_complete = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        assert!(
            has_history_complete,
            "Reconnect-to-self must run history resync (expected HistoryComplete), got: {msgs:?}"
        );

        // Connection still attached to the same conversation — no detach.
        assert_eq!(conn.current_conversation_id, Some(conv_id));
        assert!(
            conn.broadcast_rx.is_some(),
            "broadcast_rx must remain populated (no detach)"
        );
    }

    #[tokio::test]
    async fn non_singleton_new_conversation_allowed() {
        // Verify non-singleton apps still allow NewConversation (regression check).
        let (mut conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;
        let ip = conn.client_ip;

        handle_client_message(r#"{"type":"NewConversation"}"#, &mut conn, ip).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            !has_error,
            "non-singleton app should allow NewConversation: {msgs:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Multiuser integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn multiuser_non_owner_can_send_to_shared_conversation() {
        // User B should be able to send to user A's shared conversation in a multiuser app.
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), Some(test_apps_multiuser()));

        let (ws_tx, mut ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        // Alice creates a shared conversation with a live bridge.
        let (alice_id, bob_id, conv_id, bob_device_id) = {
            let conn = db.lock().await;
            let alice = create_user(&conn, "alice", "$argon2id$fake");
            let bob = create_user(&conn, "bob", "$argon2id$fake");
            let _alice_did = create_test_device(&conn, alice);
            let bob_did = create_test_device(&conn, bob);
            let cid = conversation::create_conversation(&conn, alice, "test", true);
            (alice, bob, cid, bob_did)
        };

        let bridge = ActiveBridge::inject_for_test_shared(
            alice_id,
            conv_id,
            "test",
            true,
            db.clone(),
            broadcast_tx,
        );
        state.active_bridges.insert(conv_id, bridge.clone()).await;

        // Bob connects and is pointed at Alice's shared conversation.
        let mut conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            broadcast_rx: Some(bridge.subscribe()),
            ..WsConnBuilder::with_defaults(
                bob_id,
                "bob".to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                bob_device_id,
            )
        }
        .build();

        // Bob sends a message — should succeed (not get Error/AppBusy).
        conn.handle_send_message("Hello from Bob", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        // A CC-send-failure error IS expected (injected bridge has no CC session).
        let cc_send_error = msgs.iter().find(|m| is_cc_send_failure_error(m));
        assert!(
            cc_send_error.is_some(),
            "expected CC-send-failure error frame (injected bridge has no session), got: {msgs:?}"
        );
        // Should NOT get an auth error or AppBusy.
        let has_authz_error = msgs.iter().any(|m| match m {
            WsServerMessage::AppBusy { .. } => true,
            WsServerMessage::Error { .. } => !is_cc_send_failure_error(m),
            _ => false,
        });
        assert!(
            !has_authz_error,
            "non-owner should be able to send to shared multiuser conversation, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn multiuser_non_owner_denied_private_conversation() {
        // User B should NOT be able to send to user A's private conversation,
        // even in a multiuser app.
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), Some(test_apps_multiuser()));

        let (ws_tx, mut ws_rx) = mpsc::channel(256);

        // Alice creates a PRIVATE conversation (no bridge — tests the resume path).
        let (_alice_id, bob_id, conv_id, bob_device_id) = {
            let conn = db.lock().await;
            let alice = create_user(&conn, "alice", "$argon2id$fake");
            let bob = create_user(&conn, "bob", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, alice, "test", false);
            conversation::complete_conversation(&conn, cid, None);
            let did = create_test_device(&conn, bob);
            (alice, bob, cid, did)
        };

        let mut conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            ..WsConnBuilder::with_defaults(
                bob_id,
                "bob".to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                bob_device_id,
            )
        }
        .build();

        conn.handle_send_message("Sneaky message", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        let has_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message == "Not authorized"),
        );
        assert!(
            has_error,
            "non-owner should be denied private conversation in multiuser app, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn single_instance_multiuser_auto_attach_can_send() {
        // In a single_instance + multiuser app, auto-attached users should NOT be viewer_only
        // for shared conversations — they should be able to participate.
        let (mut conn, mut ws_rx, db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance_multiuser()).await;

        // Create another user's shared bridge.
        let other_user_id = {
            let c = db.lock().await;
            create_user(&c, "otheruser", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, other_user_id, "test", true)
        };
        let (broadcast_tx, _) = broadcast::channel(64);
        let other_bridge = ActiveBridge::inject_for_test_shared(
            other_user_id,
            other_conv_id,
            "test",
            true,
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(other_conv_id, other_bridge.clone())
            .await;

        // Simulate the auto-attach path from handle_ws — the key behavior
        // is that viewer_only should be false for multiuser + shared.
        conn.attach_to_bridge(&other_bridge).await;
        // This is the fix: !(multiuser && bridge.shared) = !(true && true) = false
        conn.viewer_only = !other_bridge
            .shared
            .load(std::sync::atomic::Ordering::Relaxed);

        // User should be able to send (not get AppBusy).
        conn.handle_send_message("hello", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_app_busy = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::AppBusy { .. }));
        assert!(
            !has_app_busy,
            "multiuser participant should not get AppBusy, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn single_instance_non_multiuser_auto_attach_is_viewer_only() {
        // Regression: in non-multiuser single_instance, auto-attach should still be viewer_only.
        let (mut conn, mut ws_rx, db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        let other_user_id = {
            let c = db.lock().await;
            create_user(&c, "otheruser", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, other_user_id, "test", false)
        };
        let (broadcast_tx, _) = broadcast::channel(64);
        let other_bridge = ActiveBridge::inject_for_test(
            other_user_id,
            other_conv_id,
            "test",
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(other_conv_id, other_bridge.clone())
            .await;

        // Simulate auto-attach as viewer (non-multiuser).
        conn.attach_to_bridge(&other_bridge).await;
        conn.viewer_only = true; // non-multiuser → viewer_only

        conn.handle_send_message("hello", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_app_busy = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::AppBusy { .. }));
        assert!(
            has_app_busy,
            "non-multiuser auto-attach should be viewer_only, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn set_timezone_updates_connection() {
        let (mut conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Default is UTC.
        assert_eq!(conn.timezone, chrono_tz::Tz::UTC);

        // Process a SetTimezone message.
        handle_client_message(
            r#"{"type":"SetTimezone","timezone":"Asia/Tokyo"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        assert_eq!(conn.timezone, chrono_tz::Asia::Tokyo);
    }

    #[tokio::test]
    async fn set_timezone_invalid_keeps_current() {
        let (mut conn, _ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        handle_client_message(
            r#"{"type":"SetTimezone","timezone":"Not/A/Zone"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        // Should still be UTC (invalid timezone rejected).
        assert_eq!(conn.timezone, chrono_tz::Tz::UTC);
    }

    #[tokio::test]
    async fn set_viewport_class_compact_sends_single_pane() {
        let (mut conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Default is Wide.
        assert_eq!(conn.viewport_class, ViewportClass::Wide);

        handle_client_message(
            r#"{"type":"SetViewportClass","viewport_class":"Compact"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        assert_eq!(conn.viewport_class, ViewportClass::Compact);

        let msgs = collect_messages(&mut ws_rx).await;
        let layout_msg = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::SetLayout { .. }));
        assert!(layout_msg.is_some(), "expected SetLayout, got: {msgs:?}");
        match layout_msg.unwrap() {
            WsServerMessage::SetLayout { layout } => {
                assert_eq!(*layout, PaneLayout::SinglePane);
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn set_viewport_class_wide_sends_two_column() {
        let (mut conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Set to Compact first, then back to Wide.
        handle_client_message(
            r#"{"type":"SetViewportClass","viewport_class":"Compact"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;
        // Drain the Compact → SinglePane message.
        let _ = collect_messages(&mut ws_rx).await;

        handle_client_message(
            r#"{"type":"SetViewportClass","viewport_class":"Wide"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        assert_eq!(conn.viewport_class, ViewportClass::Wide);

        let msgs = collect_messages(&mut ws_rx).await;
        let layout_msg = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::SetLayout { .. }));
        assert!(layout_msg.is_some(), "expected SetLayout, got: {msgs:?}");
        match layout_msg.unwrap() {
            WsServerMessage::SetLayout { layout } => {
                assert_eq!(*layout, PaneLayout::TwoColumn);
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn new_conversation_in_multiuser_app_is_shared() {
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), Some(test_apps_multiuser()));
        let (ws_tx, _ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        let (user_id, dummy_conv_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            let cid = conversation::create_conversation(&conn, uid, TEST_APP_SLUG, false);
            (uid, cid, did)
        };

        let test_bridge = ActiveBridge::inject_for_test(
            user_id,
            dummy_conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );

        let mut conn = WsConnBuilder {
            // current_conversation_id: None (default) — new conversation path.
            test_bridge: Some(test_bridge),
            ..WsConnBuilder::with_defaults(
                user_id,
                TEST_USERNAME.to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        conn.handle_send_message("First message", vec![], None, vec![])
            .await;

        // handle_send_message creates a new conversation (different from dummy_conv_id).
        // Note: current_conversation_id is set to the test bridge's conversation_id
        // by attach_to_bridge (a test artifact), so we look for the new conversation
        // by finding the one we didn't create.
        let db_conn = db.lock().await;
        let all_convs = conversation::list_conversations(&db_conn, user_id, TEST_APP_SLUG);
        // Should have 2: the dummy (shared=false) and the new one (shared=true).
        assert_eq!(
            all_convs.len(),
            2,
            "expected 2 conversations, got {}",
            all_convs.len()
        );
        let new_conv = all_convs
            .iter()
            .find(|c| c.id != dummy_conv_id)
            .expect("should have a new conversation besides the dummy");
        assert!(
            new_conv.shared,
            "new conversation in multiuser app should be shared"
        );
    }

    // -----------------------------------------------------------------------
    // Privacy toggle tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn privacy_toggle_owner_can_make_private() {
        let (mut conn, mut ws_rx, db, _alice, _bob, conv_id) =
            test_multiuser_conn_for_privacy().await;

        conn.handle_set_conversation_privacy(conv_id, false).await;

        let msgs = collect_messages(&mut ws_rx).await;

        // Should NOT get an error.
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(!has_error, "owner should be able to toggle, got: {msgs:?}");

        // Should get PrivacyChanged directly (for immediate UI update).
        let has_privacy = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::PrivacyChanged { shared: false, .. }));
        assert!(
            has_privacy,
            "expected PrivacyChanged sent directly, got: {msgs:?}"
        );

        // Should get a ConversationList refresh.
        let has_list = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ConversationList { .. }));
        assert!(has_list, "expected ConversationList, got: {msgs:?}");

        // DB should be updated.
        let db_conn = db.lock().await;
        let conv = conversation::get_conversation(&db_conn, conv_id);
        assert!(!conv.shared, "conversation should be private after toggle");
    }

    #[tokio::test]
    async fn privacy_toggle_owner_can_make_shared() {
        let (mut conn, mut ws_rx, db, _alice, _bob, conv_id) =
            test_multiuser_conn_for_privacy().await;

        // First make it private.
        {
            let db_conn = db.lock().await;
            conversation::set_conversation_shared(&db_conn, conv_id, false);
        }

        conn.handle_set_conversation_privacy(conv_id, true).await;

        let msgs = collect_messages(&mut ws_rx).await;

        let has_error = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(!has_error, "owner should be able to toggle, got: {msgs:?}");

        let db_conn = db.lock().await;
        let conv = conversation::get_conversation(&db_conn, conv_id);
        assert!(conv.shared, "conversation should be shared after toggle");
    }

    #[tokio::test]
    async fn privacy_toggle_non_owner_rejected() {
        let (mut conn, mut ws_rx, _db, _alice, bob_id, conv_id) =
            test_multiuser_conn_for_privacy().await;

        // Switch connection to bob (non-owner).
        conn.user_id = bob_id;
        conn.username = "bob".to_string();

        conn.handle_set_conversation_privacy(conv_id, false).await;

        let msgs = collect_messages(&mut ws_rx).await;

        let has_error = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::Error { message } if message.contains("owner")
            )
        });
        assert!(has_error, "non-owner should get error, got: {msgs:?}");
    }

    #[tokio::test]
    async fn privacy_toggle_non_multiuser_app_rejected() {
        // Use the non-multiuser test_apps().
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);

        let (ws_tx, mut ws_rx) = mpsc::channel(256);
        let (user_id, conv_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            let cid = conversation::create_conversation(&conn, uid, TEST_APP_SLUG, false);
            (uid, cid, did)
        };

        let mut conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            ..WsConnBuilder::with_defaults(
                user_id,
                TEST_USERNAME.to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        conn.handle_set_conversation_privacy(conv_id, true).await;

        let msgs = collect_messages(&mut ws_rx).await;

        let has_error = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::Error { message } if message.contains("multiuser")
            )
        });
        assert!(
            has_error,
            "non-multiuser app should get error, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn privacy_toggle_wrong_app_rejected() {
        let (mut conn, mut ws_rx, db, _alice, _bob, _conv_id) =
            test_multiuser_conn_for_privacy().await;

        // Create a conversation in a different app.
        let other_conv_id = {
            let db_conn = db.lock().await;
            conversation::create_conversation(&db_conn, conn.user_id, "other_app", true)
        };

        conn.handle_set_conversation_privacy(other_conv_id, false)
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        let has_error = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::Error { message } if message.contains("not found")
            )
        });
        assert!(
            has_error,
            "cross-app privacy toggle should be rejected, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn privacy_toggle_broadcasts_to_bridge() {
        let (mut conn, _ws_rx, db, alice_id, _bob, conv_id) =
            test_multiuser_conn_for_privacy().await;

        // Register a bridge and subscribe.
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<WsServerMessage>(64);
        let bridge = ActiveBridge::inject_for_test_shared(
            alice_id,
            conv_id,
            "test",
            true,
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(conv_id, bridge.clone())
            .await;

        conn.handle_set_conversation_privacy(conv_id, false).await;

        // Check that PrivacyChanged was broadcast.
        let mut found_privacy = false;
        while let Ok(msg) = broadcast_rx.try_recv() {
            if matches!(msg, WsServerMessage::PrivacyChanged { conversation_id: cid, shared: false } if cid == conv_id)
            {
                found_privacy = true;
            }
        }
        assert!(found_privacy, "expected PrivacyChanged broadcast");

        // Check that bridge.shared was updated.
        assert!(
            !bridge.shared.load(std::sync::atomic::Ordering::Relaxed),
            "bridge.shared should be false after toggle"
        );
    }

    // === try_select_requested_conversation tests ===

    #[tokio::test]
    async fn select_requested_conv_loads_own_conversation() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps()).await;

        // Create a conversation for this user.
        let conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, user_id, "test", false)
        };

        let result = conn
            .try_select_requested_conversation(conv_id, false, None)
            .await;
        assert_eq!(
            result,
            Ok(true),
            "should return true for accessible conversation"
        );
        assert_eq!(conn.current_conversation_id, Some(conv_id));

        let msgs = collect_messages(&mut ws_rx).await;
        let switched = msgs.iter().find(|m| {
            matches!(m, WsServerMessage::ConversationSwitched { conversation_id: Some(id), .. } if *id == conv_id)
        });
        assert!(
            switched.is_some(),
            "expected ConversationSwitched with conv_id={conv_id}, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn select_requested_conv_rejects_other_users_conversation() {
        let (mut conn, mut ws_rx, db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Create another user and their conversation.
        let other_user_id = {
            let c = db.lock().await;
            create_user(&c, "otheruser", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, other_user_id, "test", false)
        };

        let result = conn
            .try_select_requested_conversation(other_conv_id, false, None)
            .await;
        assert_eq!(
            result,
            Ok(false),
            "should return false for unauthorized conversation"
        );
        assert_eq!(conn.current_conversation_id, None);

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::Error { message } if message.contains("Not authorized")));
        assert!(has_error, "expected 'Not authorized' error, got: {msgs:?}");
    }

    #[tokio::test]
    async fn select_requested_conv_rejects_nonexistent() {
        let (mut conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        let result = conn
            .try_select_requested_conversation(99999, false, None)
            .await;
        assert_eq!(
            result,
            Ok(false),
            "should return false for nonexistent conversation"
        );

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message.contains("not found")),
        );
        assert!(has_error, "expected 'not found' error, got: {msgs:?}");
    }

    #[tokio::test]
    async fn select_requested_conv_rejects_wrong_app() {
        let (mut conn, mut ws_rx, db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        // Create a conversation for a different app.
        let conv_id = {
            let c = db.lock().await;
            let uid = create_user(&c, "someone", "$argon2id$fake");
            conversation::create_conversation(&c, uid, "other-app", false)
        };

        let result = conn
            .try_select_requested_conversation(conv_id, false, None)
            .await;
        assert_eq!(
            result,
            Ok(false),
            "should return false for wrong-app conversation"
        );

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message.contains("not found")),
        );
        assert!(has_error, "expected 'not found' error, got: {msgs:?}");
    }

    // -----------------------------------------------------------------------
    // Eager-spawn: CcState::Connecting, history_sent, empty-conversation reuse
    // -----------------------------------------------------------------------

    /// BridgeSpawned incremental re-replay race: a message persisted by
    /// `drain_pending_events` AFTER the initial `send_history` call must appear
    /// exactly once — not zero times and not twice — when the `BridgeSpawned`
    /// handler fires `send_history(from_seq = last_sent_seq)`.
    ///
    /// This test covers the sequential shape. See
    /// `bridge_spawn_race_live_broadcast_no_double_delivery` for the concurrent shape.
    ///
    /// Scenario:
    ///   1. `send_history(None)` — sends seq 0 (user message), sets `last_sent_seq = Some(0)`.
    ///   2. `drain_pending_events` writes seq 1 (a system message) to the DB.
    ///   3. `BridgeSpawned` fires → incremental `send_history(Some(0))` delivers seq 1.
    ///   4. Assert: seq 0 appears once in total; seq 1 appears exactly once.
    #[tokio::test]
    async fn bridge_spawned_incremental_replay_delivers_drain_rows_exactly_once() {
        let (mut conn, mut ws_rx, db, user_id, conv_id) = test_ws_conn_with_resume_conv().await;

        // Step 1: Add a user message (seq 0) and send full history.
        {
            let db_conn = db.lock().await;
            brenn_lib::conversation::append_message(
                &db_conn,
                conv_id,
                brenn_lib::conversation::MessageDirection::Outgoing,
                "user",
                None,
                None,
                r#"{"type":"user","message":{"role":"user","content":"initial"}}"#,
                Some(user_id),
                Some("UTC"),
                None,
            );
        }
        conn.send_history(conv_id, None)
            .await
            .expect("send_history full");
        let after_initial = collect_messages(&mut ws_rx).await;
        let initial_echoes: Vec<_> = after_initial
            .iter()
            .filter(|m| matches!(m, WsServerMessage::UserMessageEcho { .. }))
            .collect();
        assert_eq!(
            initial_echoes.len(),
            1,
            "initial send_history must deliver exactly one UserMessageEcho: {after_initial:?}"
        );

        // Verify last_sent_seq was advanced.
        let seq_after_initial = conn.last_sent_seq;
        assert!(
            seq_after_initial.is_some(),
            "last_sent_seq must be set after send_history with messages"
        );

        // Step 2: Simulate drain_pending_events writing a row after send_history.
        {
            let db_conn = db.lock().await;
            brenn_lib::conversation::append_message(
                &db_conn,
                conv_id,
                brenn_lib::conversation::MessageDirection::Outgoing,
                "assistant",
                None,
                None,
                r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"drain row"}]}}"#,
                None,
                None,
                None,
            );
        }

        // Step 3: BridgeSpawned incremental replay — uses last_sent_seq as from_seq.
        conn.send_history(conv_id, seq_after_initial)
            .await
            .expect("send_history incremental");
        let after_bridge_spawned = collect_messages(&mut ws_rx).await;

        // The drain row (AssistantMessage seq 1) must appear exactly once here.
        let assistant_msgs: Vec<_> = after_bridge_spawned
            .iter()
            .filter(|m| matches!(m, WsServerMessage::AssistantMessage { .. }))
            .collect();
        assert_eq!(
            assistant_msgs.len(),
            1,
            "incremental replay must deliver the drain row exactly once: {after_bridge_spawned:?}"
        );

        // The initial user message (seq 0) must NOT be replayed again.
        let echoes: Vec<_> = after_bridge_spawned
            .iter()
            .filter(|m| matches!(m, WsServerMessage::UserMessageEcho { .. }))
            .collect();
        assert_eq!(
            echoes.len(),
            0,
            "incremental replay must not re-deliver already-sent seq 0: {after_bridge_spawned:?}"
        );
    }

    /// Switch to a conversation with no active bridge → CcState::Connecting.
    #[tokio::test]
    async fn switch_conversation_emits_connecting_when_no_bridge() {
        let (mut conn, mut ws_rx, db, user_id, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Create a second conversation with no bridge.
        let new_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, user_id, "test", false)
        };

        conn.handle_switch_conversation(new_conv_id).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let switched = msgs.iter().find_map(|m| match m {
            WsServerMessage::ConversationSwitched { state, .. } => Some(*state),
            _ => None,
        });
        assert_eq!(
            switched,
            Some(CcState::Connecting),
            "expected CcState::Connecting when switching to a conversation with no bridge, got: {msgs:?}"
        );
    }

    /// Switch to a conversation with an active bridge → CcState::Idle (not Connecting).
    #[tokio::test]
    async fn switch_conversation_emits_idle_when_bridge_exists() {
        let (mut conn, mut ws_rx, db, user_id, conv_id) = test_ws_conn_with_resume_conv().await;

        // Put the bridge in active_bridges (fast path).
        let (broadcast_tx, _) = broadcast::channel(64);
        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        conn.state.active_bridges.insert(conv_id, bridge).await;

        conn.handle_switch_conversation(conv_id).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let switched = msgs.iter().find_map(|m| match m {
            WsServerMessage::ConversationSwitched { state, .. } => Some(*state),
            _ => None,
        });
        assert_eq!(
            switched,
            Some(CcState::Idle),
            "expected CcState::Idle when switching to a conversation with an idle bridge, got: {msgs:?}"
        );
    }

    /// Switch conversation emits a TodoState message for graf-enabled apps.
    ///
    /// Uses a graf-enabled app config pointing to a nonexistent binary.
    /// `send_todo_state` fails to exec the subprocess, falls into the error
    /// branch, and emits an empty `TodoState` — confirming the call exists
    /// and that a non-None graf_config is passed through correctly.
    #[tokio::test]
    async fn switch_conversation_emits_todo_state() {
        let (mut conn, mut ws_rx, db, user_id, _conv_id) =
            test_ws_conn_with_resume_conv_and_apps(test_apps_with_failing_graf()).await;

        let new_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, user_id, "test", false)
        };

        conn.handle_switch_conversation(new_conv_id).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_todo_state = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::TodoState { .. }));
        assert!(
            has_todo_state,
            "expected TodoState after switch_conversation, got: {msgs:?}"
        );
    }

    /// handle_new_conversation for persistent app: eager create + CcState::Connecting.
    #[tokio::test]
    async fn new_conversation_persistent_app_emits_connecting() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_persistent().await;

        conn.handle_new_conversation().await;

        let msgs = collect_messages(&mut ws_rx).await;
        let switched = msgs.iter().find_map(|m| match m {
            WsServerMessage::ConversationSwitched { state, .. } => Some(*state),
            _ => None,
        });
        assert_eq!(
            switched,
            Some(CcState::Connecting),
            "persistent app should emit CcState::Connecting on new conversation, got: {msgs:?}"
        );
        assert!(
            conn.current_conversation_id.is_some(),
            "should eagerly create conversation"
        );
    }

    /// handle_new_conversation for non-persistent app: eager create + CcState::Idle.
    #[tokio::test]
    async fn new_conversation_non_persistent_app_emits_idle() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_new_conversation().await;

        let msgs = collect_messages(&mut ws_rx).await;
        let switched = msgs.iter().find_map(|m| match m {
            WsServerMessage::ConversationSwitched { state, .. } => Some(*state),
            _ => None,
        });
        assert_eq!(
            switched,
            Some(CcState::Idle),
            "non-persistent app should emit CcState::Idle on new conversation, got: {msgs:?}"
        );
        assert!(
            conn.current_conversation_id.is_some(),
            "should eagerly create conversation"
        );
    }

    /// handle_new_conversation reuses an empty conversation instead of creating a new one.
    #[tokio::test]
    async fn new_conversation_reuses_empty_conversation() {
        let (mut conn, mut _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // conv_id exists but has no messages (test setup creates it completed, so
        // reactivate it to make it "active" for find_empty_conversation).
        {
            let c = db.lock().await;
            conversation::reactivate_conversation(&c, conv_id);
        }
        // Detach so the bridge doesn't block reuse.
        conn.detach().await;
        // Remove bridge from active_bridges so the reuse guard passes.
        conn.state.active_bridges.remove(conv_id).await;

        conn.handle_new_conversation().await;

        assert_eq!(
            conn.current_conversation_id,
            Some(conv_id),
            "should reuse the existing empty conversation"
        );
    }

    /// handle_new_conversation creates a new one when the empty conversation has a bridge.
    #[tokio::test]
    async fn new_conversation_does_not_reuse_if_bridge_exists() {
        let (mut conn, mut _ws_rx, db, user_id, conv_id) = test_ws_conn_with_resume_conv().await;

        // Reactivate the conversation so it's findable.
        {
            let c = db.lock().await;
            conversation::reactivate_conversation(&c, conv_id);
        }
        // But leave the bridge in active_bridges — this should prevent reuse.
        let (broadcast_tx, _) = broadcast::channel(64);
        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        conn.state.active_bridges.insert(conv_id, bridge).await;
        conn.detach().await;

        conn.handle_new_conversation().await;

        assert_ne!(
            conn.current_conversation_id,
            Some(conv_id),
            "should NOT reuse conversation with an active bridge"
        );
        assert!(
            conn.current_conversation_id.is_some(),
            "should create a new conversation instead"
        );
    }

    // ── try_select_requested_conversation — WS-dead Err(()) propagation ─────

    /// Sequential post-broadcast cursor state: incremental replay must not double-deliver
    /// a message that was already forwarded by the live-broadcast path.
    ///
    /// This tests the sequential shape of the BridgeSpawned race (complementing
    /// `bridge_spawned_incremental_replay_delivers_drain_rows_exactly_once` which
    /// also covers the sequential shape). The concurrent shape — where a live-broadcast
    /// forward interleaves with the SQL SELECT inside `send_history` — is not tested
    /// here; exercising that would require a real event_loop task racing against
    /// `send_history` (out of scope for this unit test).
    ///
    /// Scenario:
    ///   1. `send_history(None)` delivers seq 0 (user message), sets `last_sent_seq = Some(0)`.
    ///   2. Seq 1 (assistant message) is written to the DB AND forwarded via live
    ///      broadcast (simulated by direct send + manual cursor advance).
    ///   3. `BridgeSpawned` fires → incremental `send_history(Some(last_sent_seq))`.
    ///      Since `last_sent_seq = Some(1)` (already advanced by the live forward),
    ///      incremental replay sends `seq > 1` — zero messages. Seq 1 was already
    ///      delivered via the live broadcast path; it must not appear again.
    #[tokio::test]
    async fn bridge_spawn_race_live_broadcast_no_double_delivery() {
        let (mut conn, mut ws_rx, db, user_id, conv_id) = test_ws_conn_with_resume_conv().await;

        // Step 1: Insert a user message (seq 0) and send full history.
        {
            let db_conn = db.lock().await;
            brenn_lib::conversation::append_message(
                &db_conn,
                conv_id,
                brenn_lib::conversation::MessageDirection::Outgoing,
                "user",
                None,
                None,
                r#"{"type":"user","message":{"role":"user","content":"initial"}}"#,
                Some(user_id),
                Some("UTC"),
                None,
            );
        }
        conn.send_history(conv_id, None)
            .await
            .expect("send_history full");
        let _ = collect_messages(&mut ws_rx).await; // drain initial batch

        assert_eq!(
            conn.last_sent_seq,
            Some(0),
            "last_sent_seq must be Some(0) after initial send_history"
        );

        // Step 2: Write seq 1 to DB AND simulate the event_loop live broadcast
        // forward: send the message directly to ws_rx and advance last_sent_seq
        // as event_loop.rs would.
        let assistant_msg = WsServerMessage::AssistantMessage {
            content: "<p>live response</p>".to_string(),
            seq: Some(1),
        };
        {
            let db_conn = db.lock().await;
            brenn_lib::conversation::append_message(
                &db_conn,
                conv_id,
                brenn_lib::conversation::MessageDirection::Incoming,
                "assistant",
                None,
                None,
                r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"live response"}]}}"#,
                None,
                None,
                None,
            );
        }
        // Simulate event_loop.rs forwarding the live broadcast to the tab's channel
        // and advancing last_sent_seq (lines 252-259 of event_loop.rs).
        //
        // NOTE: this is a manual simulation of the event_loop broadcast-forward path.
        // The actual event_loop advances last_sent_seq only on SendResult::Ok; here
        // we set it directly. A fully concurrent integration test (a real broadcast
        // task racing against send_history) is not attempted — the existing test
        // infrastructure does not wire up a live event_loop alongside send_history.
        // This test validates the property (no double delivery) under the assumption
        // that the cursor advance is correct; a regression in the advance logic itself
        // would not be caught here.
        assert_eq!(
            conn.send_ws(assistant_msg),
            super::super::connection::SendResult::Ok,
            "live-broadcast send must succeed (channel must not be full or closed)"
        );
        conn.last_sent_seq = Some(1); // event_loop advances on successful forward
        assert_eq!(
            conn.last_sent_seq,
            Some(1),
            "last_sent_seq must reflect the simulated live-broadcast advance"
        );

        // Drain the live-broadcast-forwarded message from ws_rx.
        let live_msgs = collect_messages(&mut ws_rx).await;
        let live_assistant: Vec<_> = live_msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::AssistantMessage { .. }))
            .collect();
        assert_eq!(
            live_assistant.len(),
            1,
            "live broadcast must deliver exactly one AssistantMessage: {live_msgs:?}"
        );

        // Step 3: BridgeSpawned fires incremental replay from last_sent_seq=Some(1).
        // SQL `seq > 1` returns 0 rows — seq 1 was already delivered via live path.
        conn.send_history(conv_id, conn.last_sent_seq)
            .await
            .expect("incremental send_history");
        let after_replay = collect_messages(&mut ws_rx).await;

        // Seq 1 must NOT appear again — no double delivery.
        let replay_assistant: Vec<_> = after_replay
            .iter()
            .filter(|m| matches!(m, WsServerMessage::AssistantMessage { .. }))
            .collect();
        assert_eq!(
            replay_assistant.len(),
            0,
            "incremental replay must not re-deliver seq 1 already forwarded by live broadcast: \
             {after_replay:?}"
        );
        // Seq 0 must also not re-appear.
        let replay_echoes: Vec<_> = after_replay
            .iter()
            .filter(|m| matches!(m, WsServerMessage::UserMessageEcho { .. }))
            .collect();
        assert_eq!(
            replay_echoes.len(),
            0,
            "incremental replay must not re-deliver seq 0 from initial history: {after_replay:?}"
        );
    }

    // ── try_select_requested_conversation — WS-dead Err(()) propagation ─────

    /// When the WS channel is closed before history delivery completes,
    /// `try_select_requested_conversation` must return `Err(())`.
    /// This locks in the invariant introduced by the `ws-dead-is-closed-fragile`
    /// fix: the function no longer polls `is_closed()` after the fact but
    /// propagates the `Err(())` from `send_history` directly.
    #[tokio::test]
    async fn select_requested_conv_returns_err_when_ws_dead_during_history() {
        // Use a 2-slot buffer so we can fill it and then drop the receiver to
        // simulate a dead connection.
        let (mut conn, ws_rx, db, user_id) = test_ws_conn_with_channel(2).await;

        // Create a conversation with enough messages to overflow a 2-slot buffer.
        let conv_id = {
            let c = db.lock().await;
            let cid = conversation::create_conversation(&c, user_id, "test", false);
            for i in 0..10 {
                let _ = conversation::append_message(
                    &c,
                    cid,
                    brenn_lib::conversation::MessageDirection::Outgoing,
                    "user",
                    None,
                    None,
                    &format!(
                        r#"{{"type":"user","message":{{"role":"user","content":"msg {i}"}}}}"#
                    ),
                    Some(user_id),
                    Some("UTC"),
                    None,
                );
            }
            cid
        };

        // Drop the receiver — simulates the WS connection dying.
        drop(ws_rx);

        let result = conn
            .try_select_requested_conversation(conv_id, false, None)
            .await;
        assert!(
            result.is_err(),
            "try_select_requested_conversation must return Err(()) when the WS channel is closed \
             during history delivery; got: {result:?}"
        );
    }
}

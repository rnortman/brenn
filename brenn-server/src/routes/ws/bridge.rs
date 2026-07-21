//! `ensure_bridge_starting`, `drain_queued_responses`, `spawn_bridge`.

use std::sync::Arc;

use brenn_lib::ws_types::WsServerMessage;

use super::connection::{QueuedResponse, WsConnection};
use crate::active_bridge::ActiveBridge;
#[cfg(not(test))]
use crate::active_bridge::SpawnContext;

// impl WsConnection — bridge lifecycle (ensure, drain, spawn)
impl WsConnection {
    /// Ensure a bridge is being started for the given conversation.
    /// Called when an approval response arrives but no bridge exists.
    /// Spawns/resumes a bridge so that `BridgeSpawned` fires and
    /// queued approvals are drained.
    pub(super) async fn ensure_bridge_starting(&mut self, conversation_id: i64) {
        // If we already have a bridge, nothing to do.
        if self
            .state
            .active_bridges
            .get(conversation_id)
            .await
            .is_some()
        {
            return;
        }

        // Look up the conversation to get the resume session ID.
        let conv = {
            let conn = self.state.db.lock().await;
            brenn_lib::conversation::get_conversation_opt(&conn, conversation_id)
        };
        let conv = match conv {
            Some(c) => c,
            None => {
                tracing::warn!(
                    conversation_id,
                    "ensure_bridge_starting: conversation not found"
                );
                return;
            }
        };
        let resume_id = conv.cc_session_id.clone();
        let shared = conv.shared;

        // Reactivate if completed/errored.
        if conv.status != brenn_lib::conversation::ConversationStatus::Active {
            let conn = self.state.db.lock().await;
            brenn_lib::conversation::reactivate_conversation(&conn, conversation_id);
        }

        let bridge_result = self
            .spawn_bridge(conversation_id, shared, resume_id, None)
            .await
            .map(|(bridge, _)| bridge);

        match bridge_result {
            Ok(bridge) => {
                tracing::info!(conversation_id, "bridge started for queued response");
                // Drain queued responses now — the BridgeSpawned handler won't
                // drain them because spawn_bridge already attached (broadcast_rx is set).
                self.drain_queued_responses(&bridge).await;
            }
            Err(e) => {
                tracing::error!(conversation_id, "failed to start bridge for response: {e}");
                // Drain queued responses as errors.
                let queued = std::mem::take(&mut self.queued_responses);
                for response in queued {
                    let request_id = match &response {
                        QueuedResponse::Permission { request_id, .. } => request_id,
                        QueuedResponse::ToolCard { request_id, .. } => request_id,
                    };
                    let _ = self.send_ws(WsServerMessage::Error {
                        message: format!("Failed to start Claude for request {request_id}: {e}"),
                    });
                }
            }
        }
    }

    /// Drain queued responses through an active bridge.
    pub(super) async fn drain_queued_responses(&mut self, bridge: &ActiveBridge) {
        let queued = std::mem::take(&mut self.queued_responses);
        for response in queued {
            match response {
                QueuedResponse::Permission {
                    request_id,
                    decision,
                } => {
                    tracing::info!(request_id = %request_id, "draining queued permission response");
                    bridge
                        .handle_permission_response(&request_id, decision)
                        .await;
                }
                QueuedResponse::ToolCard {
                    request_id,
                    decision,
                } => {
                    tracing::info!(request_id = %request_id, "draining queued tool card response");
                    bridge
                        .handle_tool_card_response(&request_id, decision)
                        .await;
                }
            }
        }
    }

    /// Spawn (or resume) a CC subprocess, register it, and subscribe.
    ///
    /// `shared` must reflect the conversation's actual `shared` column (not blindly the
    /// app-level `multiuser` flag — a private conversation in a multiuser app stays private).
    pub(super) async fn spawn_bridge(
        &mut self,
        conversation_id: i64,
        shared: bool,
        resume_session_id: Option<String>,
        model_override: Option<&str>,
    ) -> Result<(Arc<ActiveBridge>, Vec<String>), String> {
        // Single-instance enforcement: block spawn if another session exists for this app.
        if self.check_single_instance_blocked().await {
            return Err("single-instance app is busy".to_string());
        }

        // In tests, return the injected bridge instead of spawning CC.
        // Tests have no real CC event loop, so subscribe() is fine —
        // there's no race because nothing is broadcasting.
        // resume_session_id/model_override are not meaningful for an injected test bridge;
        // shared is not threaded through active_bridges::insert (bridge carries it already).
        #[cfg(test)]
        let (bridge, hook_warnings) = {
            let _ = (shared, resume_session_id, model_override);
            match self.test_bridge.take() {
                Some(bridge) => {
                    let rx = bridge.subscribe();
                    self.attach_to_bridge_with_rx(&bridge, rx).await;
                    // Register in active_bridges (mirrors what spawn_and_register_bridge does).
                    self.state
                        .active_bridges
                        .insert(conversation_id, bridge.clone())
                        .await;
                    // Deliver any pending tool results accumulated while CC was down.
                    bridge.deliver_pending_results().await;
                    // Notify other WS connections watching this conversation.
                    if self
                        .state
                        .bridge_notify_tx
                        .send(crate::state::BridgeSpawned {
                            conversation_id,
                            app_slug: self.app_slug.clone(),
                        })
                        .is_err()
                    {
                        tracing::debug!("bridge spawn notification with no listeners");
                    }
                    (bridge, Vec::new())
                }
                None => {
                    return Err("no test bridge injected".to_string());
                }
            }
        };

        #[cfg(not(test))]
        let (bridge, hook_warnings) = {
            let app_config = self.app_config();
            let alert_dispatcher = self
                .state
                .alert_dispatcher
                .with_field("App", &self.app_slug)
                .with_field("User", &self.username)
                .with_field("Conversation", conversation_id.to_string())
                .with_field("Lifecycle", "active");
            let (bridge, rx, warnings, model_infos) = self
                .state
                .spawn_and_register_bridge(SpawnContext {
                    user_id: self.user_id,
                    conversation_id,
                    shared,
                    db: self.state.db.clone(),
                    alert_dispatcher,
                    active_bridges: self.state.active_bridges.clone(),
                    resume_session_id,
                    log_dir: &self.state.log_dir,
                    mcp_script_path: &self.state.mcp_script_path,
                    app_config,
                    model_override,
                    tool_registry: self.state.tool_registry.clone(),
                    tools: self.state.tools.clone(),
                    server_origin: self.state.tool_server_origin.clone(),
                    server_shutting_down: self.state.server_shutting_down.clone(),
                    user_tz: self.timezone,
                    repo_sync_sender: self.state.repo_sync_sender.clone(),
                    messenger: self.state.messenger.clone(),
                    pwa_push_service: self.state.pwa_push.clone(),
                    mqtt_service: self.state.mqtt.clone(),
                    mqtt_event_router: self.state.mqtt_event_router.clone(),
                    automation_engine: self.state.automation_engine.clone(),
                    usage_session_gap_secs: self.state.usage_session_gap_secs,
                })
                .await?;

            // Send ModelsAvailable to this WS connection immediately — before
            // BridgeSpawned fires — so the triggering tab never waits.
            // Other tabs receive ModelsAvailable via the BridgeSpawned handler.
            if !model_infos.is_empty() {
                let _ = self.send_ws(WsServerMessage::ModelsAvailable {
                    available_models: model_infos,
                });
            }

            self.attach_to_bridge_with_rx(&bridge, rx).await;

            (bridge, warnings)
        };

        Ok((bridge, hook_warnings))
    }
}

#[cfg(test)]
mod tests {
    use brenn_cc::protocol::CcOutgoing;
    use brenn_lib::auth::user::create_user;
    use brenn_lib::conversation;
    use brenn_lib::db::{
        get_pending_tool_request, init_db_memory, insert_pending_tool_request,
        resolve_pending_tool_request,
    };
    use brenn_lib::ws_types::{CcState, PermissionDecision, ToolResponseDecision, WsServerMessage};
    use tokio::sync::broadcast;

    use super::super::connection::*;
    use super::super::testing::*;
    use crate::active_bridge::ActiveBridge;

    #[tokio::test]
    async fn spawn_bridge_blocked_by_single_instance() {
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
            .insert(other_conv_id, other_bridge)
            .await;

        // Try to send a message (would trigger spawn).
        conn.handle_send_message("hello", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_app_busy = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::AppBusy { .. }));
        assert!(has_app_busy, "expected AppBusy, got: {msgs:?}");
    }

    #[tokio::test]
    async fn spawn_bridge_allowed_when_not_single_instance() {
        // Use default test_apps (single_instance: false) with a test bridge injected.
        let (mut conn, mut ws_rx, _db, _user_id, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Register another user's bridge for the same app (but single_instance is false).
        let other_db = init_db_memory();
        let (broadcast_tx, _) = broadcast::channel(64);
        let other_bridge = ActiveBridge::inject_for_test(999, 999, "test", other_db, broadcast_tx);
        conn.state.active_bridges.insert(999, other_bridge).await;

        // Clear current conversation so we go through the "new conversation" path.
        conn.current_conversation_id = None;

        // Send a message — should succeed because single_instance is false.
        conn.handle_send_message("hello", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        // Should NOT get AppBusy.
        let has_app_busy = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::AppBusy { .. }));
        assert!(
            !has_app_busy,
            "should not get AppBusy for non-single-instance app, got: {msgs:?}"
        );
        // Should get ConversationSwitched (new conversation created).
        let has_switched = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ConversationSwitched { .. }));
        assert!(
            has_switched,
            "expected ConversationSwitched for new conversation, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn drain_queued_permission_response() {
        // A PermissionResponse queued before the bridge was ready should be
        // drained and forwarded to handle_permission_response when the bridge
        // becomes available. Since there's no actual pending permission, the
        // drain just logs a warning and continues (no panic).
        let (mut conn, _ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.queued_responses.push(QueuedResponse::Permission {
            request_id: "req_q_perm".into(),
            decision: PermissionDecision::Allow {
                updated_input: None,
            },
        });

        // Drain through the bridge. The bridge has no matching pending
        // permission, so handle_permission_response is a no-op (logs warning).
        let bridge = conn.test_bridge.clone().expect("test should have a bridge");
        conn.drain_queued_responses(&bridge).await;

        assert!(conn.queued_responses.is_empty(), "queue should be drained");
    }

    #[tokio::test]
    async fn drain_queued_tool_card_response() {
        // A ToolCardResponse queued before the bridge was ready should be
        // drained and forwarded to handle_tool_card_response.
        let (mut conn, _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Insert a pending tool request in the DB so the drain actually resolves it.
        {
            let db_conn = db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &db_conn,
                "req_q_tc",
                conv_id,
                "mcp__brenn__ProposeReconciliation",
                r#"{"import_id":"imp","proposals":[{"label":"X","transaction":{}}]}"#,
                None,
            );
        }

        conn.queued_responses.push(QueuedResponse::ToolCard {
            request_id: "req_q_tc".into(),
            decision: ToolResponseDecision::Deny {
                reason: Some("queued deny".into()),
            },
        });

        let bridge = conn.test_bridge.clone().expect("test should have a bridge");
        conn.drain_queued_responses(&bridge).await;

        assert!(conn.queued_responses.is_empty(), "queue should be drained");

        // The DB entry should be resolved as denied.
        {
            let db_conn = db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&db_conn, "req_q_tc")
                .expect("DB entry should exist");
            assert_eq!(req.status, "denied", "should be denied after drain");
        }
    }

    #[tokio::test]
    async fn drain_queued_mixed_responses() {
        // Queue both Permission and ToolCard responses, verify both drain.
        let (mut conn, _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Insert a pending tool request in the DB.
        {
            let db_conn = db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &db_conn,
                "req_q_mix_tc",
                conv_id,
                "mcp__brenn__ProposeReconciliation",
                r#"{"import_id":"imp","proposals":[{"label":"X","transaction":{}}]}"#,
                None,
            );
        }

        conn.queued_responses.push(QueuedResponse::Permission {
            request_id: "req_q_mix_perm".into(),
            decision: PermissionDecision::Deny {
                reason: Some("denied".into()),
            },
        });
        conn.queued_responses.push(QueuedResponse::ToolCard {
            request_id: "req_q_mix_tc".into(),
            decision: ToolResponseDecision::Deny {
                reason: Some("denied tc".into()),
            },
        });

        assert_eq!(conn.queued_responses.len(), 2);

        let bridge = conn.test_bridge.clone().expect("test should have a bridge");
        conn.drain_queued_responses(&bridge).await;

        assert!(
            conn.queued_responses.is_empty(),
            "all queued responses should be drained"
        );

        // ToolCard should be resolved in DB.
        {
            let db_conn = db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&db_conn, "req_q_mix_tc")
                .expect("DB entry should exist");
            assert_eq!(req.status, "denied");
        }
    }

    #[tokio::test]
    async fn queued_responses_cleared_on_conversation_switch() {
        let (mut conn, _ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Queue a response.
        conn.queued_responses.push(QueuedResponse::Permission {
            request_id: "req_stale".into(),
            decision: PermissionDecision::Allow {
                updated_input: None,
            },
        });
        assert_eq!(conn.queued_responses.len(), 1);

        // detach() is the real code path called on conversation switch; it must clear the queue.
        conn.detach().await;
        assert!(conn.queued_responses.is_empty());
    }

    /// Switch-back to a conversation that has a live bridge with pending
    /// synchronous permissions: `ConversationSwitched` must carry
    /// `CcState::AwaitingApproval`, and the pending permission must be
    /// replayed as a `PermissionRequest` frame. Regression test for
    /// code-review-1 finding #1 (user switches A → B → A and expects the
    /// dialog to reappear).
    #[tokio::test]
    async fn switch_conversation_replays_pending_permission() {
        let (mut conn, mut ws_rx, db, user_id, conv_id) = test_ws_conn_with_resume_conv().await;

        // Put a bridge with a pending permission in active_bridges.
        let (broadcast_tx, _) = broadcast::channel(64);
        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        bridge
            .insert_pending_permission_for_test(
                "req_switch_back",
                "Bash",
                serde_json::json!({"command": "echo hi"}),
            )
            .await;
        conn.state.active_bridges.insert(conv_id, bridge).await;

        conn.handle_switch_conversation(conv_id).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let switched = msgs.iter().find_map(|m| match m {
            WsServerMessage::ConversationSwitched { state, .. } => Some(*state),
            _ => None,
        });
        assert_eq!(
            switched,
            Some(CcState::AwaitingApproval),
            "ConversationSwitched must carry AwaitingApproval when pending_permissions is non-empty, got: {msgs:?}"
        );
        let has_permission_request = msgs.iter().any(|m| matches!(
            m,
            WsServerMessage::PermissionRequest { request_id, .. } if request_id == "req_switch_back"
        ));
        assert!(
            has_permission_request,
            "switch must replay PermissionRequest, got: {msgs:?}"
        );
    }

    /// `deliver_pending_results` success path: an undelivered resolved tool result
    /// is injected to CC and marked `delivered_to_cc = 1` in the DB.
    ///
    /// Exercises the path that was previously dead due to `session = None` in the
    /// test arm: `inject_tool_result_to_cc` → `sent = true` → `mark_delivered_to_cc`.
    #[tokio::test]
    async fn deliver_pending_results_marks_delivered_and_sends_to_cc() {
        let (mut conn, _ws_rx, db, _user_id, conv_id) = test_ws_conn_with_resume_conv().await;

        // Install a recording session on the bridge before spawn_bridge consumes it.
        // Must happen before spawn_bridge calls deliver_pending_results.
        let bridge_ref = conn
            .test_bridge
            .as_ref()
            .expect("test_bridge must be populated")
            .clone();
        let mut cc_rx = bridge_ref.install_recording_session_for_test().await;

        // Seed a resolved-but-undelivered tool result row.
        {
            let db_conn = db.lock().await;
            insert_pending_tool_request(&db_conn, "test-req-1", conv_id, "SomeTool", "{}", None);
            assert!(
                resolve_pending_tool_request(
                    &db_conn,
                    "test-req-1",
                    "completed",
                    Some(r#"{"result":"ok"}"#),
                ),
                "test seed: resolve_pending_tool_request must update exactly one row"
            );
        }

        // spawn_bridge: takes bridge from test_bridge, registers it, calls
        // deliver_pending_results with the recording session now installed.
        conn.spawn_bridge(conv_id, false, None, None)
            .await
            .expect("spawn_bridge must succeed");

        // Assert DB: delivered_to_cc flipped to 1.
        {
            let db_conn = db.lock().await;
            let req = get_pending_tool_request(&db_conn, "test-req-1")
                .expect("row must exist after spawn");
            assert!(
                req.delivered_to_cc,
                "delivered_to_cc must be true after deliver_pending_results"
            );
        }

        // Assert CC received the tool result payload as a User message.
        let envelope = cc_rx
            .try_recv()
            .expect("CC channel must have exactly one message");
        match &envelope.msg {
            CcOutgoing::User { message } => {
                // inject_tool_result_to_cc calls session.send_message(result),
                // which wraps the text in a UserContentBlock::Text.
                assert!(
                    message.content.iter().any(|block| match block {
                        brenn_cc::protocol::UserContentBlock::Text { text } =>
                            text.contains(r#"{"result":"ok"}"#),
                        _ => false,
                    }),
                    "CC message must contain the tool result payload, got: {:?}",
                    envelope.msg
                );
            }
            other => panic!("expected CcOutgoing::User, got: {other:?}"),
        }

        // No additional messages on the channel.
        assert!(
            cc_rx.try_recv().is_err(),
            "no additional messages must be on the CC channel after deliver_pending_results"
        );
    }
    /// `deliver_pending_results` — denied path: a denied row with a non-null result
    /// is delivered to CC and marked `delivered_to_cc = 1`, identical to the completed path.
    #[tokio::test]
    async fn deliver_pending_results_delivers_denied_row_with_result() {
        let (mut conn, _ws_rx, db, _user_id, conv_id) = test_ws_conn_with_resume_conv().await;

        let bridge_ref = conn
            .test_bridge
            .as_ref()
            .expect("test_bridge must be populated")
            .clone();
        let mut cc_rx = bridge_ref.install_recording_session_for_test().await;

        // Seed a denied-but-undelivered tool result row with a non-null result.
        {
            let db_conn = db.lock().await;
            insert_pending_tool_request(
                &db_conn,
                "test-req-denied-1",
                conv_id,
                "SomeTool",
                "{}",
                None,
            );
            assert!(
                resolve_pending_tool_request(
                    &db_conn,
                    "test-req-denied-1",
                    "denied",
                    Some(r#"{"error":"denied by user"}"#),
                ),
                "test seed: resolve_pending_tool_request must update exactly one row"
            );
        }

        conn.spawn_bridge(conv_id, false, None, None)
            .await
            .expect("spawn_bridge must succeed");

        // Assert DB: delivered_to_cc flipped to 1.
        {
            let db_conn = db.lock().await;
            let req = get_pending_tool_request(&db_conn, "test-req-denied-1")
                .expect("row must exist after spawn");
            assert!(
                req.delivered_to_cc,
                "delivered_to_cc must be true for denied row after deliver_pending_results"
            );
        }

        // Assert CC received the tool result payload.
        let envelope = cc_rx
            .try_recv()
            .expect("CC channel must have exactly one message for denied delivery");
        match &envelope.msg {
            CcOutgoing::User { message } => {
                assert!(
                    message.content.iter().any(|block| match block {
                        brenn_cc::protocol::UserContentBlock::Text { text } =>
                            text.contains(r#"{"error":"denied by user"}"#),
                        _ => false,
                    }),
                    "CC message must contain the denied result payload, got: {:?}",
                    envelope.msg
                );
            }
            other => panic!("expected CcOutgoing::User, got: {other:?}"),
        }

        assert!(
            cc_rx.try_recv().is_err(),
            "no additional messages must be on the CC channel after deliver_pending_results"
        );
    }

    /// `deliver_pending_results` — zero-rows fast path: when there are no undelivered
    /// rows, `deliver_pending_results` returns early (tool_card.rs:348-350) and no
    /// message is sent to CC. Catches SQL predicate regressions in `get_undelivered_results`.
    #[tokio::test]
    async fn deliver_pending_results_no_op_when_zero_undelivered_rows() {
        let (mut conn, _ws_rx, _db, _user_id, conv_id) = test_ws_conn_with_resume_conv().await;

        let bridge_ref = conn
            .test_bridge
            .as_ref()
            .expect("test_bridge must be populated")
            .clone();
        let mut cc_rx = bridge_ref.install_recording_session_for_test().await;

        // No pending tool requests seeded — zero undelivered rows.
        conn.spawn_bridge(conv_id, false, None, None)
            .await
            .expect("spawn_bridge must succeed");

        assert!(
            cc_rx.try_recv().is_err(),
            "no CC messages must be sent when there are zero undelivered rows"
        );
    }

    /// history_sent flag: set by send_history, cleared by detach.
    #[tokio::test]
    async fn history_sent_flag_set_by_send_history_cleared_by_detach() {
        let (mut conn, _ws_rx, _db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        assert!(!conn.history_sent, "initially false");

        // send_history sets it to true.
        let _ = conn.send_history(conv_id, None).await;
        assert!(conn.history_sent, "should be true after send_history");

        // detach clears it.
        conn.detach().await;
        assert!(!conn.history_sent, "should be false after detach");
    }
}

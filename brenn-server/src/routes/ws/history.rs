//! History replay, pagination, pending tool/permission replay, conversation list.

use brenn_lib::conversation;
use brenn_lib::ws_types::{CcState, WsServerMessage};
use tracing::warn;

use super::connection::WsConnection;
use crate::active_bridge::ActiveBridge;

// impl WsConnection — history replay, pagination, pending tool/permission replay, conversation list
impl WsConnection {
    /// Build and send conversation history + artifact index for a conversation.
    ///
    /// `from_seq` controls incremental replay: `None` replays everything,
    /// `Some(n)` replays only messages with `seq > n`.
    ///
    /// Uses back-pressured sends — waits for buffer space rather than dropping
    /// messages. Returns `Err(())` only if the WS channel closes (connection
    /// dead). Callers must handle the error (Result is `#[must_use]`).
    pub(super) async fn send_history(
        &mut self,
        conversation_id: i64,
        from_seq: Option<i64>,
    ) -> Result<(), ()> {
        let Some(app) = self.state.apps.get(&self.app_slug) else {
            warn!(slug = %self.app_slug, "send_history: app not found");
            return Ok(()); // Nothing to send — vacuously successful.
        };
        let working_dir = app.working_dir.as_path();
        let slug = app.slug.as_str();
        let replay_limit = app.history_replay_limit;
        let mounts = crate::artifact::mount_roots_for(&app.mounts);
        let frontmatter = &app.frontmatter;
        let (messages, artifact_index, oldest_loaded_seq, gap_reload_conv_fields) = {
            let conn = self.state.db.lock().await;

            // Find the replay seam for bounded history.
            let seam_seq = crate::history::find_replay_seam(&conn, conversation_id, replay_limit);

            // Destructure to avoid cloning cwd (Option<String> heap alloc) when conv
            // fields are only needed for the gap-reload message (Copy fields only).
            let brenn_lib::conversation::Conversation {
                user_id: conv_user_id,
                shared: conv_shared,
                cwd,
                ..
            } = conversation::get_conversation(&conn, conversation_id);

            // Detect the seam-reconnect gap: client has messages before the seam,
            // server will replay from the seam forward, leaving a silent gap.
            // Record (is_owner, shared) here; build the full ConversationSwitched
            // message after the lock drops so we can look up the real CcState from
            // the active bridge (avoids hardcoding CcState::Connecting).
            let gap_reload_conv_fields = match (from_seq, seam_seq) {
                (Some(f), Some(s)) if f < s => Some((conv_user_id == self.user_id, conv_shared)),
                _ => None,
            };
            let is_gap_reload = gap_reload_conv_fields.is_some();

            let index = crate::artifact_snapshot::get_artifact_index(&conn, conversation_id);

            // When a seam is active, filter the artifact index to post-seam files.
            let filtered_index = if let Some(seam) = seam_seq {
                index
                    .into_iter()
                    .filter(|f| f.versions.iter().any(|v| v.seq > seam))
                    .collect()
            } else {
                index
            };

            let version_counts =
                crate::artifact_snapshot::version_counts_from_index(&filtered_index);
            let msgs = crate::history::build_history(
                &conn,
                conversation_id,
                cwd.as_deref(),
                working_dir,
                slug,
                &mounts,
                &version_counts,
                from_seq,
                seam_seq,
                frontmatter,
            );

            // Compute oldest_loaded_seq from the first message in the batch.
            // Meaningful on the full-replay path and on gap-reload (where the
            // client's state was cleared and seam-based replay is a fresh start).
            // On plain incremental replay (from_seq = Some without gap), the oldest
            // message already displayed is unchanged — do not recompute.
            let is_fresh_load = from_seq.is_none() || is_gap_reload;
            let oldest_seq = if is_fresh_load && seam_seq.is_some() {
                msgs.first().and_then(WsConnection::extract_seq)
            } else {
                None
            };

            (msgs, filtered_index, oldest_seq, gap_reload_conv_fields)
        };
        // Build the gap-reload ConversationSwitched message using the real CcState from
        // the active bridge (if any), now that the DB lock has been released.
        // gap_reload_conv_fields is Some((is_owner, shared)) iff from_seq < seam_seq.
        let force_reload = if let Some((is_owner, shared)) = gap_reload_conv_fields {
            let cc_state =
                if let Some(bridge) = self.state.active_bridges.get(conversation_id).await {
                    bridge.resolve_cc_state().await
                } else {
                    CcState::Connecting
                };
            Some(WsServerMessage::ConversationSwitched {
                conversation_id: Some(conversation_id),
                state: cc_state,
                is_owner,
                shared,
                reload: true,
            })
        } else {
            None
        };
        let gap_reload = force_reload.is_some();
        // Compute the highest seq in this batch for incremental re-replay tracking.
        // Messages are ordered by seq ascending; the last seq-bearing message is the max.
        let batch_max_seq = messages.iter().rev().find_map(WsConnection::extract_seq);
        // Update oldest_loaded_seq on the full-replay path and on gap-reload
        // (where the client's state was cleared and seam-based replay is fresh).
        // On plain incremental replay (from_seq = Some without gap), the oldest
        // message already displayed is unchanged — leave the field alone.
        //
        // NOTE: this field IS rewritten by every send_history call: on full replay
        // it is set from the batch, on incremental replay it is left unchanged.
        // `last_sent_seq` is similarly rewritten — see match arms below.
        if from_seq.is_none() || gap_reload {
            self.oldest_loaded_seq = oldest_loaded_seq;
        }
        self.history_sent = true;
        // Update last_sent_seq — the cursor used by BridgeSpawned for incremental
        // re-replay to avoid losing drain rows written after send_history.
        //
        // Full replay (from_seq == None) or gap-reload: reset to batch max so
        // stale seqs don't carry over. Gap-reload is a fresh seam-based replay —
        // treat it like a full replay for cursor purposes.
        //
        // Incremental replay (from_seq == Some, no gap): advance upward only.
        self.last_sent_seq = match (from_seq, batch_max_seq, gap_reload) {
            (None, Some(max), _) | (Some(_), Some(max), true) => Some(max), // Full / gap-reload: set to batch max.
            (None, None, _) | (Some(_), None, true) => None, // Full / gap-reload of empty slice.
            (Some(_), Some(max), false) => {
                Some(self.last_sent_seq.map_or(max, |last| last.max(max)))
            } // Incremental: advance.
            (Some(_), None, false) => self.last_sent_seq, // Incremental, no new rows: keep current.
        };
        // If the client has a gap (from_seq < seam_seq), send a corrective
        // ConversationSwitched{reload:true} to clear the stale frontend state
        // before delivering the seam-based history batch.
        if let Some(reload_msg) = force_reload {
            self.send_ws_backpressure(reload_msg).await?;
        }
        for msg in messages {
            self.send_ws_backpressure(msg).await?;
        }
        // Send HistoryComplete / ArtifactIndex / pending tool requests on the
        // full-replay path AND when a gap-reload was triggered (seam-reconnect
        // gap). In the gap-reload case the frontend just cleared its state, so
        // it needs the full complement of state frames just like a fresh connect.
        //
        // Skip for incremental re-replay (BridgeSpawned, wake-spawn race fix):
        // the full state frames were already emitted by the prior full send_history
        // call and re-emitting them causes incorrect side-effects:
        //   - HistoryComplete triggers scrollToBottomNow() even when the user is
        //     scrolled up reading history.
        //   - HistoryComplete with oldest_loaded_seq = None clobbers the pagination
        //     cursor, breaking LoadMoreHistory after every CC restart.
        //   - ArtifactIndex re-renders the artifact pane even if nothing changed.
        //
        // Note: on the BridgeSpawned-incremental path (history_sent=true), a gap-reload
        // CAN fire if last_sent_seq < seam_seq (e.g., after mpsc buffer overflow where
        // reload_pending was set but more messages arrived at the seam boundary).
        // In that case the disruptive reload is intentional: the client's view was
        // already broken by the buffer overflow and a fresh seam-based replay is correct.
        if from_seq.is_none() || gap_reload {
            self.send_ws_backpressure(WsServerMessage::HistoryComplete { oldest_loaded_seq })
                .await?;
            self.send_ws_backpressure(WsServerMessage::ArtifactIndex {
                files: artifact_index,
            })
            .await?;
            // Replay pending async tool requests so the browser shows them.
            self.send_pending_tool_requests_backpressure(conversation_id)
                .await?;
        }
        Ok(())
    }

    /// Handle a `LoadMoreHistory` request for backward pagination.
    ///
    /// Guard: rejects the request when no seam was active for the current
    /// conversation (full history was already sent). The frontend should never
    /// send this message in that case — receiving it is a protocol violation.
    pub(super) async fn handle_load_more_history(&self, before_seq: i64) {
        let Some(conv_id) = self.current_conversation_id else {
            warn!("LoadMoreHistory with no active conversation");
            return;
        };

        // Guard: reject when no seam was active. The frontend shouldn't send
        // this when oldest_loaded_seq was None. Log and return empty page.
        if self.oldest_loaded_seq.is_none() {
            warn!(
                conversation_id = conv_id,
                "LoadMoreHistory received but no seam is active — protocol violation"
            );
            let _ = self.send_ws(WsServerMessage::HistoryPage {
                messages: vec![],
                has_more: false,
            });
            return;
        }

        let (messages, has_more) = {
            let conn = self.state.db.lock().await;
            crate::history::build_simplified_page(&conn, conv_id, before_seq)
        };

        let _ = self.send_ws(WsServerMessage::HistoryPage { messages, has_more });
    }

    /// Send any pending async tool requests for a conversation to the browser.
    /// These are interactive tool UIs (ProposeReconciliation, BatchReconcile)
    /// that were persisted to DB and are still awaiting user response.
    /// Renders at serve time from persisted data — same render path as live requests.
    ///
    /// Uses non-blocking `send_ws` — appropriate for contexts where buffer
    /// overflow is handled by the caller (e.g., broadcast select arm).
    #[cfg(test)]
    pub(super) async fn send_pending_tool_requests(&self, conversation_id: i64) {
        for msg in self
            .build_pending_tool_request_messages(conversation_id)
            .await
        {
            let _ = self.send_ws(msg);
        }
    }

    /// Back-pressured variant of `send_pending_tool_requests`. Waits for
    /// buffer space before each send. Used by `send_history` to keep the
    /// entire history replay pipeline under back-pressure.
    pub(super) async fn send_pending_tool_requests_backpressure(
        &self,
        conversation_id: i64,
    ) -> Result<(), ()> {
        for msg in self
            .build_pending_tool_request_messages(conversation_id)
            .await
        {
            self.send_ws_backpressure(msg).await?;
        }
        Ok(())
    }

    /// Build the WS messages for pending tool requests. Shared by both
    /// `send_pending_tool_requests` and its back-pressured variant.
    async fn build_pending_tool_request_messages(
        &self,
        conversation_id: i64,
    ) -> Vec<WsServerMessage> {
        let pending = {
            let conn = self.state.db.lock().await;
            brenn_lib::db::get_pending_tool_requests_for_conversation(&conn, conversation_id)
        };

        if pending.is_empty() {
            return vec![];
        }

        tracing::info!(
            count = pending.len(),
            "replaying pending tool requests to browser"
        );

        let viewport = self.viewport_class;
        pending
            .into_iter()
            .map(|req| {
                let tool_input: serde_json::Value = serde_json::from_str(&req.tool_input)
                    .expect("stored tool_input must be valid JSON");
                let formatted_display = crate::active_bridge::render_pending_tool_request(
                    &self.state.tool_registry,
                    &req.tool_name,
                    &tool_input,
                    req.extra.as_deref(),
                    viewport,
                );
                WsServerMessage::ToolCardRequest {
                    request_id: req.request_id,
                    tool_name: req.tool_name,
                    tool_input,
                    formatted_display,
                }
            })
            .collect()
    }

    /// Replay any in-memory synchronous permission requests that are currently
    /// pending on the bridge, so a freshly-attaching tab sees them. Symmetric
    /// with `send_pending_tool_requests_backpressure` for DB-backed async tool
    /// cards. Re-renders via `format_tool_display` each call.
    pub(super) async fn send_pending_permissions_backpressure(
        &self,
        bridge: &ActiveBridge,
    ) -> Result<(), ()> {
        let snapshots = bridge.pending_permission_snapshots().await;
        if snapshots.is_empty() {
            return Ok(());
        }
        tracing::info!(
            count = snapshots.len(),
            conversation_id = bridge.conversation_id,
            "replaying pending permissions on attach"
        );
        for snap in snapshots {
            let formatted_display = crate::approval_formatter::format_tool_display(
                &self.state.tool_registry,
                &snap.tool_name,
                &snap.display_input,
            );
            self.send_ws_backpressure(WsServerMessage::PermissionRequest {
                request_id: snap.request_id,
                tool_name: snap.tool_name,
                tool_input: snap.tool_input,
                formatted_display,
            })
            .await?;
        }
        self.send_ws_backpressure(WsServerMessage::Status {
            state: CcState::AwaitingApproval,
        })
        .await?;
        Ok(())
    }

    /// Build and send the conversation list (scoped to this connection's app).
    pub(super) async fn send_conversation_list(&self) {
        let multiuser = self.app_config().multiuser;
        let conversations = {
            let conn = self.state.db.lock().await;
            crate::history::build_conversation_list(&conn, self.user_id, &self.app_slug, multiuser)
        };
        let _ = self.send_ws(WsServerMessage::ConversationList { conversations });
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::conversation;
    use brenn_lib::ws_types::{CcState, WsServerMessage};

    use super::super::testing::*;

    #[tokio::test]
    async fn send_history_errors_when_channel_closed() {
        let (mut conn, ws_rx, db, user_id) = test_ws_conn_with_channel(2).await;

        // Create a conversation with some messages.
        let conv_id = {
            let db_conn = db.lock().await;
            let cid = conversation::create_conversation(&db_conn, user_id, "test", false);
            seed_user_messages(&db_conn, cid, user_id, 5);
            cid
        };
        conn.current_conversation_id = Some(conv_id);

        // Drop the receiver — simulates WS connection dying.
        drop(ws_rx);

        let result = conn.send_history(conv_id, None).await;
        assert!(
            result.is_err(),
            "send_history should return Err when channel is closed"
        );
    }

    #[tokio::test]
    async fn send_history_succeeds_with_sufficient_buffer() {
        let (mut conn, _ws_rx, _db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;
        // The test helper creates a conversation but doesn't add messages to it.
        // send_history with no messages just sends HistoryComplete + ArtifactIndex.
        // The 256-slot buffer can easily handle that.
        let result = conn.send_history(conv_id, None).await;
        assert!(
            result.is_ok(),
            "send_history should succeed with sufficient buffer"
        );
    }

    /// The core scenario this fix addresses: a small buffer with many messages.
    /// With back-pressure, `send_history` waits for buffer space instead of
    /// dropping messages, so all history + HistoryComplete are delivered.
    #[tokio::test]
    async fn send_history_backpressure_delivers_all_messages() {
        // Use a tiny 2-slot buffer — the old code would overflow and return false.
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_with_channel(2).await;

        // Create a conversation with enough messages to overflow the 2-slot buffer.
        let conv_id = {
            let db_conn = db.lock().await;
            let cid = conversation::create_conversation(&db_conn, user_id, "test", false);
            seed_user_messages(&db_conn, cid, user_id, 10);
            cid
        };
        conn.current_conversation_id = Some(conv_id);

        // Spawn a consumer that drains the buffer (simulates ws_writer).
        let consumer = tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(msg) = ws_rx.recv().await {
                let is_complete = matches!(msg, WsServerMessage::HistoryComplete { .. });
                received.push(msg);
                if is_complete {
                    // Drain any remaining messages (ArtifactIndex, etc.)
                    while let Ok(msg) = ws_rx.try_recv() {
                        received.push(msg);
                    }
                    break;
                }
            }
            received
        });

        // send_history should succeed — back-pressure lets the consumer drain.
        let result = conn.send_history(conv_id, None).await;
        assert!(
            result.is_ok(),
            "send_history should succeed with back-pressure"
        );

        let received = consumer.await.expect("consumer task should not panic");

        // Verify HistoryComplete was delivered (this is what was missing before).
        let has_history_complete = received
            .iter()
            .any(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        assert!(
            has_history_complete,
            "HistoryComplete must be delivered: {received:?}"
        );

        // Verify ArtifactIndex was delivered.
        let has_artifact_index = received
            .iter()
            .any(|m| matches!(m, WsServerMessage::ArtifactIndex { .. }));
        assert!(
            has_artifact_index,
            "ArtifactIndex must be delivered: {received:?}"
        );

        // Verify all 10 user messages were delivered (not dropped).
        let user_msg_count = received
            .iter()
            .filter(|m| matches!(m, WsServerMessage::UserMessageEcho { .. }))
            .count();
        assert_eq!(
            user_msg_count, 10,
            "all 10 user messages must be delivered, got {user_msg_count}: {received:?}"
        );
    }

    // -----------------------------------------------------------------------
    // QueuedResponse / drain tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn send_pending_tool_requests_uses_tool_card_request() {
        // DB-backed pending tool requests should be sent as ToolCardRequest,
        // not PermissionRequest (they're async tool cards, not CC permissions).
        let (conn, mut ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Insert a pending tool request in the DB.
        {
            let db_conn = db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &db_conn,
                "req_pending_1",
                conv_id,
                "mcp__brenn__ProposeReconciliation",
                r#"{"proposals":[]}"#,
                None,
            );
        }

        conn.send_pending_tool_requests(conv_id).await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert_eq!(msgs.len(), 1, "expected 1 message, got: {msgs:?}");
        match &msgs[0] {
            WsServerMessage::ToolCardRequest {
                request_id,
                tool_name,
                ..
            } => {
                assert_eq!(request_id, "req_pending_1");
                assert_eq!(tool_name, "mcp__brenn__ProposeReconciliation");
            }
            other => panic!("expected ToolCardRequest, got {other:?}"),
        }
    }

    /// Replay emits `PermissionRequest` + `Status(AwaitingApproval)` for each
    /// in-memory entry on a fresh attach. Symmetric with
    /// `send_pending_tool_requests_uses_tool_card_request`.
    #[tokio::test]
    async fn send_pending_permissions_replays_permission_request() {
        let (conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;
        let bridge = conn.test_bridge.clone().expect("test bridge");

        bridge
            .insert_pending_permission_for_test(
                "req_replay_1",
                "Bash",
                serde_json::json!({"command": "echo hi"}),
            )
            .await;

        conn.send_pending_permissions_backpressure(&bridge)
            .await
            .expect("send should succeed");

        let msgs = collect_messages(&mut ws_rx).await;
        // Expect PermissionRequest + Status(AwaitingApproval).
        assert_eq!(msgs.len(), 2, "expected 2 frames, got: {msgs:?}");
        match &msgs[0] {
            WsServerMessage::PermissionRequest {
                request_id,
                tool_name,
                tool_input,
                formatted_display,
            } => {
                assert_eq!(request_id, "req_replay_1");
                assert_eq!(tool_name, "Bash");
                assert_eq!(tool_input, &serde_json::json!({"command": "echo hi"}));
                // Content assertions, not just non-emptiness: a regression
                // that caches a stale `formatted_display` (violating
                // "re-render at attach, not cache") must be caught here by
                // content-from-input and wrapping-component checks.
                assert!(
                    formatted_display.contains("echo hi"),
                    "formatted_display must contain the tool input content, got: {formatted_display}"
                );
                assert!(
                    formatted_display.contains("brenn-tool-approve"),
                    "formatted_display must include the approval wrapper, got: {formatted_display}"
                );
            }
            other => panic!("expected PermissionRequest, got {other:?}"),
        }
        match &msgs[1] {
            WsServerMessage::Status { state } => {
                assert_eq!(*state, CcState::AwaitingApproval);
            }
            other => panic!("expected Status(AwaitingApproval), got {other:?}"),
        }
    }

    /// Empty pending map emits zero extra frames — no spurious
    /// `Status::AwaitingApproval`.
    #[tokio::test]
    async fn send_pending_permissions_empty_sends_nothing() {
        let (conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;
        let bridge = conn.test_bridge.clone().expect("test bridge");

        conn.send_pending_permissions_backpressure(&bridge)
            .await
            .expect("send should succeed");

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.is_empty(),
            "empty pending map must emit zero frames, got: {msgs:?}"
        );
    }

    /// `ConversationSwitched.state` is `AwaitingApproval` when a bridge has a
    /// non-empty `pending_permissions` at attach time, avoiding a Thinking →
    /// AwaitingApproval flicker.
    #[tokio::test]
    async fn cc_state_awaits_approval_on_reconnect() {
        let (mut conn, mut ws_rx, _db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;
        let bridge = conn.test_bridge.clone().expect("test bridge");

        // Register the bridge in active_bridges so try_select_requested_conversation
        // finds a live bridge on the attach path.
        conn.state
            .active_bridges
            .insert(conv_id, bridge.clone())
            .await;

        // Seed a pending permission before the attach.
        bridge
            .insert_pending_permission_for_test(
                "req_awaiting",
                "Bash",
                serde_json::json!({"command": "echo hi"}),
            )
            .await;

        // Drive the attach path.
        let selected = conn
            .try_select_requested_conversation(conv_id, false, None)
            .await;
        assert_eq!(selected, Ok(true), "attach should succeed");

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

        // And the replay frames must also appear.
        let has_permission_request = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::PermissionRequest { request_id, .. } if request_id == "req_awaiting"));
        assert!(
            has_permission_request,
            "attach must replay PermissionRequest, got: {msgs:?}"
        );
    }

    // -----------------------------------------------------------------------
    // last_sent_seq update transitions
    // -----------------------------------------------------------------------

    /// After a full send_history on a conversation with messages, last_sent_seq
    /// equals the max seq in the batch.
    #[tokio::test]
    async fn send_history_full_replay_sets_last_sent_seq_to_batch_max() {
        let (mut conn, _ws_rx, db, user_id) = test_ws_conn_with_channel(256).await;

        let conv_id = {
            let db_conn = db.lock().await;
            let cid = conversation::create_conversation(&db_conn, user_id, "test", false);
            // Insert 3 messages; seqs will be 0, 1, 2.
            seed_user_messages(&db_conn, cid, user_id, 3);
            cid
        };
        conn.current_conversation_id = Some(conv_id);
        conn.last_sent_seq = None;

        conn.send_history(conv_id, None)
            .await
            .expect("send_history");

        assert_eq!(
            conn.last_sent_seq,
            Some(2),
            "last_sent_seq must equal the max seq (2) after full replay"
        );
    }

    /// After a full send_history on an empty conversation, last_sent_seq is None.
    #[tokio::test]
    async fn send_history_full_replay_empty_conversation_leaves_last_sent_seq_none() {
        let (mut conn, _ws_rx, db, user_id) = test_ws_conn_with_channel(256).await;

        let conv_id = {
            let db_conn = db.lock().await;
            conversation::create_conversation(&db_conn, user_id, "test", false)
        };
        conn.current_conversation_id = Some(conv_id);
        conn.last_sent_seq = Some(42); // pre-set to a stale value

        conn.send_history(conv_id, None)
            .await
            .expect("send_history");

        assert_eq!(
            conn.last_sent_seq, None,
            "full replay of empty conversation must reset last_sent_seq to None"
        );
    }

    /// Incremental send_history advances last_sent_seq upward when new rows exist.
    #[tokio::test]
    async fn send_history_incremental_advances_last_sent_seq() {
        let (mut conn, _ws_rx, db, user_id) = test_ws_conn_with_channel(256).await;

        let conv_id = {
            let db_conn = db.lock().await;
            let cid = conversation::create_conversation(&db_conn, user_id, "test", false);
            // seq 0 and seq 1
            seed_user_messages(&db_conn, cid, user_id, 2);
            cid
        };
        conn.current_conversation_id = Some(conv_id);
        conn.last_sent_seq = Some(0); // cursor: already sent seq 0

        // Incremental replay from seq > 0 — should deliver seq 1 and advance.
        conn.send_history(conv_id, Some(0))
            .await
            .expect("send_history");

        assert_eq!(
            conn.last_sent_seq,
            Some(1),
            "incremental replay must advance last_sent_seq to new batch max"
        );
    }

    /// Incremental send_history with no new rows does not regress last_sent_seq.
    ///
    /// This test exercises the `(Some(_), None)` match arm in `send_history`,
    /// which fires when `from_seq` is within DB range but no rows exist beyond it.
    /// Requires `from_seq <= max_seq` so build_history does not fall back to full
    /// replay; seq 0 and seq 1 exist, cursor at seq 1, SQL `seq > 1` returns 0 rows.
    #[tokio::test]
    async fn send_history_incremental_no_new_rows_does_not_regress_last_sent_seq() {
        let (mut conn, _ws_rx, db, user_id) = test_ws_conn_with_channel(256).await;

        let conv_id = {
            let db_conn = db.lock().await;
            let cid = conversation::create_conversation(&db_conn, user_id, "test", false);
            // Insert seq 0 and seq 1 so max_seq = 1.
            seed_user_messages(&db_conn, cid, user_id, 2);
            cid
        };
        conn.current_conversation_id = Some(conv_id);
        conn.last_sent_seq = Some(1); // cursor at max; live broadcast already delivered seq 1

        // Incremental replay from seq 1 — from_seq (1) <= max_seq (1), so no stale
        // fallback. SQL `seq > 1` returns 0 rows, batch_max_seq = None.
        // The `(Some(_), None)` arm must leave last_sent_seq unchanged.
        conn.send_history(conv_id, Some(1))
            .await
            .expect("send_history");

        assert_eq!(
            conn.last_sent_seq,
            Some(1),
            "incremental replay with no new rows must leave last_sent_seq unchanged"
        );
    }

    /// After handle_switch_conversation, last_sent_seq is cleared.
    #[tokio::test]
    async fn handle_switch_conversation_clears_last_sent_seq() {
        let (mut conn, _ws_rx, db, user_id, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.last_sent_seq = Some(99);

        // Switch to a new conversation (no bridge) to trigger the clear.
        let new_conv_id = {
            let c = db.lock().await;
            brenn_lib::conversation::create_conversation(&c, user_id, "test", false)
        };
        conn.handle_switch_conversation(new_conv_id).await;

        assert_eq!(
            conn.last_sent_seq, None,
            "handle_switch_conversation must clear last_sent_seq"
        );
    }

    /// After handle_new_conversation, last_sent_seq is cleared.
    ///
    /// Regression guard: a stale cursor carried over to the new conversation would
    /// cause send_history to skip rows (incremental path) or never advance.
    #[tokio::test]
    async fn handle_new_conversation_clears_last_sent_seq() {
        let (mut conn, _ws_rx, _db, _user_id, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.last_sent_seq = Some(42);

        conn.handle_new_conversation().await;

        assert_eq!(
            conn.last_sent_seq, None,
            "handle_new_conversation must clear last_sent_seq"
        );
    }

    /// After handle_steal_app, last_sent_seq is cleared.
    ///
    /// Regression guard: stealing a single-instance app resets WsConnection to a
    /// clean state; a stale cursor carried into the next conversation open would
    /// cause incremental replay to skip rows.
    #[tokio::test]
    async fn handle_steal_app_clears_last_sent_seq() {
        use crate::active_bridge::ActiveBridge;
        use tokio::sync::broadcast;

        let (mut conn, _ws_rx, db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        // Register a bridge belonging to another user so steal_app has something
        // to kill (it returns an error and early-exits if bridges is empty).
        let other_user_id = {
            let c = db.lock().await;
            brenn_lib::auth::user::create_user(&c, "otheruser", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            brenn_lib::conversation::create_conversation(&c, other_user_id, "test", false)
        };
        let (broadcast_tx, _) = broadcast::channel(4);
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

        conn.last_sent_seq = Some(77);

        conn.handle_steal_app().await;

        assert_eq!(
            conn.last_sent_seq, None,
            "handle_steal_app must clear last_sent_seq"
        );
    }

    // -----------------------------------------------------------------------
    // seam-reconnect-reload: force reload when from_seq < seam_seq
    // -----------------------------------------------------------------------

    /// When a client reconnects with a `from_seq` that is older than the replay
    /// seam, `send_history` must send `ConversationSwitched{reload:true}` before
    /// the history batch to clear the client's stale messages.
    ///
    /// Scenario: limit=3, 5 messages (seqs 0–4). Seam at seq 1 (cutoff_offset=1).
    /// Client reconnects with from_seq=0 (< seam=1). Gap detected → reload.
    #[tokio::test]
    async fn send_history_gap_reload_when_from_seq_below_seam() {
        use indexmap::IndexMap;
        use std::sync::Arc;

        // Build a custom app with history_replay_limit=3 so 4 messages trigger a seam.
        let mut apps = IndexMap::new();
        let mut cfg = crate::test_support::app_config::default_test_app_config("test", "Test App");
        cfg.history_replay_limit = 3;
        apps.insert("test".to_string(), cfg);
        let apps = Arc::new(apps);

        let (mut conn, mut ws_rx, db, user_id, conv_id) =
            test_ws_conn_with_resume_conv_and_apps(apps).await;

        // Seed 5 messages: seqs 0–4.
        // With limit=3: cutoff_offset=1, seam_seq=Some(1). Replay sends seq > 1.
        // from_seq=0 < seam_seq=1 → gap → reload.
        {
            let db_conn = db.lock().await;
            seed_user_messages(&db_conn, conv_id, user_id, 5);
        }

        // Full replay to establish that the conversation has messages.
        conn.send_history(conv_id, None)
            .await
            .expect("full send_history must succeed");
        let _ = collect_messages(&mut ws_rx).await; // drain

        // Now simulate reconnect: client has from_seq=0 but seam is in the middle.
        // Reset conn state to mimic a fresh reconnect cursor.
        conn.last_sent_seq = Some(0);
        conn.oldest_loaded_seq = None;

        conn.send_history(conv_id, Some(0))
            .await
            .expect("gap-reload send_history must succeed");
        let msgs = collect_messages(&mut ws_rx).await;

        // Must include a ConversationSwitched with reload=true.
        let reload_switched = msgs.iter().find(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched { reload: true, .. }
            )
        });
        assert!(
            reload_switched.is_some(),
            "must emit ConversationSwitched{{reload:true}} when from_seq < seam_seq: {msgs:?}"
        );

        // Must include HistoryComplete (frontend needs to know replay is done).
        let has_history_complete = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::HistoryComplete { .. }));
        assert!(
            has_history_complete,
            "gap-reload must emit HistoryComplete: {msgs:?}"
        );

        // Must include ArtifactIndex.
        let has_artifact_index = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ArtifactIndex { .. }));
        assert!(
            has_artifact_index,
            "gap-reload must emit ArtifactIndex: {msgs:?}"
        );

        // ConversationSwitched{reload:true} must come before any history messages.
        // Assert unconditionally: reload_pos must be Some (already checked above),
        // and when history messages are present they must follow the reload frame.
        let reload_pos = msgs.iter().position(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched { reload: true, .. }
            )
        });
        assert!(
            reload_pos.is_some(),
            "reload frame must be present in the batch: {msgs:?}"
        );
        let first_history_pos = msgs.iter().position(|m| {
            matches!(
                m,
                WsServerMessage::UserMessageEcho { .. } | WsServerMessage::AssistantMessage { .. }
            )
        });
        if let Some(hi) = first_history_pos {
            assert!(
                reload_pos.unwrap() < hi,
                "ConversationSwitched{{reload:true}} must precede history messages: {msgs:?}"
            );
        }

        // Assert last_sent_seq was reset to the batch max (seqs 2-4 after seam=1).
        assert_eq!(
            conn.last_sent_seq,
            Some(4),
            "gap-reload must reset last_sent_seq to batch max, not retain stale cursor"
        );
    }

    /// Normal reconnect within the seam window must NOT trigger reload.
    /// from_seq = seam_seq (not less than) → no gap → no reload.
    #[tokio::test]
    async fn send_history_no_gap_reload_when_from_seq_at_or_above_seam() {
        use indexmap::IndexMap;
        use std::sync::Arc;

        let mut apps = IndexMap::new();
        let mut cfg = crate::test_support::app_config::default_test_app_config("test", "Test App");
        cfg.history_replay_limit = 3;
        apps.insert("test".to_string(), cfg);
        let apps = Arc::new(apps);

        let (mut conn, mut ws_rx, db, user_id, conv_id) =
            test_ws_conn_with_resume_conv_and_apps(apps).await;

        // Seed 5 messages: seqs 0–4.
        // With limit=3: seam_seq=Some(1). Full replay sends seq > 1.
        // last_sent_seq after full replay = Some(4).
        // Incremental from_seq=Some(4) → 4 >= 1 → no gap → no reload.
        {
            let db_conn = db.lock().await;
            seed_user_messages(&db_conn, conv_id, user_id, 5);
        }

        // Full replay to establish last_sent_seq at the real seam level.
        conn.send_history(conv_id, None)
            .await
            .expect("full send_history");
        let _ = collect_messages(&mut ws_rx).await;

        // from_seq = last_sent_seq (at or above seam) → no gap.
        let from = conn.last_sent_seq;
        conn.send_history(conv_id, from)
            .await
            .expect("incremental send_history");
        let msgs = collect_messages(&mut ws_rx).await;

        // Must NOT include a ConversationSwitched with reload=true.
        let has_reload = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched { reload: true, .. }
            )
        });
        assert!(
            !has_reload,
            "incremental replay within seam window must not trigger reload: {msgs:?}"
        );
    }

    /// Boundary: from_seq == seam_seq must NOT trigger reload (condition is strictly less-than).
    ///
    /// An off-by-one regression changing `f < s` to `f <= s` would incorrectly fire
    /// a reload for clients exactly at the seam, causing spurious history-clear on reconnect.
    #[tokio::test]
    async fn send_history_no_gap_reload_when_from_seq_exactly_at_seam() {
        use indexmap::IndexMap;
        use std::sync::Arc;

        let mut apps = IndexMap::new();
        let mut cfg = crate::test_support::app_config::default_test_app_config("test", "Test App");
        cfg.history_replay_limit = 3;
        apps.insert("test".to_string(), cfg);
        let apps = Arc::new(apps);

        let (mut conn, mut ws_rx, db, user_id, conv_id) =
            test_ws_conn_with_resume_conv_and_apps(apps).await;

        // Seed 5 messages: seqs 0–4. limit=3 → seam_seq=Some(1).
        // Set from_seq=Some(1) (exactly at the seam) → must not reload.
        {
            let db_conn = db.lock().await;
            seed_user_messages(&db_conn, conv_id, user_id, 5);
        }

        conn.last_sent_seq = Some(1); // Exactly at the seam boundary.
        conn.send_history(conv_id, Some(1))
            .await
            .expect("incremental send_history at seam boundary");
        let msgs = collect_messages(&mut ws_rx).await;

        let has_reload = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched { reload: true, .. }
            )
        });
        assert!(
            !has_reload,
            "from_seq == seam_seq must not trigger reload (boundary: strictly less-than): {msgs:?}"
        );
    }
}

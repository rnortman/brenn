//! `handle_run_target`, `TargetTaskContext`, `run_target_task`.

use std::sync::Arc;

use brenn_lib::conversation;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::WsServerMessage;
use tracing::warn;

use super::connection::WsConnection;
use crate::active_bridge::ActiveBridge;

/// Context for a target command execution task.
pub(super) struct TargetTaskContext {
    pub(super) bridge: Arc<ActiveBridge>,
    pub(super) db: brenn_lib::db::Db,
    pub(super) conv_id: i64,
    pub(super) target: brenn_lib::config::AttachmentTarget,
    pub(super) written_files: Vec<crate::routes::upload::WrittenFile>,
    pub(super) working_dir: std::path::PathBuf,
    pub(super) container_spawn: Option<brenn_lib::config::ContainerSpawnConfig>,
    pub(super) path_mapper: brenn_lib::config::PathMapper,
}

/// Execute a target command, persist the result, broadcast to clients, and send CC context.
///
/// Runs as a detached task so it doesn't block the WS message loop during
/// potentially slow command execution.
pub(super) async fn run_target_task(ctx: TargetTaskContext) {
    let result = crate::routes::target_handler::run_command_handler(
        &ctx.target,
        &ctx.written_files,
        &ctx.working_dir,
        ctx.container_spawn.as_ref(),
        &ctx.path_mapper,
    )
    .await;

    // Build the CC context message — this is what gets sent to Claude Code
    // and also what we show the user in the detail view, so they see the same thing.
    let cc_context =
        crate::routes::target_handler::build_cc_context(&ctx.target, &ctx.written_files, &result);

    // Persist the target result as a system message.
    let filenames: Vec<String> = ctx
        .written_files
        .iter()
        .map(|f| f.filename.clone())
        .collect();
    let payload = serde_json::json!({
        "target": ctx.target.name,
        "success": result.success,
        "summary": result.summary,
        "detail": cc_context,
        "files": filenames,
    });
    let target_db_seq = {
        let conn = ctx.db.lock().await;
        let (_, seq) = conversation::append_message(
            &conn,
            ctx.conv_id,
            conversation::MessageDirection::Incoming,
            "target_result",
            None,
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );
        seq
    };

    // Broadcast to WS clients. seq: Some(target_db_seq) lets the frontend
    // deduplicate this live broadcast against a concurrent history replay.
    ctx.bridge.broadcast(WsServerMessage::TargetResult {
        target: ctx.target.name.clone(),
        success: result.success,
        summary: result.summary.clone(),
        detail: Some(cc_context.clone()),
        files: filenames,
        seq: Some(target_db_seq),
    });

    // Send CC context so it can respond about the results.
    if let Err(e) = ctx.bridge.send_message(&cc_context).await {
        warn!("failed to send target result to CC: {e}");
    }

    // Clean up uploaded files only on success. On failure, leave them in place
    // so the user can retry. The orphan cleanup loop will eventually remove them
    // if they remain unreferenced after 24 hours.
    if result.success {
        for wf in &ctx.written_files {
            if let Err(e) = tokio::fs::remove_file(&wf.path).await {
                warn!(
                    error = %e,
                    file = %wf.path.display(),
                    "RunTarget: failed to clean up uploaded file"
                );
            }
        }
    }
}

// impl WsConnection — run-target command execution and bridge setup for targets
impl WsConnection {
    /// Handle RunTarget: execute a target handler within the current (or new) conversation.
    ///
    /// Resolves upload IDs from PendingUploads, runs the command, persists the result
    /// as a system message, sends CC context, and broadcasts to WS clients.
    /// Returns a JoinHandle for the spawned command task (None if early return).
    /// Production code ignores it; tests can await it.
    pub(super) async fn handle_run_target(
        &mut self,
        target_name: &str,
        upload_ids: &[String],
    ) -> Option<tokio::task::JoinHandle<()>> {
        if self.viewer_only {
            let _ = self.send_ws(WsServerMessage::AppBusy {
                message: "This app has an active session from another user.".to_string(),
            });
            return None;
        }

        if upload_ids.is_empty() {
            log_and_alert_security_event(
                &self.state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                self.client_ip,
                &format!("RunTarget: empty upload_ids for target {target_name:?}"),
            );
            let _ = self.send_ws(WsServerMessage::Error {
                message: "No files to process".to_string(),
            });
            return None;
        }

        // Extract everything we need from app_config upfront to avoid holding
        // the borrow across &mut self calls (ensure_bridge_for_target).
        let (target, working_dir, container_spawn, path_mapper) = {
            let app_config = self.app_config();
            let target = match app_config
                .attachment_targets
                .iter()
                .find(|t| t.name == target_name)
            {
                Some(t) => t.clone(),
                None => {
                    log_and_alert_security_event(
                        &self.state.alert_dispatcher,
                        SecurityEventType::SchemaViolation,
                        self.client_ip,
                        &format!(
                            "RunTarget: unknown target {target_name:?} for app {}",
                            self.app_slug
                        ),
                    );
                    let _ = self.send_ws(WsServerMessage::Error {
                        message: format!("Unknown target: {target_name}"),
                    });
                    return None;
                }
            };
            (
                target,
                app_config.working_dir.clone(),
                app_config.container_spawn.clone(),
                app_config.path_mapper.clone(),
            )
        };

        // Resolve upload IDs from PendingUploads.
        let mut written_files: Vec<crate::routes::upload::WrittenFile> = Vec::new();
        {
            let mut pending_guard = self.state.pending_uploads.lock().await;
            for id_str in upload_ids {
                let upload_id: uuid::Uuid = match id_str.parse() {
                    Ok(id) => id,
                    Err(_) => {
                        log_and_alert_security_event(
                            &self.state.alert_dispatcher,
                            SecurityEventType::SchemaViolation,
                            self.client_ip,
                            &format!("RunTarget: invalid upload_id: {id_str}"),
                        );
                        let _ = self.send_ws(WsServerMessage::Error {
                            message: format!("Invalid upload_id: {id_str}"),
                        });
                        return None;
                    }
                };
                let entry = match pending_guard.get(&upload_id) {
                    Some(e) => e,
                    None => {
                        let _ = self.send_ws(WsServerMessage::Error {
                            message: format!("Upload not found: {upload_id}"),
                        });
                        return None;
                    }
                };
                // Validate app slug matches.
                if entry.app_slug != self.app_slug {
                    log_and_alert_security_event(
                        &self.state.alert_dispatcher,
                        SecurityEventType::SchemaViolation,
                        self.client_ip,
                        &format!(
                            "RunTarget: upload_id {upload_id} belongs to app {}, not {}",
                            entry.app_slug, self.app_slug
                        ),
                    );
                    let _ = self.send_ws(WsServerMessage::Error {
                        message: "Upload not found".to_string(),
                    });
                    return None;
                }
                // Validate uploader matches.
                if entry.uploader_user_id != self.user_id {
                    log_and_alert_security_event(
                        &self.state.alert_dispatcher,
                        SecurityEventType::SchemaViolation,
                        self.client_ip,
                        &format!(
                            "RunTarget: upload_id {upload_id} belongs to user {}, not {}",
                            entry.uploader_user_id, self.user_id
                        ),
                    );
                    let _ = self.send_ws(WsServerMessage::Error {
                        message: "Upload not found".to_string(),
                    });
                    return None;
                }

                let file_path = working_dir.join("attachments").join(&entry.disk_filename);
                written_files.push(crate::routes::upload::WrittenFile {
                    upload_id,
                    filename: entry.filename.clone(),
                    disk_filename: entry.disk_filename.clone(),
                    media_type: entry.media_type.clone(),
                    size: entry.size,
                    path: file_path,
                });
            }
            // Remove resolved uploads from pending.
            for wf in &written_files {
                pending_guard.remove(&wf.upload_id);
            }
        }

        // Ensure we have a conversation + bridge.
        let bridge = self.resolve_bridge(None).await;
        let bridge = match bridge {
            Some(b) => b,
            None => return None, // Error already sent to client.
        };

        let conv_id = self
            .current_conversation_id
            .expect("resolve_bridge must set current_conversation_id");

        // Spawn the command execution as a detached task so it doesn't block the
        // WS message loop. The user can still switch conversations, stop CC, etc.
        // while the command runs. Everything the task needs is owned/Arc'd.
        let handle = tokio::spawn(run_target_task(TargetTaskContext {
            bridge,
            db: self.state.db.clone(),
            conv_id,
            target,
            written_files,
            working_dir,
            container_spawn,
            path_mapper,
        }));
        Some(handle)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use brenn_lib::auth::user::create_user;
    use brenn_lib::config::AppConfig;

    use crate::test_support::app_config::default_test_app_config;
    use brenn_lib::conversation;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::ws_types::WsServerMessage;
    use indexmap::IndexMap;
    use tokio::sync::{broadcast, mpsc};

    use super::super::connection::WsConnection;
    use super::super::testing::*;
    use crate::active_bridge::ActiveBridge;
    use crate::state::AppState;

    // -----------------------------------------------------------------------
    // handle_run_target tests
    // -----------------------------------------------------------------------

    /// Build an app config with an attachment target that runs `echo`.
    fn test_apps_with_target(working_dir: std::path::PathBuf) -> Arc<IndexMap<String, AppConfig>> {
        test_apps_with_target_command(working_dir, "echo", vec!["imported".to_string()])
    }

    /// Build an app config with an attachment target running the given command.
    fn test_apps_with_target_command(
        working_dir: std::path::PathBuf,
        program: &str,
        args: Vec<String>,
    ) -> Arc<IndexMap<String, AppConfig>> {
        use brenn_lib::config::AttachmentHandlerConfig;
        use brenn_lib::config::AttachmentTarget;

        let mut apps = IndexMap::new();
        let mut cfg = default_test_app_config("test", "Test App");
        cfg.working_dir = working_dir;
        cfg.attachment_targets = vec![AttachmentTarget {
            name: "import".to_string(),
            label: "Import".to_string(),
            accept: vec![".txt".to_string()],
            multi: false,
            handler: AttachmentHandlerConfig::Command {
                program: program.to_string(),
                args,
                file_roles: std::collections::HashMap::new(),
                timeout_secs: 5,
                cc_instructions: Some("Review results.".to_string()),
            },
        }];
        apps.insert("test".to_string(), cfg);
        Arc::new(apps)
    }

    /// Create a test WsConnection for target tests: has an attachment target,
    /// uses a temp dir as working_dir, injects a test bridge, and optionally
    /// creates a conversation.
    async fn test_ws_conn_for_target(
        with_conversation: bool,
    ) -> (
        WsConnection,
        mpsc::Receiver<WsServerMessage>,
        brenn_lib::db::Db,
        i64,
        std::path::PathBuf,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        let working_dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(working_dir.join("attachments")).unwrap();
        let apps = test_apps_with_target(working_dir.clone());
        test_ws_conn_for_target_with_apps(with_conversation, apps, tmp).await
    }

    /// Like `test_ws_conn_for_target` but with a custom apps config.
    async fn test_ws_conn_for_target_with_apps(
        with_conversation: bool,
        apps: Arc<IndexMap<String, AppConfig>>,
        tmp: tempfile::TempDir,
    ) -> (
        WsConnection,
        mpsc::Receiver<WsServerMessage>,
        brenn_lib::db::Db,
        i64,
        std::path::PathBuf,
        tempfile::TempDir,
    ) {
        let working_dir = tmp.path().to_path_buf();
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), Some(apps));

        let (ws_tx, ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        let (user_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            (uid, did)
        };

        let conv_id = if with_conversation {
            let conn = db.lock().await;
            conversation::create_conversation(&conn, user_id, TEST_APP_SLUG, false)
        } else {
            0 // Placeholder — no conversation
        };

        if with_conversation {
            let bridge = ActiveBridge::inject_for_test(
                user_id,
                conv_id,
                TEST_APP_SLUG,
                db.clone(),
                broadcast_tx,
            );
            // resolve_bridge uses wake_conversation (AppState) for resume/create paths.
            *state.test_wake_bridge.lock().await = Some(bridge);
        }

        let conn = WsConnBuilder {
            current_conversation_id: if with_conversation {
                Some(conv_id)
            } else {
                None
            },
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

        (conn, ws_rx, db, user_id, working_dir, tmp)
    }

    /// Register a pending upload in the state, write a file, return the UUID.
    async fn register_pending_upload(
        state: &AppState,
        working_dir: &std::path::Path,
        user_id: i64,
    ) -> uuid::Uuid {
        let upload_id = uuid::Uuid::new_v4();
        let filename = "test.txt".to_string();
        let disk_filename = format!("{upload_id}_{filename}");
        let file_path = working_dir.join("attachments").join(&disk_filename);
        tokio::fs::write(&file_path, "test content").await.unwrap();

        let pending = crate::state::PendingUpload {
            app_slug: TEST_APP_SLUG.to_string(),
            filename,
            disk_filename,
            media_type: "text/plain".to_string(),
            size: 12,
            uploaded_at: tokio::time::Instant::now(),
            uploader_user_id: user_id,
        };
        state
            .pending_uploads
            .lock()
            .await
            .insert(upload_id, pending);
        upload_id
    }

    #[tokio::test]
    async fn run_target_viewer_only_rejected() {
        let (mut conn, mut ws_rx, _db, _uid, _wd, _tmp) = test_ws_conn_for_target(true).await;
        conn.viewer_only = true;

        conn.handle_run_target("import", &["fake-id".to_string()])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, WsServerMessage::AppBusy { .. })),
            "expected AppBusy, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn run_target_empty_upload_ids_rejected() {
        let (mut conn, mut ws_rx, _db, _uid, _wd, _tmp) = test_ws_conn_for_target(true).await;

        conn.handle_run_target("import", &[]).await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.iter().any(
                |m| matches!(m, WsServerMessage::Error { message } if message.contains("No files"))
            ),
            "expected error about no files, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn run_target_unknown_target_rejected() {
        let (mut conn, mut ws_rx, _db, _uid, _wd, _tmp) = test_ws_conn_for_target(true).await;

        conn.handle_run_target("nonexistent", &["some-id".to_string()])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.iter().any(
                |m| matches!(m, WsServerMessage::Error { message } if message.contains("Unknown target"))
            ),
            "expected unknown target error, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn run_target_invalid_upload_id_rejected() {
        let (mut conn, mut ws_rx, _db, _uid, _wd, _tmp) = test_ws_conn_for_target(true).await;

        conn.handle_run_target("import", &["not-a-uuid".to_string()])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.iter().any(
                |m| matches!(m, WsServerMessage::Error { message } if message.contains("Invalid upload_id"))
            ),
            "expected invalid upload_id error, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn run_target_missing_upload_id_rejected() {
        let (mut conn, mut ws_rx, _db, _uid, _wd, _tmp) = test_ws_conn_for_target(true).await;

        let fake_id = uuid::Uuid::new_v4().to_string();
        conn.handle_run_target("import", &[fake_id]).await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.iter().any(
                |m| matches!(m, WsServerMessage::Error { message } if message.contains("Upload not found"))
            ),
            "expected upload not found error, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn run_target_wrong_app_rejected() {
        let (mut conn, mut ws_rx, _db, uid, _wd, _tmp) = test_ws_conn_for_target(true).await;

        // Register an upload with a different app slug.
        let upload_id = uuid::Uuid::new_v4();
        {
            let pending = crate::state::PendingUpload {
                app_slug: "other-app".to_string(),
                filename: "test.txt".to_string(),
                disk_filename: format!("{upload_id}_test.txt"),
                media_type: "text/plain".to_string(),
                size: 5,
                uploaded_at: tokio::time::Instant::now(),
                uploader_user_id: uid,
            };
            conn.state
                .pending_uploads
                .lock()
                .await
                .insert(upload_id, pending);
        }

        conn.handle_run_target("import", &[upload_id.to_string()])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.iter().any(
                |m| matches!(m, WsServerMessage::Error { message } if message.contains("Upload not found"))
            ),
            "expected upload not found error for wrong app, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn run_target_wrong_user_rejected() {
        let (mut conn, mut ws_rx, _db, _uid, wd, _tmp) = test_ws_conn_for_target(true).await;

        // Register an upload with a different user ID.
        let upload_id = uuid::Uuid::new_v4();
        let disk_filename = format!("{upload_id}_test.txt");
        tokio::fs::write(wd.join("attachments").join(&disk_filename), "x")
            .await
            .unwrap();
        {
            let pending = crate::state::PendingUpload {
                app_slug: TEST_APP_SLUG.to_string(),
                filename: "test.txt".to_string(),
                disk_filename,
                media_type: "text/plain".to_string(),
                size: 1,
                uploaded_at: tokio::time::Instant::now(),
                uploader_user_id: 9999, // Different user
            };
            conn.state
                .pending_uploads
                .lock()
                .await
                .insert(upload_id, pending);
        }

        conn.handle_run_target("import", &[upload_id.to_string()])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.iter().any(
                |m| matches!(m, WsServerMessage::Error { message } if message.contains("Upload not found"))
            ),
            "expected upload not found error for wrong user, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn run_target_success_persists_result() {
        let (mut conn, _ws_rx, db, uid, wd, _tmp) = test_ws_conn_for_target(true).await;

        let upload_id = register_pending_upload(&conn.state, &wd, uid).await;
        let conv_id = conn.current_conversation_id.unwrap();

        let handle = conn
            .handle_run_target("import", &[upload_id.to_string()])
            .await
            .expect("should return task handle");
        handle.await.expect("task should complete");

        // Check persistence: target_result message should be in DB.
        let db_conn = db.lock().await;
        let messages = conversation::get_messages(&db_conn, conv_id);
        let target_msg = messages.iter().find(|m| m.msg_type == "target_result");
        assert!(
            target_msg.is_some(),
            "expected target_result in DB, got types: {:?}",
            messages.iter().map(|m| &m.msg_type).collect::<Vec<_>>()
        );

        // Verify payload.
        let payload: serde_json::Value =
            serde_json::from_str(&target_msg.unwrap().payload).unwrap();
        assert_eq!(payload["target"], "import");
        assert_eq!(payload["success"], true);
        assert!(payload["detail"].as_str().unwrap().contains("imported"));

        // Verify direction is Incoming (system-generated).
        assert_eq!(
            target_msg.unwrap().direction,
            conversation::MessageDirection::Incoming
        );
    }

    #[tokio::test]
    async fn run_target_success_broadcasts_to_bridge() {
        let (mut conn, _ws_rx, _db, uid, wd, _tmp) = test_ws_conn_for_target(true).await;

        let upload_id = register_pending_upload(&conn.state, &wd, uid).await;

        // The bridge gets registered in active_bridges when handle_run_target
        // calls ensure_bridge_for_target → spawn_bridge. Subscribe to the
        // WS connection's broadcast_rx after the call to check bridge messages.
        conn.handle_run_target("import", &[upload_id.to_string()])
            .await;

        // After run_target, the bridge should be registered. Check it exists.
        let conv_id = conn.current_conversation_id.unwrap();
        let bridge = conn.state.active_bridges.get(conv_id).await;
        assert!(
            bridge.is_some(),
            "bridge should be registered after RunTarget"
        );
    }

    #[tokio::test]
    async fn run_target_removes_from_pending() {
        let (mut conn, _ws_rx, _db, uid, wd, _tmp) = test_ws_conn_for_target(true).await;

        let upload_id = register_pending_upload(&conn.state, &wd, uid).await;

        // Verify it's in pending.
        assert!(
            conn.state
                .pending_uploads
                .lock()
                .await
                .contains_key(&upload_id)
        );

        conn.handle_run_target("import", &[upload_id.to_string()])
            .await;

        // Should be removed from pending.
        assert!(
            !conn
                .state
                .pending_uploads
                .lock()
                .await
                .contains_key(&upload_id),
            "upload should be removed from pending after RunTarget"
        );
    }

    #[tokio::test]
    async fn run_target_cleans_up_uploaded_files() {
        let (mut conn, _ws_rx, _db, uid, wd, _tmp) = test_ws_conn_for_target(true).await;

        let upload_id = register_pending_upload(&conn.state, &wd, uid).await;
        let file_path = wd.join("attachments").join(format!("{upload_id}_test.txt"));

        // Verify file exists before.
        assert!(file_path.exists(), "uploaded file should exist before run");

        let handle = conn
            .handle_run_target("import", &[upload_id.to_string()])
            .await
            .expect("should return task handle");
        handle.await.expect("task should complete");

        // File should be cleaned up after.
        assert!(
            !file_path.exists(),
            "uploaded file should be removed after RunTarget"
        );
    }

    #[tokio::test]
    async fn run_target_preserves_files_on_failure() {
        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        let working_dir = tmp.path().to_path_buf();
        std::fs::create_dir_all(working_dir.join("attachments")).unwrap();
        let apps = test_apps_with_target_command(working_dir.clone(), "false", vec![]);
        let (mut conn, _ws_rx, _db, uid, wd, _tmp) =
            test_ws_conn_for_target_with_apps(true, apps, tmp).await;

        let upload_id = register_pending_upload(&conn.state, &wd, uid).await;
        let file_path = wd.join("attachments").join(format!("{upload_id}_test.txt"));

        assert!(file_path.exists(), "uploaded file should exist before run");

        let handle = conn
            .handle_run_target("import", &[upload_id.to_string()])
            .await
            .expect("should return task handle");
        handle.await.expect("task should complete");

        // File should be preserved on failure so the user can retry.
        assert!(
            file_path.exists(),
            "uploaded file should be preserved after failed RunTarget"
        );
    }

    #[tokio::test]
    async fn run_target_creates_conversation_when_none_active() {
        let (mut conn, mut ws_rx, db, uid, wd, _tmp) = test_ws_conn_for_target(false).await;

        // Inject a test bridge so wake_conversation succeeds.
        // Use conv_id=1 because it's the first conversation created in this
        // fresh DB. attach_to_bridge_with_rx overwrites current_conversation_id
        // with bridge.conversation_id, so this must match the real conv_id.
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);
        *conn.state.test_wake_bridge.lock().await = Some(ActiveBridge::inject_for_test(
            uid,
            1,
            "test",
            db.clone(),
            broadcast_tx,
        ));

        let upload_id = register_pending_upload(&conn.state, &wd, uid).await;

        assert!(conn.current_conversation_id.is_none());

        let handle = conn
            .handle_run_target("import", &[upload_id.to_string()])
            .await
            .expect("should return task handle");

        // Should have created a conversation (set before task spawns).
        assert!(
            conn.current_conversation_id.is_some(),
            "should have created a conversation"
        );

        // Wait for task to complete before checking DB persistence.
        handle.await.expect("task should complete");

        // Should have sent ConversationSwitched.
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, WsServerMessage::ConversationSwitched { .. })),
            "expected ConversationSwitched, got: {msgs:?}"
        );

        // Result should be persisted in the new conversation.
        let conv_id = conn.current_conversation_id.unwrap();
        let db_conn = db.lock().await;
        let messages = conversation::get_messages(&db_conn, conv_id);
        assert!(
            messages.iter().any(|m| m.msg_type == "target_result"),
            "expected target_result in new conversation"
        );

        // resolve_bridge is called with title_text=None; the new conversation
        // must have no title set.
        let conv = conversation::get_conversation(&db_conn, conv_id);
        assert!(
            conv.title.is_none(),
            "new conversation should have no title when resolve_bridge called with title_text=None, got: {:?}",
            conv.title
        );
    }

    /// run_target via the resume-dead-bridge path (Case 2): conversation exists
    /// in DB but no active bridge. resolve_bridge(None) must not set a title.
    #[tokio::test]
    async fn run_target_resume_dead_bridge_title_remains_none() {
        let (mut conn, mut ws_rx, db, uid, wd, _tmp) = test_ws_conn_for_target(false).await;

        // Create a conversation in DB with no title (like one created without text).
        let conv_id = {
            let db_conn = db.lock().await;
            conversation::create_conversation(&db_conn, uid, "test", false)
        };
        conn.current_conversation_id = Some(conv_id);

        // Verify no title before the call.
        {
            let db_conn = db.lock().await;
            let conv = conversation::get_conversation(&db_conn, conv_id);
            assert!(conv.title.is_none(), "precondition: no title before run");
        }

        // Inject test bridge so wake_conversation (Case 2 resume) succeeds.
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);
        *conn.state.test_wake_bridge.lock().await = Some(ActiveBridge::inject_for_test(
            uid,
            conv_id,
            "test",
            db.clone(),
            broadcast_tx,
        ));

        let upload_id = register_pending_upload(&conn.state, &wd, uid).await;

        let handle = conn
            .handle_run_target("import", &[upload_id.to_string()])
            .await
            .expect("should return task handle");
        handle.await.expect("task should complete");

        // Drain WS messages (ConversationSwitched etc.) — just ensure no error.
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, WsServerMessage::Error { .. })),
            "expected no error messages, got: {msgs:?}"
        );

        // Title must remain None: resolve_bridge(None) must not set a title.
        let db_conn = db.lock().await;
        let conv = conversation::get_conversation(&db_conn, conv_id);
        assert!(
            conv.title.is_none(),
            "conversation title should remain None after resume-dead-bridge run_target, got: {:?}",
            conv.title
        );
    }
}

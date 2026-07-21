//! `handle_reopen_artifact`, `handle_load_artifact_snapshot`.

use brenn_lib::conversation;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::WsServerMessage;
use tracing::warn;

use super::connection::WsConnection;

// impl WsConnection — artifact viewer (reopen, snapshot load)
impl WsConnection {
    /// Handle ReopenArtifact: load from DB (if message_id given) or re-read from disk.
    pub(super) async fn handle_reopen_artifact(&self, file_path: &str, message_id: Option<i64>) {
        self.touch_ui_activity("ReopenArtifact").await;
        if let Some(message_id) = message_id {
            // Load stored snapshot from DB.
            self.handle_load_artifact_snapshot(message_id).await;
            return;
        }

        // No message_id — read from disk (current behavior).
        let cwd = if let Some(conv_id) = self.current_conversation_id {
            let conn = self.state.db.lock().await;
            let conv = conversation::get_conversation_opt(&conn, conv_id);
            conv.and_then(|c| c.cwd)
        } else {
            None
        };

        let Some(cwd) = cwd else {
            let _ = self.send_ws(WsServerMessage::Error {
                message: crate::artifact::ArtifactError::NoCwd.to_string(),
            });
            return;
        };

        let Some(app) = self.state.apps.get(&self.app_slug) else {
            // App config should always exist for an active WS connection
            // — same shape as `handle_load_artifact_snapshot`.
            warn!(slug = %self.app_slug, "ReopenArtifact: app not found");
            let _ = self.send_ws(WsServerMessage::Error {
                message: "App not found".to_string(),
            });
            return;
        };
        let mounts = crate::artifact::mount_roots_for(&app.mounts);
        let frontmatter = &app.frontmatter;

        match crate::artifact::read_artifact_content(file_path, std::path::Path::new(&cwd), &mounts)
            .await
        {
            Ok((display_path, raw_content)) => {
                let rendered_html =
                    crate::frontmatter::render_markdown_with_frontmatter(&raw_content, frontmatter);
                let _ = self.send_ws(WsServerMessage::ArtifactContent {
                    file_path: display_path,
                    rendered_html,
                    raw_content,
                    snapshot: None,
                    seq: None,
                });
            }
            Err(crate::artifact::ArtifactError::PathTraversal { .. }) => {
                // Browser-originated path traversal attempt — log for fail2ban.
                log_and_alert_security_event(
                    &self.state.alert_dispatcher,
                    SecurityEventType::MalformedMessage,
                    self.client_ip,
                    &format!("ReopenArtifact path traversal attempt: {file_path}"),
                );
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Access denied".to_string(),
                });
            }
            Err(e) => {
                warn!("ReopenArtifact failed: {e}");
                let _ = self.send_ws(WsServerMessage::Error {
                    message: e.to_string(),
                });
            }
        }
    }

    /// Handle LoadArtifactSnapshot: load a stored artifact by message id.
    pub(super) async fn handle_load_artifact_snapshot(&self, message_id: i64) {
        self.touch_ui_activity("LoadArtifactSnapshot").await;
        let Some(app) = self.state.apps.get(&self.app_slug) else {
            // App config should always exist for an active WS connection.
            warn!(slug = %self.app_slug, "LoadArtifactSnapshot: app not found");
            let _ = self.send_ws(WsServerMessage::Error {
                message: "App not found".to_string(),
            });
            return;
        };
        let working_dir = app.working_dir.as_path();
        let slug = app.slug.as_str();
        let multiuser = app.multiuser;
        let mounts = crate::artifact::mount_roots_for(&app.mounts);
        let frontmatter = &app.frontmatter;
        let result = {
            let conn = self.state.db.lock().await;
            crate::artifact_snapshot::load_artifact_snapshot(
                &conn,
                message_id,
                self.user_id,
                working_dir,
                slug,
                multiuser,
                &mounts,
                frontmatter,
            )
        };
        match result {
            Ok(msg) => {
                let _ = self.send_ws(msg);
            }
            Err(crate::artifact_snapshot::LoadSnapshotError::NotFound) => {
                // Could be genuinely missing or another user's artifact.
                // Don't distinguish — avoids leaking existence info.
                warn!(
                    message_id,
                    "LoadArtifactSnapshot: not found or unauthorized"
                );
                let _ = self.send_ws(WsServerMessage::Error {
                    message: "Artifact not found".to_string(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::conversation;
    use brenn_lib::ws_types::WsServerMessage;

    use super::super::testing::*;

    #[tokio::test]
    async fn new_conversation_sends_empty_artifact_index() {
        let (mut conn, mut ws_rx, _db, _user_id) = test_ws_conn_for_app(test_apps()).await;

        conn.handle_new_conversation().await;

        let msgs = collect_messages(&mut ws_rx).await;

        // Should have ConversationSwitched, HistoryComplete, and ArtifactIndex.
        let artifact_index = msgs.iter().find_map(|m| match m {
            WsServerMessage::ArtifactIndex { files } => Some(files),
            _ => None,
        });
        assert!(
            artifact_index.is_some(),
            "expected ArtifactIndex in messages: {msgs:?}"
        );
        assert!(
            artifact_index.unwrap().is_empty(),
            "new conversation should have empty ArtifactIndex"
        );
    }

    #[tokio::test]
    async fn switch_conversation_sends_artifact_index_with_files() {
        let (mut conn, mut ws_rx, db, user_id) = test_ws_conn_for_app(test_apps()).await;

        // Create a conversation and store an artifact.
        let conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, user_id, "test", false)
        };
        {
            let c = db.lock().await;
            crate::artifact_snapshot::store_artifact_snapshot(
                &c,
                conv_id,
                "docs/plan.md",
                "# Plan\n\nHello",
                "t1",
            );
        }

        conn.handle_switch_conversation(conv_id).await;

        let msgs = collect_messages(&mut ws_rx).await;

        // ArtifactIndex should contain the stored file.
        let artifact_index = msgs.iter().find_map(|m| match m {
            WsServerMessage::ArtifactIndex { files } => Some(files),
            _ => None,
        });
        assert!(
            artifact_index.is_some(),
            "expected ArtifactIndex in messages: {msgs:?}"
        );
        let files = artifact_index.unwrap();
        assert_eq!(files.len(), 1, "should have one file in index");
        assert_eq!(files[0].file_path, "docs/plan.md");
        assert_eq!(files[0].versions.len(), 1);
        assert_eq!(files[0].versions[0].version, 1);

        // ArtifactIndex should come after HistoryComplete.
        let history_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::HistoryComplete { .. }))
            .expect("missing HistoryComplete");
        let index_idx = msgs
            .iter()
            .position(|m| matches!(m, WsServerMessage::ArtifactIndex { .. }))
            .expect("missing ArtifactIndex");
        assert!(
            history_idx < index_idx,
            "HistoryComplete should come before ArtifactIndex: {msgs:?}"
        );
    }
}

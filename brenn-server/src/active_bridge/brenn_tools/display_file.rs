//! DisplayFile tool: artifact viewer; sandbox path validation against cwd + RW mount roots.

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_lib::approval_rules::ApprovalMatch;
use brenn_lib::conversation;
use brenn_lib::ws_types::WsServerMessage;
use std::path::Path;
use tracing::{info, warn};

use super::super::ActiveBridge;
use super::super::mcp_constants::MCP_DISPLAY_FILE_TOOL;
use super::super::tool_summary::{HandleBrennToolResult, emit_tool_summary, mark_tool_handled};

/// Handle both PreToolUse and PostToolUse arms for `MCP_DISPLAY_FILE_TOOL`.
///
/// Returns `Some(...)` when the request is for this tool family (Pre or Post)
/// and `None` otherwise — letting the dispatcher fall through to other arms.
pub(super) async fn handle(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    match &req.kind {
        // --- DisplayFile PreToolUse ---
        ApprovalKind::PreToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_DISPLAY_FILE_TOOL => {
            let file_path = match tool_input.get("file_path").and_then(|v| v.as_str()) {
                Some(p) if !p.is_empty() => p,
                _ => {
                    warn!("DisplayFile called without valid file_path in tool_input");
                    return Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny {
                        reason: "mcp__brenn__DisplayFile requires a non-empty 'file_path' argument"
                            .into(),
                    }));
                }
            };

            // Translate CC-reported file path to host path if containerized.
            // Hard-deny when the path is absolute but outside all container
            // mappings: falling back to the container path would surface a
            // confusing "file not found" from validate_artifact_path when the
            // real problem is "path is outside any mounted directory".
            let host_file_path = if Path::new(file_path).is_absolute() {
                match bridge.path_mapper.to_host(Path::new(file_path)) {
                    Some(p) => p.to_string_lossy().to_string(),
                    None => {
                        warn!(
                            user_id = bridge.user_id,
                            conversation_id = bridge.conversation_id,
                            app_slug = %bridge.app_slug,
                            cc_path = %file_path,
                            "DisplayFile: path outside container mapping"
                        );
                        return Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny {
                            reason: format!(
                                "file_path {file_path} is not under any mounted directory"
                            ),
                        }));
                    }
                }
            } else {
                file_path.to_string()
            };

            info!(
                tool = %tool_name,
                cc_path = %file_path,
                host_path = %host_file_path,
                "intercepting DisplayFile PreToolUse",
            );

            let cwd = {
                let conn = bridge.db.lock().await;
                let conv = conversation::get_conversation(&conn, bridge.conversation_id);
                conv.cwd
            };

            let cwd = match cwd {
                Some(cwd) => cwd,
                None => {
                    let err = crate::artifact::ArtifactError::NoCwd;
                    warn!("DisplayFile called but no cwd set for conversation");
                    bridge.broadcast(WsServerMessage::Error {
                        message: err.to_string(),
                    });
                    return Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny {
                        reason: err.to_string(),
                    }));
                }
            };

            let mount_roots = bridge.artifact_mount_roots();

            match crate::artifact::read_artifact_content(
                &host_file_path,
                Path::new(&cwd),
                &mount_roots,
            )
            .await
            {
                Ok((display_path, raw_content)) => {
                    let snapshot_result = {
                        let conn = bridge.db.lock().await;
                        crate::artifact_snapshot::store_artifact_snapshot(
                            &conn,
                            bridge.conversation_id,
                            &display_path,
                            &raw_content,
                            tool_use_id,
                        )
                    };

                    let rendered_html = crate::frontmatter::render_markdown_with_frontmatter(
                        &raw_content,
                        &bridge.frontmatter,
                    );

                    let stable_url = crate::artifact::compute_stable_url(
                        &display_path,
                        Path::new(&cwd),
                        &bridge.working_dir,
                        &mount_roots,
                        &bridge.app_slug,
                    );

                    info!(
                        file = %display_path,
                        version = snapshot_result.version,
                        "broadcasting artifact from DisplayFile tool"
                    );
                    bridge.broadcast(WsServerMessage::ArtifactContent {
                        file_path: display_path,
                        rendered_html,
                        raw_content,
                        snapshot: Some(brenn_lib::ws_types::SnapshotMetadata {
                            message_id: snapshot_result.artifact_message_id,
                            version: snapshot_result.version,
                            total_versions: snapshot_result.total_versions,
                            seq: snapshot_result.display_seq,
                            stable_url,
                        }),
                        seq: None,
                    });

                    let artifact_index = {
                        let conn = bridge.db.lock().await;
                        crate::artifact_snapshot::get_artifact_index(&conn, bridge.conversation_id)
                    };
                    bridge.broadcast(WsServerMessage::ArtifactIndex {
                        files: artifact_index,
                    });

                    Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                        updated_input: None,
                    }))
                }
                Err(e) => {
                    warn!("DisplayFile artifact read failed: {e}");
                    bridge.broadcast(WsServerMessage::Error {
                        message: e.to_string(),
                    });
                    Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny {
                        reason: e.to_string(),
                    }))
                }
            }
        }

        // --- DisplayFile PostToolUse ---
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_DISPLAY_FILE_TOOL => {
            let file_path = tool_input
                .get("file_path")
                .and_then(|v| v.as_str())
                .unwrap_or("(unknown)");

            // Consume from pending_tool_uses and mark as handled so the ToolResult
            // handler doesn't emit a duplicate summary.
            mark_tool_handled(bridge, tool_use_id).await;

            emit_tool_summary(
                bridge,
                tool_name,
                tool_input,
                None,
                Some(&ApprovalMatch::GlobalTool),
                false,
            )
            .await;
            Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(format!(
                        "File '{file_path}' has been displayed in the user's artifact viewer."
                    )),
                },
            ))
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use brenn_cc::session::{
        ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest, SessionEvent,
    };
    use brenn_lib::config::PathMapper;
    use brenn_lib::ws_types::WsServerMessage;
    use tokio::sync::oneshot;

    use super::super::super::mcp_constants::MCP_DISPLAY_FILE_TOOL;
    use super::super::super::test_support::{
        await_fence, event_fence, mk_rw_mount_with_container, recv_broadcast, test_bridge,
        test_bridge_with_cwd, test_bridge_with_cwd_and_mounts,
        test_bridge_with_cwd_and_mounts_and_mapper,
    };

    #[tokio::test]
    async fn display_file_pre_tool_use_broadcasts_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let md_path = dir.path().join("test.md");
        // Include a YAML frontmatter block so we exercise both the
        // frontmatter and body-markdown render paths in one shot.
        std::fs::write(&md_path, "---\nstatus: in_progress\n---\n# Hello\n\nWorld").unwrap();

        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd(dir.path().to_str().unwrap()).await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_display".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": "test.md"}),
                tool_use_id: "t_display".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Should broadcast ArtifactContent.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::ArtifactContent {
                file_path,
                rendered_html,
                snapshot,
                ..
            } => {
                assert_eq!(file_path, "test.md");
                assert!(
                    rendered_html.contains("<h1>Hello</h1>"),
                    "should contain rendered heading: {rendered_html}"
                );
                assert!(
                    rendered_html.contains("class=\"fm-block\""),
                    "should contain frontmatter block: {rendered_html}"
                );
                let snap = snapshot.as_ref().expect("should have snapshot metadata");
                assert_eq!(snap.version, 1);
                assert_eq!(snap.total_versions, 1);
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }

        // Should also broadcast ArtifactIndex with the file.
        let index_msg = recv_broadcast(&mut broadcast_rx).await;
        match &index_msg {
            WsServerMessage::ArtifactIndex { files } => {
                assert_eq!(files.len(), 1, "should have one file in index");
                assert_eq!(files[0].file_path, "test.md");
                assert_eq!(files[0].versions.len(), 1);
                assert_eq!(files[0].versions[0].version, 1);
            }
            other => panic!("expected ArtifactIndex, got {other:?}"),
        }

        // Decision should be Allow.
        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Allow { .. }),
            "PreToolUse for DisplayFile should Allow, got {decision:?}"
        );
    }

    #[tokio::test]
    async fn display_file_post_tool_use_injects_result() {
        let (_bridge, event_tx, mut _broadcast_rx) = test_bridge_with_cwd("/tmp").await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_post_display".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": "docs/plan.md"}),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_post_display".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Continue { updated_output } => {
                let output = updated_output.expect("should have updated_output");
                assert!(
                    output.contains("docs/plan.md"),
                    "output should mention file path: {output}"
                );
                assert!(
                    output.contains("displayed"),
                    "output should confirm display: {output}"
                );
            }
            other => panic!("expected Continue with output, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn display_file_missing_file_path_denies() {
        let (_bridge, event_tx, mut _broadcast_rx) = test_bridge_with_cwd("/tmp").await;

        // Missing file_path key entirely.
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_no_path".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({}), // no file_path
                tool_use_id: "t_no_path".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Should Deny with a reason that names the expected argument so the LLM
        // can self-correct instead of seeing a false success.
        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Deny { reason } => {
                assert!(
                    reason.contains("file_path"),
                    "deny reason should mention file_path: {reason}"
                );
            }
            other => panic!("missing file_path should Deny, got {other:?}"),
        }

        // Empty-string file_path is the same kind of malformed call.
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_empty_path".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": ""}),
                tool_use_id: "t_empty_path".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Deny { reason } => {
                assert!(
                    reason.contains("file_path"),
                    "deny reason should mention file_path: {reason}"
                );
            }
            other => panic!("empty file_path should Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn display_file_no_cwd_denies_and_broadcasts_error() {
        // test_bridge() creates a conversation without cwd set.
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_no_cwd".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": "README.md"}),
                tool_use_id: "t_no_cwd".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Should broadcast Error about missing cwd.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::Error { message } => {
                assert!(
                    message.contains("working directory"),
                    "error should mention cwd: {message}"
                );
            }
            other => panic!("expected Error about cwd, got {other:?}"),
        }

        // CC should receive a Deny so it knows the tool failed.
        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Deny { reason } => {
                assert!(
                    reason.contains("working directory"),
                    "deny reason should mention cwd: {reason}"
                );
            }
            other => panic!("expected Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn display_file_outside_cwd_denies_with_error() {
        // Regression: DisplayFile for a path outside the working directory must
        // deny the tool use so CC gets the error, not just show a browser error.
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.md");
        std::fs::write(&outside_file, "# Secret").unwrap();

        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd(dir.path().to_str().unwrap()).await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_traversal".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": outside_file.to_str().unwrap()}),
                tool_use_id: "t_traversal".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // CC should receive a Deny with the error reason.
        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Deny { reason } => {
                assert!(
                    reason.contains("outside the working directory"),
                    "deny reason should explain the path is outside cwd: {reason}"
                );
            }
            other => panic!("expected Deny for path outside cwd, got {other:?}"),
        }

        // Browser should also get an error broadcast.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(&msg, WsServerMessage::Error { message } if message.contains("outside the working directory")),
            "should broadcast error to browser, got {msg:?}"
        );
    }

    #[tokio::test]
    async fn display_file_not_found_denies_with_error() {
        // DisplayFile for a nonexistent file should deny with a useful error.
        let dir = tempfile::tempdir().unwrap();
        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd(dir.path().to_str().unwrap()).await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_notfound".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": "nonexistent.md"}),
                tool_use_id: "t_notfound".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Deny { reason } => {
                assert!(
                    reason.contains("not found") || reason.contains("File not found"),
                    "deny reason should mention file not found: {reason}"
                );
            }
            other => panic!("expected Deny for missing file, got {other:?}"),
        }

        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(&msg, WsServerMessage::Error { .. }),
            "should broadcast error, got {msg:?}"
        );
    }

    #[tokio::test]
    async fn artifact_mount_roots_filters_working_dir_mount() {
        // The working-dir mount is already covered by the cwd root; including
        // it again would create a redundant entry whose slug could leak into
        // the display path under unusual configurations.
        let cwd = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let mounts = vec![
            brenn_lib::config::ResolvedMount {
                slug: "self".into(),
                host_path: cwd.path().to_path_buf(),
                container_path: None,
                access: brenn_lib::config::AccessLevel::ReadWrite,
                auto_pull: false,
                is_working_dir: true,
                primary: false,
            },
            brenn_lib::config::ResolvedMount {
                slug: "other".into(),
                host_path: other.path().to_path_buf(),
                container_path: None,
                access: brenn_lib::config::AccessLevel::ReadOnly,
                auto_pull: false,
                is_working_dir: false,
                primary: false,
            },
        ];

        let (bridge, _event_tx, _broadcast_rx) =
            test_bridge_with_cwd_and_mounts(cwd.path().to_str().unwrap(), mounts).await;

        let roots = bridge.artifact_mount_roots();
        assert_eq!(
            roots.len(),
            1,
            "working-dir mount should be filtered out, leaving only the non-working-dir mount"
        );
        assert_eq!(roots[0].slug, "other");
    }

    #[tokio::test]
    async fn display_file_in_working_dir_when_working_dir_is_a_mount() {
        // When working_dir IS a repo mount (working_dir_is_repo case), the
        // mount appears in `bridge.mounts` with is_working_dir=true. Files
        // in it must still display as cwd-relative (no slug prefix).
        let cwd = tempfile::tempdir().unwrap();
        let md = cwd.path().join("note.md");
        std::fs::write(&md, "# Note").unwrap();

        let mounts = vec![brenn_lib::config::ResolvedMount {
            slug: "myrepo".into(),
            host_path: cwd.path().to_path_buf(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: true,
            primary: false,
        }];

        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd_and_mounts(cwd.path().to_str().unwrap(), mounts).await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_wd_mount".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({
                    "file_path": md.to_str().unwrap(),
                }),
                tool_use_id: "t_wd_mount".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::ArtifactContent { file_path, .. } => {
                assert_eq!(
                    file_path, "note.md",
                    "working-dir mount file must NOT get a slug prefix"
                );
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn display_file_absolute_path_in_mount_succeeds_with_slug_prefix() {
        // DisplayFile with an absolute path inside a configured mount must
        // succeed (the bug we're fixing) and broadcast ArtifactContent with
        // the slug-prefixed display path.
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let sub = mount.path().join("kb").join("finance");
        std::fs::create_dir_all(&sub).unwrap();
        let mount_file = sub.join("tips.md");
        std::fs::write(&mount_file, "# Tips\n\nbody").unwrap();

        let mounts = vec![brenn_lib::config::ResolvedMount {
            slug: "graf-life".into(),
            host_path: mount.path().to_path_buf(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadOnly,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }];

        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd_and_mounts(cwd.path().to_str().unwrap(), mounts).await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_mount".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({
                    "file_path": mount_file.to_str().unwrap(),
                }),
                tool_use_id: "t_mount".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::ArtifactContent {
                file_path,
                rendered_html,
                snapshot,
                ..
            } => {
                assert_eq!(file_path, "graf-life/kb/finance/tips.md");
                assert!(rendered_html.contains("<h1>Tips</h1>"));
                let snap = snapshot.as_ref().expect("snapshot metadata");
                // Stable URL for a mount file points at the mount-aware route.
                assert_eq!(
                    snap.stable_url.as_deref(),
                    Some("/app/test/mount/graf-life/file/kb/finance/tips.md"),
                );
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }

        // Drain the index broadcast.
        let _ = recv_broadcast(&mut broadcast_rx).await;

        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Allow { .. }),
            "PreToolUse for mount file should Allow, got {decision:?}"
        );
    }

    #[tokio::test]
    async fn display_file_outside_cwd_and_mounts_denies() {
        // Path outside the cwd AND any mount must still be denied.
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.md");
        std::fs::write(&outside_file, "# Secret").unwrap();

        let mounts = vec![brenn_lib::config::ResolvedMount {
            slug: "graf-life".into(),
            host_path: mount.path().to_path_buf(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadOnly,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }];

        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd_and_mounts(cwd.path().to_str().unwrap(), mounts).await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_outside".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({
                    "file_path": outside_file.to_str().unwrap(),
                }),
                tool_use_id: "t_outside".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Deny { reason } => {
                assert!(
                    reason.contains("outside the working directory"),
                    "deny reason: {reason}"
                );
            }
            other => panic!("expected Deny, got {other:?}"),
        }
        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(matches!(&msg, WsServerMessage::Error { .. }));
    }

    #[tokio::test]
    async fn display_file_then_reopen_round_trips_mount_path() {
        // Round-trip: display a mount file (writes slug-prefixed path to DB),
        // verify the on-disk path stored in the snapshot can be re-resolved
        // by resolve_display_path so stable-URL recovery on history replay
        // works without panicking.
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        let mount_file = mount.path().join("note.md");
        std::fs::write(&mount_file, "# Note").unwrap();

        let mounts = vec![brenn_lib::config::ResolvedMount {
            slug: "graf-life".into(),
            host_path: mount.path().to_path_buf(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadOnly,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }];
        let mount_roots = vec![crate::artifact::MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "graf-life".into(),
        }];

        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd_and_mounts(cwd.path().to_str().unwrap(), mounts).await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_roundtrip".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({
                    "file_path": mount_file.to_str().unwrap(),
                }),
                tool_use_id: "t_roundtrip".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let msg = recv_broadcast(&mut broadcast_rx).await;
        let stored_display_path = match msg {
            WsServerMessage::ArtifactContent { file_path, .. } => file_path,
            other => panic!("expected ArtifactContent, got {other:?}"),
        };
        // Reverse-mapping the stored display path through the same mount
        // list must yield the original canonical mount file.
        let resolved =
            crate::artifact::resolve_display_path(&stored_display_path, cwd.path(), &mount_roots);
        assert_eq!(resolved, Some(mount_file.canonicalize().unwrap()));
    }

    /// Containerized DisplayFile: absolute container path under a mapped root
    /// must be translated, the file read, and ArtifactContent broadcast.
    #[tokio::test]
    async fn display_file_containerized_translates_absolute_path() {
        let host_root = tempfile::tempdir().unwrap();
        let host_repo = host_root.path().join("repo");
        std::fs::create_dir_all(&host_repo).unwrap();
        let host_file = host_repo.join("x.md");
        std::fs::write(&host_file, "# Hello from container").unwrap();

        let container_repo = std::path::PathBuf::from("/home/user/repos/repo");
        let mapper = PathMapper::container(vec![brenn_lib::config::PathMapping {
            host_root: host_repo.clone(),
            container_root: container_repo.clone(),
        }]);
        let mount = mk_rw_mount_with_container(host_repo.clone(), container_repo.clone());
        // cwd = host_root so validate_artifact_path can form slug-prefixed display paths.
        let cwd = host_root.path().to_str().unwrap();
        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd_and_mounts_and_mapper(cwd, vec![mount], mapper).await;

        let container_file_path = container_repo.join("x.md");
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_container_display".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({
                    "file_path": container_file_path.to_str().unwrap()
                }),
                tool_use_id: "t_container_display".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Must broadcast ArtifactContent.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::ArtifactContent {
                file_path: artifact_file_path,
                rendered_html,
                ..
            } => {
                assert!(
                    rendered_html.contains("Hello from container"),
                    "rendered HTML must contain file content: {rendered_html}"
                );
                // The file lives under cwd (which is host_root), so
                // validate_artifact_path strips cwd prefix and produces an un-prefixed
                // display path: "repo/x.md" (the sub-path relative to cwd, not slug-prefixed).
                assert_eq!(
                    artifact_file_path, "repo/x.md",
                    "ArtifactContent file_path must be cwd-relative display path: {artifact_file_path}"
                );
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }

        // Decision must be Allow.
        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Allow { .. }),
            "DisplayFile must Allow for a valid containerized path, got {decision:?}"
        );
    }

    /// Containerized DisplayFile: absolute path outside all container mappings
    /// must produce a Deny with a clear error, not a confused "file not found".
    #[tokio::test]
    async fn display_file_containerized_rejects_unmapped_path() {
        let host_root = tempfile::tempdir().unwrap();
        let host_repo = host_root.path().join("repo");
        std::fs::create_dir_all(&host_repo).unwrap();

        let container_repo = std::path::PathBuf::from("/home/user/repos/repo");
        let mapper = PathMapper::container(vec![brenn_lib::config::PathMapping {
            host_root: host_repo.clone(),
            container_root: container_repo.clone(),
        }]);
        let mount = mk_rw_mount_with_container(host_repo.clone(), container_repo.clone());
        let cwd = host_root.path().to_str().unwrap();
        let (_bridge, event_tx, mut broadcast_rx) =
            test_bridge_with_cwd_and_mounts_and_mapper(cwd, vec![mount], mapper).await;

        // /etc/shadow is outside all container mappings.
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_container_deny".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": "/etc/shadow"}),
                tool_use_id: "t_container_deny".into(),
            },
            response_tx: resp_tx,
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // No ArtifactContent must be broadcast.
        // Decision must be Deny with a reason mentioning "mounted directory".
        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Deny { reason } => {
                assert!(
                    reason.contains("mounted directory"),
                    "deny reason must mention mounted directory: {reason}"
                );
            }
            other => panic!("expected Deny, got {other:?}"),
        }

        // No ArtifactContent must have been broadcast.
        await_fence(fence).await;
        use tokio::sync::broadcast::error::TryRecvError;
        match broadcast_rx.try_recv() {
            Err(TryRecvError::Empty) => {} // correct
            Ok(msg) => panic!("unexpected broadcast: {msg:?}"),
            Err(e) => panic!("unexpected broadcast error: {e}"),
        }
    }
}

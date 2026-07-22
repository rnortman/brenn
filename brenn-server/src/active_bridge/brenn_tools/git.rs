//! Git tools: list/status/pull (read), commit-and-push/run (mutating). Uses `resolve_mounts` to dispatch slugs to working-tree paths.

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_lib::approval_rules::ApprovalMatch;
use tracing::info;

use super::super::ActiveBridge;
use super::super::mcp_constants::{
    MCP_GIT_LIST_REPOS_TOOL, MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL, MCP_GIT_REPO_RUN_TOOL,
    MCP_GIT_REPO_STATUS_TOOL,
};
use super::super::tool_summary::{HandleBrennToolResult, emit_tool_summary, mark_tool_handled};

/// Handle PreToolUse + PostToolUse arms for the Git tool family
/// (`MCP_GIT_LIST_REPOS_TOOL`, `MCP_GIT_REPO_STATUS_TOOL`,
/// `MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL`, `MCP_GIT_REPO_RUN_TOOL`). GitRepoPull is
/// a first-class registry tool, handled by `registry_adapter`.
///
/// Returns `Some(...)` when the request is for one of these tools and `None`
/// otherwise — letting the dispatcher fall through to other arms.
pub(super) async fn handle(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    match &req.kind {
        // --- GitListRepos PreToolUse ---
        ApprovalKind::PreToolUse { tool_name, .. } if tool_name == MCP_GIT_LIST_REPOS_TOOL => {
            info!("intercepting GitListRepos PreToolUse — auto-approve (read-only)");
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // --- GitListRepos PostToolUse ---
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_GIT_LIST_REPOS_TOOL => {
            mark_tool_handled(bridge, tool_use_id).await;

            let containerized = bridge.container_spawn.is_some();
            let repos: Vec<serde_json::Value> = bridge
                .mounts
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "slug": m.slug,
                        "path": m.visible_path(containerized).to_string_lossy(),
                        "access": m.access,
                        "auto_pull": m.auto_pull,
                    })
                })
                .collect();

            let output = serde_json::json!(repos);

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
                    updated_output: Some(output.to_string()),
                },
            ))
        }

        // --- GitRepoStatus PreToolUse ---
        ApprovalKind::PreToolUse { tool_name, .. } if tool_name == MCP_GIT_REPO_STATUS_TOOL => {
            info!("intercepting GitRepoStatus PreToolUse — auto-approve (read-only)");
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // --- GitRepoStatus PostToolUse ---
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_GIT_REPO_STATUS_TOOL => {
            mark_tool_handled(bridge, tool_use_id).await;

            let repo_slug = tool_input
                .get("repo")
                .and_then(|v| v.as_str())
                .unwrap_or("all");

            let resolved = resolve_mounts(&bridge.mounts, repo_slug);
            let results = match resolved {
                Ok(mounts) => {
                    let futs: Vec<_> = mounts
                        .iter()
                        .map(|m| crate::git_ops::repo_status(m, &bridge.working_dir))
                        .collect();
                    futures::future::join_all(futs).await
                }
                Err(e) => {
                    return Some(HandleBrennToolResult::Respond(
                        CcApprovalDecision::Continue {
                            updated_output: Some(serde_json::json!({"error": e}).to_string()),
                        },
                    ));
                }
            };

            let output = serde_json::json!({ "repos": results });

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
                    updated_output: Some(output.to_string()),
                },
            ))
        }

        // --- GitRepoPull ---
        // Migrated to the first-class tool registry; handled by
        // `registry_adapter` ahead of this dispatcher. No arm here.
        // TODO(tool-registry-migrate-git-family): the remaining git tools below
        // (CommitAndPush, Run — and ListRepos/Status elsewhere) still ride this
        // legacy intercept and should migrate to registry tools too.

        // --- GitRepoCommitAndPush PreToolUse ---
        // NOT intercepted — falls through to Permission flow for user approval.
        // (PreToolUse sends Continue → CC's permission system fires → user approval UI.)

        // --- GitRepoCommitAndPush PostToolUse ---
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL => {
            mark_tool_handled(bridge, tool_use_id).await;

            let message = match tool_input.get("message").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s,
                _ => {
                    emit_tool_summary(
                        bridge,
                        tool_name,
                        tool_input,
                        None,
                        Some(&ApprovalMatch::GlobalTool),
                        true,
                    )
                    .await;
                    return Some(HandleBrennToolResult::Respond(
                        CcApprovalDecision::Continue {
                            updated_output: Some(
                                serde_json::json!({"error": "message field is required"})
                                    .to_string(),
                            ),
                        },
                    ));
                }
            };
            let repo_slugs: Vec<String> = tool_input
                .get("repos")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let app_mounts = &bridge.mounts;
            // Each future returns (json_value, Option<pushed_slug>). The
            // pushed-slug option lets us emit sync triggers without
            // re-correlating by index.
            let futs: Vec<_> = repo_slugs
                .iter()
                .map(|slug| {
                    let slug = slug.clone();
                    let mount = app_mounts.iter().find(|m| m.slug == slug);
                    let working_dir = &bridge.working_dir;
                    let container_spawn = bridge.container_spawn.as_ref();
                    async move {
                        match mount {
                            Some(m) if m.access == brenn_lib::config::AccessLevel::ReadOnly => (
                                serde_json::json!({
                                    "slug": slug,
                                    "error": format!(
                                        "repo {:?} is mounted read-only — cannot commit",
                                        slug
                                    ),
                                }),
                                None,
                            ),
                            Some(m) => {
                                let result = crate::git_ops::repo_commit_and_push(
                                    m,
                                    working_dir,
                                    container_spawn,
                                    message,
                                )
                                .await;
                                let pushed = result.push_ok == Some(true);
                                (
                                    serde_json::to_value(&result).unwrap(),
                                    if pushed { Some(slug) } else { None },
                                )
                            }
                            None => (
                                serde_json::json!({
                                    "slug": slug,
                                    "error": format!("unknown repo slug {:?}", slug),
                                }),
                                None,
                            ),
                        }
                    }
                })
                .collect();
            let per_repo = futures::future::join_all(futs).await;
            let mut results = Vec::with_capacity(per_repo.len());
            let mut pushed_slugs: Vec<String> = Vec::new();
            for (value, pushed_slug) in per_repo {
                results.push(value);
                if let Some(s) = pushed_slug {
                    pushed_slugs.push(s);
                }
            }

            // Emit `SyncTrigger::Push` for every slug that successfully
            // pushed. Advance-detection on the reactor side picks up the
            // local HEAD movement and fans notifications out to sibling
            // clones; `acting_conversation_id = bridge.conversation_id`
            // suppresses self-notification on the invoking bridge.
            if let Some(sender) = &bridge.repo_sync_sender {
                for slug in &pushed_slugs {
                    sender.push_for_slug(slug, Some(bridge.conversation_id));
                }
            }

            let output = serde_json::json!({ "repos": results });

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
                    updated_output: Some(output.to_string()),
                },
            ))
        }

        // --- GitRepoRun PreToolUse ---
        // NOT intercepted — falls through to Permission flow for user approval.

        // --- GitRepoRun PostToolUse ---
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_GIT_REPO_RUN_TOOL => {
            mark_tool_handled(bridge, tool_use_id).await;

            let repo_slug = match tool_input.get("repo").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s,
                _ => {
                    emit_tool_summary(
                        bridge,
                        tool_name,
                        tool_input,
                        None,
                        Some(&ApprovalMatch::GlobalTool),
                        true,
                    )
                    .await;
                    return Some(HandleBrennToolResult::Respond(
                        CcApprovalDecision::Continue {
                            updated_output: Some(
                                serde_json::json!({"error": "repo field is required"}).to_string(),
                            ),
                        },
                    ));
                }
            };
            let args: Vec<String> = tool_input
                .get("args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let app_mounts = &bridge.mounts;
            let output = match app_mounts.iter().find(|m| m.slug == repo_slug) {
                Some(mount) => {
                    let result = crate::git_ops::repo_run(
                        mount,
                        &bridge.working_dir,
                        bridge.container_spawn.as_ref(),
                        &args,
                    )
                    .await;
                    serde_json::to_value(&result).unwrap()
                }
                None => serde_json::json!({
                    "error": format!("unknown repo slug {:?}", repo_slug),
                }),
            };

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
                    updated_output: Some(output.to_string()),
                },
            ))
        }

        _ => None,
    }
}

/// Resolve a repo slug ("all" or a specific slug) to resolved mounts.
fn resolve_mounts<'a>(
    mounts: &'a [brenn_lib::config::ResolvedMount],
    slug: &str,
) -> Result<Vec<&'a brenn_lib::config::ResolvedMount>, String> {
    if slug == "all" || slug.is_empty() {
        if mounts.is_empty() {
            return Err("No repos configured for this app.".to_string());
        }
        Ok(mounts.iter().collect())
    } else {
        mounts
            .iter()
            .find(|m| m.slug == slug)
            .map(|m| vec![m])
            .ok_or_else(|| {
                format!(
                    "Unknown repo slug {:?}. Available: {:?}",
                    slug,
                    mounts.iter().map(|m| m.slug.as_str()).collect::<Vec<_>>(),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use brenn_cc::session::{
        ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest,
    };
    use brenn_lib::db::init_db_memory;
    use tokio::sync::{broadcast, oneshot};

    use super::super::super::ActiveBridge;
    use super::super::super::mcp_constants::{
        MCP_GIT_LIST_REPOS_TOOL, MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL,
    };
    use super::super::super::test_support::test_bridge;
    use super::super::HandleBrennToolResult;
    use super::super::handle_brenn_tools;
    use super::resolve_mounts;
    use git_fixture::{add_bare_origin, seed_repo};
    /// Build a scratch bare remote + tracking clone with a writable
    /// origin. The tracking clone is clean and up-to-date with origin;
    /// the caller's test arranges local changes (or not) and then calls
    /// GitRepoCommitAndPush against it. Returns `(remote, clone)`.
    ///
    /// Commits require a repo-local identity wherever there is no global
    /// git config; `seed_repo` provides one.
    fn git_commit_and_push_fixture() -> (tempfile::TempDir, tempfile::TempDir) {
        let clone = tempfile::tempdir().unwrap();
        seed_repo(clone.path());
        let remote = add_bare_origin(clone.path());
        (remote, clone)
    }

    /// Tiny unique suffix so repeated username creations don't collide.
    fn rand_suffix() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Build a bridge wired to a receiver-side trigger channel, with
    /// one RW mount on the given `slug`. Returns the bridge, the
    /// trigger receiver, and the conversation id.
    async fn commit_push_bridge_with_sync(
        clone_path: std::path::PathBuf,
        slug: &str,
        access: brenn_lib::config::AccessLevel,
    ) -> (
        Arc<ActiveBridge>,
        tokio::sync::mpsc::Receiver<crate::repo_sync::SyncTrigger>,
        i64,
    ) {
        use brenn_lib::config::ResolvedMount;

        let (trigger_tx, trigger_rx) =
            tokio::sync::mpsc::channel::<crate::repo_sync::SyncTrigger>(4);
        let slug_to_remote = Arc::new(HashMap::from([(
            slug.to_string(),
            format!("ssh://example/{slug}.git"),
        )]));
        let sender = crate::repo_sync::SyncTriggerSender::new_for_test(trigger_tx, slug_to_remote);

        let db = init_db_memory();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(
                &conn,
                &format!("cp-{slug}-{}", rand_suffix()),
                "$argon2id$fake",
            );
            let cid = brenn_lib::conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(16);
        let mount = ResolvedMount {
            slug: slug.to_string(),
            host_path: clone_path,
            container_path: None,
            access,
            auto_pull: true,
            is_working_dir: matches!(access, brenn_lib::config::AccessLevel::ReadWrite),
            primary: matches!(access, brenn_lib::config::AccessLevel::ReadWrite),
        };
        let bridge = ActiveBridge::inject_for_test_with_mounts_and_sync(
            user_id,
            conv_id,
            "test",
            db,
            broadcast_tx,
            vec![mount],
            sender,
        );
        (bridge, trigger_rx, conv_id)
    }

    #[tokio::test]
    async fn git_repo_commit_and_push_post_tool_use_emits_trigger_on_push() {
        let (_remote, clone) = git_commit_and_push_fixture();
        // Dirty the tree so there's something to commit.
        std::fs::write(clone.path().join("change.txt"), "work").unwrap();

        let (bridge, mut trigger_rx, conv_id) = commit_push_bridge_with_sync(
            clone.path().to_path_buf(),
            "cp-slug",
            brenn_lib::config::AccessLevel::ReadWrite,
        )
        .await;

        let (resp_tx, _resp_rx) = tokio::sync::oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_cp".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL.into(),
                tool_input: serde_json::json!({
                    "repos": ["cp-slug"],
                    "message": "test commit",
                }),
                tool_use_id: "tu_cp".into(),
                tool_response: serde_json::Value::Null,
            },
            response_tx: resp_tx,
        };
        let result = handle_brenn_tools(&bridge, &req).await;
        assert!(
            result.is_some(),
            "handler should intercept GitRepoCommitAndPush"
        );

        // Trigger must arrive, scoped to the acting conversation.
        let trigger = tokio::time::timeout(std::time::Duration::from_secs(5), trigger_rx.recv())
            .await
            .expect("trigger should arrive within 5s")
            .expect("channel should not be closed");
        match trigger {
            crate::repo_sync::SyncTrigger::Push {
                remote,
                acting_conversation_id,
            } => {
                assert_eq!(remote, "ssh://example/cp-slug.git");
                assert_eq!(acting_conversation_id, Some(conv_id));
            }
            other => panic!("expected Push, got {other:?}"),
        }
        assert!(
            trigger_rx.try_recv().is_err(),
            "single slug commit+push should fire exactly one trigger",
        );
    }

    #[tokio::test]
    async fn git_repo_commit_and_push_post_tool_use_no_trigger_when_nothing_to_commit() {
        // No dirty tree → `git commit` reports "nothing to commit", the
        // handler records `push_ok == None`, and no trigger fires.
        let (_remote, clone) = git_commit_and_push_fixture();

        let (bridge, mut trigger_rx, _conv_id) = commit_push_bridge_with_sync(
            clone.path().to_path_buf(),
            "cp-nothing",
            brenn_lib::config::AccessLevel::ReadWrite,
        )
        .await;

        let (resp_tx, _resp_rx) = tokio::sync::oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_cp_none".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL.into(),
                tool_input: serde_json::json!({
                    "repos": ["cp-nothing"],
                    "message": "no changes",
                }),
                tool_use_id: "tu_cp_none".into(),
                tool_response: serde_json::Value::Null,
            },
            response_tx: resp_tx,
        };
        let _ = handle_brenn_tools(&bridge, &req).await;

        assert!(
            trigger_rx.try_recv().is_err(),
            "nothing-to-commit must not emit a sync trigger",
        );
    }

    #[tokio::test]
    async fn git_repo_commit_and_push_post_tool_use_no_trigger_on_ro_mount() {
        // RO mount returns an early error; `push_ok` stays None.
        let (_remote, clone) = git_commit_and_push_fixture();
        std::fs::write(clone.path().join("change.txt"), "work").unwrap();

        let (bridge, mut trigger_rx, _conv_id) = commit_push_bridge_with_sync(
            clone.path().to_path_buf(),
            "ro-slug",
            brenn_lib::config::AccessLevel::ReadOnly,
        )
        .await;

        let (resp_tx, _resp_rx) = tokio::sync::oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_cp_ro".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL.into(),
                tool_input: serde_json::json!({
                    "repos": ["ro-slug"],
                    "message": "try commit",
                }),
                tool_use_id: "tu_cp_ro".into(),
                tool_response: serde_json::Value::Null,
            },
            response_tx: resp_tx,
        };
        let _ = handle_brenn_tools(&bridge, &req).await;

        assert!(
            trigger_rx.try_recv().is_err(),
            "RO mount rejection must not emit a sync trigger",
        );
    }

    // -----------------------------------------------------------------------
    // resolve_mounts
    // -----------------------------------------------------------------------

    fn test_mounts() -> Vec<brenn_lib::config::ResolvedMount> {
        vec![
            brenn_lib::config::ResolvedMount {
                slug: "life".to_string(),
                host_path: PathBuf::from("/data/life"),
                container_path: None,
                access: brenn_lib::config::AccessLevel::ReadWrite,
                auto_pull: false,
                is_working_dir: false,
                primary: false,
            },
            brenn_lib::config::ResolvedMount {
                slug: "tech".to_string(),
                host_path: PathBuf::from("/data/tech"),
                container_path: None,
                access: brenn_lib::config::AccessLevel::ReadWrite,
                auto_pull: false,
                is_working_dir: false,
                primary: false,
            },
        ]
    }

    #[test]
    fn resolve_mounts_all() {
        let mounts = test_mounts();
        let result = resolve_mounts(&mounts, "all").unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn resolve_mounts_empty_string_means_all() {
        let mounts = test_mounts();
        let result = resolve_mounts(&mounts, "").unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn resolve_mounts_specific_slug() {
        let mounts = test_mounts();
        let result = resolve_mounts(&mounts, "life").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].slug, "life");
    }

    #[test]
    fn resolve_mounts_unknown_slug() {
        let mounts = test_mounts();
        let result = resolve_mounts(&mounts, "nonexistent");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("nonexistent"), "error: {err}");
        assert!(
            err.contains("life"),
            "error should list available slugs: {err}"
        );
    }

    #[test]
    fn resolve_mounts_empty_list() {
        let mounts: Vec<brenn_lib::config::ResolvedMount> = vec![];
        let result = resolve_mounts(&mounts, "all");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No repos configured"));
    }

    // -----------------------------------------------------------------------
    // GitListRepos interception
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn git_list_repos_pre_tool_use_allows() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_list_repos".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_GIT_LIST_REPOS_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_use_id: "t_list_repos_pre".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("PreToolUse for GitListRepos should Allow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn git_list_repos_post_tool_use_returns_empty_when_no_repos() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_list_repos_post".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_GIT_LIST_REPOS_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_list_repos_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                let arr = parsed.as_array().expect("should be an array");
                assert!(
                    arr.is_empty(),
                    "should return empty array when no repos configured"
                );
            }
            other => {
                panic!("PostToolUse for GitListRepos should Continue with output, got {other:?}")
            }
        }
    }
}

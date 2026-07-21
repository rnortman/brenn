//! The `git-repo-pull` tool — the first real registry tool (async class).
//!
//! Pulls one or more mounted clones ff-only and reports a per-repo outcome. The
//! implementation is caller-agnostic: an LLM conversation (via the adapter) and
//! a WASM consumer (via the bus executor, Slice B) both reach the same
//! `execute`, under the same grant/ACL. Dogpile prevention lives here — each
//! remote's pull serializes on the **shared** per-remote lock the repo-sync
//! manager also holds, so a tool-driven pull and a poller-driven pull of one
//! remote never overlap.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use brenn_lib::tools::AclClause;
use futures::future::join_all;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use super::descriptor::{AclDenied, Idempotency, ToolClass, ToolDescriptor, ToolError};
use super::tool::{AsyncTool, ToolCtx};
use crate::repo_sync::git::{pull_clone, pull_outcome_to_json};
use crate::repo_sync::{CloneInfo, SyncTriggerSender};

/// Typed args for `git-repo-pull`. `deny_unknown_fields` so a stray key is a
/// caller mistake surfaced as `InvalidArgs`, not silently ignored.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GitRepoPullArgs {
    /// Clone slugs to pull. Each must be admitted by the grant's `repo` ACL.
    repos: Vec<String>,
}

/// The `git-repo-pull` tool. Holds the shared clone index and per-remote locks
/// (both lifted from the repo-sync manager at bootstrap) plus the sync-trigger
/// sender for post-pull self-notification suppression.
pub struct GitRepoPullTool {
    descriptor: ToolDescriptor,
    /// Clone metadata keyed by slug — resolves a requested slug to its host
    /// path and remote URL. Shared with the repo-sync manager.
    clones: Arc<HashMap<String, CloneInfo>>,
    /// Per-remote serialization mutex, keyed by remote URL. Shared with the
    /// repo-sync manager so tool-driven and poller-driven pulls of one remote
    /// serialize with each other.
    remote_locks: Arc<HashMap<String, Arc<Mutex<()>>>>,
    /// Fires `SyncTrigger::Push` for advanced slugs so sibling clones of the
    /// same remote resync and consumers get notified. `None` when repo-sync is
    /// disabled (no trigger machinery running).
    sender: Option<SyncTriggerSender>,
}

impl GitRepoPullTool {
    /// Build the tool over the shared repo-sync handles.
    pub fn new(
        clones: Arc<HashMap<String, CloneInfo>>,
        remote_locks: Arc<HashMap<String, Arc<Mutex<()>>>>,
        sender: Option<SyncTriggerSender>,
    ) -> Self {
        Self {
            descriptor: Self::build_descriptor(),
            clones,
            remote_locks,
            sender,
        }
    }

    fn build_descriptor() -> ToolDescriptor {
        ToolDescriptor {
            name: "git-repo-pull",
            mcp_name: "mcp__brenn__GitRepoPull",
            description: "Fast-forward-only pull of one or more mounted git clones. \
                          Reports per-repo outcome (up-to-date, advanced, or a \
                          classified error). Safe: never rewrites history.",
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "repos": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Clone slugs to pull.",
                    },
                },
                "required": ["repos"],
                "additionalProperties": false,
            }),
            class: ToolClass::Async { max_concurrency: 4 },
            acl_keys: &["repo"],
            idempotency: Idempotency::Natural,
            auto_approve: true,
        }
    }

    /// Does `acl` admit a call naming clone `slug`? Empty ACL admits all (a
    /// tool with no ACL); otherwise any clause matching `{repo: slug}` admits.
    fn slug_allowed(acl: &[AclClause], slug: &str) -> bool {
        if acl.is_empty() {
            return true;
        }
        let mut attrs = BTreeMap::new();
        attrs.insert("repo".to_string(), slug.to_string());
        acl.iter().any(|c| c.matches(&attrs))
    }
}

#[async_trait]
impl AsyncTool for GitRepoPullTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied> {
        // Parse leniently: unparseable args are not an ACL question — `execute`
        // rejects them as `InvalidArgs` without ever running.
        let Ok(parsed) = serde_json::from_value::<GitRepoPullArgs>(args.clone()) else {
            return Ok(());
        };
        for slug in &parsed.repos {
            if !Self::slug_allowed(acl, slug) {
                return Err(AclDenied {
                    resource: slug.clone(),
                });
            }
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &ToolCtx,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        let parsed: GitRepoPullArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArgs(format!("git-repo-pull args: {e}")))?;

        // Per-request-position outcome slots, so the result preserves the input
        // order regardless of the concurrent grouping below.
        let mut repos: Vec<Option<serde_json::Value>> = vec![None; parsed.repos.len()];

        // Group requested slugs by remote URL. Distinct remotes pull
        // concurrently; same-remote slugs serialize under that remote's shared
        // lock. Granted-but-unmounted slugs resolve to a per-repo error here
        // and never enter a group.
        let mut by_remote: BTreeMap<String, Vec<(usize, String, PathBuf)>> = BTreeMap::new();
        for (i, slug) in parsed.repos.iter().enumerate() {
            match self.clones.get(slug) {
                Some(clone) => {
                    by_remote.entry(clone.remote.clone()).or_default().push((
                        i,
                        slug.clone(),
                        clone.host_path.clone(),
                    ));
                }
                None => {
                    warn!(slug = %slug, "git-repo-pull: no clone for slug — skipping");
                    repos[i] = Some(serde_json::json!({
                        "slug": slug,
                        "ok": false,
                        "error_type": "unknown",
                        "error": "no clone configured for this slug",
                    }));
                }
            }
        }

        // One future per remote: acquire that remote's shared lock (so a
        // tool-driven and a poller-driven pull of one remote never overlap),
        // then pull each of its slugs sequentially. A missing lock entry for a
        // known clone's remote is a bootstrap wiring bug.
        let remote_futures = by_remote.into_iter().map(|(remote, group)| {
            let lock = self
                .remote_locks
                .get(&remote)
                .unwrap_or_else(|| panic!("BUG: remote_locks has no entry for {remote:?}"));
            async move {
                let _guard = lock.lock().await;
                let mut results = Vec::with_capacity(group.len());
                for (i, slug, host_path) in group {
                    debug!(slug = %slug, remote = %remote, "git-repo-pull: pulling");
                    let outcome = pull_clone(&host_path).await;
                    results.push((i, pull_outcome_to_json(slug, outcome)));
                }
                results
            }
        });

        let mut advanced_slugs: Vec<String> = Vec::new();
        for group_results in join_all(remote_futures).await {
            for (i, (value, advanced)) in group_results {
                repos[i] = Some(value);
                if let Some(s) = advanced {
                    advanced_slugs.push(s);
                }
            }
        }

        let repos: Vec<serde_json::Value> = repos
            .into_iter()
            .map(|slot| slot.expect("every request position filled"))
            .collect();

        // Fire repo-sync triggers for advanced slugs. The acting conversation
        // (LLM path) is suppressed from its own fan-out; bus/executor calls
        // pass `None`.
        if let Some(sender) = &self.sender {
            for slug in &advanced_slugs {
                sender.push_for_slug(slug, ctx.acting_conversation_id);
            }
        }

        Ok(serde_json::json!({ "repos": repos }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use brenn_lib::messaging::ParticipantId;
    use brenn_lib::tools::{AclClause, ResolvedToolGrant};
    use std::collections::HashSet;
    use tokio::sync::mpsc;

    use super::*;
    use crate::repo_sync::SyncTrigger;
    use crate::repo_sync::test_git_fixtures::{
        scratch_remote_and_clone, scratch_remote_and_clone_behind_by_one,
    };

    const REMOTE: &str = "ssh://example/testclone.git";

    fn clone_info(host_path: PathBuf) -> CloneInfo {
        CloneInfo {
            slug: "testclone".to_string(),
            host_path,
            remote: REMOTE.to_string(),
            sync_enabled: true,
            consumer_apps: HashSet::new(),
            primary_apps: HashSet::new(),
        }
    }

    fn clones_index(host_path: PathBuf) -> Arc<HashMap<String, CloneInfo>> {
        Arc::new(HashMap::from([(
            "testclone".to_string(),
            clone_info(host_path),
        )]))
    }

    fn locks() -> Arc<HashMap<String, Arc<Mutex<()>>>> {
        Arc::new(HashMap::from([(
            REMOTE.to_string(),
            Arc::new(Mutex::new(())),
        )]))
    }

    fn clause(pairs: &[(&str, &str)]) -> AclClause {
        AclClause::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn ctx(acting: Option<i64>) -> ToolCtx {
        ToolCtx {
            caller: ParticipantId::for_wasm("git-consumer"),
            grant: ResolvedToolGrant {
                acl: vec![clause(&[("repo", "testclone")])],
                rate_limit: None,
            },
            acting_conversation_id: acting,
        }
    }

    fn sender_with_rx() -> (SyncTriggerSender, mpsc::Receiver<SyncTrigger>) {
        let (tx, rx) = mpsc::channel::<SyncTrigger>(8);
        let slug_to_remote = Arc::new(HashMap::from([(
            "testclone".to_string(),
            REMOTE.to_string(),
        )]));
        (SyncTriggerSender::new_for_test(tx, slug_to_remote), rx)
    }

    // --- ACL: OR of clauses, AND within, wildcard ---

    #[test]
    fn check_acl_ors_clauses_and_admits_wildcard() {
        let tool = GitRepoPullTool::new(clones_index(PathBuf::from("/x")), locks(), None);

        // OR of two single-repo clauses.
        let acl = vec![clause(&[("repo", "brenn")]), clause(&[("repo", "pfin")])];
        assert!(
            tool.check_acl(&serde_json::json!({"repos": ["pfin"]}), &acl)
                .is_ok()
        );
        // A slug named by neither clause is denied, naming the offending slug.
        assert_eq!(
            tool.check_acl(&serde_json::json!({"repos": ["brenn", "graf"]}), &acl),
            Err(AclDenied {
                resource: "graf".to_string()
            })
        );
        // Wildcard admits any slug.
        let star = vec![clause(&[("repo", "*")])];
        assert!(
            tool.check_acl(&serde_json::json!({"repos": ["anything"]}), &star)
                .is_ok()
        );
    }

    #[test]
    fn check_acl_and_within_clause_requires_every_key() {
        let tool = GitRepoPullTool::new(clones_index(PathBuf::from("/x")), locks(), None);
        // A clause requiring both `repo` and `branch` never matches: the call
        // only ever supplies `repo`, so the `branch` key is absent (AND within
        // a clause) and every request is denied.
        let acl = vec![clause(&[("repo", "brenn"), ("branch", "main")])];
        assert_eq!(
            tool.check_acl(&serde_json::json!({"repos": ["brenn"]}), &acl),
            Err(AclDenied {
                resource: "brenn".to_string()
            })
        );
    }

    #[test]
    fn check_acl_empty_acl_admits_all() {
        let tool = GitRepoPullTool::new(clones_index(PathBuf::from("/x")), locks(), None);
        assert!(
            tool.check_acl(&serde_json::json!({"repos": ["whatever"]}), &[])
                .is_ok()
        );
    }

    #[test]
    fn check_acl_unparseable_args_defer_to_execute() {
        let tool = GitRepoPullTool::new(clones_index(PathBuf::from("/x")), locks(), None);
        // Missing `repos` — check_acl defers (Ok); execute would reject.
        assert!(
            tool.check_acl(&serde_json::json!({}), &[clause(&[("repo", "x")])])
                .is_ok()
        );
    }

    // --- input_schema ↔ args-struct drift tripwire ---

    #[test]
    fn input_schema_matches_args_struct_fields() {
        let tool = GitRepoPullTool::new(clones_index(PathBuf::from("/x")), locks(), None);
        let schema = &tool.descriptor().input_schema;
        let props = schema["properties"].as_object().expect("properties object");
        let names: HashSet<&str> = props.keys().map(String::as_str).collect();
        assert_eq!(names, HashSet::from(["repos"]), "schema property drift");

        // Every declared property deserializes into the args struct.
        let obj = serde_json::json!({ "repos": [] });
        assert!(serde_json::from_value::<GitRepoPullArgs>(obj).is_ok());
        // deny_unknown_fields: an undeclared field is rejected.
        let extra = serde_json::json!({ "repos": [], "surprise": 1 });
        assert!(serde_json::from_value::<GitRepoPullArgs>(extra).is_err());
    }

    // --- execute: outcome JSON + advanced signal + push trigger ---

    #[tokio::test]
    async fn execute_up_to_date_reports_not_advanced_and_fires_no_trigger() {
        let (_remote, clone) = scratch_remote_and_clone();
        let (sender, mut rx) = sender_with_rx();
        let tool = GitRepoPullTool::new(
            clones_index(clone.path().to_path_buf()),
            locks(),
            Some(sender),
        );

        let out = tool
            .execute(&ctx(Some(7)), serde_json::json!({"repos": ["testclone"]}))
            .await
            .expect("execute ok");
        let repos = out["repos"].as_array().expect("repos array");
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0]["slug"], "testclone");
        assert_eq!(repos[0]["ok"], true);
        assert_eq!(repos[0]["advanced"], false);
        assert!(rx.try_recv().is_err(), "up-to-date must fire no trigger");
    }

    #[tokio::test]
    async fn execute_advanced_fires_trigger_with_acting_conversation() {
        let (_remote, clone) = scratch_remote_and_clone_behind_by_one();
        let (sender, mut rx) = sender_with_rx();
        let tool = GitRepoPullTool::new(
            clones_index(clone.path().to_path_buf()),
            locks(),
            Some(sender),
        );

        let out = tool
            .execute(&ctx(Some(42)), serde_json::json!({"repos": ["testclone"]}))
            .await
            .expect("execute ok");
        assert_eq!(out["repos"][0]["advanced"], true);
        match rx.try_recv() {
            Ok(SyncTrigger::Push {
                remote,
                acting_conversation_id,
            }) => {
                assert_eq!(remote, REMOTE);
                assert_eq!(acting_conversation_id, Some(42));
            }
            other => panic!("expected Push with acting id, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_advanced_bus_call_passes_no_acting_conversation() {
        let (_remote, clone) = scratch_remote_and_clone_behind_by_one();
        let (sender, mut rx) = sender_with_rx();
        let tool = GitRepoPullTool::new(
            clones_index(clone.path().to_path_buf()),
            locks(),
            Some(sender),
        );

        tool.execute(&ctx(None), serde_json::json!({"repos": ["testclone"]}))
            .await
            .expect("execute ok");
        match rx.try_recv() {
            Ok(SyncTrigger::Push {
                acting_conversation_id,
                ..
            }) => assert_eq!(acting_conversation_id, None),
            other => panic!("expected Push with no acting id, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_unknown_slug_yields_per_repo_error() {
        let tool = GitRepoPullTool::new(clones_index(PathBuf::from("/nonexistent")), locks(), None);
        let out = tool
            .execute(&ctx(None), serde_json::json!({"repos": ["ghost"]}))
            .await
            .expect("execute ok");
        assert_eq!(out["repos"][0]["ok"], false);
        assert_eq!(out["repos"][0]["error_type"], "unknown");
    }

    #[tokio::test]
    async fn execute_malformed_args_is_invalid_args() {
        let tool = GitRepoPullTool::new(clones_index(PathBuf::from("/x")), locks(), None);
        let err = tool
            .execute(&ctx(None), serde_json::json!({"repo": "testclone"}))
            .await
            .expect_err("must reject unknown field");
        assert!(matches!(err, ToolError::InvalidArgs(_)));
    }

    // --- per-remote serialization: execute blocks on the shared lock ---

    #[tokio::test]
    async fn execute_serializes_on_shared_remote_lock() {
        let (_remote, clone) = scratch_remote_and_clone();
        let locks = locks();
        let tool = Arc::new(GitRepoPullTool::new(
            clones_index(clone.path().to_path_buf()),
            locks.clone(),
            None,
        ));

        // Hold the remote's shared lock, then spawn an execute. It must not
        // complete while we hold the lock — proving it serializes on the same
        // mutex the poller uses.
        let guard = locks.get(REMOTE).unwrap().clone().lock_owned().await;

        let t = tool.clone();
        let handle = tokio::spawn(async move {
            t.execute(&ctx(None), serde_json::json!({"repos": ["testclone"]}))
                .await
                .expect("execute ok")
        });

        // Give the spawned task a chance to reach the lock and block on it.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!handle.is_finished(), "execute must block while lock held");

        // Release the lock; execute now completes.
        drop(guard);
        let out = handle.await.expect("join");
        assert_eq!(out["repos"][0]["ok"], true);
    }
}

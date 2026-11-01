//! Shared fixtures for the tool-registry suites here and above: the ACL
//! clause/grant constructors and the single-key `repo` ACL check that the stub
//! tools all use, plus the one-async-tool registry every fold-in test needs,
//! the user-bound stub tool, and a registry holding it bound to the served
//! user.
//! One copy so a change to the ACL convention is verified against one encoding,
//! not three slightly different ones.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use brenn_lib::tools::{AclClause, ResolvedToolGrant};
use serde_json::Value;

use async_trait::async_trait;

use super::descriptor::{
    AclDenied, Idempotency, ToolClass, ToolDescriptor, ToolError, UserBinding,
};
use super::git_repo_pull::GitRepoPullTool;
use super::registry::ToolRegistry;
use super::tool::{AsyncTool, RegisteredTool, ToolCtx};

/// A registry holding the one real async tool over an empty clone index and no
/// sync-trigger sender: the tool resolves by name at call time but has nothing
/// to pull.
///
/// One copy so the day `GitRepoPullTool::new` changes, or the set of registered
/// tools becomes the interesting thing, is one edit.
pub fn git_repo_pull_only() -> ToolRegistry {
    ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(GitRepoPullTool::new(
        Arc::new(HashMap::new()),
        Arc::new(HashMap::new()),
        None,
    )))])
}

/// An async tool with a configurable user binding and the ACL keys
/// `provider` and `path`, which refuses any clause whose `path` is `"bad"`
/// and answers the acting user it was handed.
struct BoundTool(ToolDescriptor);

#[async_trait]
impl AsyncTool for BoundTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.0
    }
    fn check_acl(&self, _args: &Value, _acl: &[AclClause]) -> Result<(), AclDenied> {
        Ok(())
    }
    fn validate_clause(&self, clause: &AclClause) -> Result<(), String> {
        match clause.iter().find(|(k, _)| *k == "path") {
            Some((_, "bad")) => Err("path \"bad\" is not a template".to_string()),
            _ => Ok(()),
        }
    }
    async fn execute(&self, ctx: &ToolCtx, _args: Value) -> Result<Value, ToolError> {
        let user = ctx.require_acting_user(self.0.name)?;
        Ok(serde_json::json!({ "id": user.id, "username": user.username }))
    }
}

/// The tool `name` bound to the acting user as `binding`; see [`BoundTool`].
pub fn bound(name: &'static str, binding: UserBinding) -> RegisteredTool {
    let mcp_name: &'static str = Box::leak(format!("mcp__brenn__{name}").into_boxed_str());
    RegisteredTool::Async(Arc::new(BoundTool(ToolDescriptor {
        name,
        mcp_name,
        description: "stub",
        input_schema: serde_json::json!({ "type": "object" }),
        class: ToolClass::Async { max_concurrency: 2 },
        acl_keys: &["provider", "path"],
        idempotency: Idempotency::Natural,
        auto_approve: true,
        user_binding: binding,
    })))
}

/// A registry holding only `served-user`, bound `ActingOrServedUser`: the
/// shape of a tool a providing component may be granted and no other
/// component may.
pub fn served_user_only() -> ToolRegistry {
    ToolRegistry::new(vec![bound("served-user", UserBinding::ActingOrServedUser)])
}

/// A single ACL clause from `(key, value)` pairs.
pub fn clause(pairs: &[(&str, &str)]) -> AclClause {
    AclClause::new(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

/// A resolved grant with the given ACL and no rate limit.
pub fn grant(acl: Vec<AclClause>) -> ResolvedToolGrant {
    ResolvedToolGrant {
        acl,
        rate_limit: None,
    }
}

/// The single-key `repo` ACL check shared by every stub tool: read `args.repo`
/// and admit iff the ACL is empty or some clause matches. Denies naming the
/// offending repo.
pub fn repo_acl_check(args: &Value, acl: &[AclClause]) -> Result<(), AclDenied> {
    let repo = args.get("repo").and_then(Value::as_str).unwrap_or("");
    let attrs: BTreeMap<String, String> = BTreeMap::from([("repo".to_string(), repo.to_string())]);
    if acl.is_empty() || acl.iter().any(|c| c.matches(&attrs)) {
        Ok(())
    } else {
        Err(AclDenied {
            resource: repo.to_string(),
        })
    }
}

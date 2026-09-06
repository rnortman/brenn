//! Shared fixtures for the tool-registry suites here and above: the ACL
//! clause/grant constructors and the single-key `repo` ACL check that the stub
//! tools all use, plus the one-async-tool registry every fold-in test needs.
//! One copy so a change to the ACL convention is verified against one encoding,
//! not three slightly different ones.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use brenn_lib::tools::{AclClause, ResolvedToolGrant};
use serde_json::Value;

use super::descriptor::AclDenied;
use super::git_repo_pull::GitRepoPullTool;
use super::registry::ToolRegistry;
use super::tool::RegisteredTool;

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

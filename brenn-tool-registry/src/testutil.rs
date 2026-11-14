//! Shared fixtures for the tool-registry suites here and above: the ACL
//! clause/grant constructors and the single-key `repo` ACL check that the stub
//! tools all use, plus the one-async-tool registry every fold-in test needs
//! the user-bound stub tool, the attachment-echoing tool, and `execute` with
//! no attachments for suites that test a tool's value.
//! One copy so a change to the ACL convention is verified against one encoding,
//! not three slightly different ones.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use brenn_lib::tools::{AclClause, ResolvedToolGrant};
use brenn_messaging_store::blob::HeldAttachments;
use serde_json::Value;

use async_trait::async_trait;

use super::descriptor::{
    AclDenied, Idempotency, ToolClass, ToolDescriptor, ToolError, UserBinding,
};
use super::git_repo_pull::GitRepoPullTool;
use super::registry::ToolRegistry;
use super::tool::{AsyncTool, RegisteredTool, ToolCtx, ToolOutput};

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
    async fn execute(
        &self,
        ctx: &ToolCtx,
        _args: Value,
        _attachments: HeldAttachments,
    ) -> Result<ToolOutput, ToolError> {
        let user = ctx.require_acting_user(self.0.name)?;
        Ok(serde_json::json!({ "id": user.id, "username": user.username }).into())
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
        accepts_attachments: false,
    })))
}

/// The canonical name of [`AttachmentEchoTool`].
pub const ATTACHMENT_ECHO: &str = "attachment-echo";

/// The MCP name of [`AttachmentEchoTool`].
pub const ATTACHMENT_ECHO_MCP: &str = "mcp__brenn__AttachmentEcho";

/// A host-implemented async tool that accepts attachments, answers
/// `{"args": <its arguments>, "received": {<name>: {"size", "content_type"}}}`
/// for the arguments and attachments it was handed, and returns the
/// attachments under the same names.
pub struct AttachmentEchoTool(ToolDescriptor);

impl AttachmentEchoTool {
    pub fn new() -> Self {
        AttachmentEchoTool(ToolDescriptor {
            name: ATTACHMENT_ECHO,
            mcp_name: ATTACHMENT_ECHO_MCP,
            description: "Echo the attachments handed to it.",
            input_schema: serde_json::json!({ "type": "object" }),
            class: ToolClass::Async { max_concurrency: 4 },
            acl_keys: &[],
            idempotency: Idempotency::Natural,
            auto_approve: true,
            user_binding: UserBinding::None,
            accepts_attachments: true,
        })
    }
}

impl Default for AttachmentEchoTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncTool for AttachmentEchoTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.0
    }
    fn check_acl(&self, _args: &Value, _acl: &[AclClause]) -> Result<(), AclDenied> {
        Ok(())
    }
    async fn execute(
        &self,
        _ctx: &ToolCtx,
        args: Value,
        attachments: HeldAttachments,
    ) -> Result<ToolOutput, ToolError> {
        let received: serde_json::Map<String, Value> = attachments
            .refs()
            .iter()
            .map(|(name, reference)| {
                (
                    name.as_str().to_string(),
                    serde_json::json!({
                        "size": reference.size,
                        "content_type": reference.content_type,
                    }),
                )
            })
            .collect();
        Ok(ToolOutput {
            value: serde_json::json!({ "args": args, "received": received }),
            attachments,
        })
    }
}

/// `execute` with no attachments, answering the value alone, for suites that
/// test what a tool answers. Panics if the tool answered with attachments.
#[async_trait]
pub trait ExecuteValue {
    async fn execute_value(&self, ctx: &ToolCtx, args: Value) -> Result<Value, ToolError>;
}

#[async_trait]
impl<T: AsyncTool + ?Sized> ExecuteValue for T {
    async fn execute_value(&self, ctx: &ToolCtx, args: Value) -> Result<Value, ToolError> {
        self.execute(ctx, args, HeldAttachments::none())
            .await
            .map(|output| {
                assert!(
                    output.attachments.is_empty(),
                    "{} answered with attachments",
                    self.descriptor().name
                );
                output.value
            })
    }
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

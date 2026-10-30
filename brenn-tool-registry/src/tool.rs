//! Tool trait objects and the per-call context.
//!
//! Two traits, not one trait with a class field: the type system then prevents
//! a fast tool from having an `async fn execute` at all. A `RegisteredTool`
//! erases the distinction for storage while `descriptor().class` recovers it
//! for dispatch.

use std::sync::Arc;

use async_trait::async_trait;
use brenn_lib::messaging::ParticipantId;
use brenn_lib::tools::{AclClause, ResolvedToolGrant};

use super::descriptor::{AclDenied, ToolDescriptor, ToolError};

/// The human on whose behalf a tool call runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActingUser {
    /// The user's database id.
    pub id: i64,
    /// The user's username (unique, never renamed).
    pub username: String,
}

/// Per-invocation context handed to a tool's `execute`. Carries the caller
/// principal, the resolved grant admitting the call, the acting conversation id
/// (conversation callers only; feeds `SyncTrigger::Push` self-notification
/// suppression), the acting user, and the call's deadline when it has one.
pub struct ToolCtx {
    /// The principal invoking the tool (`wasm:<slug>`, `app:<slug>@<server>`,
    /// or the executor for bus-originated calls).
    pub caller: ParticipantId,
    /// The grant admitting this call (ACL clauses + optional rate limit).
    pub grant: ResolvedToolGrant,
    /// Set only for conversation-originated calls; `None` for bus/executor
    /// calls, which pass no self-notification suppression.
    pub acting_conversation_id: Option<i64>,
    /// The human on whose behalf this call runs: the conversation's owner for
    /// a conversation-originated call; for a provider's nested call, the user
    /// of the pending call it serves; `None` for any other component or bus
    /// caller.
    pub acting_user: Option<ActingUser>,
    /// The instant by which a provided tool's call must be answered, set by
    /// the call site that admitted it and covering admission as well as
    /// execution. `None` for a native tool, which ignores it.
    pub deadline: Option<tokio::time::Instant>,
}

impl ToolCtx {
    /// The acting user of a call to a tool bound to one.
    ///
    /// # Panics
    ///
    /// Panics naming `tool` when `acting_user` is `None`: grant validation
    /// refuses an `ActingUser` tool to every caller kind that has no acting
    /// user, so reaching here without one is a bypassed validation.
    pub fn require_acting_user(&self, tool: &str) -> &ActingUser {
        self.acting_user.as_ref().unwrap_or_else(|| {
            panic!(
                "BUG: tool {tool:?} requires an acting user but caller {} has none \
                 (grant validation bypassed)",
                self.caller.as_str()
            )
        })
    }
}

/// A synchronous, effectively non-blocking tool. `execute` must stay within the
/// declared fast budget; blowing it is a tool bug, not a caller fault.
pub trait FastTool: Send + Sync {
    /// Static metadata for this tool.
    fn descriptor(&self) -> &ToolDescriptor;

    /// Reject a call whose args name a resource outside `acl` (the grant's
    /// OR'd clauses). Runs before `execute`.
    fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied>;

    /// Refuse an ACL clause this tool cannot interpret, naming the reason.
    /// Called for every clause of every grant at boot and reload, after the
    /// clause's keys are confirmed to be ones the tool declares.
    fn validate_clause(&self, _clause: &AclClause) -> Result<(), String> {
        Ok(())
    }

    /// Run the tool. Sync, bounded compute only.
    fn execute(
        &self,
        ctx: &ToolCtx,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError>;
}

/// A message-shaped tool. `execute` may await network/subprocess work; the
/// per-tool concurrency semaphore bounds how many run at once.
#[async_trait]
pub trait AsyncTool: Send + Sync {
    /// Static metadata for this tool.
    fn descriptor(&self) -> &ToolDescriptor;

    /// Reject a call whose args name a resource outside `acl`. Runs before
    /// `execute` (and again at executor dequeue, belt-and-suspenders).
    fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied>;

    /// Refuse an ACL clause this tool cannot interpret, naming the reason.
    /// Called for every clause of every grant at boot and reload, after the
    /// clause's keys are confirmed to be ones the tool declares.
    fn validate_clause(&self, _clause: &AclClause) -> Result<(), String> {
        Ok(())
    }

    /// Run the tool to completion.
    async fn execute(
        &self,
        ctx: &ToolCtx,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError>;
}

/// A registered tool, class-erased for uniform storage. `descriptor()` recovers
/// the class for dispatch.
#[derive(Clone)]
pub enum RegisteredTool {
    Fast(Arc<dyn FastTool>),
    Async(Arc<dyn AsyncTool>),
}

impl RegisteredTool {
    /// The tool's static metadata, regardless of class.
    pub fn descriptor(&self) -> &ToolDescriptor {
        match self {
            RegisteredTool::Fast(t) => t.descriptor(),
            RegisteredTool::Async(t) => t.descriptor(),
        }
    }

    /// The tool's per-clause grant validation, regardless of class.
    pub fn validate_clause(&self, clause: &AclClause) -> Result<(), String> {
        match self {
            RegisteredTool::Fast(t) => t.validate_clause(clause),
            RegisteredTool::Async(t) => t.validate_clause(clause),
        }
    }
}

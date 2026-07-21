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

/// Per-invocation context handed to a tool's `execute`. Carries the caller
/// principal, the resolved grant admitting the call, and — for conversation
/// callers only — the acting conversation id (feeds `SyncTrigger::Push`
/// self-notification suppression).
pub struct ToolCtx {
    /// The principal invoking the tool (`wasm:<slug>`, `app:<slug>@<server>`,
    /// or the executor for bus-originated calls).
    pub caller: ParticipantId,
    /// The grant admitting this call (ACL clauses + optional rate limit).
    pub grant: ResolvedToolGrant,
    /// Set only for conversation-originated calls; `None` for bus/executor
    /// calls, which pass no self-notification suppression.
    pub acting_conversation_id: Option<i64>,
}

/// A synchronous, effectively non-blocking tool. `execute` must stay within the
/// declared fast budget; blowing it is a tool bug, not a caller fault.
pub trait FastTool: Send + Sync {
    /// Static metadata for this tool.
    fn descriptor(&self) -> &ToolDescriptor;

    /// Reject a call whose args name a resource outside `acl` (the grant's
    /// OR'd clauses). Runs before `execute`.
    fn check_acl(&self, args: &serde_json::Value, acl: &[AclClause]) -> Result<(), AclDenied>;

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
}

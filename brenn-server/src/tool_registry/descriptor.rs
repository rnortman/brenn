//! Tool descriptor vocabulary: the static, per-tool metadata a `FastTool` or
//! `AsyncTool` publishes, plus the shared caps and error type both projections
//! (LLM adapter and WASM host) map onto.
//!
//! A descriptor is code, not config: the operator grants a tool by name in
//! `[[*.tool_grant]]`, and the descriptor here is what the registry validates
//! those grants against (canonical name, MCP name, ACL keys). The class field
//! is the mechanical backbone of the fast/async split — a tool declares its
//! class, the caller never chooses.

use std::time::Duration;

/// Default fast-tool budget when a tool does not override it: 10ms.
pub const DEFAULT_FAST_BUDGET: Duration = Duration::from_millis(10);

/// Hard cap on a fast tool's declared budget: 50ms. Registration panics above
/// this — a tool wanting more compute is not fast, no exceptions.
pub const MAX_FAST_BUDGET: Duration = Duration::from_millis(50);

/// Per-activation cap on fast-tool calls from a single guest activation.
/// Enforced by the WASM host (Slice B); declared here as the shared constant.
pub const MAX_FAST_CALLS_PER_ACTIVATION: usize = 64;

/// Maximum `args-json` size accepted for any tool call (both classes).
pub const MAX_ARGS_BYTES: usize = 64 * 1024;

/// Maximum serialized result size for a fast-class call.
pub const MAX_FAST_RESULT_BYTES: usize = 256 * 1024;

/// Maximum serialized result size for an async-class result activation.
pub const MAX_ASYNC_RESULT_BYTES: usize = 512 * 1024;

// The guest-facing caps live in `brenn-wasm` (which cannot depend on this crate)
// and are enforced there; these copies exist so brenn-server code can reason
// about the same numbers. brenn-server *does* see both crates, so pin the two
// copies together — a silent divergence (raising one, shipping a no-op) fails the
// build instead.
const _: () = assert!(MAX_ARGS_BYTES == brenn_wasm::PROCESSOR_MAX_TOOL_ARGS_BYTES);
const _: () = assert!(MAX_FAST_RESULT_BYTES == brenn_wasm::PROCESSOR_MAX_FAST_TOOL_RESULT_BYTES);
const _: () = assert!(
    MAX_FAST_CALLS_PER_ACTIVATION == brenn_wasm::PROCESSOR_MAX_FAST_TOOL_CALLS_PER_ACTIVATION
);

/// A tool's execution class. The tool declares it; the caller never chooses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolClass {
    /// Synchronous, effectively non-blocking: bounded compute, in-memory
    /// lookups, fast local reads. `budget` bounds a single call; overrun is a
    /// tool bug that alerts, not a guest fault.
    Fast { budget: Duration },
    /// Message-shaped: the request is a bus message and the result arrives as a
    /// later activation. `max_concurrency` is a global bound on concurrent
    /// executions across every caller path.
    Async { max_concurrency: usize },
}

/// Idempotency contract of a tool's side effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Idempotency {
    /// Repeat execution is harmless (e.g. an ff-only pull). No dedupe needed.
    Natural,
    /// The caller must supply `idempotency_key`; the executor dedupes on
    /// `(tool, caller, key)`. The dedupe table is not built in this cycle —
    /// registering such a tool panics.
    RequiresKey,
}

/// Static per-tool metadata. Canonical `name` is the registry key and the grant
/// name; `mcp_name` is the explicit (never derived) MCP projection.
pub struct ToolDescriptor {
    /// Canonical, kebab-case tool name (e.g. `"git-repo-pull"`). Registry key
    /// and grant name.
    pub name: &'static str,
    /// Explicit MCP tool name (e.g. `"mcp__brenn__GitRepoPull"`). No derivation.
    pub mcp_name: &'static str,
    /// Human-facing description; feeds the MCP projection and docs.
    pub description: &'static str,
    /// JSON-schema projection of the tool's args struct — for MCP/docs, not an
    /// enforcement layer (tools validate by deserializing).
    pub input_schema: serde_json::Value,
    /// Execution class (fast vs async).
    pub class: ToolClass,
    /// ACL attribute keys this tool understands (e.g. `["repo"]`). Empty means
    /// the tool takes no ACL — a grant alone authorizes every call.
    pub acl_keys: &'static [&'static str],
    /// Idempotency contract of the tool's side effect.
    pub idempotency: Idempotency,
    /// LLM path: skip user approval when the grant permits the call.
    pub auto_approve: bool,
}

/// ACL denial from a tool's `check_acl`: the call named a resource outside the
/// grant. `resource` is the offending value (safe to echo — the caller already
/// named it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclDenied {
    pub resource: String,
}

/// Error outcome of a tool invocation. Variants mirror the WIT `tool-error`
/// (Slice B) so both projections map onto one type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolError {
    /// Unknown tool OR ungranted tool — deliberately indistinguishable.
    NotGranted,
    /// ACL clause miss: the args named a resource outside the grant.
    Denied(String),
    /// Malformed args (deserialize failure or over-cap payload).
    InvalidArgs(String),
    /// Rate limit exhausted (fast class returns this immediately).
    RateLimited,
    /// `call-fast` on an async tool or vice versa.
    WrongClass,
    /// Tool bug; the host has already alerted.
    Internal(String),
}

impl ToolError {
    /// Stable kind token for this error. Doubly load-bearing: the result
    /// envelope's `err.kind` (wire-visible to guests) and the invocation log's
    /// `outcome` field. One definition so the log vocabulary cannot fork from the
    /// wire vocabulary.
    pub fn kind_str(&self) -> &'static str {
        match self {
            ToolError::NotGranted => "not_granted",
            ToolError::Denied(_) => "denied",
            ToolError::InvalidArgs(_) => "invalid_args",
            ToolError::RateLimited => "rate_limited",
            ToolError::WrongClass => "wrong_class",
            ToolError::Internal(_) => "internal",
        }
    }

    /// Human detail for the result envelope's `err.detail` (empty for variants
    /// that carry no payload).
    pub fn detail(&self) -> String {
        match self {
            ToolError::Denied(r) => r.clone(),
            ToolError::InvalidArgs(d) | ToolError::Internal(d) => d.clone(),
            ToolError::NotGranted | ToolError::RateLimited | ToolError::WrongClass => String::new(),
        }
    }
}

/// The `outcome` label for an invocation log line: `"ok"` on success, else the
/// error's `kind_str`.
pub fn outcome_label(result: &Result<serde_json::Value, ToolError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(e) => e.kind_str(),
    }
}

impl From<AclDenied> for ToolError {
    fn from(d: AclDenied) -> Self {
        ToolError::Denied(d.resource)
    }
}

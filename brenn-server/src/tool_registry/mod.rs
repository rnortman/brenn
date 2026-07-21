//! First-class tool registry (Slice A core).
//!
//! A tool is a registry object with a native implementation, invocable by any
//! granted participant — LLM conversation or WASM component — under one grant
//! vocabulary. This module owns the substrate: the descriptor/trait vocabulary
//! (`descriptor`, `tool`), the built-once table plus config validation
//! (`registry`), and per-`(participant, tool)` throttling (`rate_limit`).
//!
//! Config parsing/resolution and the ACL-matching primitive live in
//! `brenn_lib::tools`; execution lives here because tools need brenn-server
//! handles (`repo_sync`, DB). Concrete tool impls and the LLM/WASM caller paths
//! land in later increments.

pub mod bus_wiring;
pub mod descriptor;
pub mod executor;
pub mod git_repo_pull;
pub mod rate_limit;
pub mod registry;
pub mod tool;
pub mod wasm_host;

#[cfg(test)]
pub mod testutil;

pub use descriptor::{
    AclDenied, DEFAULT_FAST_BUDGET, Idempotency, MAX_ARGS_BYTES, MAX_ASYNC_RESULT_BYTES,
    MAX_FAST_BUDGET, MAX_FAST_CALLS_PER_ACTIVATION, MAX_FAST_RESULT_BYTES, ToolClass,
    ToolDescriptor, ToolError,
};
pub use executor::{TOOL_EXECUTOR_COMPONENT, ToolCallerGrants, ToolExecutor};
pub use git_repo_pull::GitRepoPullTool;
pub use rate_limit::RateLimiter;
pub use registry::ToolRegistry;
pub use tool::{AsyncTool, FastTool, RegisteredTool, ToolCtx};
pub use wasm_host::WasmToolHost;

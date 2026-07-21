//! Active CC session bridge, shared across WebSocket connections.
//!
//! `ActiveBridge` wraps a live CC subprocess and broadcasts events to all
//! attached WS connections. `ActiveBridges` is the global registry.
//!
//! The CC event loop runs as a detached tokio task, independent of any WS
//! connection. The bridge stays alive until CC exits, regardless of how many
//! tabs are connected.

mod approval_dispatch;
mod brenn_tools;
mod bridge;
mod bridge_io;
mod cc_event_loop;
mod cc_spawn_config;
mod compaction;
mod idle_hooks;
mod lifecycle;
mod mcp_constants;
mod permission_sync;
mod registry;
#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
pub mod test_support;
mod tool_card;
mod tool_summary;
mod watchdog;

pub(in crate::active_bridge) use brenn_tools::handle_brenn_tools;
pub(crate) use brenn_tools::render_pending_tool_request;
pub use bridge::ActiveBridge;
// SpawnContext is not needed in tests because spawn_new is never called directly in tests
// (inject_for_test is used instead). The cfg(not(test)) gate keeps test builds clean.
#[cfg(not(test))]
pub use bridge::SpawnContext;
pub(crate) use cc_spawn_config::write_virtual_tools_file;
pub(in crate::active_bridge) use compaction::CompactionPhase;
pub use registry::ActiveBridges;
pub(in crate::active_bridge) use tool_summary::{
    PendingToolUse, emit_prerendered_summary, emit_tool_result_summaries,
};
pub(crate) use tool_summary::{emit_tool_summary_for_intercept, mark_tool_handled};
pub(crate) use watchdog::spawn_watchdog;

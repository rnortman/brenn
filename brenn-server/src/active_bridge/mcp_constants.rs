//! MCP tool-name constants for the brenn MCP server.

/// The MCP tool name for displaying files in the artifact viewer.
/// CC sees this as `mcp__brenn__DisplayFile` (MCP server name prefix + tool name).
pub(super) const MCP_DISPLAY_FILE_TOOL: &str = "mcp__brenn__DisplayFile";

/// The MCP tool name for propose reconciliation (brenn's noop MCP server).
/// Re-exported from brenn-pfin for use in hook interception.
pub(super) const MCP_PROPOSE_RECONCILIATION_TOOL: &str =
    brenn_pfin::MCP_PROPOSE_RECONCILIATION_TOOL;

/// The MCP tool name for batch reconciliation (brenn's noop MCP server).
pub(super) const MCP_BATCH_RECONCILE_TOOL: &str = brenn_pfin::MCP_BATCH_RECONCILE_TOOL;

/// The MCP tool name for batch assignment (brenn's noop MCP server).
pub(super) const MCP_BATCH_ASSIGN_TOOL: &str = brenn_pfin::MCP_BATCH_ASSIGN_TOOL;

/// The MCP tool name for pfin's reconcile tool (used for per-tool display
/// enrichment in the approval path).
pub(super) const MCP_RECONCILE_TOOL: &str = brenn_pfin::MCP_RECONCILE_TOOL;

/// The MCP tool name for LLM-initiated compaction (brenn's noop MCP server).
pub(super) const MCP_REQUEST_COMPACTION_TOOL: &str = "mcp__brenn__RequestCompaction";

/// MCP tool names for device slug management (brenn's noop MCP server).
pub(super) const MCP_DEVICE_LIST_TOOL: &str = "mcp__brenn__DeviceList";
pub(super) const MCP_DEVICE_GET_TOOL: &str = "mcp__brenn__DeviceGet";
pub(super) const MCP_DEVICE_ASSIGN_SLUG_TOOL: &str = "mcp__brenn__DeviceAssignSlug";

/// MCP tool name for the timezone override tool.
pub(super) const MCP_SET_USER_TIMEZONE_TOOL: &str = "mcp__brenn__SetUserTimezone";

/// MCP tool name for the usage observability export tool.
/// Re-exported from `brenn_lib::mcp_tool_names` so both `export_usage.rs`
/// (spawner side) and `approval_formatter.rs` (formatter side) share a single
/// definition without `pub(crate)` visibility escalation of this file's constants.
pub(crate) use brenn_lib::mcp_tool_names::MCP_EXPORT_USAGE_TOOL;

/// MCP tool names for git repo management (brenn's noop MCP server).
pub(super) const MCP_GIT_LIST_REPOS_TOOL: &str = "mcp__brenn__GitListRepos";
pub(super) const MCP_GIT_REPO_STATUS_TOOL: &str = "mcp__brenn__GitRepoStatus";
// GitRepoPull is now a first-class registry tool (see `tool_registry`); its MCP
// name lives on the tool descriptor, not here.

/// CommitAndPush and Run constants are defined in `tools::git_repo` (shared
/// with AppTool::name()) and re-exported here to keep the single source of truth.
pub(super) use crate::tools::git_repo::{MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL, MCP_GIT_REPO_RUN_TOOL};

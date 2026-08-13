//! Canonical MCP tool-name constants shared across crates.
//!
//! The spawner, the formatters, the approval rules and the subsystems that
//! implement the tools all reference these strings. Defining them here prevents
//! divergence and avoids visibility escalation in crate-internal constant files
//! — a crate that only needs a tool's *name* takes an edge to this leaf module
//! rather than to the subsystem that implements it.

/// MCP tool name for the usage observability export tool.
pub const MCP_EXPORT_USAGE_TOOL: &str = "mcp__brenn__ExportUsage";

/// `mcp__brenn__AutoCreate` — create an automation job.
pub const MCP_AUTO_CREATE_TOOL: &str = "mcp__brenn__AutoCreate";
/// `mcp__brenn__AutoList` — list automation jobs owned by the caller's app.
pub const MCP_AUTO_LIST_TOOL: &str = "mcp__brenn__AutoList";
/// `mcp__brenn__AutoEdit` — edit an automation job by id.
pub const MCP_AUTO_EDIT_TOOL: &str = "mcp__brenn__AutoEdit";
/// `mcp__brenn__AutoDelete` — delete an automation job by id.
pub const MCP_AUTO_DELETE_TOOL: &str = "mcp__brenn__AutoDelete";

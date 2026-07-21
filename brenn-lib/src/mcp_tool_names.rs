//! Canonical MCP tool-name constants shared across crates.
//!
//! Both `brenn` (spawner/formatter) and `brenn-lib` (approval rules, etc.)
//! reference these strings. Defining them here prevents divergence and avoids
//! visibility escalation in crate-internal constant files.

/// MCP tool name for the usage observability export tool.
pub const MCP_EXPORT_USAGE_TOOL: &str = "mcp__brenn__ExportUsage";

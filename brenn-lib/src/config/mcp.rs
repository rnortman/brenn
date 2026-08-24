use std::collections::HashMap;

/// MCP server configuration for a custom MCP server.
#[derive(Debug, Clone, PartialEq)]
pub struct McpServerConfig {
    pub command: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

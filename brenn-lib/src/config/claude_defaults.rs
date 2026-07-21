use std::path::PathBuf;

use serde::Deserialize;

/// Server-global CC defaults, shared across all apps.
/// Per-app configs can override `model`.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClaudeDefaultsConfig {
    /// Path to the Brenn DisplayFile MCP stub script (noop_mcp.py).
    pub mcp_script_path: PathBuf,
    /// Default CC model. Per-app configs can override this.
    pub model: String,
}

impl Default for ClaudeDefaultsConfig {
    fn default() -> Self {
        Self {
            mcp_script_path: PathBuf::from("/opt/brenn/noop_mcp.py"),
            model: "sonnet".to_string(),
        }
    }
}

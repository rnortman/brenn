use std::path::PathBuf;

/// Server-global CC defaults, shared across all apps.
/// Per-app configs can override `model`.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaudeDefaultsConfig {
    /// Path to the Brenn DisplayFile MCP stub script (noop_mcp.py).
    pub mcp_script_path: PathBuf,
    /// Host path of the `brenn-tool` binary, bind-mounted into the agent's
    /// container. Read only for containerized apps; a bare app's Claude Code
    /// finds `brenn-tool` on Brenn's own `PATH`.
    pub tool_cli_path: PathBuf,
    /// Default CC model. Per-app configs can override this.
    pub model: String,
    /// Where `claude_profile` blocks without a `token_file` look for their
    /// token: `<dir>/claude-profile-<name>.token`. `None` means every profile
    /// must state its own path.
    pub profile_token_dir: Option<PathBuf>,
}

impl Default for ClaudeDefaultsConfig {
    fn default() -> Self {
        Self {
            mcp_script_path: PathBuf::from("/opt/brenn/noop_mcp.py"),
            tool_cli_path: PathBuf::from("/opt/brenn/brenn-tool"),
            model: "sonnet".to_string(),
            profile_token_dir: None,
        }
    }
}

use std::collections::HashMap;

use serde::Deserialize;

/// Raw attachment target config as deserialized from TOML `[[app.attachment_target]]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentTargetRaw {
    /// URL-safe slug identifier (e.g. "import").
    pub name: String,
    /// Human-readable label for UI (e.g. "Import bank export").
    pub label: String,
    /// Accepted file extensions (e.g. [".ofx", ".csv"]).
    pub accept: Vec<String>,
    /// Allow multiple files in one upload.
    #[serde(default)]
    pub multi: bool,
    /// Handler configuration.
    pub handler: AttachmentHandlerConfig,
}

/// Handler configuration for an attachment target.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, tag = "type")]
pub enum AttachmentHandlerConfig {
    /// Run a shell command with file-role substitution.
    #[serde(rename = "command")]
    Command {
        /// Program to execute.
        program: String,
        /// Argument template with `{role}` placeholders.
        args: Vec<String>,
        /// Maps role names to file extensions (e.g. { ofx = [".ofx", ".qfx"] }).
        file_roles: HashMap<String, Vec<String>>,
        /// Subprocess timeout in seconds. Defaults to 60.
        #[serde(default = "default_timeout_secs")]
        timeout_secs: u64,
        /// Optional static instructions prepended to the CC context message.
        #[serde(default)]
        cc_instructions: Option<String>,
    },
}

fn default_timeout_secs() -> u64 {
    60
}

/// Resolved attachment target with validation applied.
#[derive(Debug, Clone)]
pub struct AttachmentTarget {
    pub name: String,
    pub label: String,
    pub accept: Vec<String>,
    pub multi: bool,
    pub handler: AttachmentHandlerConfig,
}

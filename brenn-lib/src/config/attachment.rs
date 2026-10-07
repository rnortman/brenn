use std::collections::HashMap;

/// Raw attachment target config, lowered from an agent's `attachment_target` block.
#[derive(Debug, Clone, PartialEq)]
pub struct AttachmentTargetRaw {
    /// URL-safe slug identifier (e.g. "import").
    pub name: String,
    /// Human-readable label for UI (e.g. "Import bank export").
    pub label: String,
    /// Accepted file extensions (e.g. [".ofx", ".csv"]).
    pub accept: Vec<String>,
    /// Allow multiple files in one upload.
    pub multi: bool,
    /// Handler configuration.
    pub handler: AttachmentHandlerConfig,
}

/// Handler configuration for an attachment target.
#[derive(Debug, Clone, PartialEq)]
pub enum AttachmentHandlerConfig {
    /// Run a shell command with file-role substitution.
    Command {
        /// Program to execute.
        program: String,
        /// Argument template with `{role}` placeholders.
        args: Vec<String>,
        /// Maps role names to file extensions (e.g. { ofx = [".ofx", ".qfx"] }).
        file_roles: HashMap<String, Vec<String>>,
        /// Subprocess timeout in seconds. Defaults to 60.
        timeout_secs: u64,
        /// Optional static instructions prepended to the CC context message.
        cc_instructions: Option<String>,
    },
}

impl AttachmentHandlerConfig {
    /// The type word for [`Self::Command`].
    pub const COMMAND: &'static str = "command";

    /// The type words a `handler` block may name, one per variant, in the
    /// order a refusal lists them.
    ///
    /// What holds this list to the enum is discoverability, not proof: a new
    /// variant must be given a word in [`Self::tag`], just below, and the
    /// compile error lands beside this array. Nothing forces that word into the
    /// array. The lowering test that walks this list checks that every word
    /// here lowers to the variant that names it; it cannot see a variant whose
    /// word is missing.
    pub const TAGS: [&'static str; 1] = [Self::COMMAND];

    /// The type word that names this variant.
    ///
    /// No production caller: the exhaustive match makes a new variant a
    /// compile error beside [`Self::TAGS`], and the lowering tests read it.
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Command { .. } => Self::COMMAND,
        }
    }
}

pub(crate) fn default_timeout_secs() -> u64 {
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

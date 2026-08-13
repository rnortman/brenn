//! Approval rules: pattern-based auto-approval for tool use requests.
//!
//! Rules match on tool name (exact) and a regex pattern applied to a
//! tool-specific extract of the input (e.g., the `command` field for Bash).
//! Three layers are checked in order: static (TOML config), permanent (DB,
//! app-wide), and conversation-scoped (DB, per-conversation).

use regex::Regex;
use serde::Deserialize;

/// Result of checking a tool invocation against the approval rule set.
/// Tells the caller *why* a tool was auto-approved (or that it wasn't).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalMatch {
    /// Hardcoded read-only tool (Read, Glob, Grep, ToolSearch, app auto-approve).
    GlobalTool,
    /// Matched a rule from the TOML config file.
    ConfigRule { pattern: String },
    /// Matched an "Always Allow" rule from the DB (user-created).
    AlwaysAllowRule { pattern: String },
    /// No rule matched — requires manual approval.
    NoMatch,
}

impl ApprovalMatch {
    /// Whether this match means the tool should be auto-approved.
    pub fn is_approved(&self) -> bool {
        !matches!(self, ApprovalMatch::NoMatch)
    }

    /// Human-readable description of the approval reason.
    pub fn description(&self) -> &str {
        match self {
            ApprovalMatch::GlobalTool => "global read-only tool",
            ApprovalMatch::ConfigRule { .. } => "config rule",
            ApprovalMatch::AlwaysAllowRule { .. } => "always-allow rule",
            ApprovalMatch::NoMatch => "no match",
        }
    }
}

/// Maximum allowed pattern length (bytes). Patterns from the browser are
/// untrusted input — reject anything longer and log for fail2ban.
const MAX_PATTERN_LEN: usize = 512;

/// Compiled regex size limit (bytes). Caps the DFA to prevent pathological
/// patterns from consuming excessive memory.
const REGEX_SIZE_LIMIT: usize = 10_000;

/// Tools with subcommands — when generating default patterns for Bash,
/// extract both program and subcommand (e.g., `git status\b.*`).
const MULTI_SUBCOMMAND_TOOLS: &[&str] = &[
    "git",
    "cargo",
    "npm",
    "npx",
    "gh",
    "docker",
    "podman",
    "make",
    "kubectl",
    "systemctl",
    "journalctl",
    "apt",
    "dnf",
    "pip",
    "uv",
];

/// Hardcoded read-only tools that are always auto-approved.
/// These don't need pattern matching — any invocation is safe.
const GLOBAL_AUTO_APPROVE_TOOLS: &[&str] = &["Read", "Glob", "Grep", "ToolSearch"];

// ---------------------------------------------------------------------------
// ApprovalRuleConfig — static rules from TOML
// ---------------------------------------------------------------------------

/// A single approval rule as defined in the TOML config.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRuleConfig {
    pub tool: String,
    pub pattern: String,
}

// ---------------------------------------------------------------------------
// CompiledRule — in-memory, ready to match
// ---------------------------------------------------------------------------

/// A compiled approval rule, ready for matching.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub tool_name: String,
    pub pattern_src: String,
    regex: Regex,
}

impl CompiledRule {
    /// Compile a rule from raw components. Returns an error string if the
    /// pattern is invalid or exceeds size limits.
    pub fn compile(tool_name: &str, pattern: &str) -> Result<Self, String> {
        if pattern.len() > MAX_PATTERN_LEN {
            return Err(format!(
                "pattern too long ({} bytes, max {MAX_PATTERN_LEN})",
                pattern.len()
            ));
        }

        // Anchor the pattern: it must match the full extracted string.
        let anchored = format!("^(?:{pattern})$");

        let regex = regex::RegexBuilder::new(&anchored)
            .size_limit(REGEX_SIZE_LIMIT)
            .build()
            .map_err(|e| format!("invalid regex: {e}"))?;

        Ok(Self {
            tool_name: tool_name.to_string(),
            pattern_src: pattern.to_string(),
            regex,
        })
    }

    /// Test whether this rule matches the given tool name and input.
    pub fn matches(&self, tool_name: &str, tool_input: &serde_json::Value) -> bool {
        if self.tool_name != tool_name {
            return false;
        }
        let haystack = extract_match_string(tool_name, tool_input);
        self.regex.is_match(&haystack)
    }
}

// ---------------------------------------------------------------------------
// ApprovalRuleSet — the full set checked on each approval request
// ---------------------------------------------------------------------------

/// Holds all approval rules for a bridge: hardcoded globals, static config
/// rules, and dynamic DB rules (both app-wide and conversation-scoped).
///
/// The dynamic rules vec is behind a `tokio::sync::RwLock` because new rules
/// can be added mid-conversation via the "Always Allow" UI flow.
pub struct ApprovalRuleSet {
    /// Hardcoded read-only tool names (exact match, no pattern needed).
    global_tools: std::collections::HashSet<String>,
    /// Static rules from TOML config (immutable after bridge creation).
    static_rules: Vec<CompiledRule>,
    /// Dynamic rules from DB (app-wide + conversation-scoped).
    /// May grow during the bridge's lifetime.
    dynamic_rules: tokio::sync::RwLock<Vec<CompiledRule>>,
}

impl ApprovalRuleSet {
    /// Create a new rule set from the three sources.
    ///
    /// `global_extra` adds tool names to the hardcoded global set (from
    /// `IntegrationFactory::tools()` auto-approve registrations).
    /// `static_configs` are from the TOML config.
    /// `db_rules` are pre-loaded from the database.
    ///
    /// Invalid static/DB patterns are logged and skipped (they were valid when
    /// created; if the regex crate changed, we don't want to panic on startup).
    pub fn new(
        global_extra: &[&str],
        static_configs: &[ApprovalRuleConfig],
        db_rules: Vec<(String, String)>, // (tool_name, pattern)
    ) -> Self {
        let mut global_tools: std::collections::HashSet<String> = GLOBAL_AUTO_APPROVE_TOOLS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        for tool in global_extra {
            global_tools.insert((*tool).to_string());
        }

        let static_rules: Vec<CompiledRule> = static_configs
            .iter()
            .filter_map(|cfg| match CompiledRule::compile(&cfg.tool, &cfg.pattern) {
                Ok(rule) => Some(rule),
                Err(e) => {
                    tracing::warn!(
                        tool = %cfg.tool,
                        pattern = %cfg.pattern,
                        "skipping invalid static approval rule: {e}",
                    );
                    None
                }
            })
            .collect();

        let dynamic_rules: Vec<CompiledRule> = db_rules
            .into_iter()
            .filter_map(
                |(tool, pattern)| match CompiledRule::compile(&tool, &pattern) {
                    Ok(rule) => Some(rule),
                    Err(e) => {
                        tracing::warn!(
                            tool = %tool,
                            pattern = %pattern,
                            "skipping invalid DB approval rule: {e}",
                        );
                        None
                    }
                },
            )
            .collect();

        Self {
            global_tools,
            static_rules,
            dynamic_rules: tokio::sync::RwLock::new(dynamic_rules),
        }
    }

    /// Check if a tool invocation should be auto-approved.
    ///
    /// For Bash commands: splits compound commands (`&&`, `||`, `;`, `|`)
    /// and requires every sub-command to match at least one rule.
    /// Unparseable commands (subshells, command substitution, etc.) are
    /// never auto-approved.
    pub async fn matches(&self, tool_name: &str, tool_input: &serde_json::Value) -> bool {
        self.check(tool_name, tool_input).await.is_approved()
    }

    /// Check a tool invocation against the rule set and return *which* rule
    /// matched (if any). Use this instead of `matches()` when you need to
    /// report the approval reason.
    ///
    /// For compound Bash commands, returns the match for the *first*
    /// sub-command (they all must match, and the first is representative).
    pub async fn check(&self, tool_name: &str, tool_input: &serde_json::Value) -> ApprovalMatch {
        // 1. Hardcoded globals (exact tool name match).
        if self.global_tools.contains(tool_name) {
            return ApprovalMatch::GlobalTool;
        }

        // For Bash, split compound commands and check each sub-command.
        if tool_name == "Bash"
            && let Some(command) = tool_input.get("command").and_then(|v| v.as_str())
        {
            return match split_bash_command(command) {
                BashSplit::Simple(cmd) => {
                    let input = serde_json::json!({"command": cmd});
                    self.check_single(tool_name, &input).await
                }
                BashSplit::Compound(cmds) => {
                    let mut first_match = ApprovalMatch::NoMatch;
                    for (i, cmd) in cmds.iter().enumerate() {
                        let input = serde_json::json!({"command": cmd});
                        let m = self.check_single(tool_name, &input).await;
                        if !m.is_approved() {
                            return ApprovalMatch::NoMatch;
                        }
                        if i == 0 {
                            first_match = m;
                        }
                    }
                    first_match
                }
                BashSplit::Unparseable => ApprovalMatch::NoMatch,
            };
        }

        self.check_single(tool_name, tool_input).await
    }

    /// Check a single (non-compound) tool invocation against all rule layers.
    async fn check_single(&self, tool_name: &str, tool_input: &serde_json::Value) -> ApprovalMatch {
        // Static rules from config.
        for rule in &self.static_rules {
            if rule.matches(tool_name, tool_input) {
                return ApprovalMatch::ConfigRule {
                    pattern: rule.pattern_src.clone(),
                };
            }
        }
        // Dynamic rules from DB.
        let dynamic = self.dynamic_rules.read().await;
        for rule in dynamic.iter() {
            if rule.matches(tool_name, tool_input) {
                return ApprovalMatch::AlwaysAllowRule {
                    pattern: rule.pattern_src.clone(),
                };
            }
        }
        ApprovalMatch::NoMatch
    }

    /// Add a new dynamic rule (from an "Always Allow" action).
    /// The caller is responsible for persisting to DB.
    pub async fn add_dynamic(&self, rule: CompiledRule) {
        let mut dynamic = self.dynamic_rules.write().await;
        dynamic.push(rule);
    }
}

impl std::fmt::Debug for ApprovalRuleSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApprovalRuleSet")
            .field("global_tools", &self.global_tools)
            .field("static_rules", &self.static_rules.len())
            .field("dynamic_rules", &"<RwLock>")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Bash command splitting
// ---------------------------------------------------------------------------

/// Result of attempting to split a bash command on shell operators.
#[derive(Debug, PartialEq)]
enum BashSplit<'a> {
    /// Single simple command (no operators found).
    Simple(&'a str),
    /// Multiple commands separated by `&&`, `||`, `;`, or `|`.
    /// Each entry is the trimmed sub-command string.
    Compound(Vec<&'a str>),
    /// Command contains constructs we can't safely split (subshells,
    /// command substitution, heredocs, unmatched quotes).
    Unparseable,
}

/// Quote-aware state machine that splits a bash command on `&&`, `||`, `;`,
/// and `|` while respecting single/double quoting and backslash escapes.
///
/// Returns `Unparseable` for anything we can't confidently handle:
/// subshells `(...)`, command substitution `$(...)` or backticks, process
/// substitution `<(...)` / `>(...)`, heredocs `<<`, and unmatched quotes.
fn split_bash_command(command: &str) -> BashSplit<'_> {
    #[derive(Clone, Copy)]
    enum State {
        Unquoted,
        SingleQuoted,
        DoubleQuoted,
    }

    let bytes = command.as_bytes();
    let len = bytes.len();
    let mut state = State::Unquoted;
    let mut i = 0;
    // Split points: (start_of_next_cmd, operator_len) pairs.
    // The first entry is the implicit start with no preceding operator.
    let mut splits: Vec<(usize, usize)> = vec![(0, 0)];

    while i < len {
        match state {
            State::Unquoted => {
                match bytes[i] {
                    b'\\' => {
                        // Skip the escaped character.
                        i += 1;
                        if i >= len {
                            return BashSplit::Unparseable; // trailing backslash
                        }
                    }
                    b'\'' => state = State::SingleQuoted,
                    b'"' => state = State::DoubleQuoted,
                    b'`' | b'(' | b')' => return BashSplit::Unparseable,
                    b'$' if i + 1 < len && bytes[i + 1] == b'(' => {
                        return BashSplit::Unparseable;
                    }
                    b'<' if i + 1 < len && (bytes[i + 1] == b'<' || bytes[i + 1] == b'(') => {
                        return BashSplit::Unparseable;
                    }
                    b'<' if i + 1 < len && bytes[i + 1] == b'&' => {
                        // fd-duplication redirect (e.g., `0<&3`). Skip the `&`
                        // so it isn't mistaken for a background operator.
                        i += 2;
                        continue;
                    }
                    b'>' if i + 1 < len && bytes[i + 1] == b'(' => {
                        return BashSplit::Unparseable;
                    }
                    b'>' if i + 1 < len && bytes[i + 1] == b'&' => {
                        // fd-duplication redirect (e.g., `2>&1`, `>&2`). Skip
                        // the `&` so it isn't mistaken for a background operator.
                        i += 2;
                        continue;
                    }
                    b'>' if i + 1 < len && bytes[i + 1] == b'|' => {
                        // Clobber redirect (`>|`). Skip the `|` so it isn't
                        // mistaken for a pipe operator.
                        i += 2;
                        continue;
                    }
                    b'&' if i + 1 < len && bytes[i + 1] == b'&' => {
                        // Split on &&.
                        splits.push((i + 2, 2));
                        i += 2;
                        continue;
                    }
                    b'&' if i + 1 < len && (bytes[i + 1] == b'>') => {
                        // Bash shorthand: `&>file` / `&>>file` redirects both
                        // stdout and stderr. The `&` is part of the redirect
                        // operator, not a background separator. Skip it and
                        // let the next iteration handle `>` normally.
                        i += 1;
                        continue;
                    }
                    b'&' => {
                        // Single & is the background operator — also a command
                        // separator (`cmd1 & cmd2` runs both independently).
                        splits.push((i + 1, 1));
                    }
                    b'|' if i + 1 < len && bytes[i + 1] == b'|' => {
                        // Split on ||.
                        splits.push((i + 2, 2));
                        i += 2;
                        continue;
                    }
                    b'|' if i + 1 < len && bytes[i + 1] == b'&' => {
                        // Bash `|&` — pipe stdout+stderr. It's still a pipe
                        // operator (split here), but consume both chars so
                        // the `&` isn't treated as a background separator.
                        splits.push((i + 2, 2));
                        i += 2;
                        continue;
                    }
                    b'|' => {
                        // Split on single |.
                        splits.push((i + 1, 1));
                    }
                    b';' | b'\n' => {
                        // Split on ; and newlines (both are command separators).
                        splits.push((i + 1, 1));
                    }
                    _ => {}
                }
            }
            State::SingleQuoted => {
                if bytes[i] == b'\'' {
                    state = State::Unquoted;
                }
                // Everything else is literal in single quotes.
            }
            State::DoubleQuoted => {
                match bytes[i] {
                    b'\\' => {
                        i += 1; // skip escaped char
                        if i >= len {
                            return BashSplit::Unparseable; // trailing backslash in double quotes
                        }
                    }
                    b'"' => state = State::Unquoted,
                    b'`' => return BashSplit::Unparseable,
                    b'$' if i + 1 < len && bytes[i + 1] == b'(' => {
                        return BashSplit::Unparseable;
                    }
                    _ => {}
                }
            }
        }
        i += 1;
    }

    // Unmatched quotes.
    if !matches!(state, State::Unquoted) {
        return BashSplit::Unparseable;
    }

    if splits.len() == 1 {
        return BashSplit::Simple(command.trim());
    }

    // Extract sub-commands. Each split entry is (start_of_next_cmd, operator_len).
    // The end of sub-command N is the start of operator N+1, i.e.,
    // splits[N+1].0 - splits[N+1].1.
    let mut cmds = Vec::with_capacity(splits.len());
    for (idx, &(start, _op_len)) in splits.iter().enumerate() {
        let end = if idx + 1 < splits.len() {
            let (next_start, next_op_len) = splits[idx + 1];
            next_start - next_op_len
        } else {
            len
        };
        let subcmd = command[start..end].trim();
        if subcmd.is_empty() {
            // Empty sub-command (e.g., `; ;` or trailing `;`). Skip it.
            continue;
        }
        cmds.push(subcmd);
    }

    if cmds.is_empty() {
        // All sub-commands were empty (e.g., `; ; ;`). This had operators
        // but no actual commands — treat as unparseable.
        return BashSplit::Unparseable;
    }
    if cmds.len() == 1 {
        return BashSplit::Simple(cmds[0]);
    }

    BashSplit::Compound(cmds)
}

// ---------------------------------------------------------------------------
// Pattern heuristic
// ---------------------------------------------------------------------------

/// Extract the string to match against from tool input.
///
/// For `Bash`: the `command` field.
/// For all other tools: the JSON-serialized input.
pub fn extract_match_string(tool_name: &str, tool_input: &serde_json::Value) -> String {
    if tool_name == "Bash"
        && let Some(cmd) = tool_input.get("command").and_then(|v| v.as_str())
    {
        return cmd.to_string();
    }
    // Fallback: serialize the whole input.
    serde_json::to_string(tool_input).expect("serialize serde_json::Value")
}

/// Generate a default regex pattern for a single (non-compound) command string.
///
/// Extracts the program (and subcommand for known multi-subcommand tools)
/// and appends `\b.*` to allow trailing arguments while preventing prefix
/// collisions (e.g., `ls\b.*` won't match `lsblk`).
fn simple_command_pattern(command: &str) -> String {
    let mut tokens = command.split_whitespace();
    let program = match tokens.next() {
        Some(p) => p,
        None => return ".*".to_string(),
    };

    // For known multi-subcommand tools, include the subcommand.
    if MULTI_SUBCOMMAND_TOOLS.contains(&program)
        && let Some(subcommand) = tokens.next()
    {
        return format!(
            "{} {}\\b.*",
            regex::escape(program),
            regex::escape(subcommand)
        );
    }

    format!("{}\\b.*", regex::escape(program))
}

/// Generate default regex patterns for the "Always Allow" UI.
///
/// For Bash: splits compound commands and generates one pattern per
/// sub-command. For unparseable commands, falls back to a literal match
/// of the entire command.
///
/// For other tools: returns `vec![".*"]` (match all invocations).
pub fn default_patterns(tool_name: &str, tool_input: &serde_json::Value) -> Vec<String> {
    if tool_name != "Bash" {
        return vec![".*".to_string()];
    }

    let command = match tool_input.get("command").and_then(|v| v.as_str()) {
        Some(cmd) => cmd,
        None => return vec![".*".to_string()],
    };

    match split_bash_command(command) {
        BashSplit::Simple(cmd) => vec![simple_command_pattern(cmd)],
        BashSplit::Compound(cmds) => cmds.iter().map(|cmd| simple_command_pattern(cmd)).collect(),
        BashSplit::Unparseable => vec![regex::escape(command)],
    }
}

// ---------------------------------------------------------------------------
// DB row type
// ---------------------------------------------------------------------------

/// A row from the `approval_rules` DB table.
#[derive(Debug, Clone)]
pub struct ApprovalRuleRow {
    pub id: i64,
    pub app_slug: String,
    pub conversation_id: Option<i64>,
    pub tool_name: String,
    pub pattern: String,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------
    // split_bash_command tests
    // -------------------------------------------------------------------

    #[test]
    fn split_simple_command() {
        assert_eq!(
            split_bash_command("git status --porcelain"),
            BashSplit::Simple("git status --porcelain")
        );
    }

    #[test]
    fn split_and_operator() {
        assert_eq!(
            split_bash_command("git status && cargo test"),
            BashSplit::Compound(vec!["git status", "cargo test"])
        );
    }

    #[test]
    fn split_or_operator() {
        assert_eq!(
            split_bash_command("make build || echo failed"),
            BashSplit::Compound(vec!["make build", "echo failed"])
        );
    }

    #[test]
    fn split_semicolon() {
        assert_eq!(
            split_bash_command("cd /tmp; ls"),
            BashSplit::Compound(vec!["cd /tmp", "ls"])
        );
    }

    #[test]
    fn split_pipe() {
        assert_eq!(
            split_bash_command("cat foo | grep bar"),
            BashSplit::Compound(vec!["cat foo", "grep bar"])
        );
    }

    #[test]
    fn split_multiple_operators() {
        assert_eq!(
            split_bash_command("git add -A && git commit -m 'test' && git push"),
            BashSplit::Compound(vec!["git add -A", "git commit -m 'test'", "git push"])
        );
    }

    #[test]
    fn split_mixed_operators() {
        assert_eq!(
            split_bash_command("make build && make test; echo done"),
            BashSplit::Compound(vec!["make build", "make test", "echo done"])
        );
    }

    #[test]
    fn split_and_inside_single_quotes() {
        assert_eq!(
            split_bash_command("echo '&&' || true"),
            BashSplit::Compound(vec!["echo '&&'", "true"])
        );
    }

    #[test]
    fn split_and_inside_double_quotes() {
        assert_eq!(
            split_bash_command(r#"echo "hello && goodbye""#),
            BashSplit::Simple(r#"echo "hello && goodbye""#)
        );
    }

    #[test]
    fn split_pipe_inside_double_quotes() {
        assert_eq!(
            split_bash_command(r#"echo "hello | world""#),
            BashSplit::Simple(r#"echo "hello | world""#)
        );
    }

    #[test]
    fn split_escaped_operator() {
        // \& escapes only the first &, making it literal. The second & is
        // a lone background operator, so this splits into two commands.
        assert_eq!(
            split_bash_command(r"echo hello \&& world"),
            BashSplit::Compound(vec![r"echo hello \&", "world"])
        );
    }

    #[test]
    fn split_nested_quotes() {
        assert_eq!(
            split_bash_command(r#"echo "it's a test" && ls"#),
            BashSplit::Compound(vec![r#"echo "it's a test""#, "ls"])
        );
    }

    #[test]
    fn split_mixed_quoting() {
        assert_eq!(
            split_bash_command(r#"echo "hello 'world'" && ls"#),
            BashSplit::Compound(vec![r#"echo "hello 'world'""#, "ls"])
        );
    }

    #[test]
    fn split_trailing_semicolon() {
        // Trailing semicolon produces an empty sub-command, which is skipped.
        assert_eq!(
            split_bash_command("git status;"),
            BashSplit::Simple("git status")
        );
    }

    #[test]
    fn split_empty_subcmds() {
        // Multiple semicolons with nothing between them — all sub-commands
        // are empty after splitting, so treat as unparseable.
        assert_eq!(split_bash_command("; ; ;"), BashSplit::Unparseable);
    }

    #[test]
    fn split_trailing_and() {
        // Trailing && with nothing after.
        assert_eq!(
            split_bash_command("git status &&"),
            BashSplit::Simple("git status")
        );
    }

    #[test]
    fn split_subshell_unparseable() {
        assert_eq!(
            split_bash_command("(cd /tmp && ls)"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_command_substitution_dollar_paren() {
        assert_eq!(split_bash_command("echo $(whoami)"), BashSplit::Unparseable);
    }

    #[test]
    fn split_command_substitution_backtick() {
        assert_eq!(split_bash_command("echo `whoami`"), BashSplit::Unparseable);
    }

    #[test]
    fn split_process_substitution() {
        assert_eq!(
            split_bash_command("diff <(cmd1) <(cmd2)"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_heredoc() {
        assert_eq!(
            split_bash_command("cat <<EOF\nhello\nEOF"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_unmatched_single_quote() {
        assert_eq!(split_bash_command("echo 'unclosed"), BashSplit::Unparseable);
    }

    #[test]
    fn split_unmatched_double_quote() {
        assert_eq!(
            split_bash_command(r#"echo "unclosed"#),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_command_sub_in_double_quotes() {
        // $() inside double quotes is still command substitution.
        assert_eq!(
            split_bash_command(r#"echo "$(whoami)""#),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_backtick_in_double_quotes() {
        assert_eq!(
            split_bash_command(r#"echo "`whoami`""#),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_dollar_brace_is_not_unparseable() {
        // ${VAR} is variable expansion, not command substitution. Safe to split.
        assert_eq!(
            split_bash_command("echo ${HOME} && ls"),
            BashSplit::Compound(vec!["echo ${HOME}", "ls"])
        );
    }

    #[test]
    fn split_background_operator() {
        // Single & is a background operator / command separator.
        assert_eq!(
            split_bash_command("sleep 1 & rm -rf /"),
            BashSplit::Compound(vec!["sleep 1", "rm -rf /"])
        );
    }

    #[test]
    fn split_newline_as_separator() {
        assert_eq!(
            split_bash_command("git status\nls"),
            BashSplit::Compound(vec!["git status", "ls"])
        );
    }

    #[test]
    fn split_newline_inside_quotes_not_split() {
        assert_eq!(
            split_bash_command("echo \"hello\nworld\""),
            BashSplit::Simple("echo \"hello\nworld\"")
        );
    }

    #[test]
    fn split_escaped_semicolon_not_split() {
        assert_eq!(
            split_bash_command(r"echo hello\; world"),
            BashSplit::Simple(r"echo hello\; world")
        );
    }

    #[test]
    fn split_escaped_pipe_not_split() {
        assert_eq!(
            split_bash_command(r"echo hello\| world"),
            BashSplit::Simple(r"echo hello\| world")
        );
    }

    #[test]
    fn split_pipe_inside_single_quotes() {
        assert_eq!(
            split_bash_command("echo 'hello | world'"),
            BashSplit::Simple("echo 'hello | world'")
        );
    }

    #[test]
    fn split_semicolon_inside_single_quotes() {
        assert_eq!(
            split_bash_command("echo 'hello; world'"),
            BashSplit::Simple("echo 'hello; world'")
        );
    }

    #[test]
    fn split_redirect_not_unparseable() {
        // > and < without ( are normal redirections, not process substitution.
        assert_eq!(
            split_bash_command("echo foo > /tmp/out && cat < /tmp/out"),
            BashSplit::Compound(vec!["echo foo > /tmp/out", "cat < /tmp/out"])
        );
    }

    #[test]
    fn split_stderr_redirect_to_stdout() {
        // 2>&1 is fd-duplication, not a background operator. The & in >&
        // must not cause a split.
        assert_eq!(
            split_bash_command("git pull origin main 2>&1 | tail -3"),
            BashSplit::Compound(vec!["git pull origin main 2>&1", "tail -3"])
        );
    }

    #[test]
    fn split_stdout_redirect_to_stderr() {
        // >&2 is also fd-duplication.
        assert_eq!(
            split_bash_command("echo error >&2 && echo ok"),
            BashSplit::Compound(vec!["echo error >&2", "echo ok"])
        );
    }

    #[test]
    fn split_clobber_redirect() {
        // >| is clobber (override noclobber), not a pipe.
        assert_eq!(
            split_bash_command("echo foo >| /tmp/out && cat /tmp/out"),
            BashSplit::Compound(vec!["echo foo >| /tmp/out", "cat /tmp/out"])
        );
    }

    #[test]
    fn split_ampersand_redirect() {
        // &> is bash shorthand for redirecting both stdout and stderr.
        // The & must not be treated as a background operator.
        assert_eq!(
            split_bash_command("cmd &>/dev/null && echo ok"),
            BashSplit::Compound(vec!["cmd &>/dev/null", "echo ok"])
        );
    }

    #[test]
    fn split_ampersand_append_redirect() {
        // &>> is bash shorthand for appending both stdout and stderr.
        assert_eq!(
            split_bash_command("cmd &>>/tmp/log && echo ok"),
            BashSplit::Compound(vec!["cmd &>>/tmp/log", "echo ok"])
        );
    }

    #[test]
    fn split_pipe_ampersand() {
        // |& is bash pipe-both-streams. It's still a pipeline split,
        // but the & must not cause an additional split.
        assert_eq!(
            split_bash_command("cmd1 |& cmd2"),
            BashSplit::Compound(vec!["cmd1", "cmd2"])
        );
    }

    #[test]
    fn split_stdin_fd_duplication() {
        // 0<&3 is input fd-duplication.
        assert_eq!(
            split_bash_command("cmd 0<&3 && echo done"),
            BashSplit::Compound(vec!["cmd 0<&3", "echo done"])
        );
    }

    #[test]
    fn split_cc_heredoc_pattern() {
        // CC sends heredoc-style commits like this — should be Unparseable
        // because of the $( command substitution.
        assert_eq!(
            split_bash_command("git commit -m \"$(cat <<'EOF'\nmessage\nEOF\n)\""),
            BashSplit::Unparseable
        );
    }

    // -------------------------------------------------------------------
    // Comprehensive redirect operator tests
    // -------------------------------------------------------------------

    #[test]
    fn split_simple_output_redirect() {
        // Plain > is not an operator, just a redirect.
        assert_eq!(
            split_bash_command("echo hello > /tmp/out"),
            BashSplit::Simple("echo hello > /tmp/out")
        );
    }

    #[test]
    fn split_append_redirect() {
        // >> is append redirect, not two separate operators.
        assert_eq!(
            split_bash_command("echo hello >> /tmp/out"),
            BashSplit::Simple("echo hello >> /tmp/out")
        );
    }

    #[test]
    fn split_input_redirect() {
        assert_eq!(
            split_bash_command("sort < /tmp/data"),
            BashSplit::Simple("sort < /tmp/data")
        );
    }

    #[test]
    fn split_read_write_redirect() {
        // <> opens for read+write.
        assert_eq!(
            split_bash_command("exec 3<> /tmp/file"),
            BashSplit::Simple("exec 3<> /tmp/file")
        );
    }

    #[test]
    fn split_numbered_fd_redirect() {
        // 2>/dev/null — redirect stderr only. Plain > after a digit.
        assert_eq!(
            split_bash_command("cmd 2>/dev/null"),
            BashSplit::Simple("cmd 2>/dev/null")
        );
    }

    #[test]
    fn split_multiple_redirects() {
        // Multiple redirects on the same command.
        assert_eq!(
            split_bash_command("cmd </tmp/in >/tmp/out 2>/tmp/err"),
            BashSplit::Simple("cmd </tmp/in >/tmp/out 2>/tmp/err")
        );
    }

    #[test]
    fn split_redirect_with_pipe() {
        // Redirect and pipe together.
        assert_eq!(
            split_bash_command("cmd 2>/dev/null | grep foo"),
            BashSplit::Compound(vec!["cmd 2>/dev/null", "grep foo"])
        );
    }

    #[test]
    fn split_close_fd() {
        // >&- closes stdout, <&- closes stdin. The & must not split.
        assert_eq!(
            split_bash_command("cmd >&- && echo done"),
            BashSplit::Compound(vec!["cmd >&-", "echo done"])
        );
    }

    #[test]
    fn split_close_input_fd() {
        assert_eq!(
            split_bash_command("cmd <&- && echo done"),
            BashSplit::Compound(vec!["cmd <&-", "echo done"])
        );
    }

    #[test]
    fn split_clobber_redirect_simple() {
        // >| alone with no other operators.
        assert_eq!(
            split_bash_command("echo foo >| /tmp/out"),
            BashSplit::Simple("echo foo >| /tmp/out")
        );
    }

    #[test]
    fn split_ampersand_redirect_simple() {
        // &> alone with no other operators.
        assert_eq!(
            split_bash_command("cmd &>/dev/null"),
            BashSplit::Simple("cmd &>/dev/null")
        );
    }

    // -------------------------------------------------------------------
    // Comprehensive quoting edge cases
    // -------------------------------------------------------------------

    #[test]
    fn split_empty_single_quotes() {
        assert_eq!(
            split_bash_command("echo '' && ls"),
            BashSplit::Compound(vec!["echo ''", "ls"])
        );
    }

    #[test]
    fn split_empty_double_quotes() {
        assert_eq!(
            split_bash_command("echo \"\" && ls"),
            BashSplit::Compound(vec!["echo \"\"", "ls"])
        );
    }

    #[test]
    fn split_adjacent_quoted_strings() {
        // 'foo'"bar" is valid shell — concatenation.
        assert_eq!(
            split_bash_command("echo 'foo'\"bar\" && ls"),
            BashSplit::Compound(vec!["echo 'foo'\"bar\"", "ls"])
        );
    }

    #[test]
    fn split_backslash_in_double_quotes() {
        // \" inside double quotes is an escaped quote.
        assert_eq!(
            split_bash_command(r#"echo "say \"hello\"" && ls"#),
            BashSplit::Compound(vec![r#"echo "say \"hello\"""#, "ls"])
        );
    }

    #[test]
    fn split_single_quote_in_double_quotes() {
        assert_eq!(
            split_bash_command(r#"echo "it's" && ls"#),
            BashSplit::Compound(vec![r#"echo "it's""#, "ls"])
        );
    }

    #[test]
    fn split_double_quote_in_single_quotes() {
        assert_eq!(
            split_bash_command("echo '\"hello\"' && ls"),
            BashSplit::Compound(vec!["echo '\"hello\"'", "ls"])
        );
    }

    #[test]
    fn split_backslash_before_operator() {
        // Escaped && is not an operator.
        assert_eq!(
            split_bash_command(r"echo hello \&\& world"),
            BashSplit::Simple(r"echo hello \&\& world")
        );
    }

    #[test]
    fn split_backslash_newline() {
        // Backslash-newline is line continuation, not a separator.
        assert_eq!(
            split_bash_command("echo hello\\\nworld"),
            BashSplit::Simple("echo hello\\\nworld")
        );
    }

    // -------------------------------------------------------------------
    // Comprehensive operator interaction tests
    // -------------------------------------------------------------------

    #[test]
    fn split_all_operator_types_combined() {
        // Mix of &&, ||, ;, |, and & in one command.
        assert_eq!(
            split_bash_command("a && b || c; d | e & f"),
            BashSplit::Compound(vec!["a", "b", "c", "d", "e", "f"])
        );
    }

    #[test]
    fn split_pipe_then_and() {
        assert_eq!(
            split_bash_command("cat file | grep foo && echo found"),
            BashSplit::Compound(vec!["cat file", "grep foo", "echo found"])
        );
    }

    #[test]
    fn split_long_pipeline() {
        assert_eq!(
            split_bash_command("cat file | grep foo | sort | uniq | head"),
            BashSplit::Compound(vec!["cat file", "grep foo", "sort", "uniq", "head"])
        );
    }

    #[test]
    fn split_whitespace_around_operators() {
        // No spaces around operators — should still work.
        assert_eq!(
            split_bash_command("a&&b||c;d|e"),
            BashSplit::Compound(vec!["a", "b", "c", "d", "e"])
        );
    }

    #[test]
    fn split_lots_of_whitespace() {
        assert_eq!(
            split_bash_command("  a   &&   b   "),
            BashSplit::Compound(vec!["a", "b"])
        );
    }

    #[test]
    fn split_only_whitespace() {
        assert_eq!(split_bash_command("   "), BashSplit::Simple(""));
    }

    #[test]
    fn split_empty_string() {
        assert_eq!(split_bash_command(""), BashSplit::Simple(""));
    }

    // -------------------------------------------------------------------
    // Unparseable constructs (safety: must reject)
    // -------------------------------------------------------------------

    #[test]
    fn split_nested_subshell() {
        assert_eq!(
            split_bash_command("(a && (b || c))"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_dollar_paren_in_argument() {
        assert_eq!(
            split_bash_command("echo $(cat /etc/hostname)"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_backtick_in_argument() {
        assert_eq!(
            split_bash_command("echo `cat /etc/hostname`"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_process_sub_input() {
        assert_eq!(
            split_bash_command("diff <(sort a) <(sort b)"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_process_sub_output() {
        assert_eq!(
            split_bash_command("tee >(grep error > /tmp/err)"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_heredoc_operator() {
        assert_eq!(
            split_bash_command("cat <<EOF\nhello\nEOF"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_heredoc_with_dash() {
        // <<- is the indented heredoc variant.
        assert_eq!(
            split_bash_command("cat <<-EOF\n\thello\n\tEOF"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_subshell_in_pipeline() {
        assert_eq!(
            split_bash_command("echo foo | (cat && wc)"),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_command_sub_in_quotes_unparseable() {
        // Even inside double quotes, $() is command substitution.
        assert_eq!(
            split_bash_command(r#"echo "hello $(whoami)""#),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_trailing_backslash_unparseable() {
        assert_eq!(split_bash_command("echo hello\\"), BashSplit::Unparseable);
    }

    #[test]
    fn split_unmatched_single_quote_unparseable() {
        assert_eq!(split_bash_command("echo 'oops"), BashSplit::Unparseable);
    }

    #[test]
    fn split_unmatched_double_quote_unparseable() {
        assert_eq!(split_bash_command("echo \"oops"), BashSplit::Unparseable);
    }

    // -------------------------------------------------------------------
    // Real-world commands CC actually sends
    // -------------------------------------------------------------------

    #[test]
    fn split_git_commit_heredoc_style() {
        assert_eq!(
            split_bash_command("git commit -m \"$(cat <<'EOF'\nFix the bug\nEOF\n)\""),
            BashSplit::Unparseable
        );
    }

    #[test]
    fn split_cargo_test_with_stderr_redirect() {
        assert_eq!(
            split_bash_command("cargo test 2>&1 | tail -20"),
            BashSplit::Compound(vec!["cargo test 2>&1", "tail -20"])
        );
    }

    #[test]
    fn split_make_and_run() {
        assert_eq!(
            split_bash_command("make build && ./target/debug/myapp"),
            BashSplit::Compound(vec!["make build", "./target/debug/myapp"])
        );
    }

    #[test]
    fn split_git_status_simple() {
        assert_eq!(
            split_bash_command("git status --porcelain"),
            BashSplit::Simple("git status --porcelain")
        );
    }

    #[test]
    fn split_git_log_with_format() {
        assert_eq!(
            split_bash_command("git log --oneline -10"),
            BashSplit::Simple("git log --oneline -10")
        );
    }

    #[test]
    fn split_npm_install_and_build() {
        assert_eq!(
            split_bash_command("npm install && npm run build"),
            BashSplit::Compound(vec!["npm install", "npm run build"])
        );
    }

    #[test]
    fn split_grep_pipeline() {
        assert_eq!(
            split_bash_command("ps aux | grep myapp | grep -v grep"),
            BashSplit::Compound(vec!["ps aux", "grep myapp", "grep -v grep"])
        );
    }

    #[test]
    fn split_silent_command() {
        // Common pattern: discard all output.
        assert_eq!(
            split_bash_command("cmd >/dev/null 2>&1"),
            BashSplit::Simple("cmd >/dev/null 2>&1")
        );
    }

    #[test]
    fn split_silent_with_ampersand_redirect() {
        // Same thing with bash shorthand.
        assert_eq!(
            split_bash_command("cmd &>/dev/null"),
            BashSplit::Simple("cmd &>/dev/null")
        );
    }

    #[test]
    fn split_ls_with_pipe_to_head() {
        assert_eq!(
            split_bash_command("ls -la | head -5"),
            BashSplit::Compound(vec!["ls -la", "head -5"])
        );
    }

    #[test]
    fn split_chained_git_commands() {
        assert_eq!(
            split_bash_command("git add -A && git commit -m 'fix' && git push"),
            BashSplit::Compound(vec!["git add -A", "git commit -m 'fix'", "git push"])
        );
    }

    #[test]
    fn split_or_with_fallback() {
        assert_eq!(
            split_bash_command("make build || echo 'build failed'"),
            BashSplit::Compound(vec!["make build", "echo 'build failed'"])
        );
    }

    #[test]
    fn split_variable_expansion_safe() {
        // ${VAR} is variable expansion, not command substitution. Should be fine.
        assert_eq!(
            split_bash_command("echo ${HOME}/.config"),
            BashSplit::Simple("echo ${HOME}/.config")
        );
    }

    #[test]
    fn split_variable_expansion_in_pipeline() {
        assert_eq!(
            split_bash_command("echo ${PATH} | tr ':' '\\n' | sort"),
            BashSplit::Compound(vec!["echo ${PATH}", "tr ':' '\\n'", "sort"])
        );
    }

    // -------------------------------------------------------------------
    // Whitespace edge cases
    // -------------------------------------------------------------------

    #[test]
    fn split_no_space_before_redirect() {
        // `2>&1` with no space before the fd number.
        assert_eq!(split_bash_command("cmd2>&1"), BashSplit::Simple("cmd2>&1"));
    }

    #[test]
    fn split_no_space_around_pipe() {
        assert_eq!(
            split_bash_command("echo foo|grep bar"),
            BashSplit::Compound(vec!["echo foo", "grep bar"])
        );
    }

    #[test]
    fn split_no_space_around_and() {
        assert_eq!(
            split_bash_command("true&&false"),
            BashSplit::Compound(vec!["true", "false"])
        );
    }

    #[test]
    fn split_no_space_around_or() {
        assert_eq!(
            split_bash_command("true||false"),
            BashSplit::Compound(vec!["true", "false"])
        );
    }

    #[test]
    fn split_no_space_around_semicolon() {
        assert_eq!(
            split_bash_command("a;b;c"),
            BashSplit::Compound(vec!["a", "b", "c"])
        );
    }

    #[test]
    fn split_tabs_instead_of_spaces() {
        assert_eq!(
            split_bash_command("a\t&&\tb"),
            BashSplit::Compound(vec!["a", "b"])
        );
    }

    #[test]
    fn split_redirect_jammed_against_operator() {
        // `2>&1|` — the >&1 is a redirect, then | is a pipe, no spaces.
        assert_eq!(
            split_bash_command("cmd 2>&1|grep err"),
            BashSplit::Compound(vec!["cmd 2>&1", "grep err"])
        );
    }

    #[test]
    fn split_clobber_jammed_against_filename() {
        assert_eq!(
            split_bash_command("echo x>|/tmp/f&&echo ok"),
            BashSplit::Compound(vec!["echo x>|/tmp/f", "echo ok"])
        );
    }

    #[test]
    fn split_ampersand_redirect_jammed() {
        assert_eq!(
            split_bash_command("cmd&>/dev/null&&echo ok"),
            BashSplit::Compound(vec!["cmd&>/dev/null", "echo ok"])
        );
    }

    #[test]
    fn split_pipe_ampersand_jammed() {
        assert_eq!(
            split_bash_command("cmd1|&cmd2"),
            BashSplit::Compound(vec!["cmd1", "cmd2"])
        );
    }

    #[test]
    fn split_multiple_spaces_between_commands() {
        assert_eq!(
            split_bash_command("a   |   b   &&   c"),
            BashSplit::Compound(vec!["a", "b", "c"])
        );
    }

    #[test]
    fn split_leading_trailing_whitespace() {
        assert_eq!(
            split_bash_command("   git status   "),
            BashSplit::Simple("git status")
        );
    }

    #[test]
    fn split_leading_trailing_whitespace_compound() {
        assert_eq!(
            split_bash_command("   a && b   "),
            BashSplit::Compound(vec!["a", "b"])
        );
    }

    // -------------------------------------------------------------------
    // Combination stress tests
    // -------------------------------------------------------------------

    #[test]
    fn split_redirect_in_every_pipeline_stage() {
        assert_eq!(
            split_bash_command("cmd1 2>&1 | cmd2 >/dev/null | cmd3 2>/dev/null"),
            BashSplit::Compound(vec!["cmd1 2>&1", "cmd2 >/dev/null", "cmd3 2>/dev/null"])
        );
    }

    #[test]
    fn split_redirect_and_quotes_combined() {
        assert_eq!(
            split_bash_command(r#"echo "hello world" 2>&1 | grep "hello""#),
            BashSplit::Compound(vec![r#"echo "hello world" 2>&1"#, r#"grep "hello""#])
        );
    }

    #[test]
    fn split_ampersand_redirect_in_pipeline() {
        assert_eq!(
            split_bash_command("cmd1 &>/dev/null; cmd2 2>&1 | cmd3"),
            BashSplit::Compound(vec!["cmd1 &>/dev/null", "cmd2 2>&1", "cmd3"])
        );
    }

    #[test]
    fn split_quoted_operator_adjacent_to_real_operator() {
        // The && inside quotes is literal; the && outside is real.
        assert_eq!(
            split_bash_command(r#"echo "&&" && echo ok"#),
            BashSplit::Compound(vec![r#"echo "&&""#, "echo ok"])
        );
    }

    #[test]
    fn split_escaped_backslash_before_operator() {
        // \\\\ is two escaped backslashes (literal \\), then && is a real operator.
        assert_eq!(
            split_bash_command("echo \\\\ && ls"),
            BashSplit::Compound(vec!["echo \\\\", "ls"])
        );
    }

    // -------------------------------------------------------------------
    // Procedurally generated tests
    // -------------------------------------------------------------------

    /// For a set of simple commands, joining them with any operator and
    /// splitting should recover the same commands (modulo whitespace).
    #[test]
    fn procedural_split_with_all_operators() {
        let cmds = ["echo hello", "ls -la", "git status", "cat /tmp/f"];
        let operators = [" && ", " || ", " | ", "; ", "\n"];

        for op in &operators {
            for n in 2..=cmds.len() {
                let subset = &cmds[..n];
                let joined = subset.join(op);
                match split_bash_command(&joined) {
                    BashSplit::Compound(result) => {
                        assert_eq!(
                            result,
                            subset.iter().map(|s| s.trim()).collect::<Vec<_>>(),
                            "Failed for operator {:?} with {} commands: {:?}",
                            op,
                            n,
                            joined
                        );
                    }
                    other => panic!(
                        "Expected Compound for operator {:?} with {} commands, got {:?}. Input: {:?}",
                        op, n, other, joined
                    ),
                }
            }
        }
    }

    /// Single commands with various redirect suffixes should remain Simple.
    #[test]
    fn procedural_redirects_stay_simple() {
        let bases = ["echo hello", "cmd", "git status"];
        let redirects = [
            " >/dev/null",
            " 2>/dev/null",
            " 2>&1",
            " >&2",
            " >>/tmp/log",
            " &>/dev/null",
            " &>>/tmp/log",
            " </tmp/in",
            " 0<&3",
            " >&-",
            " <&-",
            " >|/tmp/f",
        ];

        for base in &bases {
            for redir in &redirects {
                let cmd = format!("{base}{redir}");
                match split_bash_command(&cmd) {
                    BashSplit::Simple(s) => {
                        assert_eq!(s, cmd.trim(), "Unexpected trim for: {:?}", cmd);
                    }
                    other => panic!("Expected Simple for {:?}, got {:?}", cmd, other),
                }
            }
        }
    }

    /// Redirect suffixes followed by a real operator should split correctly
    /// (redirect stays with the first command).
    #[test]
    fn procedural_redirect_then_operator() {
        let redirects = [
            "2>&1",
            ">&2",
            "&>/dev/null",
            ">|/tmp/f",
            ">/dev/null",
            "0<&3",
            ">&-",
            "<&-",
        ];
        let operators = [" && ", " || ", " | ", "; "];

        for redir in &redirects {
            for op in &operators {
                let cmd = format!("cmd1 {redir}{op}cmd2");
                match split_bash_command(&cmd) {
                    BashSplit::Compound(result) => {
                        assert_eq!(
                            result.len(),
                            2,
                            "Expected 2 sub-commands for {:?}, got {:?}",
                            cmd,
                            result
                        );
                        assert!(
                            result[0].contains(redir),
                            "First sub-command {:?} should contain redirect {:?}. Input: {:?}",
                            result[0],
                            redir,
                            cmd
                        );
                        assert_eq!(result[1], "cmd2", "Second sub-command wrong for: {:?}", cmd);
                    }
                    other => panic!("Expected Compound for {:?}, got {:?}", cmd, other),
                }
            }
        }
    }

    /// Operators inside single quotes should never cause a split.
    #[test]
    fn procedural_operators_in_single_quotes() {
        let operators = ["&&", "||", "|", ";", "&", "|&", "\n"];
        for op in &operators {
            let cmd = format!("echo '{op}'");
            match split_bash_command(&cmd) {
                BashSplit::Simple(s) => {
                    assert_eq!(s, cmd.as_str(), "Wrong result for: {:?}", cmd);
                }
                other => panic!("Expected Simple for {:?}, got {:?}", cmd, other),
            }
        }
    }

    /// Operators inside double quotes should never cause a split.
    #[test]
    fn procedural_operators_in_double_quotes() {
        let operators = ["&&", "||", "|", ";", "&", "|&"];
        for op in &operators {
            let cmd = format!("echo \"{op}\"");
            match split_bash_command(&cmd) {
                BashSplit::Simple(s) => {
                    assert_eq!(s, cmd.as_str(), "Wrong result for: {:?}", cmd);
                }
                other => panic!("Expected Simple for {:?}, got {:?}", cmd, other),
            }
        }
    }

    /// Unparseable constructs should be rejected regardless of surrounding context.
    #[test]
    fn procedural_unparseable_constructs() {
        let constructs = ["$(cmd)", "`cmd`", "(cmd)", "<(cmd)", ">(cmd)", "<<EOF"];
        for construct in &constructs {
            let cmd = format!("echo {construct}");
            assert_eq!(
                split_bash_command(&cmd),
                BashSplit::Unparseable,
                "Should be Unparseable: {:?}",
                cmd
            );
        }
    }

    // -------------------------------------------------------------------
    // extract_match_string tests
    // -------------------------------------------------------------------

    #[test]
    fn extract_bash_command() {
        let input = serde_json::json!({"command": "git status --porcelain"});
        assert_eq!(
            extract_match_string("Bash", &input),
            "git status --porcelain"
        );
    }

    #[test]
    fn extract_non_bash_serializes_json() {
        let input = serde_json::json!({"file_path": "/tmp/foo.rs"});
        let result = extract_match_string("Read", &input);
        assert!(result.contains("file_path"));
    }

    // -------------------------------------------------------------------
    // default_patterns tests
    // -------------------------------------------------------------------

    #[test]
    fn default_patterns_git_status() {
        let input = serde_json::json!({"command": "git status --porcelain"});
        assert_eq!(default_patterns("Bash", &input), vec!["git status\\b.*"]);
    }

    #[test]
    fn default_patterns_cargo_clippy() {
        let input = serde_json::json!({"command": "cargo clippy -- -D warnings"});
        assert_eq!(default_patterns("Bash", &input), vec!["cargo clippy\\b.*"]);
    }

    #[test]
    fn default_patterns_ls() {
        let input = serde_json::json!({"command": "ls -la /tmp"});
        assert_eq!(default_patterns("Bash", &input), vec!["ls\\b.*"]);
    }

    #[test]
    fn default_patterns_ls_does_not_match_lsblk() {
        let input = serde_json::json!({"command": "ls -la /tmp"});
        let patterns = default_patterns("Bash", &input);
        let rule = CompiledRule::compile("Bash", &patterns[0]).unwrap();

        // Should match ls commands
        assert!(rule.matches("Bash", &serde_json::json!({"command": "ls"})));
        assert!(rule.matches("Bash", &serde_json::json!({"command": "ls -la"})));
        assert!(rule.matches("Bash", &serde_json::json!({"command": "ls /tmp"})));

        // Should NOT match lsblk, lsof, etc.
        assert!(!rule.matches("Bash", &serde_json::json!({"command": "lsblk"})));
        assert!(!rule.matches("Bash", &serde_json::json!({"command": "lsof -i :8080"})));
    }

    #[test]
    fn default_patterns_compound_command() {
        let input = serde_json::json!({"command": "git status && cargo test"});
        assert_eq!(
            default_patterns("Bash", &input),
            vec!["git status\\b.*", "cargo test\\b.*"]
        );
    }

    #[test]
    fn default_patterns_pipeline() {
        let input = serde_json::json!({"command": "cat foo | grep bar"});
        assert_eq!(
            default_patterns("Bash", &input),
            vec!["cat\\b.*", "grep\\b.*"]
        );
    }

    #[test]
    fn default_patterns_unparseable_falls_back_to_literal() {
        let input = serde_json::json!({"command": "echo $(whoami)"});
        let patterns = default_patterns("Bash", &input);
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0], regex::escape("echo $(whoami)"));
    }

    #[test]
    fn default_patterns_non_bash() {
        let input = serde_json::json!({"file_path": "/tmp/foo.rs"});
        assert_eq!(default_patterns("Read", &input), vec![".*"]);
    }

    #[test]
    fn default_patterns_background_operator() {
        let input = serde_json::json!({"command": "sleep 1 & rm -rf /"});
        assert_eq!(
            default_patterns("Bash", &input),
            vec!["sleep\\b.*", "rm\\b.*"]
        );
    }

    #[test]
    fn default_patterns_newline_separator() {
        let input = serde_json::json!({"command": "git status\nls -la"});
        assert_eq!(
            default_patterns("Bash", &input),
            vec!["git status\\b.*", "ls\\b.*"]
        );
    }

    #[test]
    fn default_patterns_simple_no_args() {
        let input = serde_json::json!({"command": "pwd"});
        assert_eq!(default_patterns("Bash", &input), vec!["pwd\\b.*"]);
    }

    #[test]
    fn default_patterns_git_no_subcommand() {
        let input = serde_json::json!({"command": "git"});
        assert_eq!(default_patterns("Bash", &input), vec!["git\\b.*"]);
    }

    // -------------------------------------------------------------------
    // CompiledRule tests
    // -------------------------------------------------------------------

    #[test]
    fn compile_valid_pattern() {
        let rule = CompiledRule::compile("Bash", "git status\\b.*").unwrap();
        assert!(rule.matches("Bash", &serde_json::json!({"command": "git status"})));
        assert!(rule.matches(
            "Bash",
            &serde_json::json!({"command": "git status --porcelain"})
        ));
        assert!(!rule.matches("Bash", &serde_json::json!({"command": "git commit -m foo"})));
    }

    #[test]
    fn compile_rejects_too_long_pattern() {
        let long_pattern = "a".repeat(MAX_PATTERN_LEN + 1);
        let result = CompiledRule::compile("Bash", &long_pattern);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too long"));
    }

    #[test]
    fn compile_rejects_invalid_regex() {
        let result = CompiledRule::compile("Bash", "(unclosed");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid regex"));
    }

    #[test]
    fn rule_only_matches_correct_tool() {
        let rule = CompiledRule::compile("Bash", ".*").unwrap();
        assert!(rule.matches("Bash", &serde_json::json!({"command": "anything"})));
        assert!(!rule.matches("Read", &serde_json::json!({"command": "anything"})));
    }

    #[test]
    fn pattern_is_fully_anchored() {
        let rule = CompiledRule::compile("Bash", "git").unwrap();
        assert!(rule.matches("Bash", &serde_json::json!({"command": "git"})));
        assert!(!rule.matches("Bash", &serde_json::json!({"command": "git status"})));
    }

    // -------------------------------------------------------------------
    // ApprovalRuleSet tests
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn rule_set_matches_global_tools() {
        let rule_set = ApprovalRuleSet::new(&[], &[], vec![]);
        assert!(rule_set.matches("Read", &serde_json::json!({})).await);
        assert!(rule_set.matches("Glob", &serde_json::json!({})).await);
        assert!(
            !rule_set
                .matches("Bash", &serde_json::json!({"command": "ls"}))
                .await
        );
    }

    #[tokio::test]
    async fn rule_set_matches_static_rules() {
        let static_rules = vec![ApprovalRuleConfig {
            tool: "Bash".to_string(),
            pattern: "git status\\b.*".to_string(),
        }];
        let rule_set = ApprovalRuleSet::new(&[], &static_rules, vec![]);
        assert!(
            rule_set
                .matches("Bash", &serde_json::json!({"command": "git status"}))
                .await
        );
        assert!(
            !rule_set
                .matches("Bash", &serde_json::json!({"command": "git commit"}))
                .await
        );
    }

    #[tokio::test]
    async fn rule_set_matches_dynamic_rules() {
        let db_rules = vec![("Bash".to_string(), "cargo test\\b.*".to_string())];
        let rule_set = ApprovalRuleSet::new(&[], &[], db_rules);
        assert!(
            rule_set
                .matches(
                    "Bash",
                    &serde_json::json!({"command": "cargo test --release"})
                )
                .await
        );
    }

    #[tokio::test]
    async fn rule_set_add_dynamic() {
        let rule_set = ApprovalRuleSet::new(&[], &[], vec![]);
        assert!(
            !rule_set
                .matches("Bash", &serde_json::json!({"command": "make build"}))
                .await
        );

        let rule = CompiledRule::compile("Bash", "make\\b.*").unwrap();
        rule_set.add_dynamic(rule).await;

        assert!(
            rule_set
                .matches("Bash", &serde_json::json!({"command": "make build"}))
                .await
        );
    }

    #[tokio::test]
    async fn rule_set_global_extra() {
        let rule_set = ApprovalRuleSet::new(&["Write"], &[], vec![]);
        assert!(rule_set.matches("Write", &serde_json::json!({})).await);
    }

    // -------------------------------------------------------------------
    // Compound command matching (the security-critical tests)
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn compound_requires_all_subcmds_to_match() {
        let rules = vec![
            ApprovalRuleConfig {
                tool: "Bash".to_string(),
                pattern: "git status\\b.*".to_string(),
            },
            ApprovalRuleConfig {
                tool: "Bash".to_string(),
                pattern: "cargo test\\b.*".to_string(),
            },
        ];
        let rule_set = ApprovalRuleSet::new(&[], &rules, vec![]);

        // Both sub-commands match — auto-approve.
        assert!(
            rule_set
                .matches(
                    "Bash",
                    &serde_json::json!({"command": "git status && cargo test"})
                )
                .await
        );

        // Second sub-command doesn't match — deny.
        assert!(
            !rule_set
                .matches(
                    "Bash",
                    &serde_json::json!({"command": "git status && rm -rf /"})
                )
                .await
        );
    }

    #[tokio::test]
    async fn pipe_requires_all_stages_to_match() {
        let rules = vec![
            ApprovalRuleConfig {
                tool: "Bash".to_string(),
                pattern: "cat\\b.*".to_string(),
            },
            ApprovalRuleConfig {
                tool: "Bash".to_string(),
                pattern: "grep\\b.*".to_string(),
            },
        ];
        let rule_set = ApprovalRuleSet::new(&[], &rules, vec![]);

        // Both stages match.
        assert!(
            rule_set
                .matches(
                    "Bash",
                    &serde_json::json!({"command": "cat foo | grep bar"})
                )
                .await
        );

        // Pipe target doesn't match.
        assert!(
            !rule_set
                .matches("Bash", &serde_json::json!({"command": "cat foo | bash"}))
                .await
        );
    }

    #[tokio::test]
    async fn unparseable_never_auto_approved() {
        // Even with a rule that matches everything.
        let rules = vec![ApprovalRuleConfig {
            tool: "Bash".to_string(),
            pattern: ".*".to_string(),
        }];
        let rule_set = ApprovalRuleSet::new(&[], &rules, vec![]);

        assert!(
            !rule_set
                .matches("Bash", &serde_json::json!({"command": "echo $(whoami)"}))
                .await
        );
    }

    #[tokio::test]
    async fn git_status_rule_no_longer_matches_compound() {
        // The original security bug: git status\b.* used to match
        // "git status && rm -rf /" because .* matched everything.
        let rules = vec![ApprovalRuleConfig {
            tool: "Bash".to_string(),
            pattern: "git status\\b.*".to_string(),
        }];
        let rule_set = ApprovalRuleSet::new(&[], &rules, vec![]);

        // Simple git status: still works.
        assert!(
            rule_set
                .matches("Bash", &serde_json::json!({"command": "git status"}))
                .await
        );

        // Compound with unapproved second command: blocked.
        assert!(
            !rule_set
                .matches(
                    "Bash",
                    &serde_json::json!({"command": "git status && rm -rf /"})
                )
                .await
        );
    }

    #[tokio::test]
    async fn background_operator_requires_all_to_match() {
        let rules = vec![ApprovalRuleConfig {
            tool: "Bash".to_string(),
            pattern: "sleep\\b.*".to_string(),
        }];
        let rule_set = ApprovalRuleSet::new(&[], &rules, vec![]);

        // sleep alone: approved.
        assert!(
            rule_set
                .matches("Bash", &serde_json::json!({"command": "sleep 1"}))
                .await
        );

        // sleep & rm: second command not approved.
        assert!(
            !rule_set
                .matches(
                    "Bash",
                    &serde_json::json!({"command": "sleep 1 & rm -rf /"})
                )
                .await
        );
    }

    #[tokio::test]
    async fn newline_separator_requires_all_to_match() {
        let rules = vec![ApprovalRuleConfig {
            tool: "Bash".to_string(),
            pattern: "git status\\b.*".to_string(),
        }];
        let rule_set = ApprovalRuleSet::new(&[], &rules, vec![]);

        assert!(
            !rule_set
                .matches(
                    "Bash",
                    &serde_json::json!({"command": "git status\nrm -rf /"})
                )
                .await
        );
    }

    #[tokio::test]
    async fn non_bash_tool_with_rules_still_works() {
        // Non-Bash tools skip the compound-command splitting path entirely.
        let static_rules = vec![ApprovalRuleConfig {
            tool: "Write".to_string(),
            pattern: ".*".to_string(),
        }];
        let rule_set = ApprovalRuleSet::new(&[], &static_rules, vec![]);
        assert!(
            rule_set
                .matches(
                    "Write",
                    &serde_json::json!({"file_path": "/tmp/foo.rs", "content": "hello"})
                )
                .await
        );
    }

    #[tokio::test]
    async fn operators_in_quotes_not_split() {
        // "echo 'hello && world'" is a simple command — the && is inside quotes.
        let rules = vec![ApprovalRuleConfig {
            tool: "Bash".to_string(),
            pattern: "echo\\b.*".to_string(),
        }];
        let rule_set = ApprovalRuleSet::new(&[], &rules, vec![]);

        assert!(
            rule_set
                .matches(
                    "Bash",
                    &serde_json::json!({"command": "echo 'hello && world'"})
                )
                .await
        );
    }
}

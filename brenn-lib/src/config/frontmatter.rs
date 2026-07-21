use std::collections::HashMap;

use serde::Deserialize;

/// Per-app rendering rules for YAML frontmatter blocks at the top of
/// markdown files displayed via DisplayFile / the `/file/` route.
///
/// All fields are optional in TOML. The defaults shape is "show every
/// top-level key in file order, render `tldr`/`summary` as markdown,
/// pin tldr/summary to the top, cap sequence values at 5 entries".
///
/// See `docs/designs/frontmatter-rendering.md` for the full pipeline
/// (filter → pin → cap → render).
///
/// `Default` is implemented by hand because `#[derive(Default)]` would
/// give per-field language defaults (empty vec, `0_u32`, `false`),
/// whereas the runtime semantics described above require named
/// defaults that only fire on the serde path.
#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct FrontmatterRenderConfig {
    /// Whitelist of top-level keys to render. Empty = render all.
    /// When non-empty, this also dictates render order, and `pin_lede`
    /// is suppressed (the user's `show` order is authoritative).
    pub show: Vec<String>,
    /// Blacklist of top-level keys to drop. Applied after `show`.
    pub hide: Vec<String>,
    /// Maximum entries rendered for any sequence-valued field.
    /// Excess entries collapse to a single `…and N more` row.
    /// Default: 5. A value of 0 means "render only the truncation row".
    /// `usize` because the renderer indexes into a `Vec<YamlValue>` —
    /// it would convert anyway. TOML readily parses a non-negative
    /// integer into `usize`.
    #[serde(default = "default_list_cap")]
    pub list_cap: usize,
    /// Per-key overrides for `list_cap`, keyed by top-level YAML key name.
    pub list_cap_overrides: HashMap<String, usize>,
    /// YAML keys whose scalar string values are rendered through the
    /// markdown pipeline (bold, links, etc.) rather than escaped as
    /// plain text. Non-string values for these keys fall through to the
    /// normal type-rule. Default: ["tldr", "summary"].
    #[serde(default = "default_markdown_keys")]
    pub markdown_keys: Vec<String>,
    /// When true and `show` is empty, pin `tldr` (then `summary`) to the
    /// top of the rendered list when present. Suppressed when `show` is
    /// non-empty. Default: true.
    #[serde(default = "default_true")]
    pub pin_lede: bool,
}

impl Default for FrontmatterRenderConfig {
    fn default() -> Self {
        Self {
            show: Vec::new(),
            hide: Vec::new(),
            list_cap: default_list_cap(),
            list_cap_overrides: HashMap::new(),
            markdown_keys: default_markdown_keys(),
            pin_lede: true,
        }
    }
}

fn default_list_cap() -> usize {
    5
}

fn default_markdown_keys() -> Vec<String> {
    vec!["tldr".into(), "summary".into()]
}

fn default_true() -> bool {
    true
}

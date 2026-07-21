//! YAML frontmatter rendering for displayed markdown files.
//!
//! Strips a leading `---\n…\n---\n` block from the input, parses it as YAML,
//! and renders the parsed mapping as a structured `<dl>` block ahead of the
//! body. Used by the DisplayFile tool, the artifact-snapshot replay path,
//! and the static `/file/...` route.
//!
//! See `docs/designs/frontmatter-rendering.md` for the design.
//!
//! Trust contract: this module is part of the assistant-message HTML
//! pipeline. All scalar values pass through `html_escape`. The
//! markdown-key path uses `crate::markdown::render_markdown`, which
//! strips raw HTML events. Keys are escaped just like values.
//!
//! Source of truth for `FRONTMATTER_CSS`: `brenn-lib/src/frontmatter_css.rs`.
//! Re-exported here so external callers see `brenn::frontmatter::FRONTMATTER_CSS`.
//! Edit there and `make frontend-css` regenerates the matching frontend
//! Lit template mechanically.

use brenn_lib::config::FrontmatterRenderConfig;
use brenn_lib::util::html_escape;
use serde_yaml::Value as YamlValue;

/// Threshold above which an inline scalar entry forces its sequence to
/// expand to a `<ul>`. Per design: a sequence-of-scalars only inlines
/// when *every* entry is shorter than this.
const INLINE_ENTRY_LEN: usize = 40;

/// Combined-length threshold above which an inlined sequence-of-scalars
/// expands to a `<ul>` even when each entry is short. Per design.
const INLINE_TOTAL_LEN: usize = 120;

/// Strip a leading YAML frontmatter block from `input` and render it as
/// HTML. Returns `(rendered_frontmatter_html, body_markdown)`. If no
/// frontmatter is present the first element is empty and the second is
/// `input` unchanged.
///
/// Detection (no YAML library required):
///
/// - The input must start with `---\n` or `---\r\n`.
/// - The next line that consists of exactly `---` or `...` (YAML closing
///   tokens) ends the block; everything between is the YAML payload, and
///   everything after the closing line (skipping its terminating newline)
///   is the body markdown.
/// - Anything else (no opening, no closing) → `("", input)`. Callers see
///   the original markdown.
pub fn split_and_render_frontmatter<'a>(
    input: &'a str,
    cfg: &FrontmatterRenderConfig,
) -> (String, &'a str) {
    let Some((yaml_payload, body)) = strip_frontmatter(input) else {
        return (String::new(), input);
    };

    // Empty payload (e.g. `---\n---\n`) — emit nothing, return body. We
    // do this explicitly rather than letting `serde_yaml` decide, so an
    // all-blank YAML block (`---\n   \n---\n`) also produces no chrome.
    if yaml_payload.trim().is_empty() {
        return (String::new(), body);
    }

    let html = match serde_yaml::from_str::<YamlValue>(yaml_payload) {
        Ok(YamlValue::Mapping(mapping)) => {
            if mapping.is_empty() {
                String::new()
            } else {
                render_mapping(&mapping, cfg)
            }
        }
        // Top-level YAML that isn't a mapping (e.g. a bare scalar or
        // sequence). Real graf/pfin files always use a mapping; treat
        // anything else as malformed.
        Ok(_) => render_error(yaml_payload, "frontmatter must be a YAML mapping"),
        Err(e) => render_error(yaml_payload, &e.to_string()),
    };

    (html, body)
}

/// Render raw markdown file content to HTML, splitting off any leading
/// YAML frontmatter and rendering it as a structured block ahead of the
/// body. This is the standard recipe used by every site that displays
/// raw markdown file content (DisplayFile intercept, artifact-snapshot
/// load + replay, static `/file/` route, ReopenArtifact disk path).
///
/// Equivalent to:
///
/// ```ignore
/// let (fm_html, body_md) = split_and_render_frontmatter(content, cfg);
/// format!("{fm_html}{}", crate::markdown::render_markdown(body_md))
/// ```
///
/// Callers that need access to the body markdown without the frontmatter
/// (or the frontmatter HTML on its own) should keep using
/// `split_and_render_frontmatter`; nobody currently does.
pub fn render_markdown_with_frontmatter(content: &str, cfg: &FrontmatterRenderConfig) -> String {
    let (fm_html, body_md) = split_and_render_frontmatter(content, cfg);
    format!("{fm_html}{}", crate::markdown::render_markdown(body_md))
}

/// Static CSS for the frontmatter block.
///
/// Re-exported from `brenn_lib::frontmatter_css` so `brenn-cli
/// emit-frontmatter-css` can pull from a single source for
/// `frontend/src/styles/frontmatter.generated.ts`. The route handler in
/// `routes/file.rs` interpolates this constant into its inline
/// `<style>` block.
pub use brenn_lib::frontmatter_css::FRONTMATTER_CSS;

/// Detect a leading `---\n…\n---\n` (or `...\n` close) block. Returns
/// `(yaml_payload_without_delimiters, body_markdown)`. Accepts CRLF.
fn strip_frontmatter(input: &str) -> Option<(&str, &str)> {
    // Opening delimiter is exactly `---` followed by `\n` or `\r\n`.
    let after_open = input
        .strip_prefix("---\n")
        .or_else(|| input.strip_prefix("---\r\n"))?;

    // Walk lines looking for a closing `---` or `...`. The closing line
    // must contain *only* the delimiter (no leading/trailing content).
    let mut idx = 0usize;
    while idx < after_open.len() {
        // Find the end of the current line.
        let line_end = after_open[idx..]
            .find('\n')
            .map(|n| idx + n)
            .unwrap_or(after_open.len());
        let raw_line = &after_open[idx..line_end];
        // Strip a trailing \r so CRLF detection works for closers too.
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line == "---" || line == "..." {
            let yaml_payload = &after_open[..idx];
            // Consume the closing line plus its terminating newline (if
            // present — final line of the file may have no newline).
            let after_close = if line_end < after_open.len() {
                line_end + 1 // skip the '\n'
            } else {
                after_open.len()
            };
            let body = &after_open[after_close..];
            return Some((yaml_payload, body));
        }
        // Advance past this line and its newline.
        idx = if line_end < after_open.len() {
            line_end + 1
        } else {
            return None;
        };
    }
    None
}

/// Render a parsed top-level YAML mapping as the frontmatter HTML block.
/// Caller has already verified the mapping is non-empty.
fn render_mapping(mapping: &serde_yaml::Mapping, cfg: &FrontmatterRenderConfig) -> String {
    // Coerce all keys to strings up-front; non-string keys (rare in
    // real-world YAML) get a string representation for display, but the
    // filter list still matches by string form.
    let mut entries: Vec<(String, &YamlValue)> = mapping
        .iter()
        .map(|(k, v)| (yaml_key_as_string(k), v))
        .collect();

    // Filter: show wins over file order; hide always strips.
    let pin_lede_active = cfg.pin_lede && cfg.show.is_empty();

    if !cfg.show.is_empty() {
        // Reorder to match show; silently skip keys that don't exist.
        let mut by_key: std::collections::HashMap<&str, &YamlValue> =
            std::collections::HashMap::new();
        for (k, v) in &entries {
            by_key.insert(k.as_str(), *v);
        }
        let mut reordered: Vec<(String, &YamlValue)> = Vec::new();
        for key in &cfg.show {
            if let Some(v) = by_key.get(key.as_str()) {
                reordered.push((key.clone(), *v));
            }
        }
        entries = reordered;
    }

    // Apply hide.
    if !cfg.hide.is_empty() {
        let hide_set: std::collections::HashSet<&str> =
            cfg.hide.iter().map(|s| s.as_str()).collect();
        entries.retain(|(k, _)| !hide_set.contains(k.as_str()));
    }

    // Pin lede (only when show is empty).
    if pin_lede_active {
        // Move "tldr" first, then "summary", in that relative order, only
        // for those that are actually present.
        for key in ["tldr", "summary"] {
            if let Some(pos) = entries.iter().position(|(k, _)| k == key) {
                let entry = entries.remove(pos);
                // Find insertion position: after any earlier-pinned keys.
                let insert_at = entries
                    .iter()
                    .take_while(|(k, _)| k == "tldr" || k == "summary")
                    .count();
                entries.insert(insert_at, entry);
            }
        }
    }

    if entries.is_empty() {
        return String::new();
    }

    let mut html = String::new();
    html.push_str("<aside class=\"fm-block\"><dl class=\"fm-list\">");
    for (key, value) in entries {
        let row_html = render_row(&key, value, cfg);
        html.push_str(&row_html);
    }
    html.push_str("</dl></aside>");
    html
}

/// Plain-text label for null / missing values when not wrapped in
/// markup (e.g. inline within a `key: value` line).
const NULL_TEXT: &str = "none";

/// Marked-up rendering of null / missing values for use inside an
/// HTML element where styling is desired.
const NULL_SPAN: &str = "<span class=\"fm-null\">none</span>";

/// Render one `<dt>/<dd>` pair (wrapped in `<div class="fm-row">`).
fn render_row(key: &str, value: &YamlValue, cfg: &FrontmatterRenderConfig) -> String {
    let escaped_key = html_escape(key);
    let is_markdown_key = cfg.markdown_keys.iter().any(|k| k == key);

    match value {
        YamlValue::Sequence(seq) => {
            // The list_cap lookup only matters when the value is a
            // sequence; doing it on every row would be a wasted
            // HashMap probe for the common scalar / mapping cases.
            let cap = cfg
                .list_cap_overrides
                .get(key)
                .copied()
                .unwrap_or(cfg.list_cap);
            render_sequence_row(&escaped_key, seq, cap)
        }
        YamlValue::Mapping(map) => render_mapping_row(&escaped_key, map),
        scalar => render_scalar_row(&escaped_key, scalar, is_markdown_key),
    }
}

fn render_scalar_row(escaped_key: &str, value: &YamlValue, is_markdown_key: bool) -> String {
    let dd = match value {
        YamlValue::Null => format!("<dd>{NULL_SPAN}</dd>"),
        YamlValue::Bool(b) => format!("<dd>{}</dd>", bool_text(*b)),
        YamlValue::Number(n) => format!("<dd>{}</dd>", html_escape(&n.to_string())),
        YamlValue::String(s) => {
            if is_markdown_key {
                // pulldown-cmark wraps a single line in <p>...</p>; the
                // CSS strips the margin so it sits inline-ish in the
                // grid cell.
                let rendered = crate::markdown::render_markdown(s);
                format!("<dd class=\"fm-md\">{rendered}</dd>")
            } else {
                format!("<dd>{}</dd>", html_escape(s))
            }
        }
        // Tagged or other scalar; fall back to the flow-style raw form.
        _ => render_raw_dd(value),
    };
    format!("<div class=\"fm-row\"><dt>{escaped_key}</dt>{dd}</div>")
}

fn render_sequence_row(escaped_key: &str, seq: &[YamlValue], cap: usize) -> String {
    let n = seq.len();

    // Empty sequence (`labels: []`) → `<dd>none</dd>`. Matches the
    // empty-mapping path and avoids a misleading "…0 entries" line.
    if n == 0 {
        return format!("<div class=\"fm-row\"><dt>{escaped_key}</dt><dd>{NULL_SPAN}</dd></div>",);
    }

    // Degenerate: list_cap = 0 emits a fm-truncated dd with no <ul>.
    if cap == 0 {
        return format!(
            "<div class=\"fm-row fm-row--list\"><dt>{escaped_key}</dt><dd class=\"fm-truncated\">{}</dd></div>",
            entries_summary(n),
        );
    }

    // Decide inline vs sublist. Inline rule (per design): sequence of
    // scalars where every entry is short and total joined is short.
    if let Some(joined) = try_inline_scalar_seq(seq) {
        return format!("<div class=\"fm-row\"><dt>{escaped_key}</dt><dd>{joined}</dd></div>",);
    }

    // Sublist. Keep the *last* `cap` entries (most recent for
    // append-only logs); emit `…and M more` if truncated.
    let dropped = n.saturating_sub(cap);
    let kept = &seq[dropped..];

    let mut items_html = String::new();
    if dropped > 0 {
        items_html.push_str(&format!(
            "<li class=\"fm-truncated\">…and {dropped} more</li>",
        ));
    }
    for entry in kept {
        items_html.push_str("<li>");
        items_html.push_str(&render_seq_entry(entry));
        items_html.push_str("</li>");
    }

    format!(
        "<div class=\"fm-row fm-row--list\"><dt>{escaped_key}</dt><dd><ul class=\"fm-sublist\">{items_html}</ul></dd></div>",
    )
}

/// Format a count of sequence entries for the truncation row.
/// "1 entry" / "N entries" — a singleton avoids the awkward "1 entries".
fn entries_summary(n: usize) -> String {
    if n == 1 {
        "…1 entry".to_string()
    } else {
        format!("…{n} entries")
    }
}

/// Try to inline a sequence as a comma-joined string. Returns `None`
/// if any entry is non-scalar, too long, or the total is too long.
fn try_inline_scalar_seq(seq: &[YamlValue]) -> Option<String> {
    let mut parts: Vec<String> = Vec::with_capacity(seq.len());
    for entry in seq {
        let s = yaml_scalar_to_text(entry)?;
        if s.chars().count() >= INLINE_ENTRY_LEN {
            return None;
        }
        parts.push(s);
    }
    let joined_unescaped = parts.join(", ");
    if joined_unescaped.chars().count() >= INLINE_TOTAL_LEN {
        return None;
    }
    Some(html_escape(&joined_unescaped))
}

/// Render a non-inlined sequence entry.
fn render_seq_entry(entry: &YamlValue) -> String {
    match entry {
        YamlValue::Mapping(inner) => render_inline_mapping(inner),
        YamlValue::Null => NULL_SPAN.to_string(),
        YamlValue::Bool(b) => bool_text(*b).to_string(),
        YamlValue::Number(n) => html_escape(&n.to_string()),
        YamlValue::String(s) => html_escape(s),
        YamlValue::Sequence(_) | YamlValue::Tagged(_) => render_raw_inline(entry),
    }
}

/// Render a mapping appearing inside a sequence entry (e.g. one row of
/// `completion_log`). Inner scalars become `key: value` joined with
/// `, `; nested non-scalars collapse to `[N entries]` / `{N keys}`.
///
/// Both this function and `render_mapping_row` go through
/// `inline_kv_text` for the per-(key, value) text; the only difference
/// is how the surrounding rows are joined.
fn render_inline_mapping(map: &serde_yaml::Mapping) -> String {
    map.iter()
        .map(|(k, v)| inline_kv_text(k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_mapping_row(escaped_key: &str, map: &serde_yaml::Mapping) -> String {
    if map.is_empty() {
        return format!("<div class=\"fm-row\"><dt>{escaped_key}</dt><dd>{NULL_SPAN}</dd></div>",);
    }
    let mut items_html = String::new();
    for (k, v) in map {
        items_html.push_str("<li>");
        items_html.push_str(&inline_kv_text(k, v));
        items_html.push_str("</li>");
    }
    format!(
        "<div class=\"fm-row fm-row--list\"><dt>{escaped_key}</dt><dd><ul class=\"fm-sublist\">{items_html}</ul></dd></div>",
    )
}

/// Render a single `key: value` pair for the inline-mapping form
/// (used both inside sequences-of-mappings and inside a top-level
/// mapping row's sublist). Output is HTML-escaped and ready to drop
/// into either a comma-joined run or an `<li>` wrapper.
fn inline_kv_text(k: &YamlValue, v: &YamlValue) -> String {
    let key = yaml_key_as_string(k);
    let val_text = match v {
        YamlValue::Null => NULL_TEXT.to_string(),
        YamlValue::Bool(b) => bool_text(*b).to_string(),
        YamlValue::Number(n) => n.to_string(),
        YamlValue::String(s) => s.clone(),
        YamlValue::Sequence(seq) => format!("[{} entries]", seq.len()),
        YamlValue::Mapping(m) => format!("{{{} keys}}", m.len()),
        YamlValue::Tagged(_) => "{…}".to_string(),
    };
    format!("{}: {}", html_escape(&key), html_escape(&val_text))
}

/// Map a YAML scalar to its plain-text form. Returns `None` for
/// non-scalars (sequences, mappings, tagged values), which the callers
/// treat as "force the structured form" (sublist / fm-raw).
fn yaml_scalar_to_text(v: &YamlValue) -> Option<String> {
    match v {
        YamlValue::Null => Some(NULL_TEXT.to_string()),
        YamlValue::Bool(b) => Some(bool_text(*b).to_string()),
        YamlValue::Number(n) => Some(n.to_string()),
        YamlValue::String(s) => Some(s.clone()),
        YamlValue::Sequence(_) | YamlValue::Mapping(_) | YamlValue::Tagged(_) => None,
    }
}

fn bool_text(b: bool) -> &'static str {
    if b { "true" } else { "false" }
}

/// Render a value as a single-line raw `<dd>` cell — fallback for
/// shapes the structured renderer doesn't model.
fn render_raw_dd(value: &YamlValue) -> String {
    let raw = render_raw_inline(value);
    format!("<dd class=\"fm-raw\">{}</dd>", raw)
}

/// Render a value via `serde_yaml`'s flow-style serializer for fallback
/// rendering. Already HTML-escaped.
fn render_raw_inline(value: &YamlValue) -> String {
    match serde_yaml::to_string(value) {
        Ok(s) => html_escape(&collapse_to_single_line(&s)),
        Err(_) => html_escape("<unrenderable>"),
    }
}

/// Coerce a YAML key (mapping key may be any scalar/structure) to a
/// human-readable string. Non-string keys are rare in practice; we
/// flatten to a representation good enough for display + filter
/// matching. Compound keys (sequence / mapping used as a key — legal
/// YAML) are flattened to a single line so the rendered `<dt>` doesn't
/// blow up the grid layout.
fn yaml_key_as_string(key: &YamlValue) -> String {
    match key {
        YamlValue::String(s) => s.clone(),
        YamlValue::Bool(b) => b.to_string(),
        YamlValue::Number(n) => n.to_string(),
        YamlValue::Null => "null".to_string(),
        // Compound keys: round-trip via serde_yaml, then collapse any
        // newlines (block-style YAML carries them) so the rendered
        // `<dt>` stays on one line.
        _ => serde_yaml::to_string(key)
            .map(|s| collapse_to_single_line(&s))
            .unwrap_or_else(|_| "<key>".to_string()),
    }
}

/// Trim outer whitespace and replace any interior `\n` / `\r` with a
/// single space. Used to flatten serde_yaml's multi-line flow output
/// into something safe for inline display.
fn collapse_to_single_line(s: &str) -> String {
    s.trim()
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect()
}

fn render_error(yaml_payload: &str, message: &str) -> String {
    format!(
        "<div class=\"fm-error\"><strong>Frontmatter parse error:</strong> {}<pre>{}</pre></div>",
        html_escape(message),
        html_escape(yaml_payload),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_default() -> FrontmatterRenderConfig {
        FrontmatterRenderConfig::default()
    }

    #[test]
    fn no_frontmatter_returns_empty_html_and_full_body() {
        let input = "# Just a body\n\nContent.";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert_eq!(fm, "");
        assert_eq!(body, input);
    }

    #[test]
    fn no_opening_delim_is_not_frontmatter() {
        let input = "Some prefix\n---\nfoo: bar\n---\n";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert_eq!(fm, "");
        assert_eq!(body, input);
    }

    #[test]
    fn missing_closing_delim_is_not_frontmatter() {
        let input = "---\nfoo: bar\nno closing\n";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert_eq!(fm, "");
        assert_eq!(body, input);
    }

    #[test]
    fn simple_frontmatter_renders_dl_in_file_order() {
        let input = "---\nstatus: in_progress\npriority: 2\n---\n# Body\n";
        let mut cfg = cfg_default();
        // pin_lede default is on, but neither tldr nor summary present.
        cfg.pin_lede = true;
        let (fm, body) = split_and_render_frontmatter(input, &cfg);
        assert!(fm.contains("class=\"fm-block\""), "got: {fm}");
        assert!(fm.contains("<dt>status</dt>"), "got: {fm}");
        assert!(fm.contains("<dd>in_progress</dd>"), "got: {fm}");
        assert!(fm.contains("<dt>priority</dt>"), "got: {fm}");
        assert!(fm.contains("<dd>2</dd>"), "got: {fm}");
        // File order: status before priority.
        let p_status = fm.find("<dt>status</dt>").unwrap();
        let p_priority = fm.find("<dt>priority</dt>").unwrap();
        assert!(p_status < p_priority, "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn crlf_delimiters_are_accepted() {
        let input = "---\r\nstatus: ok\r\n---\r\n# Body\r\n";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("<dt>status</dt>"), "got: {fm}");
        assert_eq!(body, "# Body\r\n");
    }

    #[test]
    fn closing_dots_delimiter_accepted() {
        let input = "---\nstatus: ok\n...\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("<dt>status</dt>"), "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn malformed_yaml_falls_back_to_fm_error_and_preserves_body() {
        // Tab indent in a flow context — `serde_yaml` rejects this.
        let input = "---\n\tnot: valid: yaml: here\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("class=\"fm-error\""), "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn top_level_scalar_yaml_is_treated_as_malformed() {
        let input = "---\njust a string\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("class=\"fm-error\""), "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn tldr_value_is_markdown_rendered() {
        let input = "---\ntldr: Buy **new** tires\n---\nbody";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("class=\"fm-md\""), "got: {fm}");
        assert!(fm.contains("<strong>new</strong>"), "got: {fm}");
    }

    #[test]
    fn non_markdown_key_value_is_escaped() {
        // `status` is not in markdown_keys — angle brackets must escape.
        let input = "---\nstatus: \"<script>alert(1)</script>\"\n---\nbody";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(!fm.contains("<script>"), "raw script in: {fm}");
        assert!(fm.contains("&lt;script&gt;"), "expected escaped: {fm}");
    }

    #[test]
    fn sequence_of_scalars_inlines_when_short() {
        let input = "---\nlabels: [errands, car]\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("<dd>errands, car</dd>"), "got: {fm}");
        // Should not have the sublist wrapper.
        assert!(!fm.contains("fm-sublist"), "got: {fm}");
    }

    #[test]
    fn sequence_of_scalars_becomes_sublist_when_long() {
        // Make total length exceed the inline threshold (120 chars).
        let entries: Vec<String> = (0..20)
            .map(|i| format!("\"label_with_some_length_{i:02}\""))
            .collect();
        let input = format!("---\nlabels: [{}]\n---\n", entries.join(", "));
        let (fm, _) = split_and_render_frontmatter(&input, &cfg_default());
        assert!(
            fm.contains("fm-sublist"),
            "expected sublist for long seq: {fm}"
        );
    }

    #[test]
    fn sequence_of_mappings_renders_one_li_per_entry() {
        let input = "---\nlog:\n  - completed: 2025-09-04\n    occurrence: 2025-09-01\n  - completed: 2025-08-04\n    occurrence: 2025-08-01\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("fm-sublist"), "got: {fm}");
        assert!(fm.contains("completed: 2025-09-04"), "got: {fm}");
        assert!(fm.contains("occurrence: 2025-09-01"), "got: {fm}");
    }

    #[test]
    fn completion_log_truncates_to_last_n_with_summary_row() {
        let mut yaml = String::from("---\ncompletion_log:\n");
        for i in 0..10 {
            yaml.push_str(&format!(
                "  - completed: 2025-{:02}-04\n    occurrence: 2025-{:02}-01\n",
                i + 1,
                i + 1,
            ));
        }
        yaml.push_str("---\n");
        let (fm, _) = split_and_render_frontmatter(&yaml, &cfg_default());
        // Default cap = 5 → 10 - 5 = 5 dropped.
        assert!(fm.contains("…and 5 more"), "got: {fm}");
        // Last entry is October (10th).
        assert!(fm.contains("2025-10-04"), "got: {fm}");
        // First entry (January) is dropped.
        assert!(!fm.contains("2025-01-04"), "got: {fm}");
    }

    #[test]
    fn list_cap_override_per_key_overrides_default() {
        let mut cfg = cfg_default();
        cfg.list_cap = 5;
        cfg.list_cap_overrides.insert("labels".into(), 2);
        // Force a sublist by using long entries that won't inline.
        let input = "---\nlabels:\n  - one_label_that_is_kind_of_long_actually_yes\n  - two_label_that_is_kind_of_long_actually_yes\n  - three_label_that_is_kind_of_long_actually\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg);
        assert!(fm.contains("…and 1 more"), "got: {fm}");
    }

    #[test]
    fn show_filter_restricts_keys_and_orders_them() {
        let mut cfg = cfg_default();
        cfg.show = vec!["priority".into(), "status".into()];
        let input = "---\nstatus: ok\npriority: 2\nother: hidden\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg);
        assert!(!fm.contains(">other<"), "should not show 'other': {fm}");
        let p_priority = fm.find("<dt>priority</dt>").expect("priority present");
        let p_status = fm.find("<dt>status</dt>").expect("status present");
        assert!(
            p_priority < p_status,
            "show ordering should put priority first: {fm}"
        );
    }

    #[test]
    fn hide_filter_drops_keys() {
        let mut cfg = cfg_default();
        cfg.hide = vec!["secret".into()];
        let input = "---\nstatus: ok\nsecret: shh\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg);
        assert!(fm.contains("<dt>status</dt>"), "got: {fm}");
        assert!(!fm.contains("secret"), "got: {fm}");
        assert!(!fm.contains("shh"), "got: {fm}");
    }

    #[test]
    fn pin_lede_moves_tldr_and_summary_to_front() {
        let input = "---\nstatus: ok\ntldr: a one-liner\nsummary: longer text\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        let p_tldr = fm.find("<dt>tldr</dt>").expect("tldr present");
        let p_summary = fm.find("<dt>summary</dt>").expect("summary present");
        let p_status = fm.find("<dt>status</dt>").expect("status present");
        assert!(p_tldr < p_summary && p_summary < p_status, "got: {fm}");
    }

    #[test]
    fn pin_lede_pins_only_tldr_when_summary_absent() {
        let input = "---\nstatus: ok\ntldr: a one-liner\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        let p_tldr = fm.find("<dt>tldr</dt>").unwrap();
        let p_status = fm.find("<dt>status</dt>").unwrap();
        assert!(p_tldr < p_status, "got: {fm}");
    }

    #[test]
    fn pin_lede_pins_only_summary_when_tldr_absent() {
        let input = "---\nstatus: ok\nsummary: text\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        let p_summary = fm.find("<dt>summary</dt>").unwrap();
        let p_status = fm.find("<dt>status</dt>").unwrap();
        assert!(p_summary < p_status, "got: {fm}");
    }

    #[test]
    fn pin_lede_off_preserves_file_order() {
        let mut cfg = cfg_default();
        cfg.pin_lede = false;
        let input = "---\nstatus: ok\ntldr: a one-liner\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg);
        let p_status = fm.find("<dt>status</dt>").unwrap();
        let p_tldr = fm.find("<dt>tldr</dt>").unwrap();
        assert!(p_status < p_tldr, "got: {fm}");
    }

    #[test]
    fn null_value_renders_as_fm_null_span() {
        let input = "---\ndue_date: ~\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("class=\"fm-null\""), "got: {fm}");
        assert!(fm.contains(">none<"), "got: {fm}");
    }

    #[test]
    fn nested_mapping_renders_one_level_sublist() {
        let input = "---\nrecurrence:\n  rrule: FREQ=DAILY\n  lead_days: 3\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("fm-sublist"), "got: {fm}");
        assert!(fm.contains("rrule: FREQ=DAILY"), "got: {fm}");
        assert!(fm.contains("lead_days: 3"), "got: {fm}");
    }

    #[test]
    fn deeply_nested_mapping_summarised_as_key_count() {
        // A mapping whose value is itself a mapping containing a mapping
        // (3 levels deep) should collapse the inner mapping to `{N keys}`.
        // This is the structural-summary fallback, not the `fm-raw`
        // flow-style fallback — see `tagged_value_falls_back_to_fm_raw`
        // for that path.
        let input = "---\nouter:\n  inner:\n    deep1: a\n    deep2: b\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        // `outer` is rendered as a sublist; its `inner` value is a
        // mapping that gets summarised.
        assert!(fm.contains("inner: {2 keys}"), "got: {fm}");
        // Cross-check: the structured-summary path is not the fm-raw
        // path. If that ever changes, the test name above lies.
        assert!(!fm.contains("class=\"fm-raw\""), "got: {fm}");
    }

    #[test]
    fn tagged_value_falls_back_to_fm_raw() {
        // A YAML value with an explicit tag (legal but unusual)
        // exercises the `fm-raw` flow-style fallback at the scalar-row
        // level.
        let input = "---\nkind: !mytag bar\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("class=\"fm-raw\""), "got: {fm}");
        assert!(fm.contains("bar"), "value should be present: {fm}");
    }

    #[test]
    fn xss_in_value_is_escaped() {
        let input = "---\nfoo: \"<img onerror=alert(1)>\"\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(!fm.contains("<img"), "got: {fm}");
        assert!(fm.contains("&lt;img"), "got: {fm}");
    }

    #[test]
    fn xss_in_key_is_escaped() {
        // Quote a YAML key containing HTML metacharacters.
        let input = "---\n\"<img onerror=alert(1)>\": value\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(fm.contains("&lt;img"), "got: {fm}");
        // Make sure no raw `<img` slips through in any form.
        assert!(!fm.contains("<img onerror"), "got: {fm}");
    }

    #[test]
    fn xss_in_summary_markdown_is_stripped() {
        let input = "---\nsummary: \"<script>alert(1)</script>\"\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg_default());
        assert!(!fm.contains("<script>"), "got: {fm}");
        // `render_markdown` strips raw HTML events entirely (not even
        // escaped — see markdown.rs:85).
        assert!(!fm.contains("alert(1)"), "got: {fm}");
    }

    #[test]
    fn empty_frontmatter_emits_empty_string() {
        let input = "---\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert_eq!(fm, "", "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn pin_lede_suppressed_when_show_is_non_empty() {
        let mut cfg = cfg_default();
        cfg.show = vec!["status".into(), "tldr".into()];
        let input = "---\nstatus: ok\ntldr: lede\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg);
        // show order: status before tldr → tldr should NOT be pinned.
        let p_status = fm.find("<dt>status</dt>").unwrap();
        let p_tldr = fm.find("<dt>tldr</dt>").unwrap();
        assert!(p_status < p_tldr, "got: {fm}");
    }

    #[test]
    fn markdown_keys_with_non_string_value_falls_through() {
        let mut cfg = cfg_default();
        cfg.markdown_keys.push("labels".into());
        let input = "---\nlabels: [a, b]\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg);
        // Should render as a (possibly inlined) sequence, not as fm-md.
        assert!(!fm.contains("class=\"fm-md\""), "got: {fm}");
        assert!(fm.contains("<dd>a, b</dd>"), "got: {fm}");
    }

    #[test]
    fn list_cap_zero_emits_truncation_dd_no_ul() {
        let mut cfg = cfg_default();
        cfg.list_cap = 0;
        let input = "---\nlog:\n  - one\n  - two\n  - three\n---\n";
        let (fm, _) = split_and_render_frontmatter(input, &cfg);
        assert!(
            fm.contains("class=\"fm-truncated\""),
            "expected fm-truncated dd: {fm}"
        );
        assert!(fm.contains("…3 entries"), "got: {fm}");
        assert!(!fm.contains("<ul"), "should be no <ul>: {fm}");
    }

    #[test]
    fn empty_input_returns_empty_html_and_empty_body() {
        let input = "";
        let (fm, body) = split_and_render_frontmatter(input, &cfg_default());
        assert_eq!(fm, "");
        assert_eq!(body, "");
    }
}

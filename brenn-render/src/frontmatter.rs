//! YAML frontmatter rendering for displayed markdown files.
//!
//! Strips a leading `---\n…\n---\n` block from the input, parses it as YAML,
//! and renders the parsed mapping as a structured `<dl>` block ahead of the
//! body. Used by the DisplayFile tool, the artifact-snapshot replay path,
//! and the static `/file/...` route.
//!
//! Trust contract: this module is part of the assistant-message HTML
//! pipeline. All scalar values pass through `html_escape`. The
//! markdown-key path uses `crate::markdown::render_markdown`, which
//! strips raw HTML events. Keys are escaped just like values.
//!
//! Source of truth for `FRONTMATTER_CSS`: `brenn-lib/src/frontmatter_css.rs`.
//! Re-exported here so external callers see `brenn::frontmatter::FRONTMATTER_CSS`.
//! Edit there and the build regenerates the matching frontend Lit template
//! mechanically.

use brenn_lib::config::FrontmatterRenderConfig;
use brenn_lib::util::html_escape;
use saphyr::{Mapping, Scalar, ScalarStyle, Yaml, YamlEmitter, YamlLoader};
use saphyr_parser::{Event, Parser, Span, SpannedEventReceiver, Tag};
use std::borrow::Cow;
use tracing::warn;

/// Threshold above which an inline scalar entry forces its sequence to
/// expand to a `<ul>`. A sequence-of-scalars only inlines when
/// *every* entry is shorter than this.
const INLINE_ENTRY_LEN: usize = 40;

/// Combined-length threshold above which an inlined sequence-of-scalars
/// expands to a `<ul>` even when each entry is short.
const INLINE_TOTAL_LEN: usize = 120;

/// Deepest a payload may nest sequences and mappings. Every walk of the
/// loaded tree (loader, `resolve`, renderer, emitter, drop) recurses once per
/// level.
const MAX_DEPTH: usize = 128;

/// Most nodes a payload may hold, one per scalar or collection event.
const MAX_NODES: usize = 100_000;

/// Most bytes of scalar and tag text a payload may hold, `%TAG` handles at
/// expanded length. The bound holds for input of any size; the parser's own
/// copy of the one event that crosses it is at most the input's length. A
/// payload without `%TAG` directives holds at most its own length, so 4 MiB
/// admits any such payload under the 1 MiB file cap
/// (`artifact::MAX_FILE_SIZE`).
const MAX_SCALAR_BYTES: usize = 4 * 1024 * 1024;

const UNPARSEABLE: &str = "frontmatter contains an unparseable value";

const ANCHORS_UNSUPPORTED: &str =
    "frontmatter uses YAML anchors or aliases, which are not supported";

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
///
/// `path` names the file `input` came from; it is used only to attribute
/// the warning logged when the payload exceeds a resource budget.
pub fn split_and_render_frontmatter<'a>(
    path: &str,
    input: &'a str,
    cfg: &FrontmatterRenderConfig,
) -> (String, &'a str) {
    let Some((yaml_payload, body)) = strip_frontmatter(input) else {
        return (String::new(), input);
    };

    // Empty payload (e.g. `---\n---\n`) — emit nothing, return body. We
    // do this explicitly rather than letting the YAML parser decide, so an
    // all-blank YAML block (`---\n   \n---\n`) also produces no chrome.
    if yaml_payload.trim().is_empty() {
        return (String::new(), body);
    }

    let docs = match load_payload(path, yaml_payload) {
        Ok(docs) => docs,
        Err(message) => return (render_error(yaml_payload, &message), body),
    };

    let html = match docs.as_slice() {
        [Yaml::Mapping(mapping)] => {
            if mapping.is_empty() {
                String::new()
            } else {
                render_mapping(mapping, cfg)
            }
        }
        // Top-level YAML that isn't a mapping (a bare scalar or sequence),
        // a comments-only payload (zero documents), or a payload holding
        // more than one document. Real graf/pfin files always use a single
        // mapping; treat anything else as malformed.
        _ => render_error(yaml_payload, "frontmatter must be a YAML mapping"),
    };

    (html, body)
}

/// Parse `payload` into resolved documents (see `resolve`), or the message
/// for an `fm-error` block. A budget refusal is logged with `path`; a parse
/// error is not logged.
fn load_payload<'p>(path: &str, payload: &'p str) -> Result<Vec<Yaml<'p>>, String> {
    let mut recv = BoundedLoader::new();
    // Iterate events rather than `Parser::load`, which recurses once per
    // nesting level.
    for event in Parser::new_from_str(payload) {
        let (ev, span) = event.map_err(|e| e.to_string())?;
        recv.on_event(ev, span);
        match recv.refused {
            None => {}
            Some(Refusal::Unsupported(message)) => return Err(message.to_string()),
            Some(Refusal::Budget { budget, message }) => {
                warn!(
                    path,
                    budget,
                    nodes = recv.total.nodes,
                    bytes = recv.total.bytes,
                    depth = recv.depth,
                    "frontmatter refused: resource budget exceeded"
                );
                return Err(message);
            }
        }
    }
    if let Some(e) = recv.loader.error() {
        return Err(e.to_string());
    }
    recv.loader
        .into_documents()
        .into_iter()
        .map(resolve)
        .collect()
}

/// What a node materialises: node count and bytes of scalar and tag text.
#[derive(Clone, Copy, Default)]
struct Cost {
    nodes: usize,
    bytes: usize,
}

impl std::ops::AddAssign for Cost {
    fn add_assign(&mut self, other: Self) {
        self.nodes += other.nodes;
        self.bytes += other.bytes;
    }
}

/// Bytes of a tag's text as the parser hands it over, `%TAG` handle expanded.
fn tag_len(tag: &Option<Cow<'_, Tag>>) -> usize {
    tag.as_ref().map_or(0, |t| t.handle.len() + t.suffix.len())
}

/// Why `BoundedLoader` refused the payload.
enum Refusal {
    /// A resource budget tripped; `load_payload` logs it under `budget`.
    Budget {
        budget: &'static str,
        message: String,
    },
    /// Syntax the renderer does not support; reported like a parse error,
    /// not logged.
    Unsupported(&'static str),
}

/// Event receiver that feeds a `YamlLoader` with scalar resolution off.
/// It refuses anchors and aliases, so the loader never clones a node and
/// the loaded tree is the event stream, once. It refuses past `MAX_DEPTH`,
/// `MAX_NODES` and `MAX_SCALAR_BYTES`, counted before any event reaches the
/// loader. saphyr has none of these bounds; its event iterator is not
/// recursive, so the refusal comes before anything recursive sees an
/// over-limit tree.
struct BoundedLoader<'input> {
    loader: YamlLoader<'input, Yaml<'input>>,
    /// Sequences and mappings currently open.
    depth: usize,
    total: Cost,
    refused: Option<Refusal>,
}

impl BoundedLoader<'_> {
    fn new() -> Self {
        let mut loader = YamlLoader::default();
        loader.early_parse(false);
        Self {
            loader,
            depth: 0,
            total: Cost::default(),
            refused: None,
        }
    }

    fn refuse_budget(&mut self, budget: &'static str, message: String) {
        self.refused = Some(Refusal::Budget { budget, message });
    }
}

impl<'input> SpannedEventReceiver<'input> for BoundedLoader<'input> {
    fn on_event(&mut self, ev: Event<'input>, span: Span) {
        if self.refused.is_some() {
            return;
        }
        match &ev {
            Event::SequenceStart(anchor, _)
            | Event::MappingStart(anchor, _)
            | Event::Scalar(_, _, anchor, _)
                if *anchor > 0 =>
            {
                self.refused = Some(Refusal::Unsupported(ANCHORS_UNSUPPORTED));
                return;
            }
            // Unreachable while the parser rejects an alias to an
            // unregistered name and emits the anchored node's event, refused
            // above, before any alias to it; kept so a parser upgrade cannot
            // admit an alias.
            Event::Alias(_) => {
                self.refused = Some(Refusal::Unsupported(ANCHORS_UNSUPPORTED));
                return;
            }
            Event::SequenceStart(_, tag) | Event::MappingStart(_, tag) => {
                if self.depth == MAX_DEPTH {
                    self.refuse_budget(
                        "depth",
                        format!("frontmatter nests deeper than {MAX_DEPTH} levels"),
                    );
                    return;
                }
                self.depth += 1;
                self.total += Cost {
                    nodes: 1,
                    bytes: tag_len(tag),
                };
            }
            Event::SequenceEnd | Event::MappingEnd => {
                self.depth = self
                    .depth
                    .checked_sub(1)
                    .expect("the parser closes only collections it opened");
            }
            Event::Scalar(text, _, _, tag) => {
                self.total += Cost {
                    nodes: 1,
                    bytes: text.len() + tag_len(tag),
                };
            }
            Event::Nothing
            | Event::StreamStart
            | Event::StreamEnd
            | Event::DocumentStart(_)
            | Event::DocumentEnd => {}
        }
        if self.total.nodes > MAX_NODES {
            self.refuse_budget(
                "nodes",
                format!("frontmatter expands to more than {MAX_NODES} nodes"),
            );
            return;
        }
        if self.total.bytes > MAX_SCALAR_BYTES {
            self.refuse_budget(
                "bytes",
                format!("frontmatter expands to more than {MAX_SCALAR_BYTES} bytes of text"),
            );
            return;
        }
        self.loader.on_event(ev, span);
    }
}

/// Resolve the loader's raw scalars (`Yaml::Representation`) into the tree
/// the renderer walks. Null, booleans and strings become `Yaml::Value`; a
/// number stays a plain `Representation` of its source text, so it displays
/// as written (`012`, `18446744073709551615`, `1e400`); a scalar with a
/// non-core tag is wrapped in `Yaml::Tagged`. Errors on a core-schema tag
/// whose content does not resolve (`!!int abc`) and on two keys of one
/// mapping that resolve equal (`a` and `'a'`).
fn resolve(node: Yaml<'_>) -> Result<Yaml<'_>, String> {
    match node {
        Yaml::Representation(text, style, tag) => {
            match Yaml::value_from_cow_and_metadata(text.clone(), style, tag.as_ref()) {
                Yaml::BadValue => Err(UNPARSEABLE.to_string()),
                Yaml::Tagged(tag, inner) => {
                    let inner = if is_number(&inner) {
                        number_text(text)
                    } else {
                        *inner
                    };
                    Ok(Yaml::Tagged(tag, Box::new(inner)))
                }
                resolved if is_number(&resolved) => Ok(number_text(text)),
                resolved => Ok(resolved),
            }
        }
        Yaml::Sequence(seq) => seq
            .into_iter()
            .map(resolve)
            .collect::<Result<_, _>>()
            .map(Yaml::Sequence),
        Yaml::Mapping(map) => {
            let mut out = Mapping::new();
            for (k, v) in map {
                if out.insert(resolve(k)?, resolve(v)?).is_some() {
                    return Err("frontmatter has a duplicate key".to_string());
                }
            }
            Ok(Yaml::Mapping(out))
        }
        Yaml::Tagged(tag, inner) => Ok(Yaml::Tagged(tag, Box::new(resolve(*inner)?))),
        // The loader yields `BadValue` for an empty document.
        Yaml::BadValue => Err(UNPARSEABLE.to_string()),
        Yaml::Value(_) | Yaml::Alias(_) => {
            unreachable!("a loader without early parsing emits no Value or Alias nodes")
        }
    }
}

fn is_number(node: &Yaml<'_>) -> bool {
    matches!(
        node,
        Yaml::Value(Scalar::Integer(_) | Scalar::FloatingPoint(_))
    )
}

/// A number's node: its source text as a plain scalar, which the renderer
/// displays and the emitter writes verbatim.
fn number_text(text: Cow<'_, str>) -> Yaml<'_> {
    Yaml::Representation(text, ScalarStyle::Plain, None)
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
/// let (fm_html, body_md) = split_and_render_frontmatter(path, content, cfg);
/// format!("{fm_html}{}", crate::markdown::render_markdown(body_md))
/// ```
///
/// Callers that need access to the body markdown without the frontmatter
/// (or the frontmatter HTML on its own) should keep using
/// `split_and_render_frontmatter`; nobody currently does.
pub fn render_markdown_with_frontmatter(
    path: &str,
    content: &str,
    cfg: &FrontmatterRenderConfig,
) -> String {
    let (fm_html, body_md) = split_and_render_frontmatter(path, content, cfg);
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
fn render_mapping(mapping: &Mapping<'_>, cfg: &FrontmatterRenderConfig) -> String {
    // Coerce all keys to strings up-front; non-string keys (rare in
    // real-world YAML) get a string representation for display, but the
    // filter list still matches by string form.
    let mut entries: Vec<(String, &Yaml<'_>)> = mapping
        .iter()
        .map(|(k, v)| (yaml_key_as_string(k), v))
        .collect();

    // Filter: show wins over file order; hide always strips.
    let pin_lede_active = cfg.pin_lede && cfg.show.is_empty();

    if !cfg.show.is_empty() {
        // Reorder to match show; silently skip keys that don't exist.
        let mut by_key: std::collections::HashMap<&str, &Yaml<'_>> =
            std::collections::HashMap::new();
        for (k, v) in &entries {
            by_key.insert(k.as_str(), *v);
        }
        let mut reordered: Vec<(String, &Yaml<'_>)> = Vec::new();
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
fn render_row(key: &str, value: &Yaml<'_>, cfg: &FrontmatterRenderConfig) -> String {
    let escaped_key = html_escape(key);
    let is_markdown_key = cfg.markdown_keys.iter().any(|k| k == key);

    match value {
        Yaml::Sequence(seq) => {
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
        Yaml::Mapping(map) => render_mapping_row(&escaped_key, map),
        scalar => render_scalar_row(&escaped_key, scalar, is_markdown_key),
    }
}

fn render_scalar_row(escaped_key: &str, value: &Yaml<'_>, is_markdown_key: bool) -> String {
    let dd = match value {
        Yaml::Value(Scalar::Null) => format!("<dd>{NULL_SPAN}</dd>"),
        Yaml::Value(Scalar::String(s)) => {
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
        // Other scalars; a tagged or other node falls back to the
        // flow-style raw form.
        _ => match yaml_scalar_to_text(value) {
            Some(text) => format!("<dd>{}</dd>", html_escape(&text)),
            None => render_raw_dd(value),
        },
    };
    format!("<div class=\"fm-row\"><dt>{escaped_key}</dt>{dd}</div>")
}

fn render_sequence_row(escaped_key: &str, seq: &[Yaml<'_>], cap: usize) -> String {
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

    // Decide inline vs sublist. Inline rule: sequence of
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
fn try_inline_scalar_seq(seq: &[Yaml<'_>]) -> Option<String> {
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
fn render_seq_entry(entry: &Yaml<'_>) -> String {
    match entry {
        Yaml::Mapping(inner) => render_inline_mapping(inner),
        Yaml::Value(Scalar::Null) => NULL_SPAN.to_string(),
        _ => match yaml_scalar_to_text(entry) {
            Some(text) => html_escape(&text),
            None => render_raw_inline(entry),
        },
    }
}

/// Render a mapping appearing inside a sequence entry (e.g. one row of
/// `completion_log`). Inner scalars become `key: value` joined with
/// `, `; nested non-scalars collapse to `[N entries]` / `{N keys}`.
///
/// Both this function and `render_mapping_row` go through
/// `inline_kv_text` for the per-(key, value) text; the only difference
/// is how the surrounding rows are joined.
fn render_inline_mapping(map: &Mapping<'_>) -> String {
    map.iter()
        .map(|(k, v)| inline_kv_text(k, v))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_mapping_row(escaped_key: &str, map: &Mapping<'_>) -> String {
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
fn inline_kv_text(k: &Yaml<'_>, v: &Yaml<'_>) -> String {
    let key = yaml_key_as_string(k);
    let val_text = match v {
        Yaml::Sequence(seq) => format!("[{} entries]", seq.len()),
        Yaml::Mapping(m) => format!("{{{} keys}}", m.len()),
        _ => yaml_scalar_to_text(v).unwrap_or_else(|| "{…}".to_string()),
    };
    format!("{}: {}", html_escape(&key), html_escape(&val_text))
}

/// Map a resolved YAML scalar to its plain-text form: null is
/// `NULL_TEXT`, a number is its source text. Returns `None` for
/// non-scalars (sequences, mappings, tagged values), which the callers
/// treat as "force the structured form" (sublist / fm-raw).
fn yaml_scalar_to_text(v: &Yaml<'_>) -> Option<String> {
    match v {
        Yaml::Value(Scalar::Null) => Some(NULL_TEXT.to_string()),
        Yaml::Value(Scalar::Boolean(b)) => Some(b.to_string()),
        Yaml::Value(Scalar::String(s)) => Some(s.to_string()),
        Yaml::Representation(text, ..) => Some(text.to_string()),
        Yaml::Value(Scalar::Integer(_) | Scalar::FloatingPoint(_)) => {
            unreachable!("`resolve` keeps numbers as their source text")
        }
        _ => None,
    }
}

/// Render a value as a single-line raw `<dd>` cell — fallback for
/// shapes the structured renderer doesn't model.
fn render_raw_dd(value: &Yaml<'_>) -> String {
    let raw = render_raw_inline(value);
    format!("<dd class=\"fm-raw\">{}</dd>", raw)
}

/// Render a value via `saphyr`'s emitter, flattened to one line, for
/// fallback rendering. Already HTML-escaped.
fn render_raw_inline(value: &Yaml<'_>) -> String {
    match emit_single_line(value) {
        Ok(s) => html_escape(&s),
        Err(_) => html_escape("<unrenderable>"),
    }
}

/// Emit `node` with `saphyr`'s `YamlEmitter`, drop the `---` document-start
/// line it always writes first, and collapse the rest to one line.
fn emit_single_line(node: &Yaml<'_>) -> Result<String, saphyr::EmitError> {
    let mut out = String::new();
    YamlEmitter::new(&mut out).dump(node)?;
    let body = out
        .strip_prefix("---\n")
        .expect("YamlEmitter::dump writes a document-start line first");
    Ok(collapse_to_single_line(body))
}

/// Coerce a YAML key (mapping key may be any scalar/structure) to a
/// human-readable string. Non-string keys are rare in practice; we
/// flatten to a representation good enough for display + filter
/// matching. Compound keys (sequence / mapping used as a key — legal
/// YAML) are flattened to a single line so the rendered `<dt>` doesn't
/// blow up the grid layout.
fn yaml_key_as_string(key: &Yaml<'_>) -> String {
    match key {
        Yaml::Value(Scalar::Null) => "null".to_string(),
        // Compound keys: emit via saphyr, then collapse any newlines
        // (block-style YAML carries them) so the rendered `<dt>` stays on
        // one line.
        _ => yaml_scalar_to_text(key)
            .unwrap_or_else(|| emit_single_line(key).unwrap_or_else(|_| "<key>".to_string())),
    }
}

/// Trim outer whitespace and replace any interior `\n` / `\r` with a
/// single space. Used to flatten saphyr's multi-line block output into
/// something safe for inline display.
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
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        /// Bytes requested from the allocator on this thread.
        static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    }

    /// Forwards to `System`, adding each `alloc` to `ALLOCATED`. `realloc`
    /// and `alloc_zeroed` keep the trait defaults, which go through `alloc`,
    /// so buffer growth is counted too.
    struct CountingAlloc;

    // SAFETY: every call is forwarded to `System` unchanged. `ALLOCATED` is
    // const-initialised with no destructor, so touching it never allocates.
    unsafe impl GlobalAlloc for CountingAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCATED.with(|n| n.set(n.get() + layout.size()));
            // SAFETY: the caller's contract for `alloc` is `System.alloc`'s.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: `ptr` came from `System.alloc` with this `layout`.
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    #[global_allocator]
    static COUNTING_ALLOC: CountingAlloc = CountingAlloc;

    /// Run `f`, returning its result and the bytes this thread requested
    /// from the allocator meanwhile.
    fn allocated_during<T>(f: impl FnOnce() -> T) -> (T, usize) {
        let before = ALLOCATED.with(Cell::get);
        let out = f();
        (out, ALLOCATED.with(Cell::get) - before)
    }

    /// Ceiling on what a refused hazard render may allocate. Refused renders
    /// request 5-16 MiB; the same payloads unbounded request 1 GB and up.
    const HAZARD_ALLOC_BOUND: usize = 64 * 1024 * 1024;

    /// Render `input`, asserting the render stays under `HAZARD_ALLOC_BOUND`.
    fn render_hazard(input: &str) -> (String, &str) {
        let (out, bytes) =
            allocated_during(|| split_and_render_frontmatter(PATH, input, &cfg_default()));
        assert!(bytes < HAZARD_ALLOC_BOUND, "render allocated {bytes} bytes");
        out
    }

    const PATH: &str = "notes/alice.md";

    fn cfg_default() -> FrontmatterRenderConfig {
        FrontmatterRenderConfig::default()
    }

    #[test]
    fn no_frontmatter_returns_empty_html_and_full_body() {
        let input = "# Just a body\n\nContent.";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert_eq!(fm, "");
        assert_eq!(body, input);
    }

    #[test]
    fn no_opening_delim_is_not_frontmatter() {
        let input = "Some prefix\n---\nfoo: bar\n---\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert_eq!(fm, "");
        assert_eq!(body, input);
    }

    #[test]
    fn missing_closing_delim_is_not_frontmatter() {
        let input = "---\nfoo: bar\nno closing\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert_eq!(fm, "");
        assert_eq!(body, input);
    }

    #[test]
    fn simple_frontmatter_renders_dl_in_file_order() {
        let input = "---\nstatus: in_progress\npriority: 2\n---\n# Body\n";
        let mut cfg = cfg_default();
        // pin_lede default is on, but neither tldr nor summary present.
        cfg.pin_lede = true;
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg);
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
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("<dt>status</dt>"), "got: {fm}");
        assert_eq!(body, "# Body\r\n");
    }

    #[test]
    fn closing_dots_delimiter_accepted() {
        let input = "---\nstatus: ok\n...\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("<dt>status</dt>"), "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn malformed_yaml_falls_back_to_fm_error_and_preserves_body() {
        // Tab indent in a flow context — `saphyr` rejects this.
        let input = "---\n\tnot: valid: yaml: here\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-error\""), "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn top_level_scalar_yaml_is_treated_as_malformed() {
        let input = "---\njust a string\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-error\""), "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn tldr_value_is_markdown_rendered() {
        let input = "---\ntldr: Buy **new** tires\n---\nbody";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-md\""), "got: {fm}");
        assert!(fm.contains("<strong>new</strong>"), "got: {fm}");
    }

    #[test]
    fn non_markdown_key_value_is_escaped() {
        // `status` is not in markdown_keys — angle brackets must escape.
        let input = "---\nstatus: \"<script>alert(1)</script>\"\n---\nbody";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(!fm.contains("<script>"), "raw script in: {fm}");
        assert!(fm.contains("&lt;script&gt;"), "expected escaped: {fm}");
    }

    #[test]
    fn sequence_of_scalars_inlines_when_short() {
        let input = "---\nlabels: [errands, car]\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
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
        let (fm, _) = split_and_render_frontmatter(PATH, &input, &cfg_default());
        assert!(
            fm.contains("fm-sublist"),
            "expected sublist for long seq: {fm}"
        );
    }

    #[test]
    fn sequence_of_mappings_renders_one_li_per_entry() {
        let input = "---\nlog:\n  - completed: 2025-09-04\n    occurrence: 2025-09-01\n  - completed: 2025-08-04\n    occurrence: 2025-08-01\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
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
        let (fm, _) = split_and_render_frontmatter(PATH, &yaml, &cfg_default());
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
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg);
        assert!(fm.contains("…and 1 more"), "got: {fm}");
    }

    #[test]
    fn show_filter_restricts_keys_and_orders_them() {
        let mut cfg = cfg_default();
        cfg.show = vec!["priority".into(), "status".into()];
        let input = "---\nstatus: ok\npriority: 2\nother: hidden\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg);
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
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg);
        assert!(fm.contains("<dt>status</dt>"), "got: {fm}");
        assert!(!fm.contains("secret"), "got: {fm}");
        assert!(!fm.contains("shh"), "got: {fm}");
    }

    #[test]
    fn pin_lede_moves_tldr_and_summary_to_front() {
        let input = "---\nstatus: ok\ntldr: a one-liner\nsummary: longer text\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        let p_tldr = fm.find("<dt>tldr</dt>").expect("tldr present");
        let p_summary = fm.find("<dt>summary</dt>").expect("summary present");
        let p_status = fm.find("<dt>status</dt>").expect("status present");
        assert!(p_tldr < p_summary && p_summary < p_status, "got: {fm}");
    }

    #[test]
    fn pin_lede_pins_only_tldr_when_summary_absent() {
        let input = "---\nstatus: ok\ntldr: a one-liner\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        let p_tldr = fm.find("<dt>tldr</dt>").unwrap();
        let p_status = fm.find("<dt>status</dt>").unwrap();
        assert!(p_tldr < p_status, "got: {fm}");
    }

    #[test]
    fn pin_lede_pins_only_summary_when_tldr_absent() {
        let input = "---\nstatus: ok\nsummary: text\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        let p_summary = fm.find("<dt>summary</dt>").unwrap();
        let p_status = fm.find("<dt>status</dt>").unwrap();
        assert!(p_summary < p_status, "got: {fm}");
    }

    #[test]
    fn pin_lede_off_preserves_file_order() {
        let mut cfg = cfg_default();
        cfg.pin_lede = false;
        let input = "---\nstatus: ok\ntldr: a one-liner\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg);
        let p_status = fm.find("<dt>status</dt>").unwrap();
        let p_tldr = fm.find("<dt>tldr</dt>").unwrap();
        assert!(p_status < p_tldr, "got: {fm}");
    }

    #[test]
    fn null_value_renders_as_fm_null_span() {
        let input = "---\ndue_date: ~\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-null\""), "got: {fm}");
        assert!(fm.contains(">none<"), "got: {fm}");
    }

    #[test]
    fn nested_mapping_renders_one_level_sublist() {
        let input = "---\nrecurrence:\n  rrule: FREQ=DAILY\n  lead_days: 3\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
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
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
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
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-raw\""), "got: {fm}");
        assert!(fm.contains("bar"), "value should be present: {fm}");
        assert!(
            fm.contains("<dd class=\"fm-raw\">!mytag bar</dd>"),
            "no document-start marker in the raw cell: {fm}"
        );
    }

    #[test]
    fn multi_document_payload_is_malformed() {
        // The commented separator is not a frontmatter closer (exact-line
        // match) but is a YAML document start, so the payload holds two
        // documents.
        let input = "---\na: 1\n--- # second\nb: 2\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-error\""), "got: {fm}");
        assert!(
            fm.contains("frontmatter must be a YAML mapping"),
            "got: {fm}"
        );
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn comment_only_payload_is_malformed() {
        let input = "---\n# nothing\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-error\""), "got: {fm}");
        assert!(
            fm.contains("frontmatter must be a YAML mapping"),
            "got: {fm}"
        );
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn bad_value_is_malformed() {
        let input = "---\nx: !!int abc\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-error\""), "got: {fm}");
        assert!(
            fm.contains("frontmatter contains an unparseable value"),
            "got: {fm}"
        );
        assert!(fm.contains("<pre>x: !!int abc\n</pre>"), "got: {fm}");
        assert!(!fm.contains("<dd class=\"fm-raw\">~</dd>"), "got: {fm}");
    }

    #[test]
    fn nested_bad_value_is_malformed() {
        let input = "---\nouter:\n  - inner: !!float nope\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(
            fm.contains("frontmatter contains an unparseable value"),
            "got: {fm}"
        );
    }

    #[test]
    fn float_scalars_keep_spelling() {
        let input = "---\na: 1.0\nb: 1.5\nc: 0.00001\nd: .inf\ne: -.inf\nf: .nan\ng: 1e20\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        for (key, text) in [
            ("a", "1.0"),
            ("b", "1.5"),
            ("c", "0.00001"),
            ("d", ".inf"),
            ("e", "-.inf"),
            ("f", ".nan"),
            ("g", "1e20"),
        ] {
            assert!(
                fm.contains(&format!("<dt>{key}</dt><dd>{text}</dd>")),
                "{key} should render as {text}: {fm}"
            );
        }
    }

    #[test]
    fn float_spelling_holds_in_raw_fallback() {
        // A nested sequence goes through the emitter (sublist entry), as
        // does a compound key; both must spell floats like the scalar path.
        let input = "---\na: [[1e20, .inf, -.inf, .nan, 1.0]]\n? [0.00001]\n: v\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(
            fm.contains("<li>- 1e20 - .inf - -.inf - .nan - 1.0</li>"),
            "got: {fm}"
        );
        assert!(fm.contains("<dt>- 0.00001</dt><dd>v</dd>"), "got: {fm}");
    }

    #[test]
    fn yaml_1_1_booleans_are_strings() {
        let input = "---\nflag: yes\nother: true\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("<dt>flag</dt><dd>yes</dd>"), "got: {fm}");
        assert!(fm.contains("<dt>other</dt><dd>true</dd>"), "got: {fm}");
    }

    #[test]
    fn duplicate_keys_are_malformed() {
        let input = "---\na: 1\na: 2\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("class=\"fm-error\""), "got: {fm}");
        assert!(fm.contains("<pre>a: 1\na: 2\n</pre>"), "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn keys_equal_after_resolution_are_malformed() {
        let input = "---\na: 1\n'a': 2\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("frontmatter has a duplicate key"), "got: {fm}");
    }

    #[test]
    fn bad_value_in_key_is_malformed() {
        let input = "---\n? !!int abc\n: v\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(
            fm.contains("frontmatter contains an unparseable value"),
            "got: {fm}"
        );
    }

    #[test]
    fn numbers_display_as_written() {
        let input = "---\nzip: 012\nbig: 18446744073709551615\nhuge: 1e400\nhex: 0x1F\nplus: +5\ntagged: !mytag 007\nnested: [[012, 1e400]]\n021: k\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        for (key, text) in [
            ("zip", "012"),
            ("big", "18446744073709551615"),
            ("huge", "1e400"),
            ("hex", "0x1F"),
            ("plus", "+5"),
        ] {
            assert!(
                fm.contains(&format!("<dt>{key}</dt><dd>{text}</dd>")),
                "{key} should render as {text}: {fm}"
            );
        }
        assert!(
            fm.contains("<dt>tagged</dt><dd class=\"fm-raw\">!mytag 007</dd>"),
            "got: {fm}"
        );
        assert!(fm.contains("<li>- 012 - 1e400</li>"), "got: {fm}");
        assert!(fm.contains("<dt>021</dt><dd>k</dd>"), "got: {fm}");
    }

    #[test]
    fn anchors_and_aliases_are_refused() {
        for (payload, escaped) in [
            ("a: &x foo\nb: *x\n", "a: &amp;x foo\nb: *x\n"),
            ("a: &x foo\n", "a: &amp;x foo\n"),
            ("c: &l [x, y]\n", "c: &amp;l [x, y]\n"),
            ("a: &x [*x]\n", "a: &amp;x [*x]\n"),
        ] {
            let input = format!("---\n{payload}---\n# Body\n");
            let (fm, body) = split_and_render_frontmatter(PATH, &input, &cfg_default());
            assert!(fm.contains("class=\"fm-error\""), "{payload:?}: {fm}");
            assert!(fm.contains(ANCHORS_UNSUPPORTED), "{payload:?}: {fm}");
            assert!(fm.contains(&format!("<pre>{escaped}</pre>")), "{fm}");
            assert_eq!(body, "# Body\n");
        }
        // An alias to a name never anchored is the parser's error.
        let (fm, _) = split_and_render_frontmatter(PATH, "---\nb: *x\n---\n", &cfg_default());
        assert!(fm.contains("found unknown anchor"), "got: {fm}");
        assert!(!fm.contains(ANCHORS_UNSUPPORTED), "got: {fm}");
    }

    /// `a1` is `levels` flow sequences around `x`; each later `aN` is the
    /// same around an alias to `a(N-1)`. Loaded with aliases, each link
    /// would add `levels` to the tree's height; the payload is refused at
    /// `&a1` instead.
    fn alias_chain_payload(links: usize, levels: usize) -> String {
        let (open, close) = ("[".repeat(levels), "]".repeat(levels));
        let mut input = format!("---\na1: &a1 {open}x{close}\n");
        for n in 2..=links {
            input.push_str(&format!("a{n}: &a{n} {open}*a{}{close}\n", n - 1));
        }
        input.push_str("---\n# Body\n");
        input
    }

    #[test]
    fn alias_chain_is_refused_before_materialising() {
        // A 2.7 KB file whose aliases would build a tree about 1 270 levels
        // tall.
        let input = alias_chain_payload(10, MAX_DEPTH - 1);
        let (fm, body) = render_hazard(&input);
        assert!(fm.contains(ANCHORS_UNSUPPORTED), "got: {}", &fm[..200]);
        assert_eq!(body, "# Body\n");
    }

    /// `k:` plus `levels - 1` compact block sequences: `levels` deep.
    fn nested_payload(levels: usize) -> String {
        format!("---\nk:\n{}x\n---\n", "- ".repeat(levels - 1))
    }

    #[test]
    fn nesting_at_depth_limit_renders() {
        let (fm, _) =
            split_and_render_frontmatter(PATH, &nested_payload(MAX_DEPTH), &cfg_default());
        assert!(fm.contains("<dt>k</dt>"), "got: {fm}");
        assert!(!fm.contains("fm-error"), "got: {fm}");
    }

    #[test]
    fn nesting_past_depth_limit_is_malformed() {
        let (fm, _) =
            split_and_render_frontmatter(PATH, &nested_payload(MAX_DEPTH + 1), &cfg_default());
        assert!(
            fm.contains("frontmatter nests deeper than 128 levels"),
            "got: {fm}"
        );
        let (fm, _) = render_hazard(&nested_payload(3000));
        assert!(
            fm.contains("frontmatter nests deeper than 128 levels"),
            "got: {fm}"
        );
    }

    const BYTES_REFUSED: &str = "frontmatter expands to more than 4194304 bytes of text";

    #[test]
    fn scalar_bytes_past_budget_is_malformed() {
        let input = format!(
            "---\nk: {}\n---\n# Body\n",
            "a".repeat(MAX_SCALAR_BYTES + 1)
        );
        let (fm, body) = render_hazard(&input);
        assert!(fm.contains(BYTES_REFUSED), "got: {}", &fm[..200]);
        assert!(fm.contains("<pre>k: aaaa"), "got: {}", &fm[..200]);
        assert_eq!(body, "# Body\n");
    }

    /// `k:` holding a flow sequence of `entries` scalars: `entries + 3`
    /// nodes with the top mapping and the key.
    fn wide_payload(entries: usize) -> String {
        format!("---\nk: [{}]\n---\n# Body\n", vec!["x"; entries].join(", "))
    }

    #[test]
    fn nodes_past_budget_is_malformed() {
        let input = wide_payload(100_001);
        let (fm, body) = render_hazard(&input);
        assert!(
            fm.contains("frontmatter expands to more than 100000 nodes"),
            "got: {}",
            &fm[..200]
        );
        assert!(fm.contains("<pre>k: [x, x"), "got: {}", &fm[..200]);
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn nodes_under_budget_render() {
        let input = wide_payload(99_990);
        let (fm, _) = split_and_render_frontmatter(PATH, &input, &cfg_default());
        assert!(fm.contains("<dt>k</dt>"), "got: {}", &fm[..200]);
        assert!(!fm.contains("fm-error"), "got: {}", &fm[..200]);
    }

    #[test]
    fn tag_directive_expansion_counts_toward_scalar_budget() {
        // The parser copies the 500 000-byte prefix into each of the 40
        // tags, on scalars and then on collections. `--- ` (trailing space)
        // starts the YAML document without closing the frontmatter block.
        for item in ["!e!x a", "!e!x []"] {
            let items = [item; 40].join(", ");
            let input = format!(
                "---\n%TAG !e! tag:{}\n--- \nk: [{items}]\n---\n# Body\n",
                "a".repeat(500_000)
            );
            let (fm, body) = render_hazard(&input);
            assert!(fm.contains(BYTES_REFUSED), "{item}: {}", &fm[..200]);
            assert_eq!(body, "# Body\n");
        }
    }

    #[test]
    #[tracing_test::traced_test]
    fn budget_refusal_is_logged_and_parse_error_is_not() {
        let (fm, _) = split_and_render_frontmatter(PATH, "---\na: &x 1\n---\n", &cfg_default());
        assert!(fm.contains("fm-error"), "got: {fm}");
        assert!(!logs_contain("resource budget exceeded"));
        let (fm, _) = split_and_render_frontmatter(PATH, "---\na: [\n---\n", &cfg_default());
        assert!(fm.contains("fm-error"), "got: {fm}");
        assert!(!logs_contain("resource budget exceeded"));
        let (fm, _) = render_hazard(&nested_payload(3000));
        assert!(fm.contains("fm-error"), "got: {}", &fm[..200]);
        assert!(logs_contain("resource budget exceeded"));
        assert!(logs_contain("budget=\"depth\""));
        assert!(logs_contain("path=\"notes/alice.md\""));
    }

    #[test]
    fn scalar_bytes_at_file_cap_render() {
        let cap = usize::try_from(crate::artifact::MAX_FILE_SIZE).unwrap();
        let input = format!("---\nk: {}\n---\n", "a".repeat(cap));
        let (fm, _) = split_and_render_frontmatter(PATH, &input, &cfg_default());
        assert!(fm.contains("<dt>k</dt>"), "got: {}", &fm[..200]);
        assert!(!fm.contains("fm-error"), "got: {}", &fm[..200]);
    }

    #[test]
    fn compound_key_is_flattened_to_one_line() {
        let input = "---\n? [a, b]\n: v\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("<dt>- a - b</dt><dd>v</dd>"), "got: {fm}");
    }

    #[test]
    fn xss_in_value_is_escaped() {
        let input = "---\nfoo: \"<img onerror=alert(1)>\"\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(!fm.contains("<img"), "got: {fm}");
        assert!(fm.contains("&lt;img"), "got: {fm}");
    }

    #[test]
    fn xss_in_key_is_escaped() {
        // Quote a YAML key containing HTML metacharacters.
        let input = "---\n\"<img onerror=alert(1)>\": value\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(fm.contains("&lt;img"), "got: {fm}");
        // Make sure no raw `<img` slips through in any form.
        assert!(!fm.contains("<img onerror"), "got: {fm}");
    }

    #[test]
    fn xss_in_summary_markdown_is_stripped() {
        let input = "---\nsummary: \"<script>alert(1)</script>\"\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert!(!fm.contains("<script>"), "got: {fm}");
        // `render_markdown` strips raw HTML events entirely (not even
        // escaped — see markdown.rs:85).
        assert!(!fm.contains("alert(1)"), "got: {fm}");
    }

    #[test]
    fn empty_frontmatter_emits_empty_string() {
        let input = "---\n---\n# Body\n";
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert_eq!(fm, "", "got: {fm}");
        assert_eq!(body, "# Body\n");
    }

    #[test]
    fn pin_lede_suppressed_when_show_is_non_empty() {
        let mut cfg = cfg_default();
        cfg.show = vec!["status".into(), "tldr".into()];
        let input = "---\nstatus: ok\ntldr: lede\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg);
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
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg);
        // Should render as a (possibly inlined) sequence, not as fm-md.
        assert!(!fm.contains("class=\"fm-md\""), "got: {fm}");
        assert!(fm.contains("<dd>a, b</dd>"), "got: {fm}");
    }

    #[test]
    fn list_cap_zero_emits_truncation_dd_no_ul() {
        let mut cfg = cfg_default();
        cfg.list_cap = 0;
        let input = "---\nlog:\n  - one\n  - two\n  - three\n---\n";
        let (fm, _) = split_and_render_frontmatter(PATH, input, &cfg);
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
        let (fm, body) = split_and_render_frontmatter(PATH, input, &cfg_default());
        assert_eq!(fm, "");
        assert_eq!(body, "");
    }
}

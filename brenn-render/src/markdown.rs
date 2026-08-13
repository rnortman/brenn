//! Server-side markdown rendering.
//!
//! Converts CC assistant message content blocks into HTML. Text blocks are
//! rendered as markdown (pulldown-cmark); code blocks render as plain
//! `<pre><code>`. Thinking blocks are rendered as collapsible `<details>` elements.
//!
//! This module is the trust boundary for assistant message HTML — the frontend
//! sets `innerHTML` with the output. All content is escaped by pulldown-cmark
//! (and, for code blocks, by `html_escape`); no raw user/AI text passes through
//! unescaped.

use brenn_cc::protocol::incoming::ContentBlock;
use brenn_lib::util::html_escape;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

/// Render a list of CC content blocks into HTML.
///
/// - `Text` blocks: markdown → HTML with plain code blocks.
/// - `Thinking` blocks: markdown → HTML wrapped in a collapsible `<details>`.
/// - `ToolUse` / `Unknown`: skipped.
pub fn render_content_blocks(blocks: &[ContentBlock]) -> String {
    let mut html = String::new();
    for block in blocks {
        match block {
            ContentBlock::Text { text } => {
                html.push_str(&render_markdown(text));
            }
            ContentBlock::Thinking { thinking, .. } => {
                html.push_str("<details class=\"thinking-block\"><summary>Thinking</summary><div class=\"thinking-content\">");
                html.push_str(&render_markdown(thinking));
                html.push_str("</div></details>");
            }
            ContentBlock::ToolUse { .. } | ContentBlock::Unknown => {}
        }
    }
    html
}

/// Render a markdown string to HTML with plain code blocks.
pub fn render_markdown(input: &str) -> String {
    let options =
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;

    let parser = Parser::new_ext(input, options);

    // We intercept code block events to render them as plain escaped
    // `<pre><code>`. All other events pass through to pulldown-cmark's HTML
    // renderer. The fenced language hint is not tracked because Brenn does not
    // syntax-highlight — all code blocks render identically.
    let mut in_code_block = false;
    let mut code_buf = String::new();

    let mut events: Vec<Event<'_>> = Vec::new();

    for event in parser {
        match event {
            Event::Start(Tag::CodeBlock(_)) => {
                in_code_block = true;
                code_buf.clear();
            }
            Event::End(TagEnd::CodeBlock) => {
                in_code_block = false;
                events.push(Event::Html(render_code_block(&code_buf).into()));
            }
            Event::Text(text) if in_code_block => {
                code_buf.push_str(&text);
            }
            // Strip raw HTML from markdown source — CC responses shouldn't contain
            // raw HTML, and passing it through would be an XSS vector.
            Event::Html(_) | Event::InlineHtml(_) => {}
            other => {
                events.push(other);
            }
        }
    }

    let mut html_output = String::new();
    pulldown_cmark::html::push_html(&mut html_output, events.into_iter());
    html_output
}

/// Render a code block as a plain, HTML-escaped `<pre><code>` element.
///
/// Brenn does not do syntax highlighting, so there is no language parameter:
/// all code blocks render identically.
fn render_code_block(code: &str) -> String {
    let escaped = html_escape(code);
    format!("<pre><code>{escaped}</code></pre>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_renders_as_paragraph() {
        let html = render_markdown("Hello world");
        assert!(html.contains("<p>Hello world</p>"), "got: {html}");
    }

    #[test]
    fn headings_render() {
        let html = render_markdown("# H1\n## H2\n### H3");
        assert!(html.contains("<h1>H1</h1>"), "got: {html}");
        assert!(html.contains("<h2>H2</h2>"), "got: {html}");
        assert!(html.contains("<h3>H3</h3>"), "got: {html}");
    }

    #[test]
    fn fenced_code_block_with_language_is_plain() {
        // The fenced language hint is ignored — code blocks render as plain
        // `<pre><code>` with no inline styles regardless of language.
        let md = "```rust\nfn main() {}\n```";
        let html = render_markdown(md);
        assert!(
            html.contains("<pre><code>"),
            "expected plain code block, got: {html}"
        );
        assert!(
            !html.contains("<pre style="),
            "code block must not carry inline styles, got: {html}"
        );
        // The verbatim source is preserved (and escaped) in a plain block.
        assert!(
            html.contains("fn main() {}"),
            "source should appear verbatim in plain output, got: {html}"
        );
    }

    #[test]
    fn fenced_code_block_without_language_is_plain() {
        let md = "```\nsome code\n```";
        let html = render_markdown(md);
        assert!(
            html.contains("<pre><code>"),
            "expected plain code block, got: {html}"
        );
        assert!(html.contains("some code"), "got: {html}");
    }

    #[test]
    fn inline_code_renders() {
        let html = render_markdown("Use `foo()` here");
        assert!(html.contains("<code>foo()</code>"), "got: {html}");
    }

    #[test]
    fn xss_in_text_is_stripped() {
        // Raw HTML in markdown source is stripped entirely (not escaped, not rendered).
        let html = render_markdown("<script>alert(1)</script>");
        assert!(
            !html.contains("<script>"),
            "raw script tag found in: {html}"
        );
        assert!(
            !html.contains("alert(1)"),
            "script content should be stripped, got: {html}"
        );
    }

    #[test]
    fn angle_brackets_in_text_are_safe() {
        // Text that looks like HTML but is escaped by markdown parsing context.
        let html = render_markdown("Use `<div>` for containers");
        assert!(html.contains("&lt;div&gt;"), "got: {html}");
    }

    #[test]
    fn xss_in_code_block_is_escaped() {
        let md = "```js\n<script>alert(1)</script>\n```";
        let html = render_markdown(md);
        assert!(
            !html.contains("<script>alert"),
            "raw script tag found in: {html}"
        );
        // Plain blocks escape the source verbatim, so the fully escaped tag
        // appears contiguously.
        assert!(
            html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"),
            "code block content must be HTML-escaped, got: {html}"
        );
    }

    #[test]
    fn thinking_block_renders_as_details() {
        let blocks = vec![ContentBlock::Thinking {
            thinking: "Let me consider this.".to_string(),
            signature: None,
        }];
        let html = render_content_blocks(&blocks);
        assert!(html.contains("<details"), "got: {html}");
        assert!(html.contains("thinking-block"), "got: {html}");
        assert!(html.contains("<summary>Thinking</summary>"), "got: {html}");
        assert!(html.contains("Let me consider this."), "got: {html}");
    }

    #[test]
    fn text_and_thinking_blocks_interleave() {
        let blocks = vec![
            ContentBlock::Text {
                text: "First part.".to_string(),
            },
            ContentBlock::Thinking {
                thinking: "Hmm...".to_string(),
                signature: None,
            },
            ContentBlock::Text {
                text: "Second part.".to_string(),
            },
        ];
        let html = render_content_blocks(&blocks);
        assert!(html.contains("First part."), "got: {html}");
        assert!(html.contains("Hmm..."), "got: {html}");
        assert!(html.contains("Second part."), "got: {html}");

        // Order: text, details, text
        let first_pos = html.find("First part.").unwrap();
        let details_pos = html.find("<details").unwrap();
        let second_pos = html.find("Second part.").unwrap();
        assert!(first_pos < details_pos, "text should come before thinking");
        assert!(
            details_pos < second_pos,
            "thinking should come before second text"
        );
    }

    #[test]
    fn empty_content_blocks_produce_empty_string() {
        assert_eq!(render_content_blocks(&[]), "");
    }

    #[test]
    fn tool_use_and_unknown_blocks_are_skipped() {
        let blocks = vec![
            ContentBlock::ToolUse {
                id: "t1".to_string(),
                name: "Bash".to_string(),
                input: serde_json::json!({}),
            },
            ContentBlock::Unknown,
        ];
        assert_eq!(render_content_blocks(&blocks), "");
    }

    #[test]
    fn tables_render() {
        let md = "| A | B |\n|---|---|\n| 1 | 2 |";
        let html = render_markdown(md);
        assert!(html.contains("<table>"), "got: {html}");
        assert!(html.contains("<td>1</td>"), "got: {html}");
    }

    #[test]
    fn strikethrough_renders() {
        let html = render_markdown("~~deleted~~");
        assert!(html.contains("<del>deleted</del>"), "got: {html}");
    }

    #[test]
    fn cc_common_languages_render_plain() {
        // Code blocks render identically regardless of language hint — no
        // highlighting, no inline styles — for the CC-common tokens.
        for token in &["python", "typescript", "bash", "json"] {
            let md = format!("```{token}\ncode here\n```");
            let html = render_markdown(&md);
            assert!(
                html.contains("<pre><code>"),
                "expected plain code block for language {token:?}, got: {html}"
            );
            assert!(
                !html.contains("<pre style="),
                "code block for language {token:?} must not carry inline styles, got: {html}"
            );
            assert!(
                html.contains("code here"),
                "source should appear in plain output for {token:?}, got: {html}"
            );
        }
    }

    #[test]
    fn unrecognized_language_falls_back_to_plain() {
        let md = "```nosuchlang\nfoo bar\n```";
        let html = render_markdown(md);
        assert!(
            html.contains("<pre><code>"),
            "expected plain code block, got: {html}"
        );
        assert!(html.contains("foo bar"), "got: {html}");
    }

    #[test]
    fn bold_and_italic_render() {
        let html = render_markdown("**bold** and *italic*");
        assert!(html.contains("<strong>bold</strong>"), "got: {html}");
        assert!(html.contains("<em>italic</em>"), "got: {html}");
    }

    #[test]
    fn links_render() {
        let html = render_markdown("[click](https://example.com)");
        assert!(
            html.contains("<a href=\"https://example.com\">click</a>"),
            "got: {html}"
        );
    }

    #[test]
    fn lists_render() {
        let md = "- item 1\n- item 2\n";
        let html = render_markdown(md);
        assert!(html.contains("<ul>"), "got: {html}");
        assert!(html.contains("<li>item 1</li>"), "got: {html}");
    }

    #[test]
    fn blockquote_renders() {
        let html = render_markdown("> quoted text");
        assert!(html.contains("<blockquote>"), "got: {html}");
        assert!(html.contains("quoted text"), "got: {html}");
    }
}

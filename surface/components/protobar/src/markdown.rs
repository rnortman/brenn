//! DOM-free markdown → intermediate tree.
//!
//! Every security-relevant decision lives here, host-tested, with no DOM
//! dependency: pulldown-cmark events are turned into a pure [`Block`]/[`Inline`]
//! tree that the wasm DOM glue walks with `createElement`/`createTextNode`. No
//! HTML string is ever produced and no markup is ever parsed from untrusted
//! text, so injection is impossible by construction (strictly stronger than
//! escape-then-`innerHTML`). Two policies mirror the backend renderer:
//!
//! - **Raw HTML is dropped** — `Event::Html`/`Event::InlineHtml` contribute
//!   nothing (the same stance as the server-side pipeline).
//! - **No anchor is ever created** — links and autolinks contribute their
//!   children as text with no wrapper. Surfaces run on chrome-less kiosks where
//!   every navigation affordance would strand the display with no way back.
//!
//! Images render their alt text only and fetch nothing. Nesting is capped so an
//! adversarial delimiter pyramid in a 64 KiB body cannot overflow the stack
//! building the tree or walking it; text content is never lost to the cap.

use pulldown_cmark::{Event, HeadingLevel, Options, Parser, Tag, TagEnd};

/// Maximum block+inline nesting depth. Beyond it, no further container nodes are
/// created and deeper text flows through at the cap depth (never dropped). Bounds
/// tree-build and DOM-walk recursion against a hostile pyramid.
const MAX_DEPTH: usize = 32;

/// A block-level node. Maps one-to-one to the DOM element the glue creates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    /// `<p>`.
    Paragraph(Vec<Inline>),
    /// `<h1>`..`<h6>`; `level` is `1..=6`.
    Heading { level: u8, children: Vec<Inline> },
    /// `<ul>` (unordered) or `<ol>` (ordered, carrying the source `start`); each
    /// item is a `<li>` body.
    List {
        ordered: bool,
        start: u64,
        items: Vec<Vec<Block>>,
    },
    /// `<pre>` holding one literal text node — plain text, no highlighting; the
    /// fence info/language string is discarded.
    CodeBlock(String),
    /// `<blockquote>`.
    Blockquote(Vec<Block>),
    /// `<hr>`.
    Rule,
}

/// An inline-level node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inline {
    /// A literal text run (`createTextNode`).
    Text(String),
    /// `<code>` inline span.
    Code(String),
    /// `<em>` / `<strong>` / `<s>`.
    Styled { style: Style, children: Vec<Inline> },
    /// `<br>`.
    HardBreak,
}

/// Inline emphasis style.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Style {
    Emphasis,
    Strong,
    Strikethrough,
}

/// The single plain-text render path: one paragraph holding the text verbatim.
/// Bare bodies and `format: "plain"` bodies use this, so the display always
/// carries `Vec<Block>` and there is exactly one DOM walk.
pub fn plain(text: &str) -> Vec<Block> {
    vec![Block::Paragraph(vec![Inline::Text(text.to_string())])]
}

/// Parse markdown into the block tree. Only `ENABLE_STRIKETHROUGH` is on: tables,
/// tasklists, and footnotes stay unenabled and their syntax degrades to plain
/// paragraph text.
pub fn parse(text: &str) -> Vec<Block> {
    let mut builder = Builder::new();
    for event in Parser::new_ext(text, Options::ENABLE_STRIKETHROUGH) {
        builder.event(event);
    }
    builder.finish()
}

/// A frame on the container stack, accumulating its children until it closes.
enum Frame {
    /// The root and every block container hold completed blocks.
    Blocks(BlockHolder, Vec<Block>),
    /// A list accumulates completed item bodies.
    List {
        ordered: bool,
        start: u64,
        items: Vec<Vec<Block>>,
    },
    /// Inline-holding blocks accumulate inlines.
    Paragraph(Vec<Inline>),
    Heading {
        level: u8,
        children: Vec<Inline>,
    },
    /// An inline emphasis wrapper accumulates inlines.
    Styled {
        style: Style,
        children: Vec<Inline>,
    },
    /// A code block accumulates its literal text.
    Code(String),
}

/// Which block container a [`Frame::Blocks`] is, so it can be sealed into the
/// right [`Block`] when it closes. The root seals into nothing (it is returned).
enum BlockHolder {
    Root,
    Blockquote,
    Item,
}

struct Builder {
    stack: Vec<Frame>,
    /// Count of container starts suppressed at the depth cap, awaiting their
    /// matching ends. Strictly LIFO: once at the cap every deeper start is
    /// suppressed and unwinds before any real frame is popped.
    suppressed: usize,
}

impl Builder {
    fn new() -> Self {
        Self {
            stack: vec![Frame::Blocks(BlockHolder::Root, Vec::new())],
            suppressed: 0,
        }
    }

    /// Dispatch one parser event.
    fn event(&mut self, event: Event<'_>) {
        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(t) => self.text(&t),
            Event::Code(t) => self.sink_inline(Inline::Code(t.to_string())),
            // CommonMark renders a soft break as whitespace.
            Event::SoftBreak => self.sink_inline(Inline::Text(" ".to_string())),
            Event::HardBreak => self.sink_inline(Inline::HardBreak),
            Event::Rule => self.push_block(Block::Rule),
            // Raw HTML is dropped at the parser — never rendered, never escaped.
            Event::Html(_) | Event::InlineHtml(_) => {}
            // Only ENABLE_STRIKETHROUGH is on, so tasklist/footnote events cannot
            // arrive; any future/unrecognized event degrades to nothing here
            // while its enclosing block's text still flows through via `text`.
            _ => {}
        }
    }

    /// Handle a container start. Links and images create no frame — their
    /// children flow into the current holder with no wrapper (no anchor;
    /// image alt text only). Every other start opens a frame, subject to the cap.
    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph => self.open(Frame::Paragraph(Vec::new())),
            Tag::Heading { level, .. } => self.open(Frame::Heading {
                level: heading_level(level),
                children: Vec::new(),
            }),
            Tag::BlockQuote(_) => self.open(Frame::Blocks(BlockHolder::Blockquote, Vec::new())),
            Tag::List(first) => self.open(Frame::List {
                ordered: first.is_some(),
                start: first.unwrap_or(1),
                items: Vec::new(),
            }),
            Tag::Item => self.open(Frame::Blocks(BlockHolder::Item, Vec::new())),
            Tag::CodeBlock(_) => self.open(Frame::Code(String::new())),
            Tag::Emphasis => self.open(Frame::Styled {
                style: Style::Emphasis,
                children: Vec::new(),
            }),
            Tag::Strong => self.open(Frame::Styled {
                style: Style::Strong,
                children: Vec::new(),
            }),
            Tag::Strikethrough => self.open(Frame::Styled {
                style: Style::Strikethrough,
                children: Vec::new(),
            }),
            // No wrapper: children (the label, or an autolink's URL-as-text)
            // flow through. Nothing is fetched for an image.
            Tag::Link { .. } | Tag::Image { .. } => {}
            // Unenabled/unknown container: no wrapper, contents flow through.
            _ => {}
        }
    }

    /// Handle a container end. Close exactly the ends whose starts open a frame
    /// in `start` (kept symmetric with it); every other end — link/image, a
    /// dropped raw-HTML block, and any unenabled/future tag whose start opened no
    /// frame — closes nothing. A `_ => self.close()` catch-all would instead pop a
    /// live frame for a start that opened none (e.g. `HtmlBlock`), desyncing the
    /// tree and the suppression counter.
    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph
            | TagEnd::Heading(_)
            | TagEnd::BlockQuote(_)
            | TagEnd::List(_)
            | TagEnd::Item
            | TagEnd::CodeBlock
            | TagEnd::Emphasis
            | TagEnd::Strong
            | TagEnd::Strikethrough => self.close(),
            _ => {}
        }
    }

    /// A text run. Inside a code block it appends to the literal buffer; anywhere
    /// else it becomes an inline text node in the nearest holder.
    fn text(&mut self, t: &str) {
        if let Some(Frame::Code(buf)) = self.stack.last_mut() {
            buf.push_str(t);
        } else {
            self.sink_inline(Inline::Text(t.to_string()));
        }
    }

    /// Open a frame, or suppress it at the depth cap. Suppressed containers add no
    /// node; their text still flows to the nearest live holder via `sink_inline`.
    fn open(&mut self, frame: Frame) {
        if self.suppressed > 0 || self.stack.len() >= MAX_DEPTH {
            self.suppressed += 1;
        } else {
            self.stack.push(frame);
        }
    }

    /// Close the top frame: unwind a suppressed start if one is pending, else pop
    /// the frame, seal it into its node, and integrate that into its parent.
    fn close(&mut self) {
        if self.suppressed > 0 {
            self.suppressed -= 1;
            return;
        }
        // The root frame is never closed by an event (no `End` matches it): the
        // stack always has a live frame here.
        let frame = self.stack.pop().expect("a frame to close");
        match frame {
            Frame::Blocks(BlockHolder::Root, blocks) => {
                // Defensive: a stray root close would drop the document. Put it
                // back so `finish` still returns it.
                self.stack.push(Frame::Blocks(BlockHolder::Root, blocks));
            }
            Frame::Blocks(BlockHolder::Blockquote, blocks) => {
                self.push_block(Block::Blockquote(blocks));
            }
            Frame::Blocks(BlockHolder::Item, blocks) => self.push_item(blocks),
            Frame::List {
                ordered,
                start,
                items,
            } => self.push_block(Block::List {
                ordered,
                start,
                items,
            }),
            Frame::Paragraph(children) => self.push_block(Block::Paragraph(children)),
            Frame::Heading { level, children } => {
                self.push_block(Block::Heading { level, children });
            }
            Frame::Styled { style, children } => {
                self.sink_inline(Inline::Styled { style, children });
            }
            Frame::Code(text) => self.push_block(Block::CodeBlock(text)),
        }
    }

    /// Append a completed block to the nearest block container.
    fn push_block(&mut self, block: Block) {
        match self.stack.last_mut() {
            Some(Frame::Blocks(_, blocks)) => blocks.push(block),
            // A block produced directly inside a list (no enclosing item) is
            // malformed input; keep its content by attaching to the last item.
            Some(Frame::List { items, .. }) => match items.last_mut() {
                Some(item) => item.push(block),
                None => items.push(vec![block]),
            },
            // No block-capable holder (only reachable on malformed nesting):
            // drop the container but never panic.
            _ => {}
        }
    }

    /// Attach a completed list-item body to the enclosing list.
    fn push_item(&mut self, blocks: Vec<Block>) {
        if let Some(Frame::List { items, .. }) = self.stack.last_mut() {
            items.push(blocks);
        }
    }

    /// Append a completed inline to the nearest inline holder. If the top frame is
    /// a block container (the cap-suppression case, where a paragraph start was
    /// swallowed), merge into a trailing paragraph so text is never lost.
    fn sink_inline(&mut self, inline: Inline) {
        match self.stack.last_mut() {
            Some(Frame::Paragraph(children))
            | Some(Frame::Heading { children, .. })
            | Some(Frame::Styled { children, .. }) => children.push(inline),
            Some(Frame::Blocks(_, blocks)) => match blocks.last_mut() {
                Some(Block::Paragraph(children)) => children.push(inline),
                _ => blocks.push(Block::Paragraph(vec![inline])),
            },
            // A list is the top frame when an item start was suppressed at the
            // cap: merge into the trailing paragraph of the last item so deeply
            // nested text is never lost (mirrors `push_block`'s list arm).
            Some(Frame::List { items, .. }) => {
                let item = match items.last_mut() {
                    Some(item) => item,
                    None => {
                        items.push(Vec::new());
                        items.last_mut().expect("just pushed an item")
                    }
                };
                match item.last_mut() {
                    Some(Block::Paragraph(children)) => children.push(inline),
                    _ => item.push(Block::Paragraph(vec![inline])),
                }
            }
            // A code block swallows inline events as-is: an inline `Code` event
            // never occurs inside a fenced block, but degrade rather than panic.
            _ => {}
        }
    }

    /// Seal the document: unwind any still-open frames into the root, then return
    /// the root blocks. A well-formed stream ends with only the root open.
    fn finish(mut self) -> Vec<Block> {
        while self.stack.len() > 1 {
            self.close();
        }
        match self.stack.pop() {
            Some(Frame::Blocks(BlockHolder::Root, blocks)) => blocks,
            _ => Vec::new(),
        }
    }
}

/// Map a pulldown heading level to `1..=6`.
fn heading_level(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

/// Concatenate all text-bearing content of a block tree, depth-first (recursing
/// into every container), so a test can assert "no text was lost" without
/// spelling out the full tree. Shared by this module's tests and `logic.rs`'s.
#[cfg(test)]
pub(crate) fn all_text(blocks: &[Block]) -> String {
    fn inlines(out: &mut String, items: &[Inline]) {
        for inline in items {
            match inline {
                Inline::Text(t) | Inline::Code(t) => out.push_str(t),
                Inline::Styled { children, .. } => inlines(out, children),
                Inline::HardBreak => {}
            }
        }
    }
    fn walk(out: &mut String, blocks: &[Block]) {
        for block in blocks {
            match block {
                Block::Paragraph(c) | Block::Heading { children: c, .. } => inlines(out, c),
                Block::CodeBlock(t) => out.push_str(t),
                Block::Blockquote(b) => walk(out, b),
                Block::List { items, .. } => {
                    for item in items {
                        walk(out, item);
                    }
                }
                Block::Rule => {}
            }
        }
    }
    let mut out = String::new();
    walk(&mut out, blocks);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_wraps_text_in_one_paragraph() {
        assert_eq!(
            plain("hello *world*"),
            vec![Block::Paragraph(vec![Inline::Text(
                "hello *world*".to_string()
            )])]
        );
    }

    #[test]
    fn emphasis_strong_strikethrough() {
        assert_eq!(
            parse("*a* **b** ~~c~~"),
            vec![Block::Paragraph(vec![
                Inline::Styled {
                    style: Style::Emphasis,
                    children: vec![Inline::Text("a".to_string())],
                },
                Inline::Text(" ".to_string()),
                Inline::Styled {
                    style: Style::Strong,
                    children: vec![Inline::Text("b".to_string())],
                },
                Inline::Text(" ".to_string()),
                Inline::Styled {
                    style: Style::Strikethrough,
                    children: vec![Inline::Text("c".to_string())],
                },
            ])]
        );
    }

    #[test]
    fn inline_code() {
        assert_eq!(
            parse("use `foo()`"),
            vec![Block::Paragraph(vec![
                Inline::Text("use ".to_string()),
                Inline::Code("foo()".to_string()),
            ])]
        );
    }

    #[test]
    fn headings_h1_through_h6() {
        for level in 1..=6u8 {
            let hashes = "#".repeat(level as usize);
            let md = format!("{hashes} title");
            assert_eq!(
                parse(&md),
                vec![Block::Heading {
                    level,
                    children: vec![Inline::Text("title".to_string())],
                }],
                "level {level}"
            );
        }
    }

    #[test]
    fn bullet_list() {
        assert_eq!(
            parse("- one\n- two"),
            vec![Block::List {
                ordered: false,
                start: 1,
                items: vec![
                    vec![Block::Paragraph(vec![Inline::Text("one".to_string())])],
                    vec![Block::Paragraph(vec![Inline::Text("two".to_string())])],
                ],
            }]
        );
    }

    #[test]
    fn ordered_list_carries_non_one_start() {
        let blocks = parse("3. c\n4. d");
        let Block::List {
            ordered,
            start,
            items,
        } = &blocks[0]
        else {
            panic!("expected a list, got {blocks:?}");
        };
        assert!(ordered);
        assert_eq!(*start, 3);
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn nested_list() {
        // An outer item whose body holds a nested list.
        let blocks = parse("- outer\n  - inner");
        let Block::List { items, .. } = &blocks[0] else {
            panic!("expected a list, got {blocks:?}");
        };
        assert_eq!(items.len(), 1);
        let inner = items[0].iter().any(|b| matches!(b, Block::List { .. }));
        assert!(
            inner,
            "outer item should contain a nested list: {:?}",
            items[0]
        );
        assert_eq!(all_text(&blocks), "outerinner");
    }

    #[test]
    fn fenced_code_block_is_literal_and_drops_info_string() {
        // The info/language string is discarded; the body is one literal text
        // run including the script tag, which is never markup.
        assert_eq!(
            parse("```rust\n<script>alert(1)</script>\n```"),
            vec![Block::CodeBlock("<script>alert(1)</script>\n".to_string())]
        );
    }

    #[test]
    fn indented_code_block_is_literal() {
        assert_eq!(
            parse("    let x = 1;\n"),
            vec![Block::CodeBlock("let x = 1;\n".to_string())]
        );
    }

    #[test]
    fn blockquote() {
        assert_eq!(
            parse("> quoted"),
            vec![Block::Blockquote(vec![Block::Paragraph(vec![
                Inline::Text("quoted".to_string())
            ])])]
        );
    }

    #[test]
    fn thematic_break_is_rule() {
        assert_eq!(
            parse("a\n\n---\n\nb"),
            vec![
                Block::Paragraph(vec![Inline::Text("a".to_string())]),
                Block::Rule,
                Block::Paragraph(vec![Inline::Text("b".to_string())]),
            ]
        );
    }

    #[test]
    fn soft_break_is_space_hard_break_is_node() {
        assert_eq!(
            parse("a\nb"),
            vec![Block::Paragraph(vec![
                Inline::Text("a".to_string()),
                Inline::Text(" ".to_string()),
                Inline::Text("b".to_string()),
            ])]
        );
        assert_eq!(
            parse("a  \nb"),
            vec![Block::Paragraph(vec![
                Inline::Text("a".to_string()),
                Inline::HardBreak,
                Inline::Text("b".to_string()),
            ])]
        );
    }

    #[test]
    fn raw_html_block_and_inline_are_dropped() {
        // A raw HTML block contributes no node at all.
        assert_eq!(parse("<script>alert(1)</script>"), Vec::new());
        // Inline raw HTML is dropped but surrounding text survives.
        assert_eq!(
            parse("a <b>c</b> d"),
            vec![Block::Paragraph(vec![
                Inline::Text("a ".to_string()),
                Inline::Text("c".to_string()),
                Inline::Text(" d".to_string()),
            ])]
        );
    }

    #[test]
    fn raw_html_block_inside_blockquote_keeps_following_siblings_nested() {
        // pulldown emits Start/End(HtmlBlock) around a raw-HTML block even with no
        // option enabled. Its start opens no frame, so its end must close none —
        // otherwise it prematurely seals the blockquote and the following
        // paragraph escapes to the document root.
        assert_eq!(
            parse("> <div>\n>\n> hello"),
            vec![Block::Blockquote(vec![Block::Paragraph(vec![
                Inline::Text("hello".to_string())
            ])])]
        );
    }

    #[test]
    fn raw_html_block_inside_list_item_keeps_item_content() {
        // Same asymmetry inside a list item: the dropped HTML block must not seal
        // the item early and strand the following text.
        let blocks = parse("- <div>\n\n  item text");
        assert_eq!(all_text(&blocks), "item text");
        assert!(
            matches!(blocks.as_slice(), [Block::List { .. }]),
            "expected a single list, got {blocks:?}"
        );
    }

    #[test]
    fn deep_list_pyramid_flattens_without_text_loss() {
        // A nested-list pyramid far past the cap. Lists push two frames per level
        // (List + Item), so the cap-top frame can be a `Frame::List` — the text
        // sink must handle that case or the innermost item text is dropped.
        let mut md = String::new();
        for level in 0..100 {
            md.push_str(&"  ".repeat(level));
            md.push_str(&format!("- L{level}\n"));
        }
        let blocks = parse(&md);
        assert!(measure_depth(&blocks) <= MAX_DEPTH + 2);
        // Every level's marker survives, including the ones past the cap.
        let text = all_text(&blocks);
        assert!(text.contains("L0"), "got {text:?}");
        assert!(text.contains("L99"), "innermost text lost: {text:?}");
    }

    #[test]
    fn no_anchor_for_any_link_form() {
        // Inline links (any scheme, relative) and autolinks all render as text
        // only — there is no anchor node, so no navigation affordance exists.
        let cases = [
            ("[t](https://example.com)", "t"),
            ("[t](javascript:alert(1))", "t"),
            ("[t](data:text/html,x)", "t"),
            ("[t](/relative/path)", "t"),
            ("<https://example.com>", "https://example.com"),
        ];
        for (md, expected_text) in cases {
            let blocks = parse(md);
            assert_no_element(&blocks);
            assert_eq!(all_text(&blocks), expected_text, "md {md:?}");
        }
    }

    /// Assert the tree contains no styled/code wrapper an anchor could hide in —
    /// the tree simply has no anchor variant, so this checks the label survived
    /// as plain text with no unexpected structure.
    fn assert_no_element(blocks: &[Block]) {
        // `Inline` has no anchor variant by construction; this documents that a
        // link produced only text/plain inlines.
        for block in blocks {
            if let Block::Paragraph(inlines) = block {
                for inline in inlines {
                    assert!(
                        matches!(inline, Inline::Text(_)),
                        "link should produce plain text only, got {inline:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn image_renders_alt_text_only() {
        assert_eq!(
            parse("![alt text](https://example.com/x.png)"),
            vec![Block::Paragraph(vec![Inline::Text("alt text".to_string())])]
        );
    }

    #[test]
    fn tables_degrade_to_paragraph_text() {
        // Tables are not enabled, so the syntax is plain text (no table node).
        let blocks = parse("| A | B |\n|---|---|\n| 1 | 2 |");
        assert!(
            blocks.iter().all(|b| matches!(b, Block::Paragraph(_))),
            "table syntax should degrade to paragraphs, got {blocks:?}"
        );
        assert!(all_text(&blocks).contains("A"));
        assert!(all_text(&blocks).contains('1'));
    }

    #[test]
    fn tasklist_and_footnote_syntax_degrade_without_panicking() {
        // Neither option is enabled; both degrade to ordinary text with no loss.
        let task = parse("- [ ] todo");
        assert!(all_text(&task).contains("todo"));
        // With footnotes off, an undefined `[^1]` reference is literal text.
        let footnote = parse("text[^1] here");
        assert!(all_text(&footnote).contains("text"));
        assert!(all_text(&footnote).contains("here"));
    }

    #[test]
    fn deep_inline_pyramid_flattens_without_text_loss() {
        // A pyramid of emphasis delimiters far past the cap: the tree must build
        // without unbounded recursion and preserve the innermost text.
        let depth = 200;
        let md = format!("{}core{}", "*".repeat(depth), "*".repeat(depth));
        let blocks = parse(&md);
        // Bounded near the cap regardless of the 200-deep input (a couple of
        // extra levels for the enclosing paragraph + text leaf).
        assert!(measure_depth(&blocks) <= MAX_DEPTH + 2);
        assert!(all_text(&blocks).contains("core"));
    }

    #[test]
    fn deep_blockquote_pyramid_flattens_without_text_loss() {
        let mut md = String::new();
        for _ in 0..200 {
            md.push_str("> ");
        }
        md.push_str("core");
        let blocks = parse(&md);
        assert!(measure_depth(&blocks) <= MAX_DEPTH + 2);
        assert!(all_text(&blocks).contains("core"), "got {blocks:?}");
    }

    /// The maximum nesting depth actually present in the produced tree.
    fn measure_depth(blocks: &[Block]) -> usize {
        fn inline_depth(items: &[Inline]) -> usize {
            items
                .iter()
                .map(|i| match i {
                    Inline::Styled { children, .. } => 1 + inline_depth(children),
                    _ => 1,
                })
                .max()
                .unwrap_or(0)
        }
        fn block_depth(blocks: &[Block]) -> usize {
            blocks
                .iter()
                .map(|b| match b {
                    Block::Paragraph(c) | Block::Heading { children: c, .. } => 1 + inline_depth(c),
                    Block::Blockquote(b) => 1 + block_depth(b),
                    Block::List { items, .. } => {
                        1 + items.iter().map(|i| block_depth(i)).max().unwrap_or(0)
                    }
                    Block::CodeBlock(_) | Block::Rule => 1,
                })
                .max()
                .unwrap_or(0)
        }
        block_depth(blocks)
    }

    #[test]
    fn empty_input_is_empty_tree() {
        assert_eq!(parse(""), Vec::new());
    }
}

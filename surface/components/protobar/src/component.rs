//! The protobar component — page-hosted target only.
//!
//! Builds its two child elements in the mount activation and wires the
//! `messages` and `tick` deliveries to the DOM-free
//! [`crate::logic::ProtobarState`]. Receive-only: it publishes nothing but its
//! own expiry wake.
//!
//! The message area is a block tree, not a single string: the DOM-free
//! [`crate::markdown`] tree is walked here with [`dom::create_element`] over
//! the capability's allowed tag set and [`dom::set_text`] — never markup, and
//! never an anchor element (links degrade to text; a chrome-less kiosk has no
//! way back from a navigation). No HTML string is ever produced, so injection
//! is impossible by construction.
//!
//! Every text run gets its own `span`. The capability sets text on an element,
//! not between elements, so a bare run interleaved with `em`/`strong`/`code`
//! siblings needs an element of its own to live in.
//!
//! Priority slots expire on a wall clock, so every render is stamped with the
//! activation's instant and the next live slot's expiry is parked as a deferred
//! self-publish on the `tick` in/out port — cancelling whatever it replaces,
//! both buffered with the activation. A malformed publisher body is reported on
//! the operator log (not a trap) so one buggy publisher cannot brick a bar
//! showing other publishers' live messages.

use std::cell::RefCell;

use brenn_guest::{Activation, Error, Processor, dom, log, repark};
use chrono::{DateTime, Utc};

use crate::logic::{Display, Ingest, ProtobarState};
use crate::markdown::{Block, Inline, Style};
use crate::spec::{InPort, port::TICK};

/// The body of an expiry wake. The tick's payload is irrelevant — the wake is
/// the message — but every body on this bus is JSON.
const TICK_BODY: &str = "{}";

/// The marker attribute on the message subtree's root, which is what every
/// stylesheet rule for the message area is anchored on.
const MESSAGE_MARKER: &str = "data-protobar-message";

/// The marker attribute on the status line.
const STATUS_MARKER: &str = "data-protobar-status";

/// The styling hook carrying the displayed message's priority. Removed when no
/// message occupies the bar.
const PRIORITY_ATTRIBUTE: &str = "data-priority";

// One instantiation backs one instance for the page's lifetime, so the state
// machine, the view handles and the last render are ordinary interior-mutable
// module state. That is what lets p1 and p2 — two declarations of this one kind
// — each keep their own slots.
thread_local! {
    static BAR: RefCell<Protobar> = RefCell::new(Protobar::new());
}

struct Protobar {
    state: ProtobarState,
    /// The elements an activation writes into, built by the mount activation.
    view: Option<View>,
    /// The previously rendered display, so an unchanged message subtree is not
    /// torn down and rebuilt on every delivery.
    last: Option<Display>,
}

struct View {
    message: dom::Node,
    status: dom::Node,
}

impl Protobar {
    fn new() -> Protobar {
        Protobar {
            state: ProtobarState::new(),
            view: None,
            last: None,
        }
    }

    /// The view, which every activation after the mount one has.
    fn view(&self) -> &View {
        self.view
            .as_ref()
            .expect("the mount activation builds the view before any other call")
    }
}

struct ProtobarComponent;

impl Processor for ProtobarComponent {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        BAR.with(|bar| on_activation(&activation, &mut bar.borrow_mut()))?;
        // Mount is the only sync-call activation this component sees, and the
        // mount call is answered with nothing.
        Ok(None)
    }
}

brenn_guest::export_processor!(ProtobarComponent);

/// Build the view when this is the mount call, fold every delivered window into
/// the state machine, then render once — not once per message — and park the
/// next expiry wake.
fn on_activation(activation: &Activation, bar: &mut Protobar) -> Result<(), Error> {
    if activation.sync_is(dom::MOUNT) {
        bar.view = Some(build_view());
    }
    let now = activation_instant(activation)?;
    for window in activation.delivered_windows() {
        if InPort::of(window)? == InPort::Tick {
            continue;
        }
        let port = window.port();
        if window.dropped() > 0 {
            bar.state
                .on_drops(port, u64::from(window.dropped()))
                .map_err(|violation| violation_error(&violation))?;
        }
        for envelope in window.new_raw() {
            let ingest = bar
                .state
                .on_message(port, envelope, now)
                .map_err(|violation| violation_error(&violation))?;
            if let Ingest::Malformed(report) = ingest {
                log::error(report.log_message("protobar body"));
            }
        }
    }
    render(bar, now);
    // The cadence is expiry-driven, not periodic: a bar whose live slots never
    // expire parks nothing and wakes for deliveries alone. `next_expiry` answers
    // only with instants strictly after `now`, so an expiry wake never re-parks
    // itself at the instant it fired.
    let release_at = bar
        .state
        .next_expiry(now)
        .map(|target| target.timestamp_millis().max(0) as u64);
    repark(activation, TICK, TICK_BODY, release_at);
    Ok(())
}

/// The activation's wall clock as an instant. A window arriving without one, or
/// with one no calendar can represent, is host skew rather than a publisher
/// fault, so it fails the activation.
fn activation_instant(activation: &Activation) -> Result<DateTime<Utc>, Error> {
    let now_ms = activation
        .now()
        .ok_or_else(|| Error::failed("protobar: the host stamped no wall clock"))?;
    DateTime::from_timestamp_millis(now_ms as i64)
        .ok_or_else(|| Error::failed(format!("protobar: {now_ms} is not a representable instant")))
}

/// A wire-boundary violation fails the activation: the delivery was not one
/// this component's specification describes, so rendering anyway would mask
/// host/guest skew or operator misconfiguration.
fn violation_error(violation: &crate::logic::ContractViolation) -> Error {
    Error::failed(format!("protobar: {violation:?} on an activation window"))
}

/// Build this instance's two child elements under its host element.
///
/// Called once, from the mount activation.
fn build_view() -> View {
    let root = dom::root();
    let message = dom::marked("div", MESSAGE_MARKER);
    let status = dom::marked("div", STATUS_MARKER);
    dom::append(root, message);
    dom::append(root, status);
    View { message, status }
}

/// Draw both children as of `now`.
///
/// The status line is always rewritten (cheap); the message subtree is rebuilt
/// only when the displayed message or its priority actually changed, so an
/// activation that moved only the drops/malformed counters skips the tree walk
/// and its reflow entirely.
fn render(bar: &mut Protobar, now: DateTime<Utc>) {
    let display = bar.state.display(now);
    let rebuild_message = bar
        .last
        .as_ref()
        .is_none_or(|prev| prev.message != display.message || prev.priority != display.priority);
    let view = bar.view();
    if rebuild_message {
        // Clearing to inert text is how the previous render is discarded: the
        // capability replaces children rather than offering a traversal.
        dom::set_text(view.message, "");
        for block in &display.message {
            append_block(view.message, block);
        }
        match display.priority {
            Some(urgency) => dom::set_attribute(view.message, PRIORITY_ATTRIBUTE, urgency.as_str()),
            None => dom::remove_attribute(view.message, PRIORITY_ATTRIBUTE),
        }
    }
    dom::set_text(view.status, &display.status_text);
    bar.last = Some(display);
}

/// Append one block's subtree under `parent`. Recursion is bounded by the
/// markdown tree's depth cap, so this cannot overflow the stack on hostile
/// input.
fn append_block(parent: dom::Node, block: &Block) {
    match block {
        Block::Paragraph(children) => {
            let el = dom::create_element("p");
            append_inlines(el, children);
            dom::append(parent, el);
        }
        Block::Heading { level, children } => {
            let el = dom::create_element(heading_tag(*level));
            append_inlines(el, children);
            dom::append(parent, el);
        }
        Block::List {
            ordered,
            start,
            items,
        } => {
            let el = dom::create_element(if *ordered { "ol" } else { "ul" });
            if *ordered && *start != 1 {
                dom::set_attribute(el, "start", &start.to_string());
            }
            for item in items {
                let li = dom::create_element("li");
                for child in item {
                    append_block(li, child);
                }
                dom::append(el, li);
            }
            dom::append(parent, el);
        }
        Block::CodeBlock(text) => {
            let el = dom::create_element("pre");
            // The entire code block is the element's own text — plain, no
            // highlighting, never parsed as markup.
            dom::set_text(el, text);
            dom::append(parent, el);
        }
        Block::Blockquote(children) => {
            let el = dom::create_element("blockquote");
            for child in children {
                append_block(el, child);
            }
            dom::append(parent, el);
        }
        Block::Rule => {
            let el = dom::create_element("hr");
            dom::append(parent, el);
        }
    }
}

/// Append a run of inline nodes under `parent`.
fn append_inlines(parent: dom::Node, inlines: &[Inline]) {
    for inline in inlines {
        append_inline(parent, inline);
    }
}

/// Append one inline node under `parent`.
fn append_inline(parent: dom::Node, inline: &Inline) {
    match inline {
        // Its own `span`: text is set on an element, so a bare run sitting
        // between styled siblings needs one to occupy.
        Inline::Text(text) => {
            let el = dom::create_element("span");
            dom::set_text(el, text);
            dom::append(parent, el);
        }
        Inline::Code(text) => {
            let el = dom::create_element("code");
            dom::set_text(el, text);
            dom::append(parent, el);
        }
        Inline::Styled { style, children } => {
            let el = dom::create_element(style_tag(*style));
            append_inlines(el, children);
            dom::append(parent, el);
        }
        Inline::HardBreak => {
            let el = dom::create_element("br");
            dom::append(parent, el);
        }
    }
}

/// The `<h1>`..`<h6>` tag for a heading level (already `1..=6` from the parser).
fn heading_tag(level: u8) -> &'static str {
    match level {
        1 => "h1",
        2 => "h2",
        3 => "h3",
        4 => "h4",
        5 => "h5",
        6 => "h6",
        other => unreachable!("heading level out of range: {other}"),
    }
}

/// The element tag for an inline emphasis style.
fn style_tag(style: Style) -> &'static str {
    match style {
        Style::Emphasis => "em",
        Style::Strong => "strong",
        Style::Strikethrough => "s",
    }
}

//! The protobar component — browser target only.
//!
//! Registers `<brenn-protobar>` via the optional component-support helpers,
//! installs the module panic hook, and wires the three shell → component port
//! events to the DOM-free [`crate::logic::ProtobarState`]. Receive-only: it
//! never dispatches a publish. Every text run reaches the DOM as a text node
//! (`createTextNode`) or `set_text_content`, so a message body is inert text
//! regardless of content.
//!
//! The message area is a block tree, not a single string: the DOM-free
//! [`crate::markdown`] tree is walked here with `createElement` (a fixed
//! block/inline tag set) and `createTextNode` — never `innerHTML`, and never an
//! anchor element (links degrade to text; a chrome-less kiosk has no way back
//! from a navigation). No HTML string is ever produced, so injection is
//! impossible by construction.
//!
//! Priority slots expire on a wall clock, so the glue reads the browser clock
//! (`js_sys::Date::now`), stamps every render with it, and schedules a single
//! `setTimeout` to re-render when the next live slot expires — cancelling any
//! previously scheduled one. A malformed publisher body is reported via a
//! `COMPONENT_LOG` error (not a panic) so one buggy publisher cannot brick a bar
//! showing other publishers' live messages.

use std::cell::RefCell;
use std::rc::Rc;

use brenn_surface_component_support::{
    Activation, PersistentTimer, Publisher, append, append_node, boot, claim_initialized,
    clamp_timeout_ms, component_log, create_div, create_element, create_text_node, document,
    read_now_utc, register_component,
};
use brenn_surface_proto::LogLevel;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::{Document, HtmlElement};

use crate::logic::{Display, Ingest, ProtobarState};
use crate::markdown::{Block, Inline, Style};

/// This component's kind — its config `kind`, its element-tag stem
/// (`brenn-<kind>`), and the `component` field of its panic events.
const KIND: &str = "protobar";

/// A page-lifetime closure that reads the clock, renders both divs, and
/// reschedules the expiry timer.
type Tick = Rc<dyn Fn()>;

/// The loader's entry, called once after this module's `default` init with the
/// instance this module record was loaded for. The whole boot sequence lives
/// here rather than in `#[wasm_bindgen(start)]`: the panic hook's subject and the
/// element's tag are both this instance's, and neither exists until the bind.
#[wasm_bindgen]
pub fn brenn_bind_instance(instance: String) {
    boot(&instance);
    // This instance's state and its render closure, captured by both the
    // connected closure and the activation entry: one module record backs one
    // instance, so these are that instance's and nobody else's. That is what lets
    // p1 and p2 — two declarations of this one kind — each keep their own slots.
    let state = Rc::new(RefCell::new(ProtobarState::new()));
    let wiring: Rc<RefCell<Option<Wiring>>> = Rc::new(RefCell::new(None));
    register_component(
        KIND,
        {
            let state = Rc::clone(&state);
            let wiring = Rc::clone(&wiring);
            move |host| on_connected(host, &state, &wiring)
        },
        {
            let state = Rc::clone(&state);
            let wiring = Rc::clone(&wiring);
            move |activation: &Activation, _publisher: &mut Publisher| {
                on_activation(activation, &state, &wiring);
                Ok(())
            }
        },
    );
}

/// What an activation needs from the built element: the host to log against and
/// the render closure to run. `None` until `connectedCallback` builds them.
struct Wiring {
    host: HtmlElement,
    tick: Tick,
}

/// Build the element's two child divs and its render closure, invoked from the
/// element's `connectedCallback` with the host element as `this`.
fn on_connected(
    host: HtmlElement,
    state: &Rc<RefCell<ProtobarState>>,
    wiring: &Rc<RefCell<Option<Wiring>>>,
) {
    // Build exactly once per element: `connectedCallback` fires on every
    // insertion, so a re-insertion must not duplicate the UI or the timer.
    if !claim_initialized(&host, KIND) {
        return;
    }

    let doc = document();

    let message = create_div(&doc, "data-protobar-message");
    let status = create_div(&doc, "data-protobar-status");
    append(&host, &message);
    append(&host, &status);

    let tick = make_ticker(message, status, Rc::clone(state));
    // Render the initial "awaiting data" state before any delivery.
    tick();

    *wiring.borrow_mut() = Some(Wiring { host, tick });
}

/// Feed each activation's new messages to the pure state machine, then render
/// once — not once per message. A malformed body is a publisher fault: log it and
/// carry on (the counter shows in the status line via the trailing render).
fn on_activation(
    activation: &Activation,
    state: &Rc<RefCell<ProtobarState>>,
    wiring: &Rc<RefCell<Option<Wiring>>>,
) {
    let wiring = wiring.borrow();
    // No divs yet, so no ticker and nothing to log against. The activation is
    // still consumed; its messages remain visible as context in a later window
    // while retention covers them, and the first render runs on connect.
    let Some(wiring) = wiring.as_ref() else {
        return;
    };
    let now = read_now_utc();
    for window in &activation.ports {
        if window.dropped > 0 {
            state
                .borrow_mut()
                .on_drops(&window.port, window.dropped)
                .expect("an activation window satisfies the protobar contract");
        }
        for envelope in window.new_envelopes() {
            let envelope_json =
                serde_json::to_string(envelope).expect("a MessageEnvelope serializes to JSON");
            let ingest = state
                .borrow_mut()
                .on_message(&window.port, &envelope_json, now)
                .expect("an activation window satisfies the protobar contract");
            if let Ingest::Malformed(report) = ingest {
                component_log(
                    &wiring.host,
                    LogLevel::Error,
                    &report.log_message("protobar body"),
                );
            }
        }
    }
    // Once per activation, not once per message: only the surviving slots are
    // displayed, so rendering each superseded message would be wasted DOM work.
    (wiring.tick)();
}

/// Build the page-lifetime render/reschedule closure. The expiry timer is a
/// [`PersistentTimer`] (one fire closure, reused): each `tick` re-renders and, if
/// a live slot has a future expiry, reschedules the timer with a clamped delay,
/// or cancels it when nothing is pending expiry.
fn make_ticker(
    message: HtmlElement,
    status: HtmlElement,
    state: Rc<RefCell<ProtobarState>>,
) -> Tick {
    let timer = Rc::new(PersistentTimer::new());

    // The previously rendered display, so an unchanged message subtree is not
    // torn down and rebuilt on every port event.
    let last: Rc<RefCell<Option<Display>>> = Rc::new(RefCell::new(None));
    let tick: Tick = {
        let timer = Rc::clone(&timer);
        Rc::new(move || {
            let now = read_now_utc();
            let display = state.borrow().display(now);
            // Rebuild the message subtree only when the displayed message or its
            // priority actually changed. An activation that moved only the
            // drops/malformed counters moves only the status line, so it skips
            // the DOM walk (and reflow) entirely.
            let rebuild_message = last.borrow().as_ref().is_none_or(|prev| {
                prev.message != display.message || prev.priority != display.priority
            });
            render(&message, &status, &display, rebuild_message);
            *last.borrow_mut() = Some(display);
            match state.borrow().next_expiry(now) {
                Some(target) => {
                    let delay = clamp_timeout_ms(now, target, None);
                    timer.reschedule(delay);
                }
                None => timer.cancel(),
            }
        })
    };
    timer.set_callback(Rc::clone(&tick));
    tick
}

/// Write both child divs from `display`. The status line is always rewritten
/// (cheap); the message subtree is rebuilt only when `rebuild_message` is set —
/// the caller clears it when the displayed message and priority are unchanged.
/// The message area is rebuilt from the block tree via DOM-API construction (the
/// D12 injection guarantee); the displayed priority becomes a `data-priority`
/// styling hook, removed when no message occupies the bar.
fn render(message: &HtmlElement, status: &HtmlElement, display: &Display, rebuild_message: bool) {
    if rebuild_message {
        // Clear the previous render, then walk the block tree. `set_text_content`
        // with `None` removes all child nodes.
        message.set_text_content(None);
        let doc = document();
        for block in &display.message {
            append_block(&doc, message, block);
        }
        match display.priority {
            Some(urgency) => message
                .set_attribute("data-priority", urgency.as_str())
                .expect("set data-priority attribute"),
            None => message
                .remove_attribute("data-priority")
                .expect("remove data-priority attribute"),
        }
    }
    status.set_text_content(Some(&display.status_text));
}

/// Append one block's DOM subtree under `parent`. Recursion is bounded by the
/// markdown tree's depth cap, so this cannot overflow the stack on hostile input.
fn append_block(doc: &Document, parent: &HtmlElement, block: &Block) {
    match block {
        Block::Paragraph(children) => {
            let el = create_element(doc, "p");
            append_inlines(doc, &el, children);
            append(parent, &el);
        }
        Block::Heading { level, children } => {
            let el = create_element(doc, heading_tag(*level));
            append_inlines(doc, &el, children);
            append(parent, &el);
        }
        Block::List {
            ordered,
            start,
            items,
        } => {
            let el = create_element(doc, if *ordered { "ol" } else { "ul" });
            // A non-1 start is only meaningful on an ordered list.
            if *ordered && *start != 1 {
                el.set_attribute("start", &start.to_string())
                    .expect("set list start attribute");
            }
            for item in items {
                let li = create_element(doc, "li");
                for child in item {
                    append_block(doc, &li, child);
                }
                append(&el, &li);
            }
            append(parent, &el);
        }
        Block::CodeBlock(text) => {
            let el = create_element(doc, "pre");
            // The entire code block is one literal text node — plain text, no
            // highlighting, never parsed as markup.
            append_node(&el, create_text_node(doc, text).as_ref());
            append(parent, &el);
        }
        Block::Blockquote(children) => {
            let el = create_element(doc, "blockquote");
            for child in children {
                append_block(doc, &el, child);
            }
            append(parent, &el);
        }
        Block::Rule => append(parent, &create_element(doc, "hr")),
    }
}

/// Append a run of inline nodes under `parent`.
fn append_inlines(doc: &Document, parent: &HtmlElement, inlines: &[Inline]) {
    for inline in inlines {
        append_inline(doc, parent, inline);
    }
}

/// Append one inline node under `parent`.
fn append_inline(doc: &Document, parent: &HtmlElement, inline: &Inline) {
    match inline {
        Inline::Text(text) => append_node(parent, create_text_node(doc, text).as_ref()),
        Inline::Code(text) => {
            let el = create_element(doc, "code");
            append_node(&el, create_text_node(doc, text).as_ref());
            append(parent, &el);
        }
        Inline::Styled { style, children } => {
            let el = create_element(doc, style_tag(*style));
            append_inlines(doc, &el, children);
            append(parent, &el);
        }
        Inline::HardBreak => append(parent, &create_element(doc, "br")),
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

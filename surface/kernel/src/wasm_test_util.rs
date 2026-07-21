//! Shared browser-test helpers for the kernel's wasm-bindgen test suites.
//!
//! `dom.rs` and `entry.rs` run their `#[wasm_bindgen_test]` fns in one browser
//! page per test binary, so the helpers here give every test a fresh
//! `#surface-root`, parse CustomEvent detail the way the kernel does, capture
//! `console.warn`, and watch window/element events through a guard that removes
//! its listener on drop — a leaked listener firing in a later test would break
//! the shared-page suite.

use std::cell::RefCell;
use std::rc::Rc;

use crate::contract::SURFACE_ROOT_ID;
use js_sys::Reflect;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use web_sys::{CustomEvent, Document, Element, Event, EventTarget, HtmlElement};

/// Define a custom element named `tag` whose `connectedCallback` calls
/// `connected` with the upgraded host, so an inserted element actually upgrades
/// and fires. Idempotent: a tag already defined is left alone. This is the one
/// place the inline-JS element shim and its closure plumbing live; tests that
/// need a specific `connectedCallback` behaviour layer it on top of this.
pub(crate) fn define_test_element(tag: &str, connected: impl Fn(HtmlElement) + 'static) {
    let closure = Closure::<dyn Fn(HtmlElement)>::new(connected);
    let define = js_sys::Function::new_with_args(
        "tag, connected",
        "if (customElements.get(tag)) { return; }\n\
         class E extends HTMLElement { connectedCallback() { connected(this); } }\n\
         customElements.define(tag, E);",
    );
    define
        .call2(
            &JsValue::NULL,
            &JsValue::from_str(tag),
            closure.as_ref().unchecked_ref(),
        )
        .expect("define the test custom element");
    closure.forget();
}

/// The live document. The kernel only runs in a browser, so both `window` and
/// `document` are always present.
pub(crate) fn doc() -> Document {
    web_sys::window()
        .expect("window")
        .document()
        .expect("document")
}

/// Remove any existing `#surface-root` and append a fresh empty one to `body`,
/// returning it. Root-delegated listeners die with the removed element, so each
/// test gets a clean listener/DOM slate.
pub(crate) fn fresh_root() -> Element {
    let d = doc();
    if let Some(existing) = d.get_element_by_id(SURFACE_ROOT_ID) {
        existing.remove();
    }
    let root = d.create_element("div").expect("create #surface-root");
    root.set_id(SURFACE_ROOT_ID);
    d.body()
        .expect("test page has a body")
        .append_child(&root)
        .expect("append #surface-root");
    root
}

/// Read a named string field from a detail object, or `None` if missing or
/// non-string.
pub(crate) fn str_field(detail: &JsValue, key: &str) -> Option<String> {
    Reflect::get(detail, &JsValue::from_str(key))
        .ok()
        .and_then(|v| v.as_string())
}

/// Swap `console.warn` for a capturing closure for the duration of `body`,
/// restore it after, and return the captured single-arg messages.
/// `web_sys::console::warn_1` calls the live global `console.warn`, so the swap
/// is observed.
pub(crate) fn capture_console_warn<F: FnOnce()>(body: F) -> Vec<String> {
    let console = Reflect::get(js_sys::global().as_ref(), &JsValue::from_str("console"))
        .expect("global console");
    let original = Reflect::get(&console, &JsValue::from_str("warn")).expect("console.warn");
    let captured: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&captured);
    let closure = Closure::<dyn Fn(JsValue)>::new(move |msg: JsValue| {
        sink.borrow_mut().push(msg.as_string().unwrap_or_default());
    });
    Reflect::set(
        &console,
        &JsValue::from_str("warn"),
        closure.as_ref().unchecked_ref(),
    )
    .expect("install console.warn capture");
    body();
    Reflect::set(&console, &JsValue::from_str("warn"), &original).expect("restore console.warn");
    drop(closure);
    captured.borrow().clone()
}

/// Removes an event listener when dropped and holds its backing `Closure`, so a
/// test's captured listener cannot fire in a later test on the shared page.
pub(crate) struct ListenerGuard {
    target: EventTarget,
    name: String,
    closure: Closure<dyn Fn(Event)>,
}

impl Drop for ListenerGuard {
    fn drop(&mut self) {
        self.target
            .remove_event_listener_with_callback(&self.name, self.closure.as_ref().unchecked_ref())
            .expect("remove event listener");
    }
}

/// A sink of captured CustomEvent `detail`s and the guard keeping its listener
/// installed. Reading the sink is `.0.borrow()`; the listener is removed when
/// the guard (`.1`) drops.
pub(crate) type DetailWatch = (Rc<RefCell<Vec<JsValue>>>, ListenerGuard);

fn watch_target(target: EventTarget, name: &str) -> DetailWatch {
    let sink: Rc<RefCell<Vec<JsValue>>> = Rc::new(RefCell::new(Vec::new()));
    let s = Rc::clone(&sink);
    let closure = Closure::<dyn Fn(Event)>::new(move |event: Event| {
        let detail = event
            .dyn_ref::<CustomEvent>()
            .map(|ce| ce.detail())
            .unwrap_or(JsValue::NULL);
        s.borrow_mut().push(detail);
    });
    target
        .add_event_listener_with_callback(name, closure.as_ref().unchecked_ref())
        .expect("add event listener");
    (
        sink,
        ListenerGuard {
            target,
            name: name.to_string(),
            closure,
        },
    )
}

/// Watch `name` on `window`, recording each event's `detail` (or `NULL` for a
/// non-`CustomEvent`). The returned guard removes the listener when dropped.
pub(crate) fn watch_window(name: &str) -> DetailWatch {
    let window = web_sys::window().expect("window");
    watch_target(window.unchecked_into(), name)
}

/// Install a `window` listener for `name`, run `body`, remove the listener, and
/// return the captured details. For synchronous dispatches; async tests that
/// must observe events across `.await` points hold a [`watch_window`] guard.
pub(crate) fn capture_window_event<F: FnOnce()>(name: &str, body: F) -> Vec<JsValue> {
    let (sink, guard) = watch_window(name);
    body();
    let captured = sink.borrow().clone();
    drop(guard);
    captured
}

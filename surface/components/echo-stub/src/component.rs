//! The echo-stub component — browser target only.
//!
//! Registers the `brenn-echo-stub` custom element and installs the module panic
//! hook via [`brenn_surface_component_support`], then joins activation delivery:
//! the kernel calls its entry once per activation with every bound port
//! windowed, and its buttons dispatch `brenn-port-publish` on the host element
//! (the contract's dispatch-origin rule).
//!
//! The conformance fixture for the seam: its scrollback shows each activation's
//! new envelopes and its status line the summed `dropped`.

use std::cell::RefCell;
use std::rc::Rc;

use brenn_surface_component_support::{
    Activation, Publisher, add_listener, append, boot, claim_initialized, create_button,
    create_div, create_input, document, publish, register_component,
};
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::HtmlElement;

/// This component's kind — its config `kind`, its element-tag stem
/// (`brenn-<kind>`), and the `component` field of the panic event it dispatches.
const KIND: &str = "echo-stub";

/// The single output port this fixture publishes on, matching the dev-config
/// `[[surface.output]]` binding.
const OUTPUT_PORT: &str = "out";

/// Cap on retained scrollback entries: once exceeded, the oldest `<div>` is
/// dropped so the DOM node set stays bounded for the page lifetime.
const MAX_SCROLLBACK_ENTRIES: u32 = 100;

/// The loader's entry, called once after this module's `default` init with the
/// instance this module record was loaded for. The whole boot sequence lives
/// here rather than in `#[wasm_bindgen(start)]`: the panic hook's subject and the
/// element's tag are both this instance's, and neither exists until the bind.
#[wasm_bindgen]
pub fn brenn_bind_instance(instance: String) {
    boot(&instance);
    // The scrollback and status line this instance's activations write to. Built
    // here, captured by both closures: one module record backs one instance, so
    // this is that instance's state and nobody else's.
    let view: Rc<RefCell<Option<View>>> = Rc::new(RefCell::new(None));
    let state = Rc::new(RefCell::new(EchoState::default()));
    register_component(
        KIND,
        {
            let view = Rc::clone(&view);
            let state = Rc::clone(&state);
            move |host| on_connected(host, &view, &state)
        },
        {
            let view = Rc::clone(&view);
            let state = Rc::clone(&state);
            move |activation: &Activation, _publisher: &mut Publisher| {
                on_activation(activation, &view, &state);
                Ok(())
            }
        },
    );
}

/// The elements an activation writes to, published by `on_connected` once the UI
/// exists. `None` until then: an activation can legitimately arrive before the
/// element is inserted, and it renders nothing rather than inventing a DOM.
struct View {
    status: HtmlElement,
    scrollback: HtmlElement,
}

/// Per-instance counters shown in the status line.
#[derive(Default)]
struct EchoState {
    drops: u64,
    sent: u64,
}

/// Build the element's UI and wire its listeners, invoked from the element's
/// `connectedCallback` with the host element as `this`.
fn on_connected(
    host: HtmlElement,
    view: &Rc<RefCell<Option<View>>>,
    state: &Rc<RefCell<EchoState>>,
) {
    // Build exactly once per element: `connectedCallback` fires on every
    // insertion, so a re-insertion must not duplicate the UI or listeners.
    if !claim_initialized(&host, KIND) {
        return;
    }

    let doc = document();

    let status = create_div(&doc, "data-echo-status");
    status.set_text_content(Some("awaiting data"));
    let scrollback = create_div(&doc, "data-echo-scrollback");
    let send = create_button(&doc, "data-echo-send", "send");
    // A free-form body field plus its own send button: the counter "send" above
    // publishes a fixed body, this one publishes whatever the field holds
    // verbatim — the path a test drives to publish a structured/markdown body.
    let custom_input = create_input(&doc, "data-echo-input");
    let send_custom = create_button(&doc, "data-echo-send-custom", "send custom");
    let panic_btn = create_button(&doc, "data-echo-panic", "panic");

    append(&host, &status);
    append(&host, &scrollback);
    append(&host, &send);
    append(&host, custom_input.as_ref());
    append(&host, &send_custom);
    append(&host, &panic_btn);

    // Publish the view: from here an activation has somewhere to render.
    *view.borrow_mut() = Some(View {
        status: status.clone(),
        scrollback,
    });
    update_status(&status, &state.borrow());

    // Component → shell: publish a counter body on the output port, dispatched on
    // the host element itself per the contract's dispatch-origin rule. A button
    // click is a gesture publish — no activation is in flight, so it goes out
    // immediately and unanswered (the contract's named, bounded gap).
    {
        let host = host.clone();
        let status = status.clone();
        let state = Rc::clone(state);
        add_listener(send.as_ref(), "click", move |_event| {
            let n = {
                let mut s = state.borrow_mut();
                s.sent += 1;
                s.sent
            };
            publish(&host, OUTPUT_PORT, &format!("echo-stub message #{n}"));
            update_status(&status, &state.borrow());
        });
    }
    // Component → shell: publish the free-form body field's current value
    // verbatim, so a test can feed the bus a structured or markdown body.
    {
        let host = host.clone();
        let status = status.clone();
        let state = Rc::clone(state);
        add_listener(send_custom.as_ref(), "click", move |_event| {
            state.borrow_mut().sent += 1;
            publish(&host, OUTPUT_PORT, &custom_input.value());
            update_status(&status, &state.borrow());
        });
    }
    // Exercise the panic path from a real component module.
    add_listener(panic_btn.as_ref(), "click", |_event| {
        panic!("echo-stub panic button pressed");
    });
}

/// Render one activation: append every window's **new** envelopes to the
/// scrollback and fold the windows' `dropped` deltas into the status line.
///
/// The context ahead of `new_from` is deliberately not rendered: it is messages
/// this instance has already seen and already scrolled back, still in the view
/// only because retention has not displaced them. Rendering it would redraw the
/// scrollback's own history on every activation.
fn on_activation(
    activation: &Activation,
    view: &Rc<RefCell<Option<View>>>,
    state: &Rc<RefCell<EchoState>>,
) {
    let view = view.borrow();
    // No UI yet — the element has not been inserted. The activation is still
    // consumed (delivery-consumes, contract §doctrine); its messages remain
    // visible as context in a later window while retention covers them.
    let Some(view) = view.as_ref() else {
        return;
    };
    let doc = document();
    let dropped = activation.total_dropped();
    for window in &activation.ports {
        for envelope in window.new_envelopes() {
            let entry = create_div(&doc, "data-echo-message");
            entry.set_text_content(Some(
                &serde_json::to_string(envelope).expect("a MessageEnvelope serializes to JSON"),
            ));
            append(&view.scrollback, &entry);
        }
    }
    // Bound the scrollback: drop the oldest entries once past the cap so the DOM
    // node set cannot grow without limit for the page lifetime.
    while view.scrollback.child_element_count() > MAX_SCROLLBACK_ENTRIES {
        let oldest = view
            .scrollback
            .first_child()
            .expect("child_element_count > 0 implies a first child");
        view.scrollback
            .remove_child(&oldest)
            .expect("remove the oldest scrollback entry");
    }
    if dropped > 0 {
        state.borrow_mut().drops += dropped;
    }
    update_status(&view.status, &state.borrow());
}

/// Update the status line with the running counters.
fn update_status(status: &HtmlElement, state: &EchoState) {
    status.set_text_content(Some(&format!(
        "sent: {}  drops: {}",
        state.sent, state.drops
    )));
}

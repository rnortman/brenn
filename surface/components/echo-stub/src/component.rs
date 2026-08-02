//! The echo-stub component — browser target only.
//!
//! Registers the `brenn-echo-stub` custom element and installs the module panic
//! hook via [`brenn_surface_component_support`], then joins activation delivery:
//! the kernel calls its entry once per activation with every bound port
//! windowed, and its buttons ask for a sync-call activation of their own so the
//! publish they cause is made from inside one.
//!
//! The conformance fixture for the seam: its scrollback shows each activation's
//! new envelopes and its status line the summed `dropped`.

use std::cell::RefCell;
use std::rc::Rc;

use brenn_surface_component_support::{
    Activation, Publisher, add_listener, append, boot, claim_initialized, create_button,
    create_div, create_input, document, publish_or_fault, register_component, wire_gesture,
};
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::HtmlElement;

/// This component's kind — its config `kind`, its element-tag stem
/// (`brenn-<kind>`), and the `component` field of the panic event it dispatches.
const KIND: &str = "echo-stub";

/// The single output port this fixture publishes on, matching the dev-config
/// `[[surface.output]]` binding.
const OUTPUT_PORT: &str = "out";

/// The sync port the counter button's press arrives on. Chosen by this component
/// at request time and bound to nothing: it must only avoid colliding with an
/// input port name, which the kernel refuses.
const SEND_PORT: &str = "send";

/// The sync port the free-form field's press arrives on, carrying the field's
/// value as the request body.
const SEND_CUSTOM_PORT: &str = "send-custom";

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
            move |activation: &Activation, publisher: &mut Publisher| {
                on_activation(activation, &view, &state, publisher);
                Ok(None)
            }
        },
    );
}

/// The elements an activation writes to, published by `on_connected` once the UI
/// exists. `None` only before that: `register_component` hands the kernel the
/// entry after `on_connected` returns, so every activation finds a view.
struct View {
    /// The element a report is logged against.
    host: HtmlElement,
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
        host: host.clone(),
        status: status.clone(),
        scrollback,
    });
    update_status(&status, &state.borrow());

    // The counter button carries no payload — its body is the counter the entry
    // keeps — so it encodes nothing.
    wire_gesture(&host, send.as_ref(), "click", SEND_PORT, |_event| {
        String::new()
    });
    // The kernel never looks inside an element, so the read happens here — a
    // test feeds the bus an arbitrary body through this field.
    wire_gesture(
        &host,
        send_custom.as_ref(),
        "click",
        SEND_CUSTOM_PORT,
        move |_event| custom_input.value(),
    );
    // Exercise the panic path from a real component module.
    add_listener(panic_btn.as_ref(), "click", |_event| {
        panic!("echo-stub panic button pressed");
    });
}

/// Render one activation: publish whatever a button press asked for, append every
/// window's **new** envelopes to the scrollback, and fold the windows' `dropped`
/// deltas into the status line.
///
/// The context ahead of `new_from` is deliberately not rendered: it is messages
/// this instance has already seen and already scrolled back, still in the view
/// only because retention has not displaced them. Rendering it would redraw the
/// scrollback's own history on every activation.
fn on_activation(
    activation: &Activation,
    view: &Rc<RefCell<Option<View>>>,
    state: &Rc<RefCell<EchoState>>,
    publisher: &mut Publisher,
) {
    let view = view.borrow();
    let view = view
        .as_ref()
        .expect("on_connected builds the view before the entry is registered");
    if let Some((port, press)) = activation.sync_request() {
        on_gesture(port, &press.body, view, state, publisher);
    }
    let doc = document();
    let dropped = activation.total_dropped();
    for window in activation.delivered_windows() {
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

/// Publish for one button press, from inside the activation the press caused.
///
/// The counter is bumped here rather than in the wiring closure because this is
/// where the publish either happens or does not: a refusal that left the counter
/// advanced would make the status line claim a message the bus never saw.
fn on_gesture(
    port: &str,
    press: &str,
    view: &View,
    state: &Rc<RefCell<EchoState>>,
    publisher: &mut Publisher,
) {
    let body = match port {
        SEND_PORT => {
            let n = state.borrow().sent + 1;
            format!("echo-stub message #{n}")
        }
        SEND_CUSTOM_PORT => press.to_string(),
        other => panic!("echo-stub wired no gesture to sync port {other:?}"),
    };
    if publish_or_fault(publisher, &view.host, OUTPUT_PORT, &body) {
        state.borrow_mut().sent += 1;
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

// No host-testable half — this is a DOM fixture. Run via `make surface-wasm-test`.
#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;

    use brenn_surface_contract::{PublishError, element_name_for_instance, publish_status_str};
    use brenn_surface_test_fixtures::browser::{
        activation_json, answer_publishes_with, mount, record_ops, take_recorded,
    };
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    const TEST_INSTANCE: &str = "wbt-echo-stub";

    /// The activation clock. Nothing here reads it; it is present because every
    /// activation carries one.
    const NOW_MS: u64 = 1_770_000_123_456;

    /// The body the free-form field is filled with before its button is clicked.
    const CUSTOM_BODY: &str = "a body only the field knows";

    /// Click a button inside the mounted element the way a user does, from page
    /// script.
    fn click(instance: &str, marker: &str) {
        js_sys::Function::new_with_args("selector", "document.querySelector(selector).click();")
            .call1(
                &JsValue::NULL,
                &JsValue::from_str(&format!(
                    "{} [{marker}]",
                    element_name_for_instance(KIND, instance)
                )),
            )
            .expect("click the button");
    }

    /// Type into the free-form field from page script, so the encoder reads a
    /// value the test never handed it.
    fn fill_custom_input(instance: &str, value: &str) {
        js_sys::Function::new_with_args(
            "selector, value",
            "document.querySelector(selector).value = value;",
        )
        .call2(
            &JsValue::NULL,
            &JsValue::from_str(&format!(
                "{} [data-echo-input]",
                element_name_for_instance(KIND, instance)
            )),
            &JsValue::from_str(value),
        )
        .expect("fill the custom-body field");
    }

    /// The status line's current text.
    fn status_text(instance: &str) -> String {
        document()
            .query_selector(&format!(
                "{} [data-echo-status]",
                element_name_for_instance(KIND, instance)
            ))
            .expect("query the status line")
            .expect("the status line is in the document")
            .text_content()
            .unwrap_or_default()
    }

    /// The whole component through its own seams: connect-time code renders and
    /// wires only, the mount activation publishes nothing, each button asks for a
    /// sync activation rather than publishing, the publish happens inside the
    /// activation the press caused, and a refused publish leaves the counter
    /// where it was.
    ///
    /// One test rather than five: the bind is once-per-binary, so a test binary
    /// gets exactly one mount. The acting halves are what make the two silences
    /// worth asserting — they are the proof that a publish and a sync request
    /// reaching these seams do land in this array.
    #[wasm_bindgen_test]
    fn connecting_is_silent_and_a_press_publishes_from_its_own_activation() {
        let ops = record_ops();
        let (entry, _host) = mount(KIND, TEST_INSTANCE, brenn_bind_instance);
        assert_eq!(
            ops.length(),
            0,
            "connect-time code builds the UI and installs listeners, and reaches no kernel seam"
        );

        entry
            .call1(
                &JsValue::NULL,
                &JsValue::from_str(&activation_json(&[], None, NOW_MS)),
            )
            .expect("the entry returns ok on the mount activation");
        assert_eq!(
            ops.length(),
            0,
            "the mount activation delivered nothing and no press caused it, so nothing is published"
        );

        entry
            .call1(
                &JsValue::NULL,
                &JsValue::from_str(&activation_json(
                    &[(SEND_PORT, "")],
                    Some(SEND_PORT),
                    NOW_MS,
                )),
            )
            .expect("the entry returns ok on the press");

        click(TEST_INSTANCE, "data-echo-send");

        assert_eq!(
            take_recorded(&ops),
            vec![
                vec![
                    "publish".to_string(),
                    String::new(),
                    OUTPUT_PORT.to_string(),
                    "echo-stub message #1".to_string(),
                    String::new(),
                ],
                vec![
                    "sync".to_string(),
                    String::new(),
                    SEND_PORT.to_string(),
                    String::new(),
                    String::new(),
                ],
            ],
            "the press publishes once from the entry's publisher, and the button itself only asks \
             for the activation"
        );
        assert_eq!(status_text(TEST_INSTANCE), "sent: 1  drops: 0");

        // The custom-body path: the entry forwards the field's value verbatim.
        fill_custom_input(TEST_INSTANCE, CUSTOM_BODY);
        click(TEST_INSTANCE, "data-echo-send-custom");
        assert_eq!(
            take_recorded(&ops),
            vec![vec![
                "sync".to_string(),
                String::new(),
                SEND_CUSTOM_PORT.to_string(),
                CUSTOM_BODY.to_string(),
                String::new(),
            ]],
            "the custom button encodes the field's value into the request body"
        );

        entry
            .call1(
                &JsValue::NULL,
                &JsValue::from_str(&activation_json(
                    &[(SEND_CUSTOM_PORT, CUSTOM_BODY)],
                    Some(SEND_CUSTOM_PORT),
                    NOW_MS,
                )),
            )
            .expect("the entry returns ok on the custom press");
        assert_eq!(
            take_recorded(&ops),
            vec![vec![
                "publish".to_string(),
                String::new(),
                OUTPUT_PORT.to_string(),
                CUSTOM_BODY.to_string(),
                String::new(),
            ]],
            "the custom press publishes the request body unchanged"
        );
        assert_eq!(status_text(TEST_INSTANCE), "sent: 2  drops: 0");

        // A quota-refused publish: the message did not reach the bus, so the
        // status line must not claim it did.
        answer_publishes_with(&ops, publish_status_str(Err(PublishError::QuotaExceeded)));
        entry
            .call1(
                &JsValue::NULL,
                &JsValue::from_str(&activation_json(
                    &[(SEND_PORT, "")],
                    Some(SEND_PORT),
                    NOW_MS,
                )),
            )
            .expect("a refused publish is an answer, not a failed activation");
        let refused = take_recorded(&ops);
        assert_eq!(
            refused
                .iter()
                .map(|row| (row[0].as_str(), row[2].as_str()))
                .collect::<Vec<_>>(),
            vec![("publish", OUTPUT_PORT), ("log", "")],
            "the refusal is attempted once and reported once"
        );
        assert_eq!(
            status_text(TEST_INSTANCE),
            "sent: 2  drops: 0",
            "a refused publish leaves the counter where it was"
        );
    }
}

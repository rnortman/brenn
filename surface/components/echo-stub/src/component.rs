//! The echo-stub component — page-hosted target only.
//!
//! Builds its UI in the mount activation and joins activation delivery: the
//! kernel calls `receive` once per activation with every bound port windowed,
//! and its buttons ask for a sync-call activation of their own, so the publish
//! a press causes is made from inside one.
//!
//! The conformance fixture for the page-hosted seam: its scrollback shows each
//! activation's new envelopes, its status line the summed `dropped`, and its
//! third button traps, which is how the error-card path is exercised from a
//! real component.

use std::cell::RefCell;
use std::collections::VecDeque;

use brenn_guest::{Activation, Error, Processor, dom, log, publish};

use crate::spec::{InPort, port::OUT};

/// The sync port the counter button's press arrives on. Not bound to any
/// input port; must not collide with one.
const SEND_PORT: dom::SyncPort = dom::SyncPort("send");

/// The sync port the free-form field's send button arrives on.
const SEND_CUSTOM_PORT: dom::SyncPort = dom::SyncPort("send-custom");

/// The sync port the panic button arrives on. Answering it traps.
const PANIC_PORT: dom::SyncPort = dom::SyncPort("panic");

/// Cap on retained scrollback entries: once exceeded, the oldest entry is
/// removed, which bounds what the page renders, what this component holds, and
/// what the host holds — `dom::remove` destroys the entry's subtree and reclaims
/// every handle in it.
const MAX_SCROLLBACK_ENTRIES: usize = 100;

const AWAITING: &str = "awaiting data";

// One instantiation backs one instance for the page's lifetime, so the view
// handles and the counters are ordinary interior-mutable module state. Handles
// are page-lifetime too: the element a handle names on the mount activation is
// the same element on every activation after it.
thread_local! {
    static ECHO: RefCell<EchoStub> = const { RefCell::new(EchoStub::new()) };
}

struct EchoStub {
    /// The elements an activation writes to, built by the mount activation.
    view: Option<View>,
    /// Scrollback entries oldest-first, so the cap detaches from the front.
    ///
    /// The component's own bookkeeping rather than a read of the DOM: the
    /// capability offers no traversal, and a component that built the subtree
    /// knows what is in it.
    entries: VecDeque<dom::Node>,
    drops: u64,
    sent: u64,
}

struct View {
    status: dom::Node,
    scrollback: dom::Node,
    /// The free-form body field, read at press time through [`dom::value`].
    input: dom::Node,
}

impl EchoStub {
    const fn new() -> EchoStub {
        EchoStub {
            view: None,
            entries: VecDeque::new(),
            drops: 0,
            sent: 0,
        }
    }

    /// The view, which every activation after the mount one has.
    fn view(&self) -> &View {
        self.view
            .as_ref()
            .expect("the mount activation builds the view before any other call")
    }
}

struct EchoStubComponent;

impl Processor for EchoStubComponent {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        ECHO.with(|echo| on_activation(&activation, &mut echo.borrow_mut()))?;
        // Mount and both send gestures are answered with nothing: the mount
        // call has no reply dialect, and neither button's default action is
        // one this component cancels.
        Ok(None)
    }
}

brenn_guest::export_processor!(EchoStubComponent);

/// Handle one activation: build the UI when this is the mount call, publish
/// what a press asked for, then fold every delivered window into the scrollback
/// and the counters.
///
/// A mount activation windows whatever input was already pending, so the build
/// and the fold both run on it — a component is never told why it woke.
fn on_activation(activation: &Activation, echo: &mut EchoStub) -> Result<(), Error> {
    if activation.sync_is(dom::MOUNT) {
        echo.view = Some(build_view());
    } else if let Some(port) = activation.sync() {
        on_gesture(port, echo)?;
    }
    let mut new_entries = 0usize;
    for window in activation.delivered_windows() {
        // Matched through the specification enum so a rename fails at build
        // time rather than at runtime on the page.
        let InPort::Messages = InPort::of(window)?;
        echo.drops += u64::from(window.dropped());
        for envelope in window.new_raw() {
            // Only new envelopes are rendered: the context is what this
            // instance already scrolled back, still in the window only because
            // retention has not displaced it.
            let entry = dom::create_element("div");
            dom::set_attribute(entry, "data-echo-message", "");
            dom::set_text(entry, envelope);
            dom::append(echo.view().scrollback, entry);
            echo.entries.push_back(entry);
            new_entries += 1;
        }
    }
    if new_entries > 0 {
        trim_scrollback(echo);
    }
    update_status(echo);
    Ok(())
}

/// Destroy the oldest entries past the cap, dropping this component's handles
/// with them — `remove` reclaims a handle along with the node it names, so a
/// retained one would only trap on its next use.
fn trim_scrollback(echo: &mut EchoStub) {
    while echo.entries.len() > MAX_SCROLLBACK_ENTRIES {
        let oldest = echo
            .entries
            .pop_front()
            .expect("a deque longer than the cap has a front");
        dom::remove(oldest);
    }
}

/// The counter is bumped where the publish either happens or does not: a
/// refusal that left it advanced would make the status line claim a message the
/// bus never saw.
fn on_gesture(port: &str, echo: &mut EchoStub) -> Result<(), Error> {
    let body = if port == SEND_PORT {
        format!("echo-stub message #{}", echo.sent + 1)
    } else if port == SEND_CUSTOM_PORT {
        // The sync-call body carries no element content, so the field must be
        // read here. The sync call runs on the press's own event stack, so
        // this is the field's value at press time.
        dom::value(echo.view().input)
    } else if port == PANIC_PORT {
        panic!("echo-stub panic button pressed");
    } else {
        return Err(Error::failed(format!(
            "echo-stub wired no gesture to sync port {port:?}"
        )));
    };
    match publish(OUT, &body) {
        Ok(()) => echo.sent += 1,
        // Quota is the one refusal a conforming deployment produces transiently,
        // and the counter stays where it was so the status line never claims a
        // message the bus did not see. Anything else — an unbound port, a body
        // over the cap — is structural: no later press repairs it, so the first
        // one is the detection and the instance takes its error card.
        Err(err) => {
            assert!(
                err.is_quota(),
                "echo-stub: the publish on {OUT:?} was refused: {err:?}"
            );
            log::error(format!("publish on {OUT:?} refused: quota exceeded"));
        }
    }
    Ok(())
}

/// Build the UI under this instance's host element and wire its three gestures.
///
/// Called once, from the mount activation. Listeners are the kernel's and are
/// page-lifetime: each press arrives as a sync-call activation on its port.
fn build_view() -> View {
    let root = dom::root();

    let status = dom::marked("div", "data-echo-status");
    dom::set_text(status, AWAITING);
    let scrollback = dom::marked("div", "data-echo-scrollback");
    let send = button("data-echo-send", "send");
    let input = dom::marked("input", "data-echo-input");
    dom::set_attribute(input, "type", "text");
    let send_custom = button("data-echo-send-custom", "send custom");
    let panic_button = button("data-echo-panic", "panic");

    for child in [status, scrollback, send, input, send_custom, panic_button] {
        dom::append(root, child);
    }

    dom::listen(send, "click", SEND_PORT);
    dom::listen(send_custom, "click", SEND_CUSTOM_PORT);
    dom::listen(panic_button, "click", PANIC_PORT);

    View {
        status,
        scrollback,
        input,
    }
}

fn button(marker: &str, label: &str) -> dom::Node {
    let node = dom::marked("button", marker);
    dom::set_text(node, label);
    node
}

fn update_status(echo: &EchoStub) {
    dom::set_text(
        echo.view().status,
        &format!("sent: {}  drops: {}", echo.sent, echo.drops),
    );
}

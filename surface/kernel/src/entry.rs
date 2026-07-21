//! wasm-bindgen entry point and kernel-facing handle.
//!
//! Browser target only. [`start`] is the bootstrap's single entry into the
//! kernel: it installs the panic hook, reads the page's surface metas, wires a
//! client instance to a [`WebSysConnector`], spawns the client
//! driver and the kernel's event loop, and returns a [`KernelHandle`] the
//! bootstrap holds for its post-kernel error path.

use std::cell::RefCell;
use std::rc::Rc;

use futures_util::StreamExt;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::contract::element_name_for_instance;
use crate::proto::LogLevel;
use crate::{ClientConfig, ClientHandle, Event, EventStream, WebSysConnector, new};

use crate::dom;
use crate::logic::{
    ConnectIndicatorState, KernelAction, KernelCore, malformed_registration, route_component_alert,
    route_component_log, route_processor_alert, route_processor_log, route_publish_intent,
};

/// Bring the kernel online and hand the bootstrap a handle to it.
///
/// Installs the kernel's panic hook (which dispatches `brenn-surface-reload` so
/// the bootstrap's capped reload heals a kernel death), reads `surface-slug` +
/// `brenn-build-id` from the page metas, derives the WS URL from
/// `window.location`, constructs the surface client over a [`WebSysConnector`],
/// renders the initial `Connecting` connect indicator, and `spawn_local`s the
/// client driver and the kernel's event loop. The event loop folds each client `Event` through
/// [`KernelCore`] and applies the resulting actions to the DOM.
#[wasm_bindgen]
pub fn start() -> KernelHandle {
    // The panic hook must be installed before anything can panic: a panic during
    // start() itself or the first synchronous stretch of a spawned task must land
    // on the bootstrap's already-installed reload listener.
    std::panic::set_hook(Box::new(|info| {
        dom::report_panic(&info.to_string());
    }));

    // Stamp the page-uptime origin as early as possible so a status report's
    // uptime measures from kernel start.
    dom::mark_page_start();

    let window = web_sys::window().expect("surface kernel: no window");
    let document = window.document().expect("surface kernel: no document");

    let slug = meta_content(&document, "surface-slug");
    let build_id = meta_content(&document, "brenn-build-id");

    let location = window.location();
    let protocol = location
        .protocol()
        .expect("surface kernel: location has no protocol");
    let host = location
        .host()
        .expect("surface kernel: location has no host");
    let url = crate::logic::ws_url(&protocol, &host, &slug);

    let config = ClientConfig {
        url,
        build_id,
        ..ClientConfig::default()
    };
    let (handle, events, driver) = new(config, WebSysConnector::new());
    let handle = Rc::new(handle);

    // The DOM-free decision core is shared: `run_event_loop` folds control-plane
    // events through it, and the delegated alert listener reads its
    // `alert_granted()` flag to gate a `brenn-alert` forward. Both touches are
    // short synchronous borrows on the single-threaded page, never overlapping
    // (the event loop's borrow is released before any DOM effect that could
    // re-enter via a component event).
    let core = Rc::new(RefCell::new(KernelCore::new()));

    // Route component publish intents. The delegated `#surface-root` listener
    // hands each `brenn-port-publish` event's (retargeted host) tag and its
    // untrusted `{ port, body }` detail to the DOM-free `route_publish_intent`,
    // which decides route-vs-drop against the mounted-element registry; the
    // single resulting action is applied to the DOM. A misrouted or malformed
    // publish becomes a `Report`, never a bus message.
    //
    // Well-formed publishes then fork on one question the kernel alone can answer:
    // is this instance the one whose activation entry is on the stack? Activations
    // are serialized per instance and synchronous on the one JS thread, so exactly
    // one instance can be mid-activation — if it is this one, the publish belongs
    // in that activation's buffer (quota-checked inline, flushed only if the entry
    // returns ok, answered synchronously on the detail). Anything else is a
    // gesture publish and takes the immediate path. No new event, no mode flag,
    // and nothing the component has to know.
    {
        let handle = Rc::clone(&handle);
        dom::install_publish_listener(move |instance, target_tag, port, body, urgency, detail| {
            let action = route_publish_intent(instance, target_tag, port, body, urgency);
            if let KernelAction::Publish {
                instance,
                port,
                body,
                urgency,
            } = &action
                && let Some(status) = handle.try_buffered_publish(instance, port, body, *urgency)
            {
                // Buffered: counted like any other publish (which route the kernel
                // took is not something a status reader should have to know), and
                // answered on the detail before this dispatch returns.
                dom::count_publish(instance);
                dom::set_publish_status(detail, status);
                return;
            }
            dom::apply_actions(std::slice::from_ref(&action), &handle);
        });
    }

    // Route component activation registrations. The delegated `#surface-root`
    // listener hands each `brenn-activation-register` event's `entry` function and
    // its resolved instance to the core's gate, which admits exactly one
    // registration per mounted instance; only an admitted one reaches the client
    // core, whose own `RegisterActivation` bound panics on a duplicate or unknown
    // instance. That bound is the backstop for a *kernel* bug and must never be
    // reached by a *component* bug — a component's double registration is a
    // contained fault report, not a dead page.
    {
        let handle = Rc::clone(&handle);
        let core = Rc::clone(&core);
        dom::install_activation_register_listener(move |instance, target_tag, entry| {
            // Checked before the gate, deliberately: the gate *consumes* the
            // instance's one registration, and spending it on a detail carrying no
            // entry would lock a component out of ever registering a real one.
            let Some(entry) = entry else {
                dom::apply_actions(
                    std::slice::from_ref(&malformed_registration(instance, target_tag)),
                    &handle,
                );
                return;
            };
            let (admitted, actions) = core
                .borrow_mut()
                .on_activation_register(instance, target_tag);
            dom::apply_actions(&actions, &handle);
            let Some(instance) = admitted else { return };
            handle.register_activation(&instance, dom::wrap_activation_entry(&instance, entry));
        });
    }

    // Route component log intents. The delegated `#surface-root` listener hands
    // each `brenn-log` event's (retargeted host) tag and its untrusted
    // `{ level, message }` detail to the DOM-free `route_component_log`, which
    // resolves the mounted component, stamps `source = "component:<kind>"`, and
    // emits a `Log` frame; a misrouted or malformed log becomes a `Report`,
    // never a mis-attributed server log line.
    {
        let handle = Rc::clone(&handle);
        dom::install_log_listener(move |instance, target_tag, level, message| {
            let action = route_component_log(instance, target_tag, level, message);
            dom::apply_actions(std::slice::from_ref(&action), &handle);
        });
    }

    // Route component alert intents. The delegated `#surface-root` listener hands
    // each `brenn-alert` event's (retargeted host) tag and its untrusted
    // `{ severity, title, body }` detail to the DOM-free `route_component_alert`,
    // which — only on an alert-granted surface (`KernelCore::alert_granted`) —
    // emits an `Alert` frame; an ungranted surface yields a `log(warn)`
    // suppression breadcrumb, and a misrouted or malformed alert a drop-report. A
    // conforming kernel never sends an ungranted `Alert` (the server kills on one).
    {
        let handle = Rc::clone(&handle);
        let core = Rc::clone(&core);
        dom::install_alert_listener(move |instance, target_tag, severity, title, body| {
            let action = route_component_alert(
                instance,
                target_tag,
                severity,
                title,
                body,
                core.borrow().alert_granted(),
            );
            dom::apply_actions(std::slice::from_ref(&action), &handle);
        });
    }

    // Route component-module panics. A component's panic hook dispatches
    // `brenn-component-panic { component, message }` on `window`; the DOM-free
    // core turns the (untrusted) detail into an error-card + report for the named
    // mounted component, or a drop-and-report for an unattributable one. On an
    // alert-granted surface an attributed panic additionally pages — the one
    // client-side event that does. The borrow is a short synchronous borrow
    // released before any DOM effect, so it never overlaps the event loop's fold.
    {
        let handle = Rc::clone(&handle);
        let core = Rc::clone(&core);
        dom::install_component_panic_listener(move |kind, message| {
            let actions = core
                .borrow_mut()
                .on_component_panic(kind, message, dom::is_mounted);
            dom::apply_actions(&actions, &handle);
        });
    }

    // Render the pre-chrome connect indicator before any `Welcome`: the second
    // kernel-owned pixel class (design §6.1), removed the moment chrome first
    // mounts (or, on a chrome-less surface, at the first `Connected`).
    dom::render_connect_indicator(ConnectIndicatorState::Connecting);

    // Publish the kernel's host seam for headless processor instances. The DOM
    // seam is delegated events on `#surface-root`; a processor has no element, so
    // its imports are direct calls from the bootstrap loader's shims into the
    // free functions below, which read this cell. Set before the driver runs so a
    // loader that instantiates the moment the kernel is up finds it populated.
    PROCESSOR_HOST.with(|cell| {
        *cell.borrow_mut() = Some(ProcessorHost {
            core: Rc::clone(&core),
            handle: Rc::clone(&handle),
        });
    });

    spawn_local(driver.run());
    spawn_local(run_event_loop(Rc::clone(&core), events, Rc::clone(&handle)));

    KernelHandle { handle }
}

/// The kernel state the processor host entry points act on, published by
/// [`start`] for the page's lifetime.
struct ProcessorHost {
    core: Rc<RefCell<KernelCore>>,
    handle: Rc<ClientHandle>,
}

thread_local! {
    /// `None` until [`start`] runs. A processor host call before that is a
    /// bootstrap-ordering bug — the loader cannot have a module to call from
    /// until the kernel handed it one — so the accessor panics rather than
    /// silently dropping a component's publish.
    static PROCESSOR_HOST: RefCell<Option<ProcessorHost>> = const { RefCell::new(None) };
}

/// Run `f` against the published processor host.
fn with_processor_host<R>(what: &str, f: impl FnOnce(&ProcessorHost) -> R) -> R {
    PROCESSOR_HOST.with(|cell| {
        let borrow = cell.borrow();
        let host = borrow.as_ref().unwrap_or_else(|| {
            panic!("surface kernel: {what} before start() published the processor host")
        });
        f(host)
    })
}

/// A processor instance's `ports.publish` / `ports.publish-with-urgency` import.
///
/// `instance` comes from the loader's own closure over the manifest entry it
/// instantiated the module for — never from the component, exactly as the DOM
/// path's instance comes from the executor's element registry rather than the
/// event detail.
///
/// A processor only ever publishes from inside its own `receive`, so the buffered
/// path always takes it: the publish joins that activation's buffer, is
/// quota-checked inline, and flushes only if the entry returns ok. The answer is
/// the WIT `publish-error` string the guest lifts, or the empty string for ok. A
/// `None` from `try_buffered_publish` means no activation of this instance is in
/// flight — a component publishing outside `receive`, which its world gives it no
/// way to do — so it is refused rather than laundered onto the unbuffered gesture
/// path a headless component has no business taking.
#[wasm_bindgen]
pub fn brenn_processor_publish(
    instance: &str,
    port: &str,
    body: &str,
    urgency: Option<String>,
) -> String {
    let urgency = match urgency {
        Some(raw) => match crate::Urgency::parse(&raw) {
            Some(urgency) => Some(urgency),
            // The guest's WIT enum lifts to a fixed string set, so an
            // unrecognized value is transpile-glue drift, not a component typo.
            None => return "invalid-payload".to_string(),
        },
        None => None,
    };
    with_processor_host("processor publish", |host| {
        match host
            .handle
            .try_buffered_publish(instance, port, body, urgency)
        {
            Some(Ok(())) => {
                dom::count_publish(instance);
                String::new()
            }
            Some(Err(err)) => crate::logic::publish_error_str(err),
            // TODO(surface-wasm-test-in-ci): this None arm (absent host slot →
            // "not-permitted") depends on the live wasm host slot and can only
            // be pinned by the browser test runner, unlike the variant map,
            // which is natively tested in `logic`.
            None => "not-permitted".to_string(),
        }
    })
}

/// A processor instance's `log.*` import: one component log line, attributed to
/// the instance, on the same plane a `dom` component's `brenn-log` reaches.
#[wasm_bindgen]
pub fn brenn_processor_log(instance: &str, level: &str, message: &str) {
    let action = route_processor_log(instance, level, message);
    with_processor_host("processor log", |host| {
        dom::apply_actions(std::slice::from_ref(&action), &host.handle);
    });
}

/// A processor instance's `alert.*` import. Gated on the surface's alert grant
/// exactly as the DOM path is: boot proved the grant for a kind that imports
/// `alert`, and this is the runtime half of that same gate — a conforming kernel
/// never emits an ungranted `Alert`.
#[wasm_bindgen]
pub fn brenn_processor_alert(instance: &str, severity: &str, title: &str, body: &str) {
    with_processor_host("processor alert", |host| {
        let granted = host.core.borrow().alert_granted();
        let action = route_processor_alert(instance, severity, title, body, granted);
        dom::apply_actions(std::slice::from_ref(&action), &host.handle);
    });
}

/// A processor instance's `config.get` import. Answers from the instance's map as
/// it rode `Welcome`; a miss is `None`, which is the import's own `option<string>`.
#[wasm_bindgen]
pub fn brenn_processor_config_get(instance: &str, key: &str) -> Option<String> {
    with_processor_host("processor config get", |host| {
        host.core.borrow().processor_config_get(instance, key)
    })
}

/// Register a headless processor instance's `receive` export with the kernel.
///
/// The tail is `handle.register_activation` — the DOM path's tail, unchanged — but
/// the admission ahead of it is [`KernelCore::on_processor_register`], because the
/// DOM gate resolves its instance from a mounted element and a processor has
/// none. Returns whether the registration was admitted, so the loader can tell a
/// refusal from success without reading kernel state.
#[wasm_bindgen]
pub fn brenn_processor_register(instance: &str, entry: js_sys::Function) -> bool {
    with_processor_host("processor register", |host| {
        let (admitted, actions) = host.core.borrow_mut().on_processor_register(instance);
        dom::apply_actions(&actions, &host.handle);
        if admitted {
            host.handle
                .register_activation(instance, dom::wrap_activation_entry(instance, entry));
        }
        admitted
    })
}

/// Report that a processor instance could not be brought up — module import,
/// `instantiate`, or registration failure in the bootstrap loader.
///
/// A headless instance has no wrapper, so there is no error card to render: the
/// `failed` status row and its `surface-state` publish are the observable, plus
/// the death report. One instance's failure is one instance's; its siblings have
/// their own instantiation and their own linear memory.
#[wasm_bindgen]
pub fn brenn_processor_load_failed(instance: &str, detail: &str) {
    with_processor_host("processor load failure", |host| {
        let actions = host
            .core
            .borrow_mut()
            .on_processor_load_failed(instance, detail);
        dom::apply_actions(&actions, &host.handle);
    });
}

/// Drain the client event stream, folding each `Event` through the DOM-free core
/// and applying the emitted actions. The `is_element_defined` predicate the core
/// consults on first connect asks the live `customElements` registry whether the
/// component's element is registered; a missing registration error-cards that
/// mount (per the core's mount plan).
async fn run_event_loop(
    core: Rc<RefCell<KernelCore>>,
    mut events: EventStream,
    handle: Rc<ClientHandle>,
) {
    let registry = web_sys::window()
        .expect("surface kernel: no window")
        .custom_elements();
    // The surface-description telemetry listeners (resize + status tick) are
    // page-lifetime and installed once, the first time a `Welcome` says the
    // feature is on. Installing after the fold means the core already knows the
    // feature is on when the startup viewport read fires.
    let mut telemetry_installed = false;
    while let Some(event) = events.next().await {
        // Borrow the shared core only for the synchronous fold; the borrow is
        // released before any effect below, so a component event that re-enters
        // the core (e.g. the alert listener reading `alert_granted`) never
        // overlaps this mutable borrow.
        let actions = core.borrow_mut().on_event(&event, |kind, instance| {
            !registry
                .get(&element_name_for_instance(kind, instance))
                .is_undefined()
        });
        // Walk the actions in the order the core emitted them. Every one is a
        // web-sys effect the DOM executor runs; the core emits no task-spawn
        // action.
        for action in &actions {
            dom::apply_action(action, &handle);
        }
        if !telemetry_installed
            && let Event::Connected {
                surface_description,
                ..
            } = &event
        {
            telemetry_installed = true;
            install_telemetry(
                surface_description.status_interval_secs,
                Rc::clone(&core),
                Rc::clone(&handle),
            );
        }
    }
}

/// Install the surface-description telemetry observers: a debounced `window`
/// resize listener that folds each viewport reading through
/// [`KernelCore::on_viewport_changed`], and a periodic status-tick timer that
/// folds through [`KernelCore::on_status_tick`]. Both apply the core's emitted
/// actions to the DOM (the resulting `SendGeometry`/`SendStatus` reaches the
/// client's best-effort telemetry channel). Called once per page.
fn install_telemetry(
    status_interval_secs: u32,
    core: Rc<RefCell<KernelCore>>,
    handle: Rc<ClientHandle>,
) {
    {
        let core = Rc::clone(&core);
        let handle = Rc::clone(&handle);
        dom::install_resize_listener(move |width, height, device_pixel_ratio| {
            let actions = core
                .borrow_mut()
                .on_viewport_changed(width, height, device_pixel_ratio);
            dom::apply_actions(&actions, &handle);
        });
    }
    {
        let core = Rc::clone(&core);
        let handle = Rc::clone(&handle);
        dom::install_status_timer(status_interval_secs, move || {
            let actions = core.borrow_mut().on_status_tick();
            dom::apply_actions(&actions, &handle);
        });
    }
}

/// Read a required `<meta name="…" content="…">` from the page. The served page
/// always carries both surface metas; an absent one means a broken deploy,
/// so this panics (house policy — our own page, never attacker-reachable).
fn meta_content(document: &web_sys::Document, name: &str) -> String {
    document
        .query_selector(&format!("meta[name=\"{name}\"]"))
        .expect("surface kernel: meta selector is valid")
        .and_then(|el| el.get_attribute("content"))
        .unwrap_or_else(|| panic!("surface kernel: missing <meta name=\"{name}\">"))
}

/// The bootstrap's handle to the running kernel. Its one method is the post-kernel
/// error path: the bootstrap forwards a caught global error here, which delegates
/// to the client's best-effort leveled `log` at `Error` — every bootstrap-caught
/// global error (uncaught error, unhandled rejection, kernel panic) is error-level
/// and never pages.
#[wasm_bindgen]
pub struct KernelHandle {
    handle: Rc<ClientHandle>,
}

#[wasm_bindgen]
impl KernelHandle {
    /// Forward a bootstrap-caught global error at `Error` level: write the
    /// browser-console copy, then hand it to [`ClientHandle::report`], which
    /// publishes it to the reserved error-report port when the advertised floor
    /// admits `Error` (best-effort; console-only otherwise or when down).
    ///
    /// No report subject: a global error is caught at the window, which attests
    /// nothing about which component's code raised it. `source` is the
    /// bootstrap's untrusted best guess and stays body detail; guessing a
    /// subject from it would attribute the error — and its budget draw — to a
    /// component on no evidence. The report carries the bare surface identity.
    pub fn log_error(&self, message: &str, source: &str) {
        web_sys::console::error_1(&wasm_bindgen::JsValue::from_str(message));
        self.handle.report(LogLevel::Error, source, message, None);
    }
}

// Browser-level integration tests for the entry wiring. Run via
// `make surface-wasm-test` under a headless WebDriver browser; the whole module
// is wasm32-only, so the host `cargo test` sweep never compiles them. They drive
// `run_event_loop` directly (not `start()`) over a scripted fake connector.
// Isolation: each test starts from `fresh_root` and uses a unique `wbt-*`
// component kind (custom-element registrations and `MOUNTED` entries are
// page-lifetime and cannot be removed).
#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::time::Duration;

    use crate::Event as ClientEvent;
    use crate::contract::{ACTIVATION_REGISTER, SURFACE_READY, SURFACE_RELOAD};
    use crate::proto::{AlertSeverity, Binding, ClientFrame, OutputBinding, Urgency};
    use crate::{TransportConnection, TransportConnector, TransportError, TransportEvent};
    use brenn_surface_test_fixtures::{
        WelcomeParams, deliver_frame, subscribe_result_ok, welcome_frame, wire_cursor,
    };
    use futures_channel::mpsc;
    use js_sys::{Object, Promise, Reflect};
    use wasm_bindgen_futures::JsFuture;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
    use web_sys::{CustomEvent, CustomEventInit, Element, HtmlElement};

    use crate::wasm_test_util::{
        capture_console_warn, define_test_element, doc, fresh_root, str_field, watch_window,
    };

    wasm_bindgen_test_configure!(run_in_browser);

    /// A recorder for what the kernel delivers to a component, keyed by instance:
    /// each entry is `(instance, activation_json)`.
    type ActivationSink = Rc<RefCell<Vec<(String, String)>>>;

    /// Define `instance`'s instance-scoped element so it upgrades on insertion.
    /// Its `connectedCallback` records the host once (dedup across reparents) and,
    /// on first connect, dispatches `ACTIVATION_REGISTER` carrying a JS entry that
    /// records every activation JSON the kernel later invokes it with, tagged with
    /// the element's own `data-instance`.
    ///
    /// This is the activation-era stand-in for the old dialect: the kernel's
    /// `run_event_loop` wires the dispatched entry through
    /// `handle.register_activation`, so a `Deliver` reaches it exactly as it would
    /// a real component built on the component-support SDK.
    fn define_recording_element(
        instance: &str,
        kind: &str,
        hosts: Rc<RefCell<Vec<HtmlElement>>>,
        sink: ActivationSink,
    ) {
        define_test_element(
            &element_name_for_instance(kind, instance),
            move |host: HtmlElement| {
                if hosts
                    .borrow()
                    .iter()
                    .any(|h| h.is_same_node(Some(host.as_ref())))
                {
                    return;
                }
                hosts.borrow_mut().push(host.clone());
                let inst = host.get_attribute("data-instance").unwrap_or_default();
                let sink = Rc::clone(&sink);
                let entry = Closure::<dyn FnMut(JsValue) -> JsValue>::new(move |json: JsValue| {
                    sink.borrow_mut()
                        .push((inst.clone(), json.as_string().unwrap_or_default()));
                    JsValue::UNDEFINED
                });
                let detail = Object::new();
                Reflect::set(&detail, &JsValue::from_str("entry"), entry.as_ref())
                    .expect("set entry on the registration detail");
                let init = CustomEventInit::new();
                init.set_detail(&detail);
                init.set_bubbles(true);
                init.set_composed(true);
                let event = CustomEvent::new_with_event_init_dict(ACTIVATION_REGISTER, &init)
                    .expect("construct the registration event");
                host.dispatch_event(&event)
                    .expect("dispatch the registration event");
                entry.forget();
            },
        );
    }

    /// The bodies delivered as *new* to `instance`, across every recorded
    /// activation, decoded from the activation JSON.
    fn new_bodies_for(sink: &ActivationSink, instance: &str) -> Vec<String> {
        let mut bodies = Vec::new();
        for (inst, json) in sink.borrow().iter() {
            if inst != instance {
                continue;
            }
            let activation: crate::contract::Activation =
                serde_json::from_str(json).expect("recorded activation JSON decodes");
            for window in &activation.ports {
                for env in &window.envelopes[window.new_from as usize..] {
                    bodies.push(env.body.clone());
                }
            }
        }
        bodies
    }

    // ── fake connector ────────────────────────────────────────────────────

    type EventTx = mpsc::UnboundedSender<TransportEvent>;
    type EventRx = mpsc::UnboundedReceiver<TransportEvent>;

    /// Scripts the connect sequence and captures the frames the driver sent. One
    /// connection (its scripted inbound `TransportEvent` stream) is queued per
    /// expected connect; a connect past the script errors (a retryable outcome,
    /// never a panic), which the terminal-leg reconnect relies on staying quiet.
    #[derive(Clone)]
    struct FakeControls {
        conns: Rc<RefCell<VecDeque<EventRx>>>,
        sent: Rc<RefCell<Vec<String>>>,
        connect_count: Rc<Cell<usize>>,
    }

    impl FakeControls {
        fn new() -> Self {
            Self {
                conns: Rc::new(RefCell::new(VecDeque::new())),
                sent: Rc::new(RefCell::new(Vec::new())),
                connect_count: Rc::new(Cell::new(0)),
            }
        }

        /// Queue the next connect to succeed; returns the sender that pushes
        /// inbound transport events into that connection.
        fn add_connection(&self) -> EventTx {
            let (tx, rx) = mpsc::unbounded();
            self.conns.borrow_mut().push_back(rx);
            tx
        }

        fn connector(&self) -> FakeConnector {
            FakeConnector { ctrl: self.clone() }
        }

        fn connect_count(&self) -> usize {
            self.connect_count.get()
        }

        fn sent(&self) -> Vec<String> {
            self.sent.borrow().clone()
        }
    }

    struct FakeConnector {
        ctrl: FakeControls,
    }

    impl TransportConnector for FakeConnector {
        type Conn = FakeConnection;

        async fn connect(&mut self, _url: &str) -> Result<FakeConnection, TransportError> {
            self.ctrl
                .connect_count
                .set(self.ctrl.connect_count.get() + 1);
            match self.ctrl.conns.borrow_mut().pop_front() {
                Some(incoming) => Ok(FakeConnection {
                    incoming,
                    sent: Rc::clone(&self.ctrl.sent),
                }),
                None => Err(TransportError::new("fake connector: script exhausted")),
            }
        }
    }

    struct FakeConnection {
        incoming: EventRx,
        sent: Rc<RefCell<Vec<String>>>,
    }

    impl TransportConnection for FakeConnection {
        async fn send_text(&mut self, text: String) -> Result<(), TransportError> {
            self.sent.borrow_mut().push(text);
            Ok(())
        }

        async fn next_event(&mut self) -> TransportEvent {
            match self.incoming.next().await {
                Some(event) => event,
                // The test dropped the sender: model it as a peer close.
                None => TransportEvent::Closed {
                    code: None,
                    reason: String::new(),
                },
            }
        }

        async fn close(&mut self) {}
    }

    // ── async + DOM helpers ───────────────────────────────────────────────

    /// Resolve after `ms` via `setTimeout`, yielding to the microtask queue so
    /// the spawned driver and event-loop tasks make progress between polls.
    async fn sleep_ms(ms: i32) {
        let promise = Promise::new(&mut |resolve, _reject| {
            web_sys::window()
                .expect("window")
                .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, ms)
                .expect("setTimeout");
        });
        JsFuture::from(promise).await.expect("timeout resolves");
    }

    /// Poll `pred` every few ms until it holds; panic (naming `desc`, so a
    /// timeout points at the failed condition) after the bound rather than hang.
    async fn wait_until(desc: &str, mut pred: impl FnMut() -> bool) {
        for _ in 0..2_000 {
            if pred() {
                return;
            }
            sleep_ms(5).await;
        }
        panic!("wait_until({desc}): condition never held within the bound");
    }

    /// The kernel-owned wrapper element for `instance`, if present, so a test can
    /// observe a component's card without a DOM-executor accessor.
    fn instance_wrapper(instance: &str) -> Option<Element> {
        doc().get_element_by_id(&crate::dom::wrapper_id(instance))
    }

    /// The text of `instance`'s error card, if its wrapper currently holds one.
    fn error_card_text(instance: &str) -> Option<String> {
        instance_wrapper(instance)?
            .query_selector("[data-surface-error]")
            .expect("query_selector")
            .and_then(|card| card.text_content())
    }

    /// Whether a parsed `ClientFrame` matching `pred` is in the sent transcript.
    fn sent_has(ctrl: &FakeControls, pred: impl Fn(&ClientFrame) -> bool) -> bool {
        ctrl.sent()
            .iter()
            .filter_map(|f| serde_json::from_str::<ClientFrame>(f).ok())
            .any(|f| pred(&f))
    }

    /// A client config with near-instant backoff so the terminal leg's one
    /// scripted reconnect resolves promptly under the poll loop.
    fn config() -> ClientConfig {
        ClientConfig {
            url: "ws://localhost/surface/wbt/ws".into(),
            build_id: "wbt-build".into(),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(10),
            ..ClientConfig::default()
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn run_event_loop_wires_mount_deliver_and_reconnect() {
        const REG: &str = "wbt-entry-reg";
        const UNREG: &str = "wbt-entry-unreg";
        fresh_root();

        // Register the mounted component; its connectedCallback records the host
        // and registers a recording activation entry.
        let connected: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        let activations: ActivationSink = Rc::new(RefCell::new(Vec::new()));
        define_recording_element(REG, REG, Rc::clone(&connected), Rc::clone(&activations));

        let ctrl = FakeControls::new();
        let server1 = ctrl.add_connection();
        let (handle, events, driver) = new(config(), ctrl.connector());
        let handle = Rc::new(handle);
        let core = Rc::new(RefCell::new(KernelCore::new()));
        spawn_local(driver.run());
        spawn_local(run_event_loop(Rc::clone(&core), events, Rc::clone(&handle)));

        let (ready, _ready_c) = watch_window(SURFACE_READY);
        let (reload, _reload_c) = watch_window(SURFACE_RELOAD);

        // First Welcome: REG bound (one subscription) + a second, unregistered
        // kind; no outputs; alert granted.
        server1
            .unbounded_send(TransportEvent::Text(welcome_frame(WelcomeParams {
                subscriptions: vec![Binding {
                    channel: "ephemeral:demo".into(),
                    instance: REG.into(),
                    port: "messages".into(),
                    push_depth: 8,
                    retain_depth: 0,
                    noise: brenn_surface_proto::NoiseLevel::Silent,
                }],
                components: vec![REG, UNREG],
                ..Default::default()
            })))
            .expect("send welcome");

        // The action-walk mounts REG (connectedCallback fires), error-cards the
        // unregistered kind, routes AttachPort to a pump that subscribes, and
        // emits SURFACE_READY last.
        wait_until(
            "REG mounted, UNREG error-carded, Subscribe sent, SURFACE_READY",
            || {
                !connected.borrow().is_empty()
                    && error_card_text(UNREG).as_deref() == Some("component module missing")
                    && sent_has(&ctrl, |f| {
                        matches!(f, ClientFrame::Subscribe { channel, .. } if channel == "ephemeral:demo")
                    })
                    && !ready.borrow().is_empty()
            },
        )
        .await;

        // Activate the subscription so the Deliver below is accepted.
        server1
            .unbounded_send(TransportEvent::Text(subscribe_result_ok(
                "ephemeral:demo",
                REG,
            )))
            .expect("send subscribe result");

        // A Deliver reaches the instance's registered activation entry: the
        // recorder captures the activation, whose window carries the message new.
        server1
            .unbounded_send(TransportEvent::Text(deliver_frame(
                "ephemeral:demo",
                REG,
                "hello",
                1,
                wire_cursor("c1"),
                0,
            )))
            .expect("send deliver");
        wait_until("activation delivered to the mounted instance", || {
            !new_bodies_for(&activations, REG).is_empty()
        })
        .await;
        assert_eq!(
            new_bodies_for(&activations, REG),
            vec!["hello".to_string()],
            "the instance's activation window carries the delivered message"
        );

        // Terminal leg: close, reconnect, second Welcome drops REG's subscription
        // (same components + epoch). The instance stops being activated on the
        // dropped channel (no component-visible binding-removed vocabulary), and
        // the differing-bindings fold requests a reload.
        let server2 = ctrl.add_connection();
        server1
            .unbounded_send(TransportEvent::Closed {
                code: Some(1000),
                reason: "bye".into(),
            })
            .expect("send close");
        wait_until("driver reconnected after close", || {
            ctrl.connect_count() >= 2
        })
        .await;
        server2
            .unbounded_send(TransportEvent::Text(welcome_frame(WelcomeParams {
                components: vec![REG, UNREG],
                ..Default::default()
            })))
            .expect("send second welcome");
        wait_until(
            "SURFACE_RELOAD 'bindings changed' on the dropped binding",
            || {
                reload
                    .borrow()
                    .iter()
                    .any(|d| str_field(d, "reason").as_deref() == Some("bindings changed"))
            },
        )
        .await;
    }

    #[wasm_bindgen_test]
    async fn run_event_loop_routes_two_instances_of_one_kind_independently() {
        // End-to-end multi-instance proof: two instances of one component kind,
        // each bound to its own channel. Each instance mounts its own element and
        // a `Deliver` on one channel's port reaches only that instance's element —
        // the routing key is the instance, not the shared kind.
        // Unique per-test kind: the custom-element registry is page-global across
        // the whole wasm test binary, so a kind shared with another test's
        // registration would double-define and panic.
        const KIND: &str = "wbt-two-e2e";
        fresh_root();

        // One entry per element, in mount order (p1 then p2). `connectedCallback`
        // fires on every *insertion*, not once per element — mount stages the
        // element, and chrome's first arrange reparents its wrapper into a panel
        // — so this records each host once, which is what a conformant component
        // does with its own re-entry guard (`claim_initialized`).
        let hosts: Rc<RefCell<Vec<HtmlElement>>> = Rc::new(RefCell::new(Vec::new()));
        let activations: ActivationSink = Rc::new(RefCell::new(Vec::new()));
        // Each instance has its own instance-scoped element; define both so each
        // upgrades and registers its own recording entry.
        define_recording_element("p1", KIND, Rc::clone(&hosts), Rc::clone(&activations));
        define_recording_element("p2", KIND, Rc::clone(&hosts), Rc::clone(&activations));

        let welcome = {
            use crate::proto::{
                Abi, ComponentEntry, ServerFrame, SurfaceBindings, SurfaceDescription,
            };
            serde_json::to_string(&ServerFrame::Welcome {
                surface: "deskbar".into(),
                participant_id: "surface:deskbar".into(),
                heartbeat_secs: 20,
                max_body_bytes: 65_536,
                alert_granted: true,
                takeover_granted: false,
                error_report_floor: None,
                surface_description: SurfaceDescription {
                    status_interval_secs: 60,
                },
                bindings: SurfaceBindings {
                    components: vec![
                        ComponentEntry {
                            instance: "p1".into(),
                            kind: KIND.into(),
                            abi: Abi::Dom,
                            parked_batch_depth: 8,
                            config: Default::default(),
                        },
                        ComponentEntry {
                            instance: "p2".into(),
                            kind: KIND.into(),
                            abi: Abi::Dom,
                            parked_batch_depth: 8,
                            config: Default::default(),
                        },
                    ],
                    subscriptions: vec![
                        Binding {
                            channel: "ephemeral:a".into(),
                            instance: "p1".into(),
                            port: "messages".into(),
                            push_depth: 8,
                            retain_depth: 0,
                            noise: brenn_surface_proto::NoiseLevel::Silent,
                        },
                        Binding {
                            channel: "ephemeral:b".into(),
                            instance: "p2".into(),
                            port: "messages".into(),
                            push_depth: 8,
                            retain_depth: 0,
                            noise: brenn_surface_proto::NoiseLevel::Silent,
                        },
                    ],
                    outputs: vec![],
                    local_channels: vec![],
                    chrome_instance: String::new(),
                },
            })
            .expect("two-instance welcome serializes")
        };

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let (handle, events, driver) = new(config(), ctrl.connector());
        let handle = Rc::new(handle);
        let core = Rc::new(RefCell::new(KernelCore::new()));
        spawn_local(driver.run());
        spawn_local(run_event_loop(Rc::clone(&core), events, Rc::clone(&handle)));

        server
            .unbounded_send(TransportEvent::Text(welcome))
            .expect("send welcome");

        // Both instances mount (two hosts) and both subscriptions are sent.
        wait_until("both instances mounted and both subscriptions sent", || {
            hosts.borrow().len() == 2
                && sent_has(&ctrl, |f| {
                    matches!(f, ClientFrame::Subscribe { channel, .. } if channel == "ephemeral:a")
                })
                && sent_has(&ctrl, |f| {
                    matches!(f, ClientFrame::Subscribe { channel, .. } if channel == "ephemeral:b")
                })
        })
        .await;

        // Distinct instance ids on distinct sections and elements.
        let p1_host = hosts.borrow()[0].clone();
        let p2_host = hosts.borrow()[1].clone();
        assert_eq!(
            p1_host.get_attribute("data-instance").as_deref(),
            Some("p1")
        );
        assert_eq!(
            p2_host.get_attribute("data-instance").as_deref(),
            Some("p2")
        );
        assert!(
            !p1_host.is_same_node(Some(p2_host.as_ref())),
            "independent elements"
        );

        server
            .unbounded_send(TransportEvent::Text(subscribe_result_ok(
                "ephemeral:a",
                "p1",
            )))
            .expect("activate a");
        server
            .unbounded_send(TransportEvent::Text(subscribe_result_ok(
                "ephemeral:b",
                "p2",
            )))
            .expect("activate b");

        // A deliver on p1's channel activates only p1's instance.
        server
            .unbounded_send(TransportEvent::Text(deliver_frame(
                "ephemeral:a",
                "p1",
                "for-p1",
                1,
                wire_cursor("a1"),
                0,
            )))
            .expect("deliver a");
        wait_until("p1 received its message", || {
            !new_bodies_for(&activations, "p1").is_empty()
        })
        .await;
        assert_eq!(
            new_bodies_for(&activations, "p2"),
            Vec::<String>::new(),
            "p2 did not receive p1's message"
        );

        // A deliver on p2's channel activates only p2's instance.
        server
            .unbounded_send(TransportEvent::Text(deliver_frame(
                "ephemeral:b",
                "p2",
                "for-p2",
                1,
                wire_cursor("b1"),
                0,
            )))
            .expect("deliver b");
        wait_until("p2 received its message", || {
            !new_bodies_for(&activations, "p2").is_empty()
        })
        .await;
        assert_eq!(
            new_bodies_for(&activations, "p1"),
            vec!["for-p1".to_string()],
            "p1 unchanged by p2's message"
        );
    }

    #[wasm_bindgen_test]
    async fn apply_actions_dispatches_dom_and_routes_client_frames() {
        const MK: &str = "wbt-apply-mount";
        const EK: &str = "wbt-apply-err";
        const CK: &str = "wbt-apply-clog";
        fresh_root();

        // A granted connection with no output bindings but the error-report floor
        // advertised at `warn`: a publish to a component port is rejected
        // UnboundPort by the gate, while warn/error reports become reserved-port
        // publishes and an Alert frame reaches the wire.
        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let (handle, events, driver) = new(config(), ctrl.connector());
        let active = Rc::new(Cell::new(false));
        {
            let active = Rc::clone(&active);
            spawn_local(async move {
                let mut events = events;
                while let Some(event) = events.next().await {
                    if matches!(event, ClientEvent::Connected { .. }) {
                        active.set(true);
                    }
                }
            });
        }
        spawn_local(driver.run());
        server
            .unbounded_send(TransportEvent::Text(welcome_frame(WelcomeParams {
                error_report_floor: Some(LogLevel::Warn),
                ..Default::default()
            })))
            .expect("send welcome");
        wait_until("client Connected", || active.get()).await;

        let (ready, _ready_c) = watch_window(SURFACE_READY);

        // DOM-effect actions the executor must apply, observed in the DOM.
        dom::apply_actions(
            &[
                KernelAction::MountComponent {
                    instance: MK.into(),
                    kind: MK.into(),
                },
                KernelAction::ErrorCard {
                    instance: EK.into(),
                    kind: EK.into(),
                    reason: "boom".into(),
                },
                KernelAction::EmitReady,
            ],
            &handle,
        );
        assert!(
            doc()
                .query_selector(&element_name_for_instance(MK, MK))
                .expect("query mounted element")
                .is_some(),
            "MountComponent created the instance's element"
        );
        assert_eq!(
            error_card_text(EK).as_deref(),
            Some("boom"),
            "ErrorCard applied"
        );
        assert_eq!(ready.borrow().len(), 1, "EmitReady fired once");

        // The rejected publish and the warn ComponentLog each console.warn and
        // publish a reserved-port report; the ComponentAlert routes to an Alert.
        let warnings = capture_console_warn(|| {
            dom::apply_actions(
                &[
                    KernelAction::Publish {
                        instance: "nobody".into(),
                        port: "nowhere".into(),
                        body: "b".into(),
                        urgency: None,
                    },
                    KernelAction::ComponentLog {
                        instance: CK.into(),
                        level: LogLevel::Warn,
                        message: "clog".into(),
                    },
                    KernelAction::ComponentAlert {
                        severity: AlertSeverity::Warning,
                        title: "atitle".into(),
                        body: "abody".into(),
                    },
                ],
                &handle,
            );
        });
        assert_eq!(
            warnings.len(),
            2,
            "rejected publish + warn component log each warn once"
        );

        // Both reports become reserved-port publishes — the rejected-publish
        // report (source "kernel") and the ComponentLog (source
        // "component:<kind>") — and the ComponentAlert reaches the wire.
        wait_until(
            "kernel report + component report + component Alert on the wire",
            || {
                error_report_has(&ctrl, "kernel", None)
                    && error_report_has(&ctrl, "component:wbt-apply-clog", Some("clog"))
                    && sent_has(
                        &ctrl,
                        |f| matches!(f, ClientFrame::Alert { title, .. } if title == "atitle"),
                    )
            },
        )
        .await;
    }

    /// A `KernelAction::Publish` counts one against the publishing instance's
    /// `publishes` column, and against no one else's.
    ///
    /// `publishes` is the half of the per-instance breakdown that reads against a
    /// component's send budget — the column an operator consults to answer "which
    /// component drained its budget?". Its sibling tests in `dom` cover the drop
    /// column and assert only that `publishes` holds *still*, so without this the
    /// producer line could be deleted or misrouted and every per-instance
    /// `publishes` value would be permanently zero with the suite green.
    #[wasm_bindgen_test]
    async fn publishes_count_against_the_publishing_instance_only() {
        const A: &str = "wbt-pub-ctr-a";
        const B: &str = "wbt-pub-ctr-b";
        fresh_root();

        let ctrl = FakeControls::new();
        let server = ctrl.add_connection();
        let (handle, events, driver) = new(config(), ctrl.connector());
        let active = Rc::new(Cell::new(false));
        {
            let active = Rc::clone(&active);
            spawn_local(async move {
                let mut events = events;
                while let Some(event) = events.next().await {
                    if matches!(event, ClientEvent::Connected { .. }) {
                        active.set(true);
                    }
                }
            });
        }
        spawn_local(driver.run());
        // Both instances get a real bound output port, so the publish under test
        // takes the accepted path rather than the UnboundPort rejection.
        let out = |instance: &str| OutputBinding {
            channel: "ephemeral:pubctr".into(),
            instance: instance.into(),
            port: "out".into(),
            urgency: Urgency::Normal,
            fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
        };
        server
            .unbounded_send(TransportEvent::Text(welcome_frame(WelcomeParams {
                outputs: vec![out(A), out(B)],
                components: vec![A, B],
                ..Default::default()
            })))
            .expect("send welcome");
        wait_until("client Connected", || active.get()).await;

        let (before_a, before_b) = (dom::instance_counters(A), dom::instance_counters(B));
        dom::apply_actions(
            &[KernelAction::Publish {
                instance: A.into(),
                port: "out".into(),
                body: "b".into(),
                urgency: None,
            }],
            &handle,
        );

        assert_eq!(
            dom::instance_counters(A).publishes - before_a.publishes,
            1,
            "the publishing instance counts exactly one"
        );
        assert_eq!(
            dom::instance_counters(B).publishes,
            before_b.publishes,
            "the sibling's column is untouched"
        );
    }

    /// Whether some sent frame is a `Publish` to the reserved `#brenn`/
    /// `error-reports` port whose body carries `source` (and `message`, if given).
    fn error_report_has(ctrl: &FakeControls, source: &str, message: Option<&str>) -> bool {
        sent_has(ctrl, |f| {
            let ClientFrame::Publish {
                instance,
                port,
                body,
                ..
            } = f
            else {
                return false;
            };
            if instance != "#brenn" || port != "error-reports" {
                return false;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
                return false;
            };
            v["source"] == source && message.is_none_or(|m| v["message"] == m)
        })
    }

    #[wasm_bindgen_test]
    fn meta_content_reads_present_meta() {
        let d = doc();
        let meta = d.create_element("meta").expect("create meta");
        meta.set_attribute("name", "wbt-meta-present")
            .expect("set name");
        meta.set_attribute("content", "the-value")
            .expect("set content");
        d.document_element()
            .expect("document element")
            .append_child(&meta)
            .expect("append meta");
        assert_eq!(meta_content(&d, "wbt-meta-present"), "the-value");
    }

    #[wasm_bindgen_test]
    #[should_panic(expected = "missing <meta")]
    fn meta_content_panics_on_missing_meta() {
        meta_content(&doc(), "wbt-meta-never-present");
    }
}

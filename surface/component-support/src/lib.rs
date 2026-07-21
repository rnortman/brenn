//! Optional in-tree helpers for authoring Brenn surface component modules.
//!
//! This crate is convenience for component authors, **not** contract surface:
//! contract v0 is exactly [`brenn_surface_contract`], and a component —
//! in-tree or out-of-tree — may use these helpers or hand-roll the same web-sys
//! calls. Everything here is behind `cfg(target_arch = "wasm32")`; the host
//! build is empty.
//!
//! What it provides, lifted from the recommended module shape the contract
//! documents:
//!
//! - [`bind_instance`] — called first from the component's own
//!   `brenn_bind_instance` export (the contract's
//!   [`brenn_surface_contract::BIND_INSTANCE_EXPORT`]), naming the instance this
//!   module record was loaded for. Everything below that needs an identity reads
//!   it; nothing may run before it, which is why a component boots from that
//!   export rather than from `#[wasm_bindgen(start)]`.
//! - [`install_panic_hook`] — the never-double-panic hook dispatching
//!   [`brenn_surface_contract::COMPONENT_PANIC`] on `window`, attributed to the
//!   bound instance, so a panic has exactly one subject.
//! - [`register_component`] — the `define_component` custom-element shim (tag
//!   derived from the kind and the bound instance via
//!   [`brenn_surface_contract::element_name_for_instance`]) **plus the activation
//!   entry**: the kernel calls that entry once per activation with every bound
//!   input port windowed, and [`Publisher`] buffers the publishes it makes,
//!   flushing them atomically iff it returns ok.
//! - [`claim_initialized`] — the `connectedCallback` re-entry guard (a
//!   `data-<kind>-initialized` marker) so a re-insertion does not rebuild the UI
//!   or double-register listeners.
//! - DOM builders ([`document`], [`create_div`], [`create_button`],
//!   [`create_input`], [`create_element`], [`create_text_node`], [`append`],
//!   [`append_node`]), the
//!   page-lifetime [`add_listener`], the untrusted-detail readers
//!   ([`string_field`], [`number_field`]), [`detail_object`], the conformant
//!   [`publish`] and [`component_log`] dispatches, and the
//!   [`set_timeout`]/[`clear_timeout`] scheduling helpers.
//! - [`clamp_timeout_ms`] — DOM-free `setTimeout`-delay clamping, host-testable
//!   and shared by every component state machine (ungated, no wasm dependency).
//! - [`fault`] — DOM-free port-delivery validation ([`parse_delivery`],
//!   [`ContractViolation`]) and the shared [`FaultReport`] operator log line,
//!   host-testable and identical across components.
//! - [`PersistentTimer`] — a wasm `setTimeout` closure that reschedules/cancels
//!   without ever being recreated, owning the memory-safety invariant every
//!   component's recompute loop depends on.

mod fault;
mod timeout;
pub use fault::{ContractViolation, FaultReport, parse_delivery};
pub use timeout::clamp_timeout_ms;

// The activation vocabulary a component's handler is written against, re-exported
// from the contract for the same reason the helpers exist at all: an author on
// this SDK is already on the seam and should not restate the dependency to name
// the types the seam hands them. wasm-gated with the contract dep and with every
// consumer of these types (`register_component` and the entry it wraps).
#[cfg(target_arch = "wasm32")]
pub use brenn_surface_contract::{Activation, ActivationError, PortWindow};

/// The recommended maximum `setTimeout` delay for a wall-clock-driven component:
/// recompute at least every ~15 minutes so a wall-clock jump (suspend/resume,
/// NTP, DST) self-corrects within one interval. Pass as `clamp_timeout_ms`'s
/// `max_ms` for a component that reads the clock on each fire.
pub const MAX_WAKEUP_MS: i32 = 15 * 60 * 1000;

#[cfg(target_arch = "wasm32")]
pub use wasm::*;

#[cfg(target_arch = "wasm32")]
mod wasm {
    use brenn_surface_contract::{
        ACTIVATION_REGISTER, Activation, ActivationError, COMPONENT_LOG, COMPONENT_PANIC,
        PORT_PUBLISH, PUBLISH_STATUS_FIELD, PublishError, element_name_for_instance,
        parse_publish_status,
    };
    use brenn_surface_proto::LogLevel;
    use brenn_surface_proto::Urgency;
    use chrono::{DateTime, Utc};
    use js_sys::{Object, Reflect};
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::prelude::wasm_bindgen;
    use wasm_bindgen::{JsCast, JsValue};
    use web_sys::{
        CustomEvent, CustomEventInit, Document, Event, EventTarget, HtmlElement, HtmlInputElement,
        Node, Text,
    };

    /// The browser wall clock as a UTC instant (millisecond floor). Used for
    /// computing timeout *durations* (timezone-independent) and, for components
    /// whose math is entirely in UTC, as the recompute clock. Panics only if the
    /// browser hands back a non-finite millisecond count — a structural
    /// impossibility.
    pub fn read_now_utc() -> DateTime<Utc> {
        let ms = js_sys::Date::now();
        DateTime::<Utc>::from_timestamp_millis(ms as i64)
            .expect("Date::now yields a valid millisecond timestamp")
    }

    /// Milliseconds since the page's time origin, from `performance.now()`.
    ///
    /// Monotonic: unaffected by NTP steps, DST corrections, and suspend/resume
    /// clock jumps, which is why lifetimes measured against it (toast expiry)
    /// use this rather than [`read_now_utc`]. Only ever compared to itself, so
    /// the arbitrary origin is immaterial. Panics if the browser exposes no
    /// `window.performance` — a structural impossibility in a page.
    pub fn read_monotonic_ms() -> u64 {
        let performance = web_sys::window()
            .expect("a component runs in a window")
            .performance()
            .expect("window exposes performance");
        performance.now().max(0.0) as u64
    }

    /// A single page-lifetime `setTimeout` closure that reschedules or cancels
    /// without ever being recreated.
    ///
    /// The fire `Closure` is created once, at construction, and lives inside the
    /// timer for the page. It is *never* dropped and rebuilt while a timeout is
    /// pending — doing so would free the closure environment out from under a
    /// callback the browser is about to (or is) invoking, a use-after-free the
    /// compiler cannot catch. A component builds one timer (typically wrapped in
    /// an `Rc` so its recompute closure can hold it), calls [`set_callback`] once
    /// with that closure, and then only [`reschedule`]/[`cancel`] on each tick.
    ///
    /// [`set_callback`]: PersistentTimer::set_callback
    /// [`reschedule`]: PersistentTimer::reschedule
    /// [`cancel`]: PersistentTimer::cancel
    /// The late-bound recompute callback a [`PersistentTimer`] fires, shared
    /// between the timer's fire closure and its owner.
    type CallbackSlot = Rc<RefCell<Option<Rc<dyn Fn()>>>>;

    pub struct PersistentTimer {
        /// The pending timeout's handle, so a new schedule cancels the old.
        handle: RefCell<Option<i32>>,
        /// Late-bound recompute callback the fire closure invokes.
        callback: CallbackSlot,
        /// The page-lifetime fire closure, held so the pending timeout's callback
        /// stays alive; rescheduled by shared reference, never recreated.
        fire: Closure<dyn Fn()>,
    }

    impl Default for PersistentTimer {
        fn default() -> Self {
            Self::new()
        }
    }

    impl PersistentTimer {
        /// Build a timer with its fire closure. The callback is unset until
        /// [`set_callback`](PersistentTimer::set_callback); a fire before then is
        /// a no-op.
        pub fn new() -> Self {
            let callback: CallbackSlot = Rc::new(RefCell::new(None));
            let fire = {
                let callback = Rc::clone(&callback);
                Closure::new(move || {
                    // Clone the callback Rc out and drop the borrow before invoking
                    // it: the callback typically reschedules this same timer, which
                    // must be free to borrow the timer's cells.
                    let cb = callback.borrow().as_ref().map(Rc::clone);
                    if let Some(cb) = cb {
                        cb();
                    }
                })
            };
            PersistentTimer {
                handle: RefCell::new(None),
                callback,
                fire,
            }
        }

        /// Set the callback the timer fires. Call once, after the callback (which
        /// typically captures the timer) has been built.
        pub fn set_callback(&self, callback: Rc<dyn Fn()>) {
            *self.callback.borrow_mut() = Some(callback);
        }

        /// Cancel any pending fire, then schedule one `delay_ms` from now. The
        /// same fire closure is reused, never recreated.
        pub fn reschedule(&self, delay_ms: i32) {
            self.cancel();
            *self.handle.borrow_mut() = Some(set_timeout(&self.fire, delay_ms));
        }

        /// Cancel any pending fire. A no-op when nothing is scheduled.
        pub fn cancel(&self) {
            if let Some(handle) = self.handle.borrow_mut().take() {
                clear_timeout(handle);
            }
        }
    }

    // The custom-element class shim, per the contract's recommended module shape:
    // a few lines of JS defining an `HTMLElement` subclass whose
    // `connectedCallback` delegates to the Rust `connected` closure passed in at
    // registration. wasm-bindgen bundles `inline_js` snippets from dependency
    // crates into the final module, so the shim lives here once and every
    // component reuses it. The tag is passed in from
    // `contract::element_name_for_instance(kind, instance)`, so the
    // (kind, instance)↦tag mapping has a single home in Rust and a copied shim
    // cannot drift. The shim returns `false` when the
    // tag is already defined — a collision `register_component` fails loud on —
    // and `true` after a successful define.
    #[wasm_bindgen(inline_js = "\
export function define_component(tag, connected) {\n\
  if (customElements.get(tag)) { return false; }\n\
  class BrennComponent extends HTMLElement {\n\
    connectedCallback() { connected(this); }\n\
  }\n\
  customElements.define(tag, BrennComponent);\n\
  return true;\n\
}")]
    extern "C" {
        fn define_component(tag: &str, connected: &JsValue) -> bool;
    }

    // The instance this module was loaded for, set by `brenn_bind_instance`.
    //
    // A module-local, which is per-*instance* state and not shared-per-kind state:
    // one module evaluation backs one declared instance, with its own linear
    // memory, so this static is exactly as private as the instance is. That is
    // what makes the tag derivation and the panic attribution below name one
    // subject.
    thread_local! {
        static BOUND_INSTANCE: RefCell<Option<String>> = const { RefCell::new(None) };
    }

    /// Bind this module to the instance it was loaded for, then boot.
    ///
    /// A component calls this **first** from the `brenn_bind_instance` export it
    /// declares — the entry the loader invokes once, right after the module's
    /// `default` init, with the instance id from the manifest entry whose
    /// `?instance=` specifier produced this module record. Everything with an
    /// identity ([`install_panic_hook`], [`register_component`]) is legal only
    /// after it, which is why a component's boot sequence lives in that export
    /// rather than in `#[wasm_bindgen(start)]`: `start` runs during `default`,
    /// before the loader can say who this module is.
    ///
    /// The export's name is the contract's
    /// [`brenn_surface_contract::BIND_INSTANCE_EXPORT`]; a rename must happen on
    /// both sides or the module never learns who it is.
    ///
    /// Panics on a second bind: one module record is one instance, so a rebind
    /// means the loader deduped two instances onto one module — the exact bug the
    /// per-instance specifier exists to prevent, and one that would silently give
    /// two instances one identity.
    pub fn bind_instance(instance: &str) {
        // Read the current bind and release the borrow before deciding: a panic
        // must not fire while the thread-local is borrowed, or the wasm trap
        // leaves it poisoned for every later caller.
        let existing = BOUND_INSTANCE.with(|slot| slot.borrow().clone());
        if let Some(existing) = existing {
            panic!(
                "component-support: module already bound to instance {existing:?}, \
                 rebound to {instance:?} — two instances share one module record"
            );
        }
        BOUND_INSTANCE.with(|slot| *slot.borrow_mut() = Some(instance.to_string()));
    }

    /// The bound instance id, or a panic naming the caller that ran too early.
    ///
    /// Fail-fast rather than a fallback: every identity this module has —
    /// its element tag, its panic's subject — derives from the bind, so a
    /// pre-bind caller has no truthful answer to give and guessing one would put a
    /// wrong instance's name on a real panic.
    fn bound_instance(caller: &str) -> String {
        // Clone out and release the borrow before any panic, so a pre-bind
        // caller's trap does not leave the thread-local poisoned.
        let bound = BOUND_INSTANCE.with(|slot| slot.borrow().clone());
        bound.unwrap_or_else(|| {
            panic!(
                "component-support: {caller} ran before brenn_bind_instance — the module \
                 does not know which instance it is"
            )
        })
    }

    /// Install the module panic hook: log the panic and best-effort dispatch
    /// [`COMPONENT_PANIC`] on `window` so the kernel error-cards this instance.
    /// Call once at module init, before [`register_component`].
    ///
    /// The hook names the **instance**, resolved from the loader's bind. A panic
    /// therefore has exactly one subject: this module's memory backs this instance
    /// alone, so its poisoning is one instance's death — one error card, one
    /// status transition, one report — never the kind's.
    ///
    /// Takes no `kind`: the kind identified a panic back when a module backed a
    /// whole kind and the shell had to fan the report out across its instances.
    /// The subject is the instance now, and the instance comes from the bind.
    ///
    /// Panics if called before the bind (see [`bound_instance`]).
    pub fn install_panic_hook() {
        let instance = bound_instance("install_panic_hook");
        std::panic::set_hook(Box::new(move |info| {
            report_panic(&instance, &info.to_string())
        }));
    }

    /// Bind this module's instance identity and install its panic hook, in the
    /// one order that is correct: [`bind_instance`] first (nothing that needs an
    /// identity may run before it), then [`install_panic_hook`] (which reads that
    /// identity for the panic subject). Call once from the module's
    /// `brenn_bind_instance` export before [`register_component`]; the ordering
    /// rule then lives here rather than in each component's copy of it.
    pub fn boot(instance: &str) {
        bind_instance(instance);
        install_panic_hook();
    }

    /// The component's handle for publishing from inside an activation.
    ///
    /// Publishes made through it are **buffered**: nothing reaches the router or
    /// the wire until the handler returns ok, at which point the whole buffer
    /// flushes in call order. Returning err (or panicking) discards it. That is
    /// the same flush rule a `processor` component gets under wasmtime, from the
    /// same model — the host that mints the activation owns the boundary.
    ///
    /// The quota answer is the kernel's, returned synchronously: the handle is a
    /// courier, and every judgement below is made by the kernel's buffer.
    pub struct Publisher {
        host: HtmlElement,
    }

    impl Publisher {
        /// Buffer a publish of `body` from this instance's output `port`, at the
        /// port's configured default urgency.
        pub fn publish(&mut self, port: &str, body: &str) -> Result<(), PublishError> {
            self.publish_dispatch(port, body, None)
        }

        /// Buffer a publish at an explicit urgency, overriding the port's
        /// configured default for this one message — the counterpart of the
        /// backend guest's `publish-with-urgency`, so a component's publish
        /// semantics do not change with its hosting.
        pub fn publish_with_urgency(
            &mut self,
            port: &str,
            body: &str,
            urgency: Urgency,
        ) -> Result<(), PublishError> {
            self.publish_dispatch(port, body, Some(urgency))
        }

        /// Dispatch the publish and read the kernel's synchronous answer back off
        /// the detail.
        ///
        /// The transport is the ordinary [`PORT_PUBLISH`] event: the kernel routes
        /// it to this activation's buffer because this instance is the one whose
        /// entry is on the stack. A missing status means the kernel took the
        /// gesture path for it — structurally impossible from inside an entry, so
        /// it is a kernel/SDK contract break and panics rather than being guessed
        /// at as an ok.
        fn publish_dispatch(
            &mut self,
            port: &str,
            body: &str,
            urgency: Option<Urgency>,
        ) -> Result<(), PublishError> {
            let detail = publish_detail(port, body, urgency);
            dispatch_conformant(&self.host, PORT_PUBLISH, &detail)
                .expect("dispatch brenn-port-publish on the host element");
            let status = Reflect::get(&detail, &JsValue::from_str(PUBLISH_STATUS_FIELD))
                .ok()
                .and_then(|v| v.as_string());
            match status.as_deref().and_then(parse_publish_status) {
                Some(status) => status,
                None => panic!(
                    "component-support: no publish status on a buffered publish of port {port:?} \
                     — the kernel did not route it into this activation's buffer"
                ),
            }
        }
    }

    /// The activation entry as it crosses the wasm-module boundary: a JS function
    /// taking the activation JSON and returning `undefined` (ok) or an error
    /// string (err); a panic throws, which the kernel reads as a trap.
    type EntryFn = Closure<dyn FnMut(JsValue) -> JsValue>;

    /// Register the component's custom element (tag from
    /// [`element_name_for_instance`], on this module's bound instance) and its
    /// activation entry.
    ///
    /// `on_connected` builds the UI, called with the host element on every
    /// insertion into a connected tree (guard it with [`claim_initialized`] —
    /// reparenting re-fires it).
    ///
    /// `on_activation` is the delivery seam, and the only one: the kernel calls it
    /// once per activation with every bound input port windowed (retained context
    /// then new, split by `new_from`, with a `dropped` delta), never once per
    /// message. Publishes made through the [`Publisher`] are buffered and flush
    /// atomically iff it returns `Ok`; an `Err` discards the buffer and counts a
    /// failure, leaving the instance running; a panic is a trap and terminal for
    /// this instance alone.
    ///
    /// The entry is handed to the kernel by dispatching [`ACTIVATION_REGISTER`]
    /// from the element's first `connectedCallback` — once per instance, which is
    /// why the registration rides the same one-time claim as the UI build. The
    /// kernel resolves which instance registered from the element itself.
    ///
    /// Panics if the tag is already defined — a kind collision, a double
    /// registration, or a foreign module squatting this kind's tag. The module's
    /// panic hook then logs the message and best-effort dispatches
    /// [`COMPONENT_PANIC`], per the fail-loud posture. On the panic path the
    /// `connected` closure was never registered and is dropped, so nothing
    /// dangles.
    pub fn register_component(
        kind: &'static str,
        on_connected: impl Fn(HtmlElement) + 'static,
        on_activation: impl FnMut(&Activation, &mut Publisher) -> Result<(), ActivationError> + 'static,
    ) {
        // The tag is this instance's, derived from the loader's bind: one module
        // record, one instance, one element definition.
        let tag = element_name_for_instance(kind, &bound_instance("register_component"));
        // One entry per module, shared by the connected closure: the handler is
        // the instance's, and a module backs one instance's memory.
        let on_activation = Rc::new(RefCell::new(on_activation));
        let connected = Closure::<dyn Fn(HtmlElement)>::new(move |host: HtmlElement| {
            on_connected(host.clone());
            // Claimed on its own key, not `claim_initialized`'s: the UI build's
            // guard is the component's to spend (it may legitimately claim before
            // calling here), and registering twice is a fault the kernel reports.
            // A separate marker keeps the two one-time claims independent.
            if claim_initialized(&host, "brenn-activation") {
                register_activation_entry(&host, Rc::clone(&on_activation));
            }
        });
        if !define_component(&tag, connected.as_ref()) {
            // With per-instance tags this is no longer an expected second instance
            // of the kind — it is a real double evaluation of one instance's
            // module, or a foreign module squatting this instance's tag.
            panic!(
                "custom element tag '{tag}' already defined — double registration of one \
                 instance, or a foreign module squatting kind '{kind}'"
            );
        }
        connected.forget();
    }

    /// Wrap `on_activation` into the boundary-crossing JS entry and hand it to the
    /// kernel on `host`'s [`ACTIVATION_REGISTER`].
    ///
    /// The wrapper is the whole call convention in one place: decode the
    /// activation JSON, build the instance's [`Publisher`], call the handler, and
    /// turn its answer into what the kernel reads — `undefined` for ok, the
    /// message string for err. It never catches a panic: a trap must reach the
    /// kernel as a thrown exception, and swallowing one here would turn a poisoned
    /// memory into a component that keeps being delivered.
    fn register_activation_entry<F>(host: &HtmlElement, on_activation: Rc<RefCell<F>>)
    where
        F: FnMut(&Activation, &mut Publisher) -> Result<(), ActivationError> + 'static,
    {
        let entry: EntryFn = {
            let host = host.clone();
            Closure::new(move |activation_json: JsValue| {
                let json = activation_json
                    .as_string()
                    .expect("the kernel calls the activation entry with a JSON string");
                // A malformed activation is the kernel's bug, not input a
                // component can cause, so it traps rather than being reported as
                // this component's err.
                let activation: Activation = serde_json::from_str(&json)
                    .expect("the kernel's activation JSON decodes to the contract Activation");
                let mut publisher = Publisher { host: host.clone() };
                match on_activation.borrow_mut()(&activation, &mut publisher) {
                    Ok(()) => JsValue::UNDEFINED,
                    Err(err) => JsValue::from_str(&err.message),
                }
            })
        };
        let detail = detail_object(&[("entry", entry.as_ref().clone())]);
        dispatch_conformant(host, ACTIVATION_REGISTER, &detail)
            .expect("dispatch brenn-activation-register on the host element");
        // The kernel holds the entry for the instance's life; nothing here can
        // outlive it, so there is nothing to drop.
        entry.forget();
    }

    /// Claim the one-time init for a host element, keyed on the component `kind`.
    ///
    /// `connectedCallback` fires on every insertion into a connected tree, not
    /// once per element, so the build-the-UI step must run exactly once. This
    /// sets a `data-<kind>-initialized` marker and returns whether this call
    /// claimed it — `true` the first time, `false` on any re-insertion — so
    /// `on_connected` can bail early without duplicating UI or listeners.
    pub fn claim_initialized(host: &HtmlElement, kind: &str) -> bool {
        let marker = format!("data-{kind}-initialized");
        if host.has_attribute(&marker) {
            return false;
        }
        host.set_attribute(&marker, "")
            .expect("set the component init marker");
        true
    }

    /// The live `Document`. Panics if unavailable: a component only runs inside a
    /// browser document, so its absence is a structural impossibility.
    pub fn document() -> Document {
        web_sys::window()
            .expect("a component runs in a browser with a window")
            .document()
            .expect("window has a document")
    }

    /// Create a `<div>` carrying a marker attribute for stylesheet/test
    /// targeting.
    pub fn create_div(doc: &Document, marker_attr: &str) -> HtmlElement {
        let el = doc
            .create_element("div")
            .expect("document creates a div")
            .dyn_into::<HtmlElement>()
            .expect("created div is an HtmlElement");
        el.set_attribute(marker_attr, "")
            .expect("set marker attribute");
        el
    }

    /// Create a `<button>` with the given label text and marker attribute.
    pub fn create_button(doc: &Document, marker_attr: &str, label: &str) -> HtmlElement {
        let el = doc
            .create_element("button")
            .expect("document creates a button")
            .dyn_into::<HtmlElement>()
            .expect("created button is an HtmlElement");
        el.set_attribute(marker_attr, "")
            .expect("set marker attribute");
        el.set_text_content(Some(label));
        el
    }

    /// Create a text `<input>` with the given marker attribute. Returns the
    /// [`HtmlInputElement`] so the caller can read its `value()` on demand.
    pub fn create_input(doc: &Document, marker_attr: &str) -> HtmlInputElement {
        let el = doc
            .create_element("input")
            .expect("document creates an input")
            .dyn_into::<HtmlInputElement>()
            .expect("created input is an HtmlInputElement");
        el.set_attribute("type", "text").expect("set input type");
        el.set_attribute(marker_attr, "")
            .expect("set marker attribute");
        el
    }

    /// Append `child` under `parent`.
    pub fn append(parent: &HtmlElement, child: &HtmlElement) {
        parent
            .append_child(child)
            .expect("append child under its parent");
    }

    /// Create an element of the given tag as an [`HtmlElement`]. The caller owns
    /// the tag set; a markdown walker uses only a fixed, safe set of block/inline
    /// tags. Panics if the document rejects the tag — a structural bug, since the
    /// tag set is a fixed compile-time constant.
    pub fn create_element(doc: &Document, tag: &str) -> HtmlElement {
        doc.create_element(tag)
            .expect("document creates the element")
            .dyn_into::<HtmlElement>()
            .expect("created element is an HtmlElement")
    }

    /// Create a text node carrying `text` verbatim. This is the injection-safe
    /// path: the browser never parses `text` as markup.
    pub fn create_text_node(doc: &Document, text: &str) -> Text {
        doc.create_text_node(text)
    }

    /// Append any node (element or text) under `parent`. Complements [`append`],
    /// which is element-only, for walkers that mix element and text children.
    pub fn append_node(parent: &HtmlElement, child: &Node) {
        parent
            .append_child(child)
            .expect("append node under its parent");
    }

    /// Add a page-lifetime event listener. The `Closure` is `forget`-leaked:
    /// these listeners live as long as the element (the page), so there is
    /// nothing to drop.
    ///
    /// This is for browser events — clicks, input, the DOM's own vocabulary.
    /// Delivery does not arrive as an event: it is the activation entry
    /// [`register_component`] hands the kernel.
    pub fn add_listener(target: &EventTarget, name: &str, handler: impl Fn(Event) + 'static) {
        let closure = Closure::<dyn Fn(Event)>::new(handler);
        target
            .add_event_listener_with_callback(name, closure.as_ref().unchecked_ref())
            .expect("add event listener");
        closure.forget();
    }

    /// Read a named string field from an event's `CustomEvent` detail, or `None`
    /// if the event is not a `CustomEvent` or the field is missing/non-string.
    pub fn string_field(event: &Event, key: &str) -> Option<String> {
        let detail = event.dyn_ref::<CustomEvent>()?.detail();
        Reflect::get(&detail, &JsValue::from_str(key))
            .ok()?
            .as_string()
    }

    /// Read a named number field from an event's `CustomEvent` detail, or `None`
    /// if the event is not a `CustomEvent` or the field is missing/non-number.
    pub fn number_field(event: &Event, key: &str) -> Option<f64> {
        let detail = event.dyn_ref::<CustomEvent>()?.detail();
        Reflect::get(&detail, &JsValue::from_str(key))
            .ok()?
            .as_f64()
    }

    /// Build a plain JS detail object of primitive fields.
    pub fn detail_object(fields: &[(&str, JsValue)]) -> Object {
        let obj = Object::new();
        for (key, value) in fields {
            Reflect::set(&obj, &JsValue::from_str(key), value)
                .expect("set a field on the detail object");
        }
        obj
    }

    /// Dispatch a conformant component→shell `CustomEvent` on the host element:
    /// `bubbles` and `composed` true so the event crosses the shadow boundary and
    /// the shell derives component identity from the retargeted `event.target`.
    /// Every fallible step returns its error rather than panicking, so a caller
    /// on a must-not-panic path can swallow a failure.
    fn dispatch_conformant(host: &HtmlElement, name: &str, detail: &Object) -> Result<(), JsValue> {
        let init = CustomEventInit::new();
        init.set_detail(detail);
        init.set_bubbles(true);
        init.set_composed(true);
        let event = CustomEvent::new_with_event_init_dict(name, &init)?;
        host.dispatch_event(&event)?;
        Ok(())
    }

    /// Dispatch a conformant [`PORT_PUBLISH`] on the host element with
    /// `detail = { port, body }`, carrying no urgency: the port's configured
    /// default applies. A dispatch failure is a structural bug on the publish
    /// path (a live host cannot refuse a well-formed event), so it panics.
    pub fn publish(host: &HtmlElement, port: &str, body: &str) {
        dispatch_publish(host, port, body, None);
    }

    /// Dispatch a conformant [`PORT_PUBLISH`] with an explicit per-message
    /// urgency, overriding the port's configured default for this one message:
    /// `detail = { port, body, urgency }`.
    ///
    /// The counterpart of the backend guest's `publish-with-urgency` — same
    /// override-else-configured-default rule, so a component's publish semantics
    /// do not change with its hosting. `urgency` is a typed [`Urgency`], so an
    /// unrepresentable level cannot compile; the shell drops an unknown wire
    /// value, which only a non-conforming (non-SDK) caller can produce.
    pub fn publish_with_urgency(host: &HtmlElement, port: &str, body: &str, urgency: Urgency) {
        dispatch_publish(host, port, body, Some(urgency));
    }

    /// The dispatch the two **gesture** publish entry points share.
    ///
    /// Immediate and unanswered, because a browser event handler runs with no
    /// activation in flight — there is no boundary to attach a flush rule to.
    /// That is the one residual asymmetry in the delivery model, and it is a
    /// named, bounded gap: the sync follow-on turns the input event into a
    /// sync-call activation with a real flush-on-ok, and no code here may be
    /// shaped in a way that assumes immediate gesture publish is permanent.
    ///
    /// Inside an activation entry, this is not the path: [`Publisher`] is, and
    /// its publishes are buffered and answered.
    fn dispatch_publish(host: &HtmlElement, port: &str, body: &str, urgency: Option<Urgency>) {
        dispatch_conformant(host, PORT_PUBLISH, &publish_detail(port, body, urgency))
            .expect("dispatch brenn-port-publish on the host element");
    }

    /// The [`PORT_PUBLISH`] detail both publish paths build. `urgency: None` omits
    /// the field entirely rather than sending `"normal"` — the contract's
    /// absent-means-the-port's-default rule is what lets an operator retune a port
    /// without touching the component.
    ///
    /// Shared by the gesture path above and [`Publisher`], so the two cannot drift
    /// into two dialects of one event: the buffered-vs-gesture split is the
    /// kernel's routing decision, never a difference in what the component says.
    fn publish_detail(port: &str, body: &str, urgency: Option<Urgency>) -> Object {
        let mut fields = vec![
            ("port", JsValue::from_str(port)),
            ("body", JsValue::from_str(body)),
        ];
        if let Some(urgency) = urgency {
            fields.push(("urgency", JsValue::from_str(urgency.as_str())));
        }
        detail_object(&fields)
    }

    /// Dispatch a conformant [`COMPONENT_LOG`] on the host element with
    /// `detail = { level, message }`. `level` is a typed [`LogLevel`] so an
    /// unrepresentable level cannot compile (the shell drops an unknown wire
    /// level).
    ///
    /// **Best-effort:** a dispatch failure is logged to the console and
    /// swallowed, never propagated as a panic. This path is reached from
    /// report-and-carry-on handlers (e.g. a malformed publisher body) whose whole
    /// contract is that one bad input must not brick the component; panicking here
    /// would defeat that.
    pub fn component_log(host: &HtmlElement, level: LogLevel, message: &str) {
        let detail = detail_object(&[
            ("level", JsValue::from_str(level.as_wire_str())),
            ("message", JsValue::from_str(message)),
        ]);
        if dispatch_conformant(host, COMPONENT_LOG, &detail).is_err() {
            web_sys::console::error_1(&JsValue::from_str(
                "component-support: brenn-log dispatch failed",
            ));
        }
    }

    /// Schedule `callback` to fire once after `delay_ms` milliseconds, returning
    /// the timeout handle for [`clear_timeout`]. The caller owns `callback`'s
    /// lifetime — it must outlive the pending timeout.
    pub fn set_timeout(callback: &Closure<dyn Fn()>, delay_ms: i32) -> i32 {
        web_sys::window()
            .expect("a component runs in a browser with a window")
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                callback.as_ref().unchecked_ref(),
                delay_ms,
            )
            .expect("schedule a timeout")
    }

    /// Cancel a pending timeout by its [`set_timeout`] handle. A handle that has
    /// already fired or been cleared is a no-op per the DOM spec.
    pub fn clear_timeout(handle: i32) {
        web_sys::window()
            .expect("a component runs in a browser with a window")
            .clear_timeout_with_handle(handle);
    }

    /// The module panic hook body: log the panic and best-effort dispatch
    /// [`COMPONENT_PANIC`] on `window` so the shell error-cards this component.
    ///
    /// A panic hook must never itself panic (a double-panic aborts the module and
    /// eats the signal), so it logs first and swallows any dispatch failure.
    fn report_panic(instance: &str, info: &str) {
        web_sys::console::error_1(&JsValue::from_str(info));
        if try_dispatch_panic(instance, info).is_err() {
            web_sys::console::error_1(&JsValue::from_str(
                "component-support: panic-hook dispatch failed",
            ));
        }
    }

    /// Best-effort [`COMPONENT_PANIC`] dispatch: every fallible step returns its
    /// error instead of panicking, so [`report_panic`] can swallow a failure.
    fn try_dispatch_panic(instance: &str, message: &str) -> Result<(), JsValue> {
        let window = web_sys::window().ok_or_else(|| JsValue::from_str("no window"))?;
        let detail = Object::new();
        Reflect::set(
            &detail,
            &JsValue::from_str("instance"),
            &JsValue::from_str(instance),
        )?;
        Reflect::set(
            &detail,
            &JsValue::from_str("message"),
            &JsValue::from_str(message),
        )?;
        let init = CustomEventInit::new();
        init.set_detail(&detail);
        let event = CustomEvent::new_with_event_init_dict(COMPONENT_PANIC, &init)?;
        window.dispatch_event(&event)?;
        Ok(())
    }

    // Browser-level tests for the registration path. Run via
    // `make surface-wasm-test` under a headless WebDriver browser. Each test uses
    // a unique component kind: `customElements` definitions are page-lifetime and
    // cannot be removed, so a shared kind would collide across tests.
    #[cfg(test)]
    mod tests {
        use super::*;
        use std::cell::RefCell;
        use std::rc::Rc;
        use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

        wasm_bindgen_test_configure!(run_in_browser);

        /// The instance every test in this binary binds the module to. One module
        /// backs one instance, so the whole test binary shares a single bind; each
        /// test picks a distinct `kind` instead, and the per-instance tag
        /// (`element_name_for_instance(kind, TEST_INSTANCE)`) keeps them from
        /// colliding in the shared `customElements` registry.
        const TEST_INSTANCE: &str = "wbt";

        /// Bind the module to [`TEST_INSTANCE`] exactly once. `bind_instance`
        /// panics on a rebind (one module record is one instance), but every test
        /// runs in one wasm module, so the bind is guarded to happen a single time
        /// and every test can call this freely before registering.
        fn ensure_bound() {
            thread_local! {
                static BOUND: RefCell<bool> = const { RefCell::new(false) };
            }
            BOUND.with(|bound| {
                let mut bound = bound.borrow_mut();
                if !*bound {
                    bind_instance(TEST_INSTANCE);
                    *bound = true;
                }
            });
        }

        /// This binary's element tag for `kind`, on the shared [`TEST_INSTANCE`].
        fn test_tag(kind: &str) -> String {
            element_name_for_instance(kind, TEST_INSTANCE)
        }

        #[wasm_bindgen_test]
        fn first_registration_defines_and_connects() {
            ensure_bound();
            let kind = "wbt-cs-first";
            let tag = test_tag(kind);

            // on_connected fires only if the element was actually defined and then
            // upgraded on insertion, so a recorded host proves both define and the
            // connectedCallback delegation in one shot.
            let seen: Rc<RefCell<Option<HtmlElement>>> = Rc::new(RefCell::new(None));
            {
                let seen = Rc::clone(&seen);
                register_component(
                    kind,
                    move |host| {
                        *seen.borrow_mut() = Some(host);
                    },
                    |_a, _p| Ok(()),
                );
            }

            let doc = document();
            let el = doc
                .create_element(&tag)
                .expect("create the registered custom element");
            doc.body()
                .expect("test page has a body")
                .append_child(&el)
                .expect("append the element into the connected document");

            let host = seen.borrow();
            let host = host
                .as_ref()
                .expect("connectedCallback delegated to on_connected with the host element");
            assert_eq!(host.tag_name().to_lowercase(), tag);
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "already defined")]
        fn second_registration_of_same_kind_panics() {
            ensure_bound();
            let kind = "wbt-cs-collide";
            register_component(kind, |_host| {}, |_a, _p| Ok(()));
            // Second registration of the same instance's kind squats an
            // already-defined tag — fail loud. Holds no thread-local borrow at
            // panic time, so the wasm trap poisons nothing for later tests in this
            // binary.
            register_component(kind, |_host| {}, |_a, _p| Ok(()));
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "already bound")]
        fn second_bind_of_the_module_panics() {
            // The shared binary is already bound to `TEST_INSTANCE`; a second
            // bind is the "two instances share one module record" bug and must
            // fail loud. `bind_instance` releases its borrow before panicking, so
            // the trap poisons no thread-local for later tests.
            ensure_bound();
            bind_instance("wbt-second-bind");
        }

        /// Register `kind`, mount an instance of it in the connected document, and
        /// return the host element a `dispatch_*` fires from.
        fn mounted_host(kind: &'static str) -> HtmlElement {
            ensure_bound();
            let seen: Rc<RefCell<Option<HtmlElement>>> = Rc::new(RefCell::new(None));
            {
                let seen = Rc::clone(&seen);
                register_component(
                    kind,
                    move |host| {
                        *seen.borrow_mut() = Some(host);
                    },
                    |_a, _p| Ok(()),
                );
            }
            let doc = document();
            let el = doc
                .create_element(&test_tag(kind))
                .expect("create the registered custom element");
            doc.body()
                .expect("test page has a body")
                .append_child(&el)
                .expect("append the element into the connected document");
            let host = seen.borrow();
            host.as_ref().expect("host upgraded on insertion").clone()
        }

        /// Catch the next `brenn-port-publish` bubbling to `body` and return its
        /// `detail`. Listening at `body` rather than on the host is deliberate: it
        /// only sees the event if the SDK really set `bubbles`, which is what the
        /// shell's root-delegated listener depends on.
        fn catch_publish_detail(dispatch: impl FnOnce()) -> JsValue {
            let caught: Rc<RefCell<Option<JsValue>>> = Rc::new(RefCell::new(None));
            let closure = {
                let caught = Rc::clone(&caught);
                Closure::<dyn Fn(Event)>::new(move |event: Event| {
                    let ce = event
                        .dyn_into::<CustomEvent>()
                        .expect("the SDK dispatches a CustomEvent");
                    *caught.borrow_mut() = Some(ce.detail());
                })
            };
            let body = document().body().expect("test page has a body");
            body.add_event_listener_with_callback(PORT_PUBLISH, closure.as_ref().unchecked_ref())
                .expect("listen for the publish event");
            dispatch();
            body.remove_event_listener_with_callback(
                PORT_PUBLISH,
                closure.as_ref().unchecked_ref(),
            )
            .expect("unlisten");
            let detail = caught.borrow().clone();
            detail.expect("the publish event reached the body listener")
        }

        fn detail_field(detail: &JsValue, key: &str) -> JsValue {
            Reflect::get(detail, &JsValue::from_str(key)).expect("read a detail field")
        }

        // ── the activation seam ───────────────────────────────────────────

        /// One window as a handler saw it: (port, envelope count, new_from,
        /// dropped).
        type SeenWindow = (String, usize, u32, u64);
        /// One publish as the kernel-playing listener saw it: (port, body,
        /// urgency).
        type SeenPublish = (String, String, Option<String>);
        /// A recorder shared between a handler and the assertions.
        type Recorder<T> = Rc<RefCell<Vec<T>>>;

        /// Register `kind` on the activation seam, mount it, and return the
        /// `entry` function the SDK handed the kernel — i.e. play the kernel.
        ///
        /// Catches `brenn-activation-register` at `body`, which only sees it if the
        /// SDK really set `bubbles`/`composed`: that is exactly what the shell's
        /// root-delegated listener depends on, so the registration is proven to
        /// reach a kernel that is not listening on the host itself.
        fn registered_entry(
            kind: &'static str,
            on_activation: impl FnMut(&Activation, &mut Publisher) -> Result<(), ActivationError>
            + 'static,
        ) -> js_sys::Function {
            ensure_bound();
            let caught: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
            let closure = {
                let caught = Rc::clone(&caught);
                Closure::<dyn Fn(Event)>::new(move |event: Event| {
                    let ce = event
                        .dyn_into::<CustomEvent>()
                        .expect("the SDK dispatches a CustomEvent");
                    let entry = Reflect::get(&ce.detail(), &JsValue::from_str("entry"))
                        .expect("the registration detail has an entry")
                        .dyn_into::<js_sys::Function>()
                        .expect("entry is a function");
                    *caught.borrow_mut() = Some(entry);
                })
            };
            let body = document().body().expect("test page has a body");
            body.add_event_listener_with_callback(
                ACTIVATION_REGISTER,
                closure.as_ref().unchecked_ref(),
            )
            .expect("listen for the registration event");
            register_component(kind, |_host| {}, on_activation);
            let el = document()
                .create_element(&test_tag(kind))
                .expect("create the registered custom element");
            body.append_child(&el)
                .expect("append the element into the connected document");
            body.remove_event_listener_with_callback(
                ACTIVATION_REGISTER,
                closure.as_ref().unchecked_ref(),
            )
            .expect("unlisten");
            let entry = caught.borrow().clone();
            entry.expect("the registration event reached the body listener")
        }

        /// Call an entry the way the kernel does: one JSON string argument.
        fn call_entry(entry: &js_sys::Function, json: &str) -> Result<JsValue, JsValue> {
            entry.call1(&JsValue::NULL, &JsValue::from_str(json))
        }

        /// An activation JSON with one port carrying one context and one new
        /// envelope, in the kernel's own encoding.
        fn activation_json() -> String {
            serde_json::to_string(&Activation {
                ports: vec![brenn_surface_contract::PortWindow {
                    port: "messages".to_string(),
                    envelopes: vec![
                        brenn_surface_test_fixtures::sample_envelope("seen"),
                        brenn_surface_test_fixtures::sample_envelope("fresh"),
                    ],
                    new_from: 1,
                    dropped: 3,
                }],
            })
            .expect("the fixture activation serializes")
        }

        #[wasm_bindgen_test]
        fn the_entry_decodes_the_kernel_s_activation_into_the_contract_shape() {
            // The seam's whole load-bearing claim: what the kernel serializes is
            // what the handler sees. If the JSON codec drifted, a component would
            // read the wrong window — silently — so this asserts the window's
            // parts, not merely that a call happened.
            let seen: Recorder<SeenWindow> = Rc::new(RefCell::new(Vec::new()));
            let entry = {
                let seen = Rc::clone(&seen);
                registered_entry("wbt-cs-act-decode", move |activation, _publisher| {
                    for window in &activation.ports {
                        seen.borrow_mut().push((
                            window.port.clone(),
                            window.envelopes.len(),
                            window.new_from,
                            window.dropped,
                        ));
                    }
                    Ok(())
                })
            };
            call_entry(&entry, &activation_json()).expect("an ok entry does not throw");
            assert_eq!(
                seen.borrow().as_slice(),
                &[("messages".to_string(), 2, 1, 3)],
                "the handler sees the port windowed exactly as the kernel sent it"
            );
        }

        #[wasm_bindgen_test]
        fn the_entry_answers_ok_err_and_trap_as_the_call_convention_says() {
            // The three answers are the three outcomes, and the kernel reads them
            // off the return: undefined is ok (flush), a string is err (discard,
            // keep running), a throw is a trap (discard, terminal). Collapsing any
            // two would flush a failed activation's publishes or kill an instance
            // that merely said no.
            let ok = registered_entry("wbt-cs-act-ok", |_a, _p| Ok(()));
            assert!(
                call_entry(&ok, &activation_json())
                    .expect("ok does not throw")
                    .is_undefined(),
                "ok returns undefined"
            );

            let err = registered_entry("wbt-cs-act-err", |_a, _p| {
                Err(ActivationError {
                    message: "component said no".to_string(),
                })
            });
            assert_eq!(
                call_entry(&err, &activation_json())
                    .expect("an err returns, it does not throw")
                    .as_string()
                    .as_deref(),
                Some("component said no"),
                "err returns the component's own account as a string"
            );

            let trap = registered_entry("wbt-cs-act-trap", |_a, _p| panic!("flat out broken"));
            assert!(
                call_entry(&trap, &activation_json()).is_err(),
                "a panic crosses the boundary as a thrown exception — the kernel's \
                 only way to tell a trap from an err"
            );
        }

        #[wasm_bindgen_test]
        fn a_buffered_publish_carries_the_publish_detail_and_reads_the_status_back() {
            // The Publisher rides the ordinary PORT_PUBLISH event and takes its
            // answer off the detail. Play the kernel's buffered route: catch the
            // event, write a status, and assert the handler got it back — the
            // synchronous answer is the whole point of the in-flight routing rule.
            let body = document().body().expect("test page has a body");
            let seen: Recorder<SeenPublish> = Rc::new(RefCell::new(Vec::new()));
            let closure = {
                let seen = Rc::clone(&seen);
                Closure::<dyn Fn(Event)>::new(move |event: Event| {
                    let ce = event.dyn_into::<CustomEvent>().expect("a CustomEvent");
                    let detail = ce.detail();
                    seen.borrow_mut().push((
                        detail_field(&detail, "port")
                            .as_string()
                            .unwrap_or_default(),
                        detail_field(&detail, "body")
                            .as_string()
                            .unwrap_or_default(),
                        detail_field(&detail, "urgency").as_string(),
                    ));
                    // The first publish is admitted, the second refused: a component
                    // must see each call's own answer, not one verdict for the
                    // activation.
                    let status = if seen.borrow().len() == 1 {
                        Ok(())
                    } else {
                        Err(PublishError::QuotaExceeded)
                    };
                    Reflect::set(
                        &detail,
                        &JsValue::from_str(PUBLISH_STATUS_FIELD),
                        &JsValue::from_str(brenn_surface_contract::publish_status_str(status)),
                    )
                    .expect("write the status onto the detail");
                })
            };
            body.add_event_listener_with_callback(PORT_PUBLISH, closure.as_ref().unchecked_ref())
                .expect("listen for the publish event");

            let answers: Recorder<Result<(), PublishError>> = Rc::new(RefCell::new(Vec::new()));
            let entry = {
                let answers = Rc::clone(&answers);
                registered_entry("wbt-cs-act-pub", move |_a, publisher| {
                    answers.borrow_mut().push(publisher.publish("out", "one"));
                    answers.borrow_mut().push(publisher.publish_with_urgency(
                        "out",
                        "two",
                        Urgency::High,
                    ));
                    Ok(())
                })
            };
            call_entry(&entry, &activation_json()).expect("ok does not throw");
            body.remove_event_listener_with_callback(
                PORT_PUBLISH,
                closure.as_ref().unchecked_ref(),
            )
            .expect("unlisten");

            assert_eq!(
                seen.borrow().as_slice(),
                &[
                    ("out".to_string(), "one".to_string(), None),
                    (
                        "out".to_string(),
                        "two".to_string(),
                        Some("high".to_string())
                    ),
                ],
                "each publish crosses as the ordinary publish detail; an override \
                 rides it and a silent call carries no urgency key at all"
            );
            assert_eq!(
                answers.borrow().as_slice(),
                &[Ok(()), Err(PublishError::QuotaExceeded)],
                "each call gets its own synchronous answer back"
            );
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "no publish status")]
        fn a_publish_the_kernel_did_not_buffer_panics_rather_than_passing_for_ok() {
            // Inside an entry, a missing status means the kernel did not route the
            // publish into this activation's buffer — structurally impossible, so a
            // contract break. Reading it as an ok would tell a component its message
            // is buffered when nothing holds it.
            let entry = registered_entry("wbt-cs-act-nostatus", |_a, publisher| {
                let _ = publisher.publish("out", "into the void");
                Ok(())
            });
            // Nothing listens for PORT_PUBLISH, so no status is ever written.
            call_entry(&entry, &activation_json()).expect("the inner panic surfaces as a throw");
        }

        #[wasm_bindgen_test]
        fn publish_with_urgency_puts_the_wire_string_on_the_detail() {
            // The producing half of the urgency seam. The shell reads this exact
            // field with its three-state reader, so the value it carries — the
            // lowercase RFC 8030 wire string, not a debug-formatted enum — is the
            // contract between the two halves, and nothing else asserts it.
            let host = mounted_host("wbt-cs-urg");
            let detail = catch_publish_detail(|| {
                publish_with_urgency(&host, "out", "hello", Urgency::High);
            });
            assert_eq!(
                detail_field(&detail, "port").as_string().as_deref(),
                Some("out")
            );
            assert_eq!(
                detail_field(&detail, "body").as_string().as_deref(),
                Some("hello")
            );
            assert_eq!(
                detail_field(&detail, "urgency").as_string().as_deref(),
                Some("high")
            );
        }

        #[wasm_bindgen_test]
        fn plain_publish_omits_urgency_rather_than_sending_null_or_normal() {
            // Absent-means-the-port's-default is the contract, and the shell's
            // reader distinguishes absent from present-but-junk. Sending `"normal"`
            // would silently pin every SDK publish to normal and make the
            // operator's per-output knob dead; sending `null` reads as absent today
            // but states something the component never meant.
            let host = mounted_host("wbt-cs-noturg");
            let detail = catch_publish_detail(|| {
                publish(&host, "out", "hello");
            });
            assert!(
                detail_field(&detail, "urgency").is_undefined(),
                "a publish with no override carries no urgency key at all"
            );
        }
    }
}

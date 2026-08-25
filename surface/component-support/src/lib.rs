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
//!   flushing them atomically iff it returns ok. Deferred publishes and the
//!   cancel/edit of a message already parked ride the same buffer on the same
//!   terms.
//! - [`claim_initialized`] — the `connectedCallback` re-entry guard (a
//!   `data-<kind>-initialized` marker) so a re-insertion does not rebuild the UI
//!   or double-register listeners.
//! - DOM builders ([`document`], [`create_div`], [`create_button`],
//!   [`create_input`], [`create_element`], [`create_text_node`], [`append`],
//!   [`append_node`]), the
//!   page-lifetime [`add_listener`], the untrusted-detail readers
//!   ([`string_field`], [`number_field`]), [`detail_object`], the conformant
//!   [`component_log`] dispatch, and [`config_get`] for this instance's own
//!   static config map.
//! - [`wire_gesture`] — mount-time wiring from a browser event to a sync-call
//!   activation of this instance: the author supplies only the encoder from the
//!   event to a request body, and the reply decides whether the browser's
//!   default action is suppressed. The whole gesture feature's public surface,
//!   with [`gesture_reply`] as the entry's half of the dialect.
//! - [`repark_tick`] — the deferred-self-publish tick idiom in one call: cancel
//!   the standing tick on an in/out port and park the next one, both on the
//!   activation's buffer. Every wall-clock-driven component wants the same thirty
//!   lines, so they live here once.
//! - [`publish_or_fault`] — a buffered publish that keeps the two halves of the
//!   publish-refusal vocabulary apart: a transient quota is logged and answered,
//!   a permanent refusal kills the instance where it happened.
//! - [`fault`] — DOM-free port-delivery validation ([`parse_delivery`],
//!   [`ContractViolation`]) and the shared [`FaultReport`] operator log line,
//!   host-testable and identical across components.
//!
//! What it deliberately does **not** provide is a timer. A component's periodic
//! wakeup is a deferred self-publish on an in/out port ([`repark_tick`]), so there
//! is no `setTimeout` wrapper here to schedule work outside an activation with.

mod fault;
mod gesture;
pub use fault::{ContractViolation, FaultReport, parse_delivery};
pub use gesture::gesture_reply;

// The activation vocabulary a component's handler is written against, re-exported
// from the contract for the same reason the helpers exist at all: an author on
// this SDK is already on the seam and should not restate the dependency to name
// the types the seam hands them. wasm-gated with the contract dep and with every
// consumer of these types (`register_component` and the entry it wraps).
#[cfg(target_arch = "wasm32")]
pub use brenn_surface_contract::{
    Activation, ActivationError, DeferError, DeferredEntry, DeferredWindow, PortWindow,
};

#[cfg(target_arch = "wasm32")]
pub use wasm::*;

#[cfg(target_arch = "wasm32")]
mod wasm {
    use crate::gesture::reply_cancels;
    use brenn_surface_contract::{
        ACTIVATION_REGISTER, ACTIVATION_SYNC, Activation, ActivationError, COMPONENT_LOG,
        COMPONENT_PANIC, CONFIG_ANSWERED_FIELD, CONFIG_GET, CONFIG_VALUE_FIELD, DEFER_OP_CANCEL,
        DEFER_OP_EDIT, DEFER_OP_PUBLISH, DEFER_STATUS_FIELD, DeferError, ENTRY_REPLY_FIELD,
        PORT_DEFER, PORT_PUBLISH, PUBLISH_STATUS_FIELD, PublishError, SYNC_ERROR_FIELD,
        SYNC_REPLY_FIELD, SYNC_STATUS_FIELD, SyncStatus, element_name_for_instance,
        parse_defer_status, parse_publish_status, parse_sync_status,
    };
    use brenn_surface_schema::LogLevel;
    use brenn_surface_schema::Urgency;
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
        /// entry is on the stack. A missing status means the event reached no
        /// kernel listener at all — a broken page rather than an outcome, since a
        /// kernel that heard the publish always answers — so it panics rather than
        /// being guessed at as an ok.
        ///
        /// [`PublishError::NotPermitted`] is an answer, not a fault: it is what
        /// the buffer says about a port this instance does not have as an output.
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
                    "component-support: no publish status on a publish of port {port:?} — no \
                     kernel listener heard it"
                ),
            }
        }

        /// Buffer a publish of `body` that becomes observable no earlier than
        /// `deliver_after` (epoch milliseconds UTC).
        ///
        /// A component computes that instant from the activation's own `now`, never
        /// from a clock of its own: activations are hermetic, and the timestamp
        /// arrives with them. A `deliver_after` at or before the flush's clock
        /// reading publishes immediately, exactly like [`publish`](Self::publish).
        ///
        /// Buffered on the same terms as every other publish — nothing is parked
        /// until the entry returns ok — and answered in the same
        /// [`PublishError`] vocabulary: scheduling adds no way for a publish to be
        /// refused.
        pub fn publish_deferred(
            &mut self,
            port: &str,
            body: &str,
            deliver_after: u64,
        ) -> Result<(), PublishError> {
            let status = self.defer_dispatch(
                DEFER_OP_PUBLISH,
                port,
                &[
                    ("body", JsValue::from_str(body)),
                    (
                        "deliver_after",
                        JsValue::from_str(&deliver_after.to_string()),
                    ),
                ],
            );
            match parse_publish_status(&status) {
                Some(status) => status,
                None => panic!(
                    "component-support: {status:?} is not a publish status — the kernel answered \
                     a deferred publish of port {port:?} in a vocabulary this contract does not \
                     spell"
                ),
            }
        }

        /// Buffer a cancel of one message this component already parked on `port`'s
        /// channel, naming it by its `index` in the [`DeferredWindow`] this
        /// activation delivered for that port.
        ///
        /// Ok does not promise the message was unparked: it may release between this
        /// call and the flush, which the contract rules a benign race the kernel
        /// logs and counts. The component has already returned by then, so the race
        /// is not a refusal.
        pub fn defer_cancel(&mut self, port: &str, index: u32) -> Result<(), DeferError> {
            let status = self.defer_dispatch(
                DEFER_OP_CANCEL,
                port,
                &[("index", JsValue::from_str(&index.to_string()))],
            );
            self.parse_defer_answer(port, DEFER_OP_CANCEL, &status)
        }

        /// Buffer an edit of one message this component already parked on `port`'s
        /// channel: its body, its release time, or both. `None` leaves that half
        /// alone; the index resolves and the race treatment applies exactly as for
        /// [`defer_cancel`](Self::defer_cancel).
        pub fn defer_edit(
            &mut self,
            port: &str,
            index: u32,
            body: Option<&str>,
            deliver_after: Option<u64>,
        ) -> Result<(), DeferError> {
            let mut fields = vec![("index", JsValue::from_str(&index.to_string()))];
            // Each half is omitted rather than sent as null to leave it alone: the
            // kernel reads absence as "do not touch", and a present-but-empty body
            // is a component asking for an empty body.
            if let Some(body) = body {
                fields.push(("body", JsValue::from_str(body)));
            }
            if let Some(deliver_after) = deliver_after {
                fields.push((
                    "deliver_after",
                    JsValue::from_str(&deliver_after.to_string()),
                ));
            }
            let status = self.defer_dispatch(DEFER_OP_EDIT, port, &fields);
            self.parse_defer_answer(port, DEFER_OP_EDIT, &status)
        }

        /// Dispatch one deferred-message op on the [`PORT_DEFER`] event and return
        /// the kernel's synchronous answer as its raw wire string.
        ///
        /// `op` and `port` are stamped here so no caller can forget them; `fields`
        /// carries only what its op needs. `index` and `deliver_after` go out as
        /// decimal strings, the contract's spelling for this seam's numerics.
        ///
        /// A missing status means the kernel refused the op for want of an in-flight
        /// activation of this instance — structurally impossible from inside an
        /// entry, so it is a kernel/SDK contract break and panics rather than being
        /// guessed at as an ok. Which vocabulary the answer is in is the caller's
        /// business: a deferred publish answers in `publish-error`, the control ops
        /// in `defer-error`.
        fn defer_dispatch(
            &mut self,
            op: &'static str,
            port: &str,
            fields: &[(&str, JsValue)],
        ) -> String {
            let mut all = vec![
                ("op", JsValue::from_str(op)),
                ("port", JsValue::from_str(port)),
            ];
            all.extend(fields.iter().map(|(k, v)| (*k, v.clone())));
            let detail = detail_object(&all);
            dispatch_conformant(&self.host, PORT_DEFER, &detail)
                .expect("dispatch brenn-port-defer on the host element");
            Reflect::get(&detail, &JsValue::from_str(DEFER_STATUS_FIELD))
                .ok()
                .and_then(|v| v.as_string())
                .unwrap_or_else(|| {
                    panic!(
                        "component-support: no status on a buffered {op} of port {port:?} — the \
                         kernel did not route it into this activation's buffer"
                    )
                })
        }

        /// Read a control op's answer in the `defer-error` vocabulary, panicking on
        /// a spelling this contract does not know — the same seam-break treatment a
        /// missing status gets.
        fn parse_defer_answer(
            &self,
            port: &str,
            op: &'static str,
            status: &str,
        ) -> Result<(), DeferError> {
            match parse_defer_status(status) {
                Some(status) => status,
                None => panic!(
                    "component-support: {status:?} is not a defer status — the kernel answered a \
                     {op} of port {port:?} in a vocabulary this contract does not spell"
                ),
            }
        }
    }

    /// The activation entry as it crosses the wasm-module boundary: a JS function
    /// taking the activation JSON and returning `undefined` (ok), a
    /// `{ reply }` object (ok, answering a sync activation), or an error string
    /// (err); a panic throws, which the kernel reads as a trap.
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
    /// Its `Ok` carries the reply: `Ok(None)` for an ordinary completion, and
    /// `Ok(Some(reply))` to answer the sync port named by the activation's `sync`
    /// field. Answering an activation that asked nothing is a contract break the
    /// kernel reads as a trap, so a component that replies must first look at
    /// `sync` — or simply be a gesture entry, which knows why it was called.
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
        on_activation: impl FnMut(
            &Activation,
            &mut Publisher,
        ) -> Result<Option<String>, ActivationError>
        + 'static,
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
    /// turn its answer into what the kernel reads — `undefined` for ok with no
    /// reply, a `{ reply }` object for ok with one, the message string for err. It
    /// never catches a panic: a trap must reach the kernel as a thrown exception,
    /// and swallowing one here would turn a poisoned memory into a component that
    /// keeps being delivered.
    fn register_activation_entry<F>(host: &HtmlElement, on_activation: Rc<RefCell<F>>)
    where
        F: FnMut(&Activation, &mut Publisher) -> Result<Option<String>, ActivationError> + 'static,
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
                    Ok(None) => JsValue::UNDEFINED,
                    Ok(Some(reply)) => {
                        detail_object(&[(ENTRY_REPLY_FIELD, JsValue::from_str(&reply))]).into()
                    }
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

    /// Wire a browser event on `target` to a sync-call activation of this
    /// instance, on the sync port `port`.
    ///
    /// This is how a component acts on a gesture. It installs a page-lifetime
    /// listener that, on each event, calls `encode` for the request body, asks the
    /// kernel for an activation carrying it, and suppresses the browser's default
    /// action if the entry's reply says to.
    ///
    /// Call it once per (target, event) pair, on a target that lives as long as
    /// the page — normally at mount time, from `on_connected`. The listener's
    /// closure is never reclaimed, so wiring an element that comes and goes leaks
    /// one closure per element: for dynamic content, wire the container that holds
    /// it once and have `encode` name the row the event came from.
    ///
    /// **The whole handler runs inside the browser's dispatch of the event**: the
    /// activation is assembled, the entry runs with its full worldview and a
    /// buffered [`Publisher`], and its publishes flush — all before the listener
    /// returns. That is what keeps the browser's user-activation token live for the
    /// entry, and what lets the `preventDefault()` below still take effect.
    ///
    /// `encode` owns the payload: the kernel never looks inside a shadow root, so
    /// whatever the entry needs to know about the event — a target id, an input's
    /// value, a modifier key — must be read here and encoded into the body. It runs
    /// on the browser's stack with no activation of its own, so it may read the DOM
    /// and nothing else: publishing and scheduling exist only inside the entry.
    ///
    /// The reply is the [`crate::gesture_reply`] dialect. An entry that answers
    /// `Ok(None)` — the common case for an ack or a dismiss — lets the default
    /// action proceed.
    ///
    /// Panics if the kernel refuses the request or the instance is already
    /// terminal, and if the entry replies outside the dialect: each is a bug in
    /// this component or the kernel, never a state to carry on from.
    pub fn wire_gesture(
        host: &HtmlElement,
        target: &EventTarget,
        event: &str,
        port: &str,
        encode: impl Fn(&Event) -> String + 'static,
    ) {
        let host = host.clone();
        let port = port.to_string();
        add_listener(target, event, move |event: Event| {
            let body = encode(&event);
            // An err is the entry's own considered no: it already reported and the
            // kernel already counted it, so the wiring's part is simply to leave the
            // browser's default action alone.
            let Ok(reply) = request_sync_activation(&host, &port, &body) else {
                return;
            };
            if gesture_cancels(&port, reply.as_deref()) {
                event.prevent_default();
            }
        });
    }

    /// Read a gesture reply as the wiring must: cancel, do not cancel, or fault.
    ///
    /// A reply the wiring cannot parse means a component's entry and its own
    /// wiring speak different dialects. There is no safe reading of an
    /// unparseable cancel decision — taking it for a `false` would leave a gesture
    /// that silently stopped suppressing the browser's default — so it panics.
    fn gesture_cancels(port: &str, reply: Option<&str>) -> bool {
        match reply_cancels(reply) {
            Ok(cancel) => cancel,
            Err(reason) => panic!(
                "component-support: the entry's reply to sync port {port:?} is not the gesture \
                 dialect ({reason}) — the two halves of this component disagree"
            ),
        }
    }

    /// Ask the kernel for a sync-call activation of this instance on `port`,
    /// carrying `body`, and return what its entry answered.
    ///
    /// The request is the [`ACTIVATION_SYNC`] event, dispatched on the host
    /// element; the kernel resolves which instance from the element and writes the
    /// answer onto the same detail before the dispatch returns, so the whole
    /// activation has already happened by the time this returns.
    ///
    /// Only two of the four statuses are answers a caller can act on. `ok` yields
    /// the entry's reply, if it gave one; `err` yields the entry's own account,
    /// informational — the entry saw its own error first and the kernel already
    /// counted the failed activation. The rest **panic**:
    ///
    /// - `refused` — the kernel would not admit the request: it arrived from inside
    ///   an activation, or before registration completed, or named a port that
    ///   collides with a bound input. Every one is a bug in this component or the
    ///   kernel, and carrying on would leave a gesture silently doing nothing.
    /// - `trap` — the instance is already terminal. The closure survives the
    ///   dispatch because the wasm instance is not torn down mid-stack; the panic is
    ///   how it stops running as if alive. The kernel treats a panic report from an
    ///   already-failed instance as idempotent, so this costs one death, not two.
    /// - no status at all — the event never reached the kernel's listener. A broken
    ///   page, not an outcome.
    ///
    /// Private: the only caller is the listener [`wire_gesture`] installs. The
    /// sync class exists for the same-task reply a gesture needs, and nothing else
    /// needs it.
    fn request_sync_activation(
        host: &HtmlElement,
        port: &str,
        body: &str,
    ) -> Result<Option<String>, ActivationError> {
        let detail = detail_object(&[
            ("port", JsValue::from_str(port)),
            ("body", JsValue::from_str(body)),
        ]);
        dispatch_conformant(host, ACTIVATION_SYNC, &detail)
            .expect("dispatch brenn-activation-sync on the host element");
        let status = detail_string(&detail, SYNC_STATUS_FIELD);
        match status.as_deref().and_then(parse_sync_status) {
            Some(SyncStatus::Ok) => Ok(detail_string(&detail, SYNC_REPLY_FIELD)),
            Some(SyncStatus::Err) => Err(ActivationError {
                message: detail_string(&detail, SYNC_ERROR_FIELD)
                    .unwrap_or_else(|| "the entry gave no account".to_string()),
            }),
            Some(SyncStatus::Trap) => panic!(
                "component-support: this instance is terminal — the kernel trapped the sync \
                 activation on port {port:?}"
            ),
            Some(SyncStatus::Refused) => panic!(
                "component-support: the kernel refused a sync activation on port {port:?} — the \
                 request was re-entrant, premature, or named an unusable port"
            ),
            None => panic!(
                "component-support: {status:?} is not a sync status — the kernel did not answer \
                 the request on port {port:?}"
            ),
        }
    }

    /// Buffer a publish of `body` on `port`, splitting the refusal vocabulary the
    /// way [`repark_tick`] does: an exhausted quota is logged and survivable,
    /// anything else is fatal. Answers whether the publish reached the buffer, so a
    /// caller whose own state tracks what it sent advances that state only on
    /// `true`.
    ///
    /// The split is the point. [`PublishError`] is three unrelated conditions
    /// behind one enum, and treating them alike turns a permanent deployment fault
    /// into a page that renders normally while saying nothing on the bus — a state
    /// whose only trace is one log line per occurrence.
    ///
    /// # Panics
    ///
    /// On [`PublishError::NotPermitted`] — the component named a port its config
    /// does not give it, which nothing validates at boot, so the first publish is
    /// the first detection and no later activation repairs it — and on
    /// [`PublishError::InvalidPayload`], a component that built a body over the
    /// surface's cap. [`PublishError::QuotaExceeded`] is the one a conforming
    /// deployment produces transiently, since buckets refill per activation: it is
    /// logged against `host` and answered `false`.
    pub fn publish_or_fault(
        publisher: &mut Publisher,
        host: &HtmlElement,
        port: &str,
        body: &str,
    ) -> bool {
        match publisher.publish(port, body) {
            Ok(()) => true,
            Err(PublishError::QuotaExceeded) => {
                component_log(
                    host,
                    LogLevel::Error,
                    &format!("publish on {port:?} refused: quota exceeded"),
                );
                false
            }
            Err(err) => {
                panic!("component-support: the publish on {port:?} was refused: {err:?}")
            }
        }
    }

    /// Move this component's standing tick on `port` to `release_at`, cancelling
    /// whatever `activation` was shown parked there.
    ///
    /// The deferred self-publish is how a surface component gets a wall-clock
    /// wake: it parks a message to itself on an in/out port, and the release
    /// arrives as an ordinary async activation. Every recompute re-parks — the next
    /// boundary, the next expiry — so the standing tick is cancelled and replaced
    /// rather than edited: it is this component's own and always replaceable, and
    /// both ops ride the activation's buffer, so the pair commits or is discarded
    /// together and an entry that errs schedules nothing.
    ///
    /// `release_at` is `None` for a component with no next wake — a clock in a
    /// fixed mode, a bar whose live slots never expire — which cancels and parks
    /// nothing. The parked body is empty: the wake *is* the message, and a tick
    /// handler recomputes from the activation's own clock.
    ///
    /// Call it from inside `on_activation`, where the buffered [`Publisher`] lives.
    ///
    /// # Panics
    ///
    /// On any refusal but an exhausted quota. A ticker that fails to re-park stops
    /// ticking, and for every reason but the quota it stops *forever*: the port is
    /// not a bound output of this instance (the surface config is missing the
    /// component's `[[surface.io_port]]` declaration — nothing validates that
    /// pairing at boot, so the first park is the first detection), or the index is
    /// not one this activation's own window carried, or the release instant is not
    /// representable. Each is a deployment or a component fault that no later
    /// activation repairs, and a logged line on a page that then silently stops
    /// tracking the clock is the failure hidden rather than reported. A quota is
    /// the one refusal a conforming deployment produces transiently — buckets
    /// refill per activation — so it is logged and the entry carries on.
    pub fn repark_tick(
        activation: &Activation,
        publisher: &mut Publisher,
        host: &HtmlElement,
        port: &str,
        release_at: Option<u64>,
    ) {
        for window in &activation.deferred {
            if window.port != port {
                continue;
            }
            for entry in &window.entries {
                match publisher.defer_cancel(port, entry.index) {
                    Ok(()) => {}
                    Err(DeferError::QuotaExceeded) => component_log(
                        host,
                        LogLevel::Error,
                        &format!("stale tick on {port:?} could not be cancelled: quota exceeded"),
                    ),
                    Err(err) => panic!(
                        "component-support: the tick on {port:?} could not be cancelled: {err:?}"
                    ),
                }
            }
        }
        let Some(deliver_after) = release_at else {
            return;
        };
        match publisher.publish_deferred(port, "{}", deliver_after) {
            Ok(()) => {}
            Err(PublishError::QuotaExceeded) => component_log(
                host,
                LogLevel::Error,
                &format!("tick on {port:?} could not be scheduled: quota exceeded"),
            ),
            Err(err) => {
                panic!("component-support: the tick on {port:?} could not be scheduled: {err:?}")
            }
        }
    }

    /// Read a string field off a detail object the kernel wrote its answer onto.
    fn detail_string(detail: &Object, key: &str) -> Option<String> {
        Reflect::get(detail, &JsValue::from_str(key))
            .ok()
            .and_then(|value| value.as_string())
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

    /// The [`PORT_PUBLISH`] detail a [`Publisher`] builds. `urgency: None` omits
    /// the field entirely rather than sending `"normal"` — the contract's
    /// absent-means-the-port's-default rule is what lets an operator retune a port
    /// without touching the component.
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

    /// Read one key of this instance's static config map, as the operator wrote
    /// it in the surface's config.
    ///
    /// Synchronous. `None` means the key is absent or the instance is not
    /// granted `config`.
    ///
    /// The map is fixed for the page's lifetime (a changed one arrives as changed
    /// wiring, which reloads the page), so a component may read at mount and keep
    /// the answer.
    ///
    /// # Panics
    ///
    /// If the kernel wrote no answer at all: the event never reached its
    /// listener, which is a broken page rather than an outcome — the same verdict
    /// a missing publish status gets.
    pub fn config_get(host: &HtmlElement, key: &str) -> Option<String> {
        let detail = detail_object(&[("key", JsValue::from_str(key))]);
        dispatch_conformant(host, CONFIG_GET, &detail)
            .expect("dispatch brenn-config-get on the host element");
        let answered = Reflect::get(&detail, &JsValue::from_str(CONFIG_ANSWERED_FIELD))
            .ok()
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        assert!(
            answered,
            "component-support: the kernel did not answer the config read of {key:?}"
        );
        detail_string(&detail, CONFIG_VALUE_FIELD)
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
    // wasm-bindgen-test under a headless WebDriver browser. Each test uses
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
                    |_a, _p| Ok(None),
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
            register_component(kind, |_host| {}, |_a, _p| Ok(None));
            // Second registration of the same instance's kind squats an
            // already-defined tag — fail loud. Holds no thread-local borrow at
            // panic time, so the wasm trap poisons nothing for later tests in this
            // binary.
            register_component(kind, |_host| {}, |_a, _p| Ok(None));
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
                    |_a, _p| Ok(None),
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
            on_activation: impl FnMut(
                &Activation,
                &mut Publisher,
            ) -> Result<Option<String>, ActivationError>
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
                deferred: vec![],
                now: None,
                sync: None,
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
                    Ok(None)
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
            let ok = registered_entry("wbt-cs-act-ok", |_a, _p| Ok(None));
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

        // ── the sync seam ─────────────────────────────────────────────────

        /// The same activation, sync-caused on port `ack` — the shape an entry is
        /// allowed to answer.
        fn sync_activation_json() -> String {
            let mut activation: Activation =
                serde_json::from_str(&activation_json()).expect("the fixture round-trips");
            activation.sync = Some("ack".to_string());
            serde_json::to_string(&activation).expect("the fixture activation serializes")
        }

        #[wasm_bindgen_test]
        fn a_sync_reply_leaves_the_entry_as_the_object_the_kernel_reads() {
            // The fourth return shape, and the SDK's only way to answer a gesture.
            // The kernel reads `reply` off a returned object; returning the string
            // bare would be an err, and returning it under any other key would be a
            // trap — so the key is the contract and nothing else pins it here.
            let entry = registered_entry("wbt-cs-act-reply", |activation, _p| {
                assert_eq!(activation.sync.as_deref(), Some("ack"));
                Ok(crate::gesture_reply(true))
            });
            let returned = call_entry(&entry, &sync_activation_json()).expect("ok does not throw");
            assert_eq!(
                detail_field(&returned, ENTRY_REPLY_FIELD)
                    .as_string()
                    .as_deref(),
                Some(r#"{"cancel":true}"#),
                "the reply crosses as a string under the contract's key"
            );
        }

        /// One sync request as the kernel-playing listener saw it: (port, body).
        type SeenSync = (String, String);

        /// Play the kernel for one test's sync `port`: record each request and write
        /// `status` (plus `reply`, when given) onto the detail before the dispatch
        /// returns, which is the whole seam.
        ///
        /// Filtered on the port because these listeners live on `body` and a test
        /// that panics by design never reaches the unlisten. A leaked listener then
        /// sees another test's port and stays inert, rather than answering a request
        /// its test meant to go unanswered.
        fn with_sync_kernel(
            port: &'static str,
            status: &'static str,
            reply: Option<&'static str>,
            act: impl FnOnce(),
        ) -> Vec<SeenSync> {
            let seen: Recorder<SeenSync> = Rc::new(RefCell::new(Vec::new()));
            let closure = {
                let seen = Rc::clone(&seen);
                Closure::<dyn Fn(Event)>::new(move |event: Event| {
                    let ce = event.dyn_into::<CustomEvent>().expect("a CustomEvent");
                    let detail = ce.detail();
                    let requested = detail_field(&detail, "port")
                        .as_string()
                        .unwrap_or_default();
                    if requested != port {
                        return;
                    }
                    seen.borrow_mut().push((
                        requested,
                        detail_field(&detail, "body")
                            .as_string()
                            .unwrap_or_default(),
                    ));
                    Reflect::set(
                        &detail,
                        &JsValue::from_str(SYNC_STATUS_FIELD),
                        &JsValue::from_str(status),
                    )
                    .expect("write the status onto the detail");
                    if let Some(reply) = reply {
                        Reflect::set(
                            &detail,
                            &JsValue::from_str(SYNC_REPLY_FIELD),
                            &JsValue::from_str(reply),
                        )
                        .expect("write the reply onto the detail");
                    }
                })
            };
            let body = document().body().expect("test page has a body");
            body.add_event_listener_with_callback(
                ACTIVATION_SYNC,
                closure.as_ref().unchecked_ref(),
            )
            .expect("listen for the sync request");
            act();
            body.remove_event_listener_with_callback(
                ACTIVATION_SYNC,
                closure.as_ref().unchecked_ref(),
            )
            .expect("unlisten");
            seen.borrow().clone()
        }

        /// A cancelable event of `name`, dispatched on `target`; the returned bool
        /// is false iff something called `preventDefault()` — the browser's own
        /// report, not a proxy for it.
        fn fire_cancelable(target: &EventTarget, name: &str) -> bool {
            let init = CustomEventInit::new();
            init.set_cancelable(true);
            let event =
                CustomEvent::new_with_event_init_dict(name, &init).expect("construct the event");
            target.dispatch_event(&event).expect("dispatch the event")
        }

        #[wasm_bindgen_test]
        fn a_gesture_encodes_its_event_requests_an_activation_and_cancels_on_the_reply() {
            // The whole public feature in one dispatch: the author's encoder runs on
            // the browser's event, the body reaches the kernel under the named sync
            // port, and the entry's reply decides the browser's default action —
            // synchronously, which is the only reason `preventDefault` can still
            // land.
            let host = mounted_host("wbt-cs-gest-cancel");
            let target = create_button(&document(), "data-gesture", "press");
            wire_gesture(&host, &target, "click", "menu", |event| {
                format!("{{\"type\":\"{}\"}}", event.type_())
            });

            let mut prevented = false;
            let seen = with_sync_kernel("menu", "ok", Some(r#"{"cancel":true}"#), || {
                prevented = !fire_cancelable(&target, "click");
            });

            assert_eq!(
                seen.as_slice(),
                &[("menu".to_string(), r#"{"type":"click"}"#.to_string())],
                "the encoder's payload crosses verbatim under the wired port"
            );
            assert!(
                prevented,
                "a reply that says cancel suppresses the browser's default action"
            );
        }

        #[wasm_bindgen_test]
        fn a_gesture_the_entry_does_not_cancel_lets_the_default_action_proceed() {
            // The three ways an entry declines to cancel — no reply at all, an
            // explicit false, and an err — must be indistinguishable from the
            // browser's point of view. Reading any of them as a cancel would break
            // links, form submits and scrolling on a component that merely observed
            // the click.
            let doc = document();
            for (kind, port, status, reply) in [
                ("wbt-cs-gest-noreply", "ack", "ok", None),
                (
                    "wbt-cs-gest-false",
                    "ack-false",
                    "ok",
                    Some(r#"{"cancel":false}"#),
                ),
                ("wbt-cs-gest-err", "ack-err", "err", None),
            ] {
                let host = mounted_host(kind);
                let target = create_button(&doc, "data-gesture", "press");
                wire_gesture(&host, &target, "click", port, |_event| "{}".to_string());
                let mut prevented = false;
                let seen = with_sync_kernel(port, status, reply, || {
                    prevented = !fire_cancelable(&target, "click");
                });
                assert_eq!(seen.len(), 1, "{kind} requested its activation");
                assert!(!prevented, "{kind} left the default action alone");
            }
        }

        // The fault statuses are driven through `request_sync_activation` on the
        // test's own stack rather than through a real gesture: the DOM swallows an
        // exception thrown out of an event listener and reports it globally, so a
        // panic raised inside `dispatchEvent` never reaches the harness. The path
        // under test is the one the wiring's closure takes — the seam is the
        // request, not the listener.

        #[wasm_bindgen_test]
        #[should_panic(expected = "refused a sync activation")]
        fn a_refused_sync_request_panics_rather_than_silently_doing_nothing() {
            // A refusal means the kernel would not admit the request — re-entrant,
            // premature, or a colliding port. Every one is a bug, and swallowing it
            // would leave a button that looks wired and does nothing.
            let host = mounted_host("wbt-cs-gest-refused");
            with_sync_kernel("refused-port", "refused", None, || {
                request_sync_activation(&host, "refused-port", "{}").expect("it panics first");
            });
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "is terminal")]
        fn a_trapped_sync_request_panics_so_the_closure_stops_running_as_if_alive() {
            // The instance is already dead but the requesting closure is still on the
            // stack — the wasm module is not torn down mid-dispatch. The panic is how
            // it stops.
            let host = mounted_host("wbt-cs-gest-trap");
            with_sync_kernel("trap-port", "trap", None, || {
                request_sync_activation(&host, "trap-port", "{}").expect("it panics first");
            });
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "did not answer")]
        fn an_unanswered_sync_request_panics_rather_than_passing_for_ok() {
            // No status means the event never reached a kernel listener at all — a
            // broken page, not an outcome. Reading it as an ok-with-no-reply would
            // make every gesture on a page with no kernel look like it worked.
            let host = mounted_host("wbt-cs-gest-nostatus");
            request_sync_activation(&host, "void-port", "{}").expect("it panics first");
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "not the gesture dialect")]
        fn a_reply_outside_the_dialect_panics_at_the_wiring() {
            // The reply is a dialect between a component's two halves. One that the
            // wiring cannot read means they disagree, and the only honest reading of
            // an unparseable cancel decision is none.
            gesture_cancels("dialect-port", Some("yes please"));
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
                    Ok(None)
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

        /// One deferred-message op as the kernel-playing listener saw it: (op, port,
        /// index, body, deliver_after) — the last three exactly as they crossed, so
        /// an omitted key is distinguishable from a present one.
        type SeenDeferOp = (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        );

        #[wasm_bindgen_test]
        fn the_deferred_ops_carry_their_detail_and_read_each_answer_back() {
            // The producing half of the deferred seam, and the only thing that pins
            // it: the three ops must cross as the contract's `op` selector plus
            // decimal-string numerics, omit the halves an edit leaves alone, and take
            // each call's own answer back off the detail — in the op's own
            // vocabulary, since a deferred publish answers in `publish-error` and the
            // control ops in `defer-error`.
            let body_el = document().body().expect("test page has a body");
            let seen: Recorder<SeenDeferOp> = Rc::new(RefCell::new(Vec::new()));
            let closure = {
                let seen = Rc::clone(&seen);
                Closure::<dyn Fn(Event)>::new(move |event: Event| {
                    let ce = event.dyn_into::<CustomEvent>().expect("a CustomEvent");
                    let detail = ce.detail();
                    let op = detail_field(&detail, "op").as_string().unwrap_or_default();
                    seen.borrow_mut().push((
                        op.clone(),
                        detail_field(&detail, "port")
                            .as_string()
                            .unwrap_or_default(),
                        detail_field(&detail, "index").as_string(),
                        detail_field(&detail, "body").as_string(),
                        detail_field(&detail, "deliver_after").as_string(),
                    ));
                    // The kernel plays each op's own vocabulary, and refuses the
                    // cancel so a component is shown to read its own answer rather
                    // than one verdict for the activation.
                    let status = if op == DEFER_OP_PUBLISH {
                        brenn_surface_contract::publish_status_str(Ok(()))
                    } else if op == DEFER_OP_CANCEL {
                        brenn_surface_contract::defer_status_str(Err(DeferError::OutOfRange))
                    } else {
                        brenn_surface_contract::defer_status_str(Ok(()))
                    };
                    Reflect::set(
                        &detail,
                        &JsValue::from_str(DEFER_STATUS_FIELD),
                        &JsValue::from_str(status),
                    )
                    .expect("write the status onto the detail");
                })
            };
            body_el
                .add_event_listener_with_callback(PORT_DEFER, closure.as_ref().unchecked_ref())
                .expect("listen for the defer event");

            let published: Recorder<Result<(), PublishError>> = Rc::new(RefCell::new(Vec::new()));
            let controlled: Recorder<Result<(), DeferError>> = Rc::new(RefCell::new(Vec::new()));
            let entry = {
                let published = Rc::clone(&published);
                let controlled = Rc::clone(&controlled);
                registered_entry("wbt-cs-act-defer", move |_a, publisher| {
                    published.borrow_mut().push(publisher.publish_deferred(
                        "out",
                        "one",
                        1_770_000_000_000,
                    ));
                    controlled
                        .borrow_mut()
                        .push(publisher.defer_cancel("out", 2));
                    controlled
                        .borrow_mut()
                        .push(publisher.defer_edit("out", 0, Some("two"), None));
                    controlled
                        .borrow_mut()
                        .push(publisher.defer_edit("out", 1, None, Some(9)));
                    Ok(None)
                })
            };
            call_entry(&entry, &activation_json()).expect("ok does not throw");
            body_el
                .remove_event_listener_with_callback(PORT_DEFER, closure.as_ref().unchecked_ref())
                .expect("unlisten");

            assert_eq!(
                seen.borrow().as_slice(),
                &[
                    (
                        "publish".to_string(),
                        "out".to_string(),
                        None,
                        Some("one".to_string()),
                        Some("1770000000000".to_string()),
                    ),
                    (
                        "cancel".to_string(),
                        "out".to_string(),
                        Some("2".to_string()),
                        None,
                        None,
                    ),
                    (
                        "edit".to_string(),
                        "out".to_string(),
                        Some("0".to_string()),
                        Some("two".to_string()),
                        None,
                    ),
                    (
                        "edit".to_string(),
                        "out".to_string(),
                        Some("1".to_string()),
                        None,
                        Some("9".to_string()),
                    ),
                ],
                "each op names itself, spells its numerics in decimal, and carries no \
                 key for the half it leaves alone"
            );
            assert_eq!(
                published.borrow().as_slice(),
                &[Ok(())],
                "a deferred publish is answered in the publish vocabulary"
            );
            assert_eq!(
                controlled.borrow().as_slice(),
                &[Err(DeferError::OutOfRange), Ok(()), Ok(())],
                "each control op gets its own synchronous answer in the defer vocabulary"
            );
        }

        /// An activation JSON whose `deferred` half carries two parked messages on
        /// `port` and one on another port — the shape a re-parking ticker meets on
        /// every activation after its first.
        fn tick_activation_json(port: &str) -> String {
            let entry = |index: u32, deliver_after: u64| brenn_surface_contract::DeferredEntry {
                index,
                payload: "{}".to_string(),
                deliver_after,
            };
            let window = |port: &str, entries| brenn_surface_contract::DeferredWindow {
                port: port.to_string(),
                entries,
            };
            serde_json::to_string(&Activation {
                ports: vec![],
                deferred: vec![
                    window(
                        port,
                        vec![entry(0, 1_770_000_000_000), entry(1, 1_770_000_060_000)],
                    ),
                    window("other", vec![entry(0, 1_770_000_000_000)]),
                ],
                now: Some(1_769_999_999_000),
                sync: None,
            })
            .expect("the fixture activation serializes")
        }

        /// Listen for [`PORT_DEFER`], record `(op, port, index, deliver_after)` and
        /// answer every op with `status`, in whichever vocabulary the op speaks.
        /// Hands back the recorder and the listener's own unlisten.
        ///
        /// `once` makes the browser drop the listener after one event. A fixture
        /// whose entry is *meant* to panic never reaches its unlisten — a wasm
        /// panic does not unwind — and a listener left on the body would answer the
        /// later tests that assert on a kernel answering nothing at all.
        fn record_defer_ops(
            answer: Result<(), DeferError>,
            once: bool,
        ) -> (Recorder<SeenDeferOp>, impl FnOnce()) {
            let body_el = document().body().expect("test page has a body");
            let seen: Recorder<SeenDeferOp> = Rc::new(RefCell::new(Vec::new()));
            let closure = {
                let seen = Rc::clone(&seen);
                Closure::<dyn Fn(Event)>::new(move |event: Event| {
                    let ce = event.dyn_into::<CustomEvent>().expect("a CustomEvent");
                    let detail = ce.detail();
                    let op = detail_field(&detail, "op").as_string().unwrap_or_default();
                    seen.borrow_mut().push((
                        op.clone(),
                        detail_field(&detail, "port")
                            .as_string()
                            .unwrap_or_default(),
                        detail_field(&detail, "index").as_string(),
                        detail_field(&detail, "body").as_string(),
                        detail_field(&detail, "deliver_after").as_string(),
                    ));
                    // A deferred publish answers in the publish vocabulary and the
                    // control ops in their own, so the fixture's one verdict is
                    // spelled twice.
                    let status = if op == DEFER_OP_PUBLISH {
                        brenn_surface_contract::publish_status_str(answer.clone().map_err(|err| {
                            match err {
                                DeferError::NotPermitted => PublishError::NotPermitted,
                                DeferError::QuotaExceeded => PublishError::QuotaExceeded,
                                _ => PublishError::InvalidPayload,
                            }
                        }))
                    } else {
                        brenn_surface_contract::defer_status_str(answer.clone())
                    };
                    Reflect::set(
                        &detail,
                        &JsValue::from_str(DEFER_STATUS_FIELD),
                        &JsValue::from_str(status),
                    )
                    .expect("write the status onto the detail");
                })
            };
            if once {
                js_sys::Function::new_with_args(
                    "target, type, cb",
                    "target.addEventListener(type, cb, { once: true });",
                )
                .call3(
                    &JsValue::NULL,
                    body_el.as_ref(),
                    &JsValue::from_str(PORT_DEFER),
                    closure.as_ref(),
                )
                .expect("listen once for the defer event");
            } else {
                body_el
                    .add_event_listener_with_callback(PORT_DEFER, closure.as_ref().unchecked_ref())
                    .expect("listen for the defer event");
            }
            (seen, move || {
                if !once {
                    body_el
                        .remove_event_listener_with_callback(
                            PORT_DEFER,
                            closure.as_ref().unchecked_ref(),
                        )
                        .expect("unlisten");
                }
            })
        }

        #[wasm_bindgen_test]
        fn a_repark_cancels_its_own_ports_standing_ticks_then_parks_the_next() {
            // The idiom every ticker's chain runs on, and the whole of it is order
            // and scope: each of *this* port's parked messages is cancelled
            // — leaving another port's alone, or a component would cancel a
            // schedule it does not own — and the replacement is parked after, so a
            // discarded entry leaves exactly the standing tick it started with.
            let (seen, unlisten) = record_defer_ops(Ok(()), false);
            let host = document().body().expect("test page has a body");
            let entry = registered_entry("wbt-cs-repark-ok", move |activation, publisher| {
                repark_tick(
                    activation,
                    publisher,
                    &host,
                    "tick",
                    Some(1_770_000_120_000),
                );
                Ok(None)
            });
            call_entry(&entry, &tick_activation_json("tick")).expect("ok does not throw");
            unlisten();

            assert_eq!(
                seen.borrow().as_slice(),
                &[
                    (
                        "cancel".to_string(),
                        "tick".to_string(),
                        Some("0".to_string()),
                        None,
                        None,
                    ),
                    (
                        "cancel".to_string(),
                        "tick".to_string(),
                        Some("1".to_string()),
                        None,
                        None,
                    ),
                    (
                        "publish".to_string(),
                        "tick".to_string(),
                        None,
                        Some("{}".to_string()),
                        Some("1770000120000".to_string()),
                    ),
                ],
                "every standing tick on the port is cancelled by its own index, in \
                 window order, and the next one is parked after them"
            );
        }

        #[wasm_bindgen_test]
        fn a_repark_with_no_next_wake_cancels_and_parks_nothing() {
            // A clock in a fixed mode, a bar whose live slots never expire: the
            // chain stops on purpose. Parking anything here would wake a component
            // that has nothing to recompute, forever.
            let (seen, unlisten) = record_defer_ops(Ok(()), false);
            let host = document().body().expect("test page has a body");
            let entry = registered_entry("wbt-cs-repark-none", move |activation, publisher| {
                repark_tick(activation, publisher, &host, "tick", None);
                Ok(None)
            });
            call_entry(&entry, &tick_activation_json("tick")).expect("ok does not throw");
            unlisten();

            let seen = seen.borrow();
            assert_eq!(seen.len(), 2, "{seen:?}");
            assert!(
                seen.iter()
                    .all(|(op, port, ..)| op == DEFER_OP_CANCEL && port == "tick"),
                "{seen:?}"
            );
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "could not be scheduled")]
        fn a_tick_on_a_port_the_config_never_bound_dies_at_the_first_park() {
            // The deployment failure this idiom introduced: an operator adds a
            // ticking component to a surface without its `[[surface.io_port]]`
            // declaration, nothing validates the pairing at boot, and the first park
            // is the first detection. A logged line and an ok entry would leave a
            // page whose clock silently stopped — so the instance dies at mount and
            // the operator is told.
            let (_seen, _unlisten) = record_defer_ops(Err(DeferError::NotPermitted), true);
            let host = document().body().expect("test page has a body");
            let entry = registered_entry("wbt-cs-repark-unbound", move |activation, publisher| {
                repark_tick(
                    activation,
                    publisher,
                    &host,
                    "tick",
                    Some(1_770_000_120_000),
                );
                Ok(None)
            });
            // No deferred window, so the park is the first op: the cancel loop has
            // nothing to walk.
            call_entry(&entry, &activation_json()).expect("the inner panic surfaces as a throw");
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "could not be cancelled")]
        fn a_cancel_the_window_does_not_admit_dies_rather_than_carrying_on() {
            // An index the kernel does not hold is this component's own bug: it read
            // the window it was handed wrongly. Carrying on would leave the stale
            // tick standing beside the new one and double the chain at every
            // boundary.
            let (_seen, _unlisten) = record_defer_ops(Err(DeferError::OutOfRange), true);
            let host = document().body().expect("test page has a body");
            let entry = registered_entry("wbt-cs-repark-range", move |activation, publisher| {
                repark_tick(activation, publisher, &host, "tick", None);
                Ok(None)
            });
            call_entry(&entry, &tick_activation_json("tick"))
                .expect("the inner panic surfaces as a throw");
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "no status on a buffered cancel")]
        fn a_defer_op_the_kernel_did_not_buffer_panics_rather_than_passing_for_ok() {
            // A missing status means the kernel refused the op for want of an
            // in-flight activation — impossible from inside an entry. Reading it as
            // an ok would tell a component its parked message is cancelled when
            // nothing touched it.
            let entry = registered_entry("wbt-cs-act-nodefst", |_a, publisher| {
                let _ = publisher.defer_cancel("out", 0);
                Ok(None)
            });
            // Nothing listens for PORT_DEFER, so no status is ever written.
            call_entry(&entry, &activation_json()).expect("the inner panic surfaces as a throw");
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "no publish status")]
        fn an_unanswered_publish_panics_rather_than_passing_for_ok() {
            // A kernel that heard the publish always answers — with the buffer's
            // verdict or with `not-permitted` — so no status at all means no
            // listener heard it. Reading that as an ok would tell a component its
            // message is buffered when nothing holds it.
            let entry = registered_entry("wbt-cs-act-nostatus", |_a, publisher| {
                let _ = publisher.publish("out", "into the void");
                Ok(None)
            });
            // Nothing listens for PORT_PUBLISH, so no status is ever written.
            call_entry(&entry, &activation_json()).expect("the inner panic surfaces as a throw");
        }

        #[wasm_bindgen_test]
        fn a_not_permitted_answer_reaches_the_component_as_an_answer() {
            // `not-permitted` is what the kernel says about a port this instance
            // does not have as an output, and about a publish made with no activation
            // in flight. Both are the component's to handle — a refusal
            // is an answer, never a fault — so the SDK hands it back rather than
            // panicking on it.
            let body = document().body().expect("test page has a body");
            let closure = Closure::<dyn Fn(Event)>::new(move |event: Event| {
                let ce = event.dyn_into::<CustomEvent>().expect("a CustomEvent");
                Reflect::set(
                    &ce.detail(),
                    &JsValue::from_str(PUBLISH_STATUS_FIELD),
                    &JsValue::from_str(brenn_surface_contract::publish_status_str(Err(
                        PublishError::NotPermitted,
                    ))),
                )
                .expect("write the status onto the detail");
            });
            body.add_event_listener_with_callback(PORT_PUBLISH, closure.as_ref().unchecked_ref())
                .expect("listen for the publish event");
            let answers: Recorder<Result<(), PublishError>> = Rc::new(RefCell::new(Vec::new()));
            let entry = {
                let answers = Rc::clone(&answers);
                registered_entry("wbt-cs-act-notperm", move |_a, publisher| {
                    answers.borrow_mut().push(publisher.publish("out", "away"));
                    Ok(None)
                })
            };
            call_entry(&entry, &activation_json()).expect("ok does not throw");
            body.remove_event_listener_with_callback(
                PORT_PUBLISH,
                closure.as_ref().unchecked_ref(),
            )
            .expect("unlisten");
            assert_eq!(
                answers.borrow().as_slice(),
                &[Err(PublishError::NotPermitted)]
            );
        }

        /// The reader half of the config protocol against a stub kernel: the
        /// dispatched detail names the key, a present value comes back, and every
        /// absence the kernel spells — unknown key, no map, no grant — is one
        /// `None`, because the answered flag is what tells a live page apart from
        /// a broken one.
        #[wasm_bindgen_test]
        fn a_config_read_carries_its_key_and_reads_the_answer_back() {
            let body = document().body().expect("test page has a body");
            let seen: Recorder<String> = Rc::new(RefCell::new(Vec::new()));
            let closure = {
                let seen = Rc::clone(&seen);
                Closure::<dyn Fn(Event)>::new(move |event: Event| {
                    let ce = event.dyn_into::<CustomEvent>().expect("a CustomEvent");
                    let detail = ce.detail();
                    let key = detail_field(&detail, "key").as_string().unwrap_or_default();
                    seen.borrow_mut().push(key.clone());
                    // The kernel answers every read it heard; the value is written
                    // only when the map holds the key.
                    Reflect::set(
                        &detail,
                        &JsValue::from_str(CONFIG_ANSWERED_FIELD),
                        &JsValue::TRUE,
                    )
                    .expect("write the answered flag onto the detail");
                    if key == "mode" {
                        Reflect::set(
                            &detail,
                            &JsValue::from_str(CONFIG_VALUE_FIELD),
                            &JsValue::from_str("loud"),
                        )
                        .expect("write the value onto the detail");
                    }
                })
            };
            body.add_event_listener_with_callback(CONFIG_GET, closure.as_ref().unchecked_ref())
                .expect("listen for the config event");
            let host = mounted_host("wbt-cs-cfg");
            let hit = config_get(&host, "mode");
            let miss = config_get(&host, "absent");
            body.remove_event_listener_with_callback(CONFIG_GET, closure.as_ref().unchecked_ref())
                .expect("unlisten");

            assert_eq!(seen.borrow().as_slice(), &["mode", "absent"]);
            assert_eq!(hit, Some("loud".to_string()));
            assert_eq!(miss, None, "an unwritten value is an absent one");
        }

        #[wasm_bindgen_test]
        #[should_panic(expected = "did not answer the config read")]
        fn an_unanswered_config_read_panics_rather_than_reading_as_absent() {
            // No listener at all is a broken page, not an operator who wrote no
            // value. Reading it as `None` would hand a component the same answer a
            // real empty map gives, and it would configure itself from a page that
            // never heard the question.
            let host = mounted_host("wbt-cs-cfg-nolisten");
            let _ = config_get(&host, "mode");
        }
    }
}

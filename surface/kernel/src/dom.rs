//! web-sys effect executor for the kernel.
//!
//! Applies the DOM effects the decision core ([`crate::logic`]) emits. Compiled
//! only for the wasm32 (browser) target; the host build excludes it and unit-
//! tests the pure core instead.

use crate::contract::ActivationError;
use crate::contract::{
    ACTIVATION_REGISTER, COMPONENT_ALERT, COMPONENT_LOG, COMPONENT_PANIC, PORT_PUBLISH,
    PROCESSOR_START, PUBLISH_STATUS_FIELD, PublishError, SURFACE_READY, SURFACE_RELOAD,
    SURFACE_ROOT_ID, element_name_for_instance, publish_status_str,
};
use crate::proto::LogLevel;
use crate::{ActivationEntry, ActivationOutcome, ClientHandle};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use js_sys::{Object, Reflect};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{CustomEvent, CustomEventInit, Document, Element, Event, HtmlElement, Window};

use crate::proto::{InstanceCounters, StatusCounters};

use crate::logic::{ConnectIndicatorState, KernelAction, OptionalField};

/// The `source` reported alongside kernel-originated log messages.
const KERNEL_LOG_SOURCE: &str = "kernel";

/// The `source` prefix the kernel stamps on a component-originated `brenn-log`
/// before publishing it as an error report: `"component:<instance>"`.
///
/// Human-readable detail only — the machine-readable twin is the report's
/// `subject_instance`, which the server validates against the declared instance
/// set and derives the sender sub-identity from. The two are composed from the
/// same instance id at each call site, but only the latter is trusted.
const COMPONENT_LOG_SOURCE_PREFIX: &str = "component:";

thread_local! {
    /// The live mounted element for each component **instance**, keyed by instance
    /// id. wasm is single-threaded, so a thread-local map is the executor's whole
    /// shared state. This is the single source of truth for "is this instance
    /// mounted": the `is_mounted` predicate and [`instance_for_target`] both read
    /// it, so DOM and registry cannot disagree. An
    /// instance is present, mapped to the element the kernel created, between
    /// [`mount_component`] and the [`render_error_card`] that removes its element.
    /// One component kind may back several instances, each its own entry.
    static MOUNTED: RefCell<HashMap<String, Element>> = RefCell::new(HashMap::new());
}

/// Whether component `instance` currently has a mounted element. Backs the
/// panic path's per-instance liveness check (`KernelCore::on_component_panic`).
pub fn is_mounted(instance: &str) -> bool {
    MOUNTED.with(|m| m.borrow().contains_key(instance))
}

thread_local! {
    /// `performance.now()` at kernel start, captured by [`mark_page_start`]. Page
    /// uptime for a status report is `now - start`; `None` until start is marked,
    /// which yields an uptime of zero.
    static PAGE_START_MS: Cell<Option<f64>> = const { Cell::new(None) };
    /// Lifetime count of messages delivered to component ports — the status
    /// report's `deliveries`. wasm is single-threaded, so a plain `Cell` is the
    /// whole counter.
    static DELIVERIES: Cell<u64> = const { Cell::new(0) };
    /// Lifetime count of publishes the kernel queued — the report's `publishes`.
    static PUBLISHES: Cell<u64> = const { Cell::new(0) };
    /// Lifetime count of error cards rendered (missing modules, terminal port
    /// events, component panics) — the report's `errors`.
    static ERRORS: Cell<u64> = const { Cell::new(0) };
    /// Per-instance lifetime totals — the report's `counters.instances`. Keyed by
    /// instance id, the principal grain the bus meters at, so a status reader
    /// attributes traffic to the same principal the send budget bounds.
    ///
    /// Entries are created on an instance's first counted event and never
    /// removed: an error-carded instance's totals are exactly what an operator
    /// wants after it dies, and the key set is bounded by the surface's own
    /// config. A `RefCell` rather than a `Cell` because the value is a map;
    /// wasm is single-threaded, and no borrow spans a call out.
    static INSTANCE_COUNTERS: RefCell<BTreeMap<String, InstanceCounters>> =
        const { RefCell::new(BTreeMap::new()) };
}

/// Capture the kernel's start instant for page-uptime accounting. Called once at
/// `start()`; a status report before this is marked reads an uptime of zero.
pub fn mark_page_start() {
    if let Some(now) = performance_now() {
        PAGE_START_MS.with(|c| c.set(Some(now)));
    }
}

/// `performance.now()` in milliseconds, or `None` when the API is unavailable.
fn performance_now() -> Option<f64> {
    web_sys::window()
        .and_then(|w| w.performance())
        .map(|p| p.now())
}

/// Page uptime in whole seconds since [`mark_page_start`] (zero if unmarked or
/// the clock is unavailable) — the status report's `uptime_secs`.
fn page_uptime_secs() -> u64 {
    let (Some(now), Some(start)) = (performance_now(), PAGE_START_MS.with(Cell::get)) else {
        return 0;
    };
    ((now - start).max(0.0) / 1000.0) as u64
}

/// The current lifetime counters for a status report.
fn read_counters() -> StatusCounters {
    StatusCounters {
        deliveries: DELIVERIES.with(Cell::get),
        publishes: PUBLISHES.with(Cell::get),
        errors: ERRORS.with(Cell::get),
        instances: INSTANCE_COUNTERS.with(|c| c.borrow().clone()),
    }
}

/// One instance's counters read from [`read_counters`], or zero when it has
/// counted nothing yet.
///
/// The counters are page-lifetime thread-locals and wasm tests share one thread,
/// so a counter assertion is a *delta* against a snapshot rather than an
/// absolute: an absolute would couple the tests to each other's execution order
/// and to every other test that happens to publish. Lives here rather than in
/// this module's test mod because the publish-counting test needs a live
/// `ClientHandle`, whose rig is in `entry`.
#[cfg(test)]
pub(crate) fn instance_counters(instance: &str) -> InstanceCounters {
    read_counters()
        .instances
        .get(instance)
        .copied()
        .unwrap_or_default()
}

/// Bump a lifetime counter by one.
fn bump(counter: &'static std::thread::LocalKey<Cell<u64>>) {
    counter.with(|c| c.set(c.get().saturating_add(1)));
}

/// Add `n` to one instance's counter, creating its entry on first sight.
///
/// `field` picks the column, so the two call sites cannot transpose a publish
/// count into the drop column: each names its own field and passes nothing else
/// that could be mistaken for the other.
fn bump_instance(instance: &str, field: fn(&mut InstanceCounters) -> &mut u64, n: u64) {
    INSTANCE_COUNTERS.with(|c| {
        let mut map = c.borrow_mut();
        let entry = map.entry(instance.to_string()).or_default();
        let slot = field(entry);
        *slot = slot.saturating_add(n);
    });
}

/// Resolve the retargeted event `target` element to the mounted component
/// instance whose element it is, by element identity over the [`MOUNTED`]
/// registry — the routing identity for a delegated `brenn-port-publish` /
/// `brenn-log` / `brenn-alert`. `None` when `target` is not a mounted instance's
/// own element (a non-conformant module dispatching on an inner light-DOM node, a
/// non-component node, or an already-error-carded instance). The registry holds a
/// handful of entries, so a linear `is_same_node` scan is cheap and keeps identity
/// exact — two instances of one kind share a tag name, so only node identity
/// distinguishes them.
pub fn instance_for_target(target: &Element) -> Option<String> {
    MOUNTED.with(|m| {
        m.borrow()
            .iter()
            .find(|(_, element)| element.is_same_node(Some(target.as_ref())))
            .map(|(instance, _)| instance.clone())
    })
}

/// Apply the [`KernelAction`]s the decision core emitted, in order, dispatching
/// each to its effect primitive. This is the one bridge from the DOM-free core's
/// `Vec<KernelAction>` output to the web-sys effects above; the core decides,
/// this executes.
///
/// `handle` is the surface client handle the two client-touching actions need:
/// `Publish` resolves the output port and queues the frame via
/// [`ClientHandle::publish`], mapping a synchronous rejection to the same
/// console-log + leveled-`log` treatment as a failed `PublishResult`;
/// `Report` is that treatment for the transient/component-fault class
/// (non-`Ok` publish outcome, rejected publish, misrouted `brenn-port-publish`)
/// at `Warn`, and for a component-panic report at `Error`.
pub fn apply_actions(actions: &[KernelAction], handle: &ClientHandle) {
    for action in actions {
        apply_action(action, handle);
    }
}

/// Apply a single [`KernelAction`] by calling its effect primitive.
pub(crate) fn apply_action(action: &KernelAction, handle: &ClientHandle) {
    match action {
        KernelAction::SetConnectIndicator(state) => render_connect_indicator(*state),
        KernelAction::RemoveConnectIndicator => remove_connect_indicator(),
        // Kernel-grain, so it takes no instance and no counter column: the
        // per-instance publish counters attribute what *components* sent, and
        // the kernel's own control traffic is not a component's.
        KernelAction::PublishControl { channel, body } => {
            handle.publish_control(channel, body.clone());
        }
        KernelAction::RequestReload { reason } => request_reload(reason),
        KernelAction::ErrorCard {
            instance,
            kind,
            reason,
        } => render_error_card(instance, kind, reason),
        KernelAction::MountComponent { instance, kind } => mount_component(instance, kind),
        KernelAction::EmitReady => emit_ready(),
        KernelAction::StartProcessors { instances } => start_processors(instances),
        KernelAction::Publish {
            instance,
            port,
            body,
            urgency,
        } => {
            count_publish(instance);
            // A synchronous rejection is contained to the offending component
            // (never a panic, handle.rs) and gets the non-Ok-publish treatment.
            let published = match urgency {
                Some(urgency) => {
                    handle.publish_with_urgency(instance, port, body.clone(), *urgency)
                }
                // No override: the port's configured default applies, which the
                // server resolves. The kernel sends no urgency rather than
                // substituting its `Welcome` snapshot's copy — that snapshot can
                // be stale across a reconnect, and the server's is authoritative.
                None => handle.publish(instance, port, body.clone()),
            };
            if let Err(reject) = published {
                report(
                    handle,
                    LogLevel::Warn,
                    KERNEL_LOG_SOURCE,
                    &format!("publish of {instance}/{port} rejected: {reject:?}"),
                    // The kernel writes the line, but the report is *about* the
                    // component whose publish was rejected — and a component
                    // looping on rejected publishes is exactly the flood whose
                    // reports must draw its own budget, not the surface's.
                    Some(instance),
                );
            }
        }
        // The kernel writes the line; `subject` names the component it is *about*,
        // which the server stamps the report with. A breadcrumb with no component
        // subject carries the bare surface identity.
        KernelAction::Report {
            level,
            message,
            subject,
        } => report(
            handle,
            *level,
            KERNEL_LOG_SOURCE,
            message,
            subject.as_deref(),
        ),
        KernelAction::ComponentLog {
            instance,
            level,
            message,
        } => report(
            handle,
            *level,
            &format!("{COMPONENT_LOG_SOURCE_PREFIX}{instance}"),
            message,
            Some(instance),
        ),
        KernelAction::ComponentAlert {
            severity,
            title,
            body,
        } => {
            handle.alert(*severity, title, body);
        }
        KernelAction::SendGeometry {
            width,
            height,
            device_pixel_ratio,
        } => handle.send_geometry(*width, *height, *device_pixel_ratio),
        // The core supplies the per-instance fact set; the executor fills the page
        // uptime and lifetime counters it owns, then hands the report to the
        // client's best-effort telemetry channel.
        KernelAction::SendStatus { instances } => {
            handle.send_status(instances.clone(), page_uptime_secs(), read_counters());
        }
    }
}

/// Write `message` to the browser console at `level` (always, the durable
/// client-side record) and hand it to [`ClientHandle::report`], which publishes
/// it to the reserved error-report port when the advertised floor admits `level`
/// and otherwise keeps it console-only. `source` attributes the report:
/// [`KERNEL_LOG_SOURCE`] for the kernel's own breadcrumbs, `"component:<instance>"`
/// for a forwarded `brenn-log`.
fn report(
    handle: &ClientHandle,
    level: LogLevel,
    source: &str,
    message: &str,
    subject_instance: Option<&str>,
) {
    let console_msg = JsValue::from_str(message);
    match level {
        LogLevel::Error => web_sys::console::error_1(&console_msg),
        LogLevel::Warn => web_sys::console::warn_1(&console_msg),
        LogLevel::Info => web_sys::console::info_1(&console_msg),
        LogLevel::Debug | LogLevel::Trace => web_sys::console::debug_1(&console_msg),
    }
    handle.report(level, source, message, subject_instance);
}

/// The live `Document`. Panics if unavailable: the kernel only runs inside a
/// browser document, so its absence is a structural impossibility, not a
/// recoverable condition.
fn document() -> Document {
    web_sys::window()
        .expect("kernel runs in a browser with a window")
        .document()
        .expect("window has a document")
}

/// The kernel's DOM root (`#surface-root`), rendered by the backend page.
fn surface_root() -> Element {
    document()
        .get_element_by_id(SURFACE_ROOT_ID)
        .expect("backend page renders #surface-root")
}

/// Find the existing `#id` element, or create a `<tag>` with that id and append
/// it under `parent`. The find-or-create shape shared by the connect indicator
/// and the per-component mount sections; callers set any element-specific
/// attributes on the returned element.
fn find_or_create_child(parent: &Element, id: &str, tag: &str) -> HtmlElement {
    let doc = document();
    match doc.get_element_by_id(id) {
        Some(el) => el
            .dyn_into::<HtmlElement>()
            .expect("existing element is an HtmlElement"),
        None => {
            let el = doc
                .create_element(tag)
                .expect("document creates an element")
                .dyn_into::<HtmlElement>()
                .expect("created element is an HtmlElement");
            el.set_id(id);
            parent
                .append_child(&el)
                .expect("append created child under its parent");
            el
        }
    }
}

/// The id of the kernel-owned pre-chrome connect indicator element.
const CONNECT_INDICATOR_ID: &str = "brenn-connect-indicator";

/// Render (or update the text of) the pre-chrome connect indicator: a single
/// element under `#surface-root` carrying kernel-owned connection-state text.
/// Called by the kernel at start (before any `Welcome`) and on each link-state
/// transition until the handoff removes it. A `data-connect-state` attribute
/// carries the state name for stylesheet targeting.
pub fn render_connect_indicator(state: ConnectIndicatorState) {
    let indicator = find_or_create_child(&surface_root(), CONNECT_INDICATOR_ID, "div");
    let (text, name) = match state {
        ConnectIndicatorState::Connecting => ("Connecting…", "connecting"),
        ConnectIndicatorState::Reconnecting => ("Reconnecting…", "reconnecting"),
        // Terminal: generic text only (the fatal detail stays in the diagnostic
        // path), styled as a dead end via the `failed` state hook.
        ConnectIndicatorState::Failed => ("Connection failed", "failed"),
    };
    indicator.set_text_content(Some(text));
    indicator
        .set_attribute("data-connect-state", name)
        .expect("set data-connect-state attribute");
}

/// Remove the pre-chrome connect indicator for good. Idempotent: a no-op once
/// the element is gone, so a redundant removal action cannot fault.
pub fn remove_connect_indicator() {
    if let Some(el) = document().get_element_by_id(CONNECT_INDICATOR_ID) {
        el.remove();
    }
}

/// The id of the kernel-owned staging container: the hidden holding pen every
/// instance wrapper is created in and returns to when no layout places it.
const STAGING_ID: &str = "brenn-surface-staging";

/// The per-wrapper attribute naming the component kind it holds. A
/// kind-identifying hook on the *wrapper* (used for wrapper-level dressing such
/// as scroll containment), and the marker that distinguishes a kernel wrapper
/// from chrome's own section children. It is not a hook for styling the
/// component host itself: the wrapper may instead hold a kernel error card, so
/// host-level skin rules anchor on a component-stamped `data-<kind>-root` marker
/// rather than descending from this attribute.
const WRAPPER_KIND_ATTR: &str = "data-kind";

/// The stable id of an instance's kernel-owned wrapper element, keyed by
/// `instance`.
pub(crate) fn wrapper_id(instance: &str) -> String {
    format!("brenn-surface-wrapper-{instance}")
}

/// The stable id of an instance's chrome-owned layout section, keyed by
/// `instance`. Test-only in the kernel: chrome owns arrangement, and the kernel
/// only manufactures a section in tests to exercise mount-vs-arrange.
#[cfg(test)]
pub(crate) fn section_id(instance: &str) -> String {
    format!("brenn-surface-section-{instance}")
}

/// The kernel-owned staging container under `#surface-root`, created hidden on
/// first use. Every wrapper is born here and waits here until chrome first
/// arranges it: a staged instance is mounted, warm, and pumping — it simply has
/// no pixels yet. Hidden via the `hidden` attribute rather than a stylesheet
/// rule, so the containment does not depend on a skin remembering to hide it.
///
/// Wrappers do not come back. An instance a layout does not place stays in its
/// own section with no `data-panel`, which is chrome's existing hide, and which
/// is what keeps a layout change from moving nodes (see [`adopt_wrapper`]).
fn staging() -> HtmlElement {
    let staging = find_or_create_child(&surface_root(), STAGING_ID, "div");
    staging.set_hidden(true);
    staging
}

/// Find (or create, in staging) the kernel-owned wrapper for `instance`.
///
/// The wrapper is the mount/arrange seam: the kernel owns it and everything
/// inside it (the component's element, or an error card); chrome owns where it
/// sits and never reaches inside. It carries `data-instance` (its routing
/// identity) and `data-kind` (its component kind). Deliberately **not**
/// [`find_or_create_child`]: that appends under the parent it is handed, which
/// would drag an arranged wrapper back into staging on every remount — the
/// kernel creates the wrapper once and never moves it again.
fn mount_wrapper(instance: &str, kind: &str) -> HtmlElement {
    let doc = document();
    if let Some(existing) = doc.get_element_by_id(&wrapper_id(instance)) {
        return existing
            .dyn_into::<HtmlElement>()
            .expect("existing wrapper is an HtmlElement");
    }
    let wrapper = doc
        .create_element("div")
        .expect("document creates a div")
        .dyn_into::<HtmlElement>()
        .expect("created div is an HtmlElement");
    wrapper.set_id(&wrapper_id(instance));
    wrapper
        .set_attribute("data-instance", instance)
        .expect("set data-instance on the wrapper");
    wrapper
        .set_attribute(WRAPPER_KIND_ATTR, kind)
        .expect("set data-kind on the wrapper");
    staging()
        .append_child(&wrapper)
        .expect("append the new wrapper into staging");
    wrapper
}

/// Mount an instance: create its `brenn-<kind>--<instance>` custom element and
/// append it as the sole content of the instance's wrapper. Clears any prior
/// content (e.g. an earlier error card) first so mounting is idempotent.
///
/// The tag is the *instance's*, not the kind's: each instance's module evaluation
/// defines its own element, which is what gives it its own linear memory. The
/// element is still stamped with `data-instance` — identity rides the attribute,
/// never a parsed tag — so delegation and retargeting are unchanged.
pub fn mount_component(instance: &str, kind: &str) {
    let doc = document();
    let wrapper = mount_wrapper(instance, kind);
    wrapper.set_text_content(None);
    let element = doc
        .create_element(&element_name_for_instance(kind, instance))
        .expect("document creates the component's custom element");
    element
        .set_attribute("data-instance", instance)
        .expect("set data-instance on the component element");
    // Register the element before appending it: append_child synchronously runs
    // the custom element's connectedCallback — the earliest instant it may
    // dispatch brenn-port-publish — so instance_for_target must already resolve
    // that element for the publish to route. A house-style panic between here and
    // a successful append kills the kernel, so no stale entry can outlive the
    // failure.
    MOUNTED.with(|m| m.borrow_mut().insert(instance.to_string(), element.clone()));
    wrapper
        .append_child(&element)
        .expect("append component element into its wrapper");
}

/// Replace the instance's wrapper content with an error card carrying `reason`.
/// The instance's element (if any) is removed by clearing the wrapper; `kind`
/// stamps the wrapper's `data-kind` for the case where the wrapper is created
/// fresh here (a module whose element never registered). `reason` reaches the DOM
/// as `textContent` only — server- or component-supplied text never renders as
/// markup.
///
/// The card renders inside the wrapper, which is the kernel's own DOM: an error
/// card is damage reporting, not chrome, and chrome arranges a carded wrapper
/// exactly as it arranges a live one — a panel naming a dead instance shows its
/// card in that panel's slot.
pub fn render_error_card(instance: &str, kind: &str, reason: &str) {
    bump(&ERRORS);
    let doc = document();
    let wrapper = mount_wrapper(instance, kind);
    wrapper.set_text_content(None);
    let card = doc
        .create_element("div")
        .expect("document creates a div")
        .dyn_into::<HtmlElement>()
        .expect("created div is an HtmlElement");
    card.set_attribute("data-surface-error", "")
        .expect("set data-surface-error attribute");
    card.set_text_content(Some(reason));
    wrapper
        .append_child(&card)
        .expect("append error card into its wrapper");
    MOUNTED.with(|m| m.borrow_mut().remove(instance));
}

/// Build a plain JS detail object of kernel-owned primitive fields. Panics if a
/// field-set fails: the object and its keys are kernel-constructed, so a failure
/// is a structural impossibility, not a recoverable condition.
fn detail_object(fields: &[(&str, JsValue)]) -> Object {
    let obj = Object::new();
    for (key, value) in fields {
        Reflect::set(&obj, &JsValue::from_str(key), value)
            .expect("set a field on a plain detail object");
    }
    obj
}

/// Dispatch the `brenn-surface-ready` seam event on `window` (no detail). The TS
/// bootstrap listens for it on `window` and resets its capped-reload counter.
pub fn emit_ready() {
    dispatch_window_event(SURFACE_READY, None);
}

/// Dispatch the `brenn-processor-start { instances }` seam event on `window`,
/// naming the headless instances the bootstrap loader is to bring up. The
/// instance ids reach the detail as a JS array of string primitives.
pub fn start_processors(instances: &[String]) {
    let array = js_sys::Array::new();
    for instance in instances {
        array.push(&JsValue::from_str(instance));
    }
    let detail = detail_object(&[("instances", array.into())]);
    dispatch_window_event(PROCESSOR_START, Some(&detail));
}

/// Dispatch the `brenn-surface-reload { reason }` seam event on `window`. The TS
/// bootstrap listens for it on `window` and funnels the request through its
/// capped reload guard. `reason` reaches the detail as a string primitive.
pub fn request_reload(reason: &str) {
    let detail = detail_object(&[("reason", JsValue::from_str(reason))]);
    dispatch_window_event(SURFACE_RELOAD, Some(&detail));
}

/// The kernel's panic-hook body: log the panic message and best-effort dispatch
/// the `brenn-surface-reload` seam event so the bootstrap's capped reload can
/// heal a kernel death.
///
/// A panic hook must never itself panic — a double-panic aborts the wasm module
/// and eats the very reload signal the capped-reload guard depends on. So this
/// logs `info` first (the message survives even if dispatch fails) and then
/// attempts the dispatch through a fallible path that swallows any web-sys error
/// rather than unwinding, unlike [`request_reload`]'s house-fail-fast `expect`s
/// on the (non-hook) `KernelAction` path.
pub fn report_panic(info: &str) {
    web_sys::console::error_1(&JsValue::from_str(info));
    if try_dispatch_reload(info).is_err() {
        web_sys::console::error_1(&JsValue::from_str(
            "surface kernel: panic-hook reload dispatch failed",
        ));
    }
}

/// Best-effort `brenn-surface-reload` dispatch: every fallible web-sys step
/// returns its error instead of panicking, so [`report_panic`] can swallow a
/// failure in a degraded DOM without a double-panic.
fn try_dispatch_reload(reason: &str) -> Result<(), JsValue> {
    let window = web_sys::window().ok_or_else(|| JsValue::from_str("no window"))?;
    let detail = Object::new();
    Reflect::set(
        &detail,
        &JsValue::from_str("reason"),
        &JsValue::from_str(reason),
    )?;
    let init = CustomEventInit::new();
    init.set_detail(&detail);
    let event = CustomEvent::new_with_event_init_dict(SURFACE_RELOAD, &init)?;
    window.dispatch_event(&event)?;
    Ok(())
}

/// Extract `N` named string fields from an event's `detail`, the untrusted-input
/// parsing shared by every contract-event listener. Returns `None` for a field
/// that is missing or non-string, and all-`None` for a non-`CustomEvent`, so a
/// malformed detail is never coerced into well-formed values — the single home
/// for the kernel↔component trust-boundary parse.
fn custom_event_string_fields<const N: usize>(
    event: Event,
    keys: [&str; N],
) -> [Option<String>; N] {
    match event.dyn_into::<CustomEvent>() {
        Ok(ce) => {
            let detail = ce.detail();
            std::array::from_fn(|i| {
                Reflect::get(&detail, &JsValue::from_str(keys[i]))
                    .ok()
                    .and_then(|v| v.as_string())
            })
        }
        Err(_) => std::array::from_fn(|_| None),
    }
}

/// Read one **optional** string field from an event's `detail`, distinguishing
/// "the component omitted it" from "the component set it to something that is
/// not a string".
///
/// [`custom_event_string_fields`] folds both into `None`, which is right for a
/// required field — either way it is malformed — but wrong for an optional one:
/// omitting `urgency` is a component saying "use the port's default", while
/// setting it to `7` is a component bug. Collapsing the two would answer the bug
/// with the default and hide it, which is the coercion the trust-boundary parse
/// exists to refuse.
///
/// `undefined`/`null` ⇒ `Absent`; a string ⇒ `Present`; anything else ⇒
/// `Malformed`. A non-`CustomEvent` reports `Malformed`: the contract admits no
/// such dispatch, so it is a broken event rather than an omitted field.
fn custom_event_optional_string(event: &Event, key: &str) -> OptionalField {
    let Some(ce) = event.dyn_ref::<CustomEvent>() else {
        return OptionalField::Malformed;
    };
    let detail = ce.detail();
    match Reflect::get(&detail, &JsValue::from_str(key)) {
        Ok(v) if v.is_undefined() || v.is_null() => OptionalField::Absent,
        Ok(v) => match v.as_string() {
            Some(s) => OptionalField::Present(s),
            None => OptionalField::Malformed,
        },
        // A throwing property getter on a component's own detail object.
        Err(_) => OptionalField::Malformed,
    }
}

/// The delegated component→kernel listener scaffold: one listener on
/// `#surface-root` for a bubbling, composed contract event, resolving the
/// trust-boundary facts every such event shares and handing the raw event on.
///
/// The root-delegated component events — `brenn-port-publish`, `brenn-log`,
/// `brenn-alert` — share one shape: a component
/// dispatches the event on its mounted element (or from within its shadow root)
/// with `bubbles: true, composed: true`, it bubbles up to this single root
/// listener, and the listener resolves the retargeted `event.target` host element
/// to its mounted **instance** id (by element identity, [`instance_for_target`])
/// — the routing identity — plus that element's tag name (for the drop
/// breadcrumb). A target that does not resolve to a mounted instance forwards
/// `None` for the instance, which routes to the drop-and-report path. It makes no
/// routing decision itself.
///
/// This is the lowest rung: it owns the retargeting and identity resolution, and
/// leaves the detail read to the caller, because the events do not agree on that
/// half ([`install_root_component_listener`] reads N required string fields;
/// `brenn-port-publish` additionally needs a three-state optional read). Keeping
/// the scaffold here means a fix to retargeting or instance resolution lands once
/// for every event that crosses this boundary.
///
/// The listener is installed once for the page lifetime, so its `Closure` is
/// `forget`-leaked deliberately.
fn install_root_event_listener(
    event_name: &'static str,
    callback: impl Fn(Option<&str>, &str, Event) + 'static,
) {
    let doc = document();
    let root = doc
        .get_element_by_id(SURFACE_ROOT_ID)
        .expect("backend page renders #surface-root");
    let closure = Closure::<dyn Fn(Event)>::new(move |event: Event| {
        let target = event.target().and_then(|t| t.dyn_into::<Element>().ok());
        let target_tag = target.as_ref().map(|el| el.tag_name()).unwrap_or_default();
        let instance = target.as_ref().and_then(instance_for_target);
        callback(instance.as_deref(), &target_tag, event);
    });
    root.add_event_listener_with_callback(event_name, closure.as_ref().unchecked_ref())
        .unwrap_or_else(|err| {
            panic!("add {event_name} listener on #surface-root: {err:?}");
        });
    closure.forget();
}

/// Install a delegated component→kernel listener for an event carrying `N` named
/// **required** string detail fields, forwarding the resolved instance, the
/// retargeted tag, and those fields to `callback` (wired at `start()` to the
/// matching DOM-free router, which decides route-vs-drop).
///
/// Component-supplied detail is untrusted (all modules are same-origin and
/// operator-deployed, but a buggy or hostile one must never crash the kernel nor
/// launder malformed input onto the bus / log / alert planes). A missing or
/// non-string field, or a non-`CustomEvent` event, forwards `None` for that field
/// rather than coercing to an empty string, so the router drops and reports it as
/// malformed instead of manufacturing a well-formed frame.
fn install_root_component_listener<const N: usize>(
    event_name: &'static str,
    keys: [&'static str; N],
    callback: impl Fn(Option<&str>, &str, [Option<String>; N]) + 'static,
) {
    install_root_event_listener(event_name, move |instance, tag, event| {
        let fields = custom_event_string_fields(event, keys);
        callback(instance, tag, fields);
    });
}

/// Install the delegated `brenn-port-publish` listener: forwards the resolved
/// instance, the retargeted tag, the `{ port, body }` detail, and the optional
/// `urgency` to `callback`, wired to [`crate::logic::route_publish_intent`].
///
/// Built on [`install_root_event_listener`] rather than
/// [`install_root_component_listener`]: that rung reads a fixed set of *required*
/// string fields, and `urgency` is optional — it needs the three-state read
/// ([`custom_event_optional_string`]) so an omitted field and a non-string one
/// take different paths. Only the detail read differs; the scaffold is shared.
/// The publish callback additionally receives the event's `detail` object: a
/// publish the kernel routes into an in-flight activation's buffer is answered
/// synchronously by writing [`PUBLISH_STATUS_FIELD`] onto it (see
/// [`set_publish_status`]), which is the only channel a synchronous answer can
/// take back across the module boundary.
pub fn install_publish_listener(
    callback: impl Fn(Option<&str>, &str, Option<&str>, Option<&str>, OptionalField, &JsValue) + 'static,
) {
    install_root_event_listener(PORT_PUBLISH, move |instance, tag, event| {
        let urgency = custom_event_optional_string(&event, "urgency");
        let detail = event
            .dyn_ref::<CustomEvent>()
            .map(|ce| ce.detail())
            .unwrap_or(JsValue::UNDEFINED);
        let [port, body] = custom_event_string_fields(event, ["port", "body"]);
        callback(
            instance,
            tag,
            port.as_deref(),
            body.as_deref(),
            urgency,
            &detail,
        );
    });
}

/// Write a buffered publish's answer onto the dispatching event's `detail`, where
/// the component's SDK reads it as `publish` returns.
///
/// Only a *buffered* publish gets one: its absence is exactly what tells the SDK
/// the kernel took the gesture path (which has no synchronous answer). A detail
/// that refuses the write is a non-conformant dispatcher — some caller sending a
/// frozen or primitive detail — so the answer is simply dropped rather than
/// panicking the kernel on a component's malformed event.
pub fn set_publish_status(detail: &JsValue, status: Result<(), PublishError>) {
    let _ = Reflect::set(
        detail,
        &JsValue::from_str(PUBLISH_STATUS_FIELD),
        &JsValue::from_str(publish_status_str(status)),
    );
}

/// Count one publish for `instance` — the lifetime totals a status report carries.
/// Shared by the immediate path in [`apply_action`] and the buffered path in the
/// publish listener, so what an operator reads does not depend on which route the
/// kernel took.
pub(crate) fn count_publish(instance: &str) {
    bump(&PUBLISHES);
    bump_instance(instance, |c| &mut c.publishes, 1);
}

/// Wrap a component's registered `entry` function into the kernel's
/// [`ActivationEntry`] — the kernel's half of the call convention.
///
/// One encode per activation (not per message, which is the whole point): the
/// activation is serialized to JSON and passed as the single argument. The return
/// value is the outcome, and the three answers are the three outcomes the model
/// has:
///
/// - `undefined`/`null` → ok; the buffer flushes.
/// - a string → err, carrying the component's own account. The buffer is
///   discarded, the instance keeps running.
/// - a thrown exception → trap. `Function::call1` gives it back as `Err`, which is
///   this build's *only* way to see a trap at all — `catch_unwind` cannot observe
///   a wasm panic. The buffer is discarded and the instance is terminal.
///
/// Any other return type is a non-conformant module: reported as a trap rather
/// than read as an ok, because an entry that answered gibberish did not tell us it
/// succeeded, and treating it as success would flush publishes on its say-so.
pub fn wrap_activation_entry(instance: &str, entry: js_sys::Function) -> ActivationEntry {
    let instance = instance.to_string();
    Box::new(move |activation| {
        // Count what this activation actually delivers, before the entry can
        // trap: `deliveries` is the new envelopes across every window (the
        // retained context ahead of `new_from` was counted when it was new), and
        // `drops` is the loss each window reports since its port's last
        // activation. Counted here because this is where the numbers exist — one
        // call, every bound port, both facts on the windows.
        let mut new = 0u64;
        for window in &activation.ports {
            new = new.saturating_add(window.new_len());
        }
        let dropped = activation.total_dropped();
        DELIVERIES.with(|c| c.set(c.get().saturating_add(new)));
        if dropped > 0 {
            bump_instance(&instance, |c| &mut c.drops, dropped);
        }
        let json = serde_json::to_string(activation)
            .expect("surface kernel: an Activation serializes to JSON");
        match entry.call1(&JsValue::NULL, &JsValue::from_str(&json)) {
            Ok(value) if value.is_undefined() || value.is_null() => ActivationOutcome::Ok,
            Ok(value) => match value.as_string() {
                Some(message) => ActivationOutcome::Err(ActivationError { message }),
                None => ActivationOutcome::Trap(
                    "activation entry returned neither undefined nor an error string".to_string(),
                ),
            },
            Err(thrown) => ActivationOutcome::Trap(js_error_message(&thrown)),
        }
    })
}

/// The operator's account of a thrown activation entry.
///
/// A JS throw carries anything at all, so this reads an `Error`'s `message` when
/// there is one and falls back to the value's own string form otherwise. The text
/// is diagnostic and never parsed — but it is the only answer to "failed *how*?"
/// that will ever exist for this trap, so it is recovered rather than discarded.
fn js_error_message(thrown: &JsValue) -> String {
    if let Some(err) = thrown.dyn_ref::<js_sys::Error>() {
        return err.message().into();
    }
    thrown
        .as_string()
        .unwrap_or_else(|| format!("{:?}", thrown))
}

/// Install the delegated `brenn-activation-register` listener: forwards the
/// resolved instance, the retargeted tag, and the component's `entry` function to
/// `callback`, wired to [`crate::logic::KernelCore::on_activation_register`].
///
/// Built on [`install_root_event_listener`] rather than
/// [`install_root_component_listener`]: the detail carries a *function*, not
/// strings. That is the point of the seam — the event is in-page and never
/// serialized, so it can carry the one thing an event cannot carry over a wire.
/// A detail with no callable `entry` is a non-conformant module; it forwards
/// `None` so the caller drops and reports it rather than registering a
/// nothing.
pub fn install_activation_register_listener(
    callback: impl Fn(Option<&str>, &str, Option<js_sys::Function>) + 'static,
) {
    install_root_event_listener(ACTIVATION_REGISTER, move |instance, tag, event| {
        let entry = event
            .dyn_ref::<CustomEvent>()
            .and_then(|ce| Reflect::get(&ce.detail(), &JsValue::from_str("entry")).ok())
            .and_then(|v| v.dyn_into::<js_sys::Function>().ok());
        callback(instance, tag, entry);
    });
}

/// Install the delegated `brenn-log` listener (see
/// [`install_root_component_listener`]): forwards the resolved instance, the
/// retargeted tag, and the `{ level, message }` detail to `callback`, wired to
/// [`crate::logic::route_component_log`], which stamps the `component:<instance>`
/// source.
pub fn install_log_listener(
    callback: impl Fn(Option<&str>, &str, Option<&str>, Option<&str>) + 'static,
) {
    install_root_component_listener(
        COMPONENT_LOG,
        ["level", "message"],
        move |instance, tag, [level, message]| {
            callback(instance, tag, level.as_deref(), message.as_deref());
        },
    );
}

/// Install the delegated `brenn-alert` listener (see
/// [`install_root_component_listener`]): forwards the resolved instance, the
/// retargeted tag, and the `{ severity, title, body }` detail to `callback`, wired
/// to [`crate::logic::route_component_alert`], which gates the forward on the
/// surface's alert grant.
pub fn install_alert_listener(
    callback: impl Fn(Option<&str>, &str, Option<&str>, Option<&str>, Option<&str>) + 'static,
) {
    install_root_component_listener(
        COMPONENT_ALERT,
        ["severity", "title", "body"],
        move |instance, tag, [severity, title, body]| {
            callback(
                instance,
                tag,
                severity.as_deref(),
                title.as_deref(),
                body.as_deref(),
            );
        },
    );
}

/// Install the one `brenn-component-panic` listener on `window`.
///
/// A component module's panic hook dispatches `brenn-component-panic
/// { instance, message }` on `window` (per-instance memory means the hook
/// names the one instance it backs). This primitive reads the
/// `{ instance, message }` string
/// detail and hands both to `callback`. It makes no policy decision itself —
/// `callback` (wired at `start()`) error-cards that component's mount section
/// and reports it.
///
/// Component-supplied detail is untrusted (a buggy or hostile module must never
/// crash the kernel). A missing or non-string `instance`/`message` field, or a
/// non-`CustomEvent` event, forwards `None` for that field rather than coercing
/// to an empty string, so the wiring can drop an unattributable panic rather
/// than error-card the wrong instance. The listener is installed once for the
/// page lifetime, so its `Closure` is `forget`-leaked deliberately.
pub fn install_component_panic_listener(callback: impl Fn(Option<&str>, Option<&str>) + 'static) {
    let window = web_sys::window().expect("kernel runs in a browser with a window");
    let closure = Closure::<dyn Fn(Event)>::new(move |event: Event| {
        let [instance, message] = custom_event_string_fields(event, ["instance", "message"]);
        callback(instance.as_deref(), message.as_deref());
    });
    window
        .add_event_listener_with_callback(COMPONENT_PANIC, closure.as_ref().unchecked_ref())
        .expect("add brenn-component-panic listener on window");
    closure.forget();
}

/// Construct and dispatch a kernel → bootstrap seam CustomEvent on `window`. The
/// bootstrap's listeners are registered on `window`; the event needs no
/// bubbling because `window` is the dispatch target itself. `detail` is a plain
/// object of primitives when present, or absent for detail-less events.
fn dispatch_window_event(name: &str, detail: Option<&JsValue>) {
    let window = web_sys::window().expect("kernel runs in a browser with a window");
    let init = CustomEventInit::new();
    if let Some(detail) = detail {
        init.set_detail(detail);
    }
    let event = CustomEvent::new_with_event_init_dict(name, &init)
        .expect("construct the window seam CustomEvent");
    window
        .dispatch_event(&event)
        .expect("dispatch the seam CustomEvent on window");
}

/// Trailing-edge debounce for viewport reports: a resize drag reports once, on
/// the last resize of the burst, rather than every intermediate frame.
const RESIZE_DEBOUNCE_MS: i32 = 500;

/// Read the current viewport and hand `(width, height, device_pixel_ratio)` to
/// `callback`. `width`/`height` are CSS pixels (`window.innerWidth`/`innerHeight`
/// rounded). When either dimension is unavailable or reads as zero/non-finite,
/// the report is **skipped** rather than sent as zero: telemetry is best-effort,
/// and the server treats a `< 1` dimension as a protocol violation (kill +
/// security event), so degrading to zero would turn a browser quirk into false
/// fail2ban signal. Not reporting is the honest degrade.
fn read_viewport(window: &Window, callback: &dyn Fn(u32, u32, f64)) {
    let Some(width) = window.inner_width().ok().and_then(|v| v.as_f64()) else {
        return;
    };
    let Some(height) = window.inner_height().ok().and_then(|v| v.as_f64()) else {
        return;
    };
    // The server treats any dimension outside 1..=32768 CSS px or a DPR outside
    // 0.1..=16 as a protocol violation (kill + fail2ban security event), so a
    // reading beyond those bounds is skipped rather than sent — the same reason
    // the lower bound is skipped. `device_pixel_ratio` includes page zoom, so a
    // high-DPR display at an accessibility zoom can legitimately exceed 16; a
    // legitimate browser state must not manufacture false attacker signal.
    // Skipping is the honest degrade; telemetry is best-effort.
    if !(width.is_finite() && height.is_finite()) || width < 1.0 || height < 1.0 {
        return;
    }
    if width > 32_768.0 || height > 32_768.0 {
        return;
    }
    let device_pixel_ratio = window.device_pixel_ratio();
    if !device_pixel_ratio.is_finite() || !(0.1..=16.0).contains(&device_pixel_ratio) {
        return;
    }
    callback(width as u32, height as u32, device_pixel_ratio);
}

/// Install a debounced `window` `resize` listener that reads the viewport and
/// hands it to `callback`. Fires once immediately (the startup read), then on the
/// trailing edge of each resize burst ([`RESIZE_DEBOUNCE_MS`] after the last
/// resize). Installed once for the page lifetime, so its `Closure` is
/// `forget`-leaked deliberately.
pub fn install_resize_listener(callback: impl Fn(u32, u32, f64) + 'static) {
    let window = web_sys::window().expect("kernel runs in a browser with a window");
    let callback = Rc::new(callback);
    read_viewport(&window, callback.as_ref());
    let pending: Rc<Cell<Option<i32>>> = Rc::new(Cell::new(None));
    // The trailing-edge fire closure is allocated once and reused for every
    // re-arm. A fresh `Closure::once_into_js` per resize event would leak each
    // cancelled closure — an uninvoked `once_into_js` box is never reclaimed — so
    // a resize burst (which cancels all but its last timeout) would accrete
    // leaks unboundedly on a long-lived wall page.
    let fire = Closure::<dyn Fn()>::new({
        let callback = Rc::clone(&callback);
        move || {
            let window = web_sys::window().expect("kernel runs in a browser with a window");
            read_viewport(&window, callback.as_ref());
        }
    });
    // Each resize cancels the pending fire and re-arms the shared timeout, so only
    // the last resize of a burst reports. `fire` is moved into this closure and
    // kept alive for the page lifetime by the `forget` below.
    let closure = Closure::<dyn Fn()>::new(move || {
        let window = web_sys::window().expect("kernel runs in a browser with a window");
        if let Some(id) = pending.take() {
            window.clear_timeout_with_handle(id);
        }
        let id = window
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                fire.as_ref().unchecked_ref(),
                RESIZE_DEBOUNCE_MS,
            )
            .expect("arm resize debounce timeout");
        pending.set(Some(id));
    });
    window
        .add_event_listener_with_callback("resize", closure.as_ref().unchecked_ref())
        .expect("add resize listener on window");
    closure.forget();
}

/// Install the periodic status-tick timer: invoke `callback` every
/// `interval_secs` via `setInterval`. Installed once for the page lifetime, so
/// its `Closure` is `forget`-leaked deliberately.
pub fn install_status_timer(interval_secs: u32, callback: impl Fn() + 'static) {
    let window = web_sys::window().expect("kernel runs in a browser with a window");
    let closure = Closure::<dyn Fn()>::new(callback);
    let interval_ms = i32::try_from(interval_secs.saturating_mul(1000)).unwrap_or(i32::MAX);
    window
        .set_interval_with_callback_and_timeout_and_arguments_0(
            closure.as_ref().unchecked_ref(),
            interval_ms,
        )
        .expect("install status-tick interval");
    closure.forget();
}

// Browser-level tests for the DOM effect executor. Run via
// `make surface-wasm-test` under a headless WebDriver browser; excluded from the
// host sweep (the whole module is wasm32-only). Isolation: every test that
// touches `#surface-root` starts from `fresh_root`, and every test that touches
// `MOUNTED`/`customElements` uses a unique `wbt-*` kind (both are page-lifetime).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::Activation;
    use crate::wasm_test_util::{capture_window_event, define_test_element, fresh_root, str_field};
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    /// Define `instance`'s instance-scoped element so it upgrades on insertion,
    /// delegating each `connectedCallback` to `connected`.
    ///
    /// The kernel creates instance-scoped elements (`brenn-<kind>--<instance>`); a
    /// test that needs the element to actually upgrade (so `connectedCallback`
    /// fires) defines its tag through this bare `HTMLElement` subclass. Tests that
    /// only dispatch from the element and read its `data-instance` need no
    /// definition at all — an undefined custom element still carries attributes and
    /// a tag name.
    fn define_instance_element(
        instance: &str,
        kind: &str,
        connected: impl Fn(HtmlElement) + 'static,
    ) {
        define_test_element(&element_name_for_instance(kind, instance), connected);
    }

    /// Drive `instance`'s activation counting the way the kernel does: build the
    /// wrapped entry and invoke it with an activation whose windows carry the given
    /// `(port, new_envelopes, dropped)`. This is the sole drop/delivery counting
    /// site now — the per-message dialect markers are gone.
    fn count_activation(instance: &str, windows: &[(&str, u64, u64)]) {
        let entry = wrap_activation_entry(instance, js_entry("return undefined;"));
        let ports = windows
            .iter()
            .map(|(port, new, dropped)| crate::contract::PortWindow {
                port: (*port).to_string(),
                envelopes: (0..*new)
                    .map(|i| brenn_surface_test_fixtures::sample_envelope(&format!("m{i}")))
                    .collect(),
                new_from: 0,
                dropped: *dropped,
            })
            .collect();
        let _ = entry(&Activation { ports });
    }

    /// Sink for a listener forwarding a resolved instance, a retargeted tag, and
    /// two string fields (`install_log_listener`).
    type TagPairSink = Rc<RefCell<Vec<(Option<String>, String, Option<String>, Option<String>)>>>;
    /// Sink for `install_publish_listener`: instance + tag + `{ port, body }` plus
    /// the three-state optional `urgency` read, which is what distinguishes this
    /// listener from the required-fields rung.
    type PublishSink = Rc<
        RefCell<
            Vec<(
                Option<String>,
                String,
                Option<String>,
                Option<String>,
                OptionalField,
            )>,
        >,
    >;
    /// Sink for `install_alert_listener`: instance + tag + three string fields.
    type AlertSink = Rc<
        RefCell<
            Vec<(
                Option<String>,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
            )>,
        >,
    >;
    /// Sink for `install_component_panic_listener`: two string fields, no tag.
    type PanicSink = Rc<RefCell<Vec<(Option<String>, Option<String>)>>>;

    // ── local dispatch helpers ────────────────────────────────────────────

    /// Dispatch a bubbling + composed `CustomEvent` from `target` (the
    /// component-side dispatch shape the root-delegated listeners expect).
    fn dispatch_bubbling(target: &Element, name: &str, detail: &Object) {
        let init = CustomEventInit::new();
        init.set_detail(detail);
        init.set_bubbles(true);
        init.set_composed(true);
        let event =
            CustomEvent::new_with_event_init_dict(name, &init).expect("construct bubbling event");
        target
            .dispatch_event(&event)
            .expect("dispatch bubbling event");
    }

    /// Dispatch `name` on `window`, as a `CustomEvent` with `detail` or, when
    /// `detail` is `None`, a plain non-`CustomEvent` `Event`.
    fn dispatch_window(name: &str, detail: Option<&Object>) {
        let window = web_sys::window().expect("window");
        match detail {
            Some(detail) => {
                let init = CustomEventInit::new();
                init.set_detail(detail);
                let event = CustomEvent::new_with_event_init_dict(name, &init)
                    .expect("construct window CustomEvent");
                window.dispatch_event(&event).expect("dispatch on window");
            }
            None => {
                let event = Event::new(name).expect("construct plain Event");
                window.dispatch_event(&event).expect("dispatch on window");
            }
        }
    }

    // ── activation entry call convention ──────────────────────────────────

    /// A JS entry whose body is `source`, taking the kernel's one JSON argument.
    fn js_entry(source: &str) -> js_sys::Function {
        js_sys::Function::new_with_args("_json", source)
    }

    /// A minimal activation to feed the wrapper; its contents are irrelevant to
    /// the return-value classification these tests pin.
    fn one_port_activation() -> crate::contract::Activation {
        crate::contract::Activation {
            ports: vec![crate::contract::PortWindow {
                port: "messages".to_string(),
                envelopes: vec![brenn_surface_test_fixtures::sample_envelope("m")],
                new_from: 0,
                dropped: 0,
            }],
        }
    }

    #[wasm_bindgen_test]
    fn wrap_activation_entry_classifies_every_return() {
        let activation = one_port_activation();
        let i = "wbt-wrap-ret";

        // undefined / null → ok (buffer flushes).
        assert!(matches!(
            wrap_activation_entry(i, js_entry("return undefined;"))(&activation),
            ActivationOutcome::Ok
        ));
        assert!(matches!(
            wrap_activation_entry(i, js_entry("return null;"))(&activation),
            ActivationOutcome::Ok
        ));

        // A returned string → err carrying the component's own account.
        match wrap_activation_entry(i, js_entry("return 'declined';"))(&activation) {
            ActivationOutcome::Err(ActivationError { message }) => {
                assert_eq!(message, "declined");
            }
            other => panic!("expected Err, got {other:?}"),
        }

        // Any other return type is non-conformant → trap, never read as ok.
        assert!(matches!(
            wrap_activation_entry(i, js_entry("return 42;"))(&activation),
            ActivationOutcome::Trap(_)
        ));

        // A thrown Error → trap carrying the Error's message.
        match wrap_activation_entry(i, js_entry("throw new Error('boom');"))(&activation) {
            ActivationOutcome::Trap(message) => assert_eq!(message, "boom"),
            other => panic!("expected Trap, got {other:?}"),
        }

        // A thrown non-Error → trap, falling back to the value's string form.
        match wrap_activation_entry(i, js_entry("throw 'plain';"))(&activation) {
            ActivationOutcome::Trap(message) => assert_eq!(message, "plain"),
            other => panic!("expected Trap, got {other:?}"),
        }
    }

    // ── connect indicator ─────────────────────────────────────────────────

    #[wasm_bindgen_test]
    fn connect_indicator_renders_updates_and_removes() {
        fresh_root();
        render_connect_indicator(ConnectIndicatorState::Connecting);
        let el = document()
            .get_element_by_id(CONNECT_INDICATOR_ID)
            .expect("indicator created");
        assert_eq!(el.text_content().as_deref(), Some("Connecting…"));
        assert_eq!(
            el.get_attribute("data-connect-state").as_deref(),
            Some("connecting")
        );

        // A state change reuses the same node and updates its text/attr.
        render_connect_indicator(ConnectIndicatorState::Reconnecting);
        let again = document()
            .get_element_by_id(CONNECT_INDICATOR_ID)
            .expect("indicator kept");
        assert!(el.is_same_node(Some(again.as_ref())), "reuses the node");
        assert_eq!(again.text_content().as_deref(), Some("Reconnecting…"));

        // The terminal failed state: generic text (no server detail) and a
        // `failed` state hook for the dead-end styling.
        render_connect_indicator(ConnectIndicatorState::Failed);
        let failed = document()
            .get_element_by_id(CONNECT_INDICATOR_ID)
            .expect("indicator kept");
        assert_eq!(failed.text_content().as_deref(), Some("Connection failed"));
        assert_eq!(
            failed.get_attribute("data-connect-state").as_deref(),
            Some("failed")
        );

        // Removal takes it out of the tree; a second removal is a no-op.
        remove_connect_indicator();
        assert!(
            document().get_element_by_id(CONNECT_INDICATOR_ID).is_none(),
            "removed from the tree"
        );
        remove_connect_indicator();
    }

    // ── mount / error card ────────────────────────────────────────────────

    /// The instance's wrapper, or `None` when the kernel never mounted one.
    fn wrapper_of(instance: &str) -> Option<Element> {
        document().get_element_by_id(&wrapper_id(instance))
    }

    #[wasm_bindgen_test]
    fn mount_wrapper_is_born_in_staging_hidden_and_reused() {
        fresh_root();
        let instance = "wbt-wrap-i";
        let kind = "wbt-wrap";
        let first = mount_wrapper(instance, kind);
        assert_eq!(
            first.get_attribute(WRAPPER_KIND_ATTR).as_deref(),
            Some(kind)
        );
        assert_eq!(
            first.get_attribute("data-instance").as_deref(),
            Some(instance)
        );
        assert_eq!(first.id(), wrapper_id(instance));

        let staging = document()
            .get_element_by_id(STAGING_ID)
            .expect("staging container");
        assert!(
            first
                .parent_element()
                .expect("wrapper has a parent")
                .is_same_node(Some(staging.as_ref())),
            "a new wrapper is born in staging"
        );
        assert!(
            staging
                .dyn_ref::<HtmlElement>()
                .expect("staging is an HtmlElement")
                .hidden(),
            "staging is hidden: a staged instance is warm, not visible"
        );
        assert!(
            staging.is_same_node(surface_root().first_element_child().as_deref()),
            "staging lives under #surface-root"
        );

        let second = mount_wrapper(instance, kind);
        assert!(
            first.is_same_node(Some(second.as_ref())),
            "second call reuses the wrapper"
        );
    }

    #[wasm_bindgen_test]
    fn mount_wrapper_never_drags_an_arranged_wrapper_back_to_staging() {
        // The kernel creates the wrapper once and never moves it again: a remount
        // (or an error card) after chrome has arranged the wrapper must leave it
        // in its section. A find-or-create that appended into staging would yank
        // the panel's content back off screen on the next mount.
        fresh_root();
        let instance = "wbt-nodrag-i";
        let kind = "wbt-nodrag";
        mount_component(instance, kind);
        // Arrange the wrapper the way chrome would: create a section under
        // #surface-root and reparent the instance's wrapper into it. The kernel
        // only mounts; it owns no layout engine.
        let section = document()
            .create_element("section")
            .expect("create section");
        section.set_id(&section_id(instance));
        surface_root()
            .append_child(&section)
            .expect("append section");
        section
            .append_child(&wrapper_of(instance).expect("wrapper exists"))
            .expect("reparent wrapper into section");

        mount_component(instance, kind);
        render_error_card(instance, kind, "boom");

        let wrapper = wrapper_of(instance).expect("wrapper survives");
        assert!(
            wrapper
                .parent_element()
                .expect("wrapper has a parent")
                .is_same_node(Some(section.as_ref())),
            "the wrapper stayed in its arranged section"
        );
    }

    #[wasm_bindgen_test]
    fn mount_component_registers_before_append_and_is_idempotent() {
        fresh_root();
        let instance = "wbt-mount-i";
        let kind = "wbt-mount";
        let observed: Rc<RefCell<Vec<bool>>> = Rc::new(RefCell::new(Vec::new()));
        {
            let observed = Rc::clone(&observed);
            define_instance_element(instance, kind, move |_host| {
                observed.borrow_mut().push(is_mounted(instance));
            });
        }
        mount_component(instance, kind);
        assert_eq!(
            *observed.borrow(),
            vec![true],
            "connectedCallback saw is_mounted == true (registered before append)"
        );
        let wrapper = wrapper_of(instance).expect("wrapper");
        assert_eq!(
            wrapper.child_element_count(),
            1,
            "exactly the component element"
        );
        let child = wrapper.first_element_child().expect("component element");
        assert_eq!(
            child.tag_name().to_lowercase(),
            element_name_for_instance(kind, instance)
        );
        assert_eq!(
            child.get_attribute("data-instance").as_deref(),
            Some(instance),
            "element stamped with its instance id"
        );

        mount_component(instance, kind);
        assert_eq!(
            wrapper.child_element_count(),
            1,
            "re-mount clears prior content"
        );
        assert_eq!(observed.borrow().len(), 2, "re-mount re-connects");
    }

    #[wasm_bindgen_test]
    fn render_error_card_clears_deregisters_and_is_text() {
        fresh_root();
        let instance = "wbt-errcard-i";
        let kind = "wbt-errcard";
        let seed = document().create_element("div").expect("seed element");
        MOUNTED.with(|m| m.borrow_mut().insert(instance.to_string(), seed));
        assert!(is_mounted(instance));

        let payload = "<script>alert(1)</script>";
        render_error_card(instance, kind, payload);
        assert!(!is_mounted(instance), "error card deregisters the instance");
        let wrapper = wrapper_of(instance).expect("wrapper");
        assert_eq!(
            wrapper.child_element_count(),
            1,
            "the wrapper holds only the card"
        );
        let card = wrapper
            .query_selector("[data-surface-error]")
            .expect("query_selector")
            .expect("error card present");
        assert_eq!(card.text_content().as_deref(), Some(payload));
        assert!(
            card.inner_html().contains("&lt;script&gt;"),
            "reason escaped as text, not markup"
        );
    }

    // ── activation counting ───────────────────────────────────────────────

    /// A window's `dropped` count accrues to the instance whose port dropped —
    /// per activation, summed across the activation's windows, not one per
    /// message. The window carries the loss as a counter (the per-message drop
    /// marker is gone), so an activation losing on two ports counts the total.
    #[wasm_bindgen_test]
    fn drops_count_per_instance_by_window_dropped() {
        fresh_root();
        let instance = "wbt-ctr-drops-i";
        mount_component(instance, "wbt-ctr-drops");
        let before = instance_counters(instance);

        count_activation(instance, &[("p", 0, 3), ("other", 0, 4)]);

        let after = instance_counters(instance);
        assert_eq!(
            after.drops - before.drops,
            7,
            "both ports' dropped counts accrue to the instance"
        );
        assert_eq!(after.publishes, before.publishes, "drops are not publishes");
    }

    /// Counters are per instance: a sibling's drops never land on this one's
    /// column. The property the whole per-instance grain exists for — an
    /// operator asking "which component is losing messages?" gets an answer, not
    /// a surface-wide total.
    #[wasm_bindgen_test]
    fn instance_counters_do_not_bleed_across_siblings() {
        fresh_root();
        let (a, b) = ("wbt-ctr-sib-a", "wbt-ctr-sib-b");
        mount_component(a, "wbt-ctr-sib");
        mount_component(b, "wbt-ctr-sib");
        let (before_a, before_b) = (instance_counters(a), instance_counters(b));

        count_activation(a, &[("p", 0, 2)]);

        assert_eq!(instance_counters(a).drops - before_a.drops, 2);
        assert_eq!(
            instance_counters(b).drops,
            before_b.drops,
            "the sibling's column is untouched"
        );
    }

    /// A message delivery is not a drop: the columns are distinct, so a
    /// busy-but-healthy instance (new messages, nothing dropped) never reads as
    /// lossy.
    #[wasm_bindgen_test]
    fn deliveries_are_not_counted_as_drops() {
        fresh_root();
        let instance = "wbt-ctr-msg-i";
        mount_component(instance, "wbt-ctr-msg");
        let before = instance_counters(instance);

        count_activation(instance, &[("p", 2, 0)]);

        let after = instance_counters(instance);
        assert_eq!(after.drops, before.drops, "a delivery is not a drop");
        assert_eq!(
            after.publishes, before.publishes,
            "a delivery is not a publish"
        );
    }

    // ── window seam ───────────────────────────────────────────────────────

    #[wasm_bindgen_test]
    fn emit_ready_and_request_reload() {
        let ready = capture_window_event(SURFACE_READY, emit_ready);
        assert_eq!(ready.len(), 1);
        assert!(
            ready[0].is_null() || ready[0].is_undefined(),
            "SURFACE_READY carries no detail"
        );

        let reload = capture_window_event(SURFACE_RELOAD, || request_reload("upgrade"));
        assert_eq!(reload.len(), 1);
        assert_eq!(str_field(&reload[0], "reason"), Some("upgrade".into()));
    }

    #[wasm_bindgen_test]
    fn report_panic_dispatches_reload_without_panic() {
        let reload = capture_window_event(SURFACE_RELOAD, || report_panic("boom"));
        assert_eq!(reload.len(), 1);
        assert_eq!(str_field(&reload[0], "reason"), Some("boom".into()));
    }

    // ── untrusted-detail parse + listeners ────────────────────────────────

    #[wasm_bindgen_test]
    fn custom_event_string_fields_parse() {
        let detail = detail_object(&[("a", JsValue::from_str("x")), ("n", JsValue::from_f64(5.0))]);
        let init = CustomEventInit::new();
        init.set_detail(&detail);
        let ce = CustomEvent::new_with_event_init_dict("wbt-parse", &init).expect("custom event");
        let [a, b, n] = custom_event_string_fields(ce.into(), ["a", "b", "n"]);
        assert_eq!(a.as_deref(), Some("x"), "string field");
        assert_eq!(b, None, "missing field");
        assert_eq!(n, None, "non-string field");

        let plain = Event::new("wbt-plain").expect("plain event");
        let [pa] = custom_event_string_fields(plain, ["a"]);
        assert_eq!(pa, None, "non-CustomEvent yields all None");
    }

    /// Mount an instance of `kind` under a fresh root and return its host element
    /// — the shape the delegated listeners resolve to an instance id. The element
    /// is the retargeted `event.target` when a `dispatch_bubbling` fires from it.
    fn mount_probe(instance: &str, kind: &'static str) -> Element {
        // No element definition needed: these tests dispatch from the element and
        // read its `data-instance`/tag name, which an undefined custom element
        // carries just as well. Identity rides `data-instance`, never a defined
        // upgrade.
        mount_component(instance, kind);
        MOUNTED
            .with(|m| m.borrow().get(instance).cloned())
            .expect("mounted element")
    }

    /// The uppercase tag the delegated listeners report for `instance` of `kind`
    /// — the element's own instance-scoped tag name.
    fn probe_tag(instance: &str, kind: &str) -> String {
        element_name_for_instance(kind, instance).to_uppercase()
    }

    #[wasm_bindgen_test]
    fn install_publish_listener_resolves_instance_tag_and_fields() {
        fresh_root();
        let element = mount_probe("wbt-pub-i", "wbt-pub");
        let sink: PublishSink = Rc::new(RefCell::new(Vec::new()));
        {
            let sink = Rc::clone(&sink);
            install_publish_listener(move |instance, tag, port, body, urgency, _detail| {
                sink.borrow_mut().push((
                    instance.map(String::from),
                    tag.to_string(),
                    port.map(String::from),
                    body.map(String::from),
                    urgency,
                ));
            });
        }
        dispatch_bubbling(
            &element,
            PORT_PUBLISH,
            &detail_object(&[
                ("port", JsValue::from_str("out")),
                ("body", JsValue::from_str("hello")),
            ]),
        );
        dispatch_bubbling(
            &element,
            PORT_PUBLISH,
            &detail_object(&[("port", JsValue::from_str("out"))]),
        );
        let got = sink.borrow();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0.as_deref(), Some("wbt-pub-i"), "resolved instance");
        assert_eq!(
            got[0].1,
            probe_tag("wbt-pub-i", "wbt-pub"),
            "retargeted tag"
        );
        assert_eq!(got[0].2.as_deref(), Some("out"));
        assert_eq!(got[0].3.as_deref(), Some("hello"));
        assert_eq!(got[1].3, None, "missing body field forwards None");
    }

    #[wasm_bindgen_test]
    fn install_publish_listener_reads_urgency_as_three_states_across_the_dom_seam() {
        // The `OptionalField` reader's four reachable branches, exercised through
        // a real dispatch — the only place the DOM value and the three-state read
        // meet. `route_publish_intent`'s tests take an already-constructed
        // `OptionalField`, so nothing else pins that a component's `urgency: 3`
        // reaches the router as `Malformed` rather than being coerced to `Absent`
        // and published at the port's default — a level the component never chose.
        fresh_root();
        let element = mount_probe("wbt-urg-i", "wbt-urg");
        let sink: PublishSink = Rc::new(RefCell::new(Vec::new()));
        {
            let sink = Rc::clone(&sink);
            install_publish_listener(move |instance, tag, port, body, urgency, _detail| {
                sink.borrow_mut().push((
                    instance.map(String::from),
                    tag.to_string(),
                    port.map(String::from),
                    body.map(String::from),
                    urgency,
                ));
            });
        }
        let port = ("port", JsValue::from_str("out"));
        let body = ("body", JsValue::from_str("hello"));
        // No `urgency` key at all: the component asks for the port's default.
        dispatch_bubbling(
            &element,
            PORT_PUBLISH,
            &detail_object(&[port.clone(), body.clone()]),
        );
        // Explicit null: same meaning as omitted, not a malformed value.
        dispatch_bubbling(
            &element,
            PORT_PUBLISH,
            &detail_object(&[port.clone(), body.clone(), ("urgency", JsValue::NULL)]),
        );
        // A string: forwarded verbatim, still untrusted (the router parses it).
        dispatch_bubbling(
            &element,
            PORT_PUBLISH,
            &detail_object(&[
                port.clone(),
                body.clone(),
                ("urgency", JsValue::from_str("high")),
            ]),
        );
        // A number: a component bug, and it must not read as "use the default".
        dispatch_bubbling(
            &element,
            PORT_PUBLISH,
            &detail_object(&[
                port.clone(),
                body.clone(),
                ("urgency", JsValue::from_f64(3.0)),
            ]),
        );
        let got = sink.borrow();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].4, OptionalField::Absent, "omitted urgency is Absent");
        assert_eq!(got[1].4, OptionalField::Absent, "null urgency is Absent");
        assert_eq!(
            got[2].4,
            OptionalField::Present("high".to_string()),
            "a string urgency is forwarded verbatim for the router to parse"
        );
        assert_eq!(
            got[3].4,
            OptionalField::Malformed,
            "a non-string urgency is Malformed, never coerced to Absent"
        );
    }

    #[wasm_bindgen_test]
    fn install_publish_listener_forwards_none_instance_for_non_component_target() {
        // A dispatch from a plain node that is not a mounted instance element
        // resolves to `None`, which routes to the drop-and-report path.
        let root = fresh_root();
        let sink: PublishSink = Rc::new(RefCell::new(Vec::new()));
        {
            let sink = Rc::clone(&sink);
            install_publish_listener(move |instance, tag, port, body, urgency, _detail| {
                sink.borrow_mut().push((
                    instance.map(String::from),
                    tag.to_string(),
                    port.map(String::from),
                    body.map(String::from),
                    urgency,
                ));
            });
        }
        let child = document().create_element("span").expect("child");
        root.append_child(&child).expect("append child");
        dispatch_bubbling(
            &child,
            PORT_PUBLISH,
            &detail_object(&[("port", JsValue::from_str("out"))]),
        );
        let got = sink.borrow();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, None, "unmounted target resolves to no instance");
        assert_eq!(got[0].1, "SPAN");
    }

    #[wasm_bindgen_test]
    fn publish_listener_disambiguates_two_instances_of_one_kind() {
        // Two instances of one kind — the whole reason `instance_for_target` scans
        // `MOUNTED` by `is_same_node` (node identity) rather than parsing the tag.
        // A regression that returned the first `MOUNTED` entry would resolve both
        // dispatches to the same instance; this pins each to its own element.
        fresh_root();
        let kind = "wbt-two";
        mount_component("wbt-two-p1", kind);
        mount_component("wbt-two-p2", kind);
        let p1 = MOUNTED
            .with(|m| m.borrow().get("wbt-two-p1").cloned())
            .expect("p1 mounted");
        let p2 = MOUNTED
            .with(|m| m.borrow().get("wbt-two-p2").cloned())
            .expect("p2 mounted");
        let sink: PublishSink = Rc::new(RefCell::new(Vec::new()));
        {
            let sink = Rc::clone(&sink);
            install_publish_listener(move |instance, tag, port, body, urgency, _detail| {
                sink.borrow_mut().push((
                    instance.map(String::from),
                    tag.to_string(),
                    port.map(String::from),
                    body.map(String::from),
                    urgency,
                ));
            });
        }
        dispatch_bubbling(
            &p1,
            PORT_PUBLISH,
            &detail_object(&[("port", JsValue::from_str("out"))]),
        );
        dispatch_bubbling(
            &p2,
            PORT_PUBLISH,
            &detail_object(&[("port", JsValue::from_str("out"))]),
        );
        let got = sink.borrow();
        assert_eq!(got.len(), 2);
        assert_eq!(
            got[0].0.as_deref(),
            Some("wbt-two-p1"),
            "dispatch from p1's element resolves to p1"
        );
        assert_eq!(
            got[1].0.as_deref(),
            Some("wbt-two-p2"),
            "dispatch from p2's element resolves to p2 — not the first MOUNTED entry"
        );
    }

    #[wasm_bindgen_test]
    fn install_log_listener_resolves_instance_tag_and_fields() {
        fresh_root();
        let element = mount_probe("wbt-log-i", "wbt-log");
        let sink: TagPairSink = Rc::new(RefCell::new(Vec::new()));
        {
            let sink = Rc::clone(&sink);
            install_log_listener(move |instance, tag, level, message| {
                sink.borrow_mut().push((
                    instance.map(String::from),
                    tag.to_string(),
                    level.map(String::from),
                    message.map(String::from),
                ));
            });
        }
        dispatch_bubbling(
            &element,
            COMPONENT_LOG,
            &detail_object(&[
                ("level", JsValue::from_str("warn")),
                ("message", JsValue::from_str("hi")),
            ]),
        );
        dispatch_bubbling(
            &element,
            COMPONENT_LOG,
            &detail_object(&[("level", JsValue::from_str("warn"))]),
        );
        let got = sink.borrow();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0.as_deref(), Some("wbt-log-i"));
        assert_eq!(got[0].1, probe_tag("wbt-log-i", "wbt-log"));
        assert_eq!(got[0].2.as_deref(), Some("warn"));
        assert_eq!(got[0].3.as_deref(), Some("hi"));
        assert_eq!(got[1].3, None, "missing message field forwards None");
    }

    #[wasm_bindgen_test]
    fn install_alert_listener_resolves_instance_tag_and_fields() {
        fresh_root();
        let element = mount_probe("wbt-alert-i", "wbt-alert");
        let sink: AlertSink = Rc::new(RefCell::new(Vec::new()));
        {
            let sink = Rc::clone(&sink);
            install_alert_listener(move |instance, tag, severity, title, body| {
                sink.borrow_mut().push((
                    instance.map(String::from),
                    tag.to_string(),
                    severity.map(String::from),
                    title.map(String::from),
                    body.map(String::from),
                ));
            });
        }
        dispatch_bubbling(
            &element,
            COMPONENT_ALERT,
            &detail_object(&[
                ("severity", JsValue::from_str("page")),
                ("title", JsValue::from_str("t")),
                ("body", JsValue::from_str("b")),
            ]),
        );
        dispatch_bubbling(
            &element,
            COMPONENT_ALERT,
            &detail_object(&[
                ("severity", JsValue::from_str("page")),
                ("title", JsValue::from_str("t")),
            ]),
        );
        let got = sink.borrow();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0.as_deref(), Some("wbt-alert-i"));
        assert_eq!(got[0].1, probe_tag("wbt-alert-i", "wbt-alert"));
        assert_eq!(got[0].2.as_deref(), Some("page"));
        assert_eq!(got[0].3.as_deref(), Some("t"));
        assert_eq!(got[0].4.as_deref(), Some("b"));
        assert_eq!(got[1].4, None, "missing body field forwards None");
    }

    #[wasm_bindgen_test]
    fn instance_for_target_distinguishes_two_instances_of_one_kind() {
        // Two instances of one kind, each with its own instance-scoped element.
        // Resolution is by element identity over `MOUNTED`, never by parsing the
        // tag: each element resolves to its own instance; a never-mounted element
        // resolves to `None`.
        fresh_root();
        mount_component("wbt-ift-a", "wbt-ift");
        mount_component("wbt-ift-b", "wbt-ift");
        let a = MOUNTED
            .with(|m| m.borrow().get("wbt-ift-a").cloned())
            .expect("instance a mounted");
        let b = MOUNTED
            .with(|m| m.borrow().get("wbt-ift-b").cloned())
            .expect("instance b mounted");
        assert_eq!(instance_for_target(&a).as_deref(), Some("wbt-ift-a"));
        assert_eq!(instance_for_target(&b).as_deref(), Some("wbt-ift-b"));
        let stray = document().create_element("div").expect("stray");
        assert_eq!(instance_for_target(&stray), None);
    }

    #[wasm_bindgen_test]
    fn identity_survives_arrangement() {
        // Reparenting preserves element identity, so the MOUNTED registry and the
        // delegated-listener resolution keep working after chrome moves a wrapper.
        // This is the property that lets chrome arrange at all: a publish
        // dispatched from the component's element after the move still resolves to
        // its instance.
        fresh_root();
        let instance = "wbt-arr-i";
        let kind = "wbt-arr";
        mount_component(instance, kind);
        let element = MOUNTED
            .with(|m| m.borrow().get(instance).cloned())
            .expect("mounted element");

        // Arrange the wrapper the way chrome would: reparent it into a section.
        let section = document()
            .create_element("section")
            .expect("create section");
        section.set_id(&section_id(instance));
        surface_root()
            .append_child(&section)
            .expect("append section");
        section
            .append_child(&wrapper_of(instance).expect("wrapper exists"))
            .expect("reparent wrapper");

        assert_eq!(
            instance_for_target(&element).as_deref(),
            Some(instance),
            "identity survives the move"
        );

        let sink: PublishSink = Rc::new(RefCell::new(Vec::new()));
        {
            let sink = Rc::clone(&sink);
            install_publish_listener(move |instance, tag, port, body, urgency, _detail| {
                sink.borrow_mut().push((
                    instance.map(String::from),
                    tag.to_string(),
                    port.map(String::from),
                    body.map(String::from),
                    urgency,
                ));
            });
        }
        dispatch_bubbling(
            &element,
            PORT_PUBLISH,
            &detail_object(&[("port", JsValue::from_str("out"))]),
        );
        let got = sink.borrow();
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].0.as_deref(),
            Some(instance),
            "a publish from the moved element still routes to its instance"
        );
    }

    #[wasm_bindgen_test]
    fn install_component_panic_listener_forwards_and_tolerates_malformed() {
        let sink: PanicSink = Rc::new(RefCell::new(Vec::new()));
        {
            let sink = Rc::clone(&sink);
            install_component_panic_listener(move |component, message| {
                sink.borrow_mut()
                    .push((component.map(String::from), message.map(String::from)));
            });
        }
        dispatch_window(
            COMPONENT_PANIC,
            Some(&detail_object(&[
                ("instance", JsValue::from_str("wbt-panic")),
                ("message", JsValue::from_str("kaboom")),
            ])),
        );
        dispatch_window(
            COMPONENT_PANIC,
            Some(&detail_object(&[(
                "instance",
                JsValue::from_str("wbt-panic"),
            )])),
        );
        dispatch_window(COMPONENT_PANIC, None);
        let got = sink.borrow();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0.as_deref(), Some("wbt-panic"));
        assert_eq!(got[0].1.as_deref(), Some("kaboom"));
        assert_eq!(got[1].1, None, "missing message field forwards None");
        assert_eq!(got[2], (None, None), "non-CustomEvent forwards all None");
    }
}

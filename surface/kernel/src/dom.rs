//! web-sys effect executor for the kernel.
//!
//! Applies the DOM effects the decision core ([`crate::logic`]) emits. Compiled
//! only for the wasm32 (browser) target; the host build excludes it and unit-
//! tests the pure core instead.

use crate::contract::ActivationError;
use crate::contract::{
    ENTRY_REPLY_FIELD, PROCESSOR_START, SURFACE_READY, SURFACE_RELOAD, SURFACE_ROOT_ID,
};
use crate::front::SurfaceHandle;
use crate::schema::LogLevel;
use crate::{ActivationEntry, ActivationOutcome};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use js_sys::{Object, Reflect};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{CustomEvent, CustomEventInit, Document, Element, HtmlElement, Window};

use crate::schema::telemetry::{InstanceCounters, StatusCounters};

use crate::logic::{ConnectIndicatorState, KernelAction};

/// The `source` reported alongside kernel-originated log messages.
const KERNEL_LOG_SOURCE: &str = "kernel";

/// The `source` prefix the kernel stamps on a component-originated `brenn-log`
/// before publishing it as an error report: `"component:<instance>"`.
///
/// Human-readable detail only — the machine-readable twin is the report publish's
/// `attribution`, which the peer validates against the declared set and derives
/// the sender sub-identity from. The two are composed from the same instance id
/// at each call site, but only the latter is trusted.
const COMPONENT_LOG_SOURCE_PREFIX: &str = "component:";

thread_local! {
    /// The live mounted element for each component **instance**, keyed by instance
    /// id. wasm is single-threaded, so a thread-local map is the executor's whole
    /// shared state. This is the single source of truth for "is this instance
    /// mounted", so the DOM and the registry cannot disagree. An instance is
    /// present, mapped to the host element the kernel created, between
    /// [`mount_host`] and the [`render_error_card`] that removes its element.
    /// One component kind may back several instances, each its own entry.
    static MOUNTED: RefCell<HashMap<String, Element>> = RefCell::new(HashMap::new());
}

/// Whether component `instance` currently has a mounted element. Backs the
/// panic path's per-instance liveness check (`KernelCore::on_component_panic`).
pub fn is_mounted(instance: &str) -> bool {
    MOUNTED.with(|m| m.borrow().contains_key(instance))
}

/// The element the kernel created for `instance`, or `None` when it is not
/// mounted. The DOM host resolves `dom.root` through this, so a component's own
/// subtree is exactly the element the kernel gave it and never one it named.
pub(crate) fn mounted_element(instance: &str) -> Option<Element> {
    MOUNTED.with(|m| m.borrow().get(instance).cloned())
}

/// The kernel-owned wrapper element for `instance`, or `None` when the kernel
/// has not created one. Backs `page-dom.instance-wrapper`, whose `none` is the
/// ordinary transient of a sibling that has not registered yet.
pub(crate) fn wrapper_element(instance: &str) -> Option<Element> {
    document().get_element_by_id(&wrapper_id(instance))
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
    /// Lifetime count of Error-level reports emitted — the report's `errors`.
    /// Not a count of deaths: one death emits a varying number of Error reports.
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
    // TODO(surface-counters-host-testable): nothing here touches web-sys, but
    // living in a wasm-only module puts every "this trigger moves that column"
    // assertion behind a browser runner no gate drives, so a column's whole
    // write path can break unnoticed.
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
        // The legacy path has no telemetry-refusal counter of its own.
        telemetry_dropped: 0,
        instances: INSTANCE_COUNTERS.with(|c| c.borrow().clone()),
    }
}

/// One instance's counters read from [`read_counters`], or zero when it has
/// counted nothing yet.
///
/// The map is page-lifetime and the browser suite shares one thread, but
/// `fresh_root` clears it through [`forget_instance_counters`] before each
/// test, so a per-instance assertion reads an absolute. The surface-wide
/// scalars beside it are *not* cleared that way, so those stay delta
/// assertions against a snapshot taken in the test. Defined beside the counters
/// rather than in the test mod so it reads them without widening the
/// thread-locals' own visibility.
#[cfg(test)]
pub(crate) fn instance_counters(instance: &str) -> InstanceCounters {
    read_counters()
        .instances
        .get(instance)
        .copied()
        .unwrap_or_default()
}

/// Forget every instance's counters.
///
/// Page-lifetime state, and a page hosts one surface — so nothing outside a test
/// clears this. The browser suites share one page across every test, where the
/// accumulated key set outlives the wiring it was counted under: a later test's
/// status document would then name an instance its own bindings never declared,
/// which [`crate::telemetry::status_body`] is right to call fatal. Each test
/// starts from an empty map instead.
#[cfg(test)]
pub(crate) fn forget_instance_counters() {
    INSTANCE_COUNTERS.with(|c| c.borrow_mut().clear());
}

/// Bump a lifetime counter by one.
fn bump(counter: &'static std::thread::LocalKey<Cell<u64>>) {
    counter.with(|c| c.set(c.get().saturating_add(1)));
}

/// Add `n` to one instance's counter, creating its entry on first sight.
///
/// `field` picks the column, so no call site can transpose one column into
/// another: each names its own field and passes nothing else that could be
/// mistaken for a sibling column.
fn bump_instance(instance: &str, field: fn(&mut InstanceCounters) -> &mut u64, n: u64) {
    INSTANCE_COUNTERS.with(|c| {
        let mut map = c.borrow_mut();
        let entry = map.entry(instance.to_string()).or_default();
        let slot = field(entry);
        *slot = slot.saturating_add(n);
    });
}

/// Apply the [`KernelAction`]s the decision core emitted, in order, dispatching
/// each to its effect primitive. This is the one bridge from the DOM-free core's
/// `Vec<KernelAction>` output to the web-sys effects above; the core decides,
/// this executes.
///
/// `handle` is the surface client handle the client-touching actions need:
/// `PublishControl` queues the kernel's own control-plane statement, and
/// `Report` is the console-log + leveled-`log` treatment for the
/// transient/component-fault class (a rejected or refused publish) at `Warn`,
/// and for a component death at `Error`.
pub fn apply_actions(actions: &[KernelAction], handle: &SurfaceHandle) {
    for action in actions {
        apply_action(action, handle);
    }
}

/// Apply a single [`KernelAction`] by calling its effect primitive.
pub(crate) fn apply_action(action: &KernelAction, handle: &SurfaceHandle) {
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
        KernelAction::MountHost { instance, kind } => mount_host(instance, kind),
        KernelAction::EmitReady => emit_ready(),
        KernelAction::StartProcessors { instances } => start_processors(instances),
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
        KernelAction::Alert {
            attribution,
            severity,
            title,
            body,
        } => {
            handle.alert(attribution.as_deref(), *severity, title, body);
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
        // Counter-only: the next heartbeat carries the value, so a component
        // failing every delivery moves a number instead of a publish.
        KernelAction::CountActivationFailure { instance } => {
            bump_instance(instance, |c| &mut c.activation_failures, 1);
        }
    }
}

/// Write `message` to the browser console at `level` (always, the durable
/// client-side record) and hand it to [`SurfaceHandle::report`], which publishes
/// it to the surface's error channel when the wiring's floor admits `level`
/// and otherwise keeps it console-only. `source` attributes the report:
/// [`KERNEL_LOG_SOURCE`] for the kernel's own breadcrumbs, `"component:<instance>"`
/// for a forwarded `brenn-log`.
fn report(
    handle: &SurfaceHandle,
    level: LogLevel,
    source: &str,
    message: &str,
    subject_instance: Option<&str>,
) {
    // Sole bump site for ERRORS: the count and the report are the same act
    // and cannot drift apart.
    if level == LogLevel::Error {
        bump(&ERRORS);
    }
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
/// Called by the kernel at start (before any attachment) and on each link-state
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
    // Paint containment: the wrapper becomes the containing block for any
    // `position: fixed` descendant and clips its painting to the wrapper's box.
    // Declared here so no instance can exist without it.
    wrapper
        .style()
        .set_property("contain", "paint")
        .expect("set paint containment on the wrapper");
    staging()
        .append_child(&wrapper)
        .expect("append the new wrapper into staging");
    wrapper
}

/// Mount a page-hosted processor instance: create the plain host `div` its
/// `dom.root` resolves to and append it as the sole content of the instance's
/// wrapper. Clears any prior content first, so mounting is idempotent.
///
/// The host element carries no identity of its own. Identity lives one level up,
/// on the kernel-owned wrapper the component cannot reach: the host element is
/// handed to the component, whose allow-list admits every `data-` name, so a
/// stamp here would be a stamp the component could rewrite. Nothing needs one —
/// routing resolves instances by node identity through [`MOUNTED`], and page
/// styling selects the wrapper.
pub fn mount_host(instance: &str, kind: &str) {
    let doc = document();
    let wrapper = mount_wrapper(instance, kind);
    // The clear destroys whatever the instance built, so its handles go with it
    // — and so do any handle another instance minted into this subtree, which is why
    // the sweep runs beside the table drop, and before the clear, while the tree
    // can still say what is under the wrapper.
    crate::entry::reclaim_dom_subtree(&wrapper, false);
    crate::entry::forget_dom_instance(instance);
    wrapper.set_text_content(None);
    let element = doc.create_element("div").expect("document creates a div");
    // Must precede the append: `dom.root` must resolve when the mount
    // activation fires.
    MOUNTED.with(|m| m.borrow_mut().insert(instance.to_string(), element.clone()));
    wrapper
        .append_child(&element)
        .expect("append the host element into its wrapper");
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
    let doc = document();
    let wrapper = mount_wrapper(instance, kind);
    // The card replaces the instance's whole subtree, and the instance is
    // terminal, so nothing will ever reclaim its handles the ordinary way. The
    // sweep runs first, and before the clear: it frees every table's handles to
    // what is about to die, including a sibling's, while the tree still answers.
    crate::entry::reclaim_dom_subtree(&wrapper, false);
    crate::entry::forget_dom_instance(instance);
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

/// Count one publish for `instance` — the lifetime totals a status report carries.
/// Counted where a publish is admitted: the DOM seam's buffered route, the
/// deferred family's, and the processor host's, so an operator's total does not
/// depend on which transport a component reached the buffer through.
///
/// A publish the buffer accepted and dropped for want of a sink is not one of
/// these: it was never queued, and it is [`count_dropped_publish`]'s.
pub(crate) fn count_publish(instance: &str) {
    bump(&PUBLISHES);
    bump_instance(instance, |c| &mut c.publishes, 1);
}

/// Count one publish accepted and dropped because its declared port is unwired.
///
/// A column of its own rather than a share of `publishes`: the component was
/// told ok, the message went nowhere, and an operator reading a page's totals
/// against messages that never arrived needs the two apart. Per-instance only —
/// the question it answers is which component is publishing into an unwired
/// port, which a surface-wide total cannot say.
pub(crate) fn count_dropped_publish(instance: &str) {
    bump_instance(instance, |c| &mut c.dropped_publishes, 1);
}

/// Count whichever of the two an accepted publish turned out to be.
pub(crate) fn count_admission(instance: &str, admission: crate::publish_buffer::Admission) {
    match admission {
        crate::publish_buffer::Admission::Buffered => count_publish(instance),
        crate::publish_buffer::Admission::Dropped => count_dropped_publish(instance),
    }
}

/// Wrap a component's registered `entry` function into the kernel's
/// [`ActivationEntry`] — the kernel's half of the call convention.
///
/// One encode per activation (not per message, which is the whole point): the
/// activation is serialized to JSON and passed as the single argument. The return
/// value is the outcome, in four shapes:
///
/// - `undefined`/`null` → ok with no reply; the buffer flushes.
/// - an object carrying a string [`ENTRY_REPLY_FIELD`] → ok with that reply, the
///   buffer flushing the same way — but **only** on an activation whose `sync`
///   field names a port. A reply to a question nobody asked is a contract break,
///   so on an async activation it is a trap.
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
            Ok(value) if value.is_undefined() || value.is_null() => ActivationOutcome::Ok(None),
            Ok(value) => match value.as_string() {
                Some(message) => ActivationOutcome::Err(ActivationError { message }),
                None => classify_reply(&value, activation.sync.is_some()),
            },
            Err(thrown) => ActivationOutcome::Trap(js_error_message(&thrown)),
        }
    })
}

/// Classify an activation entry's non-string, non-nullish return: an object
/// carrying a string [`ENTRY_REPLY_FIELD`] is a sync reply, anything else is a
/// trap.
///
/// `is_sync` is the activation's own `sync` field, and it gates the whole shape
/// rather than merely the reply's usefulness: an entry that answers an async
/// activation is a component that lost track of why it was called, and reading
/// its ok would flush a buffer built under a misapprehension.
fn classify_reply(value: &JsValue, is_sync: bool) -> ActivationOutcome {
    let reply = value
        .is_object()
        .then(|| Reflect::get(value, &JsValue::from_str(ENTRY_REPLY_FIELD)).ok())
        .flatten()
        .and_then(|field| field.as_string());
    match (reply, is_sync) {
        (Some(reply), true) => ActivationOutcome::Ok(Some(reply)),
        (Some(_), false) => ActivationOutcome::Trap(
            "activation entry replied to an activation that asked nothing".to_string(),
        ),
        (None, _) => ActivationOutcome::Trap(
            "activation entry returned neither undefined, an error string, nor a reply object"
                .to_string(),
        ),
    }
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
// wasm-bindgen-test under a headless WebDriver browser; excluded from the
// host sweep (the whole module is wasm32-only). Isolation: every test that
// touches `#surface-root` starts from `fresh_root`, and every test that touches
// `MOUNTED` uses a unique `wbt-*` instance id (it is page-lifetime).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::Activation;
    use crate::wasm_test_util::{capture_window_event, fresh_root, str_field};
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

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
                    .map(|i| brenn_surface_test_fixtures::sample_envelope_json(&format!("m{i}")))
                    .collect(),
                new_from: 0,
                dropped: *dropped,
            })
            .collect();
        let _ = entry(&Activation {
            ports,
            deferred: vec![],
            now: None,
            sync: None,
        });
    }

    // ── activation entry call convention ──────────────────────────────────

    /// A JS entry whose body is `source`, taking the kernel's one JSON argument.
    fn js_entry(source: &str) -> js_sys::Function {
        js_sys::Function::new_with_args("_json", source)
    }

    /// A minimal activation to feed the wrapper; its contents are irrelevant to
    /// the return-value classification these tests pin, apart from `sync`, which
    /// gates the reply shape.
    fn one_port_activation() -> crate::contract::Activation {
        crate::contract::Activation {
            ports: vec![crate::contract::PortWindow {
                port: "messages".to_string(),
                envelopes: vec![brenn_surface_test_fixtures::sample_envelope_json("m")],
                new_from: 0,
                dropped: 0,
            }],
            deferred: vec![],
            now: None,
            sync: None,
        }
    }

    /// The same activation, but sync-caused: a reply is an answer someone asked
    /// for.
    fn sync_activation() -> crate::contract::Activation {
        crate::contract::Activation {
            sync: Some("ack".to_string()),
            ..one_port_activation()
        }
    }

    #[wasm_bindgen_test]
    fn wrap_activation_entry_classifies_every_return() {
        let activation = one_port_activation();
        let i = "wbt-wrap-ret";

        // undefined / null → ok (buffer flushes).
        assert!(matches!(
            wrap_activation_entry(i, js_entry("return undefined;"))(&activation),
            ActivationOutcome::Ok(None)
        ));
        assert!(matches!(
            wrap_activation_entry(i, js_entry("return null;"))(&activation),
            ActivationOutcome::Ok(None)
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

        // An object carrying a string reply → ok with that reply, but only on the
        // activation that asked. On an async one it is an answer nobody wanted, and
        // reading it as ok would flush a buffer built under a misapprehension.
        match wrap_activation_entry(i, js_entry("return { reply: '{\"cancel\":true}' };"))(
            &sync_activation(),
        ) {
            ActivationOutcome::Ok(Some(reply)) => assert_eq!(reply, "{\"cancel\":true}"),
            other => panic!("expected Ok with a reply, got {other:?}"),
        }
        assert!(
            matches!(
                wrap_activation_entry(i, js_entry("return { reply: 'anything' };"))(&activation),
                ActivationOutcome::Trap(_)
            ),
            "a reply to an async activation is a contract break, not an ok"
        );

        // An object with nothing readable under `reply` is gibberish, whichever
        // kind of activation it answers: a returned object claims to be an answer.
        for shape in ["return {};", "return { reply: 7 };"] {
            for activation in [&activation, &sync_activation()] {
                assert!(
                    matches!(
                        wrap_activation_entry(i, js_entry(shape))(activation),
                        ActivationOutcome::Trap(_)
                    ),
                    "{shape} is not a conformant reply"
                );
            }
        }

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
        mount_host(instance, kind);
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

        mount_host(instance, kind);
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
    fn mount_host_gives_the_instance_a_plain_div_it_can_build_in() {
        fresh_root();
        let instance = "wbt-host-i";
        let kind = "wbt-host";
        mount_host(instance, kind);

        let element = mounted_element(instance).expect("the host element is registered");
        assert_eq!(
            element.tag_name().to_lowercase(),
            "div",
            "the host element is a plain div"
        );
        let wrapper = wrapper_of(instance).expect("the wrapper exists");
        // Identity is the wrapper's, not the host element's: the component can
        // write any `data-` name on the element it is handed, so a stamp there
        // would be forgeable.
        assert_eq!(element.get_attribute("data-instance"), None);
        assert_eq!(element.get_attribute(WRAPPER_KIND_ATTR), None);
        assert_eq!(
            wrapper.get_attribute("data-instance").as_deref(),
            Some(instance)
        );
        assert_eq!(
            wrapper.get_attribute(WRAPPER_KIND_ATTR).as_deref(),
            Some(kind)
        );
        assert!(
            element
                .parent_element()
                .expect("the host element has a parent")
                .is_same_node(Some(wrapper.as_ref())),
            "the host element is the sole content of the instance's wrapper"
        );

        // Idempotent: a second call clears the wrapper and leaves exactly one
        // host element.
        mount_host(instance, kind);
        assert_eq!(wrapper.child_element_count(), 1);
        let again = mounted_element(instance).expect("still registered");
        assert!(again.is_same_node(wrapper.first_element_child().as_deref()));
    }

    #[wasm_bindgen_test]
    fn a_wrapper_declares_paint_containment() {
        fresh_root();
        let instance = "wbt-contain-i";
        let wrapper = mount_wrapper(instance, "wbt-contain");
        assert_eq!(
            computed(wrapper.as_ref(), "contain"),
            "paint",
            "declared at creation, so no instance can exist without it"
        );
    }

    #[wasm_bindgen_test]
    fn a_fixed_position_descendant_paints_inside_its_wrapper() {
        // The behavioural half: `contain: paint` makes the wrapper the
        // containing block for a `position: fixed` descendant and clips its
        // painting to the wrapper's box, so a component that positions itself
        // `fixed` covers its own region rather than the surface.
        let root = fresh_root();
        let wrapper = mount_wrapper("wbt-contain-fixed-i", "wbt-contain-fixed");
        // Out of the hidden staging pen: a `display: none` subtree has no boxes
        // to compare, which would make the assertions below vacuous.
        root.append_child(&wrapper)
            .expect("arrange the wrapper into the visible root");
        wrapper
            .style()
            .set_property("width", "120px")
            .expect("size the wrapper");
        wrapper
            .style()
            .set_property("height", "60px")
            .expect("size the wrapper");
        let escapee = document()
            .create_element("div")
            .expect("document creates a div")
            .dyn_into::<HtmlElement>()
            .expect("created div is an HtmlElement");
        for (name, value) in [
            ("position", "fixed"),
            ("top", "0"),
            ("left", "0"),
            ("right", "0"),
            ("bottom", "0"),
        ] {
            escapee
                .style()
                .set_property(name, value)
                .expect("stretch the descendant over its containing block");
        }
        wrapper
            .append_child(&escapee)
            .expect("append the descendant");

        assert_eq!(
            (wrapper.offset_width(), wrapper.offset_height()),
            (120, 60),
            "the wrapper is laid out, so the comparison below is not two empty boxes"
        );
        assert_eq!(
            (escapee.offset_width(), escapee.offset_height()),
            (wrapper.offset_width(), wrapper.offset_height()),
            "the descendant's containing block is the wrapper, not the viewport"
        );
        assert_eq!(
            (escapee.offset_left(), escapee.offset_top()),
            (0, 0),
            "and it is positioned from the wrapper's own corner"
        );
    }

    /// One element's computed value for `property`.
    fn computed(element: &Element, property: &str) -> String {
        web_sys::window()
            .expect("kernel runs in a browser with a window")
            .get_computed_style(element)
            .expect("computed style is readable")
            .expect("the element has a computed style")
            .get_property_value(property)
            .expect("the property is readable")
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

    /// The sum of every column of one instance's row, written as an exhaustive
    /// destructure so a new column cannot join [`InstanceCounters`] without
    /// being listed in the "nothing else moved" assertions below.
    fn column_sum(counters: &InstanceCounters) -> u64 {
        let InstanceCounters {
            publishes,
            drops,
            dropped_publishes,
            activation_failures,
        } = *counters;
        publishes + drops + dropped_publishes + activation_failures
    }

    /// Each per-instance column moves on its own trigger and on nothing else:
    /// one case per column of [`InstanceCounters`], asserting the named column
    /// moves by the named amount on the named instance while every other column
    /// on that instance, the whole of a sibling's row, and the surface-wide
    /// error total all stand still. The per-instance grain exists so an operator
    /// asking "which component is losing messages?" gets an answer rather than a
    /// surface-wide total, and counting a per-instance fact is never reporting
    /// one. A new column joins by adding a case.
    ///
    /// The drops case carries two lossy ports because that column takes an
    /// activation's whole loss summed across its windows, not one per message.
    /// `fresh_root` clears the counter map, so each case reads absolutes.
    ///
    /// The common surface-wide assertion is `errors` alone, because two of the
    /// cases legitimately move a surface-wide total of their own (a publish
    /// moves `publishes`, a lossy activation moves `deliveries`). The full
    /// "no surface-wide total moved" claim for the failure count is
    /// [`counting_an_activation_failure_is_counter_only`].
    #[wasm_bindgen_test]
    fn each_instance_column_moves_only_on_its_own_trigger() {
        type Trigger = fn(&str, &SurfaceHandle);
        type Column = fn(&InstanceCounters) -> u64;
        let cases: [(&str, Trigger, Column, u64); 4] = [
            // Called directly rather than through the real publish path, which
            // needs a live wasm slot this suite cannot rig. This case pins
            // the column and its isolation, not the attribution of a real
            // publish to the instance that asked for it.
            (
                "publishes",
                |instance, _handle| count_publish(instance),
                |c| c.publishes,
                1,
            ),
            (
                "drops",
                |instance, _handle| count_activation(instance, &[("p", 0, 3), ("other", 0, 4)]),
                |c| c.drops,
                7,
            ),
            // Also called directly, and for the same reason: the drop verdict is
            // read off the buffer at the wasm port seam this suite cannot rig.
            (
                "dropped_publishes",
                |instance, _handle| count_dropped_publish(instance),
                |c| c.dropped_publishes,
                1,
            ),
            (
                "activation_failures",
                |instance, handle| {
                    apply_action(
                        &KernelAction::CountActivationFailure {
                            instance: instance.to_string(),
                        },
                        handle,
                    );
                },
                |c| c.activation_failures,
                1,
            ),
        ];

        for (column, trigger, read, expected) in cases {
            fresh_root();
            let (handle, _events, _channels) = crate::front::new();
            let (a, b) = ("wbt-ctr-col-a", "wbt-ctr-col-b");
            let before_errors = errors_now();

            trigger(a, &handle);

            let (moved, sibling) = (instance_counters(a), instance_counters(b));
            assert_eq!(read(&moved), expected, "{column} moved on its own instance");
            assert_eq!(
                column_sum(&moved),
                expected,
                "{column} is the only column of its own instance that moved"
            );
            assert_eq!(
                column_sum(&sibling),
                0,
                "the sibling's row is untouched by {column}"
            );
            assert_eq!(
                errors_now(),
                before_errors,
                "counting {column} is not reporting an error"
            );
        }
    }

    /// The join between the buffer's verdict and the column it lands in. The two
    /// halves are pinned apart — `Admission::of` in `publish_buffer`, the columns
    /// above — and this is the mapping between them, which an inversion or a
    /// copy-paste would silently break while both halves stayed green.
    #[wasm_bindgen_test]
    fn an_admission_lands_in_the_column_its_verdict_names() {
        use crate::publish_buffer::Admission;

        fresh_root();
        let buffered = "wbt-ctr-adm-b";
        count_admission(buffered, Admission::Buffered);
        let counters = instance_counters(buffered);
        assert_eq!(counters.publishes, 1);
        assert_eq!(
            column_sum(&counters),
            1,
            "a buffered publish moves `publishes` and nothing else"
        );

        let dropped = "wbt-ctr-adm-d";
        count_admission(dropped, Admission::Dropped);
        let counters = instance_counters(dropped);
        assert_eq!(counters.dropped_publishes, 1);
        assert_eq!(
            column_sum(&counters),
            1,
            "a dropped publish moves `dropped_publishes` and nothing else"
        );
    }

    /// A delivery moves the surface-wide total and no per-instance column at
    /// all, so a busy-but-healthy instance (new messages, nothing dropped) never
    /// reads as lossy.
    #[wasm_bindgen_test]
    fn deliveries_move_no_per_instance_column() {
        fresh_root();
        let instance = "wbt-ctr-msg-i";
        let before = read_counters().deliveries;

        count_activation(instance, &[("p", 2, 0)]);

        assert_eq!(
            read_counters().deliveries - before,
            2,
            "both new envelopes are counted in the surface-wide total"
        );
        assert_eq!(
            column_sum(&instance_counters(instance)),
            0,
            "a delivery is neither a drop, a publish, nor a failure"
        );
    }

    /// Counting an activation failure is counter-only: the instance's column
    /// moves, and no surface-wide total, control statement, publish, alert or
    /// telemetry document goes with it. The absence is the contract — an
    /// instance failing on every delivery must not put a per-message write back
    /// on any channel — so it is asserted where a reader looks for it.
    #[wasm_bindgen_test]
    fn counting_an_activation_failure_is_counter_only() {
        fresh_root();
        let (handle, _events, mut channels) = crate::front::new();
        let instance = "wbt-ctr-only-i";
        let before = read_counters();

        apply_action(
            &KernelAction::CountActivationFailure {
                instance: instance.to_string(),
            },
            &handle,
        );

        let after = read_counters();
        assert_eq!(instance_counters(instance).activation_failures, 1);
        assert_eq!(
            (
                after.deliveries,
                after.publishes,
                after.errors,
                after.telemetry_dropped
            ),
            (
                before.deliveries,
                before.publishes,
                before.errors,
                before.telemetry_dropped
            ),
            "a per-instance failure count moves no surface-wide total"
        );
        assert!(
            channels.control_rx.try_recv().is_err(),
            "no control statement"
        );
        assert!(channels.publish_rx.try_recv().is_err(), "no publish");
        assert!(channels.alert_rx.try_recv().is_err(), "no alert");
        assert!(
            channels.telemetry_rx.try_recv().is_err(),
            "no telemetry document"
        );
    }

    // ── error counting ────────────────────────────────────────────────────

    /// The surface-wide error total. A delta assertion, like every counter
    /// assertion here: the counters are page-lifetime and the suite shares a
    /// thread.
    fn errors_now() -> u64 {
        read_counters().errors
    }

    /// A front door whose wiring declares an error channel at `floor`, so a
    /// report clearing it is published as well as written to the console. The
    /// bare [`crate::front::new`] door declares none and publishes nothing.
    fn reporting_front(
        floor: LogLevel,
    ) -> (
        crate::front::SurfaceHandle,
        crate::front::EventStream,
        crate::front::FrontChannels,
    ) {
        use crate::test_support::{bindings as fixtures, pages};

        let mut doc = fixtures::doc(
            vec![fixtures::component(fixtures::CHROME)],
            vec![],
            vec![],
            vec![],
        );
        doc.platform.error_channel = Some("brenn:site.surface.wbt.errors".to_string());
        doc.platform.error_report_floor = Some(floor);
        let page = pages::configured_page(
            "ephemeral:site.surface.wbt.bindings",
            uuid::Uuid::from_u128(0xe_44_09),
            pages::facts(),
            &[],
            &doc,
            brenn_attach_client::Millis(1_000),
        );

        let (handle, events, channels) = crate::front::new();
        channels
            .gate
            .lock()
            .expect("surface client: the publish gate mutex is poisoned")
            .refresh(&page);
        (handle, events, channels)
    }

    /// A counted error is a reported error, and the count follows the console
    /// write rather than the channel publish. A
    /// surface declaring no error channel publishes nothing and still counts —
    /// the console copy is the report — so a bump that drifted below the
    /// publish gate fails here.
    #[wasm_bindgen_test]
    fn an_error_report_counts_whether_or_not_the_channel_takes_it() {
        for with_channel in [false, true] {
            let (handle, _events, mut channels) = if with_channel {
                reporting_front(LogLevel::Error)
            } else {
                crate::front::new()
            };
            let before = errors_now();
            apply_action(
                &KernelAction::Report {
                    level: LogLevel::Error,
                    message: "wbt: a kernel error".to_string(),
                    subject: Some("wbt-errctr-gate-i".to_string()),
                },
                &handle,
            );
            assert_eq!(errors_now() - before, 1, "with_channel={with_channel}");
            assert_eq!(
                channels.publish_rx.try_recv().is_ok(),
                with_channel,
                "the publish happens only where a channel is declared, and the \
                 counter moved either way"
            );
        }
    }

    /// `counters.errors` means "Error-level reports emitted", so it moves exactly
    /// when a line goes to the console and is offered to the error channel — for
    /// the kernel's own reports and a component's alike — and not at all below
    /// Error.
    #[wasm_bindgen_test]
    fn an_error_report_counts_and_a_lesser_level_does_not() {
        let (handle, _events, _channels) = crate::front::new();
        let instance = "wbt-errctr-i";
        let before = errors_now();

        apply_action(
            &KernelAction::Report {
                level: LogLevel::Error,
                message: "wbt: a kernel error".to_string(),
                subject: Some(instance.to_string()),
            },
            &handle,
        );
        assert_eq!(errors_now() - before, 1);

        apply_action(
            &KernelAction::Report {
                level: LogLevel::Warn,
                message: "wbt: a kernel warning".to_string(),
                subject: Some(instance.to_string()),
            },
            &handle,
        );
        apply_action(
            &KernelAction::ComponentLog {
                instance: instance.to_string(),
                level: LogLevel::Warn,
                message: "wbt: a component warning".to_string(),
            },
            &handle,
        );
        assert_eq!(
            errors_now() - before,
            1,
            "nothing below Error is an error, whoever wrote it"
        );

        apply_action(
            &KernelAction::ComponentLog {
                instance: instance.to_string(),
                level: LogLevel::Error,
                message: "wbt: a component error".to_string(),
            },
            &handle,
        );
        assert_eq!(
            errors_now() - before,
            2,
            "a component's Error reaches the same console and the same channel"
        );
    }

    /// Rendering a card is not itself a report: the death that caused it emits
    /// one, and counting the card too would put a number in the status document
    /// that no line in the console accounts for.
    #[wasm_bindgen_test]
    fn rendering_an_error_card_does_not_count_on_its_own() {
        fresh_root();
        let before = errors_now();
        render_error_card("wbt-errctr-card-i", "wbt-errctr-card", "boom");
        assert_eq!(errors_now(), before);
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

    // ── identity under arrangement ────────────────────────────────────────

    #[wasm_bindgen_test]
    fn identity_survives_arrangement() {
        // Reparenting preserves element identity, so the MOUNTED registry keeps
        // working after chrome moves a wrapper. This is the property that lets
        // chrome arrange at all: the host element a component's `dom.root`
        // resolves to is the same element after the move.
        fresh_root();
        let instance = "wbt-arr-i";
        let kind = "wbt-arr";
        mount_host(instance, kind);
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

        assert!(
            mounted_element(instance)
                .expect("still mounted")
                .is_same_node(Some(element.as_ref())),
            "identity survives the move"
        );
        assert!(
            element
                .parent_element()
                .expect("host has a parent")
                .parent_element()
                .expect("wrapper has a parent")
                .is_same_node(Some(section.as_ref())),
            "the host element travelled with its wrapper"
        );
    }
}

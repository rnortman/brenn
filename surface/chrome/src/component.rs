//! The chrome component's browser (wasm32) half.
//!
//! Registers the chrome custom element via the shared component-support helpers
//! and drives the DOM from the [`ChromeAction`]s the DOM-free
//! [`crate::logic::ChromeCore`] emits. Chrome is an ordinary contract-v1 `dom`
//! component: the kernel activates it like any other, once per activation with
//! every bound input port windowed. Each delivered message body is extracted
//! from its envelope and folded into the core; the actions the fold returns are
//! applied here.
//!
//! Chrome holds the page-DOM-authority grant: it reparents the kernel's
//! `display:contents` wrappers into its own layout sections and stamps
//! `data-theme`/`data-takeover`, but only from this module — the decision logic
//! is DOM-free and host-tested.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use brenn_surface_component_support::{
    Activation, PersistentTimer, Publisher, append, boot, claim_initialized, component_log,
    create_div, document, publish, read_monotonic_ms, register_component,
};
use brenn_surface_contract::SURFACE_ROOT_ID;
use brenn_surface_proto::layout::LayoutKind;
use brenn_surface_proto::{ToastSeverity, ToastSource};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::{Document, Element, HtmlElement};

use crate::logic::{
    BannerState, ChromeAction, ChromeCore, LayoutPlacement, PORT_OVERLAY_STATE, Theme, TimerAction,
    fold_window,
};

/// This component's kind — its config `kind`, its element-tag stem
/// (`brenn-<kind>`), and the `component` field of its panic events.
const KIND: &str = "chrome";

/// The `#surface-root` attribute naming the active layout, targeted by skin CSS
/// grid templates.
const LAYOUT_ATTR: &str = "data-layout";
/// The per-section attribute naming the layout slot a section fills (`a`/`b`/`c`);
/// absent on an unassigned section, which the base CSS hides.
const PANEL_ATTR: &str = "data-panel";
/// The `#surface-root` CSS custom property carrying the layout split fraction.
const RATIO_PROP: &str = "--surface-ratio";
/// The marker attribute on a section's chrome-rendered panel-label `<header>`.
const PANEL_LABEL_ATTR: &str = "data-panel-label";
/// The `<body>` attribute carrying the runtime theme axis.
const THEME_ATTR: &str = "data-theme";
/// The `#surface-root` attribute set while a takeover overlay is active.
const TAKEOVER_ATTR: &str = "data-takeover";
/// The id of chrome's single connection-banner element.
const BANNER_ID: &str = "brenn-surface-banner";
/// The id of chrome's toast container under `#surface-root`.
const TOAST_CONTAINER_ID: &str = "brenn-surface-toasts";
/// How often the toast-lifetime timer fires to auto-dismiss expired toasts. The
/// core's TTL is coarse (seconds), so a one-second cadence expires a toast within
/// a tick of its deadline without a busy loop.
const TOAST_TICK_MS: i32 = 1_000;

/// The timestamp the core folds toast expiry against: whole milliseconds on the
/// page's monotonic clock, not the wall clock. A toast lifetime is a duration,
/// and an NTP step or a suspend/resume jump on the wall clock would otherwise
/// expire every live toast on the next tick — the operator never reading it.
fn now_ms() -> u64 {
    read_monotonic_ms()
}

/// The kernel-owned wrapper id scheme. Chrome reparents these into its sections;
/// the kernel creates and owns them (its element and everything inside it). The
/// scheme is a cross-crate contract with the kernel and must match its writer.
fn wrapper_id(instance: &str) -> String {
    format!("brenn-surface-wrapper-{instance}")
}

/// Chrome's per-instance layout section id.
fn section_id(instance: &str) -> String {
    format!("brenn-surface-section-{instance}")
}

thread_local! {
    /// The shared decision core and its rendered toast elements. wasm is
    /// single-threaded, so a thread-local is the module's whole shared state; one
    /// module record backs one chrome instance, so this is that instance's alone.
    static STATE: RefCell<Option<ChromeState>> = const { RefCell::new(None) };
}

/// Chrome's page-lifetime state: the decision core, the host element to log
/// against, and the live toast elements keyed by the core's toast id.
struct ChromeState {
    core: ChromeCore,
    host: Option<HtmlElement>,
    toasts: HashMap<u64, HtmlElement>,
    /// The toast-lifetime timer, held so its scheduled fire stays alive. Armed
    /// only while a toast with an expiry is live; while it is armed it fires
    /// every [`TOAST_TICK_MS`] to run [`ChromeCore::tick`].
    toast_timer: Rc<PersistentTimer>,
    /// Whether [`ChromeState::toast_timer`] currently has a fire scheduled.
    toast_timer_armed: bool,
}

/// Arm the toast tick while an expiring toast is live and cancel it otherwise,
/// so the page has no periodic wakeup in the steady state (no toasts, or only
/// `error` toasts, which never expire).
fn sync_toast_timer(state: &mut ChromeState) {
    match state.core.timer_action(state.toast_timer_armed) {
        Some(TimerAction::Arm) => {
            state.toast_timer.reschedule(TOAST_TICK_MS);
            state.toast_timer_armed = true;
        }
        Some(TimerAction::Cancel) => {
            state.toast_timer.cancel();
            state.toast_timer_armed = false;
        }
        None => {}
    }
}

/// The loader's entry, called once after this module's `default` init with the
/// instance this module record was loaded for. Boots the panic hook and builds
/// the core keyed on this instance (so chrome excludes itself from arrangement),
/// then registers the element and its activation entry.
#[wasm_bindgen]
pub fn brenn_bind_instance(instance: String) {
    boot(&instance);
    let toast_timer = Rc::new(PersistentTimer::new());
    {
        toast_timer.set_callback(Rc::new(move || {
            STATE.with(|s| {
                let mut guard = s.borrow_mut();
                if let Some(state) = guard.as_mut() {
                    // The fire that just ran consumed the schedule.
                    state.toast_timer_armed = false;
                    let actions = state.core.tick(now_ms());
                    apply_actions(state, &actions);
                }
            });
        }));
    }
    STATE.with(|s| {
        *s.borrow_mut() = Some(ChromeState {
            core: ChromeCore::new(instance.clone()),
            host: None,
            toasts: HashMap::new(),
            toast_timer,
            toast_timer_armed: false,
        });
    });
    register_component(
        KIND,
        on_connected,
        move |activation: &Activation, _publisher: &mut Publisher| {
            on_activation(activation);
            Ok(())
        },
    );
}

/// Record the host element on the element's first `connectedCallback`. Chrome
/// builds no UI inside its own element — it drives `#surface-root`, `<body>`,
/// and the kernel wrappers — so this only stashes the host for `brenn-log`
/// forwarding and claims the one-time init guard.
fn on_connected(host: HtmlElement) {
    if !claim_initialized(&host, KIND) {
        return;
    }
    STATE.with(|s| {
        s.borrow_mut()
            .as_mut()
            .expect("brenn_bind_instance runs before the first connectedCallback")
            .host = Some(host);
    });
}

/// Fold each window's new messages into the core and apply the actions they
/// return.
fn on_activation(activation: &Activation) {
    STATE.with(|s| {
        let mut guard = s.borrow_mut();
        let state = guard
            .as_mut()
            .expect("brenn_bind_instance runs before the first activation");
        for window in &activation.ports {
            let actions = fold_window(&mut state.core, window, now_ms());
            apply_actions(state, &actions);
        }
    });
}

/// Apply the core's actions in order. `state` is borrowed so a `ShowToast`/
/// `DismissToast` can record or drop the toast element.
fn apply_actions(state: &mut ChromeState, actions: &[ChromeAction]) {
    for action in actions {
        match action {
            ChromeAction::SetTheme(theme) => set_theme(*theme),
            ChromeAction::SetBanner(banner) => render_banner(*banner),
            ChromeAction::SetTakeover(on) => set_takeover(*on),
            ChromeAction::ApplyLayout {
                kind,
                ratio,
                panels,
                instances,
            } => apply_layout(*kind, ratio.as_deref(), panels, instances),
            ChromeAction::ShowToast {
                id,
                severity,
                text,
                source,
            } => show_toast(state, *id, *severity, text, *source),
            ChromeAction::DismissToast { id } => dismiss_toast(state, *id),
            ChromeAction::Log { level, message } => {
                if let Some(host) = state.host.as_ref() {
                    component_log(host, *level, message);
                }
            }
            ChromeAction::PublishOverlayState { body } => {
                if let Some(host) = state.host.as_ref() {
                    publish(host, PORT_OVERLAY_STATE, body);
                }
            }
        }
    }
    sync_toast_timer(state);
}

/// The live `Document`.
fn doc() -> Document {
    document()
}

/// Chrome's DOM root (`#surface-root`), rendered by the backend page.
fn surface_root() -> Element {
    doc()
        .get_element_by_id(SURFACE_ROOT_ID)
        .expect("backend page renders #surface-root")
}

/// Find the existing `#id` element, or create a `<tag>` with that id and append
/// it under `parent`.
fn find_or_create_child(parent: &Element, id: &str, tag: &str) -> HtmlElement {
    match doc().get_element_by_id(id) {
        Some(el) => el
            .dyn_into::<HtmlElement>()
            .expect("existing element is an HtmlElement"),
        None => {
            let el = doc()
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

/// Write the runtime theme axis to `<body>` (`data-theme`) — the token scope the
/// skin and theme stamps share, so a themed token override cascades to every
/// component identically.
fn set_theme(theme: Theme) {
    doc()
        .body()
        .expect("backend page renders a <body>")
        .set_attribute(THEME_ATTR, theme.as_wire_str())
        .expect("set data-theme on <body>");
}

/// Set or clear the takeover chrome flag on `#surface-root`. The synthesized
/// overlay layout is applied by a sibling [`ChromeAction::ApplyLayout`]; this
/// only carries the flag.
fn set_takeover(on: bool) {
    let root = surface_root();
    if on {
        root.set_attribute(TAKEOVER_ATTR, "true")
            .expect("set data-takeover on #surface-root");
    } else {
        root.remove_attribute(TAKEOVER_ATTR)
            .expect("remove data-takeover from #surface-root");
    }
}

/// Render the connection banner to reflect `state`. Server-supplied text never
/// reaches the DOM as markup (`textContent` only); `Hidden` hides the node
/// without removing it, so a later change re-shows the same element.
fn render_banner(state: BannerState) {
    let banner = find_or_create_child(&surface_root(), BANNER_ID, "div");
    match state {
        BannerState::Hidden => {
            banner.set_hidden(true);
            banner.set_text_content(None);
        }
        _ => {
            banner.set_hidden(false);
            banner.set_text_content(Some(banner_text(state)));
        }
    }
    banner
        .set_attribute("data-banner-state", banner_state_name(state))
        .expect("set data-banner-state attribute");
}

/// The user-facing banner text for a state, as `textContent`.
fn banner_text(state: BannerState) -> &'static str {
    match state {
        BannerState::Connecting => "Connecting…",
        BannerState::Reconnecting => "Reconnecting…",
        BannerState::Reloading => "Update available — reloading",
        BannerState::Fatal => "Connection failed — reload to retry",
        BannerState::Hidden => unreachable!("Hidden banner renders no text"),
    }
}

/// The stable state name written to `data-banner-state`.
fn banner_state_name(state: BannerState) -> &'static str {
    match state {
        BannerState::Connecting => "connecting",
        BannerState::Reconnecting => "reconnecting",
        BannerState::Reloading => "reloading",
        BannerState::Fatal => "fatal",
        BannerState::Hidden => "hidden",
    }
}

/// Find (or create, on first arrange) an instance's layout section under
/// `#surface-root`. Chrome's element: it carries the layout state (`data-panel`,
/// the label header) and holds the instance's kernel wrapper.
fn panel_section(instance: &str) -> HtmlElement {
    let section = find_or_create_child(&surface_root(), &section_id(instance), "section");
    section
        .set_attribute("data-instance", instance)
        .expect("set data-instance on the layout section");
    section
}

/// Apply a layout atomically: set the root's `data-layout` (and `--surface-ratio`
/// when present, else remove it), then place each of the surface's `instances` —
/// one named in `panels` gets its `data-panel` slot and label header; every other
/// has both cleared.
///
/// The one place that exercises chrome's page-DOM authority: it reparents each
/// instance's kernel-owned wrapper into that instance's layout section and stamps
/// layout attributes on the section — never inside the wrapper. Reparenting
/// preserves element identity, so the kernel's registry and per-element dispatch
/// are untouched; a wrapper already in its section is left alone, so a slot or
/// label change moves no node and re-runs no `connectedCallback`.
fn apply_layout(
    kind: LayoutKind,
    ratio: Option<&str>,
    panels: &[LayoutPlacement],
    instances: &[String],
) {
    let root = surface_root();
    root.set_attribute(LAYOUT_ATTR, kind.as_wire_str())
        .expect("set data-layout on #surface-root");
    let style = root
        .dyn_ref::<HtmlElement>()
        .expect("#surface-root is an HtmlElement")
        .style();
    match ratio {
        Some(value) => style
            .set_property(RATIO_PROP, value)
            .expect("set --surface-ratio custom property"),
        None => {
            style
                .remove_property(RATIO_PROP)
                .expect("remove --surface-ratio custom property");
        }
    }

    for instance in instances {
        let section = panel_section(instance);
        adopt_wrapper(&section, instance);
        match panels.iter().find(|p| &p.instance == instance) {
            Some(placement) => {
                section
                    .set_attribute(PANEL_ATTR, &placement.slot)
                    .expect("set data-panel on assigned section");
                set_panel_label(section.as_ref(), placement.label.as_deref());
            }
            None => {
                section
                    .remove_attribute(PANEL_ATTR)
                    .expect("remove data-panel from unassigned section");
                set_panel_label(section.as_ref(), None);
            }
        }
    }
}

/// Reparent `instance`'s kernel wrapper into its layout section, unless it is
/// already there. The already-there check is not an optimization: moving a node
/// re-runs the component's `connectedCallback`, so an unneeded reparent would
/// re-connect it. In steady state a wrapper moves exactly once — out of staging,
/// into its section, on the first arrange.
///
/// Panics if the wrapper does not exist: the kernel creates one per instance
/// before any layout is applied, and every layout carries the instance table
/// those wrappers were built from, so a missing wrapper is an invariant
/// violation, not a condition to route around.
fn adopt_wrapper(section: &HtmlElement, instance: &str) {
    let wrapper = doc()
        .get_element_by_id(&wrapper_id(instance))
        .expect("every instance chrome arranges has a kernel-mounted wrapper");
    let placed = wrapper
        .parent_element()
        .is_some_and(|parent| parent.is_same_node(Some(section.as_ref())));
    if !placed {
        section
            .append_child(&wrapper)
            .expect("reparent the instance's wrapper into its layout section");
    }
}

/// Render (or clear) a section's panel-label `<header>`. Label text is
/// `textContent` only — operator/LLM-supplied text never renders as markup.
fn set_panel_label(section: &Element, label: Option<&str>) {
    let existing = section
        .query_selector(&format!(":scope > header[{PANEL_LABEL_ATTR}]"))
        .expect("query panel-label header");
    match label {
        Some(text) => {
            let header = match existing {
                Some(header) => header,
                None => {
                    let header = doc()
                        .create_element("header")
                        .expect("document creates a header");
                    header
                        .set_attribute(PANEL_LABEL_ATTR, "")
                        .expect("set data-panel-label attribute");
                    section
                        .insert_before(&header, section.first_child().as_ref())
                        .expect("insert panel-label header as first child");
                    header
                }
            };
            header.set_text_content(Some(text));
        }
        None => {
            if let Some(header) = existing {
                header.remove();
            }
        }
    }
}

/// Render a new toast into the toast container and record its element under the
/// core's page-lifetime id. A click on the toast dismisses it (folding through
/// the core so the id is dropped everywhere). Toast text is `textContent` only.
fn show_toast(
    state: &mut ChromeState,
    id: u64,
    severity: ToastSeverity,
    text: &str,
    source: ToastSource,
) {
    let container = find_or_create_child(&surface_root(), TOAST_CONTAINER_ID, "div");
    let toast = create_div(&doc(), "data-surface-toast");
    toast
        .set_attribute("data-toast-severity", toast_severity_str(severity))
        .expect("set data-toast-severity");
    toast
        .set_attribute("data-toast-source", toast_source_str(source))
        .expect("set data-toast-source");
    toast.set_text_content(Some(text));
    add_dismiss_listener(&toast, id);
    append(&container, &toast);
    state.toasts.insert(id, toast);
}

/// Remove a rendered toast by its core-assigned id. A no-op for an id with no
/// live element (already dismissed).
fn dismiss_toast(state: &mut ChromeState, id: u64) {
    if let Some(toast) = state.toasts.remove(&id) {
        toast.remove();
    }
}

/// Wire a click on `toast` to dismiss the toast with `id`: fold the dismissal
/// through the core (so a later `DismissToast` action removes the element) and
/// apply the result. The listener lives as long as the toast element, so its
/// closure is `forget`-leaked.
fn add_dismiss_listener(toast: &HtmlElement, id: u64) {
    let closure = wasm_bindgen::closure::Closure::<dyn Fn(web_sys::Event)>::new(move |_event| {
        STATE.with(|s| {
            let mut guard = s.borrow_mut();
            if let Some(state) = guard.as_mut() {
                let actions = state.core.dismiss_toast(id);
                apply_actions(state, &actions);
            }
        });
    });
    toast
        .add_event_listener_with_callback("click", closure.as_ref().unchecked_ref())
        .expect("add toast dismiss listener");
    closure.forget();
}

/// The `data-toast-severity` value for a severity.
fn toast_severity_str(severity: ToastSeverity) -> &'static str {
    match severity {
        ToastSeverity::Info => "info",
        ToastSeverity::Warning => "warning",
        ToastSeverity::Error => "error",
    }
}

/// The `data-toast-source` value for a source.
fn toast_source_str(source: ToastSource) -> &'static str {
    match source {
        ToastSource::Kernel => "kernel",
    }
}

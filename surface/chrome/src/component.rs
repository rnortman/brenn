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

use brenn_surface_component_support::{
    Activation, Publisher, append, boot, claim_initialized, component_log, create_div, document,
    publish_or_fault, read_monotonic_ms, register_component, repark_tick, wire_gesture,
};
use brenn_surface_contract::SURFACE_ROOT_ID;
use brenn_surface_schema::layout::LayoutKind;
use brenn_surface_schema::{ToastSeverity, ToastSource};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::{Document, Element, Event, HtmlElement};

use crate::logic::{
    BannerState, ChromeAction, ChromeCore, LayoutPlacement, PORT_OVERLAY_STATE, Theme, fold_window,
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
/// The expiry-wake port — must match a `[[surface.io_port]] port` declaration.
const TOAST_TICK_PORT: &str = "toast-tick";

/// The sync port a click on a rendered toast arrives on, carrying the toast's id
/// as its request body.
const TOAST_DISMISS_PORT: &str = "toast-dismiss";

/// The attribute each rendered toast carries its core-assigned id on. The
/// container's one delegated click listener reads the id back off it, so no toast
/// needs a listener of its own.
const TOAST_ID_ATTR: &str = "data-toast-id";

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
}

/// The loader's entry, called once after this module's `default` init with the
/// instance this module record was loaded for. Boots the panic hook and builds
/// the core keyed on this instance (so chrome excludes itself from arrangement),
/// then registers the element and its activation entry.
#[wasm_bindgen]
pub fn brenn_bind_instance(instance: String) {
    boot(&instance);
    STATE.with(|s| {
        *s.borrow_mut() = Some(ChromeState {
            core: ChromeCore::new(instance.clone()),
            host: None,
            toasts: HashMap::new(),
        });
    });
    register_component(
        KIND,
        on_connected,
        move |activation: &Activation, publisher: &mut Publisher| {
            on_activation(activation, publisher);
            Ok(None)
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

/// Act on a toast dismissal if this activation is one, fold each window's new
/// messages into the core, sweep expired toasts, and park the next expiry wake.
///
/// The expiry sweep runs on every activation, not only on the wake's own: it is
/// an idempotent recompute from the clock, and running it wherever chrome is
/// already awake means a toast that outlived its wake by a delivery still goes.
fn on_activation(activation: &Activation, publisher: &mut Publisher) {
    STATE.with(|s| {
        let mut guard = s.borrow_mut();
        let state = guard
            .as_mut()
            .expect("brenn_bind_instance runs before the first activation");
        // The entry is registered only after `connectedCallback` records the host,
        // so an activation without one is a kernel that called an unregistered
        // component.
        let host = state
            .host
            .clone()
            .expect("connectedCallback records the host before the entry is registered");
        let now_mono = now_ms();

        if let Some((_, click)) = activation.sync_request() {
            on_toast_click(state, &click.body, publisher);
        }
        for window in activation.delivered_windows() {
            // The wake's payload is irrelevant — the wake is the message.
            if window.port == TOAST_TICK_PORT {
                continue;
            }
            let actions = fold_window(&mut state.core, window, now_mono);
            apply_actions(state, &actions, publisher);
        }
        let expired = state.core.tick(now_mono);
        apply_actions(state, &expired, publisher);

        let now_wall = activation
            .now
            .expect("the surface kernel stamps every activation with its wall clock");
        let release_at = state.core.next_wake(now_mono, now_wall);
        repark_tick(activation, publisher, &host, TOAST_TICK_PORT, release_at);
    });
}

/// Dismiss the toast one click asked for, naming it by the id its wiring encoded.
///
/// An empty body is a click that landed on the toast container but on no toast —
/// the delegated listener covers the whole container — and dismisses nothing.
fn on_toast_click(state: &mut ChromeState, click: &str, publisher: &mut Publisher) {
    if click.is_empty() {
        return;
    }
    let id: u64 = click.parse().unwrap_or_else(|_| {
        panic!("chrome's own toast wiring encoded {click:?}, which is not a toast id")
    });
    let actions = state.core.dismiss_toast(id);
    apply_actions(state, &actions, publisher);
}

/// Apply the core's actions in order. `state` is borrowed so a `ShowToast`/
/// `DismissToast` can record or drop the toast element.
fn apply_actions(state: &mut ChromeState, actions: &[ChromeAction], publisher: &mut Publisher) {
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
                let host = state
                    .host
                    .clone()
                    .expect("connectedCallback records the host before the first activation");
                // Retained state: a refusal nobody acted on would leave every
                // consumer of `overlay-state` reading a page that has moved on.
                publish_or_fault(publisher, &host, PORT_OVERLAY_STATE, body);
            }
        }
    }
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
/// core's page-lifetime id, stamped on the element so the container's delegated
/// listener can read it back. A click on the toast dismisses it (folding through
/// the core so the id is dropped everywhere). Toast text is `textContent` only.
fn show_toast(
    state: &mut ChromeState,
    id: u64,
    severity: ToastSeverity,
    text: &str,
    source: ToastSource,
) {
    let host = state
        .host
        .clone()
        .expect("connectedCallback records the host before the first toast is shown");
    let container = toast_container(&host);
    let toast = create_div(&doc(), "data-surface-toast");
    toast
        .set_attribute(TOAST_ID_ATTR, &id.to_string())
        .expect("set data-toast-id");
    toast
        .set_attribute("data-toast-severity", toast_severity_str(severity))
        .expect("set data-toast-severity");
    toast
        .set_attribute("data-toast-source", toast_source_str(source))
        .expect("set data-toast-source");
    toast.set_text_content(Some(text));
    append(&container, &toast);
    state.toasts.insert(id, toast);
}

/// The toast container under `#surface-root`, built on first use and wired then
/// with the one delegated dismiss listener every toast is read through.
///
/// One listener for the container's life, not one per toast: an SDK listener
/// closure is page-lifetime and never reclaimed, so wiring each toast element
/// would leak one closure per toast a long-lived page ever shows. The container
/// is created and never removed, which is the lifetime the wiring wants.
fn toast_container(host: &HtmlElement) -> HtmlElement {
    let already_built = doc().get_element_by_id(TOAST_CONTAINER_ID).is_some();
    let container = find_or_create_child(&surface_root(), TOAST_CONTAINER_ID, "div");
    if !already_built {
        wire_gesture(
            host,
            container.as_ref(),
            "click",
            TOAST_DISMISS_PORT,
            clicked_toast_id,
        );
    }
    container
}

/// The id of the toast a click landed inside, or an empty body when it landed on
/// the container itself rather than on any toast.
fn clicked_toast_id(event: &Event) -> String {
    let Some(target) = event
        .target()
        .and_then(|target| target.dyn_into::<Element>().ok())
    else {
        return String::new();
    };
    target
        .closest(&format!("[{TOAST_ID_ATTR}]"))
        .expect("closest runs with a well-formed attribute selector")
        .and_then(|toast| toast.get_attribute(TOAST_ID_ATTR))
        .unwrap_or_default()
}

/// Remove a rendered toast by its core-assigned id. A no-op for an id with no
/// live element (already dismissed).
fn dismiss_toast(state: &mut ChromeState, id: u64) {
    if let Some(toast) = state.toasts.remove(&id) {
        toast.remove();
    }
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

/// Browser tests for the DOM glue, run under wasm-bindgen-test via
/// `make surface-wasm-test`. wasm32-only, matching the glue itself.
///
/// The decision core is host-tested next door in `logic.rs`; what only a browser
/// can answer is the wiring between them — that a delivered toast reaches the
/// DOM with the attribute its own dismiss path reads back, that the container's
/// single delegated listener resolves a click to that toast, and that the wake
/// chain is actually re-parked from the activation rather than merely computable.
#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;

    use crate::logic::{PORT_TOAST, TOAST_TTL_MS};
    use brenn_surface_schema::{CONTROL_PLANE_VERSION, ToastBody};
    use brenn_surface_test_fixtures::browser::{activation_json, mount, record_ops, take_recorded};
    use wasm_bindgen::JsValue;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    /// The instance this test binary binds its one module record to.
    const TEST_INSTANCE: &str = "wbt-chrome";

    /// The activation wall clock. The toast lifetime is judged monotonically, so
    /// this only has to be a plausible instant far from the monotonic origin —
    /// which is exactly what makes the parked wake's arithmetic worth asserting.
    const NOW_MS: u64 = 1_770_000_123_456;

    /// One toast payload as the bus carries it.
    fn toast_body(text: &str) -> String {
        serde_json::to_string(&ToastBody {
            v: CONTROL_PLANE_VERSION,
            severity: ToastSeverity::Info,
            text: text.to_string(),
            source: ToastSource::Kernel,
        })
        .expect("the toast body serializes")
    }

    /// Build the `#surface-root` the backend page renders, which chrome's DOM
    /// half drives and the harness's bare `<body>` does not have.
    fn build_surface_root() {
        let root = create_div(&doc(), "data-surface-root");
        root.set_id(SURFACE_ROOT_ID);
        doc()
            .body()
            .expect("the document has a body")
            .append_child(&root)
            .expect("append #surface-root");
    }

    /// The live toast elements, in document order.
    fn rendered_toast_ids() -> Vec<String> {
        let nodes = doc()
            .query_selector_all(&format!("[{TOAST_ID_ATTR}]"))
            .expect("query the rendered toasts");
        (0..nodes.length())
            .filter_map(|i| nodes.get(i))
            .filter_map(|node| node.dyn_into::<Element>().ok())
            .filter_map(|el| el.get_attribute(TOAST_ID_ATTR))
            .collect()
    }

    /// Click a rendered element from the page's own event loop.
    fn click(selector: &str) {
        doc()
            .query_selector(selector)
            .expect("the selector is well-formed")
            .expect("the element under the selector is in the document")
            .dyn_into::<HtmlElement>()
            .expect("a clickable HtmlElement")
            .click();
    }

    /// Call the entry with one activation's JSON.
    fn activate(entry: &js_sys::Function, json: &str) {
        entry
            .call1(&JsValue::NULL, &JsValue::from_str(json))
            .expect("the entry returns ok");
    }

    /// The whole toast lifecycle through chrome's own seams: a delivered toast
    /// renders and parks its expiry wake, a click on it resolves to a sync
    /// request naming its id, a click that misses every toast names none, and the
    /// dismissal activation removes exactly the toast the id names.
    ///
    /// One test rather than five: the bind is once-per-binary, so a test binary
    /// gets exactly one mount.
    #[wasm_bindgen_test]
    fn a_toast_renders_parks_its_expiry_and_is_dismissed_by_its_own_click() {
        build_surface_root();
        // Installed before the mount: a connect-time publish, park or sync
        // request lands here, where silence is the assertion.
        let ops = record_ops();
        let (entry, _host) = mount(KIND, TEST_INSTANCE, brenn_bind_instance);
        assert_eq!(
            ops.length(),
            0,
            "connect-time code records the host only, and reaches no kernel seam"
        );

        // The mount activation: nothing live, so nothing to sweep and no wake to
        // aim. A page with no expiring toast never wakes.
        activate(&entry, &activation_json(&[], None, NOW_MS));
        assert_eq!(
            take_recorded(&ops),
            Vec::<Vec<String>>::new(),
            "an empty mount activation neither publishes nor parks"
        );

        activate(
            &entry,
            &activation_json(&[(PORT_TOAST, &toast_body("under test"))], None, NOW_MS),
        );
        let ids = rendered_toast_ids();
        let [toast_id] = &ids[..] else {
            panic!("exactly one toast is rendered, stamped with its id: {ids:?}")
        };
        assert_eq!(
            take_recorded(&ops),
            vec![vec![
                "defer".to_string(),
                "publish".to_string(),
                TOAST_TICK_PORT.to_string(),
                "{}".to_string(),
                (NOW_MS + TOAST_TTL_MS).to_string(),
            ]],
            "the expiry wake is parked on the in/out port, aimed a full TTL past \
             this activation's wall reading"
        );

        // A click on the toast asks for a sync naming its id.
        click(&format!("[{TOAST_ID_ATTR}='{toast_id}']"));
        assert_eq!(
            take_recorded(&ops),
            vec![vec![
                "sync".to_string(),
                String::new(),
                TOAST_DISMISS_PORT.to_string(),
                toast_id.clone(),
                String::new(),
            ]],
            "the click asks for the activation and encodes the toast it landed on"
        );

        // A click on the container but on no toast encodes nothing.
        click(&format!("#{TOAST_CONTAINER_ID}"));
        assert_eq!(
            take_recorded(&ops),
            vec![vec![
                "sync".to_string(),
                String::new(),
                TOAST_DISMISS_PORT.to_string(),
                String::new(),
                String::new(),
            ]],
            "the delegated listener covers the whole container, and a miss is empty"
        );

        // The empty request dismisses nothing, and the toast still standing keeps
        // its wake aimed — every activation re-aims it at the remaining lifetime.
        activate(
            &entry,
            &activation_json(
                &[(TOAST_DISMISS_PORT, "")],
                Some(TOAST_DISMISS_PORT),
                NOW_MS,
            ),
        );
        assert_eq!(
            rendered_toast_ids(),
            vec![toast_id.clone()],
            "a click that named no toast removes none"
        );
        let missed = take_recorded(&ops);
        let [wake] = &missed[..] else {
            panic!("a live toast keeps exactly one wake aimed at it: {missed:?}")
        };
        assert_eq!(
            (wake[0].as_str(), wake[2].as_str()),
            ("defer", TOAST_TICK_PORT)
        );
        let release: u64 = wake[4].parse().expect("a decimal release instant");
        assert!(
            release > NOW_MS && release <= NOW_MS + TOAST_TTL_MS,
            "the re-aimed wake carries what is left of the lifetime, not a fresh \
             one: {release} against {NOW_MS}"
        );

        // The named request dismisses exactly that toast, and with nothing live
        // left there is no wake to re-park.
        activate(
            &entry,
            &activation_json(
                &[(TOAST_DISMISS_PORT, toast_id)],
                Some(TOAST_DISMISS_PORT),
                NOW_MS,
            ),
        );
        assert_eq!(
            rendered_toast_ids(),
            Vec::<String>::new(),
            "the dismissal removes the toast the id names"
        );
        assert_eq!(
            take_recorded(&ops),
            Vec::<Vec<String>>::new(),
            "with nothing live expiring, the wake chain stops"
        );
    }
}

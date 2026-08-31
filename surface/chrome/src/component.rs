//! The chrome component's page glue.
//!
//! Applies the [`ChromeAction`]s the DOM-free [`crate::logic::ChromeCore`]
//! emits. Chrome is an ordinary page-hosted component: the kernel activates it
//! like any other, once per activation with every bound input port windowed.
//!
//! It is the one instance holding `page-dom`, and that is the whole difference:
//! it builds the connection banner and the toast container under the surface
//! root, arranges every *other* instance's kernel-owned wrapper into a layout
//! section, and stamps `data-theme` on `<body>` and the layout/takeover
//! attributes on the surface root. Nothing it renders lives inside its own host
//! element, which stays empty — that is what lets a takeover overlay fill the
//! surface without covering the banner.
//!
//! Every text run reaches the page through [`dom::set_text`], which writes inert
//! text and never parses markup, so a layout label, a banner line and a toast
//! are inert regardless of content.

use std::cell::RefCell;
use std::collections::HashMap;

use brenn_envelope::MessageEnvelope;
use brenn_guest::{Activation, Error, Processor, dom, log, page_dom, publish, repark};

use crate::layout::LayoutKind;
use crate::logic::{
    ActivationWindow, BannerState, ChromeAction, ChromeCore, LayoutPlacement, LogLevel, fold_window,
};
use crate::spec::{
    InPort,
    port::{OVERLAY_STATE, TOAST_TICK},
};
use crate::wire::{SurfaceStateBody, ToastSeverity, ToastSource};

/// The body of an expiry wake. The tick's payload is irrelevant — the wake is
/// the message — but every body on this bus is JSON.
const TICK_BODY: &str = "{}";

/// The sync port a click anywhere in the toast container arrives on.
const TOAST_DISMISS_PORT: dom::SyncPort = dom::SyncPort("toast-dismiss");

/// The surface-root attribute naming the active layout, targeted by skin CSS
/// grid templates.
const LAYOUT_ATTR: &str = "data-layout";
/// The per-section attribute naming the layout slot a section fills
/// (`a`/`b`/`c`); absent on an unassigned section, which the base CSS hides.
const PANEL_ATTR: &str = "data-panel";
/// The per-section attribute naming the instance the section holds.
const SECTION_INSTANCE_ATTR: &str = "data-instance";
/// The surface-root CSS custom property carrying the layout split fraction.
const RATIO_PROP: &str = "--surface-ratio";
/// The marker attribute on a section's chrome-rendered panel-label `<header>`.
const PANEL_LABEL_ATTR: &str = "data-panel-label";
/// The `<body>` attribute carrying the runtime theme axis.
const THEME_ATTR: &str = "data-theme";
/// The surface-root attribute set while a takeover overlay is active.
const TAKEOVER_ATTR: &str = "data-takeover";
/// The marker attribute identifying chrome's single connection banner. Every
/// banner rule in `surface.css` and both skins selects on it, so dropping the
/// stamp unstyles the banner with nothing raising an error.
const BANNER_MARKER: &str = "data-surface-banner";
/// The banner's styling hook carrying its rendered state.
const BANNER_STATE_ATTR: &str = "data-banner-state";
/// The marker attribute on chrome's toast container.
const TOAST_CONTAINER_MARKER: &str = "data-surface-toasts";
/// The marker attribute on each rendered toast.
const TOAST_MARKER: &str = "data-surface-toast";
/// The attribute a rendered toast carries its core-assigned id on.
const TOAST_ID_ATTR: &str = "data-toast-id";
/// The attribute a rendered toast carries its severity on.
const TOAST_SEVERITY_ATTR: &str = "data-toast-severity";
/// The attribute a rendered toast carries its raiser on.
const TOAST_SOURCE_ATTR: &str = "data-toast-source";
/// The attribute hiding the banner between states.
const HIDDEN_ATTRIBUTE: &str = "hidden";

// One instantiation backs one instance for the page's lifetime, so the decision
// core and every element handle chrome holds are ordinary interior-mutable
// module state.
thread_local! {
    static CHROME: RefCell<Chrome> = RefCell::new(Chrome::new());
}

/// Chrome's page-lifetime state.
struct Chrome {
    core: ChromeCore,
    /// The page elements chrome owns, built by the mount activation.
    view: Option<View>,
    /// Whether the core has been told which instance chrome runs as.
    identified: bool,
    /// The live toast elements, keyed by the core's page-lifetime toast id.
    toasts: HashMap<u64, dom::Node>,
    /// One layout section per arrangeable instance, built on first arrange.
    sections: HashMap<String, dom::Node>,
    /// A section's panel-label header, where one is rendered.
    labels: HashMap<String, dom::Node>,
}

/// The page furniture chrome builds once and writes into thereafter.
struct View {
    /// The surface root, which holds every instance wrapper.
    page_root: dom::Node,
    /// The document body, where the theme axis is stamped.
    body: dom::Node,
    /// Chrome's own kernel wrapper — the handle that says which surface-state
    /// row is chrome's own.
    own_wrapper: Option<dom::Node>,
    banner: dom::Node,
    toast_container: dom::Node,
}

impl Chrome {
    fn new() -> Chrome {
        Chrome {
            // The instance name is not known until the first surface-state
            // roster arrives; nothing is arrangeable before then.
            core: ChromeCore::new(String::new()),
            view: None,
            identified: false,
            toasts: HashMap::new(),
            sections: HashMap::new(),
            labels: HashMap::new(),
        }
    }

    /// The view, which every activation after the mount one has.
    fn view(&self) -> &View {
        self.view
            .as_ref()
            .expect("the mount activation builds the view before any other call")
    }
}

struct ChromeComponent;

impl Processor for ChromeComponent {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        CHROME.with(|chrome| on_activation(&activation, &mut chrome.borrow_mut()))?;
        // Mount is answered with nothing, and a click on a toast cancels no
        // default action worth cancelling.
        Ok(None)
    }
}

brenn_guest::export_processor!(ChromeComponent);

/// Build the page furniture when this is the mount call, dismiss a toast when a
/// click asked for one, fold each delivered window into the core, sweep expired
/// toasts, and park the next expiry wake.
///
/// The expiry sweep runs on every activation, not only on the wake's own: it is
/// an idempotent recompute from the clock, so a toast that outlived its wake by
/// a delivery still goes.
fn on_activation(activation: &Activation, chrome: &mut Chrome) -> Result<(), Error> {
    let now_ms = activation
        .now()
        .ok_or_else(|| Error::failed("chrome: the host stamped no wall clock"))?;

    if activation.sync_is(dom::MOUNT) {
        chrome.view = Some(build_view());
    } else if let Some(port) = activation.sync() {
        on_gesture(port, activation, chrome)?;
    }
    for window in activation.delivered_windows() {
        match InPort::of(window)? {
            // The wake's payload is irrelevant — the wake is the message.
            InPort::ToastTick => continue,
            InPort::SurfaceState => identify_self(chrome, window.new_raw()),
            _ => {}
        }
        let actions = fold_window(
            &mut chrome.core,
            &ActivationWindow {
                port: window.port(),
                context_raw: window.context_raw(),
                new_raw: window.new_raw(),
            },
            now_ms,
        );
        apply_actions(chrome, &actions);
    }
    let expired = chrome.core.tick(now_ms);
    apply_actions(chrome, &expired);

    repark(
        activation,
        TOAST_TICK,
        TICK_BODY,
        chrome.core.next_wake(now_ms),
    );
    Ok(())
}

/// Dismiss the toast the click landed on.
fn on_gesture(port: &str, activation: &Activation, chrome: &mut Chrome) -> Result<(), Error> {
    if port != TOAST_DISMISS_PORT {
        return Err(Error::failed(format!(
            "chrome wired no gesture to sync port {port:?}"
        )));
    }
    let (_, request) = activation
        .sync_request()
        .ok_or_else(|| Error::failed("chrome: a sync-call activation carried no request"))?;
    let gesture: dom::Gesture = serde_json::from_str(&request?.body)
        .map_err(|e| Error::failed(format!("chrome: the gesture body did not parse: {e}")))?;
    // The container's one delegated listener covers every toast and the gaps
    // between them, so a click that landed on no toast dismisses nothing.
    let Some(id) = chrome
        .toasts
        .iter()
        .find(|(_, node)| **node == gesture.target)
        .map(|(id, _)| *id)
    else {
        return Ok(());
    };
    let actions = chrome.core.dismiss_toast(id);
    apply_actions(chrome, &actions);
    Ok(())
}

/// Tell the core which instance chrome runs as, the first time a surface-state
/// roster names one whose wrapper is chrome's own.
///
/// There is no instance-identity import, deliberately: what chrome needs is not
/// its name but which row to leave out of the arrangement, and element identity
/// answers that directly. Handles are canonical per element within an instance,
/// so the wrapper chrome's own host element hangs under is the same handle
/// `page-dom` returns for chrome's row and for no other.
fn identify_self(chrome: &mut Chrome, new_raw: &[String]) {
    if chrome.identified {
        return;
    }
    let Some(own_wrapper) = chrome.view().own_wrapper else {
        return;
    };
    // Latest-wins, as the fold reads it: the newest roster is the whole current
    // set, so an older one can only name a subset of it.
    let Some(raw) = new_raw.last() else {
        return;
    };
    let Ok(envelope) = serde_json::from_str::<MessageEnvelope>(raw) else {
        return;
    };
    let Ok(body) = serde_json::from_str::<SurfaceStateBody>(&envelope.body) else {
        return;
    };
    for row in &body.instances {
        if page_dom::instance_wrapper(&row.instance) == Some(own_wrapper) {
            chrome.core.set_self_instance(row.instance.clone());
            chrome.identified = true;
            return;
        }
    }
}

fn apply_actions(chrome: &mut Chrome, actions: &[ChromeAction]) {
    for action in actions {
        match action {
            ChromeAction::SetTheme(theme) => {
                dom::set_attribute(chrome.view().body, THEME_ATTR, theme.as_wire_str());
            }
            ChromeAction::SetBanner(banner) => render_banner(chrome.view(), *banner),
            ChromeAction::SetTakeover(on) => {
                dom::set_attribute_present(chrome.view().page_root, TAKEOVER_ATTR, *on);
            }
            ChromeAction::ApplyLayout {
                kind,
                ratio,
                panels,
                instances,
            } => apply_layout(chrome, *kind, ratio.as_deref(), panels, instances),
            ChromeAction::ShowToast {
                id,
                severity,
                text,
                source,
            } => show_toast(chrome, *id, *severity, text, *source),
            ChromeAction::DismissToast { id } => {
                if let Some(node) = chrome.toasts.remove(id) {
                    dom::remove(node);
                }
            }
            ChromeAction::Log { level, message } => match level {
                LogLevel::Debug => log::debug(message),
                LogLevel::Warn => log::warn(message),
                LogLevel::Error => log::error(message),
            },
            ChromeAction::PublishOverlayState { body } => publish_overlay_state(body),
        }
    }
}

/// Publish chrome's overlay holdership.
///
/// The refusal taxonomy, not `?`: failing the activation would discard the whole
/// buffer, including the wake repark, and stop the page expiring toasts. A
/// structural refusal is a deployment fault and traps; a quota refusal is
/// transient, and the next transition publishes again.
fn publish_overlay_state(body: &str) {
    if let Err(err) = publish(OVERLAY_STATE, body) {
        assert!(
            err.is_quota(),
            "chrome: the overlay-state publish on {OVERLAY_STATE:?} was refused: {err:?}"
        );
        log::error(format!(
            "overlay-state publish on {OVERLAY_STATE:?} refused: quota exceeded"
        ));
    }
}

/// Build the page furniture chrome owns: the connection banner and the toast
/// container, both under the surface root rather than under chrome's own host
/// element.
///
/// Called once, from the mount activation. The container's single delegated
/// click listener is the kernel's and is page-lifetime — one listener for the
/// page rather than one per toast, which is what keeps a long-lived kiosk from
/// accumulating one listener per notice it ever showed.
fn build_view() -> View {
    let page_root = page_dom::root();
    let banner = dom::marked("div", BANNER_MARKER);
    dom::set_attribute(banner, HIDDEN_ATTRIBUTE, "");
    let toast_container = dom::marked("div", TOAST_CONTAINER_MARKER);
    dom::append(page_root, banner);
    dom::append(page_root, toast_container);
    dom::listen(toast_container, "click", TOAST_DISMISS_PORT);
    View {
        page_root,
        body: page_dom::body(),
        own_wrapper: page_dom::parent(dom::root()),
        banner,
        toast_container,
    }
}

/// Render the connection banner to reflect `state`. Server-supplied text never
/// reaches the page as markup; `Hidden` hides the node without removing it, so a
/// later change re-shows the same element.
fn render_banner(view: &View, state: BannerState) {
    match state {
        BannerState::Hidden => {
            dom::set_attribute(view.banner, HIDDEN_ATTRIBUTE, "");
            dom::set_text(view.banner, "");
        }
        _ => {
            dom::remove_attribute(view.banner, HIDDEN_ATTRIBUTE);
            dom::set_text(view.banner, banner_text(state));
        }
    }
    dom::set_attribute(view.banner, BANNER_STATE_ATTR, banner_state_name(state));
}

/// The user-facing banner text for a state, as inert text.
fn banner_text(state: BannerState) -> &'static str {
    match state {
        BannerState::Connecting => "Connecting…",
        BannerState::Reconnecting => "Reconnecting…",
        BannerState::Reloading => "Update available — reloading",
        BannerState::Fatal => "Connection failed — reload to retry",
        BannerState::Hidden => unreachable!("Hidden banner renders no text"),
    }
}

/// The stable state name written to the banner's styling hook.
fn banner_state_name(state: BannerState) -> &'static str {
    match state {
        BannerState::Connecting => "connecting",
        BannerState::Reconnecting => "reconnecting",
        BannerState::Reloading => "reloading",
        BannerState::Fatal => "fatal",
        BannerState::Hidden => "hidden",
    }
}

/// Apply a layout atomically: set the root's layout attribute (and the ratio
/// custom property when present, else clear it), then place each of the
/// surface's `instances` — one named in `panels` gets its slot and label header;
/// every other has both cleared.
///
/// The one place that exercises chrome's page-DOM authority: it reparents each
/// instance's kernel-owned wrapper into that instance's layout section and
/// stamps layout attributes on the section — never inside the wrapper.
/// Reparenting preserves element identity, so the kernel's registry and its
/// per-element dispatch are untouched.
fn apply_layout(
    chrome: &mut Chrome,
    kind: LayoutKind,
    ratio: Option<&str>,
    panels: &[LayoutPlacement],
    instances: &[String],
) {
    let page_root = chrome.view().page_root;
    dom::set_attribute(page_root, LAYOUT_ATTR, kind.as_wire_str());
    match ratio {
        Some(value) => dom::set_style_property(page_root, RATIO_PROP, value),
        None => dom::remove_style_property(page_root, RATIO_PROP),
    }

    for instance in instances {
        let section = section_for(chrome, instance);
        match panels.iter().find(|p| &p.instance == instance) {
            Some(placement) => {
                dom::set_attribute(section, PANEL_ATTR, &placement.slot);
                set_panel_label(chrome, instance, section, placement.label.as_deref());
            }
            None => {
                dom::remove_attribute(section, PANEL_ATTR);
                set_panel_label(chrome, instance, section, None);
            }
        }
        adopt_wrapper(section, instance);
    }
}

/// The instance's layout section, built under the surface root on first arrange.
/// Chrome's element: it carries the layout state and holds the instance's
/// kernel-owned wrapper.
fn section_for(chrome: &mut Chrome, instance: &str) -> dom::Node {
    if let Some(section) = chrome.sections.get(instance) {
        return *section;
    }
    let section = dom::create_element("section");
    dom::set_attribute(section, SECTION_INSTANCE_ATTR, instance);
    dom::append(chrome.view().page_root, section);
    chrome.sections.insert(instance.to_string(), section);
    section
}

/// Reparent `instance`'s kernel wrapper into its layout section, unless it is
/// already there. The already-there check keeps a slot or label change from
/// moving any node.
///
/// A wrapper that is not there yet is the ordinary transient of a page still
/// coming up: registration emits a surface-state message, so the next delivery
/// re-runs the arrangement and finds it.
fn adopt_wrapper(section: dom::Node, instance: &str) {
    let Some(wrapper) = page_dom::instance_wrapper(instance) else {
        return;
    };
    if page_dom::parent(wrapper) != Some(section) {
        dom::append(section, wrapper);
    }
}

/// Render (or clear) a section's panel-label header. Label text is inert text
/// only — operator/LLM-supplied text never renders as markup.
///
/// A new header is inserted before the instance's wrapper where the wrapper is
/// already adopted, so the label always sits above the panel it names. The
/// capability has no child traversal by design; the wrapper is the only other
/// child the section can have, and `page-dom` names it directly.
fn set_panel_label(chrome: &mut Chrome, instance: &str, section: dom::Node, label: Option<&str>) {
    match label {
        Some(text) => {
            let header = match chrome.labels.get(instance) {
                Some(header) => *header,
                None => {
                    let header = dom::marked("header", PANEL_LABEL_ATTR);
                    dom::insert_before(section, header, page_dom::instance_wrapper(instance));
                    chrome.labels.insert(instance.to_string(), header);
                    header
                }
            };
            dom::set_text(header, text);
        }
        None => {
            if let Some(header) = chrome.labels.remove(instance) {
                dom::remove(header);
            }
        }
    }
}

/// Render a new toast into the toast container and record its element under the
/// core's page-lifetime id. A click on it dismisses it, folded through the core
/// so the id is dropped everywhere. Toast text is inert text only.
fn show_toast(
    chrome: &mut Chrome,
    id: u64,
    severity: ToastSeverity,
    text: &str,
    source: ToastSource,
) {
    let toast = dom::marked("div", TOAST_MARKER);
    dom::set_attribute(toast, TOAST_ID_ATTR, &id.to_string());
    dom::set_attribute(toast, TOAST_SEVERITY_ATTR, severity.as_wire_str());
    dom::set_attribute(toast, TOAST_SOURCE_ATTR, source.as_wire_str());
    dom::set_text(toast, text);
    dom::append(chrome.view().toast_container, toast);
    chrome.toasts.insert(id, toast);
}

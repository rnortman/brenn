//! DOM-free chrome decision core.
//!
//! Pure state and transition logic over the chrome component's inputs: the
//! layout channel (an ordinary `brenn:` binding) and the reserved
//! `local:brenn/*` control planes (theme, takeover, link-state, surface-state,
//! toast). It holds no web-sys handles, compiles and unit-tests on the host
//! target, and emits [`ChromeAction`]s the wasm DOM half applies.
//!
//! Chrome is a component, not the kernel: it folds no client connection events
//! and originates no state it merely renders. It *consumes* the planes the
//! kernel publishes. Every input is an already-extracted message body (the
//! reserved plane's JSON payload, or the layout channel's layout doc); the DOM
//! half pulls it from the delivered envelope and hands it here.
//!
//! It publishes exactly one plane, `local:brenn/overlay-state`, and only
//! because overlay holdership is state chrome alone owns: chrome drops takeover
//! messages the router routes, so no other vantage point on the page can report
//! which overlay is up.

use brenn_envelope::MessageEnvelope;

use crate::layout::{LayoutDoc, LayoutKind, Panel};
use crate::wire::{
    CONTROL_PLANE_VERSION, InstanceState, LinkState, LinkStateBody, OverlayStateBody,
    SurfaceStateBody, THEME_DARK, THEME_LIGHT, TakeoverAction, TakeoverBody, ThemeBody, ToastBody,
    ToastSeverity, ToastSource,
};

/// The severity of a breadcrumb [`ChromeAction::Log`] carries.
///
/// Chrome's own three rungs, not the kernel's five: this is the argument to a
/// guest-SDK log call, not a wire body, so it is spelled for what chrome emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug,
    Warn,
    Error,
}

/// One activation window on a bound input port, in the terms this core reads it
/// by: the port it arrived on, the retained context it rode in with, and the new
/// envelope documents it carries — each oldest first, as delivered JSON.
///
/// A borrowed view rather than either host's own window type: this module
/// compiles for the host, where it is unit-tested, and for wasm32 against the
/// guest SDK, whose window is a different type in a different crate universe.
/// The glue borrows one into the other.
///
/// `context_raw` is carried and never folded. Holding it makes the skip a
/// decision this core takes and a host test can pin, rather than something the
/// caller happened not to hand over.
pub struct ActivationWindow<'a> {
    pub port: &'a str,
    pub context_raw: &'a [String],
    pub new_raw: &'a [String],
}

impl ActivationWindow<'_> {
    // TODO(surface-fault-report): the latest-wins report below is spelled here
    // because the shared home was a host-only crate the guest universe cannot
    // link; a guest-side home takes this copy and the four others with it.

    /// The operator-facing report for a latest-wins port handed more than one
    /// new message, or `None` when this window carries at most one.
    ///
    /// More than one new message on a latest-wins port means the binding's
    /// `push_depth` exceeds 1: coalescing to the latest is the subscription's
    /// job, and a binding that declines to do it makes every consumer redo it.
    /// Chrome applies the latest and keeps working, so this is a normal error to
    /// the operator — the only place the fault is detectable, since latest-wins
    /// is component semantics no config layer knows.
    fn latest_wins_misconfiguration(&self) -> Option<String> {
        if self.new_raw.len() <= 1 {
            return None;
        }
        Some(format!(
            "latest-wins port {:?} presented {} new messages; its binding's \
             push_depth should be 1 — coalescing to the latest is the \
             subscription's job, not the component's",
            self.port,
            self.new_raw.len()
        ))
    }
}

/// The runtime theme axis — a device-local cosmetic override orthogonal to the
/// config-time skin. Chrome writes it as `data-theme` on `<body>`; skins that
/// opt in compose per-theme token overrides against it.
///
/// The wire strings are the frozen `THEME_*` constants, shared with any
/// theme-driving component so a theme published on `local:brenn/theme` carries
/// the same vocabulary chrome parses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Theme {
    /// Primary for every skin; the page default and the value a surface with no
    /// theme-driving component holds forever.
    Dark,
    /// Opt-in per skin; a skin shipping no light block is unaffected.
    Light,
}

impl Theme {
    /// The frozen wire string written to the `data-theme` attribute.
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Theme::Dark => THEME_DARK,
            Theme::Light => THEME_LIGHT,
        }
    }

    /// Parse the untrusted theme wire string. `None` for any other value, so a
    /// malformed theme is dropped-and-reported, never coerced.
    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            THEME_DARK => Some(Theme::Dark),
            THEME_LIGHT => Some(Theme::Light),
            _ => None,
        }
    }
}

/// What chrome's connection banner currently shows.
///
/// Derived from the link-state plane, not from sockets: chrome renders the
/// banner but never reasons about the connection. The plane carries no `Fatal`
/// detail (the plane's payload is fixed at `{v, state}`), so — unlike the retired
/// shell's `BannerState` — `Fatal` carries none: chrome renders its own text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BannerState {
    /// The initial connection attempt is in flight.
    Connecting,
    /// A live connection dropped; the kernel is reconnecting via backoff.
    Reconnecting,
    /// A newer server build was detected; the page is reloading.
    Reloading,
    /// A terminal protocol failure. No auto-reload.
    Fatal,
    /// The connection is live; no banner is shown.
    Hidden,
}

/// The banner a link state paints. `Connected` is `Hidden`: chrome hides the
/// banner precisely when the link is up.
fn banner_of(state: LinkState) -> BannerState {
    match state {
        LinkState::Connecting => BannerState::Connecting,
        LinkState::Connected => BannerState::Hidden,
        LinkState::Reconnecting => BannerState::Reconnecting,
        LinkState::Reloading => BannerState::Reloading,
        LinkState::Fatal => BannerState::Fatal,
    }
}

/// One panel placement in an applied layout: the section (by component
/// `instance`) that fills a `slot`, with its optional operator `label`. The DOM
/// half stamps `data-panel = slot` on that instance's section and renders the
/// label as `textContent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayoutPlacement {
    /// The layout slot this instance fills (`"a"`/`"b"`/`"c"`).
    pub slot: String,
    /// The component instance whose section fills the slot.
    pub instance: String,
    /// The operator label rendered above the panel, if any.
    pub label: Option<String>,
}

/// An effect the wasm DOM half must apply, in order.
///
/// Not `Eq`-blocked: [`ChromeAction::ApplyLayout`] carries a preformatted ratio
/// string (not an `f64`) precisely so the whole enum stays `Eq`, which the
/// host tests lean on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChromeAction {
    /// Set the runtime theme axis: write `data-theme` on `<body>`.
    SetTheme(Theme),
    /// Set the connection banner to this state.
    SetBanner(BannerState),
    /// Set (or clear) the takeover chrome flag: write `data-takeover="true"` on
    /// `#surface-root` when `true`, remove the attribute when `false`.
    SetTakeover(bool),
    /// Apply a layout to the surface, atomically. The DOM half sets
    /// `data-layout = kind` on `#surface-root` and, when `ratio` is present, the
    /// `--surface-ratio` custom property; then reparents each instance's wrapper
    /// into its layout section and stamps `data-panel` + label, clearing both on
    /// every other section. `instances` is every arrangeable instance in
    /// configured order (chrome's own instance excluded), so the executor never
    /// asks the DOM what exists.
    ApplyLayout {
        kind: LayoutKind,
        ratio: Option<String>,
        panels: Vec<LayoutPlacement>,
        instances: Vec<String>,
    },
    /// Render a new toast. `id` is chrome's page-lifetime handle for a later
    /// [`ChromeAction::DismissToast`].
    ShowToast {
        id: u64,
        severity: ToastSeverity,
        text: String,
        source: ToastSource,
    },
    /// Remove a rendered toast by its chrome-assigned `id`.
    DismissToast { id: u64 },
    /// Log a breadcrumb (untrusted-input rejection). The DOM half forwards it via
    /// the component-log plane; chrome never panics on a bad plane payload.
    Log { level: LogLevel, message: String },
    /// Publish chrome's overlay holdership on [`OVERLAY_STATE`]. `body` is
    /// the serialized [`OverlayStateBody`] for the transition that just
    /// folded — emitted on every transition and only on a transition, so the
    /// plane carries no heartbeat.
    PublishOverlayState { body: String },
}

/// The CSS custom-property value for a layout `ratio` fraction (a plain decimal
/// consumed by skin CSS as `--surface-ratio`).
fn format_ratio(ratio: f64) -> String {
    format!("{ratio}")
}

/// Map a **validated** [`LayoutDoc`] to the [`ChromeAction::ApplyLayout`] it
/// applies. Slots are emitted in the kind's render order; every slot key is
/// present because the doc passed [`LayoutDoc::validate`] (or was synthesized
/// with exactly those slots).
fn layout_doc_to_action(doc: &LayoutDoc, instances: Vec<String>) -> ChromeAction {
    let panels = doc
        .kind
        .slots()
        .iter()
        .map(|slot| {
            let panel = &doc.panels[*slot];
            LayoutPlacement {
                slot: (*slot).to_string(),
                instance: panel.instance.clone(),
                label: panel.label.clone(),
            }
        })
        .collect();
    ChromeAction::ApplyLayout {
        kind: doc.kind,
        ratio: doc.ratio.map(format_ratio),
        panels,
        instances,
    }
}

/// The default layout kind for `n` arrangeable instances, or `None` when nothing
/// is arrangeable.
///
/// The one definition of the count→kind rule: chrome's synthesized default
/// layout and the generated help that documents it both read it here, so the
/// published sentence cannot describe a mapping the code does not implement.
pub(crate) fn default_kind_for_count(n: usize) -> Option<LayoutKind> {
    match n {
        0 => None,
        1 => Some(LayoutKind::Single),
        2 => Some(LayoutKind::Columns2),
        _ => Some(LayoutKind::Columns3),
    }
}

/// Synthesize the default layout from the arrangeable instances: the first three
/// in configured order, no labels, kind by [`default_kind_for_count`]. `None`
/// when nothing is arrangeable. A bare surface (no layout binding) shows this.
fn default_layout_doc(instances: &[String]) -> Option<LayoutDoc> {
    let kind = default_kind_for_count(instances.len())?;
    let panels = kind
        .slots()
        .iter()
        .zip(instances.iter())
        .map(|(slot, instance)| {
            (
                (*slot).to_string(),
                Panel {
                    instance: instance.clone(),
                    label: None,
                },
            )
        })
        .collect();
    Some(LayoutDoc {
        v: 1,
        kind,
        panels,
        ratio: None,
    })
}

/// The synthesized fullscreen overlay layout for a takeover: a `single` layout
/// placing `instance` in the sole slot with no label.
fn overlay_layout_doc(instance: &str) -> LayoutDoc {
    LayoutDoc {
        v: 1,
        kind: LayoutKind::Single,
        panels: std::iter::once((
            "a".to_string(),
            Panel {
                instance: instance.to_string(),
                label: None,
            },
        ))
        .collect(),
        ratio: None,
    }
}

/// One arrangeable instance, as learned from the surface-state plane.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ArrangeInstance {
    instance: String,
    state: InstanceState,
}

/// The most toasts chrome renders at once. A new toast at the cap evicts the
/// oldest (drop-oldest — the codebase's uniform overflow shape), so a recurring
/// producer on an unattended kiosk can never grow the live set without bound.
const MAX_TOASTS: usize = 5;

/// How long a non-`error` toast stays before it auto-dismisses, in wall-clock
/// milliseconds. `error` toasts are exempt — an operator-attention event must
/// not evaporate — and persist until manually dismissed.
pub(crate) const TOAST_TTL_MS: u64 = 8_000;

/// A rendered toast chrome is tracking for its lifetime: the core's page-lifetime
/// handle and, for a non-`error` toast, the wall-clock instant it auto-dismisses.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveToast {
    /// The core-minted handle the DOM half keys its element on.
    id: u64,
    /// Wall-clock ms after which a non-`error` toast auto-dismisses; `None` for
    /// `error` toasts, which persist until a manual dismissal.
    expires_at: Option<u64>,
}

/// The chrome component's DOM-free state and transition logic.
///
/// Not `Eq`: `base_layout` holds a [`LayoutDoc`] whose `ratio` is an `f64`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChromeCore {
    /// Chrome's own instance id, excluded from arrangement (chrome never places
    /// itself in a panel).
    self_instance: String,
    /// The arrangeable instances, in configured order, from the last
    /// surface-state delivery. Chrome's own instance is never included.
    instances: Vec<ArrangeInstance>,
    /// The current base layout — the last valid layout doc, or `None` before any
    /// doc (then the default is synthesized from `instances`). A doc published
    /// while an overlay is up updates this but is not applied until the overlay
    /// pops.
    base_layout: Option<LayoutDoc>,
    /// The instance whose fullscreen takeover overlay is pushed, or `None`.
    /// Depth ≤ 1: a second request from a different instance is denied.
    overlay: Option<String>,
    /// The current banner (before any link-state delivery, `Connecting` — the
    /// same starting point the kernel's pre-chrome indicator paints).
    banner: BannerState,
    /// The current theme (the page default until a theme is published).
    theme: Theme,
    /// Active toasts, in show order (oldest first — the eviction end).
    toasts: Vec<ActiveToast>,
    /// Next toast id to mint.
    next_toast_id: u64,
}

impl ChromeCore {
    /// A freshly constructed core for chrome instance `self_instance`.
    pub fn new(self_instance: impl Into<String>) -> Self {
        Self {
            self_instance: self_instance.into(),
            instances: Vec::new(),
            base_layout: None,
            overlay: None,
            banner: BannerState::Connecting,
            theme: Theme::Dark,
            toasts: Vec::new(),
            next_toast_id: 0,
        }
    }

    /// Name the instance this core runs as, which it excludes from every
    /// arrangement.
    ///
    /// The page glue learns the name by matching its own kernel wrapper against
    /// the surface-state roster, so it is known only once the first roster
    /// arrives — before which there is nothing to arrange anyway.
    pub fn set_self_instance(&mut self, instance: String) {
        self.self_instance = instance;
    }

    /// The current banner state.
    pub fn banner(&self) -> BannerState {
        self.banner
    }

    /// The current theme.
    pub fn theme(&self) -> Theme {
        self.theme
    }

    /// The arrangeable instances in configured order — chrome's own instance
    /// excluded. Carried on every [`ChromeAction::ApplyLayout`].
    fn arranged_instances(&self) -> Vec<String> {
        self.instances.iter().map(|i| i.instance.clone()).collect()
    }

    /// Whether `instance` is one chrome may place — a known arrangeable instance
    /// (never chrome itself). The membership check [`LayoutDoc::validate`] needs.
    fn is_placeable(&self, instance: &str) -> bool {
        self.instances.iter().any(|i| i.instance == instance)
    }

    /// The effective layout doc: the overlay's synthesized layout while a
    /// takeover is up, else the base layout, else the synthesized default.
    fn effective_layout(&self) -> Option<LayoutDoc> {
        if let Some(instance) = &self.overlay {
            return Some(overlay_layout_doc(instance));
        }
        if let Some(doc) = &self.base_layout {
            return Some(doc.clone());
        }
        default_layout_doc(&self.arranged_instances())
    }

    /// The [`ChromeAction::ApplyLayout`] for the current effective layout, or an
    /// empty vec when nothing is arrangeable.
    fn apply_effective_layout(&self) -> Vec<ChromeAction> {
        match self.effective_layout() {
            Some(doc) => vec![layout_doc_to_action(&doc, self.arranged_instances())],
            None => Vec::new(),
        }
    }

    /// Fold a `local:brenn/theme` payload. Sets the theme on a real change;
    /// re-publishing the current theme is a no-op. A malformed payload or an
    /// unrecognized theme string is dropped-and-reported, never coerced.
    pub fn on_theme(&mut self, body: &str) -> Vec<ChromeAction> {
        let parsed: ThemeBody = match serde_json::from_str(body) {
            Ok(b) => b,
            Err(err) => return vec![self.warn(format!("rejected theme payload: {err}"))],
        };
        let Some(theme) = Theme::from_wire_str(&parsed.theme) else {
            return vec![self.warn(format!(
                "rejected theme payload: unknown theme {:?}",
                parsed.theme
            ))];
        };
        if self.theme == theme {
            return Vec::new();
        }
        self.theme = theme;
        vec![ChromeAction::SetTheme(theme)]
    }

    /// Fold a `local:brenn/link-state` payload into the banner. Emits a
    /// [`ChromeAction::SetBanner`] only on a real change.
    pub fn on_link_state(&mut self, body: &str) -> Vec<ChromeAction> {
        let parsed: LinkStateBody = match serde_json::from_str(body) {
            Ok(b) => b,
            Err(err) => return vec![self.warn(format!("rejected link-state payload: {err}"))],
        };
        let banner = banner_of(parsed.state);
        if self.banner == banner {
            return Vec::new();
        }
        self.banner = banner;
        vec![ChromeAction::SetBanner(banner)]
    }

    /// Fold a `local:brenn/surface-state` payload: refresh the arrangeable
    /// instance set and re-arrange.
    ///
    /// Chrome learns what exists here, never from DOM queries. A takeover overlay
    /// held by an instance that is no longer mounted is popped — a dead instance
    /// can never publish a release, so without this the surface would be stuck
    /// fullscreen on its error card. The layout is re-applied whenever the
    /// arrangeable set changes (the DOM half skips no-op reparents).
    ///
    // TODO(chrome-stale-sections-on-shrink): a section (and `base_layout`) for an
    // instance that leaves the set is never cleaned up. Latent — set membership is
    // fixed within a page lifetime and a config change forces a reload — until
    // dynamic instance add/remove lands.
    pub fn on_surface_state(&mut self, body: &str, now_ms: u64) -> Vec<ChromeAction> {
        let parsed: SurfaceStateBody = match serde_json::from_str(body) {
            Ok(b) => b,
            Err(err) => return vec![self.warn(format!("rejected surface-state payload: {err}"))],
        };
        let next: Vec<ArrangeInstance> = parsed
            .instances
            .into_iter()
            .filter(|i| i.instance != self.self_instance)
            .map(|i| ArrangeInstance {
                instance: i.instance,
                state: i.state,
            })
            .collect();
        if next == self.instances {
            return Vec::new();
        }
        self.instances = next;
        let mut actions = Vec::new();
        // Pop an overlay whose holder is gone (absent) or failed — it cannot
        // release the overlay itself.
        if let Some(current) = self.overlay.clone() {
            let alive = self
                .instances
                .iter()
                .any(|i| i.instance == current && i.state != InstanceState::Failed);
            if !alive {
                self.overlay = None;
                actions.push(ChromeAction::SetTakeover(false));
                actions.push(self.overlay_state_action(now_ms));
            }
        }
        actions.extend(self.apply_effective_layout());
        actions
    }

    /// Fold a layout-channel document: strict-parse + validate against the
    /// arrangeable instances, then apply.
    ///
    /// A parse or validation failure yields one `warn` breadcrumb and **no**
    /// layout change, so the last-good layout stays on screen — the doc's writer
    /// is an LLM that will sometimes emit a bad doc, and a kiosk must never blank
    /// or partially apply. A valid doc always becomes the new base; it is applied
    /// immediately only when no takeover overlay is active (while one is up the
    /// base is stored but deferred, so republishing layout mid-takeover cannot
    /// cancel an active alert — the overlay pop re-applies it).
    pub fn on_layout(&mut self, body: &str) -> Vec<ChromeAction> {
        let doc: LayoutDoc = match serde_json::from_str(body) {
            Ok(doc) => doc,
            Err(err) => return vec![self.warn(format!("rejected layout doc: parse error: {err}"))],
        };
        if let Err(reason) = doc.validate(|instance| self.is_placeable(instance)) {
            return vec![self.warn(format!("rejected layout doc: {reason}"))];
        }
        self.base_layout = Some(doc.clone());
        if self.overlay.is_some() {
            return vec![ChromeAction::Log {
                level: LogLevel::Debug,
                message: "stored base layout deferred: takeover overlay active".to_string(),
            }];
        }
        vec![layout_doc_to_action(&doc, self.arranged_instances())]
    }

    /// Fold a `local:brenn/takeover` payload.
    ///
    /// The kernel's router gates the *binding* against the surface's takeover
    /// grant (capability-as-binding), so an ungranted surface never delivers here
    /// at all — chrome does no grant check. `request` from a fresh state pushes
    /// the overlay (stamp the takeover flag + apply the synthesized fullscreen
    /// `single` layout); a request from the incumbent is idempotent; a request
    /// from a *different* instance while one is overlaid is denied (only one
    /// takeover-capable component exists). `release` from the holder pops; a
    /// release from anyone else is a no-op breadcrumb. An instance the payload
    /// names that is not arrangeable is dropped-and-reported.
    pub fn on_takeover(&mut self, body: &str, now_ms: u64) -> Vec<ChromeAction> {
        let parsed: TakeoverBody = match serde_json::from_str(body) {
            Ok(b) => b,
            Err(err) => return vec![self.warn(format!("rejected takeover payload: {err}"))],
        };
        let instance = parsed.instance;
        if !self.is_placeable(&instance) {
            return vec![self.warn(format!(
                "dropped takeover {:?} from unknown instance {instance}",
                parsed.action
            ))];
        }
        match parsed.action {
            TakeoverAction::Request => match self.overlay.as_deref() {
                Some(current) if current == instance => Vec::new(),
                Some(current) => vec![self.warn(format!(
                    "denied takeover request from {instance}: {current} already holds the overlay"
                ))],
                None => {
                    self.overlay = Some(instance.clone());
                    let mut actions = vec![
                        ChromeAction::SetTakeover(true),
                        self.overlay_state_action(now_ms),
                    ];
                    actions.extend(self.apply_effective_layout());
                    actions
                }
            },
            TakeoverAction::Release => match self.overlay.as_deref() {
                Some(current) if current == instance => self.pop_overlay(now_ms),
                _ => vec![self.warn(format!(
                    "dropped takeover release from {instance}: it does not hold the overlay"
                ))],
            },
        }
    }

    /// Pop the active overlay: clear the flag, report the transition, and
    /// re-apply the effective (base or default) layout.
    fn pop_overlay(&mut self, now_ms: u64) -> Vec<ChromeAction> {
        self.overlay = None;
        let mut actions = vec![
            ChromeAction::SetTakeover(false),
            self.overlay_state_action(now_ms),
        ];
        actions.extend(self.apply_effective_layout());
        actions
    }

    /// Fold a `local:brenn/toast` payload: mint a handle and render it. Toasts
    /// are a live-only event stream (the plane's ring depth is 0); a malformed
    /// payload is dropped-and-reported.
    ///
    /// The live set is capped at [`MAX_TOASTS`]: a new toast at the cap evicts the
    /// oldest expiring toast — falling back to the oldest overall only when every
    /// live toast is an exempt `error` — emitting one
    /// [`ChromeAction::DismissToast`] before the [`ChromeAction::ShowToast`],
    /// so a recurring producer cannot grow it without bound. A non-`error` toast
    /// records an expiry `now_ms + `[`TOAST_TTL_MS`] so [`ChromeCore::tick`]
    /// auto-dismisses it; an `error` toast persists until a manual dismissal.
    pub fn on_toast(&mut self, body: &str, now_ms: u64) -> Vec<ChromeAction> {
        let parsed: ToastBody = match serde_json::from_str(body) {
            Ok(b) => b,
            Err(err) => return vec![self.warn(format!("rejected toast payload: {err}"))],
        };
        let id = self.next_toast_id;
        self.next_toast_id += 1;
        let expires_at = match parsed.severity {
            ToastSeverity::Error => None,
            _ => Some(now_ms.saturating_add(TOAST_TTL_MS)),
        };
        let mut actions = Vec::new();
        while self.toasts.len() >= MAX_TOASTS {
            // Prefer the oldest *expiring* toast: an `error` toast is exempt from
            // the TTL because it demands operator attention, and evicting it to
            // admit a warning would destroy that signal. Only when every live
            // toast is an `error` does the cap fall back to oldest-overall.
            let pos = self
                .toasts
                .iter()
                .position(|t| t.expires_at.is_some())
                .unwrap_or(0);
            let evicted = self.toasts.remove(pos);
            actions.push(ChromeAction::DismissToast { id: evicted.id });
        }
        self.toasts.push(ActiveToast { id, expires_at });
        actions.push(ChromeAction::ShowToast {
            id,
            severity: parsed.severity,
            text: parsed.text,
            source: parsed.source,
        });
        actions
    }

    /// Auto-dismiss every non-`error` toast whose expiry is at or before `now_ms`.
    /// Driven by a chrome-side timer tick; pure logic (timestamp in, actions out)
    /// so the lifetime stays host-testable. `error` toasts (no expiry) are never
    /// dismissed here.
    pub fn tick(&mut self, now_ms: u64) -> Vec<ChromeAction> {
        let mut actions = Vec::new();
        self.toasts.retain(|t| match t.expires_at {
            Some(expires_at) if expires_at <= now_ms => {
                actions.push(ChromeAction::DismissToast { id: t.id });
                false
            }
            _ => true,
        });
        actions
    }

    /// Dismiss an active toast by its chrome-assigned `id` (a user dismissal in
    /// the DOM half). A no-op for an unknown/already-dismissed id.
    pub fn dismiss_toast(&mut self, id: u64) -> Vec<ChromeAction> {
        let Some(pos) = self.toasts.iter().position(|t| t.id == id) else {
            return Vec::new();
        };
        self.toasts.remove(pos);
        vec![ChromeAction::DismissToast { id }]
    }

    /// When the soonest-expiring live toast auto-dismisses, on the same clock
    /// [`Self::tick`] judges against, or `None` when nothing live expires.
    ///
    /// The soonest rather than any: a later toast's expiry is re-parked by the
    /// activation the earlier one's wake causes.
    fn next_expiry(&self) -> Option<u64> {
        self.toasts.iter().filter_map(|t| t.expires_at).min()
    }

    /// The instant the glue parks its next expiry wake at, or `None` when
    /// nothing live expires — which is why a page with no expiring toast never
    /// wakes.
    ///
    /// One clock: every instant here is the activation's own wall-clock reading,
    /// which is what the kernel parks a deferred publish against. An expiry
    /// already past yields that reading unchanged — a wake due immediately,
    /// which is what an unswept expiry deserves.
    pub fn next_wake(&self, now_ms: u64) -> Option<u64> {
        self.next_expiry().map(|expires_at| expires_at.max(now_ms))
    }

    /// The active toast ids, in show order.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    fn active_toasts(&self) -> Vec<u64> {
        self.toasts.iter().map(|t| t.id).collect()
    }

    fn warn(&self, message: String) -> ChromeAction {
        ChromeAction::Log {
            level: LogLevel::Warn,
            message,
        }
    }

    /// The overlay-state publish for the transition just folded: the post-fold
    /// [`Self::overlay`] value, stamped with the activation reading of the fold.
    ///
    /// Built here rather than in the page glue so the payload chrome puts on
    /// the bus is host-tested like every other decision.
    fn overlay_state_action(&self, now_ms: u64) -> ChromeAction {
        let body = OverlayStateBody {
            v: CONTROL_PLANE_VERSION,
            holder: self.overlay.clone(),
            since_stamp: now_ms,
        };
        ChromeAction::PublishOverlayState {
            body: serde_json::to_string(&body).expect("an OverlayStateBody serializes to JSON"),
        }
    }
}

use crate::spec::InPort;
#[cfg(not(target_arch = "wasm32"))]
use crate::spec::port::OVERLAY_STATE;
#[cfg(not(target_arch = "wasm32"))]
use brenn_surface_schema as proto;

/// How a chrome port's new slice is folded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FoldClass {
    /// A state port: the newest message fully describes the state, so only it is
    /// folded and a window carrying more than one new message is an operator
    /// misconfiguration.
    LatestWins,
    /// An event port: each message is its own fact, so every new one is folded in
    /// order.
    EventStream,
}

/// Which fold a port's messages get.
///
/// `layout`, `theme`, `link-state` and `surface-state` each carry a complete
/// current value — a layout document replaces the base layout wholesale, a theme
/// or link state is a single value, a surface-state body is the whole instance
/// set — so folding an older one before the newest changes nothing but the
/// number of re-renders and warns. `takeover` (request/release pairing) and
/// `toast` are streams of distinct facts; dropping any of them loses an event.
///
/// Exhaustive over [`InPort`], like [`fold`] and [`input_port_doc`]: a port
/// added to the specification fails to compile here until it is classified,
/// rather than defaulting to the event stream and replaying stale retained
/// state on every activation where a latest-wins fold was meant. An unknown
/// port takes the event-stream path so the unbound-port warn in [`fold`] fires
/// once per message.
fn fold_class(port: Option<InPort>) -> FoldClass {
    match port {
        Some(InPort::Layout)
        | Some(InPort::Theme)
        | Some(InPort::LinkState)
        | Some(InPort::SurfaceState) => FoldClass::LatestWins,
        // Chrome's own deferred self-wake never reaches the fold, and an
        // unbound name is reported message by message.
        Some(InPort::Takeover) | Some(InPort::Toast) | Some(InPort::ToastTick) | None => {
            FoldClass::EventStream
        }
    }
}

/// What to bind a chrome port to.
///
/// The help generator is this type's only reader, and it runs on the host: the
/// plane addresses below come from `brenn-surface-schema`, a host-graph crate.
#[cfg(not(target_arch = "wasm32"))]
pub enum PortChannel {
    /// A fixed reserved plane address, named exactly.
    Address(&'static str),
    /// An operator-chosen channel, described rather than named.
    Described(&'static str),
}

/// One row of the port table in chrome's generated help: the port to bind, the
/// channel it expects, and what that channel carries.
///
/// The `port` and `channel` fields hold the same constants the code binds and
/// parses, so a rename reaches the published doc as a compile error rather than
/// as silence.
#[cfg(not(target_arch = "wasm32"))]
pub struct PortDoc {
    /// The port name, as declared in config on the chrome instance.
    pub port: &'static str,
    /// The channel to bind.
    pub channel: PortChannel,
    /// What the channel carries, for a reader deciding what to bind.
    pub carries: &'static str,
}

/// The help row for one inbound port, or `None` for a port that is not an
/// operator binding.
///
/// Exhaustive over [`InPort`], which the specification generates: a port added
/// to the specification fails to compile here until it is given a row or
/// explicitly refused one.
#[cfg(not(target_arch = "wasm32"))]
fn input_port_doc(port: InPort) -> Option<PortDoc> {
    let (channel, carries) = match port {
        InPort::Layout => (
            PortChannel::Described("a `brenn:` layout channel (retained, depth ≥ 1)"),
            "the layout doc (below)",
        ),
        InPort::Theme => (
            PortChannel::Address(proto::LOCAL_THEME_CHANNEL),
            "`{ v, theme }` — the runtime theme axis",
        ),
        InPort::Takeover => (
            PortChannel::Address(proto::LOCAL_TAKEOVER_CHANNEL),
            "a component's fullscreen request/release (needs the surface `takeover` grant)",
        ),
        InPort::LinkState => (
            PortChannel::Address(proto::LOCAL_LINK_STATE_CHANNEL),
            "`{ v, state }` — the connection banner",
        ),
        InPort::SurfaceState => (
            PortChannel::Address(proto::LOCAL_SURFACE_STATE_CHANNEL),
            "the mounted-instance set chrome arranges",
        ),
        InPort::Toast => (
            PortChannel::Address(proto::LOCAL_TOAST_CHANNEL),
            "transient notices (live-only, retains nothing)",
        ),
        // Chrome parks its own toast-expiry wake here and nothing else ever
        // publishes to it, so an operator has nothing to bind and the table
        // carries no row.
        InPort::ToastTick => return None,
    };
    Some(PortDoc {
        port: port.name(),
        channel,
        carries,
    })
}

/// Chrome's operator-bindable input ports, in specification order.
#[cfg(not(target_arch = "wasm32"))]
pub fn input_port_docs() -> Vec<PortDoc> {
    InPort::ALL.into_iter().filter_map(input_port_doc).collect()
}

/// Chrome's one output port row.
#[cfg(not(target_arch = "wasm32"))]
pub const OUTPUT_PORT_DOC: PortDoc = PortDoc {
    port: OVERLAY_STATE,
    channel: PortChannel::Address(proto::LOCAL_OVERLAY_STATE_CHANNEL),
    carries: "`{ v, holder, since_stamp }` — which instance holds the fullscreen \
              overlay (needs the surface `takeover` grant)",
};

/// Route a delivered body to the core method its port names. An unbound or
/// unknown port is a config/kernel error — chrome only ever receives on ports it
/// declared — so it is reported and dropped, never guessed at.
///
/// DOM-free and host-tested: this is the single most swappable piece of the
/// wasm half's wiring (transposing two arms would compile clean), so it lives in
/// the core where a host `#[test]` pins each port to its method.
pub fn fold(core: &mut ChromeCore, port: &str, body: &str, now_ms: u64) -> Vec<ChromeAction> {
    match InPort::from_name(port) {
        Some(InPort::Layout) => core.on_layout(body),
        Some(InPort::Theme) => core.on_theme(body),
        Some(InPort::LinkState) => core.on_link_state(body),
        Some(InPort::SurfaceState) => core.on_surface_state(body, now_ms),
        Some(InPort::Takeover) => core.on_takeover(body, now_ms),
        Some(InPort::Toast) => core.on_toast(body, now_ms),
        // Consumed by the activation seam before reaching fold; arriving
        // here is unexpected, so treat it as unbound.
        Some(InPort::ToastTick) | None => vec![ChromeAction::Log {
            level: LogLevel::Warn,
            message: format!("chrome received on unbound port {port:?}"),
        }],
    }
}

/// Fold one activation window's **new** messages into the core and return the
/// actions they produced.
///
/// Only the `new_from..` slice is folded; retained context is skipped because
/// chrome's folds are not idempotent. A component may rely on the new set alone
/// to catch up on attach.
///
/// How much of that slice is folded is the port's [`FoldClass`]. An event port
/// folds all of it in order. A latest-wins port folds only the newest message
/// and reports a window that carried more than one as an operator
/// misconfiguration — an error, because the binding's `push_depth` is wrong and
/// only the operator can fix it, and chrome carries on with the latest either
/// way.
pub fn fold_window(
    core: &mut ChromeCore,
    window: &ActivationWindow<'_>,
    now_ms: u64,
) -> Vec<ChromeAction> {
    let mut actions = Vec::new();
    match fold_class(InPort::from_name(window.port)) {
        FoldClass::LatestWins => {
            if let Some(message) = window.latest_wins_misconfiguration() {
                actions.push(ChromeAction::Log {
                    level: LogLevel::Error,
                    message,
                });
            }
            if let Some(raw) = window.new_raw.last() {
                fold_delivery(core, window.port, raw, now_ms, &mut actions);
            }
        }
        FoldClass::EventStream => {
            for raw in window.new_raw {
                fold_delivery(core, window.port, raw, now_ms, &mut actions);
            }
        }
    }
    actions
}

/// Fold one delivered envelope's body, or report the envelope as malformed.
///
/// A delivery that does not parse as an envelope is host/guest skew, not
/// untrusted content — but chrome renders the page, so it reports and carries on
/// rather than trapping and taking the whole surface's shell down with it.
fn fold_delivery(
    core: &mut ChromeCore,
    port: &str,
    raw: &str,
    now_ms: u64,
    actions: &mut Vec<ChromeAction>,
) {
    match serde_json::from_str::<MessageEnvelope>(raw) {
        Ok(envelope) => actions.extend(fold(core, port, &envelope.body, now_ms)),
        Err(err) => actions.push(ChromeAction::Log {
            level: LogLevel::Error,
            message: format!("chrome could not read a delivery on port {port:?}: {err}"),
        }),
    }
}

// Host-only: native `#[test]`s run in every `make check`. Excluded from the
// wasm32 target so the browser test binary carries no libtest harness.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::spec::port::{
        LAYOUT, LINK_STATE, SURFACE_STATE, TAKEOVER, THEME, TOAST, TOAST_TICK,
    };
    use crate::wire::SurfaceStateInstance;
    use brenn_surface_schema::LOCAL_TOAST_CHANNEL;
    use serde_json::json;

    /// The clock reading every fold in this suite is given. Fixed: only the
    /// overlay-state stamp reads it, and no test here asserts on elapsed time.
    const NOW: u64 = 1_000;

    fn core() -> ChromeCore {
        ChromeCore::new("chrome")
    }

    /// A surface-state payload with the given (instance, state) rows.
    fn surface_state(rows: &[(&str, InstanceState)]) -> String {
        let body = SurfaceStateBody {
            v: CONTROL_PLANE_VERSION,
            instances: rows
                .iter()
                .map(|(instance, state)| SurfaceStateInstance {
                    instance: (*instance).to_string(),
                    kind: "k".to_string(),
                    state: *state,
                    reason: None,
                })
                .collect(),
        };
        serde_json::to_string(&body).unwrap()
    }

    /// Seed the arrangeable set with mounted p1/p2/p3, discarding the emitted
    /// default-layout action.
    fn seed_three(core: &mut ChromeCore) {
        core.on_surface_state(
            &surface_state(&[
                ("p1", InstanceState::Mounted),
                ("p2", InstanceState::Mounted),
                ("p3", InstanceState::Mounted),
            ]),
            NOW,
        );
    }

    fn layout_kind(actions: &[ChromeAction]) -> Option<LayoutKind> {
        actions.iter().find_map(|a| match a {
            ChromeAction::ApplyLayout { kind, .. } => Some(*kind),
            _ => None,
        })
    }

    // ── Port routing (`fold`) ─────────────────────────────────────────────

    #[test]
    fn fold_routes_each_port_to_its_method() {
        let theme = json!({ "v": 1, "theme": "light" }).to_string();
        let link = json!({ "v": 1, "state": "reconnecting" }).to_string();

        // Theme body on the theme port → SetTheme.
        let mut c = core();
        assert_eq!(
            fold(&mut c, THEME, &theme, 0),
            vec![ChromeAction::SetTheme(Theme::Light)]
        );

        // The same theme body on the link-state port must NOT set a theme — it
        // is rejected as a malformed link-state payload, proving the arm is not
        // transposed onto `on_theme`.
        let mut c = core();
        let actions = fold(&mut c, LINK_STATE, &theme, 0);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, ChromeAction::SetTheme(_)))
        );
        assert_eq!(c.theme(), Theme::Dark);

        // A link-state body on the link-state port → SetBanner.
        let mut c = core();
        assert_eq!(
            fold(&mut c, LINK_STATE, &link, 0),
            vec![ChromeAction::SetBanner(BannerState::Reconnecting)]
        );
    }

    #[test]
    fn fold_reports_an_unbound_port() {
        let mut c = core();
        let actions = fold(&mut c, "not-a-port", "{}", 0);
        assert!(matches!(
            actions.as_slice(),
            [ChromeAction::Log {
                level: LogLevel::Warn,
                ..
            }]
        ));
    }

    // ── Theme ─────────────────────────────────────────────────────────────

    #[test]
    fn theme_parses_and_changes_once() {
        let mut c = core();
        let body = json!({ "v": 1, "theme": "light" }).to_string();
        assert_eq!(
            c.on_theme(&body),
            vec![ChromeAction::SetTheme(Theme::Light)]
        );
        // Re-publishing the same theme is a no-op.
        assert_eq!(c.on_theme(&body), Vec::new());
        assert_eq!(c.theme(), Theme::Light);
    }

    #[test]
    fn unknown_theme_is_reported_not_applied() {
        let mut c = core();
        let body = json!({ "v": 1, "theme": "chartreuse" }).to_string();
        let actions = c.on_theme(&body);
        assert!(matches!(
            actions.as_slice(),
            [ChromeAction::Log {
                level: LogLevel::Warn,
                ..
            }]
        ));
        assert_eq!(c.theme(), Theme::Dark);
    }

    #[test]
    fn malformed_theme_payload_reported() {
        let mut c = core();
        assert!(matches!(
            c.on_theme("not json").as_slice(),
            [ChromeAction::Log { .. }]
        ));
    }

    // ── Banner from link-state ──────────────────────────────────────────────

    #[test]
    fn link_state_maps_to_banner_and_dedups() {
        let mut c = core();
        for (state, banner) in [
            (LinkState::Connected, BannerState::Hidden),
            (LinkState::Reconnecting, BannerState::Reconnecting),
            (LinkState::Reloading, BannerState::Reloading),
            (LinkState::Fatal, BannerState::Fatal),
        ] {
            let body = serde_json::to_string(&LinkStateBody {
                v: CONTROL_PLANE_VERSION,
                state,
            })
            .unwrap();
            assert_eq!(
                c.on_link_state(&body),
                vec![ChromeAction::SetBanner(banner)]
            );
            // Same state again: no action.
            assert_eq!(c.on_link_state(&body), Vec::new());
            assert_eq!(c.banner(), banner);
        }
    }

    // ── Layout doc → action, last-good ──────────────────────────────────────

    #[test]
    fn valid_layout_doc_applies() {
        let mut c = core();
        seed_three(&mut c);
        let doc = json!({
            "v": 1, "kind": "columns-2", "ratio": 0.6,
            "panels": { "a": { "instance": "p1", "label": "L" }, "b": { "instance": "p2" } }
        })
        .to_string();
        let actions = c.on_layout(&doc);
        assert_eq!(
            actions,
            vec![ChromeAction::ApplyLayout {
                kind: LayoutKind::Columns2,
                ratio: Some("0.6".to_string()),
                panels: vec![
                    LayoutPlacement {
                        slot: "a".to_string(),
                        instance: "p1".to_string(),
                        label: Some("L".to_string()),
                    },
                    LayoutPlacement {
                        slot: "b".to_string(),
                        instance: "p2".to_string(),
                        label: None,
                    },
                ],
                instances: vec!["p1".to_string(), "p2".to_string(), "p3".to_string()],
            }]
        );
    }

    #[test]
    fn bad_layout_doc_keeps_last_good() {
        let mut c = core();
        seed_three(&mut c);
        let good = json!({
            "v": 1, "kind": "single", "panels": { "a": { "instance": "p1" } }
        })
        .to_string();
        assert_eq!(layout_kind(&c.on_layout(&good)), Some(LayoutKind::Single));
        // A doc naming an unknown instance is rejected: one warn, no ApplyLayout.
        let bad = json!({
            "v": 1, "kind": "single", "panels": { "a": { "instance": "nope" } }
        })
        .to_string();
        let actions = c.on_layout(&bad);
        assert!(matches!(
            actions.as_slice(),
            [ChromeAction::Log {
                level: LogLevel::Warn,
                ..
            }]
        ));
        assert_eq!(c.base_layout.as_ref().unwrap().kind, LayoutKind::Single);
    }

    #[test]
    fn parse_error_layout_doc_reported() {
        let mut c = core();
        seed_three(&mut c);
        assert!(matches!(
            c.on_layout("{").as_slice(),
            [ChromeAction::Log {
                level: LogLevel::Warn,
                ..
            }]
        ));
    }

    // ── Default layout from surface-state ───────────────────────────────────

    #[test]
    fn surface_state_synthesizes_default_layout_by_count() {
        let mut c = core();
        // Two mounted instances → columns-2 default.
        let actions = c.on_surface_state(
            &surface_state(&[
                ("p1", InstanceState::Mounted),
                ("p2", InstanceState::Mounted),
            ]),
            NOW,
        );
        assert_eq!(layout_kind(&actions), Some(LayoutKind::Columns2));
    }

    #[test]
    fn chrome_excludes_itself_from_arrangement() {
        let mut c = core();
        // Chrome's own instance in the surface-state must not be placed.
        let actions = c.on_surface_state(
            &surface_state(&[
                ("chrome", InstanceState::Mounted),
                ("p1", InstanceState::Mounted),
            ]),
            NOW,
        );
        // One placeable instance → single default, and the instances list has
        // only p1.
        match actions.into_iter().find_map(|a| match a {
            ChromeAction::ApplyLayout {
                kind, instances, ..
            } => Some((kind, instances)),
            _ => None,
        }) {
            Some((kind, instances)) => {
                assert_eq!(kind, LayoutKind::Single);
                assert_eq!(instances, vec!["p1".to_string()]);
            }
            None => panic!("expected an ApplyLayout"),
        }
    }

    #[test]
    fn unchanged_surface_state_emits_nothing() {
        let mut c = core();
        let body = surface_state(&[("p1", InstanceState::Mounted)]);
        assert!(!c.on_surface_state(&body, NOW).is_empty());
        assert_eq!(c.on_surface_state(&body, NOW), Vec::new());
    }

    // ── Takeover overlay, depth 1 ───────────────────────────────────────────

    #[test]
    fn takeover_request_pushes_and_release_pops() {
        let mut c = core();
        seed_three(&mut c);
        let base = json!({
            "v": 1, "kind": "columns-3",
            "panels": {
                "a": { "instance": "p1" }, "b": { "instance": "p2" }, "c": { "instance": "p3" }
            }
        })
        .to_string();
        c.on_layout(&base);

        let request = serde_json::to_string(&TakeoverBody {
            v: CONTROL_PLANE_VERSION,
            action: TakeoverAction::Request,
            instance: "p2".to_string(),
        })
        .unwrap();
        let actions = c.on_takeover(&request, NOW);
        assert_eq!(actions[0], ChromeAction::SetTakeover(true));
        // The overlay is a single-slot fullscreen layout on the requester.
        assert_eq!(layout_kind(&actions), Some(LayoutKind::Single));

        // A layout doc published mid-takeover is deferred, not applied.
        let mid = json!({
            "v": 1, "kind": "single", "panels": { "a": { "instance": "p1" } }
        })
        .to_string();
        assert!(matches!(
            c.on_layout(&mid).as_slice(),
            [ChromeAction::Log {
                level: LogLevel::Debug,
                ..
            }]
        ));

        let release = serde_json::to_string(&TakeoverBody {
            v: CONTROL_PLANE_VERSION,
            action: TakeoverAction::Release,
            instance: "p2".to_string(),
        })
        .unwrap();
        let actions = c.on_takeover(&release, NOW);
        assert_eq!(actions[0], ChromeAction::SetTakeover(false));
        // Pop re-applies the deferred base (the single p1 doc).
        assert_eq!(layout_kind(&actions), Some(LayoutKind::Single));
    }

    #[test]
    fn second_takeover_from_different_instance_denied() {
        let mut c = core();
        seed_three(&mut c);
        let req = |instance: &str| {
            serde_json::to_string(&TakeoverBody {
                v: CONTROL_PLANE_VERSION,
                action: TakeoverAction::Request,
                instance: instance.to_string(),
            })
            .unwrap()
        };
        c.on_takeover(&req("p1"), NOW);
        let actions = c.on_takeover(&req("p2"), NOW);
        assert!(matches!(
            actions.as_slice(),
            [ChromeAction::Log {
                level: LogLevel::Warn,
                ..
            }]
        ));
        // Idempotent re-request from the incumbent is a no-op.
        assert_eq!(c.on_takeover(&req("p1"), NOW), Vec::new());
    }

    #[test]
    fn overlay_holder_death_pops_overlay() {
        let mut c = core();
        seed_three(&mut c);
        let req = serde_json::to_string(&TakeoverBody {
            v: CONTROL_PLANE_VERSION,
            action: TakeoverAction::Request,
            instance: "p2".to_string(),
        })
        .unwrap();
        c.on_takeover(&req, NOW);
        // p2 fails: surface-state now reports it Failed. Overlay must pop.
        let actions = c.on_surface_state(
            &surface_state(&[
                ("p1", InstanceState::Mounted),
                ("p2", InstanceState::Failed),
                ("p3", InstanceState::Mounted),
            ]),
            NOW,
        );
        assert!(actions.contains(&ChromeAction::SetTakeover(false)));
        assert!(c.overlay.is_none());
    }

    #[test]
    fn takeover_from_unknown_instance_reported() {
        let mut c = core();
        seed_three(&mut c);
        let req = serde_json::to_string(&TakeoverBody {
            v: CONTROL_PLANE_VERSION,
            action: TakeoverAction::Request,
            instance: "ghost".to_string(),
        })
        .unwrap();
        assert!(matches!(
            c.on_takeover(&req, NOW).as_slice(),
            [ChromeAction::Log {
                level: LogLevel::Warn,
                ..
            }]
        ));
    }

    // ── The activation-window fold ──────────────────────────────────────────

    /// A takeover body naming `instance`.
    fn takeover(action: TakeoverAction, instance: &str) -> String {
        serde_json::to_string(&TakeoverBody {
            v: CONTROL_PLANE_VERSION,
            action,
            instance: instance.to_string(),
        })
        .unwrap()
    }

    /// A window on `port` whose first `context.len()` envelopes are retained
    /// context and the rest new.
    fn port_window(port: &str, context: &[String], new: &[String]) -> OwnedWindow {
        let wrap = |bodies: &[String]| -> Vec<String> {
            bodies
                .iter()
                .map(|body| {
                    serde_json::to_string(&brenn_surface_test_fixtures::sample_envelope(body))
                        .expect("the sample envelope serializes")
                })
                .collect()
        };
        OwnedWindow {
            port: port.to_string(),
            context_raw: wrap(context),
            new_raw: wrap(new),
        }
    }

    /// The owned backing of an [`ActivationWindow`], since the core's view
    /// borrows its slices.
    struct OwnedWindow {
        port: String,
        context_raw: Vec<String>,
        new_raw: Vec<String>,
    }

    impl OwnedWindow {
        fn view(&self) -> ActivationWindow<'_> {
            ActivationWindow {
                port: &self.port,
                context_raw: &self.context_raw,
                new_raw: &self.new_raw,
            }
        }
    }

    #[test]
    fn a_retained_release_in_context_folds_nothing() {
        // The warn-spam repro. After a completed takeover cycle the depth-1
        // plane retains the Release, so it rides along as context on every
        // later activation. Re-folding it warns about a release chrome does not
        // hold; skipping context is what stops that.
        let mut c = core();
        seed_three(&mut c);
        let window = port_window(TAKEOVER, &[takeover(TakeoverAction::Release, "p2")], &[]);
        assert_eq!(fold_window(&mut c, &window.view(), 0), Vec::new());
    }

    #[test]
    fn a_retained_request_in_context_does_not_re_push_the_overlay() {
        // The latent hazard: chrome pops the overlay when its holder dies, but
        // the holder's Request is still the plane's retained value. Re-folding
        // it would hand a dead instance a fresh fullscreen overlay.
        let mut c = core();
        seed_three(&mut c);
        c.on_takeover(&takeover(TakeoverAction::Request, "p2"), NOW);
        c.on_surface_state(
            &surface_state(&[
                ("p1", InstanceState::Mounted),
                ("p2", InstanceState::Failed),
                ("p3", InstanceState::Mounted),
            ]),
            NOW,
        );
        assert!(c.overlay.is_none());

        let window = port_window(TAKEOVER, &[takeover(TakeoverAction::Request, "p2")], &[]);
        assert_eq!(fold_window(&mut c, &window.view(), 0), Vec::new());
        assert!(c.overlay.is_none());
    }

    #[test]
    fn a_non_takeover_window_skips_its_context_too() {
        // The rule is per-window, not per-port. These ports — theme equality,
        // banner equality, layout last-good — are exactly where a "context is
        // harmless here" regression would hide. A retained *older* theme folded
        // as context flips the page light and the new value flips it back: one
        // frame of flicker, no assertion in the takeover suite.
        let mut c = core();
        let light = json!({ "v": 1, "theme": "light" }).to_string();
        let dark = json!({ "v": 1, "theme": "dark" }).to_string();
        // Chrome starts dark; the window's context is a light value it has
        // already seen and its new slice restates dark.
        let window = port_window(THEME, std::slice::from_ref(&light), &[dark]);
        assert_eq!(fold_window(&mut c, &window.view(), NOW), Vec::new());
        assert_eq!(c.theme(), Theme::Dark);

        // And the new slice still reaches the page, on this port as on takeover.
        let window = port_window(THEME, &[], &[light]);
        assert_eq!(
            fold_window(&mut c, &window.view(), NOW),
            vec![ChromeAction::SetTheme(Theme::Light)]
        );
    }

    /// A window whose new slice is handed over verbatim, so the fold meets a
    /// document that is not a `MessageEnvelope`.
    fn unwrapped_window(port: &str, new: &[&str]) -> OwnedWindow {
        OwnedWindow {
            port: port.to_string(),
            context_raw: Vec::new(),
            new_raw: new.iter().map(|raw| (*raw).to_string()).collect(),
        }
    }

    /// The one error log in `actions`, or a panic naming what was there instead.
    fn only_error(actions: &[ChromeAction]) -> &str {
        let mut errors = actions.iter().filter_map(|action| match action {
            ChromeAction::Log {
                level: LogLevel::Error,
                message,
            } => Some(message.as_str()),
            _ => None,
        });
        let first = errors
            .next()
            .unwrap_or_else(|| panic!("no error log in {actions:?}"));
        assert!(errors.next().is_none(), "more than one error: {actions:?}");
        first
    }

    #[test]
    fn a_latest_wins_delivery_that_is_not_an_envelope_is_reported_and_folds_nothing() {
        // Chrome is the one kind that does not fail its activation on a bad
        // envelope: it renders the page, and trapping would take the shell down
        // with it. So the arm is a report, and the page it was rendering is
        // untouched — the theme it already holds still stands.
        let mut c = core();
        let window = unwrapped_window(THEME, &["{"]);
        let actions = fold_window(&mut c, &window.view(), NOW);
        assert!(only_error(&actions).contains(THEME), "{actions:?}");
        assert_eq!(c.theme(), Theme::Dark);
        assert!(actions.iter().all(|action| matches!(
            action,
            ChromeAction::Log {
                level: LogLevel::Error,
                ..
            }
        )));
    }

    #[test]
    fn a_well_formed_json_document_that_is_not_an_envelope_is_reported_too() {
        // The layer under test is the envelope, not the body: a document with
        // every other field and no `body` parses as JSON and still is not a
        // delivery, so it must not reach the layout parser.
        let mut c = core();
        let window = unwrapped_window(LAYOUT, &[&json!({ "v": 1, "kind": "single" }).to_string()]);
        let actions = fold_window(&mut c, &window.view(), NOW);
        assert!(only_error(&actions).contains(LAYOUT), "{actions:?}");
        assert!(
            placed(&actions).is_empty(),
            "the layout parser never saw it: {actions:?}"
        );
    }

    #[test]
    fn an_unreadable_delivery_does_not_stop_its_siblings_in_the_same_window() {
        // An event-stream port folds every new message, so one skewed publisher
        // must cost its own message and nothing else in the window.
        let mut c = core();
        let good = serde_json::to_string(&brenn_surface_test_fixtures::sample_envelope(
            &toast_body(ToastSeverity::Warning, "still shown"),
        ))
        .expect("the sample envelope serializes");
        let window = unwrapped_window(TOAST, &["not an envelope", &good]);
        let actions = fold_window(&mut c, &window.view(), NOW);
        assert!(only_error(&actions).contains(TOAST), "{actions:?}");
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, ChromeAction::ShowToast { text, .. } if text == "still shown")),
            "{actions:?}"
        );
    }

    /// A layout doc placing `instance` in a `single` layout.
    fn layout_doc(instance: &str) -> String {
        json!({
            "v": 1, "kind": "single", "panels": { "a": { "instance": instance } }
        })
        .to_string()
    }

    /// The instances an `ApplyLayout` placed, slot by slot.
    fn placed(actions: &[ChromeAction]) -> Vec<Vec<String>> {
        actions
            .iter()
            .filter_map(|a| match a {
                ChromeAction::ApplyLayout { panels, .. } => {
                    Some(panels.iter().map(|p| p.instance.clone()).collect())
                }
                _ => None,
            })
            .collect()
    }

    /// The messages of the `Log`s at `level` in `actions`.
    fn logs_at(actions: &[ChromeAction], want: LogLevel) -> Vec<String> {
        actions
            .iter()
            .filter_map(|a| match a {
                ChromeAction::Log { level, message } if *level == want => Some(message.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_latest_wins_window_folds_only_the_newest() {
        // Latest wins *is* the fold on the layout port: a layout doc is a
        // complete document, so applying the older two before the newest is
        // three re-renders for one outcome. One ApplyLayout, from the last doc.
        let mut c = core();
        seed_three(&mut c);
        let window = port_window(
            LAYOUT,
            &[],
            &[layout_doc("p1"), layout_doc("p2"), layout_doc("p3")],
        );
        let actions = fold_window(&mut c, &window.view(), NOW);
        assert_eq!(placed(&actions), vec![vec!["p3".to_string()]]);

        // And the window itself is the report: three new messages on a
        // latest-wins port means the binding's push_depth is not 1.
        let errors = logs_at(&actions, LogLevel::Error);
        assert_eq!(errors.len(), 1, "{actions:?}");
        assert!(errors[0].contains("\"layout\""), "{}", errors[0]);
        assert!(errors[0].contains('3'), "{}", errors[0]);
        assert!(errors[0].contains("push_depth"), "{}", errors[0]);
    }

    #[test]
    fn a_single_new_latest_wins_window_reports_no_misconfiguration() {
        // The healthy shape under `push_depth = 1`: retained context plus one
        // new doc. Applied, silently — the error must nag only the operator who
        // actually set a depth above 1.
        let mut c = core();
        seed_three(&mut c);
        let window = port_window(
            LAYOUT,
            &[layout_doc("p1"), layout_doc("p2")],
            &[layout_doc("p3")],
        );
        let actions = fold_window(&mut c, &window.view(), NOW);
        assert_eq!(placed(&actions), vec![vec!["p3".to_string()]]);
        assert_eq!(logs_at(&actions, LogLevel::Error), Vec::<String>::new());
    }

    #[test]
    fn an_invalid_newest_doc_keeps_last_good_even_over_a_valid_older_new_row() {
        // The deliberate choice: take-latest applies the newest and only the
        // newest, so an invalid newest leaves the last-good layout on screen
        // rather than an older doc from the same window. Stale-presented-as-
        // current is the worse failure, and a fold-all would produce exactly
        // that — p1's layout painted as if it were the current one.
        let mut c = core();
        seed_three(&mut c);
        assert_eq!(
            placed(&c.on_layout(&layout_doc("p2"))),
            vec![vec!["p2".to_string()]]
        );

        let window = port_window(LAYOUT, &[], &[layout_doc("p1"), layout_doc("ghost")]);
        let actions = fold_window(&mut c, &window.view(), NOW);
        assert!(placed(&actions).is_empty(), "{actions:?}");
        assert_eq!(c.base_layout.as_ref().unwrap().panels["a"].instance, "p2");
        // Two reports: the misconfigured depth and the rejected doc.
        assert_eq!(logs_at(&actions, LogLevel::Error).len(), 1);
        assert_eq!(logs_at(&actions, LogLevel::Warn).len(), 1);
    }

    #[test]
    fn an_event_port_folds_every_new_envelope_in_order() {
        // Context is skipped, new is folded whole and in order: the Request
        // pushes and the Release that follows it pops, both from one window.
        let mut c = core();
        seed_three(&mut c);
        let window = port_window(
            TAKEOVER,
            &[takeover(TakeoverAction::Release, "p3")],
            &[
                takeover(TakeoverAction::Request, "p2"),
                takeover(TakeoverAction::Release, "p2"),
            ],
        );
        let actions = fold_window(&mut c, &window.view(), 0);
        let takeovers: Vec<bool> = actions
            .iter()
            .filter_map(|a| match a {
                ChromeAction::SetTakeover(on) => Some(*on),
                _ => None,
            })
            .collect();
        assert_eq!(takeovers, vec![true, false]);
        assert!(
            !actions.iter().any(|a| matches!(
                a,
                ChromeAction::Log {
                    level: LogLevel::Warn,
                    ..
                }
            )),
            "the context Release must not be folded: {actions:?}"
        );
        assert!(c.overlay.is_none());
    }

    #[test]
    fn every_port_is_classified_by_what_its_messages_mean() {
        // An arm deleted from the latest-wins side costs re-renders, an arm
        // added to it silently drops every message of a burst but the newest.
        // Neither shows up anywhere else — Part C sets `push_depth = 1` on
        // exactly the latest-wins bindings, so the misconfiguration error will
        // not fire to reveal it in dev either.
        for (port, class) in [
            (LAYOUT, FoldClass::LatestWins),
            (THEME, FoldClass::LatestWins),
            (LINK_STATE, FoldClass::LatestWins),
            (SURFACE_STATE, FoldClass::LatestWins),
            (TAKEOVER, FoldClass::EventStream),
            (TOAST, FoldClass::EventStream),
            (TOAST_TICK, FoldClass::EventStream),
            ("no-such-port", FoldClass::EventStream),
        ] {
            assert_eq!(fold_class(InPort::from_name(port)), class, "port {port:?}");
        }
    }

    #[test]
    fn a_latest_wins_port_other_than_layout_takes_the_newest_too() {
        // The behavioral half of the table on a port whose fold is a single
        // value rather than a document: two new themes, one SetTheme, from the
        // newer — and the same operator error the layout port reports.
        let mut c = core();
        let light = json!({ "v": 1, "theme": "light" }).to_string();
        let dark = json!({ "v": 1, "theme": "dark" }).to_string();
        // Chrome starts dark, so a fold-all would flip the page light and back:
        // one SetTheme(Light) this assertion would see.
        let window = port_window(THEME, &[], &[light, dark]);
        let actions = fold_window(&mut c, &window.view(), NOW);
        assert_eq!(
            actions
                .iter()
                .filter(|a| matches!(a, ChromeAction::SetTheme(_)))
                .count(),
            0,
            "the newest restates the theme chrome already holds: {actions:?}"
        );
        assert_eq!(c.theme(), Theme::Dark);
        assert_eq!(logs_at(&actions, LogLevel::Error).len(), 1, "{actions:?}");
    }

    #[test]
    fn an_event_port_burst_loses_nothing_to_the_newest() {
        // The other direction of the same table. A toast is its own fact, so a
        // window carrying two shows two — and says nothing about push_depth,
        // because a depth above 1 is what an event port is for.
        let mut c = core();
        let window = port_window(
            TOAST,
            &[],
            &[
                toast_body(ToastSeverity::Info, "first"),
                toast_body(ToastSeverity::Info, "second"),
            ],
        );
        let actions = fold_window(&mut c, &window.view(), NOW);
        let shown: Vec<String> = actions
            .iter()
            .filter_map(|a| match a {
                ChromeAction::ShowToast { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(shown, vec!["first".to_string(), "second".to_string()]);
        assert_eq!(logs_at(&actions, LogLevel::Error), Vec::<String>::new());
    }

    #[test]
    fn an_unknown_port_warns_once_per_message() {
        // Unknown ports ride the event path deliberately: the unbound-port warn
        // names every message chrome was handed and could not place, which is
        // what makes a mis-wired binding legible. Taking the newest would report
        // one message of a burst and swallow the rest.
        let mut c = core();
        let window = port_window(
            "no-such-port",
            &[json!({}).to_string()],
            &[json!({}).to_string(), json!({}).to_string()],
        );
        let actions = fold_window(&mut c, &window.view(), NOW);
        let warns = logs_at(&actions, LogLevel::Warn);
        assert_eq!(warns.len(), 2, "{actions:?}");
        assert!(warns[0].contains("unbound port"), "{}", warns[0]);
    }

    // ── Overlay-state publishes ─────────────────────────────────────────────

    /// The overlay-state bodies a fold published, parsed.
    fn overlay_states(actions: &[ChromeAction]) -> Vec<OverlayStateBody> {
        actions
            .iter()
            .filter_map(|a| match a {
                ChromeAction::PublishOverlayState { body } => {
                    Some(serde_json::from_str(body).expect("a published body parses"))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn overlay_push_and_pop_each_publish_one_transition() {
        // The instrument: chrome is the only party that knows which overlay is
        // up, so every transition of `overlay` — and nothing else — goes on the
        // plane the kernel reads.
        let mut c = core();
        seed_three(&mut c);

        let pushed = overlay_states(&c.on_takeover(&takeover(TakeoverAction::Request, "p2"), NOW));
        assert_eq!(pushed.len(), 1);
        assert_eq!(pushed[0].holder.as_deref(), Some("p2"));
        assert_eq!(pushed[0].v, CONTROL_PLANE_VERSION);
        assert_eq!(pushed[0].since_stamp, NOW);

        let popped =
            overlay_states(&c.on_takeover(&takeover(TakeoverAction::Release, "p2"), NOW + 5));
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0].holder, None);
        assert_eq!(popped[0].since_stamp, NOW + 5);
    }

    #[test]
    fn a_dead_holder_pop_publishes_the_transition() {
        // The pop chrome makes on its own initiative counts as much as one a
        // component asked for: the overlay came down, so the report must say so.
        let mut c = core();
        seed_three(&mut c);
        c.on_takeover(&takeover(TakeoverAction::Request, "p2"), NOW);
        let actions = c.on_surface_state(
            &surface_state(&[
                ("p1", InstanceState::Mounted),
                ("p2", InstanceState::Failed),
                ("p3", InstanceState::Mounted),
            ]),
            NOW + 9,
        );
        let published = overlay_states(&actions);
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].holder, None);
        assert_eq!(published[0].since_stamp, NOW + 9);
    }

    #[test]
    fn a_non_transition_publishes_nothing() {
        // No heartbeat: an idempotent re-request, a denied request, a release
        // from a non-holder, and a surface-state change that leaves the holder
        // alive all leave `overlay` where it was, so none of them speaks.
        let mut c = core();
        seed_three(&mut c);
        c.on_takeover(&takeover(TakeoverAction::Request, "p2"), NOW);

        for body in [
            takeover(TakeoverAction::Request, "p2"),
            takeover(TakeoverAction::Request, "p1"),
            takeover(TakeoverAction::Release, "p3"),
        ] {
            let actions = c.on_takeover(&body, NOW);
            assert!(
                overlay_states(&actions).is_empty(),
                "no transition, no publish: {actions:?}"
            );
        }
        let actions = c.on_surface_state(
            &surface_state(&[
                ("p1", InstanceState::Mounted),
                ("p2", InstanceState::Mounted),
            ]),
            NOW,
        );
        assert!(
            overlay_states(&actions).is_empty(),
            "the holder is still alive: {actions:?}"
        );
        assert_eq!(c.overlay.as_deref(), Some("p2"));
    }

    // ── Toast queue ─────────────────────────────────────────────────────────

    /// A toast body of the given severity/text.
    fn toast_body(severity: ToastSeverity, text: &str) -> String {
        serde_json::to_string(&ToastBody {
            v: CONTROL_PLANE_VERSION,
            severity,
            text: text.to_string(),
            source: ToastSource::Kernel,
        })
        .unwrap()
    }

    #[test]
    fn toast_shows_then_dismisses() {
        let mut c = core();
        let body = serde_json::to_string(&ToastBody {
            v: CONTROL_PLANE_VERSION,
            severity: ToastSeverity::Warning,
            text: "heads up".to_string(),
            source: ToastSource::Kernel,
        })
        .unwrap();
        let actions = c.on_toast(&body, 0);
        let id = match actions.as_slice() {
            [
                ChromeAction::ShowToast {
                    id,
                    severity,
                    text,
                    source,
                },
            ] => {
                assert_eq!(*severity, ToastSeverity::Warning);
                assert_eq!(text, "heads up");
                assert_eq!(*source, ToastSource::Kernel);
                *id
            }
            other => panic!("expected ShowToast, got {other:?}"),
        };
        assert_eq!(c.active_toasts(), vec![id]);
        assert_eq!(c.dismiss_toast(id), vec![ChromeAction::DismissToast { id }]);
        assert!(c.active_toasts().is_empty());
        // Dismissing again is a no-op.
        assert_eq!(c.dismiss_toast(id), Vec::new());
    }

    #[test]
    fn toast_ids_are_distinct_and_ordered() {
        let mut c = core();
        let body = toast_body(ToastSeverity::Info, "a");
        c.on_toast(&body, 0);
        c.on_toast(&body, 0);
        assert_eq!(c.active_toasts(), vec![0, 1]);
    }

    /// A hot `alarm` binding emits a stream of warning toasts. The cap must not
    /// destroy a live `error` toast to admit one: the `error` rung is exempt from
    /// the TTL precisely because an operator-attention event must not evaporate,
    /// and a severity-blind eviction would evaporate it within MAX_TOASTS
    /// messages of any storm.
    #[test]
    fn toast_at_cap_spares_error_toasts() {
        let mut c = core();
        let err = match c
            .on_toast(&toast_body(ToastSeverity::Error, "e"), 0)
            .as_slice()
        {
            [ChromeAction::ShowToast { id, .. }] => *id,
            other => panic!("expected a show, got {other:?}"),
        };
        let warn = toast_body(ToastSeverity::Warning, "w");
        for _ in 0..MAX_TOASTS - 1 {
            c.on_toast(&warn, 0);
        }
        assert_eq!(c.active_toasts().len(), MAX_TOASTS);
        // At the cap: the oldest *expiring* toast goes, not the older error.
        let actions = c.on_toast(&warn, 0);
        assert!(matches!(
            actions.as_slice(),
            [
                ChromeAction::DismissToast { id },
                ChromeAction::ShowToast { .. }
            ] if *id == 1
        ));
        assert!(c.active_toasts().contains(&err));
        // Keep storming: the error survives every eviction.
        for _ in 0..MAX_TOASTS * 3 {
            c.on_toast(&warn, 0);
        }
        assert!(c.active_toasts().contains(&err));
        assert_eq!(c.active_toasts().len(), MAX_TOASTS);
    }

    /// When every live toast is an exempt `error`, the cap still has to bound the
    /// set — it falls back to oldest-overall rather than growing without bound.
    #[test]
    fn toast_at_cap_of_all_errors_evicts_oldest() {
        let mut c = core();
        let body = toast_body(ToastSeverity::Error, "e");
        for _ in 0..MAX_TOASTS {
            c.on_toast(&body, 0);
        }
        let actions = c.on_toast(&body, 0);
        assert!(matches!(
            actions.as_slice(),
            [
                ChromeAction::DismissToast { id: 0 },
                ChromeAction::ShowToast { .. }
            ]
        ));
        assert_eq!(c.active_toasts().len(), MAX_TOASTS);
    }

    /// The wake the DOM half parks is the soonest live expiry, and there is none
    /// to park while nothing live expires — the reason an idle page never wakes.
    #[test]
    fn the_wake_is_the_soonest_live_expiry_and_absent_while_nothing_expires() {
        let mut c = core();
        let wake = |c: &ChromeCore, now: u64| c.next_wake(now);
        assert_eq!(wake(&c, 0), None, "nothing live, nothing to wake for");
        // An error toast never expires, so it warrants no wake.
        c.on_toast(&toast_body(ToastSeverity::Error, "e"), 0);
        assert_eq!(wake(&c, 0), None);
        // A warning does. Shown at t=100, so it expires a TTL after that.
        let warn_id = match c
            .on_toast(&toast_body(ToastSeverity::Warning, "w"), 100)
            .as_slice()
        {
            [ChromeAction::ShowToast { id, .. }] => *id,
            other => panic!("expected a show, got {other:?}"),
        };
        assert_eq!(wake(&c, 100), Some(100 + TOAST_TTL_MS));
        // A second, later toast does not move the wake: the soonest expiry is
        // what the page must be awake for, and its activation re-parks the rest.
        c.on_toast(&toast_body(ToastSeverity::Info, "i"), 500);
        assert_eq!(wake(&c, 500), Some(100 + TOAST_TTL_MS));
        // Dismissing the soonest hands the wake to the next one.
        c.dismiss_toast(warn_id);
        assert_eq!(wake(&c, 500), Some(500 + TOAST_TTL_MS));
    }

    /// An expiry the sweep has not reached yet parks a wake at the activation's
    /// own instant, never behind it: a park in the past is a park the kernel
    /// releases immediately, and one arithmetic slip the other way would stop
    /// the page expiring toasts at all with nothing else noticing.
    #[test]
    fn an_overdue_expiry_wakes_now_rather_than_in_the_past() {
        let mut c = core();
        let wall = 1_770_000_000_000;
        c.on_toast(&toast_body(ToastSeverity::Info, "i"), wall);
        assert_eq!(c.next_wake(wall + 1_000), Some(wall + TOAST_TTL_MS));
        assert_eq!(
            c.next_wake(wall + TOAST_TTL_MS * 2),
            Some(wall + TOAST_TTL_MS * 2),
            "an overdue expiry wakes immediately"
        );
    }

    #[test]
    fn toast_at_cap_evicts_oldest() {
        let mut c = core();
        let body = toast_body(ToastSeverity::Info, "x");
        // Fill to the cap: ids 0..MAX_TOASTS, no eviction yet.
        for _ in 0..MAX_TOASTS {
            let actions = c.on_toast(&body, 0);
            assert!(matches!(
                actions.as_slice(),
                [ChromeAction::ShowToast { .. }]
            ));
        }
        assert_eq!(
            c.active_toasts(),
            (0..MAX_TOASTS as u64).collect::<Vec<_>>()
        );
        // The next toast evicts the oldest (id 0) before showing the new one.
        let actions = c.on_toast(&body, 0);
        assert!(matches!(
            actions.as_slice(),
            [
                ChromeAction::DismissToast { id: 0 },
                ChromeAction::ShowToast { id, .. },
            ] if *id == MAX_TOASTS as u64
        ));
        assert_eq!(
            c.active_toasts(),
            (1..=MAX_TOASTS as u64).collect::<Vec<_>>()
        );
    }

    #[test]
    fn tick_dismisses_expired_non_error_toasts_but_keeps_errors() {
        let mut c = core();
        // A warning at t=0 (expires at TOAST_TTL_MS) and an error at t=0 (never
        // expires).
        let warn_id = match c
            .on_toast(&toast_body(ToastSeverity::Warning, "w"), 0)
            .as_slice()
        {
            [ChromeAction::ShowToast { id, .. }] => *id,
            other => panic!("expected ShowToast, got {other:?}"),
        };
        let err_id = match c
            .on_toast(&toast_body(ToastSeverity::Error, "e"), 0)
            .as_slice()
        {
            [ChromeAction::ShowToast { id, .. }] => *id,
            other => panic!("expected ShowToast, got {other:?}"),
        };
        // Just before expiry: nothing dismissed.
        assert_eq!(c.tick(TOAST_TTL_MS - 1), Vec::new());
        assert_eq!(c.active_toasts(), vec![warn_id, err_id]);
        // At expiry: the warning dismisses, the error stays.
        assert_eq!(
            c.tick(TOAST_TTL_MS),
            vec![ChromeAction::DismissToast { id: warn_id }]
        );
        assert_eq!(c.active_toasts(), vec![err_id]);
        // A later tick leaves the error alone.
        assert_eq!(c.tick(TOAST_TTL_MS * 100), Vec::new());
        assert_eq!(c.active_toasts(), vec![err_id]);
    }

    /// The wake a tick consumes is not re-parked: once the expiring toast is
    /// gone there is nothing left to wake for, so the chain stops on its own
    /// rather than re-arming forever.
    #[test]
    fn an_expired_toast_leaves_no_wake_behind() {
        let mut c = core();
        c.on_toast(&toast_body(ToastSeverity::Warning, "w"), 0);
        assert_eq!(c.next_wake(0), Some(TOAST_TTL_MS));
        c.tick(TOAST_TTL_MS);
        assert_eq!(c.next_wake(TOAST_TTL_MS), None);
    }

    #[test]
    fn malformed_toast_reported() {
        let mut c = core();
        assert!(matches!(
            c.on_toast("nope", 0).as_slice(),
            [ChromeAction::Log { .. }]
        ));
        // The reserved channel constant is referenced so the payload's home is
        // documented alongside the parser under test.
        assert_eq!(LOCAL_TOAST_CHANNEL, "local:brenn/toast");
    }
}

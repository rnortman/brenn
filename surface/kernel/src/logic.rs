//! DOM-free kernel decision core.
//!
//! Pure state and transition logic over the surface client's control-plane
//! vocabulary. It holds no web-sys handles and compiles and unit-tests on the
//! host target; the wasm effect executor consumes the [`KernelAction`]s it
//! emits.

use std::collections::{BTreeSet, HashMap, HashSet};

use brenn_attach_proto::AlertSeverity;
use brenn_envelope::grants::ComponentGrant;

use crate::PublishStatus;
use crate::schema::bindings::BindingsDocument;
use crate::schema::telemetry::InstanceReport;
use crate::schema::{
    CONTROL_PLANE_VERSION, InstanceState, LOCAL_LINK_STATE_CHANNEL, LOCAL_SURFACE_STATE_CHANNEL,
    LinkState, LinkStateBody, LogLevel, SurfaceStateBody, SurfaceStateInstance,
};
use crate::session::Event;
use crate::{contract, schema};

/// Derive the surface's whole connect URL from the page's `location` and its
/// build id. `https:` is the only secure scheme, so it maps to `wss:`; every
/// other protocol (`http:`, `file:`, …) maps to `ws:`. `host` is
/// `location.host` (host + optional port), `slug` the surface slug.
///
/// The build id rides as the `build` query parameter: the served-asset lockstep
/// check is the surface route's own, not the attachment protocol's, so it is
/// composed into the URL here rather than appended by the connection. Pure
/// string logic, host-tested; the wasm entry point feeds it
/// `location.protocol()`/`location.host()`.
pub fn connect_url(protocol: &str, host: &str, slug: &str, build_id: &str) -> String {
    let scheme = if protocol == "https:" { "wss:" } else { "ws:" };
    format!(
        "{scheme}//{host}/surface/{slug}/ws?build={}",
        encode_query_component(build_id)
    )
}

/// Percent-encode a query-string component, encoding everything outside the
/// RFC 3986 unreserved set. A tiny helper rather than a dependency: build ids
/// are hex-ish and pass through unchanged, but arbitrary input stays safe.
fn encode_query_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(hex_upper(b >> 4));
                out.push(hex_upper(b & 0x0f));
            }
        }
    }
    out
}

/// One nibble as an uppercase hex digit.
fn hex_upper(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

/// The WIT entries a suppression breadcrumb names, so an operator reads the same
/// vocabulary the component's own world spells. The `dom`/`page-dom` family
/// names its own in `dom_host.rs`, where each entry is one function.
const LOG_ENTRY: &str = "log.log";
const ALERT_ENTRY: &str = "alert.alert";
const CONFIG_ENTRY: &str = "config.get";

impl KernelCore {
    /// Route a component's log intent to a component-log action, gated on that
    /// component's own `log` grant.
    ///
    /// `instance` is the identity the loader closed over when it instantiated the
    /// module — never anything the component states — so there is nothing to
    /// resolve and nothing to check it against.
    ///
    /// The gate is the instance's own right, read here off
    /// [`instance_grants`](Self::instance_grants) rather than taken from the
    /// caller: a seam states which instance is asking and this router decides
    /// what it may do, so no entry point can hand in a verdict of its own. An
    /// ungranted instance's log is dropped with a suppression breadcrumb naming
    /// it, never forwarded as a `Log` frame.
    ///
    /// `level` is the untrusted lowercase log-level wire string, parsed via
    /// [`proto::LogLevel::from_wire_str`]; an unrecognized one is
    /// dropped-and-reported as malformed rather than coerced into a well-formed
    /// `Log` frame, which would launder transpile-glue drift into a server log
    /// line at a level the component never chose.
    pub fn route_log(&self, instance: &str, level: &str, message: &str) -> KernelAction {
        if !self.instance_granted(instance, ComponentGrant::Log) {
            return ungranted_capability(instance, ComponentGrant::Log, LOG_ENTRY);
        }
        match LogLevel::from_wire_str(level) {
            Some(level) => KernelAction::ComponentLog {
                instance: instance.to_string(),
                level,
                message: message.to_string(),
            },
            None => KernelAction::Report {
                level: LogLevel::Warn,
                message: format!(
                    "dropped malformed {LOG_ENTRY} from processor {instance}: \
                     level must be a known log level"
                ),
                subject: Some(instance.to_string()),
            },
        }
    }

    /// Route a component's alert intent to an alert action, gated on that
    /// component's own `alert` grant.
    ///
    /// `instance` is the identity the loader closed over when it instantiated the
    /// module — never anything the component states — exactly as for
    /// [`Self::route_log`].
    ///
    /// The gate is the *instance's* right, read here off its own grants
    /// ([`instance_grants`](Self::instance_grants)) rather than taken from the
    /// caller — and not the surface's. The surface grant is
    /// the transport right toward the backend and still gates the kernel's own
    /// alerts; this one is containment within the page, so one component's paging
    /// bug cannot spend the plane in another's name. An ungranted instance's
    /// well-formed alert is dropped with a `Report` suppression breadcrumb naming
    /// it: a conforming kernel never emits an alert the server would judge a
    /// violation. The component's own logs are unaffected.
    ///
    /// `severity` is the untrusted lowercase severity wire string, parsed via
    /// [`AlertSeverity::from_wire_str`]; an unrecognized one is
    /// dropped-and-reported as malformed rather than coerced into a well-formed
    /// `Alert`.
    pub fn route_alert(
        &self,
        instance: &str,
        severity: &str,
        title: &str,
        body: &str,
    ) -> KernelAction {
        if !self.instance_granted(instance, ComponentGrant::Alert) {
            return ungranted_capability(instance, ComponentGrant::Alert, ALERT_ENTRY);
        }
        match AlertSeverity::from_wire_str(severity) {
            Some(severity) => KernelAction::Alert {
                attribution: Some(instance.to_string()),
                severity,
                title: title.to_string(),
                body: body.to_string(),
            },
            None => KernelAction::Report {
                level: LogLevel::Warn,
                message: format!(
                    "dropped malformed {ALERT_ENTRY} from processor {instance}: \
                     severity must be a known severity"
                ),
                subject: Some(instance.to_string()),
            },
        }
    }
}

/// The drop-and-report for a privileged entry an instance's grants do not admit.
///
/// One shape for every capability, because the verdict is one verdict: the
/// component asked for something deny-by-default it was not given, and the page
/// says so and does nothing. A conforming kernel never forwards past this — the
/// backend judges the same grant a second time, and a frame that arrives anyway
/// means the kernel was bypassed.
pub fn ungranted_capability(instance: &str, grant: ComponentGrant, what: &str) -> KernelAction {
    KernelAction::Report {
        level: LogLevel::Warn,
        message: format!(
            "suppressed {what} from component {instance}: it is not granted the {} capability",
            grant.word()
        ),
        subject: Some(instance.to_string()),
    }
}

/// The WIT `publish-error` name for a refused buffered publish, as an owned
/// `String`.
///
/// The vocabulary is [`contract::publish_status_str`]. Lives here rather than in
/// the wasm-only entry wrapper because that wrapper cannot be tested on the host.
pub fn publish_error_str(err: contract::PublishError) -> String {
    contract::publish_status_str(Err(err)).to_string()
}

/// The WIT `defer-error` name for a refused buffered control op (cancel / edit),
/// the twin of [`publish_error_str`] for the deferred-message family.
pub fn defer_error_str(err: contract::DeferError) -> String {
    contract::defer_status_str(Err(err)).to_string()
}

/// What the kernel's pre-chrome connect indicator currently shows. This is the
/// second of the two pixel classes the kernel renders itself (the first is the
/// error card): a minimal element shown before chrome owns connection pixels,
/// driven by the kernel's own link state. Removed for good the moment chrome
/// first mounts (or, on a chrome-less surface, at the first `Connected`), never
/// re-rendered for the page's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectIndicatorState {
    /// The initial connection attempt is in flight.
    Connecting,
    /// A live connection dropped; the kernel is reconnecting via backoff.
    Reconnecting,
    /// A fatal connection error arrived before chrome took over the connection
    /// pixels. Terminal: a dead end, not a spinner — the indicator stays on
    /// screen with static error styling so a pre-chrome fatal is not mistaken
    /// for a slow connect.
    Failed,
}

/// An effect an executor must apply, in order — most by the DOM executor, but
/// [`KernelAction::AttachPort`] by the event loop's executor (a task spawn, not a
/// web-sys effect).
///
/// Not `Eq`: [`KernelAction::SendGeometry`] carries an `f64` device-pixel-ratio,
/// which has no total equality. `PartialEq` is retained.
#[derive(Debug, Clone, PartialEq)]
pub enum KernelAction {
    /// Render (or update the text of) the pre-chrome connect indicator. Emitted
    /// only while the indicator is still live; once removed it is never set
    /// again, so a stale reconnect can never re-render it.
    SetConnectIndicator(ConnectIndicatorState),
    /// Remove the pre-chrome connect indicator for good: chrome now owns
    /// connection pixels (or there is no chrome to hand off to). Idempotent at
    /// the DOM layer, but the core emits it exactly once.
    RemoveConnectIndicator,
    /// Publish one of the kernel's reserved `local:` control planes. `body` is
    /// the plane's JSON payload, already serialized: the payload shape *is* the
    /// contract, so it is fixed here in the DOM-free core rather than in the DOM
    /// executor, which only hands it to the client.
    PublishControl { channel: String, body: String },
    /// Ask the bootstrap to perform a capped page reload.
    RequestReload { reason: String },
    /// Replace the content of the instance's wrapper with an error card carrying
    /// `reason` (rendered as text). `kind` stamps the wrapper's `data-kind` for
    /// the case where the wrapper is created fresh here.
    ErrorCard {
        instance: String,
        kind: String,
        reason: String,
    },
    /// Create the plain host `div` a page-hosted processor instance's `dom.root`
    /// resolves to, inside that instance's kernel-owned wrapper.
    MountHost { instance: String, kind: String },
    /// Ask the bootstrap loader to bring up the named headless processor
    /// instances (dispatched as the `brenn-processor-start` seam event).
    ///
    /// Emitted once per page, from the mount plan that first sees wiring: the
    /// loader needs the config map and the component row the bindings document
    /// carries, and a second emission would ask it to instantiate instances it
    /// already registered (which `on_processor_register` would refuse as
    /// duplicates). Changed wiring reloads the page instead.
    StartProcessors { instances: Vec<String> },
    /// Dispatch `brenn-surface-ready` on `window` (first successful connect):
    /// the bootstrap resets its capped-reload counter on this signal.
    EmitReady,
    /// Log `message` to the browser console (at `level`) and forward it to the
    /// server as a leveled `log` frame. Covers the transient/component-fault
    /// breadcrumb class at `Warn` (a non-`Ok` publish outcome or a refused
    /// publish) and a component death at `Error`. The level is fixed at each call
    /// site, never derived.
    ///
    /// `subject` is the instance the report is *about*, which the executor sends
    /// as the report publish's `attribution` so the peer stamps it with that
    /// component's sub-identity. It is the report's subject, never its author: the
    /// kernel writes every one of these lines. Carrying it matters because a
    /// component looping on rejected publishes is exactly the flood whose reports
    /// must draw its own budget rather than the kernel's — a report about a
    /// component that goes out unattributed lets that component drain the bare
    /// surface identity's bucket and silence the kernel's own breadcrumbs.
    ///
    /// `None` only where no component is the subject: a kernel-internal breadcrumb,
    /// a layout-engine rejection, or an event whose target never resolved to a
    /// mounted instance (there is no instance to name, and guessing would
    /// misattribute).
    Report {
        level: LogLevel,
        message: String,
        subject: Option<String>,
    },
    /// Forward a component's `brenn-log` intent to the server as a `Log` frame,
    /// stamping `source = "component:<instance>"`. `level` is the component's
    /// call-site-fixed level (never derived by the kernel); the executor emits
    /// `handle.log(level, message, "component:<instance>")`.
    ComponentLog {
        instance: String,
        level: LogLevel,
        message: String,
    },
    /// Page an operator as an `Alert` frame.
    ///
    /// `attribution` names the component whose alert this is, or `None` when the
    /// kernel itself is speaking (a panic report is about a component but is not
    /// the component's own statement, and a component that panicked may hold no
    /// alert grant at all). `severity` is the call-site-fixed severity, never
    /// derived by the kernel. Emitted only for a principal that holds the right:
    /// a component's own alert needs its `alert` grant
    /// ([`KernelCore::route_alert`]), the kernel's needs the surface's
    /// ([`KernelCore::alert_granted`]); an ungranted one yields a `Report`
    /// suppression breadcrumb or nothing at all. The executor emits
    /// `handle.alert(attribution, severity, title, body)`.
    Alert {
        attribution: Option<String>,
        severity: AlertSeverity,
        title: String,
        body: String,
    },
    /// Report the current browser viewport to the peer (a best-effort geometry
    /// document via `SurfaceHandle::send_geometry`). Emitted by
    /// [`KernelCore::on_viewport_changed`] only when the viewport actually changed
    /// since the last report. `width`/`height` are CSS pixels;
    /// `device_pixel_ratio` is the display density.
    SendGeometry {
        width: u32,
        height: u32,
        device_pixel_ratio: f64,
    },
    /// Report the current per-instance mount status to the peer (a best-effort
    /// status document via `SurfaceHandle::send_status`). Emitted on the status
    /// interval ([`KernelCore::on_status_tick`]) and immediately on any
    /// transition into `failed`. `instances` is the raw per-instance fact set;
    /// the DOM executor fills page uptime and the lifetime counters it owns
    /// before handing the report over.
    SendStatus { instances: Vec<InstanceReport> },
}

/// What a page-hosted processor's registration attempt decided.
///
/// Carries the mount decision rather than leaving it to be re-derived: the
/// caller forwards it to the runner, so the host element this admission asks for
/// and the mount activation that draws into it rest on one reading of one grant.
pub struct ProcessorRegistration {
    /// Whether the caller may forward the entry to the page.
    pub admitted: bool,
    /// Whether the forwarded entry is owed its mount activation.
    pub mount: bool,
    /// What to apply, admitted or not.
    pub actions: Vec<KernelAction>,
}

impl ProcessorRegistration {
    /// A refusal: nothing forwarded, nothing mounted, the report applied.
    fn refused(actions: Vec<KernelAction>) -> Self {
        ProcessorRegistration {
            admitted: false,
            mount: false,
            actions,
        }
    }
}

/// A browser viewport reading, tracked for no-change suppression.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Geometry {
    width: u32,
    height: u32,
    device_pixel_ratio: f64,
}

/// One instance's mount status, the kernel's own record of what it commanded at
/// its mount/attach decision points plus the failures it later observed. Mapped
/// to a [`schema::InstanceReport`](crate::schema::InstanceReport) for
/// a status report.
#[derive(Debug, Clone, PartialEq)]
struct InstanceStatus {
    instance: String,
    kind: String,
    state: InstanceState,
    /// Short failure reason when `state` is `Failed`; `None` otherwise.
    reason: Option<String>,
    /// Delivery pumps attached to this instance's ports.
    ports_attached: u32,
}

/// What one component instance was granted, in the vocabulary the config layer,
/// the backend host and this kernel all read from one crate.
pub type GrantSet = BTreeSet<ComponentGrant>;

/// Why a bindings document was refused: what an operator reads, and the short
/// reason the reload request carries.
struct DocumentRefusal {
    message: String,
    reason: &'static str,
}

/// Parse every component entry's grant words, or refuse the document.
///
/// An unknown word is server/kernel build skew — the document was written by a
/// backend whose vocabulary this page's assets predate — and the page's answer is
/// to refuse the whole document rather than guess which capability was meant.
/// Guessing in either direction is wrong: dropping the word silently disables a
/// capability the operator granted, and admitting it grants one this build cannot
/// enforce.
///
/// A repeated instance id is refused on the same terms. The config layer refuses
/// duplicate instance names, so a document carrying one is a server bug; keeping
/// either entry would enforce one entry's grants while every other reader of this
/// list (the config map, the mount plan) took the first — one instance name
/// standing for two different components.
fn parse_instance_grants(
    bindings: &BindingsDocument,
) -> Result<HashMap<String, GrantSet>, DocumentRefusal> {
    let mut parsed = HashMap::with_capacity(bindings.components.len());
    for entry in &bindings.components {
        let mut grants = GrantSet::new();
        for word in &entry.grants {
            match ComponentGrant::parse(word) {
                Some(grant) => {
                    grants.insert(grant);
                }
                None => {
                    return Err(DocumentRefusal {
                        message: format!(
                            "refused the bindings document: {word:?} is not a capability this build knows, so this page's assets are older than the server's vocabulary"
                        ),
                        reason: "unknown component grant",
                    });
                }
            }
        }
        if parsed.insert(entry.instance.clone(), grants).is_some() {
            return Err(DocumentRefusal {
                message: format!(
                    "refused the bindings document: instance {:?} is declared twice, so no reader of it can say which component the name means",
                    entry.instance
                ),
                reason: "duplicate component instance",
            });
        }
    }
    Ok(parsed)
}

/// The kernel's DOM-free state and transition logic.
#[derive(Debug, Clone, PartialEq)]
pub struct KernelCore {
    /// The last link state published on `local:brenn/link-state`, for no-change
    /// suppression. Chrome renders the connection banner from this plane; the
    /// kernel (this core, the platform half) is the plane's sole producer.
    link_state: LinkState,
    /// The wiring from the first bindings document; `None` until the page is
    /// first configured. Set then, and consulted to distinguish a first connect
    /// from a reconnect.
    bindings: Option<BindingsDocument>,
    /// Whether this surface holds the alert grant, from the latest `Connected`.
    /// `false` until the first connect. The surface's transport right toward the
    /// backend, and so what gates the kernel's *own* alerts — the panic report
    /// most of all. A component's alert is gated on its own grant instead
    /// ([`KernelCore::instance_grants`]); a conforming kernel never sends an
    /// `Alert` either scope would refuse.
    alert_granted: bool,
    /// Instance → the capabilities its component entry declares, parsed once
    /// from the bindings document. Empty until the page is first configured, and
    /// the kernel's whole runtime enforcement input: a capability the operator
    /// did not write is one this page refuses to exercise, whatever the
    /// component asks for.
    instance_grants: HashMap<String, GrantSet>,
    /// The last geometry reported, for no-change suppression; `None` until the
    /// first `SendGeometry`. A resize that lands back on the same viewport (a
    /// device rotating and rotating back, a debounce coalescing a jitter) emits
    /// nothing.
    last_geometry: Option<Geometry>,
    /// Per-instance mount status in configured order, populated on first connect
    /// and mutated at the mount/attach/panic/terminal decision points. The body
    /// of a status report; the kernel reports these raw facts and the server
    /// derives the health summary.
    instances: Vec<InstanceStatus>,
    /// The instances that have handed the kernel an activation entry — the
    /// kernel's own registration gate.
    ///
    /// The kernel's `RegisterActivation` keeps a deliberate fail-fast bound: a
    /// duplicate or unknown registration panics the client core, which is the
    /// right backstop for a *kernel* bug and the wrong answer for a *component*
    /// bug. So the kernel never forwards a bad one — an in-page component
    /// dispatching a second registration, or one from an unmounted target, is a
    /// contained fault report, not a dead page.
    ///
    /// TODO(kernel-registration-gate-lifecycle): this set only ever grows — no
    /// unmount, error-card teardown, or binding removal clears it, and the kernel
    /// never calls `SurfaceHandle::deregister_activation`. Correct while an
    /// instance id is page-unique-forever (a layout change reloads the page). If
    /// an instance's element is ever torn down and a fresh element for the same
    /// id remounts within one page life, the gate rejects the remount's
    /// registration as a duplicate while the page still holds the old detached
    /// host's entry. Clearing must be wired with the kernel-driven instance-death
    /// path, distinguishing death (deregister + clear) from Phase-3 reparent
    /// (preserve delivery, never deregister).
    registered: HashSet<String>,
    /// The singleton chrome instance the bindings document names, or `None` when
    /// the surface declares no chrome. Read in exactly two places: the connect
    /// indicator handoff (this instance's first mount removes the indicator) and
    /// death-is-fatal (this instance dying reloads the page instead of showing an
    /// error card). No other path branches on it.
    chrome_instance: Option<String>,
    /// Whether the pre-chrome connect indicator is still live. True from
    /// construction (the kernel renders it at start, before any attachment) until
    /// the handoff removes it; once false it stays false for the page's life, so
    /// no reconnect ever re-renders it.
    connect_indicator_active: bool,
    /// Whether the loader has already been asked to bring up this page's
    /// processor instances. True from the first mount plan that saw any, for the
    /// page's life: instantiation is per page, not per connect.
    processors_started: bool,
}

impl KernelCore {
    /// A freshly constructed core: the initial connect attempt is in flight,
    /// so the link state is [`LinkState::Connecting`].
    pub fn new() -> Self {
        Self {
            link_state: LinkState::Connecting,
            bindings: None,
            alert_granted: false,
            instance_grants: HashMap::new(),
            last_geometry: None,
            instances: Vec::new(),
            registered: HashSet::new(),
            chrome_instance: None,
            connect_indicator_active: true,
            processors_started: false,
        }
    }

    /// Retire the connect indicator if it is still live, returning the removal
    /// action (or nothing if it is already gone). Idempotent for the caller.
    fn retire_connect_indicator(&mut self) -> Vec<KernelAction> {
        if self.connect_indicator_active {
            self.connect_indicator_active = false;
            vec![KernelAction::RemoveConnectIndicator]
        } else {
            Vec::new()
        }
    }

    /// Gate a headless processor instance's activation registration — the tag-free
    /// sibling of [`KernelCore::on_activation_register`].
    ///
    /// The DOM path cannot admit one: its gate resolves the instance from a mounted
    /// element, and a processor has no element and no tag. Instance identity here
    /// comes from the bootstrap loader's own closure — the loader instantiated the
    /// module for exactly one declared instance and names it — which is the same
    /// trust shape as the DOM path's executor-resolved instance: kernel-derived,
    /// never component-claimed.
    ///
    /// Returns the [`ProcessorRegistration`] the caller enacts: whether it may
    /// forward the entry, whether that entry is owed a mount activation, and the
    /// actions to apply. Mountability is decided here and nowhere else — it is
    /// the same fact as the host element this emits, and two readings of it could
    /// disagree into a host `div` nothing draws in or a mount call with no
    /// element under it.
    ///
    /// Refused, reported rather than forwarded into the client core's fail-fast
    /// bound, in two cases mirroring the DOM gate's refusal posture:
    ///
    /// - `instance` is not a declared `processor` entry in the stored bindings
    ///   (unknown, or a `dom` instance trying the headless door);
    /// - it already registered — never silently replaced.
    ///
    /// On admission the instance's row transitions `Pending → Mounted` (for a
    /// headless instance that *is* what mounted means) and a status report follows,
    /// so `surface-state` carries the transition.
    pub fn on_processor_register(&mut self, instance: &str) -> ProcessorRegistration {
        if !self.is_configured_instance(instance) {
            return ProcessorRegistration::refused(vec![KernelAction::Report {
                level: LogLevel::Warn,
                message: format!(
                    "dropped processor activation registration: {instance} is not a declared \
                     processor instance"
                ),
                subject: None,
            }]);
        }
        if !self.registered.insert(instance.to_string()) {
            return ProcessorRegistration::refused(vec![KernelAction::Report {
                level: LogLevel::Warn,
                message: format!(
                    "dropped duplicate processor activation registration: instance {instance} \
                     already registered an activation entry"
                ),
                subject: Some(instance.to_string()),
            }]);
        }
        // One read of the one fact: an instance holding `dom` draws, so it gets a
        // host element and the mount call that fills it.
        let mount = self.instance_granted(instance, ComponentGrant::Dom);
        // Chrome's first successful registration is the connect-indicator
        // handoff: from here chrome owns connection pixels via its banner, so the
        // kernel drops its indicator and never renders it again.
        let mut actions = if self.chrome_instance.as_deref() == Some(instance) {
            self.retire_connect_indicator()
        } else {
            Vec::new()
        };
        if let Some(status) = self.instances.iter_mut().find(|s| s.instance == instance) {
            // The host element must exist before the mount activation fires, so
            // `dom.root` resolves when the component's first call runs.
            if mount {
                actions.push(KernelAction::MountHost {
                    instance: instance.to_string(),
                    kind: status.kind.clone(),
                });
            }
            status.state = InstanceState::Mounted;
            status.reason = None;
        }
        actions.extend(self.instance_table_actions());
        ProcessorRegistration {
            admitted: true,
            mount,
            actions,
        }
    }

    /// Fail a processor instance the bootstrap loader could not bring up — a module
    /// import, `instantiate`, or registration failure.
    ///
    /// The headless counterpart of the mount plan's error card: there is no wrapper
    /// to card, so the `failed` status row plus its `surface-state` publish *is* the
    /// observable, alongside the death report. An unknown or already-identically-
    /// failed instance emits nothing (the `mark_instance_failed` no-op), so a
    /// loader that reports twice does not double-report.
    ///
    /// A currently-registered instance is *never* failed here. The loader reports
    /// `load_failed` for a refused registration, and one refusal reason is a
    /// duplicate — which means an earlier registration is live and delivering. A
    /// duplicated bring-up must not flip that live row to `Failed` (inverting the
    /// only observable a headless instance has); the inconsistency is reported
    /// instead, and the live row left telling the truth.
    pub fn on_processor_load_failed(&mut self, instance: &str, detail: &str) -> Vec<KernelAction> {
        if self.registered.contains(instance) {
            return vec![KernelAction::Report {
                level: LogLevel::Warn,
                message: format!(
                    "ignored processor load-failure for {instance} ({detail}): the instance is \
                     already registered and delivering — this is a duplicated or out-of-order \
                     bring-up, not a real failure of the live instance"
                ),
                subject: Some(instance.to_string()),
            }];
        }
        let reason = format!("processor load failed: {detail}");
        if !self.mark_instance_failed(instance, &reason) {
            return Vec::new();
        }
        let mut actions = vec![KernelAction::Report {
            level: LogLevel::Error,
            message: format!("processor instance {instance} failed to load: {detail}"),
            subject: Some(instance.to_string()),
        }];
        // A page with no layout engine is not a page to keep: the capped
        // bootstrap reload replaces the status row.
        if self.chrome_instance.as_deref() == Some(instance) {
            actions.push(KernelAction::RequestReload {
                reason: "chrome mount failed".to_string(),
            });
            return actions;
        }
        actions.extend(self.instance_table_actions());
        actions
    }

    /// The card a dead page-hosted instance is owed, if it renders at all.
    ///
    /// A `dom`-granted instance has a wrapper, so its death is visible where it
    /// was drawing: the card replaces its content. A headless instance has no
    /// wrapper, so its `failed` status row on `surface-state` is the whole
    /// observable.
    fn error_card_for_dead_instance(&self, instance: &str, reason: &str) -> Vec<KernelAction> {
        if !self.instance_granted(instance, ComponentGrant::Dom) {
            return Vec::new();
        }
        let Some(status) = self.instances.iter().find(|s| s.instance == instance) else {
            return Vec::new();
        };
        vec![KernelAction::ErrorCard {
            instance: instance.to_string(),
            kind: status.kind.clone(),
            reason: reason.to_string(),
        }]
    }

    /// Serve one config read for an instance from the map its
    /// component entry carries, gated on that instance's `config` grant.
    ///
    /// Fixed for the page's lifetime, matching the backend's process-lifetime map:
    /// a changed map arrives only as changed wiring, which reloads the page. A
    /// miss — unknown key, or an instance with no map — answers `Ok(None)`, which
    /// is the `config.get` import's own `option<string>` and not an error.
    ///
    /// `Err` is the ungranted instance's drop-and-report, returned rather than
    /// left to the caller so no seam can serve a read the grants do not admit:
    /// the two seams differ only in how they carry the absence answer back.
    pub fn component_config_get(
        &self,
        instance: &str,
        key: &str,
    ) -> Result<Option<String>, KernelAction> {
        if !self.instance_granted(instance, ComponentGrant::Config) {
            return Err(ungranted_capability(
                instance,
                ComponentGrant::Config,
                CONFIG_ENTRY,
            ));
        }
        Ok(self.bindings.as_ref().and_then(|bindings| {
            bindings
                .components
                .iter()
                .find(|c| c.instance == instance)?
                .config
                .get(key)
                .cloned()
        }))
    }

    /// Whether `instance` may reach the publish/defer family, and the refusal to
    /// report if it may not.
    ///
    /// The decision every entry of that family asks, held here rather than at the
    /// seams so it is one verdict with one breadcrumb — and so the answer is
    /// testable off the browser, which the seams themselves are not. `what` names
    /// the event family the DOM seam would have carried the call on, so an
    /// operator reads one vocabulary whichever ABI refused.
    ///
    /// The seam keeps only the answer its own ABI writes back: a status on the
    /// dispatching detail for a DOM event, the WIT error string for an export.
    pub fn component_ports_gate(&self, instance: &str, what: &str) -> Result<(), KernelAction> {
        if self.instance_granted(instance, ComponentGrant::Ports) {
            Ok(())
        } else {
            Err(ungranted_capability(instance, ComponentGrant::Ports, what))
        }
    }

    /// Whether `instance` has registered an activation entry. The gate's state,
    /// readable so a caller can assert on it.
    #[cfg(test)]
    pub fn is_registered(&self, instance: &str) -> bool {
        self.registered.contains(instance)
    }

    /// The current link state — the last value published on
    /// `local:brenn/link-state`.
    pub fn link_state(&self) -> &LinkState {
        &self.link_state
    }

    /// Whether the latest `Connected` advertised the alert grant. Read by the
    /// panic listener to gate the panic-path alert ([`on_component_panic`]) —
    /// the kernel's own paging. A component's `brenn-alert` forward is gated on
    /// that component's grant instead ([`KernelCore::instance_grants`]).
    ///
    /// This is a kernel-side shadow of the client core's grant, refreshed when the
    /// event loop folds `Connected` — one event-loop hop after the core itself
    /// flips its flag inside `on_welcome`. The **client core's gate is
    /// authoritative**: it drops any ungranted `Alert` before the wire, so this
    /// copy lagging by a hop can never produce an ungranted frame or a session
    /// kill. The only cost of the lag is a lost alert (or a misworded suppression
    /// breadcrumb) in the sub-second window between a reconnect that *changes* the
    /// grant and the `Connected` fold; it always fails closed.
    pub fn alert_granted(&self) -> bool {
        self.alert_granted
    }

    /// What `instance`'s component entry was granted — the per-instance
    /// enforcement input every privileged kernel entry consults.
    ///
    /// An instance the bindings document never declared holds nothing, which is
    /// the same answer a declared instance with an empty grants list gets:
    /// deny-by-default, and no existence oracle in the difference. Empty before
    /// the page is first configured, so a call that races the first `Connected`
    /// fails closed.
    pub fn instance_grants(&self, instance: &str) -> &GrantSet {
        static NONE: GrantSet = GrantSet::new();
        self.instance_grants.get(instance).unwrap_or(&NONE)
    }

    /// Whether `instance` holds `grant` — the question every privileged kernel
    /// entry asks of [`instance_grants`](Self::instance_grants), spelled once so a
    /// seam cannot ask it a different way.
    pub fn instance_granted(&self, instance: &str, grant: ComponentGrant) -> bool {
        self.instance_grants(instance).contains(&grant)
    }

    /// Fold one control-plane [`Event`] into the core, returning the actions
    /// the DOM executor must apply in order.
    pub fn on_event(&mut self, event: &Event) -> Vec<KernelAction> {
        match event {
            Event::Disconnected { .. } => {
                let mut actions = self.set_link_state(LinkState::Reconnecting);
                // Drive the pre-chrome indicator's own link state while it is
                // still live (before the handoff). After removal this is a no-op.
                if self.connect_indicator_active {
                    actions.push(KernelAction::SetConnectIndicator(
                        ConnectIndicatorState::Reconnecting,
                    ));
                }
                actions
            }
            Event::ReloadRequired { .. } => {
                let mut actions = self.set_link_state(LinkState::Reloading);
                actions.push(KernelAction::RequestReload {
                    reason: "stale build".to_string(),
                });
                actions
            }
            // The link-state plane carries no detail: chrome renders the banner
            // from the detail-free `{v, state}` payload. The server-supplied
            // fatal `detail` is therefore never on-screen, so keep it in the
            // diagnostic path — a `Report` breadcrumb consoles it (always) and
            // best-effort error-reports it — before the plane transition.
            Event::Fatal { detail } => {
                let mut actions = vec![KernelAction::Report {
                    level: LogLevel::Error,
                    message: format!("surface connection fatal: {detail}"),
                    subject: None,
                }];
                actions.extend(self.set_link_state(LinkState::Fatal));
                // Pre-chrome, the connect indicator is the only thing on screen;
                // drive it to its terminal failed state so a fatal that arrives
                // before chrome mounts reads as a dead end rather than a
                // perpetual "Connecting…". After the handoff this is a no-op —
                // chrome's banner (from the link-state plane) is the sole
                // post-mount fatal rendering.
                if self.connect_indicator_active {
                    actions.push(KernelAction::SetConnectIndicator(
                        ConnectIndicatorState::Failed,
                    ));
                }
                actions
            }
            // The two ends speak no common transport version. On a page the peer
            // itself served, that means stale assets, so it takes the capped
            // reload rather than the fatal banner — the reload is the only thing
            // that can heal it, and the bootstrap's cap bounds a genuine
            // incompatibility to a finite number of attempts. Both ranges are
            // reported first, since after the reload nothing remembers them.
            Event::Incompatible { ours, theirs } => {
                let mut actions = vec![KernelAction::Report {
                    level: LogLevel::Error,
                    message: format!(
                        "surface transport version mismatch: this build speaks {}..={}, \
                         the server speaks {}..={}",
                        ours.min, ours.max, theirs.min, theirs.max
                    ),
                    subject: None,
                }];
                actions.extend(self.set_link_state(LinkState::Reloading));
                actions.push(KernelAction::RequestReload {
                    reason: "protocol version".to_string(),
                });
                actions
            }
            Event::Connected {
                bindings,
                alert_granted,
                ..
            } => {
                self.alert_granted = *alert_granted;
                self.on_connected(bindings)
            }
            // The wiring in force changed under a running page: the components it
            // mounted and the ports it attached were built against a description
            // of this surface that no longer holds, and a page cannot re-wire
            // itself in place. The capped bootstrap reload is the only honest
            // answer.
            Event::WiringChanged => {
                let mut actions = self.set_link_state(LinkState::Reloading);
                actions.push(KernelAction::RequestReload {
                    reason: "bindings changed".to_string(),
                });
                actions
            }
            // A non-terminal activation error leaves the instance alive: nothing
            // to do here (the diagnostic is on the EventStream). A terminal
            // trap is contained per-instance for every component — except the
            // singleton chrome, whose death is fatal: there is no
            // layout engine left to continue with, so the kernel triggers the
            // capped bootstrap reload instead of an error card. Non-chrome
            // containment is unchanged.
            Event::ActivationFailed { .. } => Vec::new(),
            Event::InstanceFailed { instance, reason } => {
                if self.chrome_instance.as_deref() == Some(instance.as_str()) {
                    vec![KernelAction::RequestReload {
                        reason: "chrome died".to_string(),
                    }]
                } else {
                    self.error_card_for_dead_instance(instance, reason)
                }
            }
            Event::PublishResult {
                instance,
                port,
                correlation,
                status,
            } => match status {
                PublishStatus::Ok => Vec::new(),
                // Every non-`Ok` status shares the transient/component-fault
                // response (warn + report, not a kill). Listed
                // explicitly rather than a wildcard so a future `PublishStatus`
                // variant fails to compile here, forcing a conscious decision on
                // whether it warrants distinct handling.
                // The one non-`Ok` status that reports nothing here: a plane
                // guard refused the body, and the refusal already produced its
                // own attributed `OverlayStateRejected` report carrying the
                // reason. A second, thinner report about the same event doubles
                // the offender's error-channel traffic and says less. The
                // buffered publish path, which has no publisher to answer,
                // already emits exactly that one report — this keeps the two
                // paths at one report apiece.
                PublishStatus::Refused => Vec::new(),
                PublishStatus::RateLimited
                | PublishStatus::BodyTooLarge { .. }
                | PublishStatus::UnboundPort
                | PublishStatus::NotConnected
                | PublishStatus::ConnectionLost
                | PublishStatus::Failed => vec![KernelAction::Report {
                    level: LogLevel::Warn,
                    message: format!(
                        "publish #{correlation} on instance {instance} port {port} rejected: {status:?}"
                    ),
                    // The asynchronous twin of the synchronous reject report in
                    // `dom.rs`, and the one a real flood actually takes: the
                    // server answers `RateLimited` on the wire, not at the
                    // client-side gate. Attributed to the component whose publish
                    // was rejected, so a component looping on rejects draws down
                    // its own budget instead of the kernel's.
                    subject: Some(instance.clone()),
                }],
            },
            // Attributed to the publisher: a confined plane refused a body its
            // author wrote, so it is that component's (or its operator's) fault
            // and draws down its report budget, not the kernel's. The publisher
            // was answered at buffer time and told nothing since, so this is the
            // only account of the fault anyone gets.
            Event::PlaneRefused {
                instance,
                port,
                channel,
                reason,
            } => vec![KernelAction::Report {
                level: LogLevel::Error,
                message: format!("refused {instance}/{port} publish to {channel}: {reason}"),
                subject: Some(instance.clone()),
            }],
            Event::StragglerDiscarded {
                channel,
                seq,
                dropped,
                // Channel-level, not component-level: the straggler is a fact about a
                // subscription the kernel tore down, with no one component as subject.
            } => vec![KernelAction::Report {
                level: LogLevel::Debug,
                message: format!(
                    "discarded post-unsubscribe straggler on {channel} at seq {seq} (dropped: {dropped})"
                ),
                subject: None,
            }],
        }
    }

    /// First-connect handling: store the bindings, build the instance-status
    /// table, hand the loader this page's instances, publish the connected link
    /// state, and emit `EmitReady` **last**.
    ///
    /// Nothing mounts here. Every configured component is a page-hosted
    /// processor whose element, if it draws at all, is created when its
    /// registration is admitted ([`Self::on_processor_register`]), so a row
    /// starts `Pending` and this pass only records that it exists.
    ///
    /// `EmitReady` is ordered last on purpose: the bootstrap resets its
    /// capped-reload counter on it, so a panic anywhere in the application of
    /// these actions must increment the counter without an intervening reset —
    /// otherwise a deterministic failure reloads forever, never converging to the
    /// static failure message the cap guarantees.
    ///
    /// On a reconnect (a document already stored) the page has already reconciled
    /// its stores and resubscribed with resume, so this republishes the connected
    /// link state and nothing else. A document that differs from the one in force
    /// arrives as its own [`Event::WiringChanged`], which is what reloads.
    fn on_connected(&mut self, bindings: &BindingsDocument) -> Vec<KernelAction> {
        if self.bindings.is_some() {
            return self.set_link_state(LinkState::Connected);
        }
        // Vocabulary first: a document naming a capability this build cannot
        // enforce is refused whole, before a single instance is configured from
        // it. Stale assets are the only way it happens and the capped bootstrap
        // reload is the only thing that heals it — the same answer, for the same
        // reason, as a transport-version mismatch.
        let instance_grants = match parse_instance_grants(bindings) {
            Ok(parsed) => parsed,
            Err(refusal) => {
                let mut actions = vec![KernelAction::Report {
                    level: LogLevel::Error,
                    message: refusal.message,
                    subject: None,
                }];
                actions.extend(self.set_link_state(LinkState::Reloading));
                actions.push(KernelAction::RequestReload {
                    reason: refusal.reason.to_string(),
                });
                return actions;
            }
        };
        self.instance_grants = instance_grants;
        self.bindings = Some(bindings.clone());
        self.chrome_instance = if bindings.chrome_instance.is_empty() {
            None
        } else {
            Some(bindings.chrome_instance.clone())
        };
        // Rebuild the instance-status table from this bindings set: one row per
        // configured component, every one `Pending` until its own registration is
        // admitted. The table has no slot concept — an instance in no layout slot
        // is tracked identically to one that draws.
        self.instances = Vec::with_capacity(bindings.components.len());
        let mut actions = Vec::new();
        actions.extend(self.dark_overlay_instrument_report(bindings));
        // Every configured instance, for the ports count below and the loader's
        // start list. Ports are real whether or not the instance draws, so a
        // headless one must be counted or the status report would understate a
        // working surface.
        let mut configured: HashSet<&str> = HashSet::new();
        for entry in &bindings.components {
            // No element to check and no wrapper: the bootstrap loader
            // instantiates the transpiled module and registers the instance's
            // `receive`, the row sits `Pending` until `on_processor_register`
            // admits that registration, and it becomes `Failed` if the loader
            // reports the instantiation or registration failed.
            configured.insert(entry.instance.as_str());
            self.instances.push(InstanceStatus {
                instance: entry.instance.clone(),
                kind: entry.kind.clone(),
                state: InstanceState::Pending,
                reason: None,
                ports_attached: 0,
            });
        }
        // Count each configured instance's bound input ports for the status table.
        // Nothing is wired here: the kernel delivers off the instance's own
        // registration, which its loader makes after `instantiate`. A subscription
        // naming an instance absent from `components` is not counted — nothing
        // will ever register under that name, so nothing will ever be delivered.
        for binding in &bindings.subscriptions {
            if configured.contains(binding.instance.as_str())
                && let Some(status) = self
                    .instances
                    .iter_mut()
                    .find(|s| s.instance == binding.instance)
            {
                status.ports_attached += 1;
            }
        }
        // Hand the loader this page's processor instances, once. Ordered after the
        // status rows exist so a load failure reported straight back finds its row.
        if !configured.is_empty() && !self.processors_started {
            self.processors_started = true;
            let mut instances: Vec<String> = configured.iter().map(|i| (*i).to_string()).collect();
            // `configured` is a set, and the loader's report ordering is observable
            // in tests; sort so the plan is a function of the bindings alone.
            instances.sort();
            actions.push(KernelAction::StartProcessors { instances });
        }
        actions.extend(self.set_link_state(LinkState::Connected));
        // Publish the freshly built status once, so the retained status channel
        // carries this surface's real mount state right after connect (including
        // any module-missing failure) rather than waiting a full interval. Ordered
        // after the mount plan's error cards so the DOM executor's error counter is
        // current when it fills the report.
        actions.extend(self.instance_table_actions());
        // A booted surface always declares exactly one chrome (the singleton is a
        // boot-time panic), so this branch is a defensive fallback: with no chrome
        // to hand off to, nothing will ever mount to remove the indicator, so
        // retire it now. A chrome surface keeps the indicator until chrome's
        // registration is admitted (see `on_processor_register`).
        if self.chrome_instance.is_none() {
            actions.extend(self.retire_connect_indicator());
        }
        actions.push(KernelAction::EmitReady);
        actions
    }

    /// One warn when a surface that can take over the screen has no way to say
    /// so: chrome publishes its overlay transitions from inside its activation,
    /// and an unbound output port refuses that publish at buffer time, where the
    /// answer goes onto the dispatching event's detail and no report is drawn.
    /// The status document then reads `overlay: null` whether or not an overlay
    /// is held — a dark instrument that looks exactly like a healthy one, which
    /// is the failure this plane exists to end. Drawn at connect, so the gap is
    /// visible before an incident rather than during one.
    ///
    /// Only where some component is actually wired to the takeover plane:
    /// nothing else can hold an overlay, so an unbound overlay-state port on a
    /// page that never takes over is the correct configuration rather than a
    /// gap. Read off the bindings, which is where the capability now lives —
    /// binding the plane requires the binding component's own `takeover` grant,
    /// so a binding is the page's statement that overlays happen here.
    fn dark_overlay_instrument_report(&self, bindings: &BindingsDocument) -> Option<KernelAction> {
        let chrome = self.chrome_instance.as_deref()?;
        let takes_over = bindings
            .subscriptions
            .iter()
            .map(|b| b.channel.as_str())
            .chain(bindings.outputs.iter().map(|b| b.channel.as_str()))
            .any(|channel| channel == schema::LOCAL_TAKEOVER_CHANNEL);
        if !takes_over {
            return None;
        }
        let bound = bindings
            .outputs
            .iter()
            .any(|b| b.instance == chrome && b.channel == schema::LOCAL_OVERLAY_STATE_CHANNEL);
        if bound {
            return None;
        }
        Some(KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "chrome instance {chrome} has no {} output binding: overlay state is \
                 unreportable and the status document will read no-overlay while one is held",
                schema::LOCAL_OVERLAY_STATE_CHANNEL
            ),
            // The surface's own wiring, not a component's conduct: no instance
            // did anything wrong, and the operator who reads it owns the config.
            subject: None,
        })
    }

    /// Whether `instance` is a component this surface configured (from the stored
    /// wiring). `false` before the page is first configured.
    fn is_configured_instance(&self, instance: &str) -> bool {
        self.bindings
            .as_ref()
            .is_some_and(|b| b.components.iter().any(|c| c.instance == instance))
    }

    /// Set `instance`'s status-table row to `failed` with `reason`. Returns
    /// whether this was a transition (the row existed and was not already failed
    /// with the same reason), so a caller can emit an immediate status report only
    /// on a real change. A no-op for an unknown instance.
    fn mark_instance_failed(&mut self, instance: &str, reason: &str) -> bool {
        let Some(status) = self.instances.iter_mut().find(|s| s.instance == instance) else {
            return false;
        };
        if status.state == InstanceState::Failed && status.reason.as_deref() == Some(reason) {
            return false;
        }
        status.state = InstanceState::Failed;
        status.reason = Some(reason.to_string());
        true
    }

    /// The current status snapshot as a [`KernelAction::SendStatus`]. The DOM
    /// executor fills page uptime and the lifetime counters it owns; the core
    /// supplies only the per-instance fact set.
    fn status_action(&self) -> KernelAction {
        KernelAction::SendStatus {
            instances: self
                .instances
                .iter()
                .map(|s| InstanceReport {
                    instance: s.instance.clone(),
                    kind: s.kind.clone(),
                    state: s.state,
                    reason: s.reason.clone(),
                    ports_attached: s.ports_attached,
                })
                .collect(),
        }
    }

    /// Fold a debounced viewport reading into the core. Emits a
    /// [`KernelAction::SendGeometry`] only when the viewport changed since the last
    /// report; a no-change reading emits nothing.
    pub fn on_viewport_changed(
        &mut self,
        width: u32,
        height: u32,
        device_pixel_ratio: f64,
    ) -> Vec<KernelAction> {
        let geometry = Geometry {
            width,
            height,
            device_pixel_ratio,
        };
        if self.last_geometry == Some(geometry) {
            return Vec::new();
        }
        self.last_geometry = Some(geometry);
        vec![KernelAction::SendGeometry {
            width,
            height,
            device_pixel_ratio,
        }]
    }

    /// Emit the periodic status snapshot: a [`KernelAction::SendStatus`] carrying
    /// the current table.
    pub fn on_status_tick(&mut self) -> Vec<KernelAction> {
        vec![self.status_action()]
    }

    /// Record a terminal failure of `instance` (a terminal port event error-carded
    /// it) in the status table, emitting an immediate status report on a real
    /// transition, so a headless instance's terminal failure is reported like any
    /// other.
    pub fn note_instance_failed(&mut self, instance: &str, reason: &str) -> Vec<KernelAction> {
        if self.mark_instance_failed(instance, reason) {
            self.instance_table_actions()
        } else {
            Vec::new()
        }
    }

    /// Publish a link-state transition on `local:brenn/link-state`, suppressing a
    /// no-change republish. Chrome renders the connection banner from this plane;
    /// the kernel is its sole producer (`kernel_publish_only`).
    fn set_link_state(&mut self, state: LinkState) -> Vec<KernelAction> {
        if self.link_state == state {
            return Vec::new();
        }
        self.link_state = state;
        vec![self.link_state_action(state)]
    }

    /// A publish of `state` on `local:brenn/link-state`.
    fn link_state_action(&self, state: LinkState) -> KernelAction {
        control_action(
            LOCAL_LINK_STATE_CHANNEL,
            &LinkStateBody {
                v: CONTROL_PLANE_VERSION,
                state,
            },
        )
    }

    /// The actions for a change to the instance table: the `surface-state` plane
    /// and the status report.
    ///
    /// One helper for both so they cannot drift: they are two renderings of this
    /// core's single instance table — one for an operator reading the retained
    /// status document, one for whatever is arranging the page.
    fn instance_table_actions(&self) -> Vec<KernelAction> {
        let mut actions = vec![control_action(
            LOCAL_SURFACE_STATE_CHANNEL,
            &SurfaceStateBody {
                v: CONTROL_PLANE_VERSION,
                instances: self
                    .instances
                    .iter()
                    .map(|s| SurfaceStateInstance {
                        instance: s.instance.clone(),
                        kind: s.kind.clone(),
                        state: s.state,
                        reason: s.reason.clone(),
                    })
                    .collect(),
            },
        )];
        actions.push(self.status_action());
        actions
    }
}

/// A [`KernelAction::PublishControl`] carrying `body` serialized as JSON.
fn control_action<T: serde::Serialize>(channel: &str, body: &T) -> KernelAction {
    KernelAction::PublishControl {
        channel: channel.to_string(),
        // The bodies are closed types this crate owns, built from strings the
        // core already holds; serialization failure would be a bug in serde, not
        // a runtime condition.
        body: serde_json::to_string(body).expect("control-plane body serializes to JSON"),
    }
}

impl Default for KernelCore {
    fn default() -> Self {
        Self::new()
    }
}

// Excluded from the wasm32 target so the browser test binary carries no
// compiled-but-never-run libtest harness and its test count stays honest.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    // ── connect indicator + chrome-death-is-fatal (kernel pixel classes) ──

    /// A `Connected` naming `chrome_instance` as the singleton chrome. All
    /// components are otherwise the ordinary defined-element shape.
    fn connected_event_chrome(components: Vec<ComponentEntry>, chrome_instance: &str) -> Event {
        let mut bindings = document(components, vec![]);
        bindings.chrome_instance = chrome_instance.to_string();
        connected(bindings, false)
    }

    #[test]
    fn chrome_death_reloads_while_a_sibling_death_is_contained() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event_chrome(
            entries(&["chrome", "protobar"]),
            "chrome",
        ));

        let reload = core.on_event(&Event::InstanceFailed {
            instance: "chrome".to_string(),
            reason: "trap".to_string(),
        });
        assert_eq!(
            reload,
            vec![KernelAction::RequestReload {
                reason: "chrome died".to_string(),
            }],
            "chrome's death is fatal — reload, no error card"
        );

        let sibling = core.on_event(&Event::InstanceFailed {
            instance: "protobar".to_string(),
            reason: "trap".to_string(),
        });
        assert!(
            sibling.is_empty(),
            "a non-chrome death is contained per-instance, unchanged"
        );
    }

    #[test]
    fn connect_indicator_retired_on_chrome_registration() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event_chrome(
            entries(&["chrome", "protobar"]),
            "chrome",
        ));

        // A sibling's registration leaves the indicator alone.
        let sib = core.on_processor_register("protobar");
        assert!(
            !sib.actions.contains(&KernelAction::RemoveConnectIndicator),
            "a sibling's registration does not touch the indicator"
        );

        // Chrome's is the handoff: the indicator is removed once.
        let first = core.on_processor_register("chrome");
        assert!(
            first
                .actions
                .contains(&KernelAction::RemoveConnectIndicator),
            "chrome's registration retires the indicator"
        );
    }

    #[test]
    fn connect_indicator_retired_at_connect_on_a_chromeless_surface() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(entries(&["protobar"])));
        assert!(
            actions.contains(&KernelAction::RemoveConnectIndicator),
            "no chrome to hand off to — retire the indicator at connect"
        );
    }

    #[test]
    fn disconnected_drives_the_indicator_only_while_it_is_live() {
        let mut core = KernelCore::new();
        // Live indicator: a drop drives it to Reconnecting.
        let live = core.on_event(&Event::Disconnected {
            reason: DetachReason::LivenessTimeout,
        });
        assert!(
            live.contains(&KernelAction::SetConnectIndicator(
                ConnectIndicatorState::Reconnecting
            )),
            "a live indicator follows the kernel's link state"
        );

        // After a chrome-less connect retires it, a further drop is silent.
        core.on_event(&connected_event(entries(&["protobar"])));
        let after = core.on_event(&Event::Disconnected {
            reason: DetachReason::LivenessTimeout,
        });
        assert!(
            !after
                .iter()
                .any(|a| matches!(a, KernelAction::SetConnectIndicator(_))),
            "a retired indicator is never re-rendered"
        );
    }

    // ── the activation registration gate ──────────────────────────────────

    use brenn_attach_client::conn::DetachReason;
    use brenn_attach_proto::VersionRange;

    use crate::schema::bindings::{BINDINGS_DOCUMENT_VERSION, PlatformSection};
    use crate::schema::{Binding, ComponentEntry};
    use crate::{PublishStatus, Urgency};

    // ── shared builders ───────────────────────────────────────────────────

    /// One component instance whose id differs from its kind.
    fn entry(instance: &str, kind: &str) -> ComponentEntry {
        ComponentEntry {
            instance: instance.to_string(),
            kind: kind.to_string(),
            parked_batch_depth: 8,
            config: Default::default(),
            grants: vec![],
        }
    }

    /// The same entry, holding the capability words an operator wrote for it.
    fn granted_entry(instance: &str, kind: &str, grants: &[&str]) -> ComponentEntry {
        ComponentEntry {
            grants: grants.iter().map(|g| (*g).to_string()).collect(),
            ..entry(instance, kind)
        }
    }

    /// One instance per kind, its id defaulted to the kind (the single-instance
    /// shape the mount/reconnect tests share).
    fn entries(kinds: &[&str]) -> Vec<ComponentEntry> {
        kinds.iter().map(|k| entry(k, k)).collect()
    }

    fn binding(channel: &str, instance: &str, port: &str) -> Binding {
        Binding {
            channel: channel.to_string(),
            instance: instance.to_string(),
            port: port.to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: brenn_surface_schema::NoiseLevel::Silent,
        }
    }

    /// The document these tests wire a core with. `chrome_instance` is empty by
    /// default — the chromeless shape most of them want; the chrome fixtures fill
    /// it in.
    fn document(components: Vec<ComponentEntry>, subscriptions: Vec<Binding>) -> BindingsDocument {
        BindingsDocument {
            v: BINDINGS_DOCUMENT_VERSION,
            components,
            subscriptions,
            outputs: vec![],
            local_channels: vec![],
            chrome_instance: String::new(),
            platform: PlatformSection {
                geometry_channel: "brenn:site.surface.deskbar.geometry".to_string(),
                status_channel: "brenn:site.surface.deskbar.status".to_string(),
                status_interval_secs: 60,
                error_channel: None,
                error_report_floor: None,
            },
        }
    }

    /// The attachment facts around a document. Everything the surface layer folds
    /// from `Connected` other than the wiring itself.
    fn connected(bindings: BindingsDocument, alert_granted: bool) -> Event {
        Event::Connected {
            bindings,
            participant_id: "surface:deskbar".to_string(),
            session_id: "sess-1".to_string(),
            max_body_bytes: 65_536,
            alert_granted,
        }
    }

    fn connected_event(components: Vec<ComponentEntry>) -> Event {
        connected_event_full(components, vec![])
    }

    fn connected_event_full(components: Vec<ComponentEntry>, subscriptions: Vec<Binding>) -> Event {
        connected_event_granted(components, subscriptions, false)
    }

    fn connected_event_granted(
        components: Vec<ComponentEntry>,
        subscriptions: Vec<Binding>,
        alert_granted: bool,
    ) -> Event {
        connected(document(components, subscriptions), alert_granted)
    }

    /// A connected core carrying `components` (element defined for all) and the
    /// given grant — the fixture the panic tests build on, since
    /// `on_component_panic` reads stored bindings + the grant.
    /// A configured core with every instance's registration already admitted —
    /// the steady state a test about a running surface wants, since a row is
    /// `Pending` until its own module registers.
    fn connect(components: Vec<ComponentEntry>, alert_granted: bool) -> KernelCore {
        let mut core = KernelCore::new();
        let instances: Vec<String> = components.iter().map(|c| c.instance.clone()).collect();
        core.on_event(&connected_event_granted(components, vec![], alert_granted));
        for instance in &instances {
            core.on_processor_register(instance);
        }
        core
    }

    /// A configured page whose `p1` (dom) and `counter-a` (headless) hold every
    /// capability a router gates on — the granted side of the router suites,
    /// which are about routing rather than about the gate.
    fn routing_core() -> KernelCore {
        connect(
            vec![
                granted_entry("p1", "protobar", &["log", "alert"]),
                granted_entry("counter-a", "counter", &["log", "alert"]),
            ],
            true,
        )
    }

    /// The same two instances, declared and granted nothing — the ungranted side.
    fn ungranted_core() -> KernelCore {
        connect(
            vec![entry("p1", "protobar"), entry("counter-a", "counter")],
            true,
        )
    }

    // ── connect_url ───────────────────────────────────────────────────────

    #[test]
    fn connect_url_https_maps_to_wss() {
        assert_eq!(
            connect_url("https:", "example.com:8443", "pfin", "abc123"),
            "wss://example.com:8443/surface/pfin/ws?build=abc123"
        );
    }

    #[test]
    fn connect_url_http_maps_to_ws() {
        assert_eq!(
            connect_url("http:", "localhost:3000", "graf", "abc123"),
            "ws://localhost:3000/surface/graf/ws?build=abc123"
        );
    }

    #[test]
    fn connect_url_non_http_protocol_maps_to_ws() {
        assert_eq!(
            connect_url("file:", "host", "slug", "abc123"),
            "ws://host/surface/slug/ws?build=abc123"
        );
    }

    #[test]
    fn connect_url_percent_encodes_the_build_id() {
        // Build ids are hex-ish in practice, so this pins the escape rather than
        // a real case: an id carrying a `&` or a space must not be able to add a
        // query parameter of its own.
        assert_eq!(
            connect_url("https:", "host", "slug", "a b&c=d/e~f.g-h_i"),
            "wss://host/surface/slug/ws?build=a%20b%26c%3Dd%2Fe~f.g-h_i"
        );
    }

    // ── route_publish_intent ──────────────────────────────────────────────

    // ── route_sync_intent ─────────────────────────────────────────────────

    // ── route_defer_intent ────────────────────────────────────────────────

    // ── route_log ─────────────────────────────────────────────────────────

    #[test]
    fn component_log_from_mounted_instance_forwards_with_instance() {
        let action = routing_core().route_log("p1", "warn", "hi");
        assert_eq!(
            action,
            KernelAction::ComponentLog {
                instance: "p1".to_string(),
                level: LogLevel::Warn,
                message: "hi".to_string(),
            }
        );
    }

    #[test]
    fn component_log_forwards_every_level() {
        let core = routing_core();
        for (wire, level) in [
            ("trace", LogLevel::Trace),
            ("debug", LogLevel::Debug),
            ("info", LogLevel::Info),
            ("warn", LogLevel::Warn),
            ("error", LogLevel::Error),
        ] {
            let action = core.route_log("p1", wire, "m");
            assert_eq!(
                action,
                KernelAction::ComponentLog {
                    instance: "p1".to_string(),
                    level,
                    message: "m".to_string(),
                }
            );
        }
    }

    #[test]
    fn component_log_at_an_unknown_level_is_dropped_as_malformed() {
        for level in ["fatal", "", "WARN"] {
            let action = routing_core().route_log("p1", level, "m");
            let KernelAction::Report {
                level: report_level,
                message: report_message,
                ..
            } = action
            else {
                panic!("expected Report for {level:?}, got {action:?}");
            };
            assert_eq!(report_level, LogLevel::Warn);
            assert!(report_message.contains("malformed"), "{report_message}");
            assert!(report_message.contains("processor p1"), "{report_message}");
        }
    }

    /// An ungranted instance's well-formed log is a breadcrumb, never a `Log`
    /// frame carrying its name.
    #[test]
    fn an_ungranted_instance_logs_nothing() {
        let action = ungranted_core().route_log("p1", "warn", "hi");
        let KernelAction::Report {
            level,
            message,
            subject,
        } = action
        else {
            panic!("expected a Report, got {action:?}");
        };
        assert_eq!(level, LogLevel::Warn);
        assert_eq!(subject.as_deref(), Some("p1"));
        assert!(message.contains("suppressed"), "message: {message}");
        assert!(message.contains("log capability"), "message: {message}");
    }

    /// The gate is read before the detail: an ungranted instance is told it is
    /// ungranted whatever it sent, rather than learning its detail was malformed
    /// from a capability it does not hold.
    #[test]
    fn the_log_grant_is_checked_ahead_of_the_detail() {
        let action = ungranted_core().route_log("p1", "shout", "m");
        assert!(matches!(
            action,
            KernelAction::Report { message, .. } if message.contains("not granted")
        ));
    }

    // ── route_config_get ──────────────────────────────────────────────────

    // ── per-instance grants ───────────────────────────────────────────────

    #[test]
    fn a_configured_instances_grants_are_read_off_its_component_entry() {
        let core = connect(
            vec![
                granted_entry("p1", "protobar", &["ports", "alert"]),
                granted_entry("p2", "protobar", &["ports"]),
            ],
            true,
        );
        assert!(core.instance_grants("p1").contains(&ComponentGrant::Alert));
        assert!(core.instance_grants("p1").contains(&ComponentGrant::Ports));
        assert!(
            !core.instance_grants("p2").contains(&ComponentGrant::Alert),
            "a sibling of the same kind holds only its own words"
        );
    }

    #[test]
    fn an_undeclared_instance_and_an_unconfigured_page_both_hold_nothing() {
        assert!(KernelCore::new().instance_grants("p1").is_empty());
        let core = connect(vec![granted_entry("p1", "protobar", &["alert"])], true);
        assert!(
            core.instance_grants("ghost").is_empty(),
            "deny-by-default, and no existence oracle in the difference"
        );
    }

    /// Build skew: the server's vocabulary has a word this page's assets predate.
    /// The page refuses the whole document and takes the capped reload rather than
    /// guessing which capability was meant.
    #[test]
    fn a_document_naming_an_unknown_capability_is_refused_whole() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event_granted(
            vec![granted_entry("p1", "protobar", &["ports", "telepathy"])],
            vec![],
            true,
        ));
        assert!(
            actions.iter().any(|a| matches!(
                a,
                KernelAction::RequestReload { reason } if reason == "unknown component grant"
            )),
            "expected a reload request, got {actions:?}"
        );
        assert!(
            actions.iter().any(|a| matches!(
                a,
                KernelAction::Report { level: LogLevel::Error, message, .. }
                    if message.contains("telepathy")
            )),
            "expected a report naming the word, got {actions:?}"
        );
        assert!(
            core.instance_grants("p1").is_empty(),
            "nothing from a refused document is in force"
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, KernelAction::StartProcessors { .. })),
            "no instance is configured from a refused document: {actions:?}"
        );
    }

    /// A server bug rather than skew, and refused on the same terms: with two
    /// entries under one name, the grants in force would come from one and the
    /// config map every other reader finds from the other.
    #[test]
    fn a_document_declaring_one_instance_twice_is_refused_whole() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event_granted(
            vec![
                granted_entry("p1", "protobar", &["ports"]),
                granted_entry("p1", "protobar", &["ports", "alert"]),
            ],
            vec![],
            true,
        ));
        assert!(
            actions.iter().any(|a| matches!(
                a,
                KernelAction::RequestReload { reason } if reason == "duplicate component instance"
            )),
            "expected a reload request, got {actions:?}"
        );
        assert!(
            actions.iter().any(|a| matches!(
                a,
                KernelAction::Report { level: LogLevel::Error, message, .. }
                    if message.contains("p1") && message.contains("declared twice")
            )),
            "expected a report naming the instance, got {actions:?}"
        );
        assert!(
            core.instance_grants("p1").is_empty(),
            "neither entry's grants are in force"
        );
    }

    // ── route_alert ───────────────────────────────────────────────────────

    #[test]
    fn component_alert_from_granted_mounted_instance_forwards_each_severity() {
        let core = routing_core();
        for (wire, severity) in [
            ("info", AlertSeverity::Info),
            ("warning", AlertSeverity::Warning),
            ("critical", AlertSeverity::Critical),
        ] {
            let action = core.route_alert("p1", wire, "t", "b");
            assert_eq!(
                action,
                KernelAction::Alert {
                    attribution: Some("p1".to_string()),
                    severity,
                    title: "t".to_string(),
                    body: "b".to_string(),
                }
            );
        }
    }

    #[test]
    fn component_alert_from_an_ungranted_instance_is_suppressed_with_breadcrumb() {
        let action = ungranted_core().route_alert("p1", "warning", "t", "b");
        let KernelAction::Report { level, message, .. } = action else {
            panic!("expected Report suppression breadcrumb, got {action:?}");
        };
        assert_eq!(level, LogLevel::Warn);
        assert!(message.contains("suppressed"), "message: {message}");
        assert!(message.contains("p1"), "message: {message}");
    }

    #[test]
    fn component_alert_at_an_unknown_severity_is_dropped_as_malformed() {
        let core = routing_core();
        for severity in ["warn", "", "WARNING"] {
            let action = core.route_alert("p1", severity, "t", "b");
            let KernelAction::Report { level, message, .. } = action else {
                panic!("expected Report for {severity:?}, got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert!(message.contains("malformed"), "message: {message}");
            assert!(message.contains("processor p1"), "message: {message}");
        }
    }

    // ── on_component_panic ────────────────────────────────────────────────

    // ── banner / connect / reconnect ──────────────────────────────────────

    #[test]
    fn new_core_starts_connecting() {
        assert_eq!(KernelCore::new().link_state(), &LinkState::Connecting);
    }

    // ── the kernel's reserved local: control planes ──────────────────────

    #[test]
    fn every_link_state_transition_publishes_the_matching_plane_body() {
        // Scope: the plane body the core publishes per rung. Chrome renders the
        // banner from this plane; the kernel is its sole producer. Every rung,
        // because `connected` (the live state) is the one value that is not a
        // rename of its event and the rest must not drift past it. The terminal
        // rungs (`reloading`, `fatal`) route through to chrome even after the
        // client core goes terminal — the router's rings outlive the transition
        // (see the client core's terminal-state control-publish arm); this test
        // pins the body the kernel emits, and the client-core test pins that the
        // terminal publish still reaches a bound port.
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::Disconnected {
            reason: DetachReason::LivenessTimeout,
        });
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"reconnecting"}"#
        );

        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::ReloadRequired {
            server_build: "abc".to_string(),
        });
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"reloading"}"#
        );

        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::Fatal {
            detail: "bad frame".to_string(),
        });
        // No detail on the plane: the payload is fixed at `{v, state}` and a
        // consumer renders its own chrome. The `Event::Fatal` detail rides the
        // separate `Report` breadcrumb, not the plane.
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"fatal"}"#
        );

        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(entries(&["echo-stub"])));
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"connected"}"#
        );
    }

    #[test]
    fn an_unchanged_link_state_republishes_nothing() {
        // The plane is transition-driven, and its ring is depth 1: republishing an
        // identical state would be a redelivery to every bound port for no change.
        let mut core = KernelCore::new();
        let event = Event::Disconnected {
            reason: DetachReason::LivenessTimeout,
        };
        assert!(publishes_control(
            &core.on_event(&event),
            LOCAL_LINK_STATE_CHANNEL
        ));
        assert!(!publishes_control(
            &core.on_event(&event),
            LOCAL_LINK_STATE_CHANNEL
        ));
    }

    #[test]
    fn connect_publishes_the_instance_table_on_the_surface_state_plane() {
        // chrome learns the instance set from this plane and never by querying the
        // DOM — so the set must be complete at connect, before any module has
        // registered: chrome arranges a row it has not seen mount yet, and the
        // registration that follows republishes the table.
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(vec![
            entry("ok", "good"),
            entry("bad", "missing"),
        ]));
        assert_eq!(
            control_body(&actions, LOCAL_SURFACE_STATE_CHANNEL),
            r#"{"v":1,"instances":[{"instance":"ok","kind":"good","state":"pending"},{"instance":"bad","kind":"missing","state":"pending"}]}"#
        );
    }

    #[test]
    fn a_dead_instance_is_marked_alone_on_the_surface_state_plane() {
        // The plane mirrors the kernel's instance table. A trap is one instance's
        // death: p1 shows failed while its same-kind sibling p2 keeps running on
        // its own memory. A chrome that stopped arranging p2 here would be exactly
        // the false-death bug the one-subject model prevents.
        let mut core = connect(
            vec![entry("p1", "protobar"), entry("p2", "protobar")],
            false,
        );
        let actions = core.note_instance_failed("p1", "boom");
        let body = control_body(&actions, LOCAL_SURFACE_STATE_CHANNEL);
        assert!(
            body.contains(r#"{"instance":"p1","kind":"protobar","state":"failed""#),
            "{body}"
        );
        assert!(
            body.contains(r#"{"instance":"p2","kind":"protobar","state":"mounted""#),
            "{body}"
        );
    }

    #[test]
    fn disconnect_shows_reconnecting() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::Disconnected {
            reason: DetachReason::LivenessTimeout,
        });
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                // A fresh core's indicator is still live, so it follows the drop.
                KernelAction::SetConnectIndicator(ConnectIndicatorState::Reconnecting),
            ]
        );
        assert_eq!(core.link_state(), &LinkState::Reconnecting);
    }

    #[test]
    fn reload_required_publishes_reloading_and_requests_reload() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::ReloadRequired {
            server_build: "abc123".to_string(),
        });
        assert_eq!(
            without_platform_planes(&actions),
            vec![KernelAction::RequestReload {
                reason: "stale build".to_string()
            }]
        );
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"reloading"}"#
        );
        assert_eq!(core.link_state(), &LinkState::Reloading);
    }

    #[test]
    fn fatal_publishes_terminal_link_state_without_reload() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::Fatal {
            detail: "bad frame".to_string(),
        });
        // A fatal publishes the plane and does not reload; chrome draws the
        // terminal banner from the plane. The `Report` breadcrumb keeps the
        // server-supplied detail in the console/error-report path (it is
        // off-screen now the banner is gone). On a fresh core the pre-chrome
        // connect indicator is still live, so it is driven to its terminal
        // `Failed` state — the only pre-chrome fatal pixels there are.
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::Report {
                    level: LogLevel::Error,
                    message: "surface connection fatal: bad frame".to_string(),
                    subject: None,
                },
                KernelAction::SetConnectIndicator(ConnectIndicatorState::Failed),
            ]
        );
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"fatal"}"#
        );
        assert_eq!(core.link_state(), &LinkState::Fatal);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, KernelAction::RequestReload { .. }))
        );
    }

    #[test]
    fn fatal_after_handoff_leaves_the_connect_indicator_alone() {
        // Once chrome owns the connection pixels (the indicator was retired at
        // the first Connected), a fatal must not re-touch the indicator: chrome's
        // banner from the link-state plane is the sole post-mount fatal rendering.
        let mut core = KernelCore::new();
        core.on_event(&connected_event(entries(&["echo-stub"])));
        let actions = core.on_event(&Event::Fatal {
            detail: "bad frame".to_string(),
        });
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, KernelAction::SetConnectIndicator(_))),
            "no connect-indicator action after the chrome handoff"
        );
        assert_eq!(core.link_state(), &LinkState::Fatal);
    }

    #[test]
    fn connected_stores_alert_granted_from_welcome() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(entries(&["echo-stub"])));
        assert!(!core.alert_granted());

        let mut granted = KernelCore::new();
        granted.on_event(&connected_event_granted(
            entries(&["echo-stub"]),
            vec![],
            true,
        ));
        assert!(granted.alert_granted());
    }

    #[test]
    fn first_connect_starts_the_loader_and_publishes_connected() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(entries(&["echo-stub"])));
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::StartProcessors {
                    instances: vec!["echo-stub".to_string()],
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
        assert_eq!(core.link_state(), &LinkState::Connected);
    }

    #[test]
    fn a_configured_instance_is_pending_with_no_element() {
        // The whole shape of the wiring pass in one assertion set: no mount, no
        // error card, and a `Pending` row — the state that exists precisely because
        // an instance's wiring completes later, at registration.
        let mut core = KernelCore::new();
        let mut components = entries(&["protobar"]);
        components.push(entry("counter-a", "counter"));
        let actions = core.on_event(&connected_event(components));
        assert!(!actions.iter().any(|a| matches!(
            a,
            KernelAction::MountHost { instance, .. } | KernelAction::ErrorCard { instance, .. }
                if instance == "counter-a"
        )));
        let status = core
            .instances
            .iter()
            .find(|i| i.instance == "counter-a")
            .expect("a processor instance has a status row from bindings-build time");
        assert_eq!(status.state, InstanceState::Pending);
        assert_eq!(status.reason, None);
    }

    // ── grant-keyed mountability ──────────────────────────────────────────

    /// The `dom` grant is what makes a page-hosted instance render: its host
    /// element is created when its registration is admitted, and an instance
    /// without the grant gets none.
    #[test]
    fn the_dom_grant_is_what_earns_a_host_element_at_registration() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(vec![
            granted_entry("panel-a", "panel", &["dom"]),
            granted_entry("counter-a", "counter", &["config"]),
        ]));

        let ProcessorRegistration {
            admitted,
            mount,
            actions,
        } = core.on_processor_register("panel-a");
        assert!(admitted);
        assert!(
            actions.contains(&KernelAction::MountHost {
                instance: "panel-a".to_string(),
                kind: "panel".to_string(),
            }),
            "a dom-granted instance gets its host element, got {actions:?}"
        );
        assert!(mount, "and the mount call that draws into it");

        let ProcessorRegistration {
            admitted,
            mount,
            actions,
        } = core.on_processor_register("counter-a");
        assert!(admitted);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, KernelAction::MountHost { .. })),
            "an instance with no dom grant is headless, got {actions:?}"
        );
        assert!(!mount, "and is owed no mount call");
    }

    /// A page-hosted chrome is not judged at wiring time: it has no element to
    /// look for, and the mount plan must not read its absence as a failure.
    #[test]
    fn a_page_hosted_chrome_is_not_failed_by_the_mount_plan() {
        let mut core = KernelCore::new();
        let mut bindings = document(
            vec![granted_entry("chrome", "chrome", &["dom", "page-dom"])],
            vec![],
        );
        bindings.chrome_instance = "chrome".to_string();
        let actions = core.on_event(&connected(bindings, false));
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, KernelAction::RequestReload { .. })),
            "no element is expected of a page-hosted chrome yet, got {actions:?}"
        );
    }

    /// A page-hosted chrome the loader could not bring up is fatal: there is no
    /// layout engine to carry on with, so the capped bootstrap reload fires.
    #[test]
    fn a_page_hosted_chrome_that_never_loads_reloads_the_page() {
        let mut core = KernelCore::new();
        let mut bindings = document(
            vec![
                granted_entry("chrome", "chrome", &["dom", "page-dom"]),
                granted_entry("panel-a", "panel", &["dom"]),
            ],
            vec![],
        );
        bindings.chrome_instance = "chrome".to_string();
        core.on_event(&connected(bindings, false));

        let actions = core.on_processor_load_failed("chrome", "instantiate threw");
        assert!(
            actions.contains(&KernelAction::RequestReload {
                reason: "chrome mount failed".to_string(),
            }),
            "chrome's bring-up failure is fatal for the page, got {actions:?}"
        );

        let sibling = core.on_processor_load_failed("panel-a", "instantiate threw");
        assert!(
            !sibling
                .iter()
                .any(|a| matches!(a, KernelAction::RequestReload { .. })),
            "a sibling's bring-up failure is contained, got {sibling:?}"
        );
    }

    /// A rendering instance that dies — a trap in its mount activation or in any
    /// later one — is carded where it was drawing. A headless one has no wrapper
    /// and no pixels, so its status row is the whole observable.
    #[test]
    fn a_dead_rendering_instance_is_carded_and_a_headless_one_is_not() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(vec![
            granted_entry("panel-a", "panel", &["dom"]),
            granted_entry("counter-a", "counter", &["config"]),
        ]));

        let carded = core.on_event(&Event::InstanceFailed {
            instance: "panel-a".to_string(),
            reason: "trapped building its UI".to_string(),
        });
        assert_eq!(
            carded,
            vec![KernelAction::ErrorCard {
                instance: "panel-a".to_string(),
                kind: "panel".to_string(),
                reason: "trapped building its UI".to_string(),
            }]
        );

        let headless = core.on_event(&Event::InstanceFailed {
            instance: "counter-a".to_string(),
            reason: "trapped".to_string(),
        });
        assert!(
            headless.is_empty(),
            "nothing to card for an instance that never drew, got {headless:?}"
        );
    }

    #[test]
    fn processor_instances_are_handed_to_the_loader_once_per_page() {
        let mut core = KernelCore::new();
        let components = vec![entry("counter-b", "counter"), entry("counter-a", "counter")];
        let actions = core.on_event(&connected_event(components.clone()));
        let named: Vec<&Vec<String>> = actions
            .iter()
            .filter_map(|a| match a {
                KernelAction::StartProcessors { instances } => Some(instances),
                _ => None,
            })
            .collect();
        assert_eq!(
            named,
            vec![&vec!["counter-a".to_string(), "counter-b".to_string()]],
            "every headless instance, named once, in a bindings-determined order"
        );

        // A reconnect re-runs the mount plan, but instantiation is per page: a
        // second ask would have the loader re-instantiate live instances, and
        // `on_processor_register` would refuse each as a duplicate.
        let again = core.on_event(&connected_event(components));
        assert!(
            !again
                .iter()
                .any(|a| matches!(a, KernelAction::StartProcessors { .. }))
        );
    }

    #[test]
    fn processor_register_admits_once_and_mounts_the_row() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(vec![entry("counter-a", "counter")]));

        let ProcessorRegistration {
            admitted, actions, ..
        } = core.on_processor_register("counter-a");
        assert!(admitted, "the first registration is admitted");
        assert!(core.is_registered("counter-a"));
        assert_eq!(
            core.instances[0].state,
            InstanceState::Mounted,
            "for a headless instance, registered *is* mounted"
        );
        // The status row is the only observable a headless instance has, so the
        // transition must reach `surface-state` — nothing else would show it.
        assert!(publishes_control(&actions, LOCAL_SURFACE_STATE_CHANNEL));

        // A second registration is refused and reported, never silently replacing
        // the live delivery seam.
        let ProcessorRegistration {
            admitted, actions, ..
        } = core.on_processor_register("counter-a");
        assert!(!admitted);
        assert!(matches!(
            &actions[..],
            [KernelAction::Report { message, subject: Some(s), .. }]
                if message.contains("duplicate") && s == "counter-a"
        ));
        assert_eq!(core.instances[0].state, InstanceState::Mounted);
    }

    #[test]
    fn processor_register_refuses_an_undeclared_instance() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(entries(&["protobar"])));

        // Not declared at all.
        let ProcessorRegistration {
            admitted, actions, ..
        } = core.on_processor_register("ghost");
        assert!(!admitted);
        assert!(matches!(
            &actions[..],
            [KernelAction::Report { message, .. }] if message.contains("not a declared processor")
        ));
    }

    #[test]
    fn processor_load_failure_fails_the_row_once_with_a_death_report() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(vec![
            entry("counter-a", "counter"),
            entry("counter-b", "counter"),
        ]));

        let actions = core.on_processor_load_failed("counter-a", "instantiate threw");
        assert!(actions.iter().any(|a| matches!(
            a,
            KernelAction::Report { subject: Some(s), message, .. }
                if s == "counter-a" && message.contains("instantiate threw")
        )));
        assert!(publishes_control(&actions, LOCAL_SURFACE_STATE_CHANNEL));
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, KernelAction::ErrorCard { .. })),
            "a headless instance has no wrapper to card"
        );
        assert_eq!(core.instances[0].state, InstanceState::Failed);
        // Sibling isolation: one instantiation failing says nothing about the
        // other instance of the same kind, which has its own linear memory.
        assert_eq!(core.instances[1].state, InstanceState::Pending);

        // A loader that reports the same failure twice does not double-report.
        assert!(
            core.on_processor_load_failed("counter-a", "instantiate threw")
                .is_empty()
        );
    }

    #[test]
    fn processor_load_failure_never_fails_a_live_registered_row() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(vec![entry("counter-a", "counter")]));

        // The instance is registered and delivering.
        let admitted = core.on_processor_register("counter-a").admitted;
        assert!(admitted);
        assert_eq!(core.instances[0].state, InstanceState::Mounted);

        // A duplicated bring-up refuses the second registration and reports
        // `load_failed`; that must not invert the live row's status.
        let actions = core.on_processor_load_failed("counter-a", "activation registration refused");
        assert_eq!(
            core.instances[0].state,
            InstanceState::Mounted,
            "a load-failure for an already-registered instance must not flip the live row"
        );
        assert!(
            !publishes_control(&actions, LOCAL_SURFACE_STATE_CHANNEL),
            "no surface-state churn for an ignored spurious failure"
        );
        assert!(matches!(
            &actions[..],
            [KernelAction::Report { subject: Some(s), message, level: LogLevel::Warn }]
                if s == "counter-a" && message.contains("already registered and delivering")
        ));
    }

    #[test]
    fn processor_ports_are_counted_into_ports_attached() {
        // The counting gate was `dom`-mount-keyed; a headless instance's ports are
        // just as real, and an uncounted one would report a working surface as
        // having nothing attached.
        let mut core = KernelCore::new();
        core.on_event(&connected_event_full(
            vec![entry("counter-a", "counter")],
            vec![
                binding("brenn:ticks", "counter-a", "ticks"),
                binding("brenn:other", "counter-a", "other"),
            ],
        ));
        assert_eq!(core.instances[0].ports_attached, 2);
    }

    /// One config map, either ABI. The map is the component entry's; nothing
    /// about the DOM decides whether it can be read, so a `dom` instance holding
    /// `config` reads its own exactly as a headless one does.
    #[test]
    fn config_get_answers_from_welcome_for_both_abis_and_misses_are_none() {
        let mut core = KernelCore::new();
        let mut headless = entry("counter-a", "counter");
        headless
            .config
            .insert("mode".to_string(), "loud".to_string());
        headless.grants = vec!["config".to_string()];
        let mut placed = granted_entry("p1", "protobar", &["config"]);
        placed
            .config
            .insert("mode".to_string(), "quiet".to_string());
        let mut sibling = entry("counter-b", "counter");
        sibling.grants = vec!["config".to_string()];
        core.on_event(&connected_event(vec![headless, placed, sibling]));

        assert_eq!(
            core.component_config_get("counter-a", "mode"),
            Ok(Some("loud".to_string()))
        );
        assert_eq!(
            core.component_config_get("p1", "mode"),
            Ok(Some("quiet".to_string()))
        );
        assert_eq!(core.component_config_get("counter-a", "absent"), Ok(None));
        // Per-instance, not per-kind: a sibling of the same kind has its own map.
        assert_eq!(core.component_config_get("counter-b", "mode"), Ok(None));
    }

    /// The gate is the serving path's own, so no seam can serve a read the
    /// grants do not admit — including a read naming an instance the document
    /// never declared, which holds nothing.
    #[test]
    fn an_ungranted_instance_reads_no_config() {
        let mut core = KernelCore::new();
        let mut entry = entry("counter-a", "counter");
        entry.config.insert("mode".to_string(), "loud".to_string());
        core.on_event(&connected_event(vec![entry]));

        for instance in ["counter-a", "ghost"] {
            let Err(KernelAction::Report {
                level,
                message,
                subject,
            }) = core.component_config_get(instance, "mode")
            else {
                panic!("expected a refusal for {instance}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert_eq!(subject.as_deref(), Some(instance));
            assert!(message.contains("config capability"), "{message}");
        }
    }

    /// Every entry of the publish/defer family asks one question of one
    /// authority. The four entries themselves are browser-only, but the decision
    /// they share is not: an instance granted `ports` is admitted whichever entry
    /// it arrives on, and one that is not — declared or not — is refused with a
    /// breadcrumb naming the entry and the capability.
    #[test]
    fn every_ports_entry_shares_one_verdict() {
        let granted = connect(vec![granted_entry("p1", "protobar", &["ports"])], true);
        let ungranted = ungranted_core();
        // The kernel export, and the WIT entry it asks under.
        let seams = [
            ("brenn_processor_publish", "ports.publish"),
            ("brenn_processor_publish_deferred", "ports.publish-deferred"),
            ("brenn_processor_defer_cancel", "ports.defer-cancel"),
            ("brenn_processor_defer_edit", "ports.defer-edit"),
        ];
        for (seam, what) in seams {
            assert_eq!(
                granted.component_ports_gate("p1", what),
                Ok(()),
                "{seam} refused a granted instance"
            );
            for instance in ["p1", "ghost"] {
                let Err(KernelAction::Report {
                    level,
                    message,
                    subject,
                }) = ungranted.component_ports_gate(instance, what)
                else {
                    panic!("{seam} admitted {instance}, which holds no ports grant");
                };
                assert_eq!(level, LogLevel::Warn);
                assert_eq!(subject.as_deref(), Some(instance));
                assert!(message.contains(what), "{seam}: {message}");
                assert!(message.contains("ports capability"), "{seam}: {message}");
            }
        }
    }

    #[test]
    fn processor_log_and_alert_route_without_an_element() {
        let core = routing_core();
        assert_eq!(
            core.route_log("counter-a", "warn", "hi"),
            KernelAction::ComponentLog {
                instance: "counter-a".to_string(),
                level: LogLevel::Warn,
                message: "hi".to_string(),
            }
        );
        assert!(matches!(
            core.route_log("counter-a", "shout", "hi"),
            KernelAction::Report { .. }
        ));

        assert_eq!(
            core.route_alert("counter-a", "warning", "t", "b"),
            KernelAction::Alert {
                attribution: Some("counter-a".to_string()),
                severity: AlertSeverity::Warning,
                title: "t".to_string(),
                body: "b".to_string(),
            }
        );
        // Ungranted: a suppression breadcrumb, never an `Alert` the server would
        // treat as a protocol violation.
        assert!(matches!(
            ungranted_core().route_alert("counter-a", "warning", "t", "b"),
            KernelAction::Report { message, .. } if message.contains("suppressed")
        ));
        assert!(matches!(
            core.route_alert("counter-a", "loud", "t", "b"),
            KernelAction::Report { .. }
        ));
    }

    /// Each delegation must carry the error through: a helper that lost the
    /// `Err` would answer `"ok"` and tell a refused component it succeeded.
    /// Individual spellings are pinned by the contract's own round-trip test.
    #[test]
    fn the_error_str_helpers_carry_the_error_into_the_contract_vocabulary() {
        use contract::{DeferError, PublishError};
        assert_eq!(
            publish_error_str(PublishError::InvalidPayload),
            "invalid-payload"
        );
        assert_eq!(
            defer_error_str(DeferError::InvalidDeliverAfter),
            "invalid-deliver-after"
        );
    }

    #[test]
    fn first_connect_with_no_components_still_emits_ready_and_publishes_connected() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(vec![]));
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
        assert_eq!(core.link_state(), &LinkState::Connected);
    }

    #[test]
    fn first_connect_counts_an_instances_subscription() {
        // A configured instance with a bound subscription starts the loader and
        // nothing else — the registration model wires no pump; the subscription is
        // only counted into the status table's `ports_attached`.
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event_full(
            entries(&["echo-stub"]),
            vec![binding("ephemeral:dev-stub", "echo-stub", "messages")],
        ));
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::StartProcessors {
                    instances: vec!["echo-stub".to_string()],
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
        assert_eq!(core.instances[0].ports_attached, 1);
    }

    #[test]
    fn first_connect_counts_subscriptions_per_instance() {
        // Two protobar instances, each with its own channels: each instance's bound
        // input ports are counted into `ports_attached`, keyed on instance. No
        // attach action is emitted — the component registers itself.
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event_full(
            vec![entry("p1", "protobar"), entry("p2", "protobar")],
            vec![
                binding("ephemeral:one", "p2", "in"),
                binding("ephemeral:two", "p1", "feed"),
                binding("ephemeral:three", "p2", "aux"),
            ],
        ));
        let counts: Vec<(&str, u32)> = core
            .instances
            .iter()
            .map(|s| (s.instance.as_str(), s.ports_attached))
            .collect();
        assert_eq!(counts, vec![("p1", 1u32), ("p2", 2u32)]);
        // The plan is one loader start naming both, plus the indicator/ready tail
        // — no per-subscription action.
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::StartProcessors {
                    instances: vec!["p1".to_string(), "p2".to_string()],
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
    }

    #[test]
    fn first_connect_subscription_for_unlisted_instance_gets_no_attach() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event_full(
            entries(&["echo-stub"]),
            vec![binding("ephemeral:ghost", "ghost", "feed")],
        ));
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::StartProcessors {
                    instances: vec!["echo-stub".to_string()],
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
        assert_eq!(core.instances[0].ports_attached, 0);
    }

    #[test]
    fn reconnect_with_equal_bindings_republishes_connected() {
        let subs = vec![binding("ephemeral:dev-stub", "echo-stub", "messages")];
        let mut core = KernelCore::new();
        core.on_event(&connected_event_full(entries(&["echo-stub"]), subs.clone()));
        core.on_event(&Event::Disconnected {
            reason: DetachReason::LivenessTimeout,
        });
        assert_eq!(core.link_state(), &LinkState::Reconnecting);
        let actions = core.on_event(&connected_event_full(entries(&["echo-stub"]), subs));
        // The only action is the connected link-state publish (a platform plane).
        assert_eq!(without_platform_planes(&actions), vec![]);
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"connected"}"#
        );
        assert_eq!(core.link_state(), &LinkState::Connected);
    }

    #[test]
    fn a_reconnect_leaves_the_instance_table_as_the_page_life_made_it() {
        // Mounting and failing are page-lifetime facts: the loader instantiates
        // and registers once per page, and a reconnect re-delivers the same
        // document. A table rebuilt from that document would report every live
        // instance `Pending` and erase every failure reason for the rest of the
        // page life, on the surface's own primary health observable.
        let mut core = KernelCore::new();
        core.on_event(&connected_event(entries(&["echo-stub", "protobar"])));
        core.on_processor_register("echo-stub");
        core.on_processor_load_failed("protobar", "instantiate threw");
        core.on_event(&Event::Disconnected {
            reason: DetachReason::LivenessTimeout,
        });
        core.on_event(&connected_event(entries(&["echo-stub", "protobar"])));

        assert_eq!(core.instances[0].state, InstanceState::Mounted);
        assert_eq!(core.instances[1].state, InstanceState::Failed);
        assert!(
            core.instances[1]
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("instantiate threw")),
            "{:?}",
            core.instances[1].reason
        );
    }

    #[test]
    fn changed_wiring_requests_reload() {
        // Which difference made the wiring change is the page's question — it
        // compares the retained bodies — and this is the whole of the platform
        // half's answer to one: state the reload on the link plane so chrome can
        // draw it, then ask the bootstrap for the (capped) reload. A page cannot
        // re-wire the elements it already mounted.
        let mut core = KernelCore::new();
        core.on_event(&connected_event(entries(&["echo-stub"])));
        let actions = core.on_event(&Event::WiringChanged);
        assert_eq!(
            without_platform_planes(&actions),
            vec![KernelAction::RequestReload {
                reason: "bindings changed".to_string(),
            }]
        );
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"reloading"}"#
        );
    }

    #[test]
    fn an_incompatible_peer_reports_both_ranges_and_reloads() {
        // A page the peer itself served speaking no version the peer speaks is
        // stale assets; the reload is the only thing that can heal it, and the
        // bootstrap's cap bounds a genuine incompatibility. Both ranges are
        // reported before the reload, since nothing after it remembers them.
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::Incompatible {
            ours: VersionRange { min: 2, max: 2 },
            theirs: VersionRange { min: 1, max: 1 },
        });
        let rest = without_platform_planes(&actions);
        let [KernelAction::Report { level, message, .. }, reload] = rest.as_slice() else {
            panic!("expected a report then a reload, got {actions:?}");
        };
        assert_eq!(*level, LogLevel::Error);
        assert!(message.contains("2..=2"), "ours in the message: {message}");
        assert!(
            message.contains("1..=1"),
            "theirs in the message: {message}"
        );
        assert_eq!(
            *reload,
            KernelAction::RequestReload {
                reason: "protocol version".to_string(),
            }
        );
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"reloading"}"#
        );
    }

    // ── publish results / stragglers ──

    #[test]
    fn ok_publish_result_is_noop() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::PublishResult {
            instance: "echo-stub".to_string(),
            port: "out".to_string(),
            correlation: 1,
            status: PublishStatus::Ok,
        });
        assert!(actions.is_empty());
        assert_eq!(core.link_state(), &LinkState::Connecting);
    }

    #[test]
    fn non_ok_publish_result_warns_and_reports_without_touching_link_state() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::PublishResult {
            instance: "echo-stub".to_string(),
            port: "out".to_string(),
            correlation: 2,
            status: PublishStatus::RateLimited,
        });
        let [KernelAction::Report { level, message, .. }] = actions.as_slice() else {
            panic!("expected a single Report, got {actions:?}");
        };
        assert_eq!(*level, LogLevel::Warn);
        assert!(message.contains("echo-stub"), "message: {message}");
        assert!(message.contains("out"), "message: {message}");
        assert!(message.contains("RateLimited"), "message: {message}");
        assert!(message.contains('2'), "message: {message}");
        assert_eq!(core.link_state(), &LinkState::Connecting);
    }

    #[test]
    fn every_non_ok_publish_result_attributes_its_report_to_the_rejected_instance() {
        // The blast-radius property this pins: a component looping on rejected
        // publishes floods reports, and each report must draw down *its* budget.
        // An unattributed report (`subject: None`) publishes under the bare
        // surface identity and drains the kernel's own bucket instead — silencing
        // the kernel's genuine self-reports while the offender stays clean in
        // attribution. `RateLimited` is the status a real flood actually earns,
        // and it arrives here (asynchronously, from the server) rather than at the
        // synchronous client-side gate, so this is the path that matters most.
        //
        // Every reporting status is listed. `Refused` is the one that reports
        // nothing here — its plane guard already reported, attributed, with the
        // reason — and `a_refused_publish_result_draws_no_second_report` pins
        // that half.
        for status in [
            PublishStatus::RateLimited,
            PublishStatus::BodyTooLarge { len: 32, max: 16 },
            PublishStatus::UnboundPort,
            PublishStatus::NotConnected,
            PublishStatus::ConnectionLost,
            PublishStatus::Failed,
        ] {
            let mut core = KernelCore::new();
            let actions = core.on_event(&Event::PublishResult {
                instance: "echo-stub".to_string(),
                port: "out".to_string(),
                correlation: 7,
                status,
            });
            let [KernelAction::Report { subject, .. }] = actions.as_slice() else {
                panic!("expected a single Report for {status:?}, got {actions:?}");
            };
            assert_eq!(
                subject.as_deref(),
                Some("echo-stub"),
                "the {status:?} report must name the component it is about, not the kernel"
            );
        }
    }

    #[test]
    fn a_refused_publish_result_draws_no_second_report() {
        // `Refused` is the one non-`Ok` status the plane guard already reported,
        // with the reason and the publisher attached. A generic warn on top of it
        // says less and doubles what a looping component puts on the error
        // channel — and the buffered path, which answers no publisher, emits only
        // the guard's report. One event, one report, whichever path it took.
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::PublishResult {
            instance: "chrome".to_string(),
            port: "overlay-state".to_string(),
            correlation: 3,
            status: PublishStatus::Refused,
        });
        assert_eq!(actions, Vec::new(), "the guard's own report is the report");
    }

    #[test]
    fn a_plane_refusal_reports_an_error_naming_the_publisher() {
        // The refusal's only operator-visible trace. Error, so it clears an error
        // channel's report floor — a downgrade would silence a component (or an
        // operator binding) claiming to speak for chrome's screen. Attributed to
        // the publisher, so a looping offender drains its own report budget
        // rather than the kernel's.
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::PlaneRefused {
            instance: "meeting".to_string(),
            port: "overlay-state".to_string(),
            channel: schema::LOCAL_OVERLAY_STATE_CHANNEL.to_string(),
            reason: "only the surface's chrome instance may publish it".to_string(),
        });
        let [
            KernelAction::Report {
                level,
                message,
                subject,
            },
        ] = actions.as_slice()
        else {
            panic!("expected a single Report, got {actions:?}");
        };
        assert_eq!(*level, LogLevel::Error);
        assert_eq!(subject.as_deref(), Some("meeting"));
        assert!(message.contains("meeting"), "message: {message}");
        assert!(
            message.contains("only the surface's chrome instance may publish it"),
            "the reason must survive into the report: {message}"
        );
    }

    // ── the overlay-state instrument's own wiring ─────────────────────────

    /// `connected_event_chrome` with a component wired to the takeover plane and
    /// `outputs`. The binding is what makes this a page that takes over — the
    /// grant that consents to it is the binding component's, and boot has
    /// already checked it.
    fn connected_event_takeover(
        components: Vec<ComponentEntry>,
        outputs: Vec<schema::OutputBinding>,
    ) -> Event {
        let mut bindings = document(
            components,
            vec![binding(
                schema::LOCAL_TAKEOVER_CHANNEL,
                "chrome",
                "takeover",
            )],
        );
        bindings.chrome_instance = "chrome".to_string();
        bindings.outputs = outputs;
        connected(bindings, false)
    }

    /// The chrome output binding that makes the overlay-state plane reportable.
    fn overlay_state_output() -> schema::OutputBinding {
        schema::OutputBinding {
            channel: schema::LOCAL_OVERLAY_STATE_CHANNEL.to_string(),
            instance: "chrome".to_string(),
            port: "overlay-state".to_string(),
            urgency: Urgency::Normal,
            fill_mt: 1_000,
            capacity_mt: 8_000,
        }
    }

    /// The overlay-state wiring warns in an action list, if it warned.
    fn instrument_warn(actions: &[KernelAction]) -> Option<&str> {
        actions.iter().find_map(|a| match a {
            KernelAction::Report { message, level, .. }
                if *level == LogLevel::Warn && message.contains("overlay-state") =>
            {
                Some(message.as_str())
            }
            _ => None,
        })
    }

    #[test]
    fn a_takeover_wired_surface_whose_chrome_cannot_report_overlay_state_warns_at_connect() {
        // The dark instrument. Chrome's overlay publishes are made from inside its
        // activation, where an unbound port is answered on the dispatching event's
        // detail and draws no report of its own — so without this, a surface
        // deployed before its config caught up reports `overlay: null` forever and
        // reads exactly like a healthy one.
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event_takeover(
            entries(&["chrome", "meeting"]),
            vec![],
        ));
        let message = instrument_warn(&actions).expect("a warn about the unbound plane");
        assert!(message.contains("chrome"), "message: {message}");
    }

    #[test]
    fn a_wired_or_takeover_free_surface_says_nothing_about_overlay_state() {
        // Bound: the instrument is live, nothing to say.
        let mut core = KernelCore::new();
        let wired = core.on_event(&connected_event_takeover(
            entries(&["chrome", "meeting"]),
            vec![overlay_state_output()],
        ));
        assert_eq!(instrument_warn(&wired), None);

        // Nothing wired to the takeover plane: no component can hold an overlay,
        // so an unbound port there is the correct configuration, not a gap.
        let mut core = KernelCore::new();
        let ungranted = core.on_event(&connected_event_chrome(
            entries(&["chrome", "meeting"]),
            "chrome",
        ));
        assert_eq!(instrument_warn(&ungranted), None);
    }

    #[test]
    fn a_surface_that_publishes_takeover_is_read_as_taking_over() {
        // The request travels outbound — a component publishes it — so the scan
        // reads outputs as well as subscriptions. A page whose only takeover
        // wiring is the publish is still a page that takes over.
        let takeover_output = schema::OutputBinding {
            channel: schema::LOCAL_TAKEOVER_CHANNEL.to_string(),
            instance: "meeting".to_string(),
            port: "takeover".to_string(),
            urgency: Urgency::Normal,
            fill_mt: 1_000,
            capacity_mt: 8_000,
        };
        let event = |outputs: Vec<schema::OutputBinding>| {
            let mut bindings = document(entries(&["chrome", "meeting"]), vec![]);
            bindings.chrome_instance = "chrome".to_string();
            bindings.outputs = outputs;
            connected(bindings, false)
        };

        let mut core = KernelCore::new();
        let actions = core.on_event(&event(vec![takeover_output.clone()]));
        let message = instrument_warn(&actions).expect("a warn about the unbound plane");
        assert!(message.contains("chrome"), "message: {message}");

        // ...and with chrome's overlay-state output beside it the instrument is
        // live, so nothing is said.
        let mut core = KernelCore::new();
        let wired = core.on_event(&event(vec![takeover_output, overlay_state_output()]));
        assert_eq!(instrument_warn(&wired), None);
    }

    #[test]
    fn straggler_discarded_emits_single_debug_report() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::StragglerDiscarded {
            channel: "ephemeral:demo".to_string(),
            seq: 9,
            dropped: 7,
        });
        let [KernelAction::Report { level, message, .. }] = actions.as_slice() else {
            panic!("expected a single Report, got {actions:?}");
        };
        assert_eq!(*level, LogLevel::Debug);
        assert!(message.contains("ephemeral:demo"), "message: {message}");
        assert!(message.contains('7'), "message: {message}");
        assert_eq!(core.link_state(), &LinkState::Connecting);
    }

    #[test]
    fn a_kernel_internal_breadcrumb_names_no_subject() {
        // The contrast that proves attribution did not widen into "stamp whatever
        // instance is handy": a straggler is a fact about a subscription the kernel
        // tore down, so it carries the bare surface identity. If this ever gains a
        // subject, some component starts paying for the kernel's breadcrumbs.
        let mut core = KernelCore::new();
        let actions = core.on_event(&Event::StragglerDiscarded {
            channel: "ephemeral:demo".to_string(),
            seq: 9,
            dropped: 7,
        });
        let [KernelAction::Report { subject, .. }] = actions.as_slice() else {
            panic!("expected a single Report, got {actions:?}");
        };
        assert_eq!(subject.as_deref(), None);
    }

    // ── surface-description telemetry (geometry + status) ─────────────────

    /// The instances of the single `SendStatus` in a slice carrying nothing else
    /// but control-plane publishes; panics otherwise. Control publishes are
    /// admitted because the `surface-state` plane rides every instance-table
    /// change by construction — what this asserts is that no *other* action did.
    fn only_status(actions: &[KernelAction]) -> &[InstanceReport] {
        let rest: Vec<_> = actions
            .iter()
            .filter(|a| !matches!(a, KernelAction::PublishControl { .. }))
            .collect();
        match rest.as_slice() {
            [KernelAction::SendStatus { instances }] => instances,
            other => panic!("expected exactly one SendStatus, got {other:?}"),
        }
    }

    /// The body published on `channel` within a slice; panics if none is.
    fn control_body<'a>(actions: &'a [KernelAction], channel: &str) -> &'a str {
        actions
            .iter()
            .find_map(|a| match a {
                KernelAction::PublishControl { channel: c, body } if c == channel => {
                    Some(body.as_str())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected a publish on {channel} in {actions:?}"))
    }

    /// `actions` with the platform planes filtered out: the control-plane
    /// publishes and the status telemetry frame.
    ///
    /// For the tests whose subject is the mount action stream. The
    /// `link-state` and `surface-state` planes and the status frame all ride
    /// those same transitions by construction and are pinned by their own tests
    /// below, so folding them into every exact-vector expectation would restate
    /// one fact everywhere and make each of those tests fail for unrelated
    /// reasons.
    fn without_platform_planes(actions: &[KernelAction]) -> Vec<KernelAction> {
        actions
            .iter()
            .filter(|a| {
                !matches!(
                    a,
                    KernelAction::PublishControl { .. } | KernelAction::SendStatus { .. }
                )
            })
            .cloned()
            .collect()
    }

    /// Whether any action publishes on `channel`.
    fn publishes_control(actions: &[KernelAction], channel: &str) -> bool {
        actions
            .iter()
            .any(|a| matches!(a, KernelAction::PublishControl { channel: c, .. } if c == channel))
    }

    /// The instances of the `SendStatus` within a multi-action slice; panics if
    /// none is present.
    fn status_within(actions: &[KernelAction]) -> &[InstanceReport] {
        actions
            .iter()
            .find_map(|a| match a {
                KernelAction::SendStatus { instances } => Some(instances.as_slice()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected a SendStatus in {actions:?}"))
    }

    #[test]
    fn viewport_changed_emits_geometry_only_on_change() {
        let mut core = connect(vec![entry("m1", "meeting")], false);
        assert_eq!(
            core.on_viewport_changed(1920, 1080, 2.0),
            vec![KernelAction::SendGeometry {
                width: 1920,
                height: 1080,
                device_pixel_ratio: 2.0,
            }]
        );
        // Same viewport again → suppressed.
        assert!(core.on_viewport_changed(1920, 1080, 2.0).is_empty());
        // A real change → emitted again.
        assert_eq!(
            core.on_viewport_changed(1920, 900, 2.0),
            vec![KernelAction::SendGeometry {
                width: 1920,
                height: 900,
                device_pixel_ratio: 2.0,
            }]
        );
    }

    #[test]
    fn status_tick_reports_the_full_mount_table() {
        let mut core = connect(vec![entry("a", "k1"), entry("b", "k2")], false);
        let actions = core.on_status_tick();
        let instances = only_status(&actions);
        assert_eq!(instances.len(), 2);
        assert!(instances.iter().all(|i| i.state == InstanceState::Mounted));
        assert_eq!(instances[0].instance, "a");
        assert_eq!(instances[0].kind, "k1");
    }

    #[test]
    fn a_dead_instance_is_marked_failed_and_emits_immediate_status() {
        let mut core = connect(vec![entry("m1", "meeting")], false);
        let actions = core.note_instance_failed("m1", "component trapped: boom");
        let m1 = status_within(&actions)
            .iter()
            .find(|i| i.instance == "m1")
            .expect("m1 row");
        assert_eq!(m1.state, InstanceState::Failed);
        assert_eq!(m1.reason.as_deref(), Some("component trapped: boom"));
    }

    #[test]
    fn terminal_port_failure_marks_failed_and_is_idempotent() {
        let mut core = connect(vec![entry("m1", "meeting")], false);
        let actions = core.note_instance_failed("m1", "binding removed");
        let m1 = only_status(&actions)
            .iter()
            .find(|i| i.instance == "m1")
            .expect("m1 row");
        assert_eq!(m1.state, InstanceState::Failed);
        assert_eq!(m1.reason.as_deref(), Some("binding removed"));
        // The same failure again is not a transition → no report.
        assert!(
            core.note_instance_failed("m1", "binding removed")
                .is_empty()
        );
        // An unknown instance is a no-op.
        assert!(core.note_instance_failed("ghost", "boom").is_empty());
    }

    #[test]
    fn headless_instance_is_tracked_like_any_other() {
        // Four components: the default layout places the first three; the fourth
        // is unplaced (headless) but still mounted and still in the status table.
        let mut core = connect(
            vec![
                entry("a", "k1"),
                entry("b", "k2"),
                entry("c", "k3"),
                entry("d", "k4"),
            ],
            false,
        );
        let actions = core.on_status_tick();
        let instances = only_status(&actions);
        assert_eq!(instances.len(), 4);
        let d = instances.iter().find(|i| i.instance == "d").expect("d row");
        assert_eq!(
            d.state,
            InstanceState::Mounted,
            "the unplaced (headless) instance is mounted like any other"
        );
    }

    #[test]
    fn ports_attached_counted_per_instance() {
        let mut core = KernelCore::new();
        let event = connected_event_full(
            vec![entry("p1", "protobar")],
            vec![binding("ephemeral:x", "p1", "messages")],
        );
        core.on_event(&event);
        let actions = core.on_status_tick();
        let p1 = only_status(&actions)
            .iter()
            .find(|i| i.instance == "p1")
            .expect("p1 row");
        assert_eq!(p1.ports_attached, 1);
    }
}

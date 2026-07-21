//! DOM-free kernel decision core.
//!
//! Pure state and transition logic over the surface client's control-plane
//! vocabulary. It holds no web-sys handles and compiles and unit-tests on the
//! host target; the wasm effect executor consumes the [`KernelAction`]s it
//! emits.

use std::collections::{HashMap, HashSet};

use crate::proto::{
    AlertSeverity, CONTROL_PLANE_VERSION, InstanceReport, InstanceState, LOCAL_LINK_STATE_CHANNEL,
    LOCAL_SURFACE_STATE_CHANNEL, LinkState, LinkStateBody, LogLevel, SurfaceStateBody,
    SurfaceStateInstance,
};
use crate::{Event, PublishStatus, Urgency};
use crate::{contract, proto};

/// Derive the surface WebSocket URL from the page's `location`. `https:` is the
/// only secure scheme, so it maps to `wss:`; every other protocol (`http:`,
/// `file:`, …) maps to `ws:`. `host` is `location.host` (host + optional port),
/// `slug` the surface slug. Pure string logic, host-tested; the wasm entry
/// point feeds it `location.protocol()`/`location.host()`.
pub fn ws_url(protocol: &str, host: &str, slug: &str) -> String {
    let scheme = if protocol == "https:" { "wss:" } else { "ws:" };
    format!("{scheme}//{host}/surface/{slug}/ws")
}

/// Resolve the mounted component `instance` a delegated contract event targets,
/// or the drop-and-report `KernelAction` for a target that does not resolve to a
/// currently-mounted instance element. `instance` is the id the DOM executor
/// resolved from the retargeted target element by element identity over the
/// mounted-instance registry (`None` when the target is not a mounted instance
/// element — a non-component node, or a bug). `target_tag` is that element's tag
/// name, carried only for the drop breadcrumb; `event_name` names the contract
/// event. Shared by every `route_*` entry point so the `Publish`/`Log`/`Alert`
/// paths keep identical mounted-target semantics and drop wording — a divergence
/// here would silently differ between the routing planes.
fn require_mounted_instance<'a>(
    instance: Option<&'a str>,
    target_tag: &str,
    event_name: &str,
) -> Result<&'a str, KernelAction> {
    match instance {
        Some(instance) => Ok(instance),
        // The target resolved to no mounted instance, so there is no subject to
        // name: this drop is unattributable by construction.
        None => Err(KernelAction::Report {
            level: LogLevel::Warn,
            message: format!("dropped {event_name} from non-component target <{target_tag}>"),
            subject: None,
        }),
    }
}

/// The drop-and-report for a `brenn-activation-register` whose detail carries no
/// callable `entry`: a non-conformant module, contained exactly like a malformed
/// publish. Kept out of [`KernelCore::on_activation_register`] so the gate's one
/// registration per instance is not spent answering a malformed event.
pub fn malformed_registration(instance: Option<&str>, target_tag: &str) -> KernelAction {
    KernelAction::Report {
        level: LogLevel::Warn,
        message: format!(
            "dropped malformed {} from <{target_tag}>: detail.entry must be a function",
            contract::ACTIVATION_REGISTER
        ),
        subject: instance.map(str::to_string),
    }
}

/// The three states an **optional** contract detail field can be in, as read at
/// the kernel↔component trust boundary.
///
/// A required field needs only `Option`: missing and non-string are both
/// malformed. An optional one must tell them apart — omitting `urgency` is a
/// component saying "use the port's configured default", while setting it to a
/// number is a component bug. Answering the bug with the default would hide it,
/// so the two carry different variants and take different paths.
///
/// DOM-free (the executor in `dom.rs` constructs it from the event detail) so
/// the routers here stay testable without a browser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionalField {
    /// The component omitted the field: the contract's documented default applies.
    Absent,
    /// The component supplied a string. Still untrusted — the value itself may
    /// not parse.
    Present(String),
    /// The component supplied a non-string, or the event was not a `CustomEvent`.
    Malformed,
}

/// Route a component's `brenn-port-publish` intent to a publish action.
///
/// `instance` is the mounted-instance id the DOM executor resolved from the
/// event's target — the component's host element after shadow retargeting, per
/// the contract's dispatch-origin rule — by element identity over the
/// mounted-instance registry. `None` means the target did not resolve to a
/// mounted instance element (a non-conformant module dispatching on an inner
/// light-DOM node, or a bug): drop it with `Report` rather than guess at
/// attribution. `target_tag` is that element's tag name, carried for the drop
/// breadcrumb only.
///
/// `port` and `body` are `None` when the component's event detail omitted them
/// or carried a non-string value. Malformed detail from an otherwise-valid
/// mounted instance is dropped-and-reported as malformed — never coerced into a
/// well-formed publish, which would launder a component bug into a real message
/// on the bus. Only a fully-formed detail from a mounted instance emits
/// `Publish`.
///
/// `urgency` is the component's optional per-message override: the untrusted
/// lowercase RFC 8030 wire string, parsed via [`Urgency::parse`].
/// [`OptionalField::Absent`] means the component stated no preference and the
/// port's configured default applies — the server resolves it, so the kernel
/// simply sends no urgency. A present-but-unparseable value is dropped and
/// reported as malformed, exactly like an unrecognized `level` on `brenn-log`:
/// silently downgrading a component's stated intent to the default would publish
/// at an urgency the component never chose, and hide the typo that caused it.
pub fn route_publish_intent(
    instance: Option<&str>,
    target_tag: &str,
    port: Option<&str>,
    body: Option<&str>,
    urgency: OptionalField,
) -> KernelAction {
    let instance = match require_mounted_instance(instance, target_tag, contract::PORT_PUBLISH) {
        Ok(instance) => instance,
        Err(drop) => return drop,
    };
    let malformed = |detail: &str| KernelAction::Report {
        level: LogLevel::Warn,
        message: format!(
            "dropped malformed {} from <{target_tag}>: {detail}",
            contract::PORT_PUBLISH
        ),
        subject: Some(instance.to_string()),
    };
    let urgency = match urgency {
        OptionalField::Absent => None,
        OptionalField::Present(raw) => match Urgency::parse(&raw) {
            Some(u) => Some(u),
            None => return malformed("urgency must be a known urgency level"),
        },
        OptionalField::Malformed => return malformed("urgency must be a string"),
    };
    match (port, body) {
        (Some(port), Some(body)) => KernelAction::Publish {
            instance: instance.to_string(),
            port: port.to_string(),
            body: body.to_string(),
            urgency,
        },
        _ => malformed("port and body must be strings"),
    }
}

/// Route a component's `brenn-log` intent to a component-log action.
///
/// Dispatch identity resolves exactly as [`route_publish_intent`]: `instance` is
/// the DOM-resolved mounted-instance id for the retargeted target, and a target
/// that does not resolve to a mounted instance element is dropped with `Report`
/// rather than attributed by guess. `target_tag` is carried for the breadcrumb.
///
/// `level` and `message` are `None` when the component's event detail omitted
/// them or carried a non-string value (untrusted component-supplied detail);
/// `level` is additionally the untrusted lowercase log-level wire string, parsed
/// via [`proto::LogLevel::from_wire_str`]. A missing/non-string field or an
/// unrecognized `level` is dropped-and-reported as malformed — never coerced
/// into a well-formed `Log` frame, which would launder a component bug into a
/// server log line at a level the component never chose.
pub fn route_component_log(
    instance: Option<&str>,
    target_tag: &str,
    level: Option<&str>,
    message: Option<&str>,
) -> KernelAction {
    let instance = match require_mounted_instance(instance, target_tag, contract::COMPONENT_LOG) {
        Ok(instance) => instance,
        Err(drop) => return drop,
    };
    match (level.and_then(LogLevel::from_wire_str), message) {
        (Some(level), Some(message)) => KernelAction::ComponentLog {
            instance: instance.to_string(),
            level,
            message: message.to_string(),
        },
        _ => KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "dropped malformed {} from <{target_tag}>: level must be a known log \
                 level and message a string",
                contract::COMPONENT_LOG
            ),
            subject: Some(instance.to_string()),
        },
    }
}

/// Route a component's `brenn-alert` intent to an alert action, gated on the
/// surface's alert grant.
///
/// Dispatch identity resolves exactly as [`route_publish_intent`]: `instance` is
/// the DOM-resolved mounted-instance id for the retargeted target, and a target
/// that does not resolve to a mounted instance element is dropped with `Report`
/// rather than attributed by guess. `target_tag` is carried for the breadcrumb.
///
/// `alert_granted` is the surface's current grant (from the latest `Welcome`,
/// [`KernelCore::alert_granted`]). On an **ungranted** surface a well-formed alert
/// from a mounted instance is dropped with a `Report` suppression breadcrumb
/// naming the instance — a conforming kernel never emits an ungranted `Alert`
/// frame (the server treats one as a protocol violation). The component's own
/// logs are unaffected: it can still record via `brenn-log`.
///
/// `severity`/`title`/`body` are `None` when the component's event detail omitted
/// them or carried a non-string value (untrusted component-supplied detail);
/// `severity` is additionally the untrusted lowercase severity wire string,
/// parsed via [`proto::AlertSeverity::from_wire_str`]. On a granted surface a
/// missing/non-string field or an unrecognized `severity` is dropped-and-reported
/// as malformed — never coerced into a well-formed `Alert`.
pub fn route_component_alert(
    instance: Option<&str>,
    target_tag: &str,
    severity: Option<&str>,
    title: Option<&str>,
    body: Option<&str>,
    alert_granted: bool,
) -> KernelAction {
    let instance = match require_mounted_instance(instance, target_tag, contract::COMPONENT_ALERT) {
        Ok(instance) => instance,
        Err(drop) => return drop,
    };
    if !alert_granted {
        return KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "suppressed {} from component {instance}: surface is not granted the alert plane",
                contract::COMPONENT_ALERT
            ),
            subject: Some(instance.to_string()),
        };
    }
    match (severity.and_then(AlertSeverity::from_wire_str), title, body) {
        (Some(severity), Some(title), Some(body)) => KernelAction::ComponentAlert {
            severity,
            title: title.to_string(),
            body: body.to_string(),
        },
        _ => KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "dropped malformed {} from <{target_tag}>: severity must be a known severity \
                 and title and body strings",
                contract::COMPONENT_ALERT
            ),
            subject: Some(instance.to_string()),
        },
    }
}

/// Route a headless processor instance's `log.*` import call.
///
/// The tag-free sibling of [`route_component_log`]. It takes no
/// [`require_mounted_instance`] step, and that is the whole difference: a
/// processor has no element to resolve from, so its identity is the loader's
/// closure over the instance it instantiated for — kernel-derived rather than
/// element-derived, but never component-claimed either way.
///
/// `level` is the lowercase wire string the guest's WIT enum lifts to. An
/// unrecognized one is transpile-glue drift rather than a component typo, but the
/// answer is the same as the DOM path's: drop and report, never coerce to a
/// default level the component did not choose.
pub fn route_processor_log(instance: &str, level: &str, message: &str) -> KernelAction {
    match LogLevel::from_wire_str(level) {
        Some(level) => KernelAction::ComponentLog {
            instance: instance.to_string(),
            level,
            message: message.to_string(),
        },
        None => KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "dropped processor log from {instance}: {level:?} is not a known log level"
            ),
            subject: Some(instance.to_string()),
        },
    }
}

/// Route a headless processor instance's `alert.*` import call, gated on the
/// surface's alert grant.
///
/// The tag-free sibling of [`route_component_alert`], with identical grant
/// semantics: on an ungranted surface a well-formed alert is dropped with a
/// suppression breadcrumb, because a conforming kernel never emits an ungranted
/// `Alert` frame. Boot additionally refuses to start a surface declaring an
/// `alert`-importing processor kind without the grant, so reaching the
/// suppression arm means the config changed under a live page.
pub fn route_processor_alert(
    instance: &str,
    severity: &str,
    title: &str,
    body: &str,
    alert_granted: bool,
) -> KernelAction {
    if !alert_granted {
        return KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "suppressed alert from processor {instance}: surface is not granted the alert plane"
            ),
            subject: Some(instance.to_string()),
        };
    }
    match AlertSeverity::from_wire_str(severity) {
        Some(severity) => KernelAction::ComponentAlert {
            severity,
            title: title.to_string(),
            body: body.to_string(),
        },
        None => KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "dropped processor alert from {instance}: {severity:?} is not a known severity"
            ),
            subject: Some(instance.to_string()),
        },
    }
}

/// The WIT `publish-error` name for a refused buffered publish. Each arm is one
/// exact wire string the guest matches on; a wrong mapping hands the component
/// the wrong error variant, so the map is pinned natively here rather than in
/// the wasm-only entry wrapper that calls it.
pub fn publish_error_str(err: contract::PublishError) -> String {
    use contract::PublishError;
    match err {
        PublishError::NotPermitted => "not-permitted",
        PublishError::InvalidPayload => "invalid-payload",
        PublishError::QuotaExceeded => "quota-exceeded",
    }
    .to_string()
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
    /// Create the instance's `brenn-<kind>` custom element and append it inside
    /// the instance's kernel-owned wrapper.
    MountComponent { instance: String, kind: String },
    /// Ask the bootstrap loader to bring up the named headless processor
    /// instances (dispatched as the `brenn-processor-start` seam event).
    ///
    /// Emitted once per page, from the mount plan that first sees bindings: the
    /// loader needs the config map and the bindings row that arrive with
    /// `Welcome`, and a second emission would ask it to instantiate instances it
    /// already registered (which `on_processor_register` would refuse as
    /// duplicates). A reconnect whose bindings changed reloads the page instead.
    StartProcessors { instances: Vec<String> },
    /// Dispatch `brenn-surface-ready` on `window` (first successful connect):
    /// the bootstrap resets its capped-reload counter on this signal.
    EmitReady,
    /// Resolve the instance's output `port` to a channel and publish `body`. A
    /// synchronous rejection is handled by the executor as a `Report`, matching a
    /// non-`Ok` `PublishResult`.
    ///
    /// `urgency` is the component's per-message override; `None` sends no urgency
    /// on the frame, which the server reads as "the port's configured default".
    /// The kernel deliberately does not substitute the default itself: the
    /// authoritative value is the server's, and the kernel's `Welcome` snapshot
    /// can be stale across a reconnect.
    Publish {
        instance: String,
        port: String,
        body: String,
        urgency: Option<Urgency>,
    },
    /// Log `message` to the browser console (at `level`) and forward it to the
    /// server as a leveled `log` frame. Covers the transient/component-fault
    /// breadcrumb class at `Warn` (a non-`Ok` publish outcome, a rejected publish,
    /// or a misrouted `brenn-port-publish` dropped by [`route_publish_intent`])
    /// and a component-panic report at `Error`. The level is fixed at each call
    /// site, never derived.
    ///
    /// `subject` is the instance the report is *about*, which the executor sends
    /// as the frame's `subject_instance` so the server stamps the report with that
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
    /// Forward a component's `brenn-alert` intent to the server as an `Alert`
    /// frame. Emitted only on an alert-granted surface (an ungranted surface
    /// yields a `Report` suppression breadcrumb instead); `severity` is the
    /// component's call-site-fixed severity (never derived by the kernel). The
    /// executor emits `handle.alert(severity, title, body)`; component identity
    /// is not carried on the frame — the server attributes the alert to the
    /// surface, not the component.
    ComponentAlert {
        severity: AlertSeverity,
        title: String,
        body: String,
    },
    /// Report the current browser viewport to the server (a best-effort
    /// `Geometry` telemetry frame via `ClientHandle::send_geometry`). Emitted by
    /// [`KernelCore::on_viewport_changed`] only when the viewport actually changed
    /// since the last report. `width`/`height` are CSS pixels;
    /// `device_pixel_ratio` is the display density.
    SendGeometry {
        width: u32,
        height: u32,
        device_pixel_ratio: f64,
    },
    /// Report the current per-instance mount status to the server (a best-effort
    /// `Status` telemetry snapshot via [`ClientHandle::send_status`]). Emitted on
    /// the status interval ([`KernelCore::on_status_tick`]) and immediately on any
    /// transition into `failed`. `instances` is the
    /// raw per-instance fact set; the DOM executor fills page uptime and the
    /// lifetime counters it owns before handing the frame to the client via
    /// `ClientHandle::send_status`.
    SendStatus { instances: Vec<InstanceReport> },
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
/// to a [`proto::InstanceReport`](crate::proto::InstanceReport) for
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

/// The kernel's DOM-free state and transition logic.
#[derive(Debug, Clone, PartialEq)]
pub struct KernelCore {
    /// The last link state published on `local:brenn/link-state`, for no-change
    /// suppression. Chrome renders the connection banner from this plane; the
    /// kernel (this core, the platform half) is the plane's sole producer.
    link_state: LinkState,
    /// The bindings from the first `Welcome`; `None` until the first connect.
    /// Set on first-connect, and consulted to distinguish first-connect from a
    /// reconnect.
    bindings: Option<proto::SurfaceBindings>,
    /// Whether this surface holds the alert grant, from the latest `Connected`.
    /// `false` until the first connect. A `brenn-alert` from a component is
    /// forwarded as an `Alert` frame only when granted; otherwise the kernel
    /// drops it with a `log(warn)` breadcrumb, and the panic-path alert is
    /// gated on it too — a conforming kernel never sends an ungranted `Alert`.
    alert_granted: bool,
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
    /// never calls `ClientHandle::deregister_activation`. Correct while an
    /// instance id is page-unique-forever (a layout change reloads the page). If
    /// an instance's element is ever torn down and a fresh element for the same
    /// id remounts within one page life, the gate rejects the remount's
    /// registration as a duplicate while the core still holds the old detached
    /// host's entry. Clearing must be wired with the kernel-driven instance-death
    /// path, distinguishing death (deregister + clear) from Phase-3 reparent
    /// (preserve delivery, never deregister).
    registered: HashSet<String>,
    /// The singleton chrome instance from the latest `Welcome`
    /// (`SurfaceBindings.chrome_instance`), or `None` when the surface declares
    /// no chrome. Read in exactly two places: the connect
    /// indicator handoff (this instance's first mount removes the indicator) and
    /// death-is-fatal (this instance dying reloads the page instead of showing an
    /// error card). No other path branches on it.
    chrome_instance: Option<String>,
    /// Whether the pre-chrome connect indicator is still live. True from
    /// construction (the kernel renders it at start, before any `Welcome`) until
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

    /// Gate a component's `brenn-activation-register`: decide whether the kernel
    /// may be handed this entry.
    ///
    /// `instance` is the mounted-instance id the DOM executor resolved from the
    /// retargeted target — the component never claims an instance, exactly as on
    /// every other delegated event. Returns the instance to register, plus the
    /// actions to apply; `None` means the registration is refused and the caller
    /// must not forward it.
    ///
    /// Refused in two cases, both in-page component bugs and both reported rather
    /// than forwarded into the core's fail-fast panic:
    ///
    /// - the target resolves to no mounted instance (unknown, unmounted, or a
    ///   non-conformant dispatch site);
    /// - the instance already registered — never silently replaced, which would
    ///   let a component swap another's delivery seam out from under it.
    pub fn on_activation_register(
        &mut self,
        instance: Option<&str>,
        target_tag: &str,
    ) -> (Option<String>, Vec<KernelAction>) {
        let instance =
            match require_mounted_instance(instance, target_tag, contract::ACTIVATION_REGISTER) {
                Ok(instance) => instance.to_string(),
                Err(drop) => return (None, vec![drop]),
            };
        if !self.registered.insert(instance.clone()) {
            return (
                None,
                vec![KernelAction::Report {
                    level: LogLevel::Warn,
                    message: format!(
                        "dropped duplicate {} from <{target_tag}>: instance {instance} already \
                         registered an activation entry",
                        contract::ACTIVATION_REGISTER
                    ),
                    subject: Some(instance),
                }],
            );
        }
        // Chrome's first successful mount is the connect-indicator handoff: from
        // here chrome owns connection pixels via its banner, so the kernel drops
        // its indicator and never renders it again.
        let actions = if self.chrome_instance.as_deref() == Some(instance.as_str()) {
            self.retire_connect_indicator()
        } else {
            Vec::new()
        };
        (Some(instance), actions)
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
    /// Returns whether the caller may forward the entry, plus the actions to apply.
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
    pub fn on_processor_register(&mut self, instance: &str) -> (bool, Vec<KernelAction>) {
        if !self.is_processor_instance(instance) {
            return (
                false,
                vec![KernelAction::Report {
                    level: LogLevel::Warn,
                    message: format!(
                        "dropped processor activation registration: {instance} is not a declared \
                         processor instance"
                    ),
                    subject: None,
                }],
            );
        }
        if !self.registered.insert(instance.to_string()) {
            return (
                false,
                vec![KernelAction::Report {
                    level: LogLevel::Warn,
                    message: format!(
                        "dropped duplicate processor activation registration: instance {instance} \
                         already registered an activation entry"
                    ),
                    subject: Some(instance.to_string()),
                }],
            );
        }
        let mut actions = Vec::new();
        if let Some(status) = self.instances.iter_mut().find(|s| s.instance == instance) {
            status.state = InstanceState::Mounted;
            status.reason = None;
        }
        actions.extend(self.instance_table_actions());
        (true, actions)
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
        actions.extend(self.instance_table_actions());
        actions
    }

    /// Whether `instance` is a declared `processor` component in the stored
    /// bindings. `false` before the first `Welcome`.
    fn is_processor_instance(&self, instance: &str) -> bool {
        self.bindings.as_ref().is_some_and(|b| {
            b.components
                .iter()
                .any(|c| c.instance == instance && c.abi == proto::Abi::Processor)
        })
    }

    /// Serve one `config.get` for a processor instance from the map that rode
    /// `Welcome`.
    ///
    /// Fixed for the page's lifetime, matching the backend's process-lifetime map:
    /// a changed map arrives only with a reconnect `Welcome`, which the
    /// bindings-changed check turns into a reload. A miss — unknown key, or an
    /// instance with no map — answers `None`, which is `config.get`'s own
    /// `option<string>` and not an error.
    pub fn processor_config_get(&self, instance: &str, key: &str) -> Option<String> {
        self.bindings
            .as_ref()?
            .components
            .iter()
            .find(|c| c.instance == instance)?
            .config
            .get(key)
            .cloned()
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
    /// kernel's `brenn-alert` forward (via [`route_component_alert`]) to gate an
    /// `Alert` frame vs. a suppression breadcrumb, and by the panic listener to
    /// gate the panic-path alert ([`on_component_panic`]).
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

    /// Fold one control-plane [`Event`] into the core, returning the actions
    /// the DOM executor must apply in order.
    ///
    /// `is_element_defined` takes `(kind, instance)` and reports whether that
    /// instance's custom element is registered
    /// (`customElements.get("brenn-<kind>--<instance>")`); it keeps this core
    /// DOM-free while letting the first-connect mount plan decide mount vs. error
    /// card per instance. Per-instance, not per-kind: each instance's module
    /// defines only its own element, so one instance's module failing to load
    /// error-cards that instance and leaves its siblings mountable. It is
    /// consulted only on the first connect.
    pub fn on_event(
        &mut self,
        event: &Event,
        is_element_defined: impl Fn(&str, &str) -> bool,
    ) -> Vec<KernelAction> {
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
            Event::Connected {
                bindings,
                alert_granted,
                ..
            } => {
                self.alert_granted = *alert_granted;
                self.on_connected(bindings, is_element_defined)
            }
            // The kernel mounts only `dom` components, and every one of them still
            // rides the condemned per-message dialect — nothing this kernel mounts
            // is activation-registered, so neither event can reach it. They are
            // matched rather than wildcarded so that porting the components off
            // the dialect fails to compile here, forcing the error-card and
            // `surface-state` wiring to be a decision rather than an omission.
            // A non-terminal activation error leaves the instance alive: nothing
            // to do here (the diagnostic is on the EventStream). A terminal
            // trap is contained per-instance for every component — except the
            // singleton chrome, whose death is fatal: there is no
            // layout engine left to continue with, so the kernel triggers the
            // capped bootstrap reload instead of an error card. Non-chrome
            // containment is unchanged.
            Event::ActivationFailed { .. } => Vec::new(),
            Event::InstanceFailed { instance, .. } => {
                if self.chrome_instance.as_deref() == Some(instance.as_str()) {
                    vec![KernelAction::RequestReload {
                        reason: "chrome died".to_string(),
                    }]
                } else {
                    Vec::new()
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

    /// First-connect handling: store the bindings, produce the mount plan — one
    /// `MountComponent` per component whose element is defined, an `ErrorCard`
    /// for one whose module never registered its element — publish the connected
    /// link state, and emit `EmitReady` **last**.
    ///
    /// `EmitReady` is ordered last on purpose: the bootstrap resets its
    /// capped-reload counter on it, so a panic anywhere in mount-plan
    /// application (e.g. a component constructor that panics the kernel) must
    /// increment the counter without an intervening reset — otherwise a
    /// deterministic mount panic reloads forever, never converging to the
    /// static failure message the cap guarantees.
    ///
    /// On a reconnect (bindings already stored), compare the new bindings to the
    /// stored ones: equal → republish the connected link state (the client core
    /// has already reconciled and resubscribed with resume); differ →
    /// `RequestReload { reason: "bindings changed" }`, because a config change
    /// across a restart may have made the page's manifest stale too, so a fresh
    /// page is the only state the kernel can trust.
    fn on_connected(
        &mut self,
        bindings: &proto::SurfaceBindings,
        is_element_defined: impl Fn(&str, &str) -> bool,
    ) -> Vec<KernelAction> {
        if let Some(stored) = &self.bindings {
            if stored == bindings {
                return self.set_link_state(LinkState::Connected);
            }
            return vec![KernelAction::RequestReload {
                reason: "bindings changed".to_string(),
            }];
        }
        self.bindings = Some(bindings.clone());
        self.chrome_instance = if bindings.chrome_instance.is_empty() {
            None
        } else {
            Some(bindings.chrome_instance.clone())
        };
        // Rebuild the instance-status table from this bindings set: one row per
        // configured component, `mounted` when its element is defined or `failed`
        // when its module never registered — the same decision the mount plan
        // makes. Headless instances (a component in no layout slot) are tracked
        // identically; the table has no slot concept.
        self.instances = Vec::with_capacity(bindings.components.len());
        let mut actions = Vec::new();
        // instance → kind for the instances that actually mounted (element
        // defined), so a subscription's pump can carry the kind for its terminal
        // error card without re-scanning the component list.
        let mut mounted: HashMap<&str, &str> = HashMap::new();
        // Headless instances that took the processor arm. They have no element and
        // so cannot be in `mounted`, but their ports are real and their windows are
        // assembled exactly like a `dom` instance's, so `ports_attached` must count
        // them or the status report would understate a working surface.
        let mut headless: HashSet<&str> = HashSet::new();
        for entry in &bindings.components {
            // Chrome mount failure is fatal: a chrome whose element
            // never registers (bad ABI or missing module) has no error card — a
            // page with no layout engine is not a page to keep, so reload. The
            // capped bootstrap path bounds the retry.
            let is_chrome = self.chrome_instance.as_deref() == Some(entry.instance.as_str());
            let mountable =
                entry.abi == proto::Abi::Dom && is_element_defined(&entry.kind, &entry.instance);
            if is_chrome && !mountable {
                return vec![KernelAction::RequestReload {
                    reason: "chrome mount failed".to_string(),
                }];
            }
            let (state, reason) = if entry.abi == proto::Abi::Processor {
                // Headless by construction: no element to check, no wrapper, no
                // mount. The bootstrap loader instantiates the transpiled module
                // and registers the instance's `receive`; the row sits `Pending`
                // until `on_processor_register` admits that registration, and
                // becomes `Failed` if the loader reports the instantiation or
                // registration failed. Chrome is a `dom` component by definition,
                // so the is_chrome check above can never select this arm.
                headless.insert(entry.instance.as_str());
                (InstanceState::Pending, None)
            } else if entry.abi != proto::Abi::Dom {
                // The remaining ABIs are reserved and unloadable. Boot rejects
                // them, so this is peer input the server should never send —
                // error-carded, not panicked, because that is the containment this
                // loop already gives every other unloadable instance: one dead
                // card, the rest of the surface alive.
                let reason = format!("unsupported component abi: {}", entry.abi.as_str());
                actions.push(KernelAction::ErrorCard {
                    instance: entry.instance.clone(),
                    kind: entry.kind.clone(),
                    reason: reason.clone(),
                });
                (InstanceState::Failed, Some(reason))
            } else if is_element_defined(&entry.kind, &entry.instance) {
                mounted.insert(entry.instance.as_str(), entry.kind.as_str());
                actions.push(KernelAction::MountComponent {
                    instance: entry.instance.clone(),
                    kind: entry.kind.clone(),
                });
                (InstanceState::Mounted, None)
            } else {
                actions.push(KernelAction::ErrorCard {
                    instance: entry.instance.clone(),
                    kind: entry.kind.clone(),
                    reason: "component module missing".to_string(),
                });
                (
                    InstanceState::Failed,
                    Some("component module missing".to_string()),
                )
            };
            self.instances.push(InstanceStatus {
                instance: entry.instance.clone(),
                kind: entry.kind.clone(),
                state,
                reason,
                ports_attached: 0,
            });
        }
        // Count each live instance's bound input ports for the status table.
        // Nothing is wired here: the kernel delivers off the instance's own
        // registration — which a `dom` instance's element makes from
        // `connectedCallback` and a processor instance's loader makes after
        // `instantiate`. A subscription on an error-carded instance (element never
        // defined, or a reserved ABI) or on an instance absent from `components`
        // is in neither set and is not counted — that instance will never register
        // and nothing will ever be delivered to it.
        for binding in &bindings.subscriptions {
            if (mounted.contains_key(binding.instance.as_str())
                || headless.contains(binding.instance.as_str()))
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
        if !headless.is_empty() && !self.processors_started {
            self.processors_started = true;
            let mut instances: Vec<String> = headless.iter().map(|i| (*i).to_string()).collect();
            // `headless` is a set, and the loader's report ordering is observable in
            // tests; sort so the plan is a function of the bindings alone.
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
        // retire it now. A chrome surface keeps the indicator until chrome's first
        // mount (see `on_activation_register`).
        if self.chrome_instance.is_none() {
            actions.extend(self.retire_connect_indicator());
        }
        actions.push(KernelAction::EmitReady);
        actions
    }

    /// Whether `instance` is a component this surface configured (from the stored
    /// `Welcome` bindings) — the membership check the panic-subject filter needs.
    /// `false` before the first `Welcome` (no bindings yet).
    fn is_configured_instance(&self, instance: &str) -> bool {
        self.bindings
            .as_ref()
            .is_some_and(|b| b.components.iter().any(|c| c.instance == instance))
    }

    /// Decide the kernel's response to a `brenn-component-panic { instance,
    /// message }` seam event a component module's panic hook dispatched on
    /// `window`.
    ///
    /// **One panic, one subject.** The detail names the panicked **instance**,
    /// because a module backs exactly one instance's linear memory: its poisoning
    /// is that instance's death and nobody else's. One error card, one `failed`
    /// transition, one report — and a sibling of the same kind keeps running on
    /// its own memory, untouched.
    ///
    /// `instance`/`message` are `None` when the event detail omitted them or
    /// carried a non-string value (untrusted component-supplied detail). An
    /// instance that is not currently mounted — unattributable (`None`), never
    /// configured, or already error-carded — is dropped and reported once under
    /// the bare surface identity, never error-carding a mount the panic does not
    /// own.
    ///
    /// A component panic is the one client-side event that pages: on an
    /// alert-granted surface ([`KernelCore::alert_granted`]) an attributed panic
    /// additionally emits one `ComponentAlert { Warning, "component panic:
    /// <instance>", <detail> }`. On an ungranted surface it stays error-cards +
    /// `log(error)` only — a conforming kernel never emits an ungranted `Alert`
    /// (the server treats one as a protocol violation). An unattributable panic
    /// never pages regardless of the grant.
    ///
    /// `is_mounted` reports whether an `instance` currently has a mounted element;
    /// the DOM executor owns that registry, keeping this core DOM-free.
    pub fn on_component_panic(
        &mut self,
        instance: Option<&str>,
        message: Option<&str>,
        is_mounted: impl Fn(&str) -> bool,
    ) -> Vec<KernelAction> {
        let detail = message.unwrap_or("component panicked");
        // The subject must be a live mount of a configured instance: `is_mounted`
        // alone would accept an instance this surface never declared, and the
        // detail is component-supplied.
        let subject = instance.filter(|i| self.is_configured_instance(i) && is_mounted(i));
        let Some(subject) = subject else {
            return vec![KernelAction::Report {
                level: LogLevel::Error,
                message: format!(
                    "dropped unattributable {}: instance={instance:?}",
                    contract::COMPONENT_PANIC
                ),
                subject: None,
            }];
        };
        let kind = self
            .instances
            .iter()
            .find(|s| s.instance == subject)
            .map(|s| s.kind.clone())
            .expect("a configured, mounted instance has a status row");
        let reason = format!("component panicked: {detail}");
        let mut actions = vec![
            KernelAction::ErrorCard {
                instance: subject.to_string(),
                kind,
                reason: reason.clone(),
            },
            // The report follows the card, under the dead instance's own
            // sub-identity: it is the principal that failed, so it reports its own
            // failure and draws its own budget. (Budget-exempt as a death report;
            // the server caps it at one per instance per connection.)
            KernelAction::Report {
                level: LogLevel::Error,
                message: format!("component instance {subject} panicked: {detail}"),
                subject: Some(subject.to_string()),
            },
        ];
        if self.alert_granted {
            actions.push(KernelAction::ComponentAlert {
                severity: AlertSeverity::Warning,
                title: format!("component panic: {subject}"),
                body: detail.to_string(),
            });
        }
        // Fail the instance in the status table, then emit an immediate status
        // report (a transition into `failed` reports at once, not on the next
        // tick). The report rides after the error card so the executor's error
        // counter already reflects it.
        self.mark_instance_failed(subject, &reason);
        actions.extend(self.instance_table_actions());
        actions
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

// Host-only: these are native `#[test]`s run in every `make check`. Excluded
// from the wasm32 target so the browser test binary (`make surface-wasm-test`)
// carries no compiled-but-never-run libtest harness and its test count stays
// honest.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    // ── connect indicator + chrome-death-is-fatal (kernel pixel classes) ──

    /// A `Connected` naming `chrome_instance` as the singleton chrome. All
    /// components are otherwise the ordinary defined-element shape.
    fn connected_event_chrome(components: Vec<ComponentEntry>, chrome_instance: &str) -> Event {
        let Event::Connected { mut bindings, .. } = connected_event(components) else {
            unreachable!("connected_event builds a Connected");
        };
        bindings.chrome_instance = chrome_instance.to_string();
        Event::Connected {
            bindings,
            participant_id: "surface:deskbar".to_string(),
            max_body_bytes: 65_536,
            alert_granted: false,
            takeover_granted: false,
            error_report_floor: None,
            surface_description: SurfaceDescription {
                status_interval_secs: 60,
            },
        }
    }

    #[test]
    fn chrome_death_reloads_while_a_sibling_death_is_contained() {
        let mut core = KernelCore::new();
        core.on_event(
            &connected_event_chrome(entries(&["chrome", "protobar"]), "chrome"),
            |_, _| true,
        );

        let reload = core.on_event(
            &Event::InstanceFailed {
                instance: "chrome".to_string(),
                reason: "trap".to_string(),
            },
            |_, _| true,
        );
        assert_eq!(
            reload,
            vec![KernelAction::RequestReload {
                reason: "chrome died".to_string(),
            }],
            "chrome's death is fatal — reload, no error card"
        );

        let sibling = core.on_event(
            &Event::InstanceFailed {
                instance: "protobar".to_string(),
                reason: "trap".to_string(),
            },
            |_, _| true,
        );
        assert!(
            sibling.is_empty(),
            "a non-chrome death is contained per-instance, unchanged"
        );
    }

    #[test]
    fn chrome_mount_failure_reloads_instead_of_error_carding() {
        let mut core = KernelCore::new();
        // chrome's element never registered: is_element_defined is false for it.
        let actions = core.on_event(
            &connected_event_chrome(entries(&["chrome"]), "chrome"),
            |kind, _| kind != "chrome",
        );
        assert_eq!(
            actions,
            vec![KernelAction::RequestReload {
                reason: "chrome mount failed".to_string(),
            }],
            "a chrome that cannot mount reloads the page, no error card"
        );
    }

    #[test]
    fn connect_indicator_retired_on_chrome_first_mount() {
        let mut core = KernelCore::new();
        core.on_event(
            &connected_event_chrome(entries(&["chrome", "protobar"]), "chrome"),
            |_, _| true,
        );

        // A non-chrome mount leaves the indicator alone.
        let (_, sib) = core.on_activation_register(Some("protobar"), "BRENN-PROTOBAR");
        assert!(
            !sib.contains(&KernelAction::RemoveConnectIndicator),
            "a sibling's mount does not touch the indicator"
        );

        // Chrome's first mount is the handoff: the indicator is removed once.
        let (_, first) = core.on_activation_register(Some("chrome"), "BRENN-CHROME");
        assert!(
            first.contains(&KernelAction::RemoveConnectIndicator),
            "chrome's first mount retires the indicator"
        );
    }

    #[test]
    fn connect_indicator_retired_at_connect_on_a_chromeless_surface() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(entries(&["protobar"])), |_, _| true);
        assert!(
            actions.contains(&KernelAction::RemoveConnectIndicator),
            "no chrome to hand off to — retire the indicator at connect"
        );
    }

    #[test]
    fn disconnected_drives_the_indicator_only_while_it_is_live() {
        let mut core = KernelCore::new();
        // Live indicator: a drop drives it to Reconnecting.
        let live = core.on_event(
            &Event::Disconnected {
                reason: DisconnectReason::LivenessTimeout,
            },
            |_, _| true,
        );
        assert!(
            live.contains(&KernelAction::SetConnectIndicator(
                ConnectIndicatorState::Reconnecting
            )),
            "a live indicator follows the kernel's link state"
        );

        // After a chrome-less connect retires it, a further drop is silent.
        core.on_event(&connected_event(entries(&["protobar"])), |_, _| true);
        let after = core.on_event(
            &Event::Disconnected {
                reason: DisconnectReason::LivenessTimeout,
            },
            |_, _| true,
        );
        assert!(
            !after
                .iter()
                .any(|a| matches!(a, KernelAction::SetConnectIndicator(_))),
            "a retired indicator is never re-rendered"
        );
    }

    // ── the activation registration gate ──────────────────────────────────

    #[test]
    fn a_mounted_instance_registers_exactly_once() {
        // The gate's whole job. The client core panics on a duplicate
        // `RegisterActivation` — the right backstop for a kernel bug, and a page
        // death for what is really an in-page component bug. So the second
        // registration must stop here, as a report naming the offender, and must
        // never be silently *accepted* either: replacing a live entry would swap a
        // component's delivery seam out from under it.
        let mut core = KernelCore::new();

        let (admitted, actions) = core.on_activation_register(Some("p1"), "BRENN-PROTOBAR");
        assert_eq!(admitted.as_deref(), Some("p1"));
        assert!(
            actions.is_empty(),
            "an admitted registration reports nothing"
        );
        assert!(core.is_registered("p1"));

        let (admitted, actions) = core.on_activation_register(Some("p1"), "BRENN-PROTOBAR");
        assert_eq!(admitted, None, "the duplicate never reaches the core");
        assert!(matches!(
            actions.as_slice(),
            [KernelAction::Report { level: LogLevel::Warn, message, subject: Some(s) }]
                if message.contains("already registered") && s == "p1"
        ));
        assert!(
            core.is_registered("p1"),
            "the first registration still stands"
        );
    }

    #[test]
    fn a_registration_from_an_unmounted_target_is_dropped_not_forwarded() {
        // `None` is the DOM half saying the retargeted target is no mounted
        // instance's element — unknown, already dead, or a non-conformant dispatch
        // site. There is no subject to name and nothing to register.
        let mut core = KernelCore::new();
        let (admitted, actions) = core.on_activation_register(None, "DIV");
        assert_eq!(admitted, None);
        assert!(matches!(
            actions.as_slice(),
            [KernelAction::Report { message, subject: None, .. }]
                if message.contains("non-component target")
        ));
    }

    #[test]
    fn registrations_are_gated_per_instance_not_per_kind() {
        // Two instances of one kind are two principals with two entries. A gate
        // keyed on kind would let the first sibling to register lock the second
        // out of delivery entirely.
        let mut core = KernelCore::new();
        assert_eq!(
            core.on_activation_register(Some("p1"), "BRENN-PROTOBAR")
                .0
                .as_deref(),
            Some("p1")
        );
        assert_eq!(
            core.on_activation_register(Some("p2"), "BRENN-PROTOBAR")
                .0
                .as_deref(),
            Some("p2")
        );
    }

    #[test]
    fn a_malformed_registration_does_not_spend_the_instance_s_one_claim() {
        // The entry-less detail is reported *before* the gate, so a component that
        // dispatched a malformed registration can still register a real one. A
        // report is a breadcrumb, not a sentence.
        let mut core = KernelCore::new();
        assert!(matches!(
            malformed_registration(Some("p1"), "BRENN-PROTOBAR"),
            KernelAction::Report { message, subject: Some(s), .. }
                if message.contains("must be a function") && s == "p1"
        ));
        assert!(!core.is_registered("p1"));
        assert_eq!(
            core.on_activation_register(Some("p1"), "BRENN-PROTOBAR")
                .0
                .as_deref(),
            Some("p1"),
        );
    }

    use crate::proto::{Abi, Binding, ComponentEntry, SurfaceBindings, SurfaceDescription};
    use crate::{DisconnectReason, PublishStatus};

    // ── shared builders ───────────────────────────────────────────────────

    /// One component instance whose id differs from its kind.
    fn entry(instance: &str, kind: &str) -> ComponentEntry {
        ComponentEntry {
            instance: instance.to_string(),
            kind: kind.to_string(),
            abi: Abi::Dom,
            parked_batch_depth: 8,
            config: Default::default(),
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
            noise: brenn_surface_proto::NoiseLevel::Silent,
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
        Event::Connected {
            bindings: SurfaceBindings {
                components,
                subscriptions,
                outputs: vec![],
                local_channels: vec![],
                chrome_instance: String::new(),
            },
            participant_id: "surface:deskbar".to_string(),
            max_body_bytes: 65_536,
            alert_granted,
            takeover_granted: false,
            error_report_floor: None,
            surface_description: SurfaceDescription {
                status_interval_secs: 60,
            },
        }
    }

    /// A connected core carrying `components` (element defined for all) and the
    /// given grant — the fixture the panic tests build on, since
    /// `on_component_panic` reads stored bindings + the grant.
    fn connect(components: Vec<ComponentEntry>, alert_granted: bool) -> KernelCore {
        let mut core = KernelCore::new();
        core.on_event(
            &connected_event_granted(components, vec![], alert_granted),
            |_, _| true,
        );
        core
    }

    // ── ws_url ────────────────────────────────────────────────────────────

    #[test]
    fn ws_url_https_maps_to_wss() {
        assert_eq!(
            ws_url("https:", "example.com:8443", "pfin"),
            "wss://example.com:8443/surface/pfin/ws"
        );
    }

    #[test]
    fn ws_url_http_maps_to_ws() {
        assert_eq!(
            ws_url("http:", "localhost:3000", "graf"),
            "ws://localhost:3000/surface/graf/ws"
        );
    }

    #[test]
    fn ws_url_non_http_protocol_maps_to_ws() {
        assert_eq!(ws_url("file:", "host", "slug"), "ws://host/surface/slug/ws");
    }

    // ── route_publish_intent ──────────────────────────────────────────────

    #[test]
    fn publish_intent_from_mounted_instance_routes_to_publish() {
        // No urgency in the detail: the kernel sends none, so the port's
        // configured default applies server-side.
        let action = route_publish_intent(
            Some("p1"),
            "brenn-protobar",
            Some("out"),
            Some("42"),
            OptionalField::Absent,
        );
        assert_eq!(
            action,
            KernelAction::Publish {
                instance: "p1".to_string(),
                port: "out".to_string(),
                body: "42".to_string(),
                urgency: None,
            }
        );
    }

    #[test]
    fn publish_intent_carries_a_stated_urgency_through() {
        // Every rung of the ladder round-trips from the wire string the component
        // dispatched to the typed override the frame carries.
        for (raw, expected) in [
            ("very-low", Urgency::VeryLow),
            ("low", Urgency::Low),
            ("normal", Urgency::Normal),
            ("high", Urgency::High),
        ] {
            let action = route_publish_intent(
                Some("p1"),
                "brenn-protobar",
                Some("out"),
                Some("42"),
                OptionalField::Present(raw.to_string()),
            );
            assert_eq!(
                action,
                KernelAction::Publish {
                    instance: "p1".to_string(),
                    port: "out".to_string(),
                    body: "42".to_string(),
                    urgency: Some(expected),
                },
                "urgency {raw}"
            );
        }
    }

    #[test]
    fn publish_intent_with_an_absent_urgency_is_not_the_same_as_normal() {
        // The distinction the whole `OptionalField` three-state exists for:
        // "absent" must reach the frame as `None` (defer to the port's default),
        // not as `Some(Normal)`. Collapsing them would silently pin every publish
        // to `normal` and make the config knob dead.
        let absent = route_publish_intent(
            Some("p1"),
            "brenn-protobar",
            Some("out"),
            Some("42"),
            OptionalField::Absent,
        );
        let stated = route_publish_intent(
            Some("p1"),
            "brenn-protobar",
            Some("out"),
            Some("42"),
            OptionalField::Present("normal".to_string()),
        );
        assert_ne!(absent, stated);
    }

    #[test]
    fn publish_intent_with_an_unknown_urgency_is_dropped_as_malformed() {
        // A typo'd or non-string urgency is a component bug. Reporting it beats
        // coercing to the default, which would publish at a level the component
        // never chose and hide the typo. Mirrors the unknown-`level` rule on
        // `brenn-log`.
        for urgency in [
            OptionalField::Present("urgent".to_string()),
            OptionalField::Present("NORMAL".to_string()),
            OptionalField::Present(String::new()),
            OptionalField::Malformed,
        ] {
            let action = route_publish_intent(
                Some("p1"),
                "brenn-protobar",
                Some("out"),
                Some("42"),
                urgency.clone(),
            );
            let KernelAction::Report { level, message, .. } = action else {
                panic!("expected Report for {urgency:?}, got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert!(message.contains("malformed"), "message: {message}");
            assert!(message.contains("urgency"), "message: {message}");
        }
    }

    #[test]
    fn publish_intent_from_unresolved_target_is_dropped_and_reported() {
        // The DOM executor could not resolve the target to a mounted instance
        // (unmounted element, or a non-component node): drop-and-report, never
        // guess attribution. The breadcrumb names the offending tag.
        for tag in ["brenn-protobar", "button"] {
            let action =
                route_publish_intent(None, tag, Some("out"), Some("42"), OptionalField::Absent);
            let KernelAction::Report { level, message, .. } = action else {
                panic!("expected Report for <{tag}>, got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert!(message.contains(tag), "message: {message}");
        }
    }

    #[test]
    fn publish_intent_with_malformed_detail_from_mounted_instance_is_dropped_as_malformed() {
        // A missing/non-string port or body from an otherwise-valid mounted
        // instance must be reported as malformed, not coerced into a well-formed
        // publish (which would launder a component bug onto the bus).
        for (port, body) in [(None, Some("42")), (Some("out"), None), (None, None)] {
            let action = route_publish_intent(
                Some("p1"),
                "brenn-protobar",
                port,
                body,
                OptionalField::Absent,
            );
            let KernelAction::Report { level, message, .. } = action else {
                panic!("expected Report for ({port:?}, {body:?}), got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert!(message.contains("malformed"), "message: {message}");
            assert!(message.contains("brenn-protobar"), "message: {message}");
        }
    }

    // ── route_component_log ───────────────────────────────────────────────

    #[test]
    fn component_log_from_mounted_instance_forwards_with_instance() {
        let action = route_component_log(Some("p1"), "brenn-protobar", Some("warn"), Some("hi"));
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
        for (wire, level) in [
            ("trace", LogLevel::Trace),
            ("debug", LogLevel::Debug),
            ("info", LogLevel::Info),
            ("warn", LogLevel::Warn),
            ("error", LogLevel::Error),
        ] {
            let action = route_component_log(Some("p1"), "brenn-protobar", Some(wire), Some("m"));
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
    fn component_log_from_unresolved_target_is_dropped_and_reported() {
        for tag in ["brenn-protobar", "button"] {
            let action = route_component_log(None, tag, Some("warn"), Some("m"));
            let KernelAction::Report { level, message, .. } = action else {
                panic!("expected Report for <{tag}>, got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert!(message.contains(tag), "message: {message}");
        }
    }

    #[test]
    fn component_log_with_malformed_detail_is_dropped_as_malformed() {
        let cases = [
            (None, Some("m")),
            (Some("warn"), None),
            (Some("fatal"), Some("m")),
            (None, None),
        ];
        for (level, message) in cases {
            let action = route_component_log(Some("p1"), "brenn-protobar", level, message);
            let KernelAction::Report {
                level: report_level,
                message: report_message,
                ..
            } = action
            else {
                panic!("expected Report for ({level:?}, {message:?}), got {action:?}");
            };
            assert_eq!(report_level, LogLevel::Warn);
            assert!(report_message.contains("malformed"), "{report_message}");
            assert!(
                report_message.contains("brenn-protobar"),
                "{report_message}"
            );
        }
    }

    // ── route_component_alert ─────────────────────────────────────────────

    #[test]
    fn component_alert_from_granted_mounted_instance_forwards_each_severity() {
        for (wire, severity) in [
            ("info", AlertSeverity::Info),
            ("warning", AlertSeverity::Warning),
            ("critical", AlertSeverity::Critical),
        ] {
            let action = route_component_alert(
                Some("p1"),
                "brenn-protobar",
                Some(wire),
                Some("t"),
                Some("b"),
                true,
            );
            assert_eq!(
                action,
                KernelAction::ComponentAlert {
                    severity,
                    title: "t".to_string(),
                    body: "b".to_string(),
                }
            );
        }
    }

    #[test]
    fn component_alert_on_ungranted_surface_is_suppressed_with_breadcrumb() {
        let action = route_component_alert(
            Some("p1"),
            "brenn-protobar",
            Some("warning"),
            Some("t"),
            Some("b"),
            false,
        );
        let KernelAction::Report { level, message, .. } = action else {
            panic!("expected Report suppression breadcrumb, got {action:?}");
        };
        assert_eq!(level, LogLevel::Warn);
        assert!(message.contains("suppressed"), "message: {message}");
        assert!(message.contains("p1"), "message: {message}");
    }

    #[test]
    fn component_alert_from_unresolved_target_is_dropped_and_reported() {
        for tag in ["brenn-protobar", "button"] {
            let action =
                route_component_alert(None, tag, Some("warning"), Some("t"), Some("b"), true);
            let KernelAction::Report { level, message, .. } = action else {
                panic!("expected Report for <{tag}>, got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert!(message.contains(tag), "message: {message}");
        }
    }

    #[test]
    fn component_alert_with_malformed_detail_on_granted_surface_is_dropped_as_malformed() {
        let cases = [
            (None, Some("t"), Some("b")),
            (Some("warning"), None, Some("b")),
            (Some("warning"), Some("t"), None),
            (Some("warn"), Some("t"), Some("b")),
            (None, None, None),
        ];
        for (severity, title, body) in cases {
            let action =
                route_component_alert(Some("p1"), "brenn-protobar", severity, title, body, true);
            let KernelAction::Report { level, message, .. } = action else {
                panic!("expected Report for ({severity:?}, {title:?}, {body:?}), got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert!(message.contains("malformed"), "message: {message}");
            assert!(message.contains("brenn-protobar"), "message: {message}");
        }
    }

    // ── on_component_panic ────────────────────────────────────────────────

    #[test]
    fn component_panic_on_ungranted_surface_error_cards_and_reports_without_paging() {
        let mut core = connect(vec![entry("e1", "echo-stub")], false);
        let actions = core.on_component_panic(Some("e1"), Some("boom"), |i| i == "e1");
        let shown = without_platform_planes(&actions);
        let [
            KernelAction::ErrorCard {
                instance,
                kind,
                reason,
            },
            KernelAction::Report { level, message, .. },
        ] = shown.as_slice()
        else {
            panic!("expected exactly ErrorCard + Report, got {actions:?}");
        };
        assert_eq!(instance, "e1");
        assert_eq!(kind, "echo-stub");
        assert!(reason.contains("boom"), "reason: {reason}");
        assert_eq!(*level, LogLevel::Error);
        assert!(message.contains("e1"), "message: {message}");
        assert!(message.contains("boom"), "message: {message}");
    }

    #[test]
    fn component_panic_on_granted_surface_also_pages() {
        let mut core = connect(vec![entry("e1", "echo-stub")], true);
        let actions = core.on_component_panic(Some("e1"), Some("boom"), |i| i == "e1");
        let shown = without_platform_planes(&actions);
        let [
            KernelAction::ErrorCard { .. },
            KernelAction::Report {
                level: LogLevel::Error,
                ..
            },
            KernelAction::ComponentAlert {
                severity,
                title,
                body,
            },
        ] = shown.as_slice()
        else {
            panic!("expected ErrorCard + Report + ComponentAlert, got {actions:?}");
        };
        assert_eq!(*severity, AlertSeverity::Warning);
        assert_eq!(title, "component panic: e1");
        assert_eq!(body, "boom");
    }

    #[test]
    fn component_panic_error_cards_only_its_own_instance_leaving_siblings_alive() {
        // Two protobar instances plus one of another kind. A module backs exactly
        // one instance's linear memory, so a panic naming p1 is p1's death and
        // nobody else's: p1 is error-carded, its sibling p2 (same kind, own memory)
        // and q1 are untouched, and the page fires once for the one subject. The
        // §8 two-siblings isolation pin.
        let mut core = connect(
            vec![
                entry("p1", "protobar"),
                entry("p2", "protobar"),
                entry("q1", "other"),
            ],
            true,
        );
        let actions = core.on_component_panic(Some("p1"), Some("boom"), |_| true);
        let carded: Vec<&str> = actions
            .iter()
            .filter_map(|a| match a {
                KernelAction::ErrorCard { instance, kind, .. } => {
                    assert_eq!(kind, "protobar");
                    Some(instance.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(carded, vec!["p1"], "only the panicked instance is carded");
        // One report, under the dead instance's own subject: it is the principal
        // that failed, so it reports under itself and draws its own send budget.
        let subjects: Vec<Option<&str>> = actions
            .iter()
            .filter_map(|a| match a {
                KernelAction::Report { subject, .. } => Some(subject.as_deref()),
                _ => None,
            })
            .collect();
        assert_eq!(subjects, vec![Some("p1")], "one report, naming itself");
        assert_eq!(
            actions
                .iter()
                .filter(|a| matches!(a, KernelAction::ComponentAlert { .. }))
                .count(),
            1,
            "one page for the one dead instance",
        );
    }

    #[test]
    fn component_panic_naming_an_unmounted_instance_is_dropped() {
        // p2 is not mounted (currently error-carded). A panic naming it owns no
        // live mount, so it is dropped and reported once under the bare surface
        // identity — it never error-cards a mount it does not own.
        let mut core = connect(
            vec![entry("p1", "protobar"), entry("p2", "protobar")],
            false,
        );
        let actions = core.on_component_panic(Some("p2"), Some("boom"), |i| i == "p1");
        let [
            KernelAction::Report {
                message, subject, ..
            },
        ] = actions.as_slice()
        else {
            panic!("expected a single drop Report, got {actions:?}");
        };
        assert!(message.contains("unattributable"), "message: {message}");
        assert_eq!(*subject, None);
    }

    #[test]
    fn component_panic_missing_message_uses_fallback_reason() {
        let mut core = connect(vec![entry("e1", "echo-stub")], false);
        let actions = core.on_component_panic(Some("e1"), None, |_| true);
        let KernelAction::ErrorCard { reason, .. } = &actions[0] else {
            panic!("expected ErrorCard, got {actions:?}");
        };
        assert!(reason.contains("component panicked"), "reason: {reason}");
    }

    #[test]
    fn component_panic_with_no_mounted_instance_never_pages_even_when_granted() {
        // No kind named, a kind this surface never configured, or a configured
        // kind whose instances are all unmounted: drop-and-report only, never
        // error-card a mount the panic does not own, never page.
        let mut core = connect(vec![entry("e1", "echo-stub")], true);
        let cases: [(Option<&str>, bool); 3] = [
            (None, true),               // unattributable
            (Some("ghost"), true),      // kind never configured
            (Some("echo-stub"), false), // configured kind, its instance unmounted
        ];
        for (kind, mounted) in cases {
            let actions = core.on_component_panic(kind, Some("boom"), move |_| mounted);
            let [KernelAction::Report { level, message, .. }] = actions.as_slice() else {
                panic!("expected a single Report for {kind:?}, got {actions:?}");
            };
            assert_eq!(*level, LogLevel::Error);
            assert!(message.contains("unattributable"), "message: {message}");
        }
    }

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
        let actions = core.on_event(
            &Event::Disconnected {
                reason: DisconnectReason::LivenessTimeout,
            },
            |_, _| false,
        );
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"reconnecting"}"#
        );

        let mut core = KernelCore::new();
        let actions = core.on_event(
            &Event::ReloadRequired {
                server_build: "abc".to_string(),
            },
            |_, _| false,
        );
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"reloading"}"#
        );

        let mut core = KernelCore::new();
        let actions = core.on_event(
            &Event::Fatal {
                detail: "bad frame".to_string(),
            },
            |_, _| false,
        );
        // No detail on the plane: the payload is fixed at `{v, state}` and a
        // consumer renders its own chrome. The `Event::Fatal` detail rides the
        // separate `Report` breadcrumb, not the plane.
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"fatal"}"#
        );

        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(entries(&["echo-stub"])), |_, _| true);
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
            reason: DisconnectReason::LivenessTimeout,
        };
        assert!(publishes_control(
            &core.on_event(&event, |_, _| false),
            LOCAL_LINK_STATE_CHANNEL
        ));
        assert!(!publishes_control(
            &core.on_event(&event, |_, _| false),
            LOCAL_LINK_STATE_CHANNEL
        ));
    }

    #[test]
    fn connect_publishes_the_mount_table_on_the_surface_state_plane() {
        // chrome learns the instance set from this plane and never by
        // querying the DOM — so the set must be complete at connect, including the
        // instance that failed its mount: chrome arranges it too, placing its
        // error card in a panel exactly as the pre-rewrite kernel did.
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &connected_event(vec![entry("ok", "good"), entry("bad", "missing")]),
            |kind, _| kind == "good",
        );
        assert_eq!(
            control_body(&actions, LOCAL_SURFACE_STATE_CHANNEL),
            r#"{"v":1,"instances":[{"instance":"ok","kind":"good","state":"mounted"},{"instance":"bad","kind":"missing","state":"failed","reason":"component module missing"}]}"#
        );
    }

    #[test]
    fn a_component_panic_marks_only_its_own_instance_failed_on_the_surface_state_plane() {
        // The plane mirrors the kernel's instance table. A panic is one instance's
        // death: p1 shows failed while its same-kind sibling p2 keeps running on
        // its own memory. A chrome that stopped arranging p2 here would be exactly
        // the false-death bug the one-subject model prevents.
        let mut core = connect(
            vec![entry("p1", "protobar"), entry("p2", "protobar")],
            false,
        );
        let actions = core.on_component_panic(Some("p1"), Some("boom"), |_| true);
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
        let actions = core.on_event(
            &Event::Disconnected {
                reason: DisconnectReason::LivenessTimeout,
            },
            |_, _| false,
        );
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
        let actions = core.on_event(
            &Event::ReloadRequired {
                server_build: "abc123".to_string(),
            },
            |_, _| false,
        );
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
        let actions = core.on_event(
            &Event::Fatal {
                detail: "bad frame".to_string(),
            },
            |_, _| false,
        );
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
        core.on_event(&connected_event(entries(&["echo-stub"])), |_, _| true);
        let actions = core.on_event(
            &Event::Fatal {
                detail: "bad frame".to_string(),
            },
            |_, _| false,
        );
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
        core.on_event(&connected_event(entries(&["echo-stub"])), |_, _| true);
        assert!(!core.alert_granted());

        let mut granted = KernelCore::new();
        granted.on_event(
            &connected_event_granted(entries(&["echo-stub"]), vec![], true),
            |_, _| true,
        );
        assert!(granted.alert_granted());
    }

    #[test]
    fn first_connect_emits_ready_mounts_defined_and_publishes_connected() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(entries(&["echo-stub"])), |_, _| true);
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::MountComponent {
                    instance: "echo-stub".to_string(),
                    kind: "echo-stub".to_string(),
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
        assert_eq!(core.link_state(), &LinkState::Connected);
    }

    #[test]
    fn first_connect_error_cards_undefined_element() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(entries(&["echo-stub"])), |_, _| false);
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::ErrorCard {
                    instance: "echo-stub".to_string(),
                    kind: "echo-stub".to_string(),
                    reason: "component module missing".to_string(),
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
    }

    #[test]
    fn first_connect_error_cards_an_abi_the_kernel_cannot_load() {
        // Boot rejects the reserved ABIs, so this frame is one no conforming
        // server sends. The kernel still must not mount it, must not panic on it
        // (peer input), and must not let it take the surface down with it: one
        // error card, and the rest of the page lives.
        let mut core = KernelCore::new();
        let mut components = entries(&["protobar"]);
        components.push(processor_entry("reserved", "counter"));
        components[1].abi = Abi::DomTs;
        // Element defined for *every* kind: the rejection must come from the ABI
        // and nothing else, so the missing-module path cannot explain the card.
        let actions = core.on_event(&connected_event(components), |_, _| true);
        assert!(actions.contains(&KernelAction::MountComponent {
            instance: "protobar".to_string(),
            kind: "protobar".to_string(),
        }));
        assert!(actions.contains(&KernelAction::ErrorCard {
            instance: "reserved".to_string(),
            kind: "counter".to_string(),
            reason: "unsupported component abi: dom-ts".to_string(),
        }));
        assert!(!actions.iter().any(|a| matches!(
            a,
            KernelAction::MountComponent { instance, .. } if instance == "reserved"
        )));
        let status = core.instances.iter().find(|i| i.instance == "reserved");
        let status = status.expect("a rejected instance still has a status row");
        assert_eq!(status.state, InstanceState::Failed);
        assert_eq!(
            status.reason.as_deref(),
            Some("unsupported component abi: dom-ts")
        );
    }

    /// A declared `processor` component entry with an empty config map.
    fn processor_entry(instance: &str, kind: &str) -> ComponentEntry {
        ComponentEntry {
            instance: instance.to_string(),
            kind: kind.to_string(),
            abi: Abi::Processor,
            parked_batch_depth: 8,
            config: Default::default(),
        }
    }

    #[test]
    fn processor_entry_is_headless_and_pending_with_no_wrapper() {
        // The whole shape of the processor arm in one assertion set: no mount, no
        // error card, and a `Pending` row — the state that exists precisely because
        // a headless instance's wiring completes later, at registration.
        let mut core = KernelCore::new();
        let mut components = entries(&["protobar"]);
        components.push(processor_entry("counter-a", "counter"));
        // `is_element_defined` answers false for everything: a processor must not
        // consult it at all, so protobar cards while the processor still goes
        // Pending rather than "component module missing".
        let actions = core.on_event(&connected_event(components), |_, _| false);
        assert!(!actions.iter().any(|a| matches!(
            a,
            KernelAction::MountComponent { instance, .. } | KernelAction::ErrorCard { instance, .. }
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

    #[test]
    fn processor_instances_are_handed_to_the_loader_once_per_page() {
        let mut core = KernelCore::new();
        let components = vec![
            processor_entry("counter-b", "counter"),
            processor_entry("counter-a", "counter"),
        ];
        let actions = core.on_event(&connected_event(components.clone()), |_, _| false);
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
        let again = core.on_event(&connected_event(components), |_, _| false);
        assert!(
            !again
                .iter()
                .any(|a| matches!(a, KernelAction::StartProcessors { .. }))
        );
    }

    #[test]
    fn a_dom_only_surface_never_asks_the_loader_for_processors() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(entries(&["protobar"])), |_, _| true);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, KernelAction::StartProcessors { .. }))
        );
    }

    #[test]
    fn processor_register_admits_once_and_mounts_the_row() {
        let mut core = KernelCore::new();
        core.on_event(
            &connected_event(vec![processor_entry("counter-a", "counter")]),
            |_, _| false,
        );

        let (admitted, actions) = core.on_processor_register("counter-a");
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
        let (admitted, actions) = core.on_processor_register("counter-a");
        assert!(!admitted);
        assert!(matches!(
            &actions[..],
            [KernelAction::Report { message, subject: Some(s), .. }]
                if message.contains("duplicate") && s == "counter-a"
        ));
        assert_eq!(core.instances[0].state, InstanceState::Mounted);
    }

    #[test]
    fn processor_register_refuses_unknown_and_non_processor_instances() {
        let mut core = KernelCore::new();
        let mut components = entries(&["protobar"]);
        components.push(processor_entry("counter-a", "counter"));
        core.on_event(&connected_event(components), |_, _| true);

        // Not declared at all.
        let (admitted, actions) = core.on_processor_register("ghost");
        assert!(!admitted);
        assert!(matches!(
            &actions[..],
            [KernelAction::Report { message, .. }] if message.contains("not a declared processor")
        ));

        // Declared, but a `dom` instance: the headless door is not a second way in
        // for a component that already has the DOM one.
        let (admitted, actions) = core.on_processor_register("protobar");
        assert!(!admitted);
        assert!(matches!(
            &actions[..],
            [KernelAction::Report { message, .. }] if message.contains("not a declared processor")
        ));
        assert!(!core.is_registered("protobar"));
    }

    #[test]
    fn processor_load_failure_fails_the_row_once_with_a_death_report() {
        let mut core = KernelCore::new();
        core.on_event(
            &connected_event(vec![
                processor_entry("counter-a", "counter"),
                processor_entry("counter-b", "counter"),
            ]),
            |_, _| false,
        );

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
        core.on_event(
            &connected_event(vec![processor_entry("counter-a", "counter")]),
            |_, _| false,
        );

        // The instance is registered and delivering.
        let (admitted, _) = core.on_processor_register("counter-a");
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
        core.on_event(
            &connected_event_full(
                vec![processor_entry("counter-a", "counter")],
                vec![
                    binding("brenn:ticks", "counter-a", "ticks"),
                    binding("brenn:other", "counter-a", "other"),
                ],
            ),
            |_, _| false,
        );
        assert_eq!(core.instances[0].ports_attached, 2);
    }

    #[test]
    fn processor_config_get_answers_from_welcome_and_misses_are_none() {
        let mut core = KernelCore::new();
        let mut entry = processor_entry("counter-a", "counter");
        entry.config.insert("mode".to_string(), "loud".to_string());
        core.on_event(
            &connected_event(vec![entry, processor_entry("counter-b", "counter")]),
            |_, _| false,
        );

        assert_eq!(
            core.processor_config_get("counter-a", "mode").as_deref(),
            Some("loud")
        );
        assert_eq!(core.processor_config_get("counter-a", "absent"), None);
        // Per-instance, not per-kind: a sibling of the same kind has its own map.
        assert_eq!(core.processor_config_get("counter-b", "mode"), None);
        assert_eq!(core.processor_config_get("ghost", "mode"), None);
    }

    #[test]
    fn processor_log_and_alert_route_without_an_element() {
        assert_eq!(
            route_processor_log("counter-a", "warn", "hi"),
            KernelAction::ComponentLog {
                instance: "counter-a".to_string(),
                level: LogLevel::Warn,
                message: "hi".to_string(),
            }
        );
        assert!(matches!(
            route_processor_log("counter-a", "shout", "hi"),
            KernelAction::Report { .. }
        ));

        assert_eq!(
            route_processor_alert("counter-a", "warning", "t", "b", true),
            KernelAction::ComponentAlert {
                severity: AlertSeverity::Warning,
                title: "t".to_string(),
                body: "b".to_string(),
            }
        );
        // Ungranted: a suppression breadcrumb, never an `Alert` the server would
        // treat as a protocol violation.
        assert!(matches!(
            route_processor_alert("counter-a", "warning", "t", "b", false),
            KernelAction::Report { message, .. } if message.contains("suppressed")
        ));
        assert!(matches!(
            route_processor_alert("counter-a", "loud", "t", "b", true),
            KernelAction::Report { .. }
        ));
    }

    #[test]
    fn publish_error_str_maps_each_variant_to_its_wit_name() {
        use contract::PublishError;
        assert_eq!(
            publish_error_str(PublishError::NotPermitted),
            "not-permitted"
        );
        assert_eq!(
            publish_error_str(PublishError::InvalidPayload),
            "invalid-payload"
        );
        assert_eq!(
            publish_error_str(PublishError::QuotaExceeded),
            "quota-exceeded"
        );
    }

    #[test]
    fn first_connect_with_no_components_still_emits_ready_and_publishes_connected() {
        let mut core = KernelCore::new();
        let actions = core.on_event(&connected_event(vec![]), |_, _| false);
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
    fn first_connect_mounts_two_instances_of_one_kind() {
        // Two protobar instances on one surface: distinct instance ids, one shared
        // kind. Both mount (element defined for the kind) in declaration order.
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &connected_event(vec![entry("p1", "protobar"), entry("p2", "protobar")]),
            |_, _| true,
        );
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::MountComponent {
                    instance: "p1".to_string(),
                    kind: "protobar".to_string(),
                },
                KernelAction::MountComponent {
                    instance: "p2".to_string(),
                    kind: "protobar".to_string(),
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
    }

    #[test]
    fn first_connect_mount_plan_follows_binding_order_and_definedness() {
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &connected_event(entries(&["alpha", "beta", "gamma"])),
            |kind, _| kind != "beta",
        );
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::MountComponent {
                    instance: "alpha".to_string(),
                    kind: "alpha".to_string(),
                },
                KernelAction::ErrorCard {
                    instance: "beta".to_string(),
                    kind: "beta".to_string(),
                    reason: "component module missing".to_string(),
                },
                KernelAction::MountComponent {
                    instance: "gamma".to_string(),
                    kind: "gamma".to_string(),
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
    }

    #[test]
    fn first_connect_mounts_instance_and_counts_its_subscription() {
        // A mounted instance with a bound subscription produces MountComponent and
        // nothing else — the registration model wires no pump; the subscription is
        // only counted into the status table's `ports_attached`.
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &connected_event_full(
                entries(&["echo-stub"]),
                vec![binding("ephemeral:dev-stub", "echo-stub", "messages")],
            ),
            |_, _| true,
        );
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::MountComponent {
                    instance: "echo-stub".to_string(),
                    kind: "echo-stub".to_string(),
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
        assert_eq!(core.instances[0].ports_attached, 1);
    }

    #[test]
    fn first_connect_counts_subscriptions_per_mounted_instance() {
        // Two protobar instances, each with its own channels: each mounted
        // instance's bound input ports are counted into `ports_attached`, keyed on
        // instance. No attach action is emitted — the component registers itself.
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &connected_event_full(
                vec![entry("p1", "protobar"), entry("p2", "protobar")],
                vec![
                    binding("ephemeral:one", "p2", "in"),
                    binding("ephemeral:two", "p1", "feed"),
                    binding("ephemeral:three", "p2", "aux"),
                ],
            ),
            |_, _| true,
        );
        let counts: Vec<(&str, u32)> = core
            .instances
            .iter()
            .map(|s| (s.instance.as_str(), s.ports_attached))
            .collect();
        assert_eq!(counts, vec![("p1", 1u32), ("p2", 2u32)]);
        // Both mounted, so the plan is two MountComponents plus the
        // indicator/ready tail — no per-subscription action.
        assert_eq!(
            without_platform_planes(&actions)
                .iter()
                .filter(|a| matches!(a, KernelAction::MountComponent { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn first_connect_error_carded_instance_gets_no_attach() {
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &connected_event_full(
                entries(&["alpha", "beta"]),
                vec![
                    binding("ephemeral:a", "alpha", "feed"),
                    binding("ephemeral:b", "beta", "feed"),
                ],
            ),
            |kind, _| kind != "beta",
        );
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::MountComponent {
                    instance: "alpha".to_string(),
                    kind: "alpha".to_string(),
                },
                KernelAction::ErrorCard {
                    instance: "beta".to_string(),
                    kind: "beta".to_string(),
                    reason: "component module missing".to_string(),
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
    }

    #[test]
    fn first_connect_subscription_for_unlisted_instance_gets_no_attach() {
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &connected_event_full(
                entries(&["echo-stub"]),
                vec![binding("ephemeral:ghost", "ghost", "feed")],
            ),
            |_, _| true,
        );
        assert_eq!(
            without_platform_planes(&actions),
            vec![
                KernelAction::MountComponent {
                    instance: "echo-stub".to_string(),
                    kind: "echo-stub".to_string(),
                },
                KernelAction::RemoveConnectIndicator,
                KernelAction::EmitReady,
            ]
        );
    }

    #[test]
    fn reconnect_with_equal_bindings_republishes_connected() {
        let subs = vec![binding("ephemeral:dev-stub", "echo-stub", "messages")];
        let mut core = KernelCore::new();
        core.on_event(
            &connected_event_full(entries(&["echo-stub"]), subs.clone()),
            |_, _| true,
        );
        core.on_event(
            &Event::Disconnected {
                reason: DisconnectReason::LivenessTimeout,
            },
            |_, _| true,
        );
        assert_eq!(core.link_state(), &LinkState::Reconnecting);
        let actions = core.on_event(
            &connected_event_full(entries(&["echo-stub"]), subs),
            |_, _| true,
        );
        // The only action is the connected link-state publish (a platform plane).
        assert_eq!(without_platform_planes(&actions), vec![]);
        assert_eq!(
            control_body(&actions, LOCAL_LINK_STATE_CHANNEL),
            r#"{"v":1,"state":"connected"}"#
        );
        assert_eq!(core.link_state(), &LinkState::Connected);
    }

    #[test]
    fn reconnect_with_changed_bindings_requests_reload() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(entries(&["echo-stub"])), |_, _| true);
        let actions = core.on_event(&connected_event(entries(&["protobar"])), |_, _| true);
        assert_eq!(
            actions,
            vec![KernelAction::RequestReload {
                reason: "bindings changed".to_string(),
            }]
        );
    }

    #[test]
    fn reconnect_with_changed_subscriptions_requests_reload() {
        let mut core = KernelCore::new();
        core.on_event(&connected_event(entries(&["echo-stub"])), |_, _| true);
        let changed = connected_event_full(
            entries(&["echo-stub"]),
            vec![binding("ephemeral:dev-stub", "echo-stub", "messages")],
        );
        let actions = core.on_event(&changed, |_, _| true);
        assert_eq!(
            actions,
            vec![KernelAction::RequestReload {
                reason: "bindings changed".to_string(),
            }]
        );
    }

    // ── publish results / stragglers ──

    #[test]
    fn ok_publish_result_is_noop() {
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &Event::PublishResult {
                instance: "echo-stub".to_string(),
                port: "out".to_string(),
                correlation: 1,
                status: PublishStatus::Ok,
            },
            |_, _| false,
        );
        assert!(actions.is_empty());
        assert_eq!(core.link_state(), &LinkState::Connecting);
    }

    #[test]
    fn non_ok_publish_result_warns_and_reports_without_touching_link_state() {
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &Event::PublishResult {
                instance: "echo-stub".to_string(),
                port: "out".to_string(),
                correlation: 2,
                status: PublishStatus::RateLimited,
            },
            |_, _| false,
        );
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
        for status in [
            PublishStatus::RateLimited,
            PublishStatus::BodyTooLarge { len: 32, max: 16 },
            PublishStatus::UnboundPort,
            PublishStatus::NotConnected,
            PublishStatus::ConnectionLost,
            PublishStatus::Failed,
        ] {
            let mut core = KernelCore::new();
            let actions = core.on_event(
                &Event::PublishResult {
                    instance: "echo-stub".to_string(),
                    port: "out".to_string(),
                    correlation: 7,
                    status,
                },
                |_, _| false,
            );
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
    fn straggler_discarded_emits_single_debug_report() {
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &Event::StragglerDiscarded {
                channel: "ephemeral:demo".to_string(),
                seq: 9,
                dropped: 7,
            },
            |_, _| false,
        );
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
        let actions = core.on_event(
            &Event::StragglerDiscarded {
                channel: "ephemeral:demo".to_string(),
                seq: 9,
                dropped: 7,
            },
            |_, _| false,
        );
        let [KernelAction::Report { subject, .. }] = actions.as_slice() else {
            panic!("expected a single Report, got {actions:?}");
        };
        assert_eq!(subject.as_deref(), None);
    }

    #[test]
    fn a_malformed_publish_intent_from_a_mounted_instance_is_attributed_to_it() {
        // The drop is a fact about the component that dispatched the malformed
        // event, and the instance resolved, so the report names it — same rule as
        // the rejection path, applied at the trust boundary.
        let action = route_publish_intent(
            Some("echo-stub"),
            "BRENN-ECHO-STUB",
            Some("out"),
            Some("body"),
            OptionalField::Malformed,
        );
        let KernelAction::Report { subject, .. } = action else {
            panic!("expected a Report, got {action:?}");
        };
        assert_eq!(subject.as_deref(), Some("echo-stub"));
    }

    #[test]
    fn a_publish_intent_from_an_unresolved_target_names_no_subject() {
        // No mounted instance resolved, so there is nothing to attribute to and
        // guessing would misattribute. The `None` here is the honest answer, not
        // the oversight the rejection path had.
        let action = route_publish_intent(
            None,
            "SPAN",
            Some("out"),
            Some("body"),
            OptionalField::Absent,
        );
        let KernelAction::Report { subject, .. } = action else {
            panic!("expected a Report, got {action:?}");
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
    fn missing_module_marks_instance_failed_and_connect_emits_initial_status() {
        let mut core = KernelCore::new();
        let event =
            connected_event_full(vec![entry("ok", "good"), entry("bad", "missing")], vec![]);
        // Only "good" has a defined element; "missing" fails the mount plan.
        let actions = core.on_event(&event, |kind, _| kind == "good");
        let instances = status_within(&actions);
        let bad = instances
            .iter()
            .find(|i| i.instance == "bad")
            .expect("bad row");
        assert_eq!(bad.state, InstanceState::Failed);
        assert_eq!(bad.reason.as_deref(), Some("component module missing"));
        let ok = instances
            .iter()
            .find(|i| i.instance == "ok")
            .expect("ok row");
        assert_eq!(ok.state, InstanceState::Mounted);
        assert_eq!(ok.reason, None);
    }

    #[test]
    fn component_panic_marks_failed_and_emits_immediate_status() {
        let mut core = connect(vec![entry("m1", "meeting")], false);
        let actions = core.on_component_panic(Some("m1"), Some("boom"), |_| true);
        let m1 = status_within(&actions)
            .iter()
            .find(|i| i.instance == "m1")
            .expect("m1 row");
        assert_eq!(m1.state, InstanceState::Failed);
        assert_eq!(m1.reason.as_deref(), Some("component panicked: boom"));
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
        core.on_event(&event, |_, _| true);
        let actions = core.on_status_tick();
        let p1 = only_status(&actions)
            .iter()
            .find(|i| i.instance == "p1")
            .expect("p1 row");
        assert_eq!(p1.ports_attached, 1);
    }
}

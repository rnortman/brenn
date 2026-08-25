//! DOM-free kernel decision core.
//!
//! Pure state and transition logic over the surface client's control-plane
//! vocabulary. It holds no web-sys handles and compiles and unit-tests on the
//! host target; the wasm effect executor consumes the [`KernelAction`]s it
//! emits.

use std::collections::{BTreeSet, HashMap, HashSet};

use brenn_attach_proto::AlertSeverity;
use brenn_envelope::grants::ComponentGrant;

use crate::schema::bindings::BindingsDocument;
use crate::schema::telemetry::InstanceReport;
use crate::schema::{
    CONTROL_PLANE_VERSION, InstanceState, LOCAL_LINK_STATE_CHANNEL, LOCAL_SURFACE_STATE_CHANNEL,
    LinkState, LinkStateBody, LogLevel, SurfaceStateBody, SurfaceStateInstance,
};
use crate::session::Event;
use crate::{PublishStatus, Urgency};
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

/// The drop-and-report for a well-formed publish the kernel could not buffer: no
/// activation of the dispatching instance is on the stack.
///
/// The component is *also* answered `not-permitted` on the event's status field,
/// so this is the operator's copy rather than the component's. Both exist because
/// the two audiences differ: the SDK turns the status into a panic at the call
/// site, and a non-SDK dispatcher — page script, a hand-rolled module — leaves no
/// other trace.
///
/// There is no second publish path for it to take. Every component-origin publish
/// is an activation's, buffered and flushed iff the activation returns ok; a
/// publish made outside one has no flush boundary to belong to and is refused
/// rather than sent on its own.
pub fn unbuffered_publish_refused(instance: &str, port: &str) -> KernelAction {
    KernelAction::Report {
        level: LogLevel::Warn,
        message: format!(
            "refused {} of port {port:?} from component {instance}: no activation of it is in \
             flight, and a publish has no path outside one",
            contract::PORT_PUBLISH
        ),
        subject: Some(instance.to_string()),
    }
}

/// A `brenn-port-defer` event's untrusted detail, as read at the
/// kernel↔component trust boundary.
///
/// Every field arrives as the DOM executor read it and nothing more: `op`/`port`
/// are `None` for a missing or non-string value, and the three string-typed
/// numerics keep their [`OptionalField`] three-state because which of them an op
/// requires is the router's decision, not the reader's. Grouped into a struct
/// rather than spread across the listener's callback because five untrusted
/// fields on one event is where a positional argument list stops being readable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferDetail {
    /// Which op: [`contract::DEFER_OP_PUBLISH`], [`contract::DEFER_OP_CANCEL`] or
    /// [`contract::DEFER_OP_EDIT`].
    pub op: Option<String>,
    /// The output port the op names.
    pub port: Option<String>,
    /// Decimal-string position in the port's deferred window (cancel, edit).
    pub index: OptionalField,
    /// The message body (publish), or its replacement (edit).
    pub body: OptionalField,
    /// Decimal-string epoch-millisecond release time (publish), or its
    /// replacement (edit).
    pub deliver_after: OptionalField,
}

/// One well-formed deferred-message op, resolved to the instance that dispatched
/// it and ready for the in-flight activation's buffer.
///
/// Not a [`KernelAction`], unlike a publish, because there is no effect to apply
/// outside an activation: every op here is buffered-only
/// ([`contract::PORT_DEFER`]), so the only two outcomes are "hand it to the
/// buffer" and "drop and report", and the router returns them as the two arms of a
/// `Result`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeferIntent {
    /// Park `body` on the port's channel until `deliver_after` (epoch ms UTC).
    Publish {
        instance: String,
        port: String,
        body: String,
        deliver_after: u64,
    },
    /// Unpark the message at `index` in the port's deferred window.
    Cancel {
        instance: String,
        port: String,
        index: u32,
    },
    /// Rewrite the message at `index`: its body, its release time, or both. `None`
    /// leaves that half alone.
    Edit {
        instance: String,
        port: String,
        index: u32,
        body: Option<String>,
        deliver_after: Option<u64>,
    },
}

impl DeferIntent {
    /// The op's contract name, for the breadcrumb a refused op leaves.
    pub fn op_name(&self) -> &'static str {
        match self {
            DeferIntent::Publish { .. } => contract::DEFER_OP_PUBLISH,
            DeferIntent::Cancel { .. } => contract::DEFER_OP_CANCEL,
            DeferIntent::Edit { .. } => contract::DEFER_OP_EDIT,
        }
    }

    /// The instance that dispatched the op — the routing identity the DOM
    /// executor resolved, never anything the detail claimed.
    pub fn instance(&self) -> &str {
        match self {
            DeferIntent::Publish { instance, .. }
            | DeferIntent::Cancel { instance, .. }
            | DeferIntent::Edit { instance, .. } => instance,
        }
    }
}

/// Read one of [`DeferDetail`]'s decimal-string numerics: `Ok(None)` for an
/// omitted field, `Err(())` for a non-string one or one that is not a decimal
/// integer in range. Callers that require the field turn `None` into malformed
/// themselves, so absence stays the router's judgement.
fn optional_number<T: std::str::FromStr>(field: &OptionalField) -> Result<Option<T>, ()> {
    match field {
        OptionalField::Absent => Ok(None),
        OptionalField::Present(raw) => raw.parse::<T>().map(Some).map_err(|_| ()),
        OptionalField::Malformed => Err(()),
    }
}

/// The drop-and-report for a `brenn-port-defer` whose detail does not spell a
/// well-formed op. Malformed detail from a mounted instance is never coerced into
/// an op: a guessed index names another message, and a guessed release time
/// schedules a message for a moment the component never chose.
fn malformed_defer(instance: &str, target_tag: &str, detail: &str) -> KernelAction {
    KernelAction::Report {
        level: LogLevel::Warn,
        message: format!(
            "dropped malformed {} from <{target_tag}>: {detail}",
            contract::PORT_DEFER
        ),
        subject: Some(instance.to_string()),
    }
}

/// The drop-and-report for a well-formed op the kernel could not buffer: no
/// activation of the dispatching instance is on the stack.
///
/// Not a status a component can read — by the time the kernel knows, the dispatch
/// is the only thing that happened — and deliberately not an immediate effect
/// either: the whole point of the buffered-only rule is that a schedule staged
/// outside the flush boundary must not exist.
pub fn unbuffered_defer_refused(intent: &DeferIntent) -> KernelAction {
    KernelAction::Report {
        level: LogLevel::Warn,
        message: format!(
            "dropped {} {} from component {}: no activation of it is in flight, and a \
             deferred-message op has no unbuffered path",
            contract::PORT_DEFER,
            intent.op_name(),
            intent.instance()
        ),
        subject: Some(intent.instance().to_string()),
    }
}

/// Route a component's `brenn-port-defer` intent to the op the in-flight
/// activation's buffer should be offered, or to the drop-and-report for a detail
/// that does not spell one.
///
/// Dispatch identity resolves exactly as [`route_publish_intent`]: `instance` is
/// the DOM-resolved mounted-instance id for the retargeted target, and a target
/// that does not resolve to a mounted instance element is dropped with `Report`.
/// `target_tag` is carried for the breadcrumb.
///
/// What each op requires of the detail is stated here and nowhere else: a publish
/// needs a body and a release time and no index; a cancel needs an index; an edit
/// needs an index and takes the other two as present-to-change / absent-to-leave.
/// Fields an op does not read are ignored rather than rejected, matching every
/// other event on this seam.
pub fn route_defer_intent(
    instance: Option<&str>,
    target_tag: &str,
    detail: DeferDetail,
) -> Result<DeferIntent, KernelAction> {
    let instance = require_mounted_instance(instance, target_tag, contract::PORT_DEFER)?;
    let Some(port) = detail.port else {
        return Err(malformed_defer(
            instance,
            target_tag,
            "port must be a string",
        ));
    };
    // Each field is read — and so judged — only by the ops that use it. Parsing
    // them all up front would reject a publish carrying a stray malformed
    // `index`, which is precisely the "ignored rather than rejected" this seam
    // promises, and would blame a field the op never looks at.
    let index = || {
        optional_number::<u32>(&detail.index)
            .map_err(|()| malformed_defer(instance, target_tag, "index must be a decimal u32"))
    };
    let deliver_after = || {
        optional_number::<u64>(&detail.deliver_after).map_err(|()| {
            malformed_defer(
                instance,
                target_tag,
                "deliver_after must be decimal epoch milliseconds",
            )
        })
    };
    let body = || match &detail.body {
        OptionalField::Absent => Ok(None),
        OptionalField::Present(body) => Ok(Some(body.clone())),
        OptionalField::Malformed => Err(malformed_defer(
            instance,
            target_tag,
            "body must be a string",
        )),
    };
    match detail.op.as_deref() {
        Some(contract::DEFER_OP_PUBLISH) => {
            let (body, deliver_after) = (body()?, deliver_after()?);
            let instance = instance.to_string();
            match (body, deliver_after) {
                (Some(body), Some(deliver_after)) => Ok(DeferIntent::Publish {
                    instance,
                    port,
                    body,
                    deliver_after,
                }),
                _ => Err(malformed_defer(
                    &instance,
                    target_tag,
                    "a deferred publish needs both body and deliver_after",
                )),
            }
        }
        Some(contract::DEFER_OP_CANCEL) => {
            let index = index()?;
            let instance = instance.to_string();
            match index {
                Some(index) => Ok(DeferIntent::Cancel {
                    instance,
                    port,
                    index,
                }),
                None => Err(malformed_defer(
                    &instance,
                    target_tag,
                    "a cancel needs an index",
                )),
            }
        }
        Some(contract::DEFER_OP_EDIT) => {
            let (index, body, deliver_after) = (index()?, body()?, deliver_after()?);
            let instance = instance.to_string();
            match index {
                Some(index) => Ok(DeferIntent::Edit {
                    instance,
                    port,
                    index,
                    body,
                    deliver_after,
                }),
                None => Err(malformed_defer(
                    &instance,
                    target_tag,
                    "an edit needs an index",
                )),
            }
        }
        _ => Err(malformed_defer(
            instance,
            target_tag,
            "op must be publish, cancel or edit",
        )),
    }
}

/// One well-formed sync-call request, resolved to the instance that dispatched
/// it.
///
/// Not a [`KernelAction`], for [`DeferIntent`]'s reason: there is no effect to
/// apply outside an activation. The only two outcomes are "ask the sync door for
/// an activation" and "drop and report", which the router returns as the two arms
/// of a `Result`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncIntent {
    /// The dispatching instance — the routing identity the DOM executor resolved,
    /// never anything the detail claimed.
    pub instance: String,
    /// The sync port the request arrives on, chosen by the component.
    pub port: String,
    /// The request payload, opaque to the kernel.
    pub body: String,
}

/// Route a component's `brenn-activation-sync` intent to the request the sync
/// door should run, or to the drop-and-report for a detail that does not spell
/// one.
///
/// Dispatch identity resolves exactly as [`route_publish_intent`]: `instance` is
/// the DOM-resolved mounted-instance id for the retargeted target, and a target
/// that does not resolve to a mounted instance element is dropped with `Report`.
/// `target_tag` is carried for the breadcrumb.
///
/// `port` and `body` are `None` when the detail omitted them or carried a
/// non-string value. Malformed detail is never coerced into a request: a guessed
/// port name would activate the component on a port it never asked for, and the
/// entry has no way to tell that apart from a real one.
pub fn route_sync_intent(
    instance: Option<&str>,
    target_tag: &str,
    port: Option<&str>,
    body: Option<&str>,
) -> Result<SyncIntent, KernelAction> {
    let instance = require_mounted_instance(instance, target_tag, contract::ACTIVATION_SYNC)?;
    match (port, body) {
        (Some(port), Some(body)) => Ok(SyncIntent {
            instance: instance.to_string(),
            port: port.to_string(),
            body: body.to_string(),
        }),
        _ => Err(KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "dropped malformed {} from <{target_tag}>: port and body must be strings",
                contract::ACTIVATION_SYNC
            ),
            subject: Some(instance.to_string()),
        }),
    }
}

/// The drop-and-report for a well-formed request the kernel would not admit.
///
/// Every refusal is a bug — see [`crate::outward::SyncRefusal`] — so the sentence
/// is the operator's account of which one, alongside the `refused` status the
/// requester itself faults on.
///
/// A refusal is reported by instance and port alone, never by payload content.
pub fn sync_refused(
    instance: &str,
    port: &str,
    refusal: &crate::outward::SyncRefusal,
) -> KernelAction {
    KernelAction::Report {
        level: LogLevel::Warn,
        message: refusal.describe(instance, port),
        subject: Some(instance.to_string()),
    }
}

impl KernelCore {
    /// Route a component's log intent to a component-log action, gated on that
    /// component's own `log` grant.
    ///
    /// One router for both ABIs, because the only difference between them is how the
    /// call arrived — the same shape [`Self::route_alert`] takes. `target_tag` states which
    /// seam it came through: `Some(tag)` is a `dom` component's delegated
    /// `brenn-log` event, whose `instance` is the DOM-resolved mounted-instance id
    /// for the retargeted target — a target that resolves to no mounted instance is
    /// dropped with a `Report` rather than attributed by guess. `None` is a headless
    /// processor's `log.*` import, whose identity is the loader's closure over the
    /// instance it instantiated for; there is no element to resolve and nothing to
    /// check it against.
    ///
    /// The gate is the instance's own right, read here off
    /// [`instance_grants`](Self::instance_grants) rather than taken from the
    /// caller: a seam states which instance is asking and this router decides
    /// what it may do, so no entry point can hand in a verdict of its own. An
    /// ungranted instance's log is dropped with a suppression breadcrumb naming
    /// it, never forwarded as a `Log` frame.
    ///
    /// `level` and `message` are `None` when a `dom` component's event detail omitted
    /// them or carried a non-string value (untrusted component-supplied detail); a
    /// processor's WIT call always states both. `level` is additionally the untrusted
    /// lowercase log-level wire string, parsed via [`proto::LogLevel::from_wire_str`].
    /// A missing/non-string field or an unrecognized `level` is dropped-and-reported
    /// as malformed — never coerced into a well-formed `Log` frame, which would
    /// launder a component bug into a server log line at a level the component never
    /// chose. For a processor an unrecognized level is transpile-glue drift rather
    /// than a component typo, but the answer is the same.
    ///
    /// # Panics
    ///
    /// On a `None` instance with no `target_tag`: a processor log carries the
    /// identity its loader closed over, so an absent one is a kernel bug rather than
    /// component input.
    pub fn route_log(
        &self,
        instance: Option<&str>,
        target_tag: Option<&str>,
        level: Option<&str>,
        message: Option<&str>,
    ) -> KernelAction {
        let instance = match target_tag {
            Some(tag) => match require_mounted_instance(instance, tag, contract::COMPONENT_LOG) {
                Ok(instance) => instance,
                Err(drop) => return drop,
            },
            None => instance.expect("a processor log names the instance its loader closed over"),
        };
        if !self.instance_granted(instance, ComponentGrant::Log) {
            return ungranted_capability(instance, ComponentGrant::Log, contract::COMPONENT_LOG);
        }
        match (level.and_then(LogLevel::from_wire_str), message) {
            (Some(level), Some(message)) => KernelAction::ComponentLog {
                instance: instance.to_string(),
                level,
                message: message.to_string(),
            },
            _ => KernelAction::Report {
                level: LogLevel::Warn,
                message: format!(
                    "dropped malformed {} from {}: level must be a known log \
                     level and message a string",
                    contract::COMPONENT_LOG,
                    component_origin(instance, target_tag)
                ),
                subject: Some(instance.to_string()),
            },
        }
    }

    /// Route a component's alert intent to an alert action, gated on that
    /// component's own `alert` grant.
    ///
    /// One router for both ABIs, because the only difference between them is how the
    /// call arrived. `target_tag` states which seam it came through: `Some(tag)` is a
    /// `dom` component's delegated `brenn-alert` event, whose `instance` is the
    /// DOM-resolved mounted-instance id for the retargeted target — a target that
    /// resolves to no mounted instance is dropped with a `Report` rather than
    /// attributed by guess. `None` is a headless processor's `alert.*` import, whose
    /// identity is the loader's closure over the instance it instantiated for; there
    /// is no element to resolve and nothing to check it against.
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
    /// `severity`/`title`/`body` are `None` when a `dom` component's event detail
    /// omitted them or carried a non-string value (untrusted component-supplied
    /// detail); a processor's WIT call always states all three. `severity` is
    /// additionally the untrusted lowercase severity wire string, parsed via
    /// [`AlertSeverity::from_wire_str`]. A missing/non-string field or an
    /// unrecognized `severity` is dropped-and-reported as malformed — never coerced
    /// into a well-formed `Alert`.
    ///
    /// # Panics
    ///
    /// On a `None` instance with no `target_tag`: a processor alert carries the
    /// identity its loader closed over, so an absent one is a kernel bug rather than
    /// component input.
    pub fn route_alert(
        &self,
        instance: Option<&str>,
        target_tag: Option<&str>,
        severity: Option<&str>,
        title: Option<&str>,
        body: Option<&str>,
    ) -> KernelAction {
        let instance = match target_tag {
            Some(tag) => match require_mounted_instance(instance, tag, contract::COMPONENT_ALERT) {
                Ok(instance) => instance,
                Err(drop) => return drop,
            },
            None => instance.expect("a processor alert names the instance its loader closed over"),
        };
        if !self.instance_granted(instance, ComponentGrant::Alert) {
            return ungranted_capability(
                instance,
                ComponentGrant::Alert,
                contract::COMPONENT_ALERT,
            );
        }
        match (severity.and_then(AlertSeverity::from_wire_str), title, body) {
            (Some(severity), Some(title), Some(body)) => KernelAction::Alert {
                attribution: Some(instance.to_string()),
                severity,
                title: title.to_string(),
                body: body.to_string(),
            },
            _ => KernelAction::Report {
                level: LogLevel::Warn,
                message: format!(
                    "dropped malformed {} from {}: severity must be a known severity \
                     and title and body strings",
                    contract::COMPONENT_ALERT,
                    component_origin(instance, target_tag)
                ),
                subject: Some(instance.to_string()),
            },
        }
    }
}

/// Route a component's config read to the instance and key it names, or to the
/// drop-and-report for a detail that does not spell one.
///
/// Dispatch identity resolves exactly as [`route_publish_intent`]: `instance` is
/// the DOM-resolved mounted-instance id for the retargeted target, and a target
/// that does not resolve to a mounted instance element is dropped with `Report`.
/// `target_tag` is carried for the breadcrumb.
///
/// `key` is `None` when the detail omitted it or carried a non-string value. A
/// guessed key would answer a read the component never made, so it is dropped
/// and reported instead. Either way the seam still writes the absence answer:
/// the reader faults on a missing one.
///
/// The grant is not read here — [`KernelCore::component_config_get`] holds that
/// gate, because it is the thing that would otherwise serve the value.
pub fn route_config_get<'a>(
    instance: Option<&'a str>,
    target_tag: &str,
    key: Option<&'a str>,
) -> Result<(&'a str, &'a str), KernelAction> {
    let instance = require_mounted_instance(instance, target_tag, contract::CONFIG_GET)?;
    match key {
        Some(key) => Ok((instance, key)),
        None => Err(KernelAction::Report {
            level: LogLevel::Warn,
            message: format!(
                "dropped malformed {} from <{target_tag}>: key must be a string",
                contract::CONFIG_GET
            ),
            subject: Some(instance.to_string()),
        }),
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

/// How a dropped intent names where it came from: the dispatching element for a
/// `dom` component, the instance itself for a headless one that has no element.
fn component_origin(instance: &str, target_tag: Option<&str>) -> String {
    match target_tag {
        Some(tag) => format!("<{tag}>"),
        None => format!("processor {instance}"),
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
    /// Create the instance's `brenn-<kind>` custom element and append it inside
    /// the instance's kernel-owned wrapper.
    MountComponent { instance: String, kind: String },
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
    /// Resolve the instance's output `port` to a channel and publish `body`. A
    /// synchronous rejection is handled by the executor as a `Report`, matching a
    /// non-`Ok` `PublishResult`.
    ///
    /// `urgency` is the component's per-message override; `None` sends no urgency
    /// on the frame, which the server reads as "the port's configured default".
    /// The kernel deliberately does not substitute the default itself: the
    /// authoritative value is the wiring the page is running on, which this core
    /// does not resolve ports against.
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
    /// wiring. `false` before the page is first configured.
    fn is_processor_instance(&self, instance: &str) -> bool {
        self.bindings.as_ref().is_some_and(|b| {
            b.components
                .iter()
                .any(|c| c.instance == instance && c.abi == schema::Abi::Processor)
        })
    }

    /// Serve one config read for an instance of either ABI from the map its
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
                contract::CONFIG_GET,
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
                self.on_connected(bindings, is_element_defined)
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
    /// On a reconnect (a document already stored) the page has already reconciled
    /// its stores and resubscribed with resume, so this republishes the connected
    /// link state and nothing else. A document that differs from the one in force
    /// arrives as its own [`Event::WiringChanged`], which is what reloads.
    fn on_connected(
        &mut self,
        bindings: &BindingsDocument,
        is_element_defined: impl Fn(&str, &str) -> bool,
    ) -> Vec<KernelAction> {
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
        // configured component, `mounted` when its element is defined or `failed`
        // when its module never registered — the same decision the mount plan
        // makes. Headless instances (a component in no layout slot) are tracked
        // identically; the table has no slot concept.
        self.instances = Vec::with_capacity(bindings.components.len());
        let mut actions = Vec::new();
        actions.extend(self.dark_overlay_instrument_report(bindings));
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
                entry.abi == schema::Abi::Dom && is_element_defined(&entry.kind, &entry.instance);
            if is_chrome && !mountable {
                return vec![KernelAction::RequestReload {
                    reason: "chrome mount failed".to_string(),
                }];
            }
            // The one thing the ABI still decides in this core: a `dom` component
            // renders into an element and a headless one has nowhere to render.
            // DOM-forced. Every other per-instance decision — grants, config,
            // ports, logs, alerts — reads the same entry the same way for both.
            let (state, reason) = if entry.abi == schema::Abi::Processor {
                // Headless by construction: no element to check, no wrapper, no
                // mount. The bootstrap loader instantiates the transpiled module
                // and registers the instance's `receive`; the row sits `Pending`
                // until `on_processor_register` admits that registration, and
                // becomes `Failed` if the loader reports the instantiation or
                // registration failed. Chrome is a `dom` component by definition,
                // so the is_chrome check above can never select this arm.
                headless.insert(entry.instance.as_str());
                (InstanceState::Pending, None)
            } else if entry.abi != schema::Abi::Dom {
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
    /// wiring) — the membership check the panic-subject filter needs. `false`
    /// before the page is first configured.
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
    /// additionally emits one `Alert { None, Warning, "component panic:
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
            actions.push(KernelAction::Alert {
                // The kernel's own statement, not the dead component's: a panicked
                // instance is in no position to hold a capability, and the operator
                // must hear about the death whatever the instance was granted.
                attribution: None,
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
                reason: DetachReason::LivenessTimeout,
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
                reason: DetachReason::LivenessTimeout,
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

    use brenn_attach_client::conn::DetachReason;
    use brenn_attach_proto::VersionRange;

    use crate::PublishStatus;
    use crate::schema::bindings::{BINDINGS_DOCUMENT_VERSION, PlatformSection};
    use crate::schema::{Abi, Binding, ComponentEntry};

    // ── shared builders ───────────────────────────────────────────────────

    /// One component instance whose id differs from its kind.
    fn entry(instance: &str, kind: &str) -> ComponentEntry {
        ComponentEntry {
            instance: instance.to_string(),
            kind: kind.to_string(),
            abi: Abi::Dom,
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
    fn connect(components: Vec<ComponentEntry>, alert_granted: bool) -> KernelCore {
        let mut core = KernelCore::new();
        core.on_event(
            &connected_event_granted(components, vec![], alert_granted),
            |_, _| true,
        );
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

    // ── route_sync_intent ─────────────────────────────────────────────────

    #[test]
    fn sync_intent_from_mounted_instance_carries_port_and_body_through() {
        let intent =
            route_sync_intent(Some("p1"), "brenn-protobar", Some("ack"), Some("{\"i\":2}"))
                .expect("a well-formed request from a mounted instance routes");
        assert_eq!(
            intent,
            SyncIntent {
                instance: "p1".to_string(),
                port: "ack".to_string(),
                body: "{\"i\":2}".to_string(),
            }
        );
    }

    #[test]
    fn sync_intent_from_unresolved_target_is_dropped_and_reported() {
        // Identical posture to every other event on this seam: the instance is
        // the DOM executor's answer, and a target that resolves to nothing is
        // unattributable rather than guessed at.
        for tag in ["brenn-protobar", "button"] {
            let drop = route_sync_intent(None, tag, Some("ack"), Some("{}"))
                .expect_err("an unresolved target does not route");
            let KernelAction::Report {
                level,
                message,
                subject,
            } = drop
            else {
                panic!("expected Report for <{tag}>");
            };
            assert_eq!(level, LogLevel::Warn);
            assert_eq!(subject, None);
            assert!(message.contains(tag), "message: {message}");
            assert!(
                message.contains(contract::ACTIVATION_SYNC),
                "message: {message}"
            );
        }
    }

    #[test]
    fn sync_intent_with_malformed_detail_is_dropped_as_malformed() {
        // A guessed port would activate the component on a port it never asked
        // for, which its entry cannot tell apart from a real request.
        for (port, body) in [(None, Some("{}")), (Some("ack"), None), (None, None)] {
            let drop = route_sync_intent(Some("p1"), "brenn-protobar", port, body)
                .expect_err("malformed detail does not route");
            let KernelAction::Report {
                level,
                message,
                subject,
            } = drop
            else {
                panic!("expected Report for ({port:?}, {body:?})");
            };
            assert_eq!(level, LogLevel::Warn);
            assert_eq!(subject, Some("p1".to_string()));
            assert!(message.contains("malformed"), "message: {message}");
            assert!(message.contains("brenn-protobar"), "message: {message}");
        }
    }

    #[test]
    fn a_refusal_reports_its_own_sentence_against_the_requesting_instance() {
        let action = sync_refused("p1", "ack", &crate::outward::SyncRefusal::ReEntrant);
        let KernelAction::Report {
            level,
            message,
            subject,
        } = action
        else {
            panic!("a refusal reports");
        };
        assert_eq!(level, LogLevel::Warn);
        assert_eq!(subject, Some("p1".to_string()));
        assert!(message.contains("in flight"), "{message}");
        assert!(
            message.contains("p1") && message.contains("ack"),
            "{message}"
        );
    }

    // ── route_defer_intent ────────────────────────────────────────────────

    /// A `brenn-port-defer` detail with everything omitted but the op and the
    /// port — the shape each case below fills in only what it is about.
    fn defer_detail(op: &str, port: &str) -> DeferDetail {
        DeferDetail {
            op: Some(op.to_string()),
            port: Some(port.to_string()),
            index: OptionalField::Absent,
            body: OptionalField::Absent,
            deliver_after: OptionalField::Absent,
        }
    }

    #[test]
    fn a_deferred_publish_carries_its_body_and_release_time() {
        let intent = route_defer_intent(
            Some("p1"),
            "brenn-protobar",
            DeferDetail {
                body: OptionalField::Present("42".to_string()),
                deliver_after: OptionalField::Present("1770000000000".to_string()),
                ..defer_detail(contract::DEFER_OP_PUBLISH, "out")
            },
        );
        assert_eq!(
            intent,
            Ok(DeferIntent::Publish {
                instance: "p1".to_string(),
                port: "out".to_string(),
                body: "42".to_string(),
                deliver_after: 1_770_000_000_000,
            })
        );
    }

    #[test]
    fn a_cancel_and_an_edit_resolve_their_snapshot_index() {
        assert_eq!(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    index: OptionalField::Present("2".to_string()),
                    ..defer_detail(contract::DEFER_OP_CANCEL, "out")
                }
            ),
            Ok(DeferIntent::Cancel {
                instance: "p1".to_string(),
                port: "out".to_string(),
                index: 2,
            })
        );
        assert_eq!(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    index: OptionalField::Present("0".to_string()),
                    body: OptionalField::Present("later".to_string()),
                    deliver_after: OptionalField::Present("7".to_string()),
                    ..defer_detail(contract::DEFER_OP_EDIT, "out")
                }
            ),
            Ok(DeferIntent::Edit {
                instance: "p1".to_string(),
                port: "out".to_string(),
                index: 0,
                body: Some("later".to_string()),
                deliver_after: Some(7),
            })
        );
    }

    #[test]
    fn an_edit_omits_the_half_it_leaves_alone() {
        // Absent is the contract's "do not touch", and it must not collapse into an
        // empty body or a zero release time — either would rewrite the parked
        // message with something the component never said.
        assert_eq!(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    index: OptionalField::Present("1".to_string()),
                    deliver_after: OptionalField::Present("9".to_string()),
                    ..defer_detail(contract::DEFER_OP_EDIT, "out")
                }
            ),
            Ok(DeferIntent::Edit {
                instance: "p1".to_string(),
                port: "out".to_string(),
                index: 1,
                body: None,
                deliver_after: Some(9),
            })
        );
        // Both halves absent is a well-formed no-op edit, not malformed detail: the
        // WIT lets a guest ask for one, and refusing it here would be the kernel
        // inventing a rule the seam does not have.
        assert_eq!(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    index: OptionalField::Present("1".to_string()),
                    ..defer_detail(contract::DEFER_OP_EDIT, "out")
                }
            ),
            Ok(DeferIntent::Edit {
                instance: "p1".to_string(),
                port: "out".to_string(),
                index: 1,
                body: None,
                deliver_after: None,
            })
        );
    }

    /// Assert an op was dropped as malformed and attributed to its instance, with
    /// `needle` naming the field at fault.
    fn assert_malformed_defer(intent: Result<DeferIntent, KernelAction>, needle: &str) {
        let Err(KernelAction::Report {
            level,
            message,
            subject,
        }) = intent
        else {
            panic!("expected a malformed Report, got {intent:?}");
        };
        assert_eq!(level, LogLevel::Warn);
        assert_eq!(subject.as_deref(), Some("p1"));
        assert!(message.contains("malformed"), "message: {message}");
        assert!(message.contains(needle), "message: {message}");
    }

    #[test]
    fn a_numeric_field_that_is_not_a_decimal_integer_is_malformed() {
        // The seam's numerics are decimal strings, so junk is a component bug and
        // must not be coerced: a guessed index names another message and a guessed
        // release time schedules a moment the component never chose.
        for index in [
            OptionalField::Present("1.5".to_string()),
            OptionalField::Present("-1".to_string()),
            OptionalField::Present(String::new()),
            OptionalField::Present("4294967296".to_string()),
            OptionalField::Malformed,
        ] {
            assert_malformed_defer(
                route_defer_intent(
                    Some("p1"),
                    "brenn-protobar",
                    DeferDetail {
                        index: index.clone(),
                        ..defer_detail(contract::DEFER_OP_CANCEL, "out")
                    },
                ),
                "index",
            );
        }
        for deliver_after in [
            OptionalField::Present("soon".to_string()),
            OptionalField::Present("1e12".to_string()),
            OptionalField::Malformed,
        ] {
            assert_malformed_defer(
                route_defer_intent(
                    Some("p1"),
                    "brenn-protobar",
                    DeferDetail {
                        body: OptionalField::Present("42".to_string()),
                        deliver_after: deliver_after.clone(),
                        ..defer_detail(contract::DEFER_OP_PUBLISH, "out")
                    },
                ),
                "deliver_after",
            );
        }
    }

    #[test]
    fn each_op_needs_the_fields_it_reads() {
        // A publish with no release time is not a publish, and a control op with no
        // index names nothing. Each op states its own requirement, so each is
        // pinned.
        assert_malformed_defer(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    body: OptionalField::Present("42".to_string()),
                    ..defer_detail(contract::DEFER_OP_PUBLISH, "out")
                },
            ),
            "deferred publish",
        );
        assert_malformed_defer(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    deliver_after: OptionalField::Present("7".to_string()),
                    ..defer_detail(contract::DEFER_OP_PUBLISH, "out")
                },
            ),
            "deferred publish",
        );
        assert_malformed_defer(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                defer_detail(contract::DEFER_OP_CANCEL, "out"),
            ),
            "cancel",
        );
        assert_malformed_defer(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                defer_detail(contract::DEFER_OP_EDIT, "out"),
            ),
            "edit",
        );
        // A missing port is malformed for every op: the seam names ports, and there
        // is no default one.
        assert_malformed_defer(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    port: None,
                    index: OptionalField::Present("0".to_string()),
                    ..defer_detail(contract::DEFER_OP_CANCEL, "out")
                },
            ),
            "port",
        );
    }

    #[test]
    fn a_field_an_op_does_not_read_is_ignored_however_malformed() {
        // The seam's stated convention, both directions. A dispatcher that emits a
        // stray field a given op ignores must not have its op dropped — and the
        // drop would name a field the op never looks at, which is a diagnosis
        // dead end on top of the lost work.
        assert_eq!(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    body: OptionalField::Present("42".to_string()),
                    deliver_after: OptionalField::Present("7".to_string()),
                    index: OptionalField::Present("not-a-number".to_string()),
                    ..defer_detail(contract::DEFER_OP_PUBLISH, "out")
                },
            ),
            Ok(DeferIntent::Publish {
                instance: "p1".to_string(),
                port: "out".to_string(),
                body: "42".to_string(),
                deliver_after: 7,
            }),
            "a publish reads no index, so a junk one is ignored"
        );
        assert_eq!(
            route_defer_intent(
                Some("p1"),
                "brenn-protobar",
                DeferDetail {
                    index: OptionalField::Present("2".to_string()),
                    deliver_after: OptionalField::Present("later".to_string()),
                    body: OptionalField::Malformed,
                    ..defer_detail(contract::DEFER_OP_CANCEL, "out")
                },
            ),
            Ok(DeferIntent::Cancel {
                instance: "p1".to_string(),
                port: "out".to_string(),
                index: 2,
            }),
            "a cancel reads neither body nor release time"
        );
    }

    #[test]
    fn an_unknown_op_is_dropped_as_malformed() {
        // The op selector is the only thing that says what the rest of the detail
        // means, so an unrecognized one cannot be guessed at.
        for op in [None, Some("park"), Some("PUBLISH"), Some("")] {
            let mut detail = defer_detail(contract::DEFER_OP_CANCEL, "out");
            detail.op = op.map(str::to_string);
            detail.index = OptionalField::Present("0".to_string());
            assert_malformed_defer(
                route_defer_intent(Some("p1"), "brenn-protobar", detail),
                "op must be",
            );
        }
    }

    #[test]
    fn a_defer_op_from_an_unresolved_target_is_dropped_and_reported() {
        // Same rule as every other event on this seam: a target that resolves to no
        // mounted instance has no subject to name, so nothing is attributed by
        // guess.
        let intent = route_defer_intent(
            None,
            "button",
            defer_detail(contract::DEFER_OP_CANCEL, "out"),
        );
        let Err(KernelAction::Report {
            level,
            message,
            subject,
        }) = intent
        else {
            panic!("expected a Report, got {intent:?}");
        };
        assert_eq!(level, LogLevel::Warn);
        assert_eq!(subject, None);
        assert!(message.contains("button"), "message: {message}");
        assert!(message.contains(contract::PORT_DEFER), "message: {message}");
    }

    #[test]
    fn a_publish_the_buffer_cannot_take_is_reported_against_its_instance() {
        // The component reads `not-permitted` off the detail; this is the
        // operator's copy, and the only trace a non-SDK dispatcher leaves. It names
        // the instance so a looping component draws down its own report budget.
        let action = unbuffered_publish_refused("p1", "out");
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
        assert!(message.contains("\"out\""), "message: {message}");
        assert!(message.contains("in flight"), "message: {message}");
        assert!(
            message.contains(contract::PORT_PUBLISH),
            "message: {message}"
        );
    }

    #[test]
    fn an_op_the_buffer_cannot_take_is_reported_against_its_instance() {
        // The buffered-only rule's other half: there is no immediate path, so an op
        // dispatched with no activation of its instance in flight is dropped with a
        // breadcrumb naming the instance and the op.
        let action = unbuffered_defer_refused(&DeferIntent::Cancel {
            instance: "p1".to_string(),
            port: "out".to_string(),
            index: 0,
        });
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
        assert!(message.contains("cancel"), "message: {message}");
        assert!(message.contains("in flight"), "message: {message}");
    }

    // ── route_log ─────────────────────────────────────────────────────────

    #[test]
    fn component_log_from_mounted_instance_forwards_with_instance() {
        let action =
            routing_core().route_log(Some("p1"), Some("brenn-protobar"), Some("warn"), Some("hi"));
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
            let action = core.route_log(Some("p1"), Some("brenn-protobar"), Some(wire), Some("m"));
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
        let core = routing_core();
        for tag in ["brenn-protobar", "button"] {
            let action = core.route_log(None, Some(tag), Some("warn"), Some("m"));
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
            let action =
                routing_core().route_log(Some("p1"), Some("brenn-protobar"), level, message);
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

    /// An ungranted instance's well-formed log is a breadcrumb, never a `Log`
    /// frame carrying its name.
    #[test]
    fn an_ungranted_instance_logs_nothing_at_either_seam() {
        for target_tag in [Some("brenn-protobar"), None] {
            let action =
                ungranted_core().route_log(Some("p1"), target_tag, Some("warn"), Some("hi"));
            let KernelAction::Report {
                level,
                message,
                subject,
            } = action
            else {
                panic!("expected a Report for {target_tag:?}, got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert_eq!(subject.as_deref(), Some("p1"));
            assert!(message.contains("suppressed"), "message: {message}");
            assert!(message.contains("log capability"), "message: {message}");
        }
    }

    /// The gate is read before the detail: an ungranted instance is told it is
    /// ungranted whatever it sent, rather than learning its detail was malformed
    /// from a capability it does not hold.
    #[test]
    fn the_log_grant_is_checked_ahead_of_the_detail() {
        let action =
            ungranted_core().route_log(Some("p1"), Some("brenn-protobar"), Some("shout"), None);
        assert!(matches!(
            action,
            KernelAction::Report { message, .. } if message.contains("not granted")
        ));
    }

    /// One router, two seams: the headless one names the instance where the DOM
    /// one names the element, and nothing else differs.
    #[test]
    fn a_headless_log_names_the_instance_in_its_drop() {
        let action = routing_core().route_log(Some("counter-a"), None, Some("shout"), Some("m"));
        let KernelAction::Report { message, .. } = action else {
            panic!("expected a Report, got {action:?}");
        };
        assert!(
            message.contains("processor counter-a"),
            "message: {message}"
        );
        assert!(message.contains("malformed"), "message: {message}");
    }

    #[test]
    #[should_panic(expected = "names the instance its loader closed over")]
    fn a_headless_log_with_no_instance_is_a_kernel_bug() {
        let _ = routing_core().route_log(None, None, Some("warn"), Some("m"));
    }

    #[test]
    #[should_panic(expected = "names the instance its loader closed over")]
    fn a_headless_alert_with_no_instance_is_a_kernel_bug() {
        // The alert twin of the log panic: a softened identity here would page
        // the backend under an empty attribution, which it judges undeclared and
        // kills the session for.
        let _ = routing_core().route_alert(None, None, Some("warning"), Some("t"), Some("b"));
    }

    // ── route_config_get ──────────────────────────────────────────────────

    #[test]
    fn a_config_read_resolves_its_instance_and_key() {
        assert_eq!(
            route_config_get(Some("p1"), "brenn-protobar", Some("mode")),
            Ok(("p1", "mode"))
        );
    }

    #[test]
    fn a_config_read_from_an_unresolved_target_is_dropped_and_reported() {
        let Err(KernelAction::Report {
            level,
            message,
            subject,
        }) = route_config_get(None, "button", Some("mode"))
        else {
            panic!("expected a Report");
        };
        assert_eq!(level, LogLevel::Warn);
        assert_eq!(subject, None);
        assert!(message.contains("button"), "message: {message}");
        assert!(message.contains(contract::CONFIG_GET), "message: {message}");
    }

    #[test]
    fn a_config_read_with_no_key_is_dropped_as_malformed() {
        let Err(KernelAction::Report {
            message, subject, ..
        }) = route_config_get(Some("p1"), "brenn-protobar", None)
        else {
            panic!("expected a Report");
        };
        assert_eq!(subject.as_deref(), Some("p1"));
        assert!(message.contains("key must be a string"), "{message}");
    }

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
        let actions = core.on_event(
            &connected_event_granted(
                vec![granted_entry("p1", "protobar", &["ports", "telepathy"])],
                vec![],
                true,
            ),
            |_, _| true,
        );
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
                .any(|a| matches!(a, KernelAction::MountComponent { .. })),
            "no instance is configured from a refused document: {actions:?}"
        );
    }

    /// A server bug rather than skew, and refused on the same terms: with two
    /// entries under one name, the grants in force would come from one and the
    /// config map every other reader finds from the other.
    #[test]
    fn a_document_declaring_one_instance_twice_is_refused_whole() {
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &connected_event_granted(
                vec![
                    granted_entry("p1", "protobar", &["ports"]),
                    granted_entry("p1", "protobar", &["ports", "alert"]),
                ],
                vec![],
                true,
            ),
            |_, _| true,
        );
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
            let action = core.route_alert(
                Some("p1"),
                Some("brenn-protobar"),
                Some(wire),
                Some("t"),
                Some("b"),
            );
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
        let action = ungranted_core().route_alert(
            Some("p1"),
            Some("brenn-protobar"),
            Some("warning"),
            Some("t"),
            Some("b"),
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
        let core = routing_core();
        for tag in ["brenn-protobar", "button"] {
            let action = core.route_alert(None, Some(tag), Some("warning"), Some("t"), Some("b"));
            let KernelAction::Report { level, message, .. } = action else {
                panic!("expected Report for <{tag}>, got {action:?}");
            };
            assert_eq!(level, LogLevel::Warn);
            assert!(message.contains(tag), "message: {message}");
        }
    }

    #[test]
    fn component_alert_with_malformed_detail_from_a_granted_instance_is_dropped_as_malformed() {
        let core = routing_core();
        let cases = [
            (None, Some("t"), Some("b")),
            (Some("warning"), None, Some("b")),
            (Some("warning"), Some("t"), None),
            (Some("warn"), Some("t"), Some("b")),
            (None, None, None),
        ];
        for (severity, title, body) in cases {
            let action =
                core.route_alert(Some("p1"), Some("brenn-protobar"), severity, title, body);
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
            KernelAction::Alert {
                attribution,
                severity,
                title,
                body,
            },
        ] = shown.as_slice()
        else {
            panic!("expected ErrorCard + Report + Alert, got {actions:?}");
        };
        assert_eq!(
            *attribution, None,
            "the kernel pages about the death; the dead instance states nothing"
        );
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
                .filter(|a| matches!(a, KernelAction::Alert { .. }))
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
                reason: DetachReason::LivenessTimeout,
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
            reason: DetachReason::LivenessTimeout,
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
                reason: DetachReason::LivenessTimeout,
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
            grants: vec![],
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

    /// One config map, either ABI. The map is the component entry's; nothing
    /// about the DOM decides whether it can be read, so a `dom` instance holding
    /// `config` reads its own exactly as a headless one does.
    #[test]
    fn config_get_answers_from_welcome_for_both_abis_and_misses_are_none() {
        let mut core = KernelCore::new();
        let mut headless = processor_entry("counter-a", "counter");
        headless
            .config
            .insert("mode".to_string(), "loud".to_string());
        headless.grants = vec!["config".to_string()];
        let mut placed = granted_entry("p1", "protobar", &["config"]);
        placed
            .config
            .insert("mode".to_string(), "quiet".to_string());
        let mut sibling = processor_entry("counter-b", "counter");
        sibling.grants = vec!["config".to_string()];
        core.on_event(&connected_event(vec![headless, placed, sibling]), |_, _| {
            false
        });

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
        let mut entry = processor_entry("counter-a", "counter");
        entry.config.insert("mode".to_string(), "loud".to_string());
        core.on_event(&connected_event(vec![entry]), |_, _| false);

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
    /// authority. The six seams themselves are browser-only, but the decision
    /// they share is not: an instance granted `ports` is admitted whichever
    /// family it arrives on, and one that is not — declared or not — is refused
    /// with a breadcrumb naming the family and the capability.
    #[test]
    fn every_ports_entry_shares_one_verdict() {
        let granted = connect(vec![granted_entry("p1", "protobar", &["ports"])], true);
        let ungranted = ungranted_core();
        // The seam, and the event family it asks under: the two DOM listeners,
        // then the four headless exports.
        let seams = [
            ("dom publish listener", contract::PORT_PUBLISH),
            ("dom defer listener", contract::PORT_DEFER),
            ("brenn_processor_publish", contract::PORT_PUBLISH),
            ("brenn_processor_publish_deferred", contract::PORT_DEFER),
            ("brenn_processor_defer_cancel", contract::PORT_DEFER),
            ("brenn_processor_defer_edit", contract::PORT_DEFER),
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
            core.route_log(Some("counter-a"), None, Some("warn"), Some("hi")),
            KernelAction::ComponentLog {
                instance: "counter-a".to_string(),
                level: LogLevel::Warn,
                message: "hi".to_string(),
            }
        );
        assert!(matches!(
            core.route_log(Some("counter-a"), None, Some("shout"), Some("hi")),
            KernelAction::Report { .. }
        ));

        assert_eq!(
            core.route_alert(
                Some("counter-a"),
                None,
                Some("warning"),
                Some("t"),
                Some("b")
            ),
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
            ungranted_core().route_alert(
                Some("counter-a"),
                None,
                Some("warning"),
                Some("t"),
                Some("b")
            ),
            KernelAction::Report { message, .. } if message.contains("suppressed")
        ));
        assert!(matches!(
            core.route_alert(Some("counter-a"), None, Some("loud"), Some("t"), Some("b")),
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
                reason: DetachReason::LivenessTimeout,
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
    fn changed_wiring_requests_reload() {
        // Which difference made the wiring change is the page's question — it
        // compares the retained bodies — and this is the whole of the platform
        // half's answer to one: state the reload on the link plane so chrome can
        // draw it, then ask the bootstrap for the (capped) reload. A page cannot
        // re-wire the elements it already mounted.
        let mut core = KernelCore::new();
        core.on_event(&connected_event(entries(&["echo-stub"])), |_, _| true);
        let actions = core.on_event(&Event::WiringChanged, |_, _| true);
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
        let actions = core.on_event(
            &Event::Incompatible {
                ours: VersionRange { min: 2, max: 2 },
                theirs: VersionRange { min: 1, max: 1 },
            },
            |_, _| true,
        );
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
    fn a_refused_publish_result_draws_no_second_report() {
        // `Refused` is the one non-`Ok` status the plane guard already reported,
        // with the reason and the publisher attached. A generic warn on top of it
        // says less and doubles what a looping component puts on the error
        // channel — and the buffered path, which answers no publisher, emits only
        // the guard's report. One event, one report, whichever path it took.
        let mut core = KernelCore::new();
        let actions = core.on_event(
            &Event::PublishResult {
                instance: "chrome".to_string(),
                port: "overlay-state".to_string(),
                correlation: 3,
                status: PublishStatus::Refused,
            },
            |_, _| false,
        );
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
        let actions = core.on_event(
            &Event::PlaneRefused {
                instance: "meeting".to_string(),
                port: "overlay-state".to_string(),
                channel: schema::LOCAL_OVERLAY_STATE_CHANNEL.to_string(),
                reason: "only the surface's chrome instance may publish it".to_string(),
            },
            |_, _| false,
        );
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
        let actions = core.on_event(
            &connected_event_takeover(entries(&["chrome", "meeting"]), vec![]),
            |_, _| true,
        );
        let message = instrument_warn(&actions).expect("a warn about the unbound plane");
        assert!(message.contains("chrome"), "message: {message}");
    }

    #[test]
    fn a_wired_or_takeover_free_surface_says_nothing_about_overlay_state() {
        // Bound: the instrument is live, nothing to say.
        let mut core = KernelCore::new();
        let wired = core.on_event(
            &connected_event_takeover(
                entries(&["chrome", "meeting"]),
                vec![overlay_state_output()],
            ),
            |_, _| true,
        );
        assert_eq!(instrument_warn(&wired), None);

        // Nothing wired to the takeover plane: no component can hold an overlay,
        // so an unbound port there is the correct configuration, not a gap.
        let mut core = KernelCore::new();
        let ungranted = core.on_event(
            &connected_event_chrome(entries(&["chrome", "meeting"]), "chrome"),
            |_, _| true,
        );
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
        let actions = core.on_event(&event(vec![takeover_output.clone()]), |_, _| true);
        let message = instrument_warn(&actions).expect("a warn about the unbound plane");
        assert!(message.contains("chrome"), "message: {message}");

        // ...and with chrome's overlay-state output beside it the instrument is
        // live, so nothing is said.
        let mut core = KernelCore::new();
        let wired = core.on_event(
            &event(vec![takeover_output, overlay_state_output()]),
            |_, _| true,
        );
        assert_eq!(instrument_warn(&wired), None);
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

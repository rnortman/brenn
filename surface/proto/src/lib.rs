//! Brenn surface WS wire protocol types.
//!
//! This crate holds the frames and shared contract constants for the surface
//! WebSocket protocol (`/surface/{slug}/ws`). Both ends compile against it: the
//! Rust/Axum backend and the `brenn-surface-kernel` crate (which builds to
//! `wasm32-unknown-unknown` for the kernel and to native for tests). It is kept
//! free of I/O, tokio, and host-only dependencies so the wasm build stays
//! clean — the only dependencies are `serde`, `uuid`, and `brenn-envelope`
//! (itself wasm-clean).
//!
//! **No runtime version skew.** The build-ID handshake rejects any client whose
//! build differs from the server's, so both ends of a live connection always
//! compiled the same `brenn-surface-proto`. Frame-shape stability across
//! development steps protects *sequencing* — clients built against an earlier
//! step keep compiling as the backend grows — not runtime negotiation. A binary
//! or negotiated-version encoding, if ever wanted, is a separate protocol
//! version, not a silent switch.
//!
//! Serde representation: frame enums are `#[serde(tag = "type")]`; inner enums
//! (outcomes, `GapReason`) are `#[serde(tag = "kind")]`. Variant names are
//! PascalCase. JSON text frames. Every shape is pinned by the golden-JSON tests
//! below.
//!
//! **Strictness is at the variant level, not the field level.** An unknown
//! `type`/`kind` tag fails to parse (a protocol violation at the transport). An
//! internally-tagged representation cannot use `deny_unknown_fields`, so unknown
//! *fields* inside a known variant are silently ignored — a deliberate,
//! test-pinned tolerance (`unknown_field_in_known_variant_is_ignored`), not an
//! oversight. Field-level strictness is unnecessary here: the build-ID handshake
//! guarantees a first-party client speaks the exact same frame shapes, and a
//! misnamed field degrades to its `Option`/default rather than to a wrong value.

use std::collections::BTreeMap;

use brenn_envelope::{ChannelScheme, DeliveryClass, MessageEnvelope};
use chrono::{DateTime, Utc};

/// The RFC 8030 urgency ladder, re-exported from the carrier crate.
///
/// Named in this crate's own surface — [`OutputBinding::urgency`],
/// [`ClientFrame::Publish::urgency`], and the `PORT_PUBLISH` detail — so
/// component authors and the kernel reach it through the contract they already
/// depend on rather than taking a direct dependency on `brenn-envelope` for one
/// enum.
pub use brenn_envelope::Urgency;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod layout;

/// Which toolchain artifact backs a component instance, and how the kernel loads
/// it.
///
/// A **build/loading fact only** — never an execution mode, never a capability
/// statement. What a component is allowed to reach is its import profile: a
/// component importing `store`/`mqtt`/`tools` is backend-only, and one importing
/// DOM capability is surface-only. Those are the same rule reading a different
/// profile, not a property of the value below.
///
/// The set is open the way `ChannelScheme` is open: a named value per artifact
/// shape, extended additively. The kernel loads `Dom` and `Processor`; boot
/// rejects the reserved values by name rather than half-supporting them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Abi {
    /// A wasm-bindgen module defining a custom element (`brenn-<kind>`),
    /// speaking the `brenn-surface-contract` DOM-event seam. Surface-only by
    /// profile: it imports DOM capability via wasm-bindgen/web-sys.
    Dom,
    /// A `brenn:processor` component-model artifact — the same artifact that
    /// deploys backend-side under `[[wasm_consumer]]`. Headless by profile: its
    /// world has no DOM imports, so it gets no element, no mount, and no layout
    /// slot, and the kernel calls its exports directly rather than through the
    /// event seam.
    Processor,
    /// Reserved: a TypeScript/Lit component kind.
    DomTs,
    /// Reserved: server-rendered HTML with declarative actions.
    Html,
}

impl Abi {
    /// Every value, so enumerating tests and validators cannot hand-list the set
    /// and silently skip a new one (the `ChannelScheme::ALL` pattern).
    pub const ALL: [Abi; 4] = [Abi::Dom, Abi::Processor, Abi::DomTs, Abi::Html];

    /// The config/wire string for this ABI.
    pub fn as_str(self) -> &'static str {
        match self {
            Abi::Dom => "dom",
            Abi::Processor => "processor",
            Abi::DomTs => "dom-ts",
            Abi::Html => "html",
        }
    }

    /// The ABI for a config/wire string, or `None` when the string names no ABI
    /// this contract defines.
    pub fn parse(s: &str) -> Option<Abi> {
        Abi::ALL.into_iter().find(|abi| abi.as_str() == s)
    }
}

// ---------------------------------------------------------------------------
// Client → server frames
// ---------------------------------------------------------------------------

/// Client → server frame.
///
/// Not `Eq`: the `Geometry` frame carries an `f64` device-pixel-ratio, which has
/// no total equality. `PartialEq` is retained (tests compare extracted fields).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientFrame {
    /// Bus plane. `channel` must be a config-bound subscription channel of
    /// `instance`.
    ///
    /// A subscription belongs to the **principal that binds it**, not to the
    /// page: `instance` names the declared component instance whose binding this
    /// subscribe opens, and (channel, instance) is the subscription's identity
    /// end to end — its own push window, its own `resume` cursor, its own lag.
    /// Two instances bound to one channel open two subscriptions and each is
    /// delivered under its own name at its own position, exactly as two backend
    /// `[[app]]`s are. That is a statement about subscriptions, not about
    /// frames: sibling deliveries of one message travel as targets of a single
    /// `Deliver` (the payload rides the wire once), and the kernel fans them
    /// out.
    ///
    /// `instance` names the owning component; every surface subscription is an
    /// instance's. There is no kernel grain: a surface subscription always
    /// names its instance, and the bare `surface:<slug>` grain is
    /// publisher-only.
    Subscribe {
        channel: String,
        instance: String,
        resume: Option<Cursor>,
    },
    /// Bus plane. `(channel, instance)` must have an active subscription on this
    /// connection.
    Unsubscribe { channel: String, instance: String },
    /// Bus plane. `(instance, port)` must be a config-bound output.
    Publish {
        instance: String,
        port: String,
        body: String,
        correlation: Option<u64>,
        /// Which component a report on the reserved error-report port is *about*.
        ///
        /// Exists because that port's `instance` is the reserved error-report
        /// instance (`#brenn`, `brenn-surface-contract`) — by construction outside
        /// the declared instance set — so the subject cannot ride `instance` the way
        /// an ordinary publish's does. The report's sender sub-identity *is* this
        /// field, once the server admits it against its own declared instance set,
        /// which is what makes a crash-looping component's report flood draw down its
        /// own send budget rather than its neighbours'.
        ///
        /// Validated exactly like `instance`: a value naming an instance outside the
        /// surface's declared set is a protocol violation (kill + log). The claim
        /// surface stays one field — the server never trusts the body's `source`
        /// string, which remains human-readable detail only.
        ///
        /// `None` on every ordinary publish (the sub-identity comes from `instance`)
        /// and on a kernel self-report, which has no component subject and carries
        /// the bare `surface:<slug>` platform identity. Present on any *other* port
        /// is a violation: it would be a claim with no meaning.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject_instance: Option<String>,
        /// The component's per-message urgency override: sender intent about how
        /// hard the bus should work to wake a subscriber.
        ///
        /// `None` ⇒ the port's configured default, which the server holds in its
        /// own boot-resolved output map and also advertises on the port's
        /// [`OutputBinding`]. Absent-means-default rather than the client
        /// restating the default on every frame: the resolved default is
        /// server-side config, so a client echoing it back would be an
        /// unnecessary claim, and a stale echo (bindings changed under a
        /// reconnect) would silently override the operator.
        ///
        /// Unlike `subject_instance` this needs no validation beyond the enum:
        /// urgency is *sender intent*, and the sender is entitled to any rung of
        /// the ladder on a port it is already bound to publish on. What bounds
        /// the traffic is the per-component send budget, not the urgency.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        urgency: Option<Urgency>,
    },
    /// Bus plane. One activation's whole flush, atomically.
    ///
    /// The transport for the activation model's flush-on-ok rule: publishes are
    /// buffered by the kernel during a handler and, iff it returns ok, arrive
    /// here as one frame. The server applies the durable entries in **one
    /// transaction** (all-or-nothing, in call order) and fans the ephemeral ones
    /// out at the same point. `local:` entries never appear — the kernel's own
    /// router commits those page-side and no server sees them.
    ///
    /// Contract: call order is preserved within each delivery class; cross-class
    /// relative ordering is not guaranteed (one class commits in the browser,
    /// another in the server).
    ///
    /// **Every per-entry error is violation-grade, not outcome-grade** — unlike
    /// the single `Publish`, whose `BodyTooLarge` is an outcome. The kernel is
    /// the enforcer here: it checks every entry's port binding, body size, and
    /// the per-activation caps at buffer time and answers the component with the
    /// `processor.wit` error triple. An entry arriving broken at the server
    /// therefore means a non-compliant client — fail2ban signal, not a soft
    /// answer.
    PublishBatch {
        /// The declared instance whose activation produced this flush. The
        /// server derives the sender sub-identity from it against its own
        /// boot-resolved declaration set; an undeclared instance is a violation.
        /// The client asserts no identity — it names its instance, exactly as the
        /// single `Publish` does.
        instance: String,
        /// Routes the eventual [`ServerFrame::PublishBatchResult`] back. Required,
        /// unlike `Publish.correlation`: a batch is only ever produced by a
        /// kernel flush, which always wants to know whether the server took it.
        correlation: u64,
        /// The activation's buffered publishes, in call order. Bounded by
        /// `brenn_budget::MAX_PUBLISHES_PER_ACTIVATION`; a longer batch is a
        /// violation.
        publishes: Vec<BatchEntry>,
    },
    /// Alert plane. Grant-gated (deny-by-default) and disciplined like `Publish`:
    /// an `Alert` from an ungranted surface is a protocol
    /// violation, oversized fields are a violation, and `severity` is required
    /// with no serde default. `(severity, title, body)` is the WIT `alert`
    /// signature verbatim. A conforming kernel learns whether the alert plane is
    /// granted from the server at attach time and suppresses ungranted alerts
    /// client-side, so this frame reaches the server only from a granted surface
    /// (or a non-conforming client).
    Alert {
        severity: AlertSeverity,
        title: String,
        body: String,
    },
    /// Telemetry plane. The browser viewport, reported once after connect and on
    /// debounced resize. `width`/`height` are CSS pixels; `device_pixel_ratio` is
    /// the display density. Accepted unconditionally; out-of-bounds values are a
    /// protocol violation. The server validates bounds, wraps the values into a
    /// server-stamped document, and publishes it to the surface's derived geometry
    /// channel.
    Geometry {
        width: u32,
        height: u32,
        device_pixel_ratio: f64,
    },
    /// Telemetry plane. A per-instance mount-status snapshot, reported on the
    /// status interval and immediately on any transition into `failed`. Accepted
    /// unconditionally; naming an instance the surface does not configure is a
    /// protocol violation. The kernel reports raw facts; the server derives the
    /// health summary and publishes the snapshot to the surface's derived status
    /// channel.
    Status {
        instances: Vec<InstanceReport>,
        uptime_secs: u64,
        counters: StatusCounters,
        /// The overlay the surface's chrome holds, or `None` when it holds none.
        /// Read by the kernel off [`LOCAL_OVERLAY_STATE_CHANNEL`] at the router's
        /// mint point, so the shell reports what chrome actually folded rather
        /// than what the router routed. Naming an instance the surface does not
        /// configure is a protocol violation, like the instance reports.
        overlay: Option<OverlayReport>,
    },
}

/// The held-overlay fact a [`ClientFrame::Status`] report carries, and the
/// `overlay` object of the derived status document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverlayReport {
    /// The instance holding the overlay — one of the surface's configured
    /// instances.
    pub holder: String,
    /// When the hold began: the publish time of the chrome transition the kernel
    /// recorded it from.
    pub since: DateTime<Utc>,
}

/// One buffered publish inside a [`ClientFrame::PublishBatch`].
///
/// Names the **port**, not a channel, exactly as [`ClientFrame::Publish`] does:
/// components see logical port names only, and the server resolves port →
/// channel and default urgency from its own boot-resolved bindings. No
/// `subject_instance`: the reserved error-report port is the kernel's own
/// breadcrumb path, not something an activation flushes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchEntry {
    /// A bound output port of the batch's `instance`. Anything else is a
    /// violation — the kernel already answered the component `not-permitted`.
    pub port: String,
    pub body: String,
    /// The per-call urgency override; `None` ⇒ the port's configured default,
    /// which the server applies. Same absent-means-default rule, and the same
    /// enum-only validation, as [`ClientFrame::Publish::urgency`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urgency: Option<Urgency>,
}

/// One instance's mount status in a [`ClientFrame::Status`] report. The kernel
/// reports the raw facts it already tracks at its mount/attach/panic decision
/// points; the server validates the instance is configured and derives the
/// surface health summary from the set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceReport {
    /// The instance id (routing/mount key), one of the surface's configured
    /// instances.
    pub instance: String,
    /// The component kind backing the instance.
    pub kind: String,
    /// Mount state.
    pub state: InstanceState,
    /// Short failure reason when `state` is `Failed` (module missing, element
    /// undefined, component panic, terminal port event); `None` otherwise.
    pub reason: Option<String>,
    /// Count of delivery pumps attached to this instance's ports.
    pub ports_attached: u32,
}

/// Mount state of one instance, serialized lowercase (`"mounted"`/`"failed"`/
/// `"pending"`) to match the status document schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    /// The instance is wired and delivering. For a `dom` instance that means its
    /// element mounted; for a headless `processor` instance, which has no element,
    /// it means its activation entry is registered — there is nothing else
    /// "mounted" could mean for a component with no DOM.
    Mounted,
    /// The instance is dead: it never loaded, its registration was refused, or it
    /// trapped. Delivery to it has stopped and `reason` says why.
    Failed,
    /// The instance is declared and not yet wired. Reached by a `processor`
    /// instance between the bindings table being built and the bootstrap loader's
    /// registration being admitted; a `dom` instance resolves straight to
    /// `Mounted` or `Failed` at mount-plan time.
    Pending,
}

/// Kernel-side lifetime totals carried in a [`ClientFrame::Status`] report. The
/// extensible counters object; v1 ships the kernel's own totals. Server-side drop
/// counters are a future additive export.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusCounters {
    /// Deliveries received by the kernel over the connection's lifetime.
    pub deliveries: u64,
    /// Publishes the kernel has sent.
    pub publishes: u64,
    /// Component errors/panics the kernel has observed.
    pub errors: u64,
    /// Per-instance breakdown, keyed by instance id. The surface's totals above
    /// answer "is the wall working?"; this answers "which component is doing
    /// it?" — the same principal grain the bus meters and attributes publishes
    /// at, carried onto the plane an operator reads.
    ///
    /// Every key must be one of the surface's configured instances (the server
    /// validates it exactly as it validates `instances`, and a key naming
    /// anything else is a protocol violation). An instance that has neither
    /// published nor dropped may be absent — the map reports what happened, so
    /// an absent key reads as zero.
    pub instances: BTreeMap<String, InstanceCounters>,
}

/// One instance's lifetime totals within [`StatusCounters`].
///
/// Deliberately not a copy of the surface-wide triple. `deliveries` would
/// duplicate what `InstanceReport.ports_attached` already tells an operator
/// about a live instance, and `errors` is bounded at one per instance (an
/// error-carded instance is dead and stops counting), so neither earns a
/// per-instance column. What varies per instance without bound, and so answers
/// a question the totals cannot, is what it *sent* and what it *lost*.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceCounters {
    /// Publishes the kernel queued on this instance's behalf. Counted at the seam
    /// — a publish this instance asked for, whether or not the bus later
    /// accepted it — so it is the instance's *attempt* rate, which is what
    /// reads against its send budget.
    pub publishes: u64,
    /// Messages dropped from this instance's port queues by push overflow
    /// (drop-oldest, counted). Sustained non-zero drops mean the component is
    /// not keeping up with its bindings' `push_depth`.
    pub drops: u64,
}

/// Severity level of a surface log report. 1:1 with the backend WASM
/// WIT `log.level` enum (itself 1:1 with `tracing::Level`); serialized
/// lowercase so the wire strings match the tracing/WIT vocabulary. The variant
/// order is ascending severity (`Trace` < … < `Error`), so `Ord` compares by
/// severity — the kernel's error-report floor admits `level >= floor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    /// The [`LogLevel`] for a lowercase wire string (`"trace"`…`"error"`), or
    /// `None` for anything else — the inverse of the serde-lowercase
    /// serialization. Parses the untrusted `level` field a component supplies in
    /// a `brenn-log` CustomEvent detail; an unrecognized string is a malformed
    /// component log, dropped rather than coerced to a level.
    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "trace" => Some(Self::Trace),
            "debug" => Some(Self::Debug),
            "info" => Some(Self::Info),
            "warn" => Some(Self::Warn),
            "error" => Some(Self::Error),
            _ => None,
        }
    }

    /// The lowercase wire string — the inverse of [`from_wire_str`] and of the
    /// serde-lowercase serialization. Lets a sender pass a typed level rather
    /// than a raw string the receiver would silently drop on a typo.
    ///
    /// [`from_wire_str`]: LogLevel::from_wire_str
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Severity of a [`ClientFrame::Alert`]. 1:1 with the backend WASM WIT
/// `alert.severity` enum and the native `AlertSeverity`; serialized lowercase so
/// the wire strings match that shared vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl AlertSeverity {
    /// The [`AlertSeverity`] for a lowercase wire string (`"info"`/`"warning"`/
    /// `"critical"`), or `None` for anything else — the inverse of the
    /// serde-lowercase serialization. Parses the untrusted `severity` field a
    /// component supplies in a `brenn-alert` CustomEvent detail; an unrecognized
    /// string is a malformed component alert, dropped rather than coerced to a
    /// severity.
    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "info" => Some(Self::Info),
            "warning" => Some(Self::Warning),
            "critical" => Some(Self::Critical),
            _ => None,
        }
    }
}

/// A binding's overflow loudness rung, as resolved by the server and carried on
/// [`Binding`]. 1:1 with the backend `brenn-lib` `NoiseLevel`; serialized
/// lowercase so the wire strings match that shared vocabulary. The page never
/// re-runs the ladder — it receives the resolved rung and enacts it on overflow.
///
/// Declaration order is the loudness ladder (`Silent < Metered < Alarm <
/// Fatal`); `Ord` lets the kernel read "at least this loud" as a comparison.
///
/// **`Fatal` on a binding means overflow here kills the instance.** It is opt-in
/// per binding and never a default. On a chrome binding the kill takes chrome's
/// fatal path (a capped bootstrap reload) — an operator who marks a chrome
/// binding `fatal` has declared "overflow here reloads the page".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NoiseLevel {
    /// Overflow drops oldest; no further signal.
    Silent,
    /// Silent, plus a per-binding lifetime drop counter in the kernel.
    Metered,
    /// Metered, plus a backend alert and a toast on every overflowing activation.
    Alarm,
    /// Alarm, plus killing the overflowing instance.
    Fatal,
}

impl NoiseLevel {
    /// The [`NoiseLevel`] for a lowercase wire string, or `None` for anything
    /// else — the inverse of the serde-lowercase serialization.
    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "silent" => Some(Self::Silent),
            "metered" => Some(Self::Metered),
            "alarm" => Some(Self::Alarm),
            "fatal" => Some(Self::Fatal),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Server → client frames
// ---------------------------------------------------------------------------

/// Server → client frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerFrame {
    /// First frame on every connection, enqueued before any inbound frame is
    /// read. Carries the surface's identity, the idle-heartbeat interval, the
    /// publish-body cap (for client pre-validation and frame-cap derivation),
    /// and the resolved binding table.
    Welcome {
        /// Surface slug.
        surface: String,
        /// `"surface:<slug>"`.
        participant_id: String,
        /// Idle-heartbeat interval advertisement.
        heartbeat_secs: u32,
        /// Server's publish-body cap (config); the client pre-validates and
        /// derives the same WS frame cap via [`max_client_frame_bytes`].
        max_body_bytes: u64,
        /// Whether this surface's policy grants the alert plane. A conforming
        /// kernel suppresses [`ClientFrame::Alert`] client-side when this is
        /// false, so an ungranted `Alert` reaches the server only from a
        /// non-conforming client (a protocol violation). The kernel learns its
        /// rights from the server and never guesses.
        alert_granted: bool,
        /// Whether this surface's policy grants the takeover plane. Governs
        /// whether the kernel honours a component's
        /// takeover-request seam event: on an ungranted surface the
        /// kernel drops the request with a `warn` breadcrumb and never pushes an
        /// overlay. Surface-level, mirroring `alert_granted`. The kernel learns
        /// its rights from the server and never guesses.
        takeover_granted: bool,
        /// Publish floor for surface error reports, present only when
        /// `[observability] surface_error_channel` is configured. `Some(floor)`:
        /// the reserved error-report output port (`#brenn`/`error-reports`,
        /// `brenn-surface-contract`) is live; a conforming
        /// kernel publishes reports at `floor` and above to it and keeps
        /// lower-level output console-only. `None`: no port; the kernel is
        /// console-only. The kernel learns its rights from the server and never
        /// guesses (the `alert_granted` pattern).
        error_report_floor: Option<LogLevel>,
        /// Surface self-description telemetry parameters. The kernel installs the
        /// resize listener and status timer and reports geometry + status; the
        /// cadence is the operator's, delivered here rather than guessed.
        surface_description: SurfaceDescription,
        bindings: SurfaceBindings,
    },
    /// Idle-liveness signal (the client-side liveness probe, since browser WS
    /// cannot observe pings).
    Heartbeat,
    /// The answer to one `Subscribe`, echoing its `(channel, instance)` so a
    /// page with sibling instances on one channel can tell whose subscription
    /// this settles.
    SubscribeResult {
        channel: String,
        instance: String,
        outcome: SubscribeOutcome,
        replay_count: u32,
        gap: Option<GapInfo>,
    },
    /// One delivered envelope on `channel`, to one or more subscriptions.
    ///
    /// Bus plane. The envelope is carried **once per (connection, channel,
    /// message)**; `targets` names every subscription of that channel on this
    /// connection the message is delivered to, and the kernel fans out to them.
    /// The wire never multiplies the payload by the number of sibling instances
    /// — that fan-out is the kernel's job, exactly as the backend's dispatcher
    /// fans out to its consumers without copying the body per subscriber.
    ///
    /// `targets` is non-empty, and no two entries name the same subscription;
    /// either is a fatal protocol error, as is a target naming a subscription
    /// the kernel does not hold.
    ///
    /// A single-target frame is semantically identical to a multi-target frame
    /// with one entry and is always legal: paths where per-subscription answers
    /// legitimately diverge (resume/replay, subscribe-time context, a sibling
    /// subscribed mid-stream or lagging behind its own backlog) emit per-target
    /// frames. That is per-subscription state doing its job, not duplication.
    Deliver {
        channel: String,
        envelope: MessageEnvelope,
        targets: Vec<DeliverTarget>,
    },
    /// Re-anchor one subscription: unsubscribe it and subscribe it again with
    /// the cursor the kernel currently holds for it.
    ///
    /// The server's ask, not a report of anything. The server carries
    /// per-subscription resume bookkeeping whose reconcile only runs when a
    /// subscription re-attaches; on a connection that never reconnects that
    /// bookkeeping — and the cursors carrying it — grow without bound. This
    /// frame lets the server run the reconcile without waiting for a reconnect.
    ///
    /// Class-blind: the kernel re-resumes whatever subscription is named,
    /// echoing the opaque cursor it holds. It reads no class and no cursor
    /// contents. The component seam observes at most a
    /// first-window-after-resubscribe, which the contract already defines as
    /// unremarkable.
    ///
    /// A `ReAnchor` for a subscription the kernel never held is a fatal
    /// protocol error — a correct server cannot produce it. One naming a
    /// subscription the kernel holds but is not live on (a teardown crossing
    /// this frame in flight, a transport-down channel awaiting reconcile) is a
    /// benign cross and is ignored: the resubscribe those paths already perform
    /// is the re-anchor.
    ReAnchor { channel: String, instance: String },
    PublishResult {
        correlation: Option<u64>,
        outcome: PublishOutcome,
    },
    /// The answer to one [`ClientFrame::PublishBatch`].
    PublishBatchResult {
        correlation: u64,
        outcome: PublishBatchOutcome,
    },
}

/// Surface self-description telemetry parameters, delivered in every `Welcome`.
/// The operator tunes the cadence; there is no off state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceDescription {
    /// Status heartbeat cadence, seconds. The kernel emits a status snapshot on
    /// this interval (and immediately on any transition into `failed`).
    pub status_interval_secs: u32,
}

// ---------------------------------------------------------------------------
// Bindings
// ---------------------------------------------------------------------------

/// The resolved instance/port/channel table, delivered in `Welcome` so the
/// kernel learns its wiring from the backend and never hardcodes channels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceBindings {
    /// Mounted component instances, declaration order. Each names its `instance`
    /// id (the routing/mount key) and its component `kind` (the element tag and
    /// wasm module). One kind may back several instances.
    pub components: Vec<ComponentEntry>,
    /// Channel → instance/port.
    pub subscriptions: Vec<Binding>,
    /// Instance/port → channel, each carrying the port's default urgency.
    pub outputs: Vec<OutputBinding>,
    /// Every distinct `local:` channel some binding above names, with the ring
    /// depth its page-local router must retain. Page-local channels have no
    /// `[[channel]]` block and no directory entry — the per-surface config block
    /// *is* the declaration — so this table is the only place their per-channel
    /// parameters can be resolved. Deduped, in first-binding order.
    pub local_channels: Vec<LocalChannel>,
    /// The `instance` id of this surface's chrome component: the singleton the
    /// kernel treats specially (pre-chrome connect-indicator handoff and
    /// chrome-death-is-fatal). One field rather than a per-entry flag makes the
    /// singleton invariant unrepresentable-wrong on the wire. Empty when no
    /// chrome instance is declared yet (transitional).
    pub chrome_instance: String,
}

/// One page-local channel a surface declares, resolved for its router.
///
/// `ring_depth` is the retained-ring depth: the number of most-recent messages
/// the router replays to a port on attach. Bounded by construction — the ring
/// lives in page memory, so an unbounded depth is rejected at boot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalChannel {
    pub channel: String,
    pub ring_depth: u64,
}

/// One declared component instance: its routing/mount `instance` id, the
/// component `kind` that backs it, and the `abi` that says what shape that
/// backing artifact is. Several instances may share a kind (one wasm module, N
/// elements).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentEntry {
    pub instance: String,
    pub kind: String,
    /// How the kernel loads this instance's artifact. Advertised rather than
    /// inferred from the kind: the same logic may ship as more than one artifact
    /// shape over its life, and the page must never guess which one it is
    /// holding.
    pub abi: Abi,
    /// How many of this instance's activation flushes the kernel parks while the
    /// link is down, before dropping the oldest whole batch. Resolved at boot;
    /// bounded and `>= 1`.
    ///
    /// Advertised because the kernel is the enforcer: activations continue with
    /// the link down, so their flushes queue in page memory, and the operator
    /// bounds that queue per instance like any other.
    pub parked_batch_depth: u64,
    /// This instance's static config map, read by a `processor` component
    /// through its `config` import. Empty on every other ABI.
    ///
    /// Fixed for the page's lifetime — the backend's process-lifetime config
    /// map, at the page's grain. A changed map arrives only with a reconnect
    /// `Welcome`, which the bindings-changed check turns into a reload.
    ///
    /// **Confidentiality:** this map is delivered to every authenticated page
    /// session of the surface. It is operator configuration, never a place for
    /// credentials or secrets.
    pub config: BTreeMap<String, String>,
}

/// One config input binding: a channel wired to a component instance's port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub channel: String,
    pub instance: String,
    pub port: String,
    /// This binding's port-queue depth: how many undelivered messages the page
    /// holds for the port before overflow policy applies. Resolved at boot from
    /// the binding's `push_depth` (inheriting binding → channel → global on
    /// both wire classes; `local:` has no channel rung and inherits binding →
    /// global), and bounded `>= 1` on every class — the queue is page memory.
    ///
    /// Advertised rather than assumed because it is a per-binding operator knob:
    /// a low-rate control plane and a high-rate meter on one page want different
    /// depths, and the page has no other way to learn either.
    pub push_depth: u64,
    /// This binding's context-window depth: how many of the subscription's
    /// most-recent messages the kernel puts before `new_from` when it windows
    /// this port. Resolved at boot on every class, bounded — the retained ring
    /// is page memory.
    ///
    /// Per binding, not per subscription: two ports of one instance on one
    /// channel share one ring (folded to the max of their depths) and each reads
    /// its own depth out of it.
    pub retain_depth: u64,
    /// This binding's overflow loudness, resolved at boot down the class-uniform
    /// binding → channel → global ladder (`local:` has no channel rung, so binding
    /// → global). The page receives the resolved rung — it never re-runs the
    /// ladder — and the kernel enacts it (count / alert+toast / kill) when a drop
    /// is observed for this binding.
    pub noise: NoiseLevel,
}

/// One config output binding: a component instance's port wired to a channel.
///
/// Distinct from [`Binding`] because an output carries a knob an input has no
/// meaning for: `urgency`, the port's configured default. Urgency is a property
/// of *sending* — sender intent about how hard the bus should work to wake a
/// subscriber — so an input binding has nothing to say about it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputBinding {
    pub channel: String,
    pub instance: String,
    pub port: String,
    /// The port's configured default urgency, resolved at boot. A publish that
    /// carries no explicit urgency is sent at this level; the component's
    /// per-message override wins over it.
    ///
    /// Advertised rather than kept server-side because the page needs it to
    /// stamp page-local envelopes (whose router never consults the server) and
    /// so a component can read back the default it is publishing under.
    pub urgency: Urgency,
    /// This sink's token-bucket fill per activation, in millitokens (one publish
    /// costs `brenn_budget::MILLITOKENS_PER_PUBLISH`). Resolved at boot from the
    /// binding's `publish_per_activation`.
    ///
    /// Advertised because the kernel enforces it: the kernel mints this
    /// component's activations, so it is the party that can meter per-activation
    /// spending and answer `quota-exceeded` synchronously — exactly what the
    /// backend host does for the same component. The server ships resolved
    /// millitokens rather than the operator's `f64` so the page never re-derives
    /// config: there is one resolver, and it is the server's.
    pub fill_mt: u64,
    /// This sink's carryover ceiling, in millitokens: what an idle component may
    /// accumulate, clamped at the start of each activation. Resolved at boot
    /// from the binding's `publish_capacity`.
    pub capacity_mt: u64,
}

// ---------------------------------------------------------------------------
// Surface bindability
// ---------------------------------------------------------------------------

/// Whether a scheme's channels bind to a surface at all.
///
/// This is the only scheme question that is genuinely surface-local. It is
/// orthogonal to delivery class ([`ChannelScheme::delivery_class`], which is
/// bus-wide knowledge): `mqtt:` and `webhook:` are durable-class yet not
/// surface transports, and `pwa_push:` is egress-only. Deciding both questions
/// from one enum would be a parallel taxonomy — the exact drift the One True
/// Enum exists to prevent.
///
/// Exhaustive over [`ChannelScheme`] so a new transport cannot be added without
/// answering "does this bind to a surface?".
pub fn surface_bindable(scheme: ChannelScheme) -> bool {
    match scheme {
        ChannelScheme::Brenn | ChannelScheme::Ephemeral | ChannelScheme::Local => true,
        ChannelScheme::Mqtt | ChannelScheme::Webhook | ChannelScheme::PwaPush => false,
    }
}

/// The delivery class of a surface-bound channel address, or `None` when the
/// address carries no recognized prefix, names a scheme that does not bind to a
/// surface, or has no delivery class at all.
///
/// The single derivation both ends of the surface wire share, so the client's
/// port/resume decisions and the server's binding classification cannot drift.
pub fn surface_delivery_class(channel: &str) -> Option<DeliveryClass> {
    let scheme = ChannelScheme::of(channel)?;
    surface_bindable(scheme)
        .then(|| scheme.delivery_class())
        .flatten()
}

/// Whether `channel` names a page-local (`local:`) channel.
///
/// The single spelling of "does this address stay in the page?", shared by both
/// ends of the surface wire: the client branches its subscribe/publish/resume
/// paths on it, and the server excludes local bindings from every wire map on
/// it. One question, one derivation — a scheme addition or a change of answer
/// has one call shape to find.
///
/// A non-local scheme, an unrecognized prefix, and a scheme that does not bind
/// to a surface all answer `false`: this is a positive identification, not a
/// classification, so it never panics and never speaks for the other classes.
pub fn is_local_channel(channel: &str) -> bool {
    surface_delivery_class(channel) == Some(DeliveryClass::Local)
}

// ---------------------------------------------------------------------------
// Reserved `local:` control channels
// ---------------------------------------------------------------------------

/// The page-local theme plane: any producer → chrome.
pub const LOCAL_THEME_CHANNEL: &str = "local:brenn/theme";
/// The page-local takeover plane: a takeover-capable component → chrome.
pub const LOCAL_TAKEOVER_CHANNEL: &str = "local:brenn/takeover";
/// The page-local link-state plane: kernel → subscribers (chrome renders the
/// banner from it).
pub const LOCAL_LINK_STATE_CHANNEL: &str = "local:brenn/link-state";
/// The page-local surface-state plane: kernel → subscribers; the mount/failure
/// mirror of what the kernel reports on the status channel.
pub const LOCAL_SURFACE_STATE_CHANNEL: &str = "local:brenn/surface-state";
/// The page-local toast stream: kernel → chrome.
pub const LOCAL_TOAST_CHANNEL: &str = "local:brenn/toast";
/// The page-local overlay-state plane: chrome → the kernel's status telemetry.
/// Chrome's post-fold overlay holdership, which no other vantage point can see —
/// the kernel routes takeover traffic chrome may drop, so routed traffic and
/// held overlay are different facts.
pub const LOCAL_OVERLAY_STATE_CHANNEL: &str = "local:brenn/overlay-state";

/// A reserved `local:brenn/*` control channel and the contract-fixed rules that
/// govern it.
///
/// Reserved names are reserved *by construction*: every one contains `/`, which
/// the operator channel-name charset (`is_unreserved_char`) can never produce,
/// so no declared channel can collide with one — the same reservation the
/// `tools/` namespace rests on. Operators still name them in surface *bindings*
/// (that is how a component reaches a control plane); this table is what boot
/// validation checks such a binding against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReservedLocalChannel {
    pub address: &'static str,
    /// Contract-fixed retained-ring depth. The control planes carry 1
    /// (last-value replay — what makes a late-attaching chrome's handoff
    /// gap-free); the toast stream carries 0: it is an event stream, not a
    /// control plane, and replaying a stale toast would resurface a past event.
    pub ring_depth: u64,
    /// Whether only the kernel may publish here. An `[[surface.output]]` bound
    /// to such a channel is rejected at boot: v1 has no component producers for
    /// these planes, and widening the producer set later is additive.
    pub kernel_publish_only: bool,
    /// Whether binding this channel — in either direction — requires the
    /// surface's `takeover` grant. Capability-as-binding: the grant gates the
    /// wiring rather than a runtime DOM-event check.
    pub requires_takeover_grant: bool,
}

/// Every reserved `local:brenn/*` control channel. Exhaustive: a `local:brenn/*`
/// address absent from this table is undefined vocabulary and boot rejects it.
pub const RESERVED_LOCAL_CHANNELS: &[ReservedLocalChannel] = &[
    ReservedLocalChannel {
        address: LOCAL_THEME_CHANNEL,
        ring_depth: 1,
        kernel_publish_only: false,
        requires_takeover_grant: false,
    },
    ReservedLocalChannel {
        address: LOCAL_TAKEOVER_CHANNEL,
        ring_depth: 1,
        kernel_publish_only: false,
        requires_takeover_grant: true,
    },
    ReservedLocalChannel {
        address: LOCAL_LINK_STATE_CHANNEL,
        ring_depth: 1,
        kernel_publish_only: true,
        requires_takeover_grant: false,
    },
    ReservedLocalChannel {
        address: LOCAL_SURFACE_STATE_CHANNEL,
        ring_depth: 1,
        kernel_publish_only: true,
        requires_takeover_grant: false,
    },
    ReservedLocalChannel {
        address: LOCAL_TOAST_CHANNEL,
        ring_depth: 0,
        kernel_publish_only: true,
        requires_takeover_grant: false,
    },
    ReservedLocalChannel {
        address: LOCAL_OVERLAY_STATE_CHANNEL,
        ring_depth: 1,
        kernel_publish_only: false,
        // The plane exists only where takeover exists: a surface without the
        // grant can never hold an overlay, so it has no overlay state to report.
        requires_takeover_grant: true,
    },
];

/// The payload version every reserved control plane's body carries as `v`.
///
/// One constant for every plane: they are one contract, versioned together per
/// the self-description discipline. A consumer that does not recognize `v` must
/// not guess at the rest of the body.
// TODO(plane-version-check): every control-plane body carries `v`, but the
// consumers (chrome's on_theme/on_takeover, and the link-state/surface-state/
// toast folds) deserialize it and never check it, so a future v2 body is folded
// under v1 semantics instead of dropped-and-reported. Decide the cross-plane
// versioning rule (check `v == CONTROL_PLANE_VERSION` and drop-and-warn on
// mismatch, or drop the field) and apply it uniformly across the planes.
pub const CONTROL_PLANE_VERSION: u8 = 1;

/// The link state the kernel reports on [`LOCAL_LINK_STATE_CHANNEL`].
///
/// The connection's state as the *page* experiences it, which is why it is a
/// plane rather than a component-visible transport detail: a consumer renders it
/// (the banner) and must not reason about sockets to do so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkState {
    Connecting,
    Connected,
    Reconnecting,
    Reloading,
    /// Terminal. The plane payload is `{v, state}` only, so a server-supplied
    /// fatal *detail* never reaches this plane or the on-screen banner; the
    /// kernel keeps that detail in the console/error-report path instead (see
    /// the kernel's `Event::Fatal` handling).
    Fatal,
}

/// The body published on [`LOCAL_LINK_STATE_CHANNEL`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkStateBody {
    pub v: u8,
    pub state: LinkState,
}

/// The body published on [`LOCAL_SURFACE_STATE_CHANNEL`]: the page-local mirror
/// of the mount/failure facts the kernel reports on the status channel.
///
/// The mirror, not a second source: both are rendered from the kernel's one
/// instance table. It carries no `ports_attached` — that column answers "is the
/// wall working?" for an operator reading the retained status document, whereas
/// this plane exists so a consumer can arrange what is mounted, and a pump count
/// is not an arrangement fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceStateBody {
    pub v: u8,
    pub instances: Vec<SurfaceStateInstance>,
}

/// One instance's mount state on [`LOCAL_SURFACE_STATE_CHANNEL`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceStateInstance {
    pub instance: String,
    pub kind: String,
    pub state: InstanceState,
    /// Short failure reason when `state` is `Failed`; `None` otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// How loud a [`ToastBody`] is. Serialized lowercase, as every wire enum here is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToastSeverity {
    Info,
    Warning,
    Error,
}

/// Who raised a toast. The kernel's own notices are distinguishable from a
/// component's, because a consumer renders them differently and an operator
/// reading one needs to know whose voice it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToastSource {
    Kernel,
}

/// The body published on [`LOCAL_TOAST_CHANNEL`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToastBody {
    pub v: u8,
    pub severity: ToastSeverity,
    pub text: String,
    pub source: ToastSource,
}

/// The two legal [`ThemeBody`] `theme` values, and the two `data-theme`
/// attribute values chrome writes. Chrome and any theme-driving component share
/// these constants so the wire vocabulary has a single home rather than
/// hand-copied string literals kept in lockstep by comment.
pub const THEME_DARK: &str = "dark";
/// See [`THEME_DARK`].
pub const THEME_LIGHT: &str = "light";

/// The body published on [`LOCAL_THEME_CHANNEL`]: the runtime theme axis a
/// producer asks chrome to apply.
///
/// `theme` stays a string here so the chrome component owns wire-string parsing:
/// an unrecognized value is dropped-and-reported by the consumer, never rejected
/// at deserialize time (a bad theme must not brick delivery of a well-formed
/// envelope).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThemeBody {
    pub v: u8,
    pub theme: String,
}

/// The action a [`TakeoverBody`] asks chrome to take on the takeover overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TakeoverAction {
    Request,
    Release,
}

/// The body published on [`LOCAL_TAKEOVER_CHANNEL`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TakeoverBody {
    pub v: u8,
    pub action: TakeoverAction,
    /// The requesting instance. The kernel's local router injects it from its own
    /// port wiring, overwriting any value the publisher supplied, so a component
    /// cannot name another instance as the takeover holder; chrome's `on_takeover`
    /// then trusts this field as the sole request/deny/release identity.
    pub instance: String,
}

/// The body published on [`LOCAL_OVERLAY_STATE_CHANNEL`]: chrome's overlay
/// holdership after the fold that changed it.
///
/// Published on every transition and only on a transition — there is no
/// heartbeat, and the plane's depth-1 ring is what hands the current value to
/// anything attaching later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverlayStateBody {
    pub v: u8,
    /// The instance holding the fullscreen overlay, or `None` when none is held.
    pub holder: Option<String>,
    /// The page-monotonic millisecond reading at the transition. Page-local by
    /// nature (a monotonic clock means nothing off the page), so a consumer that
    /// needs wall-clock time takes it from the envelope instead.
    pub since_stamp: u64,
}

/// The reserved-channel rules for `address`, or `None` when it names no reserved
/// channel.
pub fn reserved_local_channel(address: &str) -> Option<&'static ReservedLocalChannel> {
    RESERVED_LOCAL_CHANNELS
        .iter()
        .find(|c| c.address == address)
}

/// Whether `address` sits in the reserved `local:brenn/` namespace, whether or
/// not it names a channel [`RESERVED_LOCAL_CHANNELS`] defines.
///
/// Distinct from [`reserved_local_channel`] on purpose: `local:brenn/nonesuch`
/// is *reserved* (an operator can never declare it) but *undefined*, and boot
/// must reject it as undefined vocabulary rather than treat it as an ordinary
/// operator channel.
pub fn is_reserved_local_namespace(address: &str) -> bool {
    address.starts_with("local:brenn/")
}

// ---------------------------------------------------------------------------
// Cursor / LocalPos
// ---------------------------------------------------------------------------

/// An opaque per-subscription resume token the kernel stores and echoes
/// verbatim, never interprets.
///
/// A `Deliver` carries the latest one; the client keeps it per subscription and
/// presents it as `Subscribe.resume` on the next re-`Subscribe`. Only the server
/// mints and reads its contents — a durable cursor's high-water, an ephemeral
/// cursor's `(epoch, seq)`. The inner string is **private** and the type has
/// **no accessor and no constructor**: the sanctioned server-side access is a
/// serde round-trip only (build via `serde_json::from_value::<Cursor>(
/// Value::String(s))`, read via matching `serde_json::to_value(&cursor)` for a
/// `Value::String`).
///
/// Interpretation code lives in `brenn-server`, never here: `surface/proto`
/// links into the wasm surface client, so any code in this crate — even
/// never-called interpretation code — executes on the surface. Moving a
/// durable-vs-ephemeral branch into another crate changes which file holds the
/// branch and nothing about where it runs; that crate-laundering pattern is
/// forbidden. The maxim's jurisdiction is *where code executes*, not which
/// crate names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(String);

/// One subscription's share of a [`ServerFrame::Deliver`].
///
/// Every field is per-subscription state: the semantics are exactly what a
/// whole frame carried when a frame served one subscription. Coalescing several
/// targets into one frame is a wire *encoding*; it folds no per-subscription
/// state together and gives no subscription another's answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliverTarget {
    /// The subscribing instance, matching the `Subscribe` that opened this
    /// subscription. Every surface subscription names its instance; there is no
    /// kernel grain (the bare `surface:<slug>` grain is publisher-only).
    pub instance: String,
    /// A delivery-time span sequence, assigned at socket-write time and
    /// strictly increasing per subscription-span (a span starts at each
    /// `SubscribeResult`, the counter restarting at 1), both wire classes and
    /// replay/live rows alike. It exists solely for the kernel's continuity
    /// check on a peer it must not trust blindly; a non-increasing `seq` is a
    /// fatal protocol error.
    pub seq: u64,
    /// A server-minted opaque resume token. The kernel stores the latest
    /// accepted one for this subscription and echoes it verbatim on the next
    /// `Subscribe`; it never interprets it.
    pub cursor: Cursor,
    /// Messages lost on **this subscription** since its previous delivery on
    /// this connection — broadcast-ring overflow for ephemeral channels,
    /// messages GC'd past the retained/push window for durable ones. `0` = none.
    pub dropped: u64,
}

/// Page-local delivery position, assigned by the surface kernel's `local:`
/// router — the sole source of truth for a `local:` channel. `epoch` is the
/// **page-load** epoch (fresh per boot of the page). `seq` is dense and
/// ascending per channel, assigned atomically with delivery.
///
/// Never crosses the wire in either direction: `local:` traffic is page-local
/// by contract, so this type exists only in page memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalPos {
    pub epoch: Uuid,
    pub seq: u64,
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// Result of a `Subscribe`. Every config-bound subscribe class (ephemeral and
/// durable) is supported, so `Ok` is the only success outcome; a subscribe that
/// cannot be honoured is a protocol violation that kills the connection, never a
/// wire outcome. Kept a tagged enum so a future non-fatal outcome is additive.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SubscribeOutcome {
    Ok,
}

/// Result of a `Publish`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PublishOutcome {
    Ok,
    RateLimited,
    BodyTooLarge {
        len: u64,
        max: u64,
    },
    /// The server accepted the frame but the durable publish failed on a path
    /// that must not kill the connection — the reserved error-report port, whose
    /// broken-boot-invariant outcomes are logged with the report body rather than
    /// panicked. Produced only by that backstop arm; ordinary bound outputs panic
    /// on the same outcomes.
    Failed,
}

/// Result of a [`ClientFrame::PublishBatch`].
///
/// Two variants, deliberately not a reuse of [`PublishOutcome`]: the single
/// publish's other outcomes are violation-grade here (`BodyTooLarge` — the
/// kernel gates bodies at buffer time) or impossible (`Failed` — the
/// error-report backstop is not a batch path), so reusing that enum would
/// advertise arms this frame can never carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PublishBatchOutcome {
    /// Applied: durable entries committed in one transaction, ephemeral entries
    /// fanned out, all in call order.
    Ok,
    /// The instance's server-side send budget refused the batch. The honest
    /// outcome when the two budget tiers disagree — never a violation and never
    /// a kill: the kernel is the primary limiter, and this backstop is only
    /// reached by a surface out-running it.
    RateLimited,
}

// ---------------------------------------------------------------------------
// Gap signalling
// ---------------------------------------------------------------------------

/// A gap in the replay window, attached to `SubscribeResult` when replay could
/// not cover the client's requested resume point.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GapInfo {
    pub reason: GapReason,
}

/// Why replay was gapped.
///
/// Deliberately has no `ResumeAhead`: a matching epoch with a seq the server
/// never assigned is impossible for an honest client, so the transport treats
/// it as a protocol violation rather than a wire gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum GapReason {
    EpochChanged,
    HoleExceedsRing,
    /// Durable resume could not be covered from the retained window: the
    /// requested `last_seq` predates the channel's oldest retained message, or
    /// the per-subscriber retain clamp truncated the re-send set. Conservative —
    /// a false "may have missed" is honest; a false negative is not.
    BeyondRetained,
}

// ---------------------------------------------------------------------------
// Shared contract constants and derivations
// ---------------------------------------------------------------------------

/// Generous allowance for everything in a `Publish` frame besides the body:
/// JSON keys, type/kind tags, instance/port names, correlation, and the
/// error-report `subject_instance`. All of them are config-charset identifiers
/// orders of magnitude under this allowance; the slack is what lets the cap be
/// derived from `max_body_bytes` alone rather than tracking each field.
pub const PUBLISH_FRAME_OVERHEAD_BYTES: usize = 8 * 1024;

/// The WS read cap, derived — not a fixed constant. `max_body_bytes` is
/// operator config, so a fixed frame cap could contradict a legal config; and
/// worst-case JSON string escaping expands one body byte to six (`\u00XX` for
/// control characters), so even a default-config-legal body needs ~6x headroom.
/// Both ends compute the same number: the server from its config at route
/// setup, the client from `Welcome.max_body_bytes`. Any config-legal publish is
/// guaranteed to fit under the cap by construction.
pub fn max_client_frame_bytes(max_body_bytes: usize) -> usize {
    // Checked, not wrapping: this value gates a fail2ban decision (an oversized
    // frame is a protocol violation), and it must equal the number a 32-bit
    // wasm client derives. An operator `max_body_bytes` large enough to overflow
    // is a config contradiction — fail fast rather than derive a wrong cap.
    max_body_bytes
        .checked_mul(6)
        .and_then(|scaled| scaled.checked_add(PUBLISH_FRAME_OVERHEAD_BYTES))
        .expect("max_body_bytes too large: WS frame-cap derivation overflowed usize")
}

/// Error-report `message` truncation cap — same rationale and value as the
/// legacy `MAX_CLIENT_ERROR_BYTES`. Client-enforced: the kernel truncates a
/// report's `message` field to this before composing the reserved-port publish.
pub const MAX_LOG_MESSAGE_BYTES: usize = 4 * 1024;

/// Error-report `source` truncation cap (`"kernel"`, `"bootstrap"`,
/// `"component:<kind>"`). Client-enforced, like [`MAX_LOG_MESSAGE_BYTES`].
pub const MAX_LOG_SOURCE_BYTES: usize = 256;

/// `Alert.title` cap.
pub const MAX_ALERT_TITLE_BYTES: usize = 256;

/// `Alert.body` cap.
pub const MAX_ALERT_BODY_BYTES: usize = 4 * 1024;

/// WS close code (RFC 6455 §7.4.2 private range 3000-3999) signalling that the
/// client bundle predates the deployed server; the close reason carries the
/// server `BUILD_ID`. The surface client maps this to "reload required" and
/// stops reconnecting. This is protocol-contract surface shared by both ends.
pub const STALE_BUILD_CLOSE_CODE: u16 = 3001;

/// Maximum number of subscription bindings a single `[[surface]]` may declare.
///
/// The kernel attaches one port per subscription binding in one synchronous
/// first-connect burst, and on wasm's single thread the client's driver cannot
/// drain its bounded control channel until that burst returns. This one shared
/// bound keeps the two ends from drifting: the client sizes its control channel
/// to absorb a burst this large, and the backend boot-validates a surface's
/// subscription count against it so an oversized-but-otherwise-valid config
/// fails fast at boot rather than bricking the kernel at first connect.
pub const MAX_SURFACE_SUBSCRIPTION_BINDINGS: usize = 64;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {

    // ── reserved control-plane payloads ──────────────────────────────────────

    #[test]
    fn link_state_wire_strings_are_pinned() {
        // These strings are the contract a chrome — in-tree or out — matches on.
        for (state, wire) in [
            (LinkState::Connecting, "connecting"),
            (LinkState::Connected, "connected"),
            (LinkState::Reconnecting, "reconnecting"),
            (LinkState::Reloading, "reloading"),
            (LinkState::Fatal, "fatal"),
        ] {
            let body = LinkStateBody {
                v: CONTROL_PLANE_VERSION,
                state,
            };
            assert_eq!(
                serde_json::to_string(&body).unwrap(),
                format!(r#"{{"v":1,"state":"{wire}"}}"#)
            );
        }
    }

    #[test]
    fn a_surface_state_instance_omits_reason_unless_it_failed() {
        // Absent, not null: a mounted instance has no reason, and a consumer
        // should not have to distinguish "no reason" from "reason: null".
        let body = SurfaceStateBody {
            v: CONTROL_PLANE_VERSION,
            instances: vec![
                SurfaceStateInstance {
                    instance: "a".into(),
                    kind: "k".into(),
                    state: InstanceState::Mounted,
                    reason: None,
                },
                SurfaceStateInstance {
                    instance: "b".into(),
                    kind: "k".into(),
                    state: InstanceState::Failed,
                    reason: Some("boom".into()),
                },
            ],
        };
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            r#"{"v":1,"instances":[{"instance":"a","kind":"k","state":"mounted"},{"instance":"b","kind":"k","state":"failed","reason":"boom"}]}"#
        );
    }

    #[test]
    fn every_kernel_publish_only_plane_is_one_the_kernel_can_name() {
        // The kernel's publish API panics on a plane outside this set, so the set
        // and the constants must not drift apart.
        let kernel_planes: Vec<&str> = RESERVED_LOCAL_CHANNELS
            .iter()
            .filter(|c| c.kernel_publish_only)
            .map(|c| c.address)
            .collect();
        assert_eq!(
            kernel_planes,
            vec![
                LOCAL_LINK_STATE_CHANNEL,
                LOCAL_SURFACE_STATE_CHANNEL,
                LOCAL_TOAST_CHANNEL
            ]
        );
    }
    use super::*;
    use brenn_envelope::{ChannelScheme, Urgency};
    use chrono::{DateTime, Utc};
    use serde_json::json;

    fn sample_envelope() -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap(),
            source: "src".to_string(),
            channel: "ephemeral:demo".to_string(),
            sender: "surface:deskbar".to_string(),
            publish_ts: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            body: "hello".to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Ephemeral,
        }
    }

    // ── Surface bindability ───────────────────────────────────────────────

    #[test]
    fn surface_delivery_class_by_scheme() {
        assert_eq!(
            surface_delivery_class("brenn:orders"),
            Some(DeliveryClass::Durable)
        );
        assert_eq!(
            surface_delivery_class("ephemeral:protobar"),
            Some(DeliveryClass::Ephemeral)
        );
        assert_eq!(
            surface_delivery_class("local:brenn/theme"),
            Some(DeliveryClass::Local)
        );
        // Non-surface schemes and garbage classify as None.
        assert_eq!(surface_delivery_class("mqtt:topic"), None);
        assert_eq!(surface_delivery_class("webhook:hook"), None);
        assert_eq!(surface_delivery_class("pwa_push:target"), None);
        assert_eq!(surface_delivery_class("bare"), None);
    }

    /// `mqtt:`/`webhook:` are durable-class on the bus yet do not bind to a
    /// surface: the two questions are independent, which is why the surface
    /// keeps only the bindability predicate and defers class to the envelope
    /// crate. A single fused taxonomy could not express this row.
    #[test]
    fn bindability_is_independent_of_delivery_class() {
        for scheme in [ChannelScheme::Mqtt, ChannelScheme::Webhook] {
            assert!(!surface_bindable(scheme));
            assert_eq!(scheme.delivery_class(), Some(DeliveryClass::Durable));
        }
    }

    // ── ClientFrame golden JSON ───────────────────────────────────────────

    /// A `Cursor` is opaque: the kernel echoes whatever string the server minted,
    /// so a resume rides the wire as a bare JSON string with no interior shape.
    fn sample_cursor() -> Cursor {
        serde_json::from_value(json!("opaque-token-7")).unwrap()
    }

    #[test]
    fn client_subscribe_golden_and_roundtrip() {
        let f = ClientFrame::Subscribe {
            channel: "ephemeral:demo".to_string(),
            instance: "ticker".to_string(),
            resume: Some(sample_cursor()),
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Subscribe"));
        assert_eq!(v["channel"], json!("ephemeral:demo"));
        assert_eq!(v["instance"], json!("ticker"));
        // The cursor is serde-transparent: a bare string, not a tagged object.
        assert_eq!(v["resume"], json!("opaque-token-7"));
        let s = serde_json::to_string(&f).unwrap();
        let _back: ClientFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn client_subscribe_no_resume_golden() {
        let f = ClientFrame::Subscribe {
            channel: "ephemeral:demo".to_string(),
            instance: "ticker".to_string(),
            resume: None,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Subscribe"));
        assert_eq!(v["resume"], json!(null));
    }

    /// Sibling instances on one channel are distinct frames — the wire must
    /// carry the grain, or the server cannot tell whose subscription it is.
    #[test]
    fn client_subscribe_distinguishes_sibling_instances() {
        let alice = ClientFrame::Subscribe {
            channel: "brenn:agenda".to_string(),
            instance: "agenda-alice".to_string(),
            resume: None,
        };
        let bob = ClientFrame::Subscribe {
            channel: "brenn:agenda".to_string(),
            instance: "agenda-bob".to_string(),
            resume: None,
        };
        assert_ne!(
            serde_json::to_string(&alice).unwrap(),
            serde_json::to_string(&bob).unwrap()
        );
    }

    /// A `Cursor` round-trips serde-transparently: it is a newtype over `String`
    /// with a private field and no accessor, so the only way to build or read one
    /// is a serde round-trip through a JSON string.
    #[test]
    fn cursor_round_trips_transparently_as_a_string() {
        let c: Cursor = serde_json::from_value(json!("some-opaque-blob")).unwrap();
        assert_eq!(serde_json::to_value(&c).unwrap(), json!("some-opaque-blob"));
        // Anything that is not a JSON string is not a cursor.
        assert!(serde_json::from_value::<Cursor>(json!({ "kind": "Durable" })).is_err());
        assert!(serde_json::from_value::<Cursor>(json!(42)).is_err());
    }

    #[test]
    fn client_unsubscribe_golden_and_roundtrip() {
        let f = ClientFrame::Unsubscribe {
            channel: "ephemeral:demo".to_string(),
            instance: "ticker".to_string(),
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Unsubscribe"));
        assert_eq!(v["channel"], json!("ephemeral:demo"));
        assert_eq!(v["instance"], json!("ticker"));
        let s = serde_json::to_string(&f).unwrap();
        let _back: ClientFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn client_publish_golden_and_roundtrip() {
        let f = ClientFrame::Publish {
            instance: "p1".to_string(),
            port: "out".to_string(),
            body: "payload".to_string(),
            correlation: Some(99),
            subject_instance: None,
            urgency: None,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Publish"));
        assert_eq!(v["instance"], json!("p1"));
        assert_eq!(v["port"], json!("out"));
        assert_eq!(v["body"], json!("payload"));
        assert_eq!(v["correlation"], json!(99));
        // An ordinary publish names no report subject, and the field is absent
        // from the wire entirely rather than riding as an explicit null.
        assert!(
            v.get("subject_instance").is_none(),
            "subject_instance must not appear on an ordinary publish: {v}"
        );
        let s = serde_json::to_string(&f).unwrap();
        let _back: ClientFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn log_level_variants_golden_lowercase() {
        assert_eq!(
            serde_json::to_value(LogLevel::Trace).unwrap(),
            json!("trace")
        );
        assert_eq!(
            serde_json::to_value(LogLevel::Debug).unwrap(),
            json!("debug")
        );
        assert_eq!(serde_json::to_value(LogLevel::Info).unwrap(), json!("info"));
        assert_eq!(serde_json::to_value(LogLevel::Warn).unwrap(), json!("warn"));
        assert_eq!(
            serde_json::to_value(LogLevel::Error).unwrap(),
            json!("error")
        );
    }

    #[test]
    fn log_level_from_wire_str_inverts_serialization() {
        // Pins `from_wire_str` as the exact inverse of the serde-lowercase
        // serialization for every variant, so the two cannot drift.
        for level in [
            LogLevel::Trace,
            LogLevel::Debug,
            LogLevel::Info,
            LogLevel::Warn,
            LogLevel::Error,
        ] {
            let wire = serde_json::to_value(level).unwrap();
            let s = wire.as_str().unwrap();
            assert_eq!(LogLevel::from_wire_str(s), Some(level));
        }
        // Unrecognized strings (including the PascalCase variant names and a
        // valid-looking non-level) parse to `None`.
        assert_eq!(LogLevel::from_wire_str("fatal"), None);
        assert_eq!(LogLevel::from_wire_str("Warn"), None);
        assert_eq!(LogLevel::from_wire_str(""), None);
    }

    #[test]
    fn alert_severity_from_wire_str_inverts_serialization() {
        // Pins `from_wire_str` as the exact inverse of the serde-lowercase
        // serialization for every variant, so the two cannot drift.
        for severity in [
            AlertSeverity::Info,
            AlertSeverity::Warning,
            AlertSeverity::Critical,
        ] {
            let wire = serde_json::to_value(severity).unwrap();
            let s = wire.as_str().unwrap();
            assert_eq!(AlertSeverity::from_wire_str(s), Some(severity));
        }
        // Unrecognized strings (including PascalCase variant names and a
        // valid-looking non-severity) parse to `None`.
        assert_eq!(AlertSeverity::from_wire_str("warn"), None);
        assert_eq!(AlertSeverity::from_wire_str("Warning"), None);
        assert_eq!(AlertSeverity::from_wire_str(""), None);
    }

    #[test]
    fn noise_level_wire_codec_covers_every_rung() {
        // Every rung serializes to its lowercase string and `from_wire_str`
        // inverts it — the exhaustive mapping the wire contract depends on.
        for (level, s) in [
            (NoiseLevel::Silent, "silent"),
            (NoiseLevel::Metered, "metered"),
            (NoiseLevel::Alarm, "alarm"),
            (NoiseLevel::Fatal, "fatal"),
        ] {
            assert_eq!(serde_json::to_value(level).unwrap(), json!(s));
            assert_eq!(NoiseLevel::from_wire_str(s), Some(level));
        }
        assert_eq!(NoiseLevel::from_wire_str("Fatal"), None);
        assert_eq!(NoiseLevel::from_wire_str(""), None);
    }

    #[test]
    fn noise_level_ord_is_ascending_loudness() {
        assert!(NoiseLevel::Silent < NoiseLevel::Metered);
        assert!(NoiseLevel::Metered < NoiseLevel::Alarm);
        assert!(NoiseLevel::Alarm < NoiseLevel::Fatal);
        // "at least this loud" as a comparison.
        assert!(NoiseLevel::Fatal >= NoiseLevel::Alarm);
    }

    #[test]
    fn log_level_ord_is_ascending_severity() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
        // The floor admission predicate the kernel uses.
        assert!(LogLevel::Error >= LogLevel::Warn);
        assert!(LogLevel::Info < LogLevel::Warn);
    }

    #[test]
    fn client_alert_golden_and_roundtrip() {
        let f = ClientFrame::Alert {
            severity: AlertSeverity::Warning,
            title: "component panic: protobar".to_string(),
            body: "the panic detail".to_string(),
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Alert"));
        assert_eq!(v["severity"], json!("warning"));
        assert_eq!(v["title"], json!("component panic: protobar"));
        assert_eq!(v["body"], json!("the panic detail"));
        let s = serde_json::to_string(&f).unwrap();
        let _back: ClientFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn alert_severity_variants_golden_lowercase() {
        assert_eq!(
            serde_json::to_value(AlertSeverity::Info).unwrap(),
            json!("info")
        );
        assert_eq!(
            serde_json::to_value(AlertSeverity::Warning).unwrap(),
            json!("warning")
        );
        assert_eq!(
            serde_json::to_value(AlertSeverity::Critical).unwrap(),
            json!("critical")
        );
    }

    #[test]
    fn client_alert_each_severity_roundtrips() {
        for severity in [
            AlertSeverity::Info,
            AlertSeverity::Warning,
            AlertSeverity::Critical,
        ] {
            let f = ClientFrame::Alert {
                severity,
                title: "t".to_string(),
                body: "b".to_string(),
            };
            let s = serde_json::to_string(&f).unwrap();
            let back: ClientFrame = serde_json::from_str(&s).unwrap();
            match back {
                ClientFrame::Alert { severity: got, .. } => assert_eq!(got, severity),
                _ => panic!("expected Alert"),
            }
        }
    }

    #[test]
    fn client_geometry_golden_and_roundtrip() {
        let f = ClientFrame::Geometry {
            width: 1920,
            height: 515,
            device_pixel_ratio: 2.0,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Geometry"));
        assert_eq!(v["width"], json!(1920));
        assert_eq!(v["height"], json!(515));
        assert_eq!(v["device_pixel_ratio"], json!(2.0));
        let s = serde_json::to_string(&f).unwrap();
        let _back: ClientFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn client_status_golden_and_roundtrip() {
        let f = ClientFrame::Status {
            instances: vec![
                InstanceReport {
                    instance: "p1".to_string(),
                    kind: "protobar".to_string(),
                    state: InstanceState::Mounted,
                    reason: None,
                    ports_attached: 1,
                },
                InstanceReport {
                    instance: "mode-clock".to_string(),
                    kind: "mode-clock".to_string(),
                    state: InstanceState::Failed,
                    reason: Some("module missing".to_string()),
                    ports_attached: 0,
                },
            ],
            uptime_secs: 86_400,
            counters: StatusCounters {
                deliveries: 1042,
                publishes: 12,
                errors: 3,
                instances: BTreeMap::from([
                    (
                        "p1".to_string(),
                        InstanceCounters {
                            publishes: 12,
                            drops: 7,
                        },
                    ),
                    // A reported instance with nothing to report is legal as an
                    // explicit zero; an instance absent from the map is the other
                    // legal spelling of the same fact.
                    ("mode-clock".to_string(), InstanceCounters::default()),
                ]),
            },
            overlay: Some(OverlayReport {
                holder: "p1".to_string(),
                since: DateTime::UNIX_EPOCH,
            }),
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Status"));
        assert_eq!(v["uptime_secs"], json!(86_400));
        assert_eq!(
            v["instances"][0],
            json!({ "instance": "p1", "kind": "protobar", "state": "mounted", "reason": null, "ports_attached": 1 })
        );
        assert_eq!(v["instances"][1]["state"], json!("failed"));
        assert_eq!(v["instances"][1]["reason"], json!("module missing"));
        assert_eq!(
            v["counters"],
            json!({
                "deliveries": 1042,
                "publishes": 12,
                "errors": 3,
                "instances": {
                    "p1": { "publishes": 12, "drops": 7 },
                    "mode-clock": { "publishes": 0, "drops": 0 },
                },
            })
        );
        assert_eq!(
            v["overlay"],
            json!({ "holder": "p1", "since": "1970-01-01T00:00:00Z" })
        );
        let s = serde_json::to_string(&f).unwrap();
        let _back: ClientFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn instance_state_variants_golden_lowercase() {
        assert_eq!(
            serde_json::to_value(InstanceState::Mounted).unwrap(),
            json!("mounted")
        );
        assert_eq!(
            serde_json::to_value(InstanceState::Failed).unwrap(),
            json!("failed")
        );
        assert_eq!(
            serde_json::to_value(InstanceState::Pending).unwrap(),
            json!("pending")
        );
    }

    #[test]
    fn alert_missing_severity_fails_to_parse() {
        let err = serde_json::from_str::<ClientFrame>(r#"{"type":"Alert","title":"t","body":"b"}"#);
        assert!(err.is_err());
    }

    #[test]
    fn alert_unknown_severity_fails_to_parse() {
        let err = serde_json::from_str::<ClientFrame>(
            r#"{"type":"Alert","severity":"fatal","title":"t","body":"b"}"#,
        );
        assert!(err.is_err());
    }

    // ── ServerFrame golden JSON ───────────────────────────────────────────

    #[test]
    fn server_welcome_golden_and_roundtrip() {
        let f = ServerFrame::Welcome {
            surface: "deskbar".to_string(),
            participant_id: "surface:deskbar".to_string(),
            heartbeat_secs: 20,
            max_body_bytes: 65_536,
            alert_granted: true,
            takeover_granted: true,
            error_report_floor: Some(LogLevel::Warn),
            surface_description: SurfaceDescription {
                status_interval_secs: 60,
            },
            bindings: SurfaceBindings {
                components: vec![ComponentEntry {
                    instance: "p1".to_string(),
                    kind: "protobar".to_string(),
                    abi: Abi::Dom,
                    parked_batch_depth: 8,
                    config: BTreeMap::from([("horizon-days".to_string(), "30".to_string())]),
                }],
                subscriptions: vec![Binding {
                    channel: "ephemeral:demo".to_string(),
                    instance: "p1".to_string(),
                    port: "messages".to_string(),
                    push_depth: 8,
                    retain_depth: 2,
                    noise: NoiseLevel::Alarm,
                }],
                outputs: vec![OutputBinding {
                    channel: "ephemeral:outdemo".to_string(),
                    instance: "p1".to_string(),
                    port: "out".to_string(),
                    urgency: Urgency::High,
                    fill_mt: 2500,
                    capacity_mt: 1000,
                }],
                local_channels: vec![LocalChannel {
                    channel: LOCAL_THEME_CHANNEL.to_string(),
                    ring_depth: 1,
                }],
                chrome_instance: "p1".to_string(),
            },
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Welcome"));
        assert_eq!(v["surface"], json!("deskbar"));
        assert_eq!(v["participant_id"], json!("surface:deskbar"));
        assert!(
            v.get("epoch").is_none(),
            "Welcome no longer carries a bus epoch: {v}"
        );
        assert_eq!(v["heartbeat_secs"], json!(20));
        assert_eq!(v["max_body_bytes"], json!(65_536));
        assert_eq!(v["alert_granted"], json!(true));
        assert_eq!(v["takeover_granted"], json!(true));
        assert_eq!(v["error_report_floor"], json!("warn"));
        assert_eq!(
            v["surface_description"],
            json!({ "status_interval_secs": 60 })
        );
        assert_eq!(
            v["bindings"]["components"][0],
            json!({
                "instance": "p1",
                "kind": "protobar",
                "abi": "dom",
                "parked_batch_depth": 8,
                "config": { "horizon-days": "30" },
            })
        );
        assert_eq!(
            v["bindings"]["subscriptions"][0],
            json!({
                "channel": "ephemeral:demo",
                "instance": "p1",
                "port": "messages",
                "push_depth": 8,
                "retain_depth": 2,
                "noise": "alarm",
            })
        );
        assert_eq!(
            v["bindings"]["outputs"][0],
            json!({
                "channel": "ephemeral:outdemo",
                "instance": "p1",
                "port": "out",
                "urgency": "high",
                "fill_mt": 2500,
                "capacity_mt": 1000,
            })
        );
        assert_eq!(
            v["bindings"]["local_channels"],
            json!([{ "channel": "local:brenn/theme", "ring_depth": 1 }])
        );
        let s = serde_json::to_string(&f).unwrap();
        let _back: ServerFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn server_heartbeat_golden_and_roundtrip() {
        let f = ServerFrame::Heartbeat;
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v, json!({ "type": "Heartbeat" }));
        let s = serde_json::to_string(&f).unwrap();
        let _back: ServerFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn server_subscribe_result_golden_and_roundtrip() {
        let f = ServerFrame::SubscribeResult {
            channel: "ephemeral:demo".to_string(),
            instance: "ticker".to_string(),
            outcome: SubscribeOutcome::Ok,
            replay_count: 3,
            gap: Some(GapInfo {
                reason: GapReason::EpochChanged,
            }),
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("SubscribeResult"));
        assert_eq!(v["channel"], json!("ephemeral:demo"));
        assert_eq!(v["instance"], json!("ticker"));
        assert_eq!(v["outcome"], json!({ "kind": "Ok" }));
        assert_eq!(v["replay_count"], json!(3));
        assert_eq!(v["gap"], json!({ "reason": { "kind": "EpochChanged" } }));
        let s = serde_json::to_string(&f).unwrap();
        let _back: ServerFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn server_subscribe_result_no_gap_golden() {
        let f = ServerFrame::SubscribeResult {
            channel: "brenn:events".to_string(),
            instance: "feed".to_string(),
            outcome: SubscribeOutcome::Ok,
            replay_count: 0,
            gap: None,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["outcome"], json!({ "kind": "Ok" }));
        assert_eq!(v["gap"], json!(null));
    }

    #[test]
    fn server_deliver_golden_and_roundtrip() {
        let f = ServerFrame::Deliver {
            channel: "ephemeral:demo".to_string(),
            envelope: sample_envelope(),
            targets: vec![DeliverTarget {
                instance: "ticker".to_string(),
                seq: 12,
                cursor: sample_cursor(),
                dropped: 5,
            }],
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("Deliver"));
        assert_eq!(v["channel"], json!("ephemeral:demo"));
        assert_eq!(v["targets"][0]["instance"], json!("ticker"));
        assert_eq!(v["targets"][0]["dropped"], json!(5));
        // The span seq is a bare integer; the cursor is a bare opaque string.
        assert_eq!(v["targets"][0]["seq"], json!(12));
        assert_eq!(v["targets"][0]["cursor"], json!("opaque-token-7"));
        // Envelope embeds via brenn-envelope's own serde, unchanged.
        assert_eq!(
            v["envelope"]["message_id"],
            json!("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(v["envelope"]["envelope_type"], json!("ephemeral"));
        let s = serde_json::to_string(&f).unwrap();
        let back: ServerFrame = serde_json::from_str(&s).unwrap();
        match back {
            ServerFrame::Deliver {
                envelope, targets, ..
            } => {
                assert_eq!(envelope.body, "hello");
                assert_eq!(targets[0].seq, 12);
            }
            _ => panic!("expected Deliver"),
        }
    }

    /// The payload rides once however many subscriptions it lands on: sibling
    /// instances on one channel appear as extra `targets`, each with its own
    /// per-subscription `(seq, cursor, dropped)`, and the envelope is not
    /// repeated. This is the wire half of "one envelope per (connection,
    /// channel, message)".
    #[test]
    fn server_deliver_carries_one_envelope_for_many_targets() {
        let f = ServerFrame::Deliver {
            channel: "ephemeral:demo".to_string(),
            envelope: sample_envelope(),
            targets: vec![
                DeliverTarget {
                    instance: "alice".to_string(),
                    seq: 3,
                    cursor: sample_cursor(),
                    dropped: 0,
                },
                DeliverTarget {
                    instance: "bob".to_string(),
                    seq: 9,
                    cursor: sample_cursor(),
                    dropped: 4,
                },
            ],
        };
        let v = serde_json::to_value(&f).unwrap();
        assert!(
            v.get("envelope").is_some() && v["targets"].as_array().unwrap().len() == 2,
            "one envelope, two targets: {v}"
        );
        assert_eq!(v["targets"][0]["instance"], json!("alice"));
        assert_eq!(v["targets"][1]["instance"], json!("bob"));
        assert_eq!(v["targets"][1]["dropped"], json!(4));
        // The envelope appears exactly once in the serialized frame — the
        // duplication this consolidation exists to remove.
        let s = serde_json::to_string(&f).unwrap();
        assert_eq!(s.matches("\"envelope_type\"").count(), 1, "{s}");
        let back: ServerFrame = serde_json::from_str(&s).unwrap();
        match back {
            ServerFrame::Deliver { targets, .. } => {
                assert_eq!(targets.len(), 2);
                assert_eq!(targets[1].instance, "bob");
            }
            _ => panic!("expected Deliver"),
        }
    }

    #[test]
    fn local_pos_carries_epoch_and_seq() {
        // The page-local position never crosses the wire; it carries the
        // page-load epoch and a dense per-channel seq, and round-trips as itself.
        let epoch = Uuid::parse_str("00000000-0000-0000-0000-0000000000e0").unwrap();
        let v = serde_json::to_value(LocalPos { epoch, seq: 3 }).unwrap();
        assert_eq!(
            v,
            json!({ "epoch": "00000000-0000-0000-0000-0000000000e0", "seq": 3 })
        );
        let back: LocalPos = serde_json::from_value(v).unwrap();
        assert_eq!(back, LocalPos { epoch, seq: 3 });
    }

    /// One activation's flush on the wire: the instance it is attributed to, the
    /// correlation its result routes back on, and the entries in call order —
    /// each naming a **port**, never a channel.
    #[test]
    fn client_publish_batch_golden_and_roundtrip() {
        let f = ClientFrame::PublishBatch {
            instance: "agenda".into(),
            correlation: 7,
            publishes: vec![
                BatchEntry {
                    port: "out".into(),
                    body: "{}".into(),
                    urgency: None,
                },
                BatchEntry {
                    port: "out".into(),
                    body: "[]".into(),
                    urgency: Some(Urgency::High),
                },
            ],
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("PublishBatch"));
        assert_eq!(v["instance"], json!("agenda"));
        assert_eq!(v["correlation"], json!(7));
        // Absent urgency means "the port's configured default", which the server
        // owns — so it must be *absent*, never a null the server might read as a
        // claim, and never an echoed default that could be stale.
        assert_eq!(v["publishes"][0], json!({ "port": "out", "body": "{}" }));
        assert_eq!(
            v["publishes"][1],
            json!({ "port": "out", "body": "[]", "urgency": "high" })
        );
        let back: ClientFrame = serde_json::from_str(&serde_json::to_string(&f).unwrap()).unwrap();
        assert_eq!(back, f);
    }

    /// The batch result carries exactly two outcomes. The single publish's other
    /// arms are violation-grade here (the kernel gates bodies at buffer time) or
    /// impossible, so this enum must not grow them by reuse.
    #[test]
    fn server_publish_batch_result_golden_and_roundtrip() {
        for (outcome, kind) in [
            (PublishBatchOutcome::Ok, "Ok"),
            (PublishBatchOutcome::RateLimited, "RateLimited"),
        ] {
            let f = ServerFrame::PublishBatchResult {
                correlation: 7,
                outcome,
            };
            let v = serde_json::to_value(&f).unwrap();
            assert_eq!(v["type"], json!("PublishBatchResult"));
            assert_eq!(v["correlation"], json!(7));
            assert_eq!(v["outcome"], json!({ "kind": kind }));
            let back: ServerFrame =
                serde_json::from_str(&serde_json::to_string(&f).unwrap()).unwrap();
            assert!(matches!(
                back,
                ServerFrame::PublishBatchResult { correlation: 7, .. }
            ));
        }
    }

    #[test]
    fn server_publish_result_golden_and_roundtrip() {
        let f = ServerFrame::PublishResult {
            correlation: Some(99),
            outcome: PublishOutcome::BodyTooLarge {
                len: 1000,
                max: 500,
            },
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["type"], json!("PublishResult"));
        assert_eq!(v["correlation"], json!(99));
        assert_eq!(
            v["outcome"],
            json!({ "kind": "BodyTooLarge", "len": 1000, "max": 500 })
        );
        let s = serde_json::to_string(&f).unwrap();
        let _back: ServerFrame = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn publish_outcome_variants_golden() {
        assert_eq!(
            serde_json::to_value(PublishOutcome::Ok).unwrap(),
            json!({ "kind": "Ok" })
        );
        assert_eq!(
            serde_json::to_value(PublishOutcome::RateLimited).unwrap(),
            json!({ "kind": "RateLimited" })
        );
        assert_eq!(
            serde_json::to_value(PublishOutcome::Failed).unwrap(),
            json!({ "kind": "Failed" })
        );
    }

    #[test]
    fn gap_reason_variants_golden() {
        assert_eq!(
            serde_json::to_value(GapReason::EpochChanged).unwrap(),
            json!({ "kind": "EpochChanged" })
        );
        assert_eq!(
            serde_json::to_value(GapReason::HoleExceedsRing).unwrap(),
            json!({ "kind": "HoleExceedsRing" })
        );
    }

    // ── Parse-failure classification ──────────────────────────────────────

    #[test]
    fn unknown_client_frame_type_fails_to_parse() {
        let err = serde_json::from_str::<ClientFrame>(r#"{"type":"Nope"}"#);
        assert!(err.is_err());
    }

    #[test]
    fn unknown_server_frame_type_fails_to_parse() {
        let err = serde_json::from_str::<ServerFrame>(r#"{"type":"Nope"}"#);
        assert!(err.is_err());
    }

    #[test]
    fn unknown_subscribe_outcome_kind_fails_to_parse() {
        let err = serde_json::from_str::<SubscribeOutcome>(r#"{"kind":"Nope"}"#);
        assert!(err.is_err());
    }

    #[test]
    fn unknown_publish_outcome_kind_fails_to_parse() {
        let err = serde_json::from_str::<PublishOutcome>(r#"{"kind":"Nope"}"#);
        assert!(err.is_err());
    }

    #[test]
    fn unknown_gap_reason_kind_fails_to_parse() {
        let err = serde_json::from_str::<GapReason>(r#"{"kind":"Nope"}"#);
        assert!(err.is_err());
    }

    #[test]
    fn malformed_json_fails_to_parse() {
        let err = serde_json::from_str::<ClientFrame>("{not json");
        assert!(err.is_err());
    }

    /// Unknown *fields* inside a known variant are silently ignored (the
    /// internally-tagged representation cannot `deny_unknown_fields`). Pinned so
    /// a future serde/repr change cannot flip this tolerance unnoticed; the
    /// crate doc records the rationale.
    #[test]
    fn unknown_field_in_known_variant_is_ignored() {
        let parsed: ClientFrame = serde_json::from_str(
            r#"{"type":"Unsubscribe","channel":"ephemeral:demo","instance":"ticker","bogus":1}"#,
        )
        .expect("stray field is tolerated, not a parse error");
        match parsed {
            ClientFrame::Unsubscribe { channel, instance } => {
                assert_eq!(channel, "ephemeral:demo");
                assert_eq!(instance, "ticker");
            }
            _ => panic!("expected Unsubscribe"),
        }
    }

    // ── Frame-cap derivation ──────────────────────────────────────────────

    #[test]
    fn max_client_frame_bytes_pinned_at_default_config() {
        // Default config max_body_bytes = 65_536.
        assert_eq!(max_client_frame_bytes(65_536), 401_408);
    }

    /// A maximally-escaping body of exactly `max_body_bytes` bytes (every byte a
    /// JSON control char expanding to `\u00XX` = 6 bytes) serialized inside a
    /// full `Publish` frame stays under the derived cap. This is the property
    /// the cap derivation exists to guarantee.
    #[test]
    fn maximal_escaping_body_fits_under_cap() {
        let max_body_bytes = 65_536usize;
        let cap = max_client_frame_bytes(max_body_bytes);
        // 0x01 is a control char that JSON escapes to the 6-byte "".
        let body = "\u{0001}".repeat(max_body_bytes);
        assert_eq!(body.len(), max_body_bytes);
        let frame = ClientFrame::Publish {
            instance: "some-instance-id".to_string(),
            port: "some-output-port".to_string(),
            body,
            correlation: Some(u64::MAX),
            // Present, so the headroom proof covers the widest frame shape the
            // contract admits rather than only the subject-less one.
            subject_instance: Some("some-subject-instance-id".to_string()),
            urgency: None,
        };
        let serialized = serde_json::to_string(&frame).unwrap();
        assert!(
            serialized.len() < cap,
            "serialized frame {} must be under cap {}",
            serialized.len(),
            cap
        );
    }

    #[test]
    fn every_abi_round_trips_its_wire_string() {
        // Driven off `ALL` rather than a hand-listed set: a new ABI joins this
        // test by existing, which is the point of `ALL`.
        for abi in Abi::ALL {
            assert_eq!(Abi::parse(abi.as_str()), Some(abi));
        }
    }

    #[test]
    fn abi_wire_strings_are_pinned() {
        // Operator config and the wire share these spellings; they are contract.
        assert_eq!(Abi::Dom.as_str(), "dom");
        assert_eq!(Abi::Processor.as_str(), "processor");
        assert_eq!(Abi::DomTs.as_str(), "dom-ts");
        assert_eq!(Abi::Html.as_str(), "html");
        assert_eq!(Abi::parse("nonesuch"), None);
        assert_eq!(Abi::parse("Dom"), None);
    }

    #[test]
    fn abi_serde_matches_its_config_spelling() {
        // The `Welcome` encoding and the config string must be the same word:
        // an operator reading a frame in the console must not find a second
        // spelling of the value they wrote.
        for abi in Abi::ALL {
            let json = serde_json::to_value(abi).unwrap();
            assert_eq!(json, json!(abi.as_str()));
            assert_eq!(serde_json::from_value::<Abi>(json).unwrap(), abi);
        }
    }
}

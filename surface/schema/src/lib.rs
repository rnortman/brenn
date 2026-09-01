//! Brenn surface application schemas.
//!
//! This crate holds the payload schemas of the surface application layer: the
//! [`bindings`] document a surface learns its wiring from, the [`telemetry`]
//! documents it writes about itself, the reserved `local:brenn/*` control-plane
//! bodies, and the contract constants both ends share. The layout document is
//! not here: `brenn-chrome` is its only reader and owns it.
//! Both ends compile against it: the Rust/Axum backend and the
//! `brenn-surface-kernel` crate (which builds to `wasm32-unknown-unknown` for
//! the kernel and to native for tests). It is kept free of I/O, tokio, and
//! host-only dependencies so the wasm build stays clean — the only dependencies
//! are `serde`, `serde_json`, `chrono`, `uuid`, and `brenn-envelope` (itself
//! wasm-clean).
//!
//! **Payloads, not frames.** Nothing here rides the socket as a frame. The
//! attachment protocol — handshake, subscribe/publish/deliver, cursors,
//! outcomes — is `brenn-attach-proto`, and it knows nothing of components, DOM,
//! or pixels. Everything in this crate travels as a message *body* on an
//! ordinary channel, so a schema change here is a body-version bump
//! (`BINDINGS_DOCUMENT_VERSION`, `TELEMETRY_DOCUMENT_VERSION`,
//! [`CONTROL_PLANE_VERSION`]), never a transport-version bump.
//!
//! Serde representation: JSON objects, `snake_case` fields, lowercase enum
//! wire strings, and `deny_unknown_fields` on the documents that carry their
//! own `v`. Shapes are pinned by the golden-JSON tests beside each type.

use std::collections::BTreeMap;

use brenn_envelope::ChannelScheme;

/// The RFC 8030 urgency ladder, re-exported from the carrier crate.
///
/// Named in this crate's own surface — [`OutputBinding::urgency`] and the
/// `PORT_PUBLISH` detail — so component authors and the kernel reach it through
/// the contract they already depend on rather than taking a direct dependency on
/// `brenn-envelope` for one enum.
pub use brenn_envelope::Urgency;
use serde::{Deserialize, Serialize};

pub mod bindings;
pub mod telemetry;

/// Mount state of one component instance, serialized lowercase
/// (`"mounted"`/`"failed"`/`"pending"`).
///
/// Read by both planes the kernel's one instance table feeds: the retained
/// [`telemetry::StatusDocument`] an operator reads and the page-local
/// [`SurfaceStateBody`] chrome arranges from. One vocabulary, because the two
/// are renderings of the same fact and a divergence would be a second source of
/// truth about what is mounted.
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
// Bindings
// ---------------------------------------------------------------------------

/// One page-local channel a surface declares, resolved for its router.
///
/// `ring_depth` is the floor under the depth the page sizes the channel's store
/// to; the store's actual depth folds it with what the deepest window bound on
/// the channel must be served. Everything the store holds is the channel's
/// history, so a port coming into existence is owed the retained tail capped at
/// its own `push_depth` — which may be more than `ring_depth` when a binding
/// deepened the store. Bounded by construction: the store lives in page memory,
/// so an unbounded depth is rejected at boot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalChannel {
    pub channel: String,
    pub ring_depth: u64,
}

/// One declared component instance: its routing `instance` id and the component
/// `kind` that backs it. Several instances may share a kind — one compiled
/// module, N instantiations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentEntry {
    pub instance: String,
    pub kind: String,
    /// How many of this instance's activation flushes the kernel parks while the
    /// link is down, before dropping the oldest whole batch. Resolved at boot;
    /// bounded and `>= 1`.
    ///
    /// Advertised because the kernel is the enforcer: activations continue with
    /// the link down, so their flushes queue in page memory, and the operator
    /// bounds that queue per instance like any other.
    pub parked_batch_depth: u64,
    /// The capability interfaces this instance is given, deny-by-default, in
    /// canonical `ComponentGrant::word()` spellings, sorted and deduplicated.
    ///
    /// Enforced page-side: the kernel gates every privileged entry a component
    /// reaches on this list — the publish/defer family, the log router, the alert
    /// router, config reads, and the two DOM capabilities. A word this build
    /// cannot parse means server/kernel skew and refuses the whole document.
    ///
    /// The backend judges the same grants again, at boot against a processor's
    /// imports and the surface's own grants, and per frame for an attributed
    /// alert. Page-side gating is containment within the page; the backend's is
    /// the security boundary.
    ///
    /// TODO(bindings-doc-typed-grants): carry `ComponentGrant` itself rather
    /// than its spelling, so an unparseable word is a document-parse error
    /// instead of a reparse obligation on every reader.
    pub grants: Vec<String>,
    /// Every port name this instance's component kind declares with direction
    /// `out` or `io` — the complete vocabulary of names it may legally publish
    /// to. Sorted and duplicate-free, so the document stays byte-stable.
    ///
    /// Carried because the bound-output table alone cannot tell a declared port
    /// the deployer chose not to wire from a name the component's specification
    /// never mentioned. The first is a legal publish onto no channel; the second
    /// is the component contradicting the specification its artifact is
    /// hash-bound to, and the kernel ends the activation for it.
    ///
    /// Inbound ports do not travel: the kernel builds activation windows from
    /// the bindings, so an undeclared inbound port is unreachable by
    /// construction.
    pub declared_out_ports: Vec<String>,
    /// This instance's static config map, read through the component's `config`
    /// import. Empty unless the instance declares one.
    ///
    /// Fixed for the page's lifetime — the backend's process-lifetime config
    /// map, at the page's grain. A changed map arrives only with a redelivered
    /// bindings document, which the bindings-changed check turns into a reload.
    ///
    /// **Confidentiality:** this map rides the surface's retained config
    /// channel, so it is readable by every authenticated page session of the
    /// surface and by any principal the operator grants a covering
    /// ephemeral-subscribe matcher. It is operator configuration, never a place
    /// for credentials or secrets.
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
/// orthogonal to capabilities ([`ChannelScheme::capabilities`], which is
/// bus-wide knowledge): `mqtt:` and `webhook:` are durable and transportable
/// yet not surface transports, and `pwa_push:` is egress-only. Deciding both
/// questions from one enum would be a parallel taxonomy — the exact drift the
/// One True Enum exists to prevent.
///
/// Exhaustive over [`ChannelScheme`] so a new transport cannot be added without
/// answering "does this bind to a surface?".
pub fn surface_bindable(scheme: ChannelScheme) -> bool {
    match scheme {
        ChannelScheme::Brenn | ChannelScheme::Ephemeral | ChannelScheme::Local => true,
        ChannelScheme::Mqtt | ChannelScheme::Webhook | ChannelScheme::PwaPush => false,
    }
}

/// Whether the channel at `channel` binds to a surface: the address-string
/// spelling of [`surface_bindable`], `false` for an address carrying no
/// recognized prefix.
///
/// The gate every surface-side classifier of an address runs first. What a
/// channel class *does* — retention, transportability — is the bus's question,
/// answered by [`channel_capabilities`](brenn_envelope::channel_capabilities);
/// whether a surface may bind it at all is this one, and the two compose rather
/// than fusing. Fusing them would answer `None` for `mqtt:`, which is a durable
/// transportable channel that simply is not a surface transport.
pub fn surface_bindable_address(channel: &str) -> bool {
    ChannelScheme::of(channel).is_some_and(surface_bindable)
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
    /// Contract-fixed floor under the depth the page's store for this plane is
    /// sized to; the store's actual depth folds it with the windows bound on the
    /// plane. The control planes carry 1 (last-value replay — what makes a
    /// late-attaching chrome's handoff gap-free); the toast stream carries 0,
    /// promising no retention of its own, so its store is exactly as deep as its
    /// bindings require.
    ///
    /// The floor is not a bound on what a late attach is owed. Everything a store
    /// holds is the channel's history, and a binding coming into existence is
    /// owed the retained tail capped at its own `push_depth` on every plane — so
    /// a consumer attaching just after a toast was published is woken by it and
    /// served it as new.
    pub ring_depth: u64,
    /// Whether only the kernel may publish here. An `[[surface.output]]` bound
    /// to such a channel is rejected at boot: v1 has no component producers for
    /// these planes, and widening the producer set later is additive.
    pub kernel_publish_only: bool,
    /// Whether binding this channel — in either direction — requires the
    /// binding *component's* `takeover` grant. Capability-as-binding: the grant
    /// gates the wiring rather than a runtime check at the publish. A component's
    /// grants are its own; its surface holds no page capabilities.
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
// Shared contract constants and derivations
// ---------------------------------------------------------------------------

/// Error-report `message` truncation cap. Client-enforced: the kernel truncates
/// a report's `message` field to this before composing the publish onto the
/// surface's error channel, so an oversize report is trimmed rather than refused.
pub const MAX_LOG_MESSAGE_BYTES: usize = 4 * 1024;

/// Error-report `source` truncation cap (`"kernel"`, `"bootstrap"`,
/// `"component:<kind>"`). Client-enforced, like [`MAX_LOG_MESSAGE_BYTES`].
pub const MAX_LOG_SOURCE_BYTES: usize = 256;

/// WS close code (RFC 6455 §7.4.2 private range 3000-3999) signalling that the
/// client bundle predates the deployed server; the close reason carries the
/// server `BUILD_ID`. The kernel maps this to "reload required" and stops
/// reconnecting.
///
/// Surface-local, not part of the attachment protocol: the served-asset lockstep
/// check is the surface route's, applied at upgrade before any protocol frame,
/// and an attacher that serves itself no assets has no use for it. It lives here
/// because both ends of *that* check compile against this crate.
pub const STALE_BUILD_CLOSE_CODE: u16 = 3001;

/// Maximum number of subscription bindings a single `[[surface]]` may declare.
///
/// The kernel composes one `Subscribe` per bound channel in a single
/// first-connect run, and the peer meters subscribe traffic per attachment. This
/// one shared bound keeps the two ends from drifting: the backend derives the
/// attachment's subscribe-burst allowance from it and boot-validates a surface's
/// subscription count against it, so an oversized-but-otherwise-valid config
/// fails fast at boot rather than tripping the meter at first connect.
pub const MAX_SURFACE_SUBSCRIPTION_BINDINGS: usize = 64;

/// Maximum number of components a single `[[surface]]` may declare.
///
/// The bound under the kernel's control channel, which panics when full. Both
/// ends derive their limits from this value: the client sizes its control
/// channel to absorb a mount burst this large, and the backend boot-validates a
/// surface's component count against it — so an oversized-but-otherwise-valid
/// config fails fast at boot rather than bricking the kernel at first mount.
pub const MAX_SURFACE_COMPONENTS: usize = 64;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use brenn_envelope::{ChannelCapabilities, ChannelScheme};
    use serde_json::json;

    use super::*;

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

    // ── Surface bindability ───────────────────────────────────────────────

    /// The bindability table, row by row, at the address grain the gate sites
    /// use. A row silently changing its answer would admit a channel class the
    /// surface cannot route (or refuse one it can), so the literals are the
    /// pin.
    #[test]
    fn surface_bindable_address_by_scheme() {
        for (channel, expected) in [
            ("brenn:orders", true),
            ("ephemeral:protobar", true),
            ("local:brenn/theme", true),
            ("mqtt:topic", false),
            ("webhook:hook", false),
            ("pwa_push:target", false),
            ("bare", false),
            ("", false),
        ] {
            assert_eq!(
                surface_bindable_address(channel),
                expected,
                "{channel}: unexpected surface bindability"
            );
        }
    }

    /// `mqtt:`/`webhook:` are durable and transportable on the bus yet do not
    /// bind to a surface: the two questions are independent, which is why the
    /// surface keeps only the bindability predicate and defers capabilities to
    /// the envelope crate. A single fused taxonomy could not express this row.
    #[test]
    fn bindability_is_independent_of_capabilities() {
        for scheme in [ChannelScheme::Mqtt, ChannelScheme::Webhook] {
            assert!(!surface_bindable(scheme));
            assert_eq!(
                scheme.capabilities(),
                Some(ChannelCapabilities::DURABLE_TRANSPORTABLE)
            );
            // The bus answers what the channel does; the surface answers
            // whether it may bind it. A gate site composes the two.
            assert!(!surface_bindable_address(scheme.prefix()));
            assert_eq!(
                brenn_envelope::channel_capabilities(scheme.prefix()),
                Some(ChannelCapabilities::DURABLE_TRANSPORTABLE)
            );
        }
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
}

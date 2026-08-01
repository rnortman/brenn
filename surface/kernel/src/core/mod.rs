//! `ClientCore` — the sans-I/O protocol state machine.
//!
//! This module is pure and synchronous: no I/O, no timers, no clock reads. The
//! async driver feeds [`Input`]s (each with a caller-supplied monotonic
//! timestamp) and executes the returned [`Effect`]s in order. Keeping the
//! protocol logic here makes it deterministically unit-testable and guarantees
//! the wasm and native builds run identical logic by construction.
//!
//! It owns the connection lifecycle and backoff schedule, the `Welcome`
//! handshake and the fatal-protocol-error path, the inbound-silence liveness
//! rule, and the subscription table: per-channel refcounts, the
//! `Unsubscribed → Pending → Active` wire-state machine, and the
//! `Subscribe`/`Unsubscribe` edges as ports attach and detach.
//!
//! It also owns the **router for confined channels**: page-local pub/sub whose
//! sole source of truth is this core. A confined channel has no wire state at
//! all — no `Subscribe`, no refcount, no resume token — because no server
//! mediates it. The router mints the envelope, appends it to the channel's
//! store, and wakes every bound instance synchronously, so page-local delivery
//! keeps working with the link down. Retention itself is class-blind: confined
//! and transportable channels are held by one type, [`SurfaceChannelStore`],
//! over the channel mechanics the backend runs.
//!
//! # Err consumes; retention is the recovery
//!
//! Every window an activation is assembled from advances its binding's cursor
//! **at assembly**, on both hostings. A failed activation is therefore never
//! re-driven: returning err or trapping discards the buffered publishes and
//! nothing else, and the messages that activation saw reappear only as retained
//! context, in this or a later window whose `retain_depth` still covers them.
//! There is no gap-and-replay choreography and no terminal port failure.
//!
//! Author rule: if you cannot afford to lose it on a failed activation, either
//! do not err after observing it, or give the port retention.
//!
//! # The loudness ladder
//!
//! The kernel is the single surface-side enforcement site for per-binding
//! overflow loudness. It acts on drops from three origins, all charged against
//! the same per-binding counter: a retirement that outran the binding's cursor
//! (charged where it happened — the arrival, or the depth shrink, that retired
//! it), a still-retained span the binding's advance passed unserved (charged at
//! assembly, frontier-bounded so no span is enacted twice), and the server-side
//! push window's own loss (reported on `Deliver` and folded in per binding).
//! Rungs are cumulative: `silent` does nothing beyond
//! the existing `dropped` accounting; `metered` adds kernel-internal per-binding
//! lifetime counters; `alarm` adds an `Alert` frame and a coalesced
//! `local:brenn/toast` (one per binding per activation); `fatal` adds the kill,
//! taking the same trap-terminal path an entry's own trap takes. Noise governs
//! loudness only — it never changes what happens to the data, which is always
//! the delivery class's own overflow behaviour.
//!
//! Counting and announcing therefore happen at different moments. A loss is
//! *counted* the instant it happens, so a lagging binding is on the books
//! whether or not it ever runs again; the `alarm` rung's alert and toast are
//! *announced* at the binding's next window, naming the whole delta that window
//! reports. Announcing at each retirement instead would emit one alert frame and
//! one toast per message for as long as a binding lagged — a storm in exactly the
//! degraded condition the rung exists to report calmly — and a retirement always
//! implies traffic the binding will be activated for, so nothing is lost by
//! waiting for it. The one exception is the `fatal` rung: the kill ends the
//! instance, so there is no next window and its announcement rides the kill.
//!
//! Server-side push windows for surface subscriptions are registered with noise
//! clamped to `min(resolved, Metered)`, so the loud half fires here and only
//! here — no double alerting, and identical behaviour for every message class.
//!
//! # In-page separation is never a security boundary
//!
//! Everything this core enforces against component modules is **bug
//! containment**, not security: the modules run unsandboxed in the
//! authenticated page's realm with its full authority, so a malicious module is
//! stopped by nothing here. Capabilities degrade to advisory in-page, the
//! surface config is page-visible by construction, and a component that jams the
//! main thread jams the whole page — the kernel's serialization keeps an honest
//! component's bug inside that component and makes no stronger claim. Real
//! enforcement is server-side, past the WS gates, which trust nothing the page
//! says about itself.

use std::collections::HashMap;
use std::time::Duration;

use brenn_envelope::{ChannelScheme, MessageEnvelope, Urgency, surface_sub_identity};
use brenn_surface_contract::{
    Activation, ActivationError, DeferredEntry, DeferredWindow, PortWindow,
};
use brenn_surface_schema::{
    AlertSeverity, BatchDeferredOp, BatchEntry, CONTROL_PLANE_VERSION, ClientFrame, Cursor,
    DeferredOpKind, DeferredViewEntry, DeliverTarget, GapInfo, InstanceReport,
    LOCAL_OVERLAY_STATE_CHANNEL, LOCAL_TAKEOVER_CHANNEL, LOCAL_TOAST_CHANNEL, LogLevel,
    MAX_ALERT_BODY_BYTES, MAX_ALERT_TITLE_BYTES, NoiseLevel, OverlayReport, OverlayStateBody,
    PublishBatchOutcome, PublishOutcome, RESERVED_LOCAL_CHANNELS, STALE_BUILD_CLOSE_CODE,
    ServerFrame, StatusCounters, SubscribeOutcome, SurfaceBindings, SurfaceDescription,
    TakeoverBody, ToastBody, ToastSeverity, ToastSource, reserved_local_channel,
};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Overwrite the `instance` field of a `local:brenn/takeover` body with the
/// authenticated publishing instance. A body that does not parse as a
/// [`TakeoverBody`] is passed through unchanged — chrome's `on_takeover` then
/// drops and reports it, so a malformed spoof attempt survives no better than a
/// well-formed one.
// TODO(takeover-parser-symmetry-guard): the anti-spoof guarantee rests on the
// router and chrome sharing the exact same parse strictness for `TakeoverBody`
// (both reject the same malformed bodies). Nothing structural enforces that
// cross-crate symmetry; if chrome's parser is ever loosened, an unstamped body
// the router passed through could be accepted, reopening instance forgery.
// Close the passthrough at the trust boundary, or pin the symmetry structurally.
fn inject_takeover_instance(body: String, instance: &str) -> String {
    match serde_json::from_str::<TakeoverBody>(&body) {
        Ok(mut parsed) => {
            parsed.instance = instance.to_string();
            serde_json::to_string(&parsed).expect("a TakeoverBody serializes to JSON")
        }
        Err(_) => body,
    }
}

/// Who is publishing on a `local:` channel, at the two identity grains a surface
/// has: a mounted component instance, or the kernel itself.
///
/// Carried as a typed origin rather than a preformatted sender string so the
/// mint point can do more than compose an identity with it — the takeover
/// plane's anti-spoof stamping needs the instance name, and a sender string has
/// already thrown that away.
#[derive(Debug, Clone, Copy)]
enum LocalOrigin<'a> {
    Instance(&'a str),
    Kernel,
}

/// What the reserved confined planes' guard made of one body.
enum GuardedBody {
    /// The body to carry onto the channel — rewritten where the plane rewrites
    /// it, the caller's own bytes everywhere else.
    Carry(String),
    /// Refused, with the report that says why. Nothing reaches the channel.
    Refused(Effect),
}

/// What the local mint did with one publish, carrying the effects it produced
/// either way.
///
/// The router accepts every publish except on a plane whose guard refuses it
/// ([`LOCAL_OVERLAY_STATE_CHANNEL`]), so the disposition has to travel back to
/// the caller: a refused message is neither retained nor delivered, and telling
/// its publisher `Ok` would report a delivery that did not happen.
enum LocalMint {
    /// Minted, retained in the store, and fanned out to every bound port.
    Routed(Vec<Effect>),
    /// Refused at a plane guard before the mint. The effects carry the
    /// violation report.
    Refused(Vec<Effect>),
}

impl LocalMint {
    /// The effects, whichever way it went — for the callers that have no
    /// publisher to answer.
    fn into_effects(self) -> Vec<Effect> {
        match self {
            LocalMint::Routed(effects) | LocalMint::Refused(effects) => effects,
        }
    }
}

mod activation;
mod store;
mod util;

use activation::{ParkedBatch, RegisteredInstance};
use brenn_attach_client::store::DeferOp as ClientDeferOp;
use brenn_envelope::is_local_channel;
use brenn_queue::CursorOverflow;

pub use crate::publish_buffer::PublishBuffer;
use crate::publish_buffer::{BufferedDeferOp, BufferedPublish, OutputSpec};
/// Re-exported so the handle's `PublishGate` asks the same question the core's
/// publish paths do: a publish to a confined port must not be pre-rejected as
/// `NotConnected` while the link is down.
pub(crate) use store::channel_is_transportable;
use store::{BindingKey, DeferOp, DeferOpOutcome, StoreKey, SurfaceChannelStore, store_key};
use util::*;
pub(crate) use util::{checked_epoch_ms, epoch_ms, truncate_report_field};

/// The monotonic timestamp the driver supplies on every input. Owned by the
/// attach client (the shim that reads the per-target clock produces it);
/// re-exported so the core's callers name one type.
pub use brenn_attach_client::Millis;

// TODO(attach-cutover): this vocabulary and the `on_input`/`dispatch_input`
// routing over it are duplicated by `crate::turn`, whose inputs are the attach
// driver's rather than the transport's. Delete them here when the kernel cuts
// over.

/// An input to the core, produced by the driver from transport and timer
/// events. A transport-sourced input arriving in a state that no longer owns
/// that transport is a post-close straggler and is absorbed; the core never
/// panics on peer input.
///
/// Not `Eq`: a carried [`Command::SendGeometry`] holds an `f64`
/// device-pixel-ratio, which has no total equality. `PartialEq` is retained.
#[derive(Debug, Clone, PartialEq)]
pub enum Input {
    /// The connect attempt succeeded; the socket is open but no `Welcome` has
    /// been received yet.
    Opened,
    /// The connect attempt failed before a socket was established.
    ConnectFailed,
    /// An established transport went away — a peer close (carrying its close
    /// `code` and `reason`) or a transport-level failure (`code: None`). Both are
    /// one failure class to the backoff logic, with one exception: a close whose
    /// `code` is `STALE_BUILD_CLOSE_CODE` means this client compiled against an
    /// older build than the server now serves, and enters the terminal
    /// `ReloadRequired` state instead of backing off.
    Disconnected { code: Option<u16>, reason: String },
    /// A text frame (JSON `ServerFrame`) arrived from the transport.
    TextFrame(String),
    /// A binary frame arrived. The server never sends binary, so this is always
    /// a fatal protocol error.
    BinaryFrame,
    /// A precondition the core cannot check for itself failed on the host side —
    /// today, a device clock reading before the Unix epoch. Terminal, and it
    /// carries its own diagnosis into the ordinary fatal path so the banner, the
    /// link-state plane and the terminal `Event::Fatal` all fire as for any
    /// other fatal.
    HostFatal { detail: String },
    /// The armed timer fired.
    Tick,
    /// The armed outbox-retry timer fired: every instance whose outbox head is
    /// waiting on a refusal gets one more attempt.
    RetryTick,
    /// The armed release timer fired: every message parked on a confined channel
    /// whose release time has arrived enters retention now. `now_ms` is the
    /// driver's wall-clock read at the fire, epoch milliseconds UTC — the same
    /// sans-I/O seam as [`Millis`] on every other input, in the currency release
    /// times are stated in.
    ///
    /// A fire that finds nothing due (a wall clock that stepped back, a timer
    /// that fired early) releases nothing and is not an error.
    ReleaseDue { now_ms: u64 },
    /// A command issued through the client handle and routed to the core by the
    /// driver.
    Command(Command),
    /// An instance registered an activation entry with the driver. The entry
    /// itself stays driver-side (it is a callback; the core is pure data), so
    /// this carries only the identity: the core needs to know which instances are
    /// activation-delivered to build their pending queues and schedule them.
    ///
    /// Registering an instance already in the registered set is a kernel
    /// invariant violation and panics — see
    /// [`ClientCore::on_activation_registered`]. The kernel's registration gate
    /// is the backstop that keeps a bad registration from reaching it.
    ActivationRegistered { instance: String },
    /// An instance's activation entry was withdrawn (fixture teardown; the
    /// mirror of registration). Its pending queues go with it; its rings do not —
    /// they are the subscription's, not the entry's.
    ActivationDeregistered { instance: String },
    /// An invoked activation entry returned. Carries the buffer the core seeded
    /// and the driver handed to the entry, plus what the entry did: the core
    /// flushes it or discards it, and clears `in_flight`.
    ActivationDone {
        instance: String,
        outcome: ActivationOutcome,
        buffer: PublishBuffer,
        /// One envelope stamp per buffered publish, minted at the driver.
        ///
        /// The core is the router for the `local:` entries of a flush, so it must
        /// mint their envelopes — and it reads neither a clock nor an entropy
        /// source. Stamped per entry unconditionally rather than only for the
        /// local ones, for the same reason `Command::Publish` is: locality is
        /// resolved from the bindings, and only the core holds the authoritative
        /// bindings. A wire entry discards its stamp and takes the server's
        /// authoritative envelope, as it always has.
        stamps: Vec<MessageStamp>,
    },
}

/// How an invoked activation entry finished.
///
/// Three outcomes, not two, because err and trap are different facts about the
/// component and the design gives them different consequences. The driver
/// discriminates them at the invocation boundary: a returned `Err` is `Err`, an
/// unwind (a JS exception under wasm, a `catch_unwind` natively) is `Trap`.
///
/// The two failure arms carry the component's own account of what went wrong.
/// The kernel never parses it — every err is treated identically — but it is the
/// only answer anyone has to "failed *how*?", so it rides through to the
/// diagnostic event rather than being dropped at the boundary that observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivationOutcome {
    /// Returned ok. The buffer flushes atomically, in call order.
    Ok,
    /// Returned err, with the component's description of why. The buffer is
    /// discarded and a failure is counted; the instance keeps running and keeps
    /// being delivered. A failed activation is not a death — backend parity.
    Err(ActivationError),
    /// Panicked, with the unwind's message where one could be recovered. The
    /// buffer is discarded and the instance is terminal: its memory is presumed
    /// poisoned, so nothing further is delivered to it. Terminal for that one
    /// instance, never page death.
    Trap(String),
}

/// One activation, ready to invoke: which instance, what it sees, and the buffer
/// its publishes go into.
///
/// Handed out by [`ClientCore::take_ready_activation`] with the instance already
/// marked in flight and its queues already acked, so the core has no further say
/// until the driver returns an [`Input::ActivationDone`] for it. That is the
/// serialization: there is no way to obtain two of these for one instance.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadyActivation {
    pub instance: String,
    pub activation: Activation,
    pub buffer: PublishBuffer,
    /// Loud-rung effects enacted at window assembly for any input binding that
    /// dropped on this activation: an `alarm` binding's backend `Alert` and
    /// `local:brenn/toast`, and — for a `fatal` binding — the `InstanceFailed`
    /// event that kills the instance. Empty on the common no-drop / `silent` /
    /// `metered` path. The driver executes these before invoking the entry, and
    /// skips the invocation entirely when the instance was killed
    /// ([`ClientCore::is_failed`]).
    pub effects: Vec<Effect>,
}

/// The `alarm` rung's reaction to a binding that dropped this activation: a
/// backend `Alert` (severity `Warning`) plus a `local:brenn/toast`, both naming
/// the instance, port, channel, and the drop delta.
///
/// The kernel is the single surface-side alert/toast site for drops, so both
/// origins (a kernel-queue overflow and a server-reported delta) fold into this
/// one delta and produce one alert and one toast. The `Alert` carries no
/// instance field — it rides the ordinary alert plane the surface's alert grant
/// (proven at boot for any `alarm`/`fatal` binding) authorizes. The toast is
/// minted by the driver (this states the intent; the core reads no clock).
fn loud_drop_effects(instance: &str, channel: &str, port: &str, dropped: u64) -> Vec<Effect> {
    let text = format!(
        "{instance}: dropped {dropped} message(s) on port {port} ({channel}) — input overflow"
    );
    vec![
        Effect::SendFrame(ClientFrame::Alert {
            severity: AlertSeverity::Warning,
            title: truncate_report_field(
                format!("surface input overflow on {instance}"),
                MAX_ALERT_TITLE_BYTES,
            ),
            body: truncate_report_field(text.clone(), MAX_ALERT_BODY_BYTES),
        }),
        Effect::PublishControl {
            channel: LOCAL_TOAST_CHANNEL.to_string(),
            body: serde_json::to_string(&ToastBody {
                v: CONTROL_PLANE_VERSION,
                severity: ToastSeverity::Warning,
                text,
                source: ToastSource::Kernel,
            })
            .expect("surface client: a toast body serializes"),
        },
    ]
}

/// One activation's read of what its instance has parked: the windows the
/// component sees, and the identity behind each entry of each window.
///
/// Two views of one read. The component works in indices, which are only
/// meaningful against the window it was handed; the kernel works in identities,
/// which survive a release. Taking both from the same read is what makes an index
/// the component names resolvable to the message it meant.
#[derive(Debug, Default)]
struct DeferredSnapshot {
    windows: Vec<DeferredWindow>,
    /// Per output port, the identities of that port's window entries, in window
    /// order.
    ids: HashMap<String, Vec<Uuid>>,
}

/// One binding's drop charge for the loudness ladder.
///
/// Two figures, because counting and announcing happen at different moments (see
/// the module's ladder section): `counted` is this site's own accountable span,
/// never overlapping another site's, and `announced` is the delta a coalesced
/// alert and toast name — zero where the site defers the announcement to the
/// binding's next window.
struct DropCharge {
    port: String,
    channel: String,
    noise: NoiseLevel,
    /// Charged to the metered counter, and what arms the `fatal` kill.
    counted: u64,
    /// The delta the `alarm` rung announces here, or `0` to announce nothing.
    announced: u64,
}

// TODO(attach-cutover): this vocabulary and the `on_command` dispatch over it
// are duplicated by `crate::command`, which resolves a port against the bindings
// document instead of against `Welcome`. Delete them here when the kernel cuts
// over.

/// A command to the core, carried on [`Input::Command`], originating from the
/// client handle.
///
/// Not `Eq`: [`Command::SendGeometry`] carries an `f64` device-pixel-ratio,
/// which has no total equality. `PartialEq` is retained.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Publish `body` from `(instance, port)`, tagged with a handle-assigned
    /// `correlation` so its [`Event::PublishResult`] can be routed back. The
    /// handle already pre-validated against its `Welcome` snapshot; the core
    /// re-checks authoritatively (never sending an unbound, oversized, or
    /// disconnected publish) and answers a check failure — a stale-snapshot race
    /// around a reconnect — with an `Event::PublishResult` carrying the local
    /// status rather than a wire frame.
    Publish {
        correlation: u64,
        instance: String,
        port: String,
        body: String,
        /// The report subject for the reserved error-report port; `None` for
        /// every ordinary publish. Forwarded verbatim onto the wire frame — the
        /// core neither derives nor validates it (the authoritative declaration
        /// set lives server-side, which is the whole point: the client asserts no
        /// identity). A `local:` publish drops it: the page-local router mints
        /// its own envelope and already knows which instance published, from its
        /// own port wiring. See [`crate::proto::ClientFrame::Publish`].
        subject_instance: Option<String>,
        /// The caller's per-message urgency override; `None` ⇒ the port's
        /// configured default. Forwarded verbatim onto the wire frame (the
        /// server holds the authoritative default and applies it). A `local:`
        /// publish resolves it against the port's advertised default here
        /// instead, because this core *is* that traffic's router — no server
        /// ever sees the envelope it mints. See
        /// [`crate::proto::ClientFrame::Publish`].
        urgency: Option<Urgency>,
        /// The driver-read values a synthesized envelope needs, supplied as data
        /// so the core stays sans-I/O (it can read neither a wall clock nor an
        /// entropy source). Consumed only on the `local:` path, where this core
        /// *is* the router and must mint the envelope itself.
        ///
        /// Deliberately stamped for **every** publish rather than only the local
        /// ones: locality is resolved from the bindings, and only the core holds
        /// the authoritative bindings — the handle's snapshot can be stale across
        /// a reconnect, which is the whole reason the core re-checks. Deciding
        /// where to stamp would mean duplicating that resolution in the driver
        /// and getting it wrong exactly when the snapshot races. A wire publish
        /// discards the stamp: the server mints the authoritative envelope, as it
        /// always has. The cost is one `Date.now()` and one v4 UUID per publish,
        /// against a per-component send budget measured in tens per burst.
        stamp: MessageStamp,
    },
    /// Send a best-effort `Alert` to page an operator. Fire-and-forget:
    /// it rides the same WS, so the core sends it only while `Active` and
    /// silently drops it otherwise. Title and body are truncated to the proto
    /// caps before they reach the wire. The alert grant is enforced server-side;
    /// a conforming kernel only issues an alert on an alert-granted surface.
    Alert {
        severity: AlertSeverity,
        title: String,
        body: String,
    },
    /// Send a best-effort `Geometry` telemetry frame. Best-effort like `Alert`:
    /// the frame rides the same WS, so the core sends it only while `Active`,
    /// silently dropping it otherwise. The server validates bounds and publishes
    /// the value to the surface's derived geometry channel.
    SendGeometry {
        width: u32,
        height: u32,
        device_pixel_ratio: f64,
    },
    /// Send a best-effort `Status` telemetry snapshot. Best-effort like `Alert`
    /// and `SendGeometry`: sent only while `Active`. The kernel reports raw
    /// per-instance facts; the server derives the health summary and publishes to
    /// the surface's derived status channel.
    SendStatus {
        instances: Vec<InstanceReport>,
        uptime_secs: u64,
        counters: StatusCounters,
    },
    /// Publish one of the kernel's reserved `local:` control planes. Carries no
    /// correlation: the kernel is not a component awaiting a `PublishResult`,
    /// and no server answers page-local traffic. The channel must be a
    /// kernel-publish-only entry of `RESERVED_LOCAL_CHANNELS`; anything else is
    /// a kernel bug and panics.
    PublishControl {
        channel: String,
        body: String,
        /// The envelope's non-deterministic values, read at the driver — the
        /// router mints the envelope, so it needs them for the same reason
        /// `Publish` does.
        stamp: MessageStamp,
    },
    /// Orderly shutdown requested by the kernel (test teardown or page unload):
    /// close the transport, fail any outstanding publishes with
    /// `ConnectionLost`, and enter the terminal `Closed` state (no reconnect).
    Close,
}

/// One publish, as it reaches the core: everything [`Command::Publish`] carries.
///
/// A struct rather than a parameter list because `instance`, `port`, and `body`
/// are all `String` and `subject_instance`/`urgency` are both `Option`, so a
/// transposed pair would typecheck and misroute or misattribute the message —
/// the same argument the server's `PublishRequest` makes. See
/// [`Command::Publish`]'s field docs for each field's contract.
struct PublishIntent {
    correlation: u64,
    instance: String,
    port: String,
    body: String,
    subject_instance: Option<String>,
    urgency: Option<Urgency>,
    stamp: MessageStamp,
}

/// The non-deterministic values an envelope needs, read by the driver and handed
/// to the core as data. Carried on [`Command::Publish`]; see that field's docs
/// for why it is stamped unconditionally.
///
/// This is the sans-I/O seam for envelope synthesis, the same shape as the
/// `now: Millis` every input carries: the core never reads a clock or an entropy
/// source, so a test drives it with fixed values and asserts exact envelopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageStamp {
    /// A fresh v4 UUID for the envelope's `message_id`. Uniqueness is the whole
    /// requirement — components that need exactly-once-seen track their own
    /// high-water by `message_id`, on every hosting.
    pub message_id: Uuid,
    /// Wall-clock publish time. Never used for ordering (a wall clock steps);
    /// `local:` ordering is the router's dense per-channel seq.
    pub publish_ts: DateTime<Utc>,
}

/// A `PublishBatch` on the wire, awaiting its `PublishBatchResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingBatch {
    /// The instance whose flush this is.
    instance: String,
    /// The entries the frame carries, kept so a `RateLimited` answer can re-park
    /// the batch verbatim rather than reconstruct it.
    entries: Vec<BatchEntry>,
    /// The control ops the frame carries, kept for the same reason. Re-parked
    /// with the entries: a refused batch was applied nowhere, ops included.
    ops: Vec<BatchDeferredOp>,
}

// TODO(attach-cutover): this constant and the outbox/retry plane it drives
// (`pump_outbox`, `retry_wakeup`, `outbox_blocked`, `on_retry_tick`) are
// duplicated by `brenn_attach_client::publish::Outboxes`, which is the surviving
// copy and already arms its timer more narrowly. Delete this plane when the
// kernel embeds that crate.

/// How long the kernel waits before re-offering a refused outbox head.
///
/// A constant, not config. The server's backstop refill — 15s per publish by
/// default — is what decides when the head is admitted; this only decides how
/// promptly the kernel notices, and a 1s probe against that refill is idle-cheap
/// and costs nothing when no outbox is blocked (the timer is disarmed). A knob
/// here would be a number nobody can state a requirement for.
const RETRY_INTERVAL_MS: u64 = 1_000;

// TODO(attach-cutover): this vocabulary is duplicated by
// `crate::session::Effect`, the half of it a page still asks for once the
// connection's own effects are the attach driver's business. Delete it here when
// the kernel cuts over.

/// An effect the driver must execute, in order.
///
/// Not `Eq`: `SendFrame` may carry a `ClientFrame::Geometry`, whose `f64`
/// device-pixel-ratio has no total equality. `PartialEq` is retained.
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// Open a transport connection to this fully-formed URL (query included).
    Connect { url: String },
    /// Close the current transport, best-effort. In `Connecting` this cancels a
    /// still-pending connect attempt.
    CloseTransport,
    /// Arm the timer to fire at this deadline, or disarm it (`None`).
    SetWakeup(Option<Millis>),
    /// Arm the outbox-retry timer to fire at this deadline, or disarm it
    /// (`None`).
    ///
    /// A separate deadline from [`Effect::SetWakeup`], which carries the
    /// connection's liveness/handshake schedule: the two are independent
    /// promises and folding them into one would make each re-arm cancel the
    /// other. The core states the deadline; the driver owns the clock and the
    /// select arm, the same division every timer here uses.
    SetRetryWakeup(Option<Millis>),
    /// Arm the release timer to fire when the soonest message parked on a
    /// confined channel comes due, or disarm it (`None`). The deadline is epoch
    /// milliseconds UTC, not [`Millis`]: a release time is a wall-clock instant a
    /// component named, so the driver converts it against the wall clock it reads
    /// and the core states it in the currency the model uses.
    ///
    /// A third deadline alongside [`Effect::SetWakeup`] and
    /// [`Effect::SetRetryWakeup`] for the same reason those two are separate:
    /// each is an independent promise, and folding them would make every re-arm
    /// cancel the others. Emitted only when the soonest deadline changes.
    SetReleaseWakeup(Option<u64>),
    /// Send a client frame over the transport; the driver serializes and
    /// writes it.
    SendFrame(ClientFrame),
    /// Emit a control-plane event to the kernel's EventStream.
    EmitEvent(Event),
    /// Publish one of the kernel's reserved `local:` control planes, minting the
    /// envelope's stamp at the driver.
    ///
    /// The core decides *that* a control publish happens (a parked batch hit its
    /// cap, a reconcile orphaned one) but cannot mint the envelope: it reads
    /// neither a clock nor an entropy source. So it says so as an effect and the
    /// driver stamps it, exactly as it stamps a handle-issued publish at the same
    /// edge, and feeds it back as `Command::PublishControl`. The alternative —
    /// stamping every input on the chance the core toasts — would put an unused
    /// UUID on every frame the page receives.
    PublishControl { channel: String, body: String },
}

/// A control-plane event the core emits for the kernel (delivered by the driver
/// on its EventStream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The handshake completed: `Welcome` was received and validated. Carries
    /// the resolved binding table and identity so the kernel can wire components.
    Connected {
        bindings: SurfaceBindings,
        participant_id: String,
        /// The server's advertised publish body cap from this `Welcome`. The
        /// driver seeds the handle's publish gate with it so an oversized publish
        /// is rejected locally, before it reaches the wire.
        max_body_bytes: u64,
        /// Whether this surface holds the alert grant, from `Welcome`. The kernel
        /// drops a `brenn-alert` from an ungranted component with a `log(warn)`
        /// breadcrumb and gates the panic-path alert on it; a conforming kernel
        /// never sends an ungranted `Alert`.
        alert_granted: bool,
        /// Whether this surface holds the takeover grant, from `Welcome`. The
        /// kernel drops a `brenn-takeover-request` from a component on an
        /// ungranted surface and never pushes an overlay; mirrors `alert_granted`.
        takeover_granted: bool,
        /// The surface error-report floor from this `Welcome`. `Some(floor)`: the
        /// reserved `#brenn`/`error-reports` output port is live; the driver seeds
        /// the handle's publish gate so `ClientHandle::report` publishes a report
        /// at `floor` and above to it and keeps lower levels console-only. `None`:
        /// no reserved port; every report stays console-only. Mirrors the
        /// `alert_granted` rights-from-the-server pattern.
        error_report_floor: Option<LogLevel>,
        /// The surface self-description telemetry parameters from this `Welcome`.
        /// The kernel observes the viewport and per-instance mount status and
        /// reports them via
        /// [`ClientHandle::send_geometry`](crate::ClientHandle::send_geometry) /
        /// [`send_status`](crate::ClientHandle::send_status) on this interval.
        surface_description: SurfaceDescription,
    },
    /// The live connection went away for a diagnosable reason. Reconnection
    /// proceeds via backoff; the kernel surfaces the reason (e.g. a banner).
    Disconnected { reason: DisconnectReason },
    /// A fatal protocol error: a server frame could not be reconciled with the
    /// protocol contract. Terminal — the client does not reconnect.
    Fatal { detail: String },
    /// The server closed with the stale-build code: this client compiled against
    /// an older build than the server now serves. Terminal — the client does not
    /// reconnect; the kernel's bootstrap performs the (capped) reload. The client
    /// never reloads anything itself. `server_build` is the build id the server
    /// reported in the close reason: opaque peer-supplied text (bounded to the WS
    /// close-reason limit), never validated against any build-id shape. Render it
    /// as text only — never interpolate it into markup or a URL.
    ReloadRequired { server_build: String },
    /// The outcome of a publish issued through the handle, routed to the kernel by
    /// its `correlation`. `status` is the server's wire outcome, a core-side
    /// local rejection (stale-snapshot race around a reconnect), or
    /// `ConnectionLost` when the connection dropped with the publish still
    /// outstanding.
    PublishResult {
        instance: String,
        port: String,
        correlation: u64,
        status: PublishStatus,
    },
    /// A tolerated post-`Unsubscribe` straggler `Deliver` was discarded.
    /// Diagnostic only — the discard semantics (token untouched, no port
    /// delivery) are unchanged. `dropped` is the straggler's dropped-count,
    /// which is discarded along with it. Emitted at most once per channel per
    /// activation span (see `ChannelState::straggler_reported`), so its
    /// EventStream rate is client-paced, not server-paced.
    StragglerDiscarded {
        channel: String,
        seq: u64,
        dropped: u64,
    },
    /// An activation entry returned err. Diagnostic: the buffer was discarded and
    /// a failure counted, but the instance is alive and still being delivered.
    /// The embedder surfaces this however it likes; it is not an error card and
    /// not a status transition.
    ActivationFailed { instance: String, message: String },
    /// An instance is terminal: its activation entry trapped. Its memory is
    /// presumed poisoned, so the kernel has stopped delivering to it and dropped
    /// its pending queues. Its subscription rings live on — they are
    /// page-lifetime and shared with whatever else binds those channels.
    ///
    /// Never page death, and never a sibling's problem: a trap has exactly one
    /// subject. The embedder renders the error card and reports the death.
    InstanceFailed { instance: String, reason: String },
    /// A publish on `local:brenn/overlay-state` was refused: the message was
    /// dropped and the kernel's recorded overlay is unchanged. `instance` is the
    /// publisher, `reason` names the rule it broke. The embedder reports it —
    /// the plane's only legitimate publisher is the surface's own chrome, so
    /// anything else is a wiring fault an operator has to see.
    OverlayStateRejected { instance: String, reason: String },
}

/// The disposition of a publish, carried on [`Event::PublishResult`]. It unifies
/// the server's wire [`PublishOutcome`] with the core-side local rejections and
/// the connection-drop signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishStatus {
    /// Server accepted the publish.
    Ok,
    /// Server rate-limited it (its per-connection token bucket). The client does
    /// not retry — that is the instance's business.
    RateLimited,
    /// The body exceeded the server's cap. Carries the server's reported
    /// `len`/`max` when the server rejected it, or the client's own view when
    /// the core rejected it before sending (a stale-snapshot race).
    BodyTooLarge { len: u64, max: u64 },
    /// `(instance, port)` is not a bound output in the current bindings: the
    /// core refused to send an unbound-port publish (a stale-snapshot race).
    UnboundPort,
    /// The connection was not `Active` when the command reached the core (a
    /// stale-snapshot race): the publish was not sent.
    NotConnected,
    /// The connection dropped with this publish's result still outstanding.
    ConnectionLost,
    /// The server accepted the frame but the durable publish failed on a path
    /// that must not kill the connection (the reserved error-report port
    /// backstop). Client-facing meaning is "it did not land"; the client does
    /// not retry.
    Failed,
    /// A `local:` plane guard refused the body: the page-local router minted
    /// nothing, so the message was neither retained nor delivered. The
    /// violation itself is reported separately, attributed to the publisher.
    Refused,
}

/// The three local publish pre-check rejections, in authoritative check order.
/// Single source of truth shared by the handle's fast gate and the core's
/// authoritative recheck; each caller converts into its own reject vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishCheckReject {
    NotConnected,
    UnboundPort,
    BodyTooLarge { len: u64, max: u64 },
}

impl From<PublishCheckReject> for PublishStatus {
    fn from(reject: PublishCheckReject) -> Self {
        match reject {
            PublishCheckReject::NotConnected => PublishStatus::NotConnected,
            PublishCheckReject::UnboundPort => PublishStatus::UnboundPort,
            PublishCheckReject::BodyTooLarge { len, max } => {
                PublishStatus::BodyTooLarge { len, max }
            }
        }
    }
}

/// The local publish pre-check: predicates and their order live here and only
/// here. `output_bound` is lazy, so a caller may resolve it however it likes.
///
/// `reachable` is "connected, **or** the target is a `local:` port": page-local
/// traffic never touches the wire, so the link being down is no reason to reject
/// it — that offline-correctness is the whole point of the class (the kiosk that
/// must still accept a takeover with the network out). Callers compute it; the
/// distinction cannot be made here, where no bindings are in scope.
///
/// The body cap applies to local publishes too, deliberately. It is nominally a
/// server ingress limit, but ports are ports: a component's body-size contract
/// must not silently change because an operator rebound its output port from
/// `brenn:` to `local:`. It also bounds the router's rings, which are page memory.
pub(crate) fn check_publish(
    reachable: bool,
    output_bound: impl FnOnce() -> bool,
    body_len: u64,
    max_body_bytes: u64,
) -> Result<(), PublishCheckReject> {
    if !reachable {
        return Err(PublishCheckReject::NotConnected);
    }
    if !output_bound() {
        return Err(PublishCheckReject::UnboundPort);
    }
    if body_len > max_body_bytes {
        return Err(PublishCheckReject::BodyTooLarge {
            len: body_len,
            max: max_body_bytes,
        });
    }
    Ok(())
}

/// Why a live connection dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    /// No inbound text frame arrived within `liveness_multiplier × heartbeat`;
    /// the connection is treated as dead.
    LivenessTimeout,
    /// The transport closed under us — a clean peer WS close or a transport
    /// failure — while live or awaiting `Welcome`.
    TransportClosed,
}

/// Connection lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// A connect attempt is in flight. Handshake deadline armed.
    Connecting,
    /// Transport open; awaiting the first `Welcome`. Same handshake deadline.
    AwaitingWelcome,
    /// `Welcome` received and validated; the connection is live.
    Active,
    /// Waiting out a backoff delay before the next connect attempt.
    Backoff,
    /// A fatal protocol error was hit. Terminal: no reconnect; further inputs
    /// are absorbed.
    Fatal,
    /// A stale-build close (code `STALE_BUILD_CLOSE_CODE`) was observed.
    /// Terminal: no reconnect; further inputs are absorbed. The kernel reloads.
    ReloadRequired,
    /// The kernel requested an orderly shutdown (`Command::Close`). Terminal: the
    /// transport is closed, no reconnect, and further inputs are absorbed.
    Closed,
}

/// Per-channel wire-subscription state. `Active → Unsubscribed` is the detach
/// edge (refcount reaching zero on an `Active` channel sends `Unsubscribe`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireState {
    /// No `Subscribe` is outstanding for this channel on the wire.
    Unsubscribed,
    /// A `Subscribe` was sent; awaiting its `SubscribeResult`.
    Pending,
    /// `SubscribeResult::Ok` received; the subscription is live.
    Active,
}

/// Per-channel bookkeeping: how many local ports are bound to it, its wire
/// state, and the high-water resume token. N ports on one channel = one wire
/// subscription.
struct ChannelState {
    refcount: u32,
    wire: WireState,
    /// The opaque [`Cursor`] of the last `Deliver` accepted while `Active`,
    /// presented as `Subscribe.resume` on the next reconnect re-`Subscribe`. The
    /// kernel never interprets it — it stores the latest accepted one and echoes
    /// it verbatim. Its lifetime is exactly "at least one port attached": it is
    /// discarded the moment the refcount reaches zero (so a later fresh 0→1
    /// attach subscribes with `resume: None` and receives the retained tail
    /// rather than resuming past the latest value). Survives disconnects and
    /// Backoff — the ports stay attached across a transport blip and are resumed
    /// at reconcile.
    token: Option<Cursor>,
    /// The span high-water: the largest delivery-time `seq` accepted on the
    /// current subscription span. Class-blind — the server assigns `seq` strictly
    /// increasing per span for both wire classes, so a `Deliver` whose `seq` does
    /// not exceed this is a server bug and fatal. Reset to `None` at each
    /// `Subscribe` (a span starts at its `SubscribeResult`, the server restarting
    /// its counter at 1).
    span_hw: Option<u64>,
    /// Whether this channel has reached `WireState::Active` at some point on the
    /// current connection (including the momentary Active of the
    /// deferred-`Unsubscribe` path). A `Deliver` while this is set but the
    /// channel is not *currently* `Active` is a tolerated post-`Unsubscribe`
    /// straggler and is discarded; a `Deliver` while this is unset is
    /// inexplicable (the server's FIFO writer orders `SubscribeResult` before
    /// any replay) and is fatal. Reset on transport teardown — it is
    /// per-connection. The discard is surfaced via
    /// [`Event::StragglerDiscarded`].
    has_been_active: bool,
    /// Whether a `StragglerDiscarded` diagnostic has already been emitted for
    /// the current post-`Active` window. Set on the first straggler after the
    /// channel leaves `Active`; cleared when the channel reaches `Active`
    /// again (`on_subscribe_result`) and on transport teardown
    /// (`reset_bus_plane`). Caps the diagnostic at one EventStream
    /// event per channel per activation span — the EventStream's overflow
    /// contract is a panic (`Driver::emit`), so nothing server-paced may ride
    /// it unbounded.
    straggler_reported: bool,
}

impl ChannelState {
    /// Release one port reference. Panics on underflow (a detach without a
    /// matching attach is a core bug, not peer input). On reaching zero the
    /// resume token is discarded — its lifetime is exactly "at least one port
    /// attached", so a later fresh 0→1 attach subscribes with `resume: None`
    /// and receives the retained tail rather than resuming past the latest
    /// value. Returns the new refcount.
    fn release_ref(&mut self) -> u32 {
        self.refcount = self
            .refcount
            .checked_sub(1)
            .expect("surface client: channel refcount underflow");
        if self.refcount == 0 {
            self.token = None;
        }
        self.refcount
    }

    /// Prepare a fresh `Subscribe` for this channel: transition it to `Pending`,
    /// reset the span high-water, and return the resume to present on the wire.
    /// Callable from `Unsubscribed` (a fresh attach or a reconnect subscribe) or
    /// from `Active` (a server-initiated re-anchor drives `Active` → `Pending`
    /// with the stored cursor); never from `Pending`.
    ///
    /// Class-blind for every wire class: a fresh attach (no stored cursor)
    /// presents `resume: None` and receives the retained window; a reconnect
    /// echoes the stored opaque cursor verbatim. The kernel never interprets it —
    /// the server decides what a cursor means.
    fn prepare_subscribe(&mut self) -> Option<Cursor> {
        self.wire = WireState::Pending;
        self.span_hw = None;
        self.token.clone()
    }
}

/// The identity of one wire subscription: the principal that owns it and the
/// channel it covers.
///
/// The principal is the grain the whole subscription is cut at — its own push
/// window server-side, its own resume cursor, its own lag. Two instances bound
/// to one channel are two `SubKey`s: two `Subscribe`s, two cursors, two
/// `Deliver` streams, exactly as two backend `[[app]]`s on one channel would be.
/// Two *ports of one instance* on one channel are one `SubKey`, refcounted —
/// that is the only case where a surface subscription is genuinely shared.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubKey {
    /// The owning component instance. Every surface subscription is an
    /// instance's — the kernel-grain layout subscription was the last producerless
    /// exception and it is gone.
    instance: String,
    channel: String,
}

impl SubKey {
    /// The subscription a component instance's binding draws from.
    fn for_instance(instance: &str, channel: &str) -> Self {
        Self {
            instance: instance.to_string(),
            channel: channel.to_string(),
        }
    }
}

/// The depth the wire mirror for `sub` is built at: the fold over the
/// subscription's bindings of `max(push_depth, retain_depth)`.
///
/// Both halves are load-bearing — `retain_depth` is what a binding reads as
/// context, `push_depth` is what it can be handed as new — and the store is the
/// only thing holding either. A mirror is created at two moments (the `Welcome`
/// reconcile, and a registration attaching to a subscription whose mirror was
/// discarded at refcount zero), so the fold lives here rather than at either of
/// them.
fn wire_store_depth(bindings: &SurfaceBindings, sub: &SubKey) -> u64 {
    bindings
        .subscriptions
        .iter()
        .filter(|b| b.instance == sub.instance && b.channel == sub.channel)
        .map(|b| b.push_depth.max(b.retain_depth))
        .max()
        .unwrap_or(0)
}

/// Whether a store is a reserved control plane's: the kernel's own, seeded at
/// construction from the contract and never declared — or un-declared — by a
/// `Welcome`.
fn reserved_store(key: &StoreKey) -> bool {
    match key {
        StoreKey::Confined(channel) => reserved_local_channel(channel).is_some(),
        StoreKey::Wire(_) => false,
    }
}

/// The `Welcome` handshake fields the core consumes, grouped so `on_welcome`
/// takes one payload rather than a long positional argument list.
struct WelcomeParams {
    participant_id: String,
    heartbeat_secs: u32,
    max_body_bytes: u64,
    alert_granted: bool,
    takeover_granted: bool,
    error_report_floor: Option<LogLevel>,
    surface_description: SurfaceDescription,
    bindings: SurfaceBindings,
}

/// Construction parameters the connection-lifecycle layer needs. A superset —
/// the full public `ClientConfig` — is assembled with the handle and driver.
pub struct CoreConfig {
    /// Bare `ws(s)://…/surface/<slug>/ws`, no query; the core appends `?build`.
    pub url: String,
    pub build_id: String,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub connect_timeout: Duration,
    /// Multiple of `heartbeat_secs` (from `Welcome`) of inbound silence that
    /// marks the connection dead. Default 3, matching the server's reaper.
    pub liveness_multiplier: u32,
    /// Seed for the backoff-jitter PRNG. Distinct per client (seeded from
    /// per-target entropy by `handle::new`) so a fleet reconnecting in lockstep
    /// after a deploy restart decorrelates its reconnect attempts; a fixed value
    /// in tests keeps the core deterministic. Only cross-client distinctness
    /// matters — this is load-spreading entropy, never a secret.
    pub backoff_jitter_seed: u64,
    /// The page-load epoch stamped on every `LocalPos` this core's router
    /// assigns. Minted once per page by `handle::new` (the core reads no entropy
    /// itself), so it is constant for the router's whole life and a fixed value
    /// in tests keeps the core deterministic.
    ///
    /// Per-page: it changes only when the page reloads, which is also the only
    /// event that discards the rings it labels. It never crosses the wire — a
    /// `LocalPos` is page-local by contract.
    pub local_epoch: Uuid,
}

/// The sans-I/O connection state machine.
pub struct ClientCore {
    connect_url: String,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    connect_timeout_ms: u64,
    liveness_multiplier: u32,
    /// Inbound-silence window in millis, computed at each `Welcome` from
    /// `heartbeat_secs × liveness_multiplier`. Zero until the first `Welcome`.
    liveness_ms: u64,
    state: State,
    backoff_step: u32,
    /// The single armed deadline: a handshake deadline in
    /// `Connecting`/`AwaitingWelcome`, a backoff deadline in `Backoff`, a
    /// liveness deadline in `Active`.
    deadline: Millis,
    /// The bindings from the most recent `Welcome`; `None` until the first one.
    /// Used to resolve `(instance, port)` attaches to their channel.
    bindings: Option<SurfaceBindings>,
    /// The server's publish-body cap from the most recent `Welcome`; `0` until
    /// the first one. The core rejects an oversized publish against it before
    /// sending, so no doomed frame ever reaches the wire.
    max_body_bytes: u64,
    /// Whether the alert plane is granted for this surface, from the most recent
    /// `Welcome`; `false` until the first one. The core drops any `Alert` command
    /// while this is unset, so `ClientHandle::alert` on an ungranted surface can
    /// never reach the wire and trip the server's grant-violation session kill.
    alert_granted: bool,
    /// The surface error-report floor from the most recent `Welcome`;
    /// `Some(floor)` means the reserved `#brenn`/`error-reports` output port is
    /// live and the kernel publishes reports at `floor` and above onto it. `None`
    /// (the default until the first `Welcome`, and whenever the operator leaves
    /// the error channel unconfigured) means no reserved port: the kernel keeps
    /// reports console-only and the core rejects a publish to the reserved port
    /// as an unbound-port violation, exactly as it would any other unbound pair.
    error_report_floor: Option<LogLevel>,
    /// Publishes sent this connection and awaiting their `PublishResult`, keyed
    /// by `correlation`, valued by the `(instance, port)` the result routes back
    /// to (the wire `PublishResult` carries only the correlation). Drained and
    /// answered `ConnectionLost` on transport teardown — it is per-connection.
    pending_publishes: HashMap<u64, (String, String)>,
    /// Sent `PublishBatch`es awaiting their `PublishBatchResult`, keyed by
    /// correlation.
    ///
    /// A separate correlation space from `pending_publishes`: the two are
    /// different frames with different answers, and the batch's correlations are
    /// the kernel's own (a batch is only ever produced by a flush, never by a
    /// handle caller who needs the number back). Cleared on transport teardown —
    /// it is per-connection, and a batch outstanding when the link died was
    /// already flushed as far as the component is concerned.
    ///
    /// The entries ride along because a `RateLimited` answer re-parks the batch
    /// for retry: nothing else holds them once the frame is out.
    pending_batches: HashMap<u64, PendingBatch>,
    /// The next `PublishBatch` correlation. Monotone per core.
    next_batch_correlation: u64,
    /// Whether the driver's retry timer is currently armed. Mirrors what the last
    /// `SetRetryWakeup` said, so the core can emit the effect only when the
    /// answer changes.
    retry_armed: bool,
    /// The instance dispatched by the most recent [`Self::take_ready_activation`],
    /// if any. The next dispatch resumes strictly *after* it in sorted order,
    /// wrapping — round-robin rather than always-lowest-name.
    ///
    /// Fairness is not cosmetic here. A component that republishes onto a
    /// `local:` channel one of its own bindings reads is ready again the instant
    /// its flush routes, so a lowest-name-wins pick would hand every activation
    /// to the same instance forever and no sibling would ever run. The cursor
    /// bounds each instance to one activation per pass over the ready set, which
    /// is what makes the driver's per-turn dispatch budget a fair one.
    dispatch_cursor: Option<String>,
    /// Per-subscription refcount + wire state, keyed by [`SubKey`] — the owning
    /// principal *and* the channel, because the subscription is the principal's,
    /// not the page's. Wire subscriptions only: a confined channel has no wire
    /// state, only the store every class has.
    channels: HashMap<SubKey, ChannelState>,
    /// Every channel this page retains, keyed by [`StoreKey`] — the channel
    /// address for a confined channel, the subscription for a transportable one
    /// (the distinction, and its reason, are `store`'s).
    ///
    /// Depth is the fold over the store's bindings of `max(push_depth,
    /// retain_depth)`: the store serves delivery as well as context, so a
    /// binding whose push window is deeper than its retained one still needs the
    /// capacity to hold what it will be served. A confined store takes its
    /// server-resolved `ring_depth` as a floor, and a reserved plane its
    /// contract-fixed depth. Each binding still windows at its own depths, and
    /// each attach primes from the channel's declared retention rather than the
    /// folded depth, so a deeper store never widens anyone's visibility and
    /// never owes anyone the padding as new.
    ///
    /// Fed for **every** channel uniformly, registered instances or not, because
    /// retention is what makes a dropped message recoverable and is not a fact
    /// about who is listening. Page-lifetime state: a reconnect preserves every
    /// store, since discarding one would manufacture a loss the link never
    /// caused; only a page reload clears them, and that mints a new
    /// `local_epoch` too. The reserved `local:brenn/*` planes are seeded at
    /// construction at their contract-fixed depths, because the contract, not a
    /// `Welcome`, is what declares them.
    stores: HashMap<StoreKey, SurfaceChannelStore>,
    /// Instances delivered by activation, keyed by instance id. Holds the
    /// scheduler flags, the sink carryover, and the parked flushes; what each of
    /// its bindings is owed is a cursor in that channel's store. Every `dom` and
    /// headless instance the page delivers to is in here — it is the only
    /// delivery model there is.
    ///
    // TODO(attach-cutover): the registration half of this table and the passes
    // over it are duplicated by `crate::registry::Registrations` (positions and
    // subscription references) and `crate::outbound::SurfaceOutbound` (the
    // parked flushes). Delete them here when the kernel cuts over.
    registered: HashMap<String, RegisteredInstance>,
    /// The page-load epoch stamped on every `LocalPos` (`CoreConfig::local_epoch`).
    local_epoch: Uuid,
    /// This surface's participant id from the most recent `Welcome`
    /// (`surface:<slug>`); empty until the first one. The `local:` router stamps
    /// envelopes with identities derived from it — for wire publishes the server
    /// derives the sender from the instance its own declaration set admits and
    /// the client asserts nothing, but no server sees page-local traffic, so the
    /// router is the only party that can attribute it.
    participant_id: String,
    /// The backoff-jitter PRNG, seeded once from `CoreConfig.backoff_jitter_seed`.
    /// Seeded distinctly per client so a lockstep fleet decorrelates its
    /// reconnects; deterministic given the seed so the core stays purely
    /// testable. Advanced only by `backoff_delay_ms`.
    jitter_rng: SplitMix64,
    /// The overlay chrome reported holding on [`LOCAL_OVERLAY_STATE_CHANNEL`],
    /// or `None` when it reported none — the one field of the status report the
    /// kernel does not derive from its own instance table.
    ///
    /// Read at the router's mint point rather than inferred from routed takeover
    /// traffic: chrome drops takeover messages the router routes (unknown
    /// instance, mismatched release), so "what the router carried" and "what
    /// chrome holds" are different facts, and only the second one is the
    /// overlay. Page-local like the stores — a reload starts at `None`, which is
    /// the truth for a fresh page.
    overlay: Option<OverlayReport>,
    /// The release deadline the driver's timer is armed at, epoch milliseconds
    /// UTC, or `None` when nothing is parked on any confined channel.
    ///
    /// Held so the re-arm after every input emits [`Effect::SetReleaseWakeup`]
    /// only when the soonest deadline actually moved: parking, editing,
    /// cancelling, and releasing can all change it, and a page whose components
    /// publish steadily would otherwise re-state the same deadline on every turn.
    release_wakeup: Option<u64>,
    /// What the backend says each `(channel, instance)` has parked on a
    /// transportable output channel, release-ordered — the page's cache of the
    /// only answer there is.
    ///
    /// A transportable channel's deferral authority is the backend: its parked
    /// entries can outlive the page, they release on the server's clock, and
    /// every session of the surface shares the sender identity that owns them. So
    /// the page holds no deferred set for one, it holds what it was last told,
    /// and it is told by [`ServerFrame::DeferredView`] — a full snapshot that
    /// replaces the entry wholesale.
    ///
    /// Cleared at every `Welcome`, so an unmentioned pair is an empty set rather
    /// than a stale one. Distinct from a wire store, which mirrors delivered
    /// retention for a *subscription*; this mirrors a *sender's* schedule.
    deferred_views: HashMap<(String, String), Vec<DeferredViewEntry>>,
}

impl ClientCore {
    /// Build the core and start connecting immediately (connect-on-spawn). The
    /// returned effects open the first transport and arm the handshake timer.
    pub fn new(config: CoreConfig, now: Millis) -> (Self, Vec<Effect>) {
        let mut core = Self {
            connect_url: build_connect_url(&config.url, &config.build_id),
            initial_backoff_ms: duration_ms(config.initial_backoff),
            max_backoff_ms: duration_ms(config.max_backoff),
            connect_timeout_ms: duration_ms(config.connect_timeout),
            liveness_multiplier: config.liveness_multiplier,
            liveness_ms: 0,
            state: State::Connecting,
            backoff_step: 0,
            deadline: now,
            bindings: None,
            max_body_bytes: 0,
            alert_granted: false,
            error_report_floor: None,
            pending_publishes: HashMap::new(),
            pending_batches: HashMap::new(),
            next_batch_correlation: 0,
            retry_armed: false,
            dispatch_cursor: None,
            channels: HashMap::new(),
            // The reserved control planes exist from the page's first instant,
            // before any `Welcome`: they are contract-defined, so their depths
            // come from the contract and not from a server that has not answered
            // yet. An unbound plane has no binding windows to fold in, so the
            // contract depth is the whole of it. Seeding them here is what
            // "auto-bound by the kernel" means.
            stores: RESERVED_LOCAL_CHANNELS
                .iter()
                .map(|c| {
                    (
                        StoreKey::Confined(c.address.to_string()),
                        SurfaceChannelStore::new(config.local_epoch, c.ring_depth),
                    )
                })
                .collect(),
            registered: HashMap::new(),
            local_epoch: config.local_epoch,
            participant_id: String::new(),
            jitter_rng: SplitMix64::new(config.backoff_jitter_seed),
            overlay: None,
            release_wakeup: None,
            deferred_views: HashMap::new(),
        };
        let effects = core.begin_connect(now);
        (core, effects)
    }

    /// Feed one input at monotonic time `now`; returns the ordered effects.
    ///
    /// The release timer is re-armed after every input, here rather than at each
    /// site that could move it: a park at flush, a release sweep, and a depth
    /// retune that refuses new parks all change when the next message comes due,
    /// and one re-arm over the stores cannot be forgotten at a new site the way
    /// per-site arming can.
    pub fn on_input(&mut self, input: Input, now: Millis) -> Vec<Effect> {
        let mut effects = self.dispatch_input(input, now);
        effects.extend(self.rearm_release());
        effects
    }

    /// State the release deadline if the soonest parked message moved.
    ///
    /// Confined stores only: a transportable channel's deferred set lives with its
    /// retention authority, which is the backend, so the page parks nothing there
    /// and has nothing to time.
    fn rearm_release(&mut self) -> Vec<Effect> {
        let next = self
            .stores
            .iter()
            .filter(|(key, _)| matches!(key, StoreKey::Confined(_)))
            .filter_map(|(_, store)| store.next_release())
            .min();
        if next == self.release_wakeup {
            return Vec::new();
        }
        self.release_wakeup = next;
        vec![Effect::SetReleaseWakeup(next)]
    }

    /// Take every confined channel's due parked messages into retention.
    ///
    /// Each release is an ordinary arrival on its channel: a fresh tail seq, the
    /// same eviction charges, and every bound instance woken by it exactly as a
    /// page-local publish wakes them. Channels are swept in address order so a
    /// page releasing on several at one fire enacts its loud rungs
    /// reproducibly.
    fn on_release_due(&mut self, now_ms: u64) -> Vec<Effect> {
        let mut due: Vec<String> = self
            .stores
            .iter()
            .filter_map(|(key, store)| match key {
                StoreKey::Confined(channel)
                    if store.next_release().is_some_and(|at| at <= now_ms) =>
                {
                    Some(channel.clone())
                }
                _ => None,
            })
            .collect();
        due.sort();
        let mut effects = Vec::new();
        for channel in due {
            let report = self
                .stores
                .get_mut(&StoreKey::Confined(channel.clone()))
                .expect("surface client: the store named by this sweep")
                .release_due(now_ms);
            tracing::debug!(
                %channel,
                released = report.released.len(),
                "surface client: released parked messages into retention"
            );
            // A release is the first moment a parked message is observable, so a
            // plane whose state the kernel reports records it here — the same
            // point an immediate publish records it.
            for released in &report.released {
                self.record_overlay_state(&released.message);
            }
            effects.extend(self.enact_overflow(&channel, report.overflow));
        }
        effects
    }

    /// The input's own state transition, ahead of the release re-arm every input
    /// gets ([`Self::on_input`]).
    fn dispatch_input(&mut self, input: Input, now: Millis) -> Vec<Effect> {
        match (self.state, input) {
            (_, Input::HostFatal { detail }) => self.go_fatal(detail),
            (State::Connecting, Input::Opened) => {
                self.state = State::AwaitingWelcome;
                Vec::new()
            }
            (State::Connecting, Input::ConnectFailed) => self.enter_backoff(now),
            (State::AwaitingWelcome | State::Active, Input::Disconnected { code, reason }) => {
                // A stale-build close is the one disconnect that is terminal: the
                // server serves a newer build than this client compiled against,
                // so reconnecting would only race the reload. Every other close
                // (or a transport failure) is an ordinary drop → backoff.
                if code == Some(STALE_BUILD_CLOSE_CODE) {
                    self.enter_reload_required(reason)
                } else {
                    // The transport dropped while live or awaiting `Welcome`.
                    // Surface it so the kernel can show "Reconnecting…", then back
                    // off. No `CloseTransport`: the driver already dropped the
                    // connection before feeding this input.
                    let mut effects = vec![Effect::EmitEvent(Event::Disconnected {
                        reason: DisconnectReason::TransportClosed,
                    })];
                    effects.extend(self.enter_backoff(now));
                    effects
                }
            }
            (State::AwaitingWelcome, Input::TextFrame(text)) => self.on_text_awaiting(&text, now),
            (State::Active, Input::TextFrame(text)) => self.on_text_active(&text, now),
            (State::AwaitingWelcome | State::Active, Input::BinaryFrame) => {
                self.go_fatal("unexpected binary frame from server".to_string())
            }
            // A publish issued after the core went `Fatal` (the advisory publish
            // gate can lag the terminal transition) still owes its caller exactly
            // one result: `on_publish` answers `NotConnected` since the state is
            // not `Active` and `local_router_live` is false, sending no frame and
            // routing nothing locally. Every other command stays absorbed by the
            // terminal arm below.
            (
                State::Fatal | State::ReloadRequired | State::Closed,
                Input::Command(Command::Publish {
                    correlation,
                    instance,
                    port,
                    body,
                    subject_instance,
                    urgency,
                    stamp,
                }),
            ) => self.on_publish(PublishIntent {
                correlation,
                instance,
                port,
                body,
                subject_instance,
                urgency,
                stamp,
            }),
            // A control-plane publish is the terminal transition's own final
            // notification: the kernel folds the death event and publishes the
            // matching link state (`fatal` / `reloading`) so chrome can draw the
            // terminal banner. The page-local router and its rings outlive the
            // terminal transition and chrome is still mounted, so this must route
            // rather than be absorbed by the catch-all below — otherwise the death
            // banner lands on a dead router and is never drawn.
            (
                State::Fatal | State::ReloadRequired | State::Closed,
                Input::Command(Command::PublishControl {
                    channel,
                    body,
                    stamp,
                }),
            ) => self.on_publish_control(channel, body, stamp),
            // A release is the page's own schedule maturing, and it is honoured in
            // every state, terminal ones included — like the control publish above,
            // and for a related reason: the confined router and its stores outlive
            // the terminal transition, and a fire absorbed without releasing would
            // leave the driver's armed deadline permanently due.
            (_, Input::ReleaseDue { now_ms }) => self.on_release_due(now_ms),
            // Terminal: after the death decision (fatal error, stale-build
            // reload, or kernel-requested close), any in-flight transport or timer
            // event is expected and simply absorbed — not a bug to panic on.
            // Commands are dropped too; the kernel must quiesce once it receives
            // Event::Fatal or Event::ReloadRequired, or once it called close.
            (State::Fatal | State::ReloadRequired | State::Closed, _) => Vec::new(),
            // Commands are accepted in any live state: the core parks them
            // pre-`Welcome` and resolves them against bindings once a connection
            // is up.
            (_, Input::Command(cmd)) => self.on_command(cmd),
            // Registration is accepted in any live state: the instance's queues
            // come from the bindings, so a pre-`Welcome` registration simply has
            // none until the first one arrives.
            (_, Input::ActivationRegistered { instance }) => {
                self.on_activation_registered(instance)
            }
            (_, Input::ActivationDeregistered { instance }) => {
                self.on_activation_deregistered(&instance)
            }
            // A completion is accepted in any live state, including one the
            // connection dropped out from under: the activation ran and returned,
            // and `local:` delivery plus parking need no link. That is the point
            // of parking.
            (
                _,
                Input::ActivationDone {
                    instance,
                    outcome,
                    buffer,
                    stamps,
                },
            ) => self.on_activation_done(instance, outcome, buffer, stamps),
            (State::Connecting | State::AwaitingWelcome, Input::Tick) => {
                if now >= self.deadline {
                    let mut effects = vec![Effect::CloseTransport];
                    effects.extend(self.enter_backoff(now));
                    effects
                } else {
                    vec![Effect::SetWakeup(Some(self.deadline))]
                }
            }
            (State::Active, Input::Tick) => {
                if now >= self.deadline {
                    // Inbound silence past the liveness deadline: the connection
                    // is dead. Close, surface the reason, and back off.
                    let mut effects = vec![
                        Effect::CloseTransport,
                        Effect::EmitEvent(Event::Disconnected {
                            reason: DisconnectReason::LivenessTimeout,
                        }),
                    ];
                    effects.extend(self.enter_backoff(now));
                    effects
                } else {
                    vec![Effect::SetWakeup(Some(self.deadline))]
                }
            }
            (State::Backoff, Input::Tick) => {
                if now >= self.deadline {
                    self.begin_connect(now)
                } else {
                    vec![Effect::SetWakeup(Some(self.deadline))]
                }
            }
            (State::Active, Input::RetryTick) => self.on_retry_tick(now),
            // A retry tick outside `Active` has nothing to act on: an outbox can
            // only drain onto a live wire, and `Welcome` re-arms the timer. The
            // timer is disarmed on the way out of `Active`, so this is a
            // straggler tick, not a state to correct.
            (State::Connecting | State::AwaitingWelcome | State::Backoff, Input::RetryTick) => {
                self.disarm_retry()
            }
            // A transport-sourced input arriving in a state that no longer owns
            // the transport it came from is a post-close straggler: the core
            // already told the driver to close that connection (CloseTransport
            // / enter_backoff), and an in-flight frame or close from it is an
            // ordinary async race, not a bug. Absorb it — peer input never
            // panics.
            (
                State::Connecting | State::Backoff | State::AwaitingWelcome | State::Active,
                Input::Opened
                | Input::ConnectFailed
                | Input::Disconnected { .. }
                | Input::TextFrame(_)
                | Input::BinaryFrame,
            ) => Vec::new(),
        }
    }

    /// Enter `Connecting`: emit a `Connect` and arm the handshake timer.
    fn begin_connect(&mut self, now: Millis) -> Vec<Effect> {
        self.state = State::Connecting;
        self.deadline = now.saturating_add_ms(self.connect_timeout_ms);
        vec![
            Effect::Connect {
                url: self.connect_url.clone(),
            },
            Effect::SetWakeup(Some(self.deadline)),
        ]
    }

    // TODO(attach-cutover): the connection lifecycle around here — connect,
    // handshake, `Welcome` intake, liveness, and this backoff schedule — is
    // duplicated by `brenn_attach_client::conn::Connection`. Delete it when the
    // kernel embeds that crate.

    /// Enter `Backoff`: reset the bus plane, fail any outstanding publishes,
    /// consume one backoff step, and arm the backoff timer.
    fn enter_backoff(&mut self, now: Millis) -> Vec<Effect> {
        self.reset_bus_plane();
        let mut effects = self.fail_pending_publishes();
        let delay = self.backoff_delay_ms();
        self.backoff_step = self.backoff_step.saturating_add(1);
        self.state = State::Backoff;
        self.deadline = now.saturating_add_ms(delay);
        effects.push(Effect::SetWakeup(Some(self.deadline)));
        // No wire to retry onto. The outboxes survive the gap; `Welcome` re-arms.
        effects.extend(self.disarm_retry());
        effects
    }

    /// Enter the terminal `ReloadRequired` state on a stale-build close: reset
    /// the bus plane, fail any outstanding publishes with `ConnectionLost`,
    /// surface `ReloadRequired` carrying the server's build id, and disarm the
    /// timer. No reconnect and no `CloseTransport` — the transport is already
    /// gone (the driver dropped it before feeding the disconnect), and the
    /// kernel's bootstrap owns the (capped) reload.
    fn enter_reload_required(&mut self, server_build: String) -> Vec<Effect> {
        self.reset_bus_plane();
        let mut effects = self.fail_pending_publishes();
        self.state = State::ReloadRequired;
        effects.push(Effect::EmitEvent(Event::ReloadRequired { server_build }));
        effects.push(Effect::SetWakeup(None));
        effects.extend(self.disarm_retry());
        effects
    }

    /// Reset every channel's wire state to `Unsubscribed` on transport teardown.
    /// The subscription lives only on the connection that opened it, so a lost
    /// transport invalidates all of them at once — including a still-`Pending`
    /// subscription whose ack will never come (it gets a fresh re-`Subscribe`
    /// at the next reconnect). This
    /// runs the instant the transport goes away, not at the next `Welcome`, so
    /// no command handled while disconnected can observe stale wire state and
    /// emit an `Unsubscribe` (or any other bus-plane frame) with no live
    /// connection to carry it. Refcounts, attachments, and pending pre-`Welcome`
    /// attaches survive; the next `Welcome` derives the wire set fresh. Produces
    /// no effects: nothing goes on the wire while disconnected.
    fn reset_bus_plane(&mut self) {
        // The has-been-`Active` flag and the straggler-diagnostic flag are both
        // scoped to the connection whose transport just went away; an activation
        // span cannot outlive its connection, so the next connection starts with
        // both clear.
        for cs in self.channels.values_mut() {
            cs.wire = WireState::Unsubscribed;
            cs.has_been_active = false;
            cs.straggler_reported = false;
        }
        // Outstanding batches die with the connection that carried them. There is
        // nothing to answer and nothing to retry: the component's guarantee was
        // discharged when the kernel flushed, and a batch the server may or may
        // not have applied is exactly the case a resend would double-apply.
        //
        // The outboxes themselves survive — a queued flush was never sent and is
        // owed the wire — but each instance's in-flight marker is cleared with the
        // frame it named, so the next connection's outbox starts free.
        self.pending_batches.clear();
        for reg in self.registered.values_mut() {
            reg.batch_in_flight = None;
        }
    }

    /// The next backoff delay: doubling-capped nominal with equal jitter applied.
    ///
    /// The nominal is plain doubling from `initial_backoff_ms`, capped at
    /// `max_backoff_ms` (3s → 6 → … → 60s). Equal jitter then spreads it
    /// uniformly over `[nominal/2, nominal]`: a client never retries sooner than
    /// half its nominal step (backoff stays meaningful against a genuinely-down
    /// server) while a lockstep fleet decorrelates across a `nominal/2`-wide
    /// window at every step, including the cap. Integer arithmetic only — modulo
    /// bias over a `u64` draw against a ≤30001-ms range is irrelevant for
    /// load-spreading; `nominal == 0` degenerates to `0`, harmless.
    fn backoff_delay_ms(&mut self) -> u64 {
        let mut nominal = self.initial_backoff_ms;
        for _ in 0..self.backoff_step {
            nominal = nominal.saturating_mul(2);
            if nominal >= self.max_backoff_ms {
                break;
            }
        }
        // Clamps both the loop's overshoot on the last doubling and the
        // `initial_backoff_ms > max_backoff_ms` config edge (`backoff_step == 0`),
        // so the cap lives in exactly one place.
        let nominal = nominal.min(self.max_backoff_ms);
        // Equal jitter: uniform in [nominal/2, nominal]. Never exceeds `nominal`,
        // so a test that ticks at the nominal deadline still fires.
        let half = nominal / 2;
        (nominal - half) + (self.jitter_rng.next_u64() % (half + 1))
    }

    /// A text frame arrived while awaiting the first `Welcome`. Only `Welcome`
    /// is legal here; anything else — unparseable, a non-`Welcome` server frame,
    /// or a frame with a bad binding scheme — is a fatal protocol error.
    fn on_text_awaiting(&mut self, text: &str, now: Millis) -> Vec<Effect> {
        let frame = match serde_json::from_str::<ServerFrame>(text) {
            Ok(frame) => frame,
            Err(err) => return self.go_fatal(format!("unparseable server frame: {err}")),
        };
        match frame {
            ServerFrame::Welcome {
                participant_id,
                heartbeat_secs,
                max_body_bytes,
                alert_granted,
                takeover_granted,
                error_report_floor,
                surface_description,
                bindings,
                ..
            } => self.on_welcome(
                now,
                WelcomeParams {
                    participant_id,
                    heartbeat_secs,
                    max_body_bytes,
                    alert_granted,
                    takeover_granted,
                    error_report_floor,
                    surface_description,
                    bindings,
                },
            ),
            other => self.go_fatal(format!(
                "expected Welcome as the first server frame, got {}",
                frame_type_name(&other)
            )),
        }
    }

    // TODO(attach-cutover): the bindings-application sequence below — this
    // `Welcome` intake, `reconcile_stores`, `reconcile_registered`, and
    // `send_parked_batches` — is duplicated by the two-phase connect in
    // `crate::connect`, `crate::registry` and `crate::outbound`, which take the
    // same wiring off the bindings document instead. Delete it here when the
    // kernel cuts over to them.

    /// Process the `Welcome` handshake: validate binding schemes, reset backoff,
    /// enter `Active`, arm the liveness deadline, run the reconnect-reconcile
    /// against the new bindings, and surface `Connected`.
    fn on_welcome(&mut self, now: Millis, welcome: WelcomeParams) -> Vec<Effect> {
        let WelcomeParams {
            participant_id,
            heartbeat_secs,
            max_body_bytes,
            alert_granted,
            takeover_granted,
            error_report_floor,
            surface_description,
            bindings,
        } = welcome;
        // Every binding channel must carry a supported scheme. An unroutable
        // scheme is inexplicable — the backend boot-panics on such config — so it
        // is a fatal protocol error, not a tolerated binding.
        // Inputs and outputs are separate structs (an output carries a default
        // urgency an input has nothing to say about), and these checks read only
        // the address — so walk the channels, not the bindings.
        let binding_channels = bindings
            .subscriptions
            .iter()
            .map(|b| &b.channel)
            .chain(bindings.outputs.iter().map(|b| &b.channel));
        for channel in binding_channels {
            if !channel_scheme_supported(channel) {
                return self.go_fatal(format!(
                    "Welcome binding channel has an unsupported scheme: {channel}"
                ));
            }
            // A local binding's channel must appear in the router table, which is
            // the only place its ring depth can come from (local channels have no
            // `[[channel]]` block). The server resolves that table from these very
            // bindings, so a gap is inexplicable ⇒ fatal. Checked here so the
            // router can index its rings infallibly: past this point a resolved
            // local binding always has a ring.
            if is_local_channel(channel)
                && !bindings
                    .local_channels
                    .iter()
                    .any(|lc| lc.channel == *channel)
            {
                return self.go_fatal(format!(
                    "Welcome binds local channel {channel} but declares no router entry for it"
                ));
            }
        }
        // A reserved control plane's ring depth is contract-fixed, so a router
        // entry that restates it must restate it *exactly*. The server resolves
        // these entries from the same contract table this client seeds its rings
        // from (boot rejects an operator override), so a divergent depth is
        // inexplicable ⇒ fatal. Never silently honoured: the depth is the plane's
        // semantics, not a tunable — `link-state` at 0 would kill the late-attach
        // replay the plane exists for, and `toast` above 0 would resurface stale
        // events to a late chrome. The depth is a *floor* on the store, which a
        // deep push window may raise; it is never lowered, and never sourced
        // from anywhere but the contract.
        for lc in &bindings.local_channels {
            if let Some(reserved) = reserved_local_channel(&lc.channel)
                && lc.ring_depth != reserved.ring_depth
            {
                return self.go_fatal(format!(
                    "Welcome declares reserved local channel {} at ring depth {}, but the \
                     contract fixes it at {}",
                    lc.channel, lc.ring_depth, reserved.ring_depth
                ));
            }
        }
        // Every subscription's `push_depth` must be usable as a queue depth here:
        // representable as a `usize` — the wasm target's is 32-bit, so a depth
        // the server could serialize is not automatically one this page can
        // allocate against. Inexplicable ⇒ fatal, and checked once here so the
        // queue-building paths convert infallibly.
        //
        // `0` is legal and meaningful: a depth-0 binding is sampled/context-only
        // — it never activates its instance and never carries new envelopes, so
        // it has no queue to size. It is *not* legal on the condemned dialect,
        // which cannot express a context-only port; `bind_port` holds that line
        // where it belongs, on the binding's delivery model, rather than
        // forbidding the value for everyone here.
        for b in &bindings.subscriptions {
            if usize::try_from(b.push_depth).is_err() {
                return self.go_fatal(format!(
                    "Welcome binding {}/{} on {} declares an unusable push_depth: {}",
                    b.instance, b.port, b.channel, b.push_depth
                ));
            }
        }
        // Every binding's instance must appear in the instance map. The server
        // resolves both from one declaration set (boot rejects a binding naming
        // an undeclared instance), so a gap is inexplicable ⇒ fatal. Checked
        // here so `local_sender` can derive an identity infallibly: past this
        // point every resolvable binding has a declared instance, and an
        // unattributable publish is what the identity model exists to prevent.
        let binding_instances = bindings
            .subscriptions
            .iter()
            .map(|b| (&b.instance, &b.port, &b.channel))
            .chain(
                bindings
                    .outputs
                    .iter()
                    .map(|b| (&b.instance, &b.port, &b.channel)),
            );
        for (instance, port, channel) in binding_instances {
            if !bindings.components.iter().any(|c| c.instance == *instance) {
                return self.go_fatal(format!(
                    "Welcome binding {instance}/{port} on {channel} names an instance absent \
                     from the component map"
                ));
            }
        }
        // A zero heartbeat yields a zero liveness window, which would declare
        // every connection dead on the first tick and churn reconnects forever.
        // The server's value is a positive constant, so zero is inexplicable —
        // fatal, like any other unreconcilable server value.
        if heartbeat_secs == 0 {
            return self.go_fatal("Welcome heartbeat_secs is zero".to_string());
        }
        self.backoff_step = 0;
        self.liveness_ms = u64::from(self.liveness_multiplier)
            .saturating_mul(u64::from(heartbeat_secs))
            .saturating_mul(1000);
        self.state = State::Active;
        self.participant_id = participant_id.clone();
        self.bindings = Some(bindings.clone());
        // The recorded overlay was validated against the *previous* bindings.
        // A `Welcome` that no longer declares its holder has retired that
        // instance, and reporting it onward would be a `Status` frame naming an
        // unconfigured instance — which the server treats as a protocol
        // violation and kills the session over, so the page would reconnect and
        // violate again on every status tick. Dropped rather than kept: the
        // kernel cannot stand behind a holder for a component this surface no
        // longer has, and chrome republishes on its next transition. A holder
        // the new bindings still declare survives, because a reconnect is
        // exactly when a live wedge most needs reporting.
        if let Some(overlay) = &self.overlay
            && !bindings
                .components
                .iter()
                .any(|c| c.instance == overlay.holder)
        {
            self.overlay = None;
        }
        // Every deferred-view mirror goes: the backend re-seeds the nonempty ones
        // immediately behind this frame, so an unmentioned pair is an empty set.
        // Clearing is what makes that true — a set that emptied while the page was
        // away is answered by the seeding pass saying nothing about it, and a
        // retained mirror would answer with the schedule it had before the
        // disconnect.
        self.deferred_views.clear();
        // Before `reconcile_attached`, which may force-detach ports on confined
        // channels this Welcome dropped, and before any attach or delivery
        // resolves — a store must exist for every declared channel before a
        // binding can take a position in it.
        let retuned = self.reconcile_stores(&bindings);
        self.max_body_bytes = max_body_bytes;
        self.alert_granted = alert_granted;
        self.error_report_floor = error_report_floor;
        // Arm the liveness deadline in place of the now-satisfied handshake
        // deadline; any inbound text frame will push it out.
        let deadline = self.arm_liveness(now);
        let mut effects = vec![Effect::SetWakeup(Some(deadline))];
        // A depth shrink's loss, enacted before the position reconcile below: a
        // `fatal` binding trimmed past is killed here, and the reconcile then
        // treats it as the terminal instance it now is.
        effects.extend(retuned);
        // Reconnect-reconcile before a single Subscribe goes out: every registered
        // instance's queues follow the new binding table and its subscription
        // references are diffed onto it, so `resubscribe_survivors` below opens
        // exactly the set this `Welcome` authorizes — never a channel it dropped.
        // It runs before Connected, so a correct client is fully reconciled before
        // the kernel reacts.
        effects.extend(self.reconcile_registered(&bindings));
        effects.extend(self.resubscribe_survivors());
        // Parked flushes go out post-handshake, before this instance's normal
        // traffic resumes: they are older than anything the reconnected page will
        // produce, and the activations that made them already returned ok.
        effects.extend(self.send_parked_batches(&bindings, now));
        effects.push(Effect::EmitEvent(Event::Connected {
            bindings,
            participant_id,
            max_body_bytes,
            alert_granted,
            takeover_granted,
            error_report_floor,
            surface_description,
        }));
        effects
    }

    /// Reconcile every channel store against a just-received `Welcome`.
    ///
    /// Existing stores are **preserved across a reconnect**, contents, positions
    /// and seq counter intact: references are diffed rather than dropped and
    /// retaken, so no subscription's contents are scoped out by a reconnect, and
    /// the store is exactly what a post-reconnect window's context is read from.
    /// Discarding one here would manufacture a loss the link never caused. A store
    /// no surviving binding names is dropped: nothing can route on it again. A
    /// surviving one is retuned in place, which takes effect at each binding's next
    /// window.
    ///
    /// A wire store this reconcile creates may be empty for a subscription nothing
    /// currently references — its contents were discarded when its last reference
    /// went ([`Self::release_channel_ref`]), and only the contents are
    /// subscription-scoped, not the map entry.
    ///
    /// A store has one size. It is the fold over the store's bindings of
    /// `max(push_depth, retain_depth)`, floored for a confined channel by the
    /// server-resolved `ring_depth` (and for a reserved plane by its
    /// contract-fixed depth). Both halves of the per-binding max are
    /// load-bearing: `retain_depth` is what a binding reads as context, and
    /// `push_depth` is what it can be handed as new — and since the store is the
    /// only thing holding either, a store shallower than a binding's push window
    /// would silently cap its delivery. The default wire shape is exactly that
    /// case (`retain_depth` unset is 0). What the store holds under that depth is
    /// the channel's history on the page, and a position coming into existence is
    /// primed from it.
    ///
    /// Reserved `local:brenn/*` planes are never dropped: the contract declares
    /// them, so no `Welcome` declares them into existence and none can un-declare
    /// them. Ports attached to a dropped confined channel are force-detached by
    /// [`Self::reconcile_attached`], which runs off the same bindings.
    fn reconcile_stores(&mut self, bindings: &SurfaceBindings) -> Vec<Effect> {
        let mut wanted: HashMap<StoreKey, u64> = HashMap::new();
        // A declared confined channel gets a store whether or not anything binds
        // it: the router accepts publishes on it, and the server's resolution is
        // the floor under its depth.
        for lc in &bindings.local_channels {
            let depth = wanted
                .entry(StoreKey::Confined(lc.channel.clone()))
                .or_default();
            *depth = (*depth).max(lc.ring_depth);
        }
        for b in &bindings.subscriptions {
            let depth = wanted
                .entry(store_key(&b.channel, &b.instance))
                .or_default();
            *depth = (*depth).max(b.push_depth.max(b.retain_depth));
        }
        for reserved in RESERVED_LOCAL_CHANNELS {
            if let Some(depth) = wanted.get_mut(&StoreKey::Confined(reserved.address.to_string())) {
                *depth = (*depth).max(reserved.ring_depth);
            }
        }
        // A store going away takes its deferred set with it, and a dropped
        // schedule is the only account of a timer a component believes it set —
        // the same reason the quota refusal and the lost control-op race are
        // loud. An operator un-declaring a `local:` channel is as accountable a
        // cause of that loss as a full deferred set. Sorted by channel so the
        // page's telemetry does not depend on hash order.
        let mut lost: Vec<(String, Vec<String>)> = self
            .stores
            .iter()
            .filter(|(key, _)| !reserved_store(key) && !wanted.contains_key(*key))
            .filter_map(|(key, store)| match key {
                StoreKey::Confined(channel) => {
                    let senders: Vec<String> = store.parked_senders().map(str::to_string).collect();
                    (!senders.is_empty()).then(|| (channel.clone(), senders))
                }
                // A wire channel's deferral authority is the backend, so a
                // discarded mirror holds no schedule to lose.
                StoreKey::Wire(_) => None,
            })
            .collect();
        lost.sort_by(|a, b| a.0.cmp(&b.0));
        let participant_id = self.participant_id.clone();
        for (channel, senders) in lost {
            tracing::warn!(
                %channel,
                schedules = senders.len(),
                "surface client: this Welcome no longer declares the channel, dropping the \
                 schedules parked on it"
            );
            for sender in senders {
                if let Some((_, reg)) = self
                    .registered
                    .iter_mut()
                    .find(|(instance, _)| surface_sub_identity(&participant_id, instance) == sender)
                {
                    reg.deferred_dropped += 1;
                }
            }
        }
        self.stores
            .retain(|key, _| reserved_store(key) || wanted.contains_key(key));
        let epoch = self.local_epoch;
        // A shrink retires messages out from under lagging positions, which is the
        // ladder's business exactly as an arrival's eviction is. Collected here and
        // enacted below because enacting mutates the same maps this loop borrows.
        let mut retired: Vec<(String, Vec<CursorOverflow<BindingKey>>)> = Vec::new();
        for (key, depth) in wanted {
            let channel = match &key {
                StoreKey::Confined(channel) => channel.clone(),
                StoreKey::Wire(sub) => sub.channel.clone(),
            };
            let overflow = self
                .stores
                .entry(key)
                .or_insert_with(|| SurfaceChannelStore::new(epoch, depth))
                .retune(depth);
            if !overflow.is_empty() {
                retired.push((channel, overflow));
            }
        }
        let mut effects = Vec::new();
        // Stable order across stores: the map iteration above is not ordered, and a
        // page's telemetry should not depend on hash order.
        retired.sort_by(|a, b| a.0.cmp(&b.0));
        for (channel, overflow) in retired {
            effects.extend(self.enact_overflow(&channel, overflow));
        }
        effects
    }

    /// Rebuild every registered instance's delivery positions and subscription
    /// references against `bindings`.
    ///
    /// Run at every `Welcome` and at each registration — the two moments either
    /// side of the relationship can change. It is idempotent, which is what lets
    /// both call it without either knowing about the other.
    ///
    /// A position is per binding and lives in the store the binding's channel
    /// resolves to, so the binding table defines the set: a binding that vanished
    /// loses its position (nothing can deliver to a port that no longer exists),
    /// a port rebound to a different channel is removed from the old channel's
    /// store and attached fresh to the new one (its old channel's history is
    /// stale under the new binding), and a surviving one keeps its position with
    /// its push depth retuned. A dropped position takes its undelivered drop
    /// count with it: the count describes losses on a binding, and that binding
    /// is gone — carrying it across would mean claiming losses on a channel the
    /// port was never bound to. A registered instance whose bindings all vanish
    /// simply stops being activated; it is not failed and not deregistered — the
    /// operator un-wired it, which is not the component's fault.
    ///
    /// A position coming into existence is **primed** behind the retained tail,
    /// capped at `push_depth`, on both classes: attach is a delivery point, so a
    /// component that binds after a publish still receives it as new — as much of
    /// it as its push window can hold. Surviving positions are never re-primed,
    /// which is what keeps a reconcile at every `Welcome` idempotent.
    ///
    /// Depth-0 bindings hold no position — that is the mechanism of "never
    /// activates me", not an optimization — but they *do* take a subscription
    /// reference: they still see their channel.
    ///
    /// A terminal instance is attached nothing and stripped of every position it
    /// held: it will never activate again, so a position kept for it would be one
    /// every eviction charges and no window will ever serve.
    ///
    /// References are diffed rather than dropped-and-retaken. Releasing a
    /// surviving reference to zero would discard the subscription's resume token
    /// (that is what refcount zero means), so a reconnect would re-subscribe from
    /// scratch and re-replay the retained window — manufacturing exactly the
    /// duplicate delivery the store's dedup exists to prevent.
    fn reconcile_registered(&mut self, bindings: &SurfaceBindings) -> Vec<Effect> {
        let mut instances: Vec<String> = self.registered.keys().cloned().collect();
        instances.sort();
        let mut release: Vec<SubKey> = Vec::new();
        let mut acquire: Vec<SubKey> = Vec::new();
        for instance in &instances {
            let mut positions: Vec<(StoreKey, BindingKey, u64)> = Vec::new();
            let mut wanted_subs: Vec<String> = Vec::new();
            for b in bindings
                .subscriptions
                .iter()
                .filter(|b| b.instance == *instance)
            {
                let key = store_key(&b.channel, &b.instance);
                if matches!(key, StoreKey::Wire(_)) {
                    wanted_subs.push(b.channel.clone());
                }
                positions.push((key, BindingKey::new(instance, &b.port), b.push_depth));
            }
            let failed = self
                .registered
                .get(instance)
                .expect("surface client: instance from this map")
                .failed;
            for (key, store) in self.stores.iter_mut() {
                let stale: Vec<BindingKey> = store
                    .bindings()
                    .filter(|held| held.instance == *instance)
                    .filter(|held| {
                        failed
                            || !positions
                                .iter()
                                .any(|(wanted_key, wanted, _)| wanted_key == key && wanted == *held)
                    })
                    .cloned()
                    .collect();
                for binding in stale {
                    store.detach(&binding);
                }
            }
            if !failed {
                let epoch = self.local_epoch;
                for (key, binding, push_depth) in positions {
                    // A wire mirror's contents are the subscription's, discarded
                    // when its last reference went, so a subscription being
                    // referenced again starts from an empty store: the fresh
                    // `Subscribe` this reconcile is about to send is the catch-up
                    // authority, and its replay must arrive unseen. The shape is
                    // computed into a local first — the fold reads the bindings
                    // while the entry borrows the stores.
                    let store = match &key {
                        StoreKey::Wire(sub) => {
                            let depth = wire_store_depth(bindings, sub);
                            self.stores
                                .entry(key.clone())
                                .or_insert_with(|| SurfaceChannelStore::new(epoch, depth))
                        }
                        // A confined channel's store is page-lifetime, so its
                        // absence is a seeding bug rather than a lifetime.
                        StoreKey::Confined(_) => self.stores.get_mut(&key).expect(
                            "surface client: every bound confined channel has a store (reserved \
                             planes seeded at construction, the rest reconciled from this Welcome)",
                        ),
                    };
                    store.attach(binding, push_depth);
                }
            }
            let reg = self
                .registered
                .get_mut(instance)
                .expect("surface client: instance from this map");
            // Multiset diff against the references currently held: what is left
            // over is released, what was not matched is acquired, and everything
            // matched is untouched.
            let mut stale = std::mem::replace(&mut reg.subs, wanted_subs.clone());
            for channel in &wanted_subs {
                match stale.iter().position(|c| c == channel) {
                    Some(pos) => {
                        stale.remove(pos);
                    }
                    None => acquire.push(SubKey::for_instance(instance, channel)),
                }
            }
            release.extend(
                stale
                    .into_iter()
                    .map(|channel| SubKey::for_instance(instance, &channel)),
            );
        }
        let mut effects = Vec::new();
        for sub in release {
            effects.extend(self.release_channel_ref(sub));
        }
        for sub in acquire {
            effects.extend(self.acquire_channel_ref(sub));
        }
        effects
    }

    // TODO(attach-cutover): the wire-subscription plane — these refcounts, the
    // resume custody, span validation, and `resubscribe_survivors` — is
    // duplicated by `brenn_attach_client::subs::Subscriptions`, keyed by channel
    // rather than by `SubKey`. Delete it when the kernel embeds that crate.

    /// Take one reference on a wire subscription, opening it if this is the
    /// first and the connection is live.
    ///
    /// The subscribe half of `bind_port`, without the port: same `ChannelState`,
    /// same refcount, same resume choice, so a registered instance's
    /// subscription is indistinguishable from an attached port's everywhere
    /// downstream. Off `Active` it stays `Unsubscribed` and the next `Welcome`'s
    /// `resubscribe_survivors` opens it — the ordinary path.
    fn acquire_channel_ref(&mut self, sub: SubKey) -> Vec<Effect> {
        let active = self.state == State::Active;
        let cs = self.channels.entry(sub.clone()).or_insert(ChannelState {
            refcount: 0,
            wire: WireState::Unsubscribed,
            token: None,
            span_hw: None,
            has_been_active: false,
            straggler_reported: false,
        });
        cs.refcount = cs.refcount.saturating_add(1);
        match cs.wire {
            WireState::Unsubscribed if active => {
                let resume = cs.prepare_subscribe();
                vec![Effect::SendFrame(ClientFrame::Subscribe {
                    channel: sub.channel,
                    instance: sub.instance,
                    resume,
                })]
            }
            WireState::Unsubscribed | WireState::Pending | WireState::Active => Vec::new(),
        }
    }

    /// Register an instance for activation delivery.
    ///
    /// Re-registering an already-registered instance is a caller bug and panics:
    /// the second registration would silently orphan the first entry's queued
    /// messages. This is the fail-fast backstop behind the kernel's registration
    /// gate, which is what an in-page component bug actually meets.
    fn on_activation_registered(&mut self, instance: String) -> Vec<Effect> {
        assert!(
            !self.registered.contains_key(&instance),
            "surface client: activation entry registered twice for instance {instance:?}"
        );
        self.registered.insert(instance, RegisteredInstance::new());
        // Queues and subscriptions both come from the bindings, so the reconcile
        // that runs at every `Welcome` is exactly the work a registration needs —
        // it is idempotent and it is the only place that mapping lives. Before
        // the first `Welcome` there is no table yet and this is a no-op; that
        // `Welcome` will reconcile the instance in with everything else.
        match self.bindings.clone() {
            Some(bindings) => self.reconcile_registered(&bindings),
            None => Vec::new(),
        }
    }

    /// Withdraw an instance's activation entry — the mirror of `detach`.
    ///
    /// Its positions go with it (nothing will consume what they were owed), and so
    /// does every subscription reference it held — which discards those
    /// subscriptions' mirrors. A confined channel's store stays: the page is its
    /// retention authority and no subscription scopes it, so a re-register reads
    /// the same retained history a reconnect would have kept.
    ///
    /// A re-registration is therefore a fresh attach: the positions it takes are
    /// primed from each confined channel's retained tail and replayed into by the
    /// wire subscribes, so retained messages the instance already folded arrive
    /// again as new. That is wire symmetry, and the reason a component with
    /// side-effecting folds owes itself at-most-once handling by `message_id` on
    /// any class.
    ///
    /// Deregistering an unregistered instance is a caller bug and panics, exactly
    /// as detaching an unknown port is.
    fn on_activation_deregistered(&mut self, instance: &str) -> Vec<Effect> {
        let reg = self.registered.remove(instance).unwrap_or_else(|| {
            panic!("surface client: deregistration of unregistered instance {instance:?}")
        });
        for store in self.stores.values_mut() {
            store.detach_instance(instance);
        }
        // The instance's parked outbox dies with it. Those are ok'd flushes not
        // yet applied — announce the drop rather than let it vanish silently.
        if !reg.parked.is_empty() {
            let dropped: usize = reg.parked.iter().map(|b| b.entries.len()).sum();
            tracing::warn!(
                %instance,
                batches = reg.parked.len(),
                entries = dropped,
                "surface client: instance deregistered with a non-empty outbox — ok'd flushes \
                 dropped"
            );
        }
        // Release every subscription reference it held. The last reference off a
        // live channel sends `Unsubscribe`, exactly as the last port detaching
        // does — a registered instance is a subscriber like any other, and it
        // stops being one here.
        let mut effects = Vec::new();
        for channel in reg.subs {
            effects.extend(self.release_channel_ref(SubKey::for_instance(instance, &channel)));
        }
        effects
    }

    /// Take the next instance with an activation ready to run, or `None` when
    /// none is.
    ///
    /// The dispatch point. The driver drains everything immediately available
    /// (WS frames, timers, commands) into the core *first*, then calls this —
    /// which is exactly what makes the batching real: every delivery of the turn
    /// is already in its pending queue by the time the window is assembled, so
    /// they coalesce into one activation instead of N.
    ///
    /// The driver calls this a bounded number of times per turn rather than until
    /// it answers `None`. It cannot be drained to exhaustion: an instance that
    /// republishes onto a `local:` channel it reads is ready again the moment its
    /// flush routes, so `None` is a state that never arrives. See
    /// `Driver::drain_activations`.
    ///
    /// Handing one out marks the instance in flight, and nothing clears that but
    /// an [`Input::ActivationDone`], so there is no way to obtain two activations
    /// for one instance. That is the serialization guarantee, structurally.
    ///
    /// Instances are considered in a stable order (sorted by id) so a page with
    /// several ready instances dispatches deterministically rather than in
    /// `HashMap` order — the instances are independent, so any total order is
    /// correct, but a stable one keeps tests honest. The pick then *rotates*
    /// through that order (see `dispatch_cursor`): a stable order alone would
    /// starve every instance but the lowest-named one as soon as one of them
    /// re-readies itself synchronously, which a `local:` republisher does.
    ///
    /// `wall_now_ms` is the driver's wall-clock read for this assembly, epoch
    /// milliseconds UTC. One read serves both the `now` the component is handed
    /// and the boundary its deferred view is taken at, so what a component is told
    /// the time is and what it is shown as still parked cannot disagree.
    pub fn take_ready_activation(&mut self, wall_now_ms: u64) -> Option<ReadyActivation> {
        let mut ready: Vec<&String> = self
            .registered
            .iter()
            .filter(|(instance, reg)| reg.runnable() && self.owed_anything(instance))
            .map(|(instance, _)| instance)
            .collect();
        ready.sort();
        // Resume after the last dispatch, wrapping to the front. With no cursor
        // (or a cursor past the end of the current ready set) this is the plain
        // lowest-named pick.
        let start = match &self.dispatch_cursor {
            Some(last) => ready.partition_point(|i| *i <= last),
            None => 0,
        };
        let instance = ready
            .get(start)
            .or_else(|| ready.first())
            .map(|i| (*i).clone())?;
        self.dispatch_cursor = Some(instance.clone());
        Some(self.dispatch_activation(instance, wall_now_ms))
    }

    /// Whether any instance could be dispatched right now — [`Self::
    /// take_ready_activation`] without the dispatch.
    ///
    /// The driver's select loop asks this so a component that re-readies itself
    /// synchronously (a `local:` publish onto a channel it reads) is dispatched
    /// from the loop, one activation per turn, instead of inside an unbounded
    /// drain that would never return to the transport.
    pub fn has_ready_activation(&self) -> bool {
        self.registered
            .iter()
            .any(|(instance, reg)| reg.runnable() && self.owed_anything(instance))
    }

    /// Whether any of `instance`'s input bindings is owed a message its channel
    /// still holds — the wake question, asked of the stores rather than of a
    /// queue of copies.
    ///
    /// Coalescing falls out of this: arrivals move no position, so an instance
    /// woken once for three arrivals is woken once, and the window it is
    /// assembled from serves the newest.
    fn owed_anything(&self, instance: &str) -> bool {
        self.stores
            .values()
            .any(|store| store.has_deliverable_for_instance(instance))
    }

    /// How many instances hold a registered activation entry. The driver's
    /// per-turn dispatch budget: one pass over the ready set, then back to the
    /// select loop.
    pub fn registered_count(&self) -> usize {
        self.registered.len()
    }

    /// Lifetime `metered`-rung drop count for one instance's input binding, keyed
    /// by port. Zero for a port that has never dropped or whose binding resolves
    /// to `Silent` (uncounted). Kernel-internal observability for the loudness
    /// ladder; distinct from `InstanceCounters.drops`.
    pub fn metered_drop_count(&self, instance: &str, port: &str) -> u64 {
        self.registered
            .get(instance)
            .and_then(|reg| reg.metered_drops.get(port))
            .copied()
            .unwrap_or(0)
    }

    /// Lifetime count of this instance's deferred publishes whose schedule was
    /// dropped because the target channel's deferred set was full. Zero for an
    /// instance that never over-scheduled, and for one the core does not hold.
    pub fn deferred_drop_count(&self, instance: &str) -> u64 {
        self.registered
            .get(instance)
            .map_or(0, |reg| reg.deferred_dropped)
    }

    /// Lifetime count of this instance's control ops that found their message
    /// already released — the benign race, not a failure. Zero for an instance the
    /// core does not hold.
    pub fn deferred_race_count(&self, instance: &str) -> u64 {
        self.registered
            .get(instance)
            .map_or(0, |reg| reg.deferred_races)
    }

    /// Whether an instance is terminal (its activation entry trapped, or a
    /// `fatal`-rung binding overflowed). The driver consults this after a
    /// ready activation's loud-rung effects run: a killed instance is not
    /// invoked, and its assembled buffer is discarded.
    pub fn is_failed(&self, instance: &str) -> bool {
        self.registered.get(instance).is_some_and(|reg| reg.failed)
    }

    /// Assemble one activation for a ready instance: window, advance, seed.
    fn dispatch_activation(&mut self, instance: String, wall_now_ms: u64) -> ReadyActivation {
        // Only this instance's input bindings, and of those only what a window
        // needs. Lifting them out first is what lets the loop below borrow the
        // stores; cloning the whole table instead would make every activation pay
        // for every sibling component's config.
        let inputs: Vec<(String, String, u64, u64, NoiseLevel)> = self
            .bindings
            .as_ref()
            .expect("surface client: a ready activation implies bindings")
            .subscriptions
            .iter()
            .filter(|b| b.instance == instance)
            .map(|b| {
                (
                    b.port.clone(),
                    b.channel.clone(),
                    b.push_depth,
                    b.retain_depth,
                    b.noise,
                )
            })
            .collect();
        self.registered
            .get_mut(&instance)
            .expect("surface client: dispatch of an unregistered instance")
            .in_flight = true;
        // Window every bound input port, in config order, present or not: a port
        // with nothing new is a pure-context window, and a component must be able
        // to read every port's view on every activation.
        //
        // Each window's cursor advances **before the entry runs**. That is backend
        // parity and it is what makes err/trap consume: what the activation saw is
        // behind the binding's position whatever the entry does with it, and
        // retention is its only recovery. A window that served nothing, and a
        // sampled binding (which holds no position), advance nothing.
        let mut ports = Vec::new();
        // Drop charges, collected and enacted after the loop: the ladder's metered
        // counter and its fatal kill both mutate `self.registered`, and the fatal
        // rung must fire once for the activation rather than once per port.
        let mut charges: Vec<DropCharge> = Vec::new();
        for (port, channel, push_depth, retain_depth, noise) in inputs {
            let binding = BindingKey::new(&instance, &port);
            // Both `expect`s are invariants of the reconcile, which is the only
            // thing that creates stores and positions and runs at both moments
            // either side of the relationship: a runnable registered instance
            // holds a position in an existing store for every binding the table
            // gives it. Answering a broken one with an empty window would leave a
            // component silently starved of a port it is bound to.
            let store = self
                .stores
                .get_mut(&store_key(&channel, &instance))
                .expect("surface client: a bound channel has a store at dispatch");
            let window = store.window(&binding, push_depth, retain_depth).expect(
                "surface client: a push-enabled binding of a runnable instance holds a position",
            );
            let advance = window
                .advance_span()
                .and_then(|(through, seen_floor)| store.advance(&binding, through, seen_floor));
            // Drained with the advance whose figures it joins, so one activation
            // reports each loss exactly once. The two are disjoint: server-side
            // loss never reached this store, so no cursor subtraction can see it.
            let from_server = store.take_server_drops(&binding);
            let new_from = u32::try_from(window.new_from)
                .expect("surface client: a window's depth is a config-bounded page-memory value");
            let envelopes: Vec<MessageEnvelope> = window
                .entries
                .into_iter()
                .map(|entry| entry.message)
                .collect();
            let dropped = advance.map_or(0, |a| a.dropped) + from_server;
            // Counted here: only the still-retained part, since a span the
            // retirement that retired it already charged must not be enacted
            // twice. Announced here: the whole delta this window reports, which is
            // the coalesced figure — every earlier retirement charge deferred its
            // announcement to exactly this moment.
            let charge = DropCharge {
                port: port.clone(),
                channel,
                noise,
                counted: advance.map_or(0, |a| a.noise_charge) + from_server,
                announced: dropped,
            };
            if charge.counted > 0 || charge.announced > 0 {
                charges.push(charge);
            }
            ports.push(PortWindow {
                port,
                envelopes,
                new_from,
                dropped,
            });
        }
        let loud_effects = self.enact_drop_charges(&instance, charges);
        let DeferredSnapshot {
            windows: deferred,
            ids: deferred_ids,
        } = self.deferred_windows(&instance, wall_now_ms);
        // Seed the publish buffer: the entry gets inline quota answers without the
        // driver re-entering the core mid-handler, and the identities behind the
        // deferred windows it was just handed, so a control op resolves against the
        // very window the component read.
        let buffer = self.seed_buffer(&instance, &ports, deferred_ids);
        ReadyActivation {
            instance,
            activation: Activation {
                ports,
                deferred,
                // The wall clock the driver read for this assembly. A component
                // gets time only here — an activation stays hermetic, and a live
                // clock call inside one would make it neither reproducible nor
                // short-lived.
                now: Some(wall_now_ms),
            },
            buffer,
            effects: loud_effects,
        }
    }

    /// One deferred window per bound output port, in config order: what this
    /// instance itself has parked on each output channel, soonest release first —
    /// and, beside it, the identity of each entry the window presents.
    ///
    /// Scoped to the instance's own sender identity — the same string the router
    /// stamps on its confined publishes — so a channel two components park on
    /// still shows each of them only its own schedule.
    ///
    /// Every bound output port appears, empty or not, so an index into the window
    /// means the same thing on every activation.
    ///
    /// The identities never reach the component: an index is what it names a parked
    /// message by, and the identity is what the kernel resolves that index to while
    /// the two are still known to agree. Built here, from this one read, for exactly
    /// that reason — a second read could have released an entry and shifted every
    /// index after it.
    fn deferred_windows(&self, instance: &str, wall_now_ms: u64) -> DeferredSnapshot {
        let Some(bindings) = self.bindings.as_ref() else {
            return DeferredSnapshot::default();
        };
        let outputs: Vec<(String, String)> = bindings
            .outputs
            .iter()
            .filter(|b| b.instance == instance)
            .map(|b| (b.port.clone(), b.channel.clone()))
            .collect();
        if outputs.is_empty() {
            return DeferredSnapshot::default();
        }
        let sender = self.local_sender(instance);
        let mut snapshot = DeferredSnapshot::default();
        for (port, channel) in outputs {
            let (entries, ids): (Vec<DeferredEntry>, Vec<Uuid>) =
                if channel_is_transportable(&channel) {
                    // The backend is this channel's deferral authority, so the
                    // window is the snapshot it last pushed, already release-
                    // ordered and already scoped to this sender. A pair with no
                    // mirror is an empty set — that is what the clearance at
                    // `Welcome` plus seed-only-if-nonempty means.
                    self.deferred_views
                        .get(&(channel.clone(), instance.to_string()))
                        .map_or_else(
                            || (Vec::new(), Vec::new()),
                            |view| {
                                view.iter()
                                    .enumerate()
                                    .map(|(index, entry)| {
                                        (
                                            DeferredEntry {
                                                index: u32::try_from(index).expect(
                                                    "surface client: a deferred view is bounded \
                                                     by the channel's depth",
                                                ),
                                                payload: entry.body.clone(),
                                                deliver_after: entry.deliver_after,
                                            },
                                            entry.message_id,
                                        )
                                    })
                                    .unzip()
                            },
                        )
                } else {
                    self.stores
                        .get(&StoreKey::Confined(channel))
                        .expect(
                            "surface client: every routable confined channel has a store \
                             (reserved planes seeded at construction, the rest proven by \
                             on_welcome)",
                        )
                        .deferred_for_sender(&sender, wall_now_ms)
                        .enumerate()
                        .map(|(index, parked)| {
                            (
                                DeferredEntry {
                                    index: u32::try_from(index).expect(
                                        "surface client: a deferred set is bounded by its \
                                         channel's depth",
                                    ),
                                    // The body, not the envelope: what a component gets
                                    // back is what it handed the host, on every hosting.
                                    payload: parked.message.body.clone(),
                                    deliver_after: parked.release_at,
                                },
                                parked.message.message_id,
                            )
                        })
                        .unzip()
                };
            snapshot.ids.insert(port.clone(), ids);
            snapshot.windows.push(DeferredWindow { port, entries });
        }
        snapshot
    }

    /// Enact the loudness ladder for a set of per-binding drop charges.
    ///
    /// Rungs are cumulative: `Silent` does nothing beyond the honest `dropped`
    /// figure the window already carries, `metered` counts per binding, `alarm`
    /// adds one coalesced alert + toast per binding, and `fatal` kills the
    /// instance. Noise governs loudness only — it never changes what happens to
    /// the data, which is always drop-oldest.
    ///
    /// The alert and toast are emitted for [`DropCharge::announced`] and the
    /// counter takes [`DropCharge::counted`]: a charge raised where the loss
    /// happened counts there and announces at the binding's next window, which is
    /// what keeps one alert per binding per activation however many messages the
    /// binding lagged by.
    ///
    /// The kill fires once for the whole set, naming the first `fatal` binding:
    /// an instance dies once however many of its bindings overflowed together.
    fn enact_drop_charges(&mut self, instance: &str, charges: Vec<DropCharge>) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut fatal: Option<(String, String, u64)> = None;
        for DropCharge {
            port,
            channel,
            noise,
            counted,
            announced,
        } in charges
        {
            if noise >= NoiseLevel::Metered
                && counted > 0
                && let Some(reg) = self.registered.get_mut(instance)
            {
                *reg.metered_drops.entry(port.clone()).or_insert(0) += counted;
            }
            if noise >= NoiseLevel::Alarm && announced > 0 {
                effects.extend(loud_drop_effects(instance, &channel, &port, announced));
            }
            if noise >= NoiseLevel::Fatal && counted > 0 && fatal.is_none() {
                fatal = Some((port, channel, counted));
            }
        }
        if let Some((port, channel, dropped)) = fatal {
            effects.extend(self.fail_instance(
                instance,
                format!(
                    "input overflow: {dropped} message(s) dropped on port {port:?} ({channel}) — \
                     binding noise is fatal"
                ),
            ));
        }
        effects
    }

    /// Take an instance terminal: stop delivering to it, drop the positions and
    /// the ok'd flushes nobody is left to answer for, and surface the failure.
    ///
    /// Its channels' stores are untouched: a channel does not stop retaining
    /// because one of its readers died, and the remaining readers are owed
    /// exactly what they were before. An instance already terminal is left alone
    /// and reported once.
    fn fail_instance(&mut self, instance: &str, reason: String) -> Vec<Effect> {
        let reg = self
            .registered
            .get_mut(instance)
            .expect("surface client: failing an unregistered instance");
        if reg.failed {
            return Vec::new();
        }
        reg.in_flight = false;
        reg.failed = true;
        // Its parked flushes die with it: they were produced by a component whose
        // memory is now presumed poisoned, and there is nobody left to answer for
        // them.
        reg.parked.clear();
        for store in self.stores.values_mut() {
            store.detach_instance(instance);
        }
        vec![Effect::EmitEvent(Event::InstanceFailed {
            instance: instance.to_string(),
            reason,
        })]
    }

    /// Seed one activation's publish buffer: the instance's outputs, their sink
    /// buckets, and the body cap.
    ///
    /// Buckets are `seed_sink_budget(carry, budget, grant)` — the backend's own
    /// arithmetic from the same crate, so a component's budget means the same
    /// thing on either hosting. The grant is the input amplification at the
    /// uniform v1 default (`MILLITOKENS_PER_PUBLISH` per **new** envelope, never
    /// context): a component that republishes what it consumes stays solvent at
    /// 1:1 without an operator raising a knob. No per-cause amplification
    /// vocabulary is invented — reserve, don't fake.
    fn seed_buffer(
        &self,
        instance: &str,
        ports: &[PortWindow],
        deferred_ids: HashMap<String, Vec<Uuid>>,
    ) -> PublishBuffer {
        let bindings = self
            .bindings
            .as_ref()
            .expect("surface client: a ready activation implies bindings");
        let grant = brenn_budget::grant_input_mt(ports.iter().map(|w| {
            let new_count = w.envelopes.len() as u64 - u64::from(w.new_from);
            (brenn_budget::MILLITOKENS_PER_PUBLISH, new_count)
        }));
        let carry = &self
            .registered
            .get(instance)
            .expect("surface client: seeding a buffer for an unregistered instance")
            .carry_mt;
        let mut outputs = HashMap::new();
        let mut sink_mt = HashMap::new();
        for b in bindings.outputs.iter().filter(|b| b.instance == instance) {
            outputs.insert(
                b.port.clone(),
                OutputSpec {
                    channel: b.channel.clone(),
                    default_urgency: b.urgency,
                },
            );
            sink_mt.insert(
                b.port.clone(),
                brenn_budget::seed_sink_budget(
                    carry.get(&b.port).copied().unwrap_or(0),
                    brenn_budget::SinkBudget {
                        fill_mt: b.fill_mt,
                        capacity_mt: b.capacity_mt,
                    },
                    grant,
                ),
            );
        }
        PublishBuffer::new(outputs, sink_mt, self.max_body_bytes, deferred_ids)
    }

    /// An activation entry returned: flush or discard, and clear `in_flight`.
    fn on_activation_done(
        &mut self,
        instance: String,
        outcome: ActivationOutcome,
        buffer: PublishBuffer,
        stamps: Vec<MessageStamp>,
    ) -> Vec<Effect> {
        // A completion for an instance that deregistered mid-flight (fixture
        // teardown) has nowhere to land: the entry is gone, so its publishes have
        // no principal to attribute and no budget to return to. Absorbed rather
        // than panicked — the driver holds the entry and the core cannot order
        // the two events.
        if !self.registered.contains_key(&instance) {
            return Vec::new();
        }
        match outcome {
            ActivationOutcome::Ok => {
                let flushed = buffer.take();
                let reg = self.registered.get_mut(&instance).unwrap();
                reg.in_flight = false;
                reg.carry_mt = flushed.carry;
                // Each op goes to its channel's deferral authority: a confined
                // channel's set is here, a transportable channel's is at the
                // backend, so those ops ride the flush's batch frame. Both halves
                // apply ahead of this activation's publishes — an op acts on a
                // message an earlier activation parked, and applying it first
                // keeps the two from interleaving.
                let (wire_ops, local_ops): (Vec<_>, Vec<_>) = flushed
                    .defer_ops
                    .into_iter()
                    .partition(|op| channel_is_transportable(&op.channel));
                let mut effects = self.apply_defer_ops(&instance, local_ops);
                effects.extend(self.flush(&instance, flushed.publishes, stamps, wire_ops));
                effects
            }
            ActivationOutcome::Err(err) => {
                let carry = buffer.into_carry();
                let reg = self.registered.get_mut(&instance).unwrap();
                reg.in_flight = false;
                // Carryover returns even though the entries do not: what the
                // component spent is a fact about the activation that ran, and an
                // err does not un-spend it.
                reg.carry_mt = carry;
                reg.activation_failures += 1;
                vec![Effect::EmitEvent(Event::ActivationFailed {
                    instance,
                    message: err.message,
                })]
            }
            ActivationOutcome::Trap(reason) => self.fail_instance(&instance, reason),
        }
    }

    /// Apply one ok activation's control ops against **confined** channels to its
    /// own parked messages. A transportable channel's ops are the flush's
    /// business, not this function's: they ride the batch frame.
    ///
    /// Each op names its message by the identity the component's own deferred
    /// window carried, resolved when the component made the call. So the only thing
    /// that can have changed by now is whether that message is still parked:
    ///
    /// - **Still parked** — cancelled or edited. An edit to a release time already
    ///   past does not publish here; the release pass the caller's input re-arms
    ///   takes it, which is the same answer a deferred publish in the past gets.
    /// - **Gone** — the benign drain-vs-release race the contract names explicitly.
    ///   Logged and counted, never an error: the component had already returned by
    ///   the time the race was resolvable, and a conforming component can always
    ///   lose it.
    /// - **Someone else's** — a panic. The identity came from a view scoped to this
    ///   instance's own sender, so a cross-sender hit is the page having built that
    ///   view wrong, not anything a component did.
    ///
    /// An edit's replacement body runs the reserved planes' guard
    /// ([`Self::guard_local_body`]) before it is written, exactly as a publish's
    /// body does: an edit is how a component states a new body on a confined
    /// channel, so a guard it skipped would police only half the ways onto the
    /// plane. A refused edit changes nothing and reports the violation.
    ///
    /// Nothing else is emitted: a schedule changing wakes nobody. Only a release
    /// does, and that is the timer's business.
    fn apply_defer_ops(&mut self, instance: &str, ops: Vec<BufferedDeferOp>) -> Vec<Effect> {
        if ops.is_empty() {
            return Vec::new();
        }
        let mut effects = Vec::new();
        let sender = self.local_sender(instance);
        for BufferedDeferOp {
            port,
            channel,
            message_id,
            kind: op,
        } in ops
        {
            debug_assert!(
                !channel_is_transportable(&channel),
                "surface client: a transportable channel's op belongs on the batch frame"
            );
            let op = match op {
                ClientDeferOp::Edit {
                    body: Some(body),
                    deliver_after,
                } => match self.guard_local_body(&channel, LocalOrigin::Instance(instance), body) {
                    GuardedBody::Carry(body) => ClientDeferOp::Edit {
                        body: Some(body),
                        deliver_after,
                    },
                    GuardedBody::Refused(effect) => {
                        effects.push(effect);
                        continue;
                    }
                },
                op => op,
            };
            let op = match op {
                ClientDeferOp::Cancel => DeferOp::Cancel,
                ClientDeferOp::Edit {
                    body,
                    deliver_after,
                } => DeferOp::Edit {
                    body,
                    deliver_after,
                },
            };
            // The op carries an identity from a confined channel's own deferred
            // set, so that store existed when the component read it and a flush
            // cannot outrun a `Welcome`: the driver holds one activation at a time
            // and feeds its completion before any other input.
            let outcome = self
                .stores
                .get_mut(&StoreKey::Confined(channel.clone()))
                .expect("surface client: a deferred identity implies its channel's store")
                .apply_defer_op(&sender, message_id, op);
            match outcome {
                DeferOpOutcome::Applied => {}
                DeferOpOutcome::NotParked => {
                    tracing::info!(
                        instance,
                        %port,
                        %channel,
                        "surface client: deferred control op is a no-op — the message released \
                         between the activation's snapshot and the flush"
                    );
                    if let Some(reg) = self.registered.get_mut(instance) {
                        reg.deferred_races += 1;
                    }
                }
                DeferOpOutcome::WrongSender { owner } => panic!(
                    "surface client: {instance} named message {message_id} on {channel}, parked \
                     by {owner} — the deferred window this page built for {sender} carried an id \
                     it does not own"
                ),
            }
        }
        effects
    }

    /// Flush one ok activation's buffer: `local:` entries through the router,
    /// wire entries and `wire_ops` as one `PublishBatch`.
    ///
    /// Both commit at this one point. Call order is preserved **within** each
    /// class — the router routes its entries in order, the frame carries its
    /// entries in order — but the two classes commit in different places (one in
    /// this page, one in the server), so their relative order is not guaranteed.
    /// That is contract text, not an implementation artifact.
    ///
    /// `wire_ops` are the flush's control ops against transportable channels, in
    /// call order. They travel with the entries because the server applies one
    /// batch's ops and publishes together, ops first — and because a flush that
    /// carries only ops is still a flush the server has to answer.
    fn flush(
        &mut self,
        instance: &str,
        entries: Vec<BufferedPublish>,
        stamps: Vec<MessageStamp>,
        wire_ops: Vec<BufferedDeferOp>,
    ) -> Vec<Effect> {
        assert_eq!(
            entries.len(),
            stamps.len(),
            "surface client: the driver stamps one envelope per buffered publish"
        );
        let mut effects = Vec::new();
        let mut wire: Vec<BatchEntry> = Vec::new();
        for (entry, stamp) in entries.into_iter().zip(stamps) {
            if !channel_is_transportable(&entry.channel) {
                // The router commits it here and now: seq assigned, store fed,
                // fan-out — synchronously, in call order, with no await between,
                // which is the single-router property `local:` rests on. It never
                // touches the wire, so a down link is no reason to delay it.
                //
                // A plane guard's refusal produces its violation report and no
                // envelope; there is no publisher to answer here, because a
                // buffered publish got its synchronous answer at buffer time.
                effects.extend(
                    self.mint_and_route_local(
                        &entry.channel,
                        LocalOrigin::Instance(instance),
                        entry.body,
                        stamp,
                        entry.urgency,
                        entry.deliver_after,
                    )
                    .into_effects(),
                );
            } else {
                // The stamp is discarded, exactly as it is for a single wire
                // publish: the server mints the authoritative envelope. The raw
                // override rides the frame, not the resolved urgency — the
                // server holds the port's default and applies it, and echoing
                // back a possibly-stale advertised default would let the client
                // override the operator. The release time rides it verbatim for
                // the same reason: this channel's deferral authority is the
                // backend, so the page states the time and the server decides
                // park-vs-immediate against its own clock.
                wire.push(BatchEntry {
                    port: entry.port,
                    body: entry.body,
                    urgency: entry.urgency_override,
                    deliver_after: entry.deliver_after,
                });
            }
        }
        let ops: Vec<BatchDeferredOp> = wire_ops
            .into_iter()
            .map(|op| BatchDeferredOp {
                port: op.port,
                message_id: op.message_id,
                op: match op.kind {
                    ClientDeferOp::Cancel => DeferredOpKind::Cancel,
                    ClientDeferOp::Edit {
                        body,
                        deliver_after,
                    } => DeferredOpKind::Edit {
                        body,
                        deliver_after,
                    },
                },
            })
            .collect();
        if !wire.is_empty() || !ops.is_empty() {
            effects.extend(self.send_or_park(instance, wire, ops));
        }
        effects
    }

    /// Offer one activation's wire entries to the instance's outbox.
    ///
    /// Sent straight out only when the wire is free for this instance: the link
    /// is up, nothing of its own is queued, and none of its own flushes is
    /// unanswered. Otherwise it queues, and the outbox drains in order.
    ///
    /// Queueing is not an error path. The activation already returned ok, so the
    /// kernel's guarantee is "flushed, not discarded" — up to a stated bound.
    /// Activations continue while disconnected (`local:` delivery and timers need
    /// no WS), so the outbox is a queue like every other and takes the same
    /// overflow model: bounded per instance, drop-oldest at the cap, counted.
    ///
    /// The batch drops **whole**. It is the unit the server applies in one
    /// transaction, so half of one is not a smaller version of it — it is a
    /// different, wrong thing.
    fn send_or_park(
        &mut self,
        instance: &str,
        entries: Vec<BatchEntry>,
        ops: Vec<BatchDeferredOp>,
    ) -> Vec<Effect> {
        if self.wire_free_for(instance) {
            return vec![self.batch_frame(instance, entries, ops)];
        }
        self.park_batch(instance, ParkedBatch { entries, ops }, false)
    }

    /// Whether a flush for this instance may go straight to the wire.
    fn wire_free_for(&self, instance: &str) -> bool {
        if self.state != State::Active {
            return false;
        }
        let reg = self
            .registered
            .get(instance)
            .expect("surface client: a flush implies a registered instance");
        reg.parked.is_empty() && reg.batch_in_flight.is_none()
    }

    /// Put a batch in the instance's outbox — at the back for a new flush, at the
    /// head for a refused one being retried — and enforce the cap.
    ///
    /// Overflow is drop-oldest, whole, counted, toasted, exactly as it was when
    /// the queue only ever held disconnect-parked flushes. A refused head
    /// re-parked into a full outbox is therefore itself the drop: it *is* the
    /// oldest, and a queue at its cap with a head the server keeps refusing is
    /// the mis-provisioned-refill failure mode converging where the design says
    /// it converges — to counted, announced drops rather than unbounded memory or
    /// silent discard.
    fn park_batch(&mut self, instance: &str, batch: ParkedBatch, at_head: bool) -> Vec<Effect> {
        let depth = self.parked_batch_depth(instance);
        let reg = self
            .registered
            .get_mut(instance)
            .expect("surface client: parking a flush for an unregistered instance");
        if at_head {
            reg.parked.push_front(batch);
        } else {
            reg.parked.push_back(batch);
        }
        let mut effects = Vec::new();
        while reg.parked.len() as u64 > depth {
            reg.parked.pop_front();
            reg.parked_dropped += 1;
            effects.push(Self::parked_drop_toast(instance));
        }
        effects
    }

    /// Send this instance's outbox head if the wire is free for it. The one place
    /// a queued flush leaves the page.
    fn pump_outbox(&mut self, instance: &str) -> Vec<Effect> {
        if self.state != State::Active {
            return Vec::new();
        }
        let reg = self
            .registered
            .get_mut(instance)
            .expect("surface client: pumping an unregistered instance");
        if reg.batch_in_flight.is_some() {
            return Vec::new();
        }
        let Some(batch) = reg.parked.pop_front() else {
            return Vec::new();
        };
        vec![self.batch_frame(instance, batch.entries, batch.ops)]
    }

    /// Arm or disarm the retry timer from the outbox state.
    ///
    /// Armed exactly while some instance has a queued flush and the link is up —
    /// the only state a retry can act on. Disarmed otherwise, so an idle page
    /// with empty outboxes has no timer at all, and a disconnected page waits for
    /// `Welcome` rather than ticking against a dead socket.
    ///
    /// Emitted only on the unblocked↔blocked transition. An already-armed timer
    /// is never moved forward here: re-arming on every input would let unrelated
    /// activity — a sibling instance's steady stream of `Ok` results — push a
    /// parked head's deadline out indefinitely, starving the retry. Re-arming an
    /// already-fired timer is `on_retry_tick`'s job, the one place that knows the
    /// deadline was just consumed.
    fn retry_wakeup(&mut self, now: Millis) -> Vec<Effect> {
        let blocked = self.outbox_blocked();
        if blocked == self.retry_armed {
            return Vec::new();
        }
        self.retry_armed = blocked;
        if blocked {
            vec![Effect::SetRetryWakeup(Some(
                now.saturating_add_ms(RETRY_INTERVAL_MS),
            ))]
        } else {
            vec![Effect::SetRetryWakeup(None)]
        }
    }

    /// Some instance has a queued flush and the link is up — the only state a
    /// retry can act on.
    fn outbox_blocked(&self) -> bool {
        self.state == State::Active && self.registered.values().any(|reg| !reg.parked.is_empty())
    }

    /// Disarm the retry timer on the way out of `Active`, if it was armed.
    fn disarm_retry(&mut self) -> Vec<Effect> {
        if !self.retry_armed {
            return Vec::new();
        }
        self.retry_armed = false;
        vec![Effect::SetRetryWakeup(None)]
    }

    /// The retry timer fired: offer every blocked instance's head once more.
    ///
    /// One head per instance per tick — the head is the oldest un-applied flush,
    /// and anything behind it must not overtake it. Instances are independent, so
    /// a starved one never blocks a sibling.
    fn on_retry_tick(&mut self, now: Millis) -> Vec<Effect> {
        let mut instances: Vec<String> = self.registered.keys().cloned().collect();
        instances.sort();
        let mut effects = Vec::new();
        for instance in instances {
            effects.extend(self.pump_outbox(&instance));
        }
        // The firing consumed the timer. Re-arm directly while still blocked
        // (`retry_wakeup` treats an already-armed timer as a no-op and would not
        // re-arm it); disarm through it once the outbox drains.
        if self.outbox_blocked() {
            self.retry_armed = true;
            effects.push(Effect::SetRetryWakeup(Some(
                now.saturating_add_ms(RETRY_INTERVAL_MS),
            )));
        } else {
            effects.extend(self.retry_wakeup(now));
        }
        effects
    }

    /// The instance's resolved `parked_batch_depth` from `Welcome`. Boot proves
    /// it bounded and `>= 1`, and `on_welcome` proves every binding's instance is
    /// in the component map, so a registered instance always has one.
    fn parked_batch_depth(&self, instance: &str) -> u64 {
        self.bindings
            .as_ref()
            .expect("surface client: a flush implies bindings")
            .components
            .iter()
            .find(|c| c.instance == instance)
            .map(|c| c.parked_batch_depth)
            .unwrap_or_else(|| {
                panic!("surface client: no component entry for registered instance {instance:?}")
            })
    }

    /// Announce a dropped parked batch on the toast plane.
    ///
    /// The toast plane, not a backend alert: this can only happen with the link
    /// down (a parked batch exists only while disconnected), and queueing an
    /// alert against a dead link would be a message nobody will read written to a
    /// socket that is gone. The plane works offline, and the per-instance counter
    /// carries the evidence to reconnect for anyone who wants the total.
    fn parked_drop_toast(instance: &str) -> Effect {
        Effect::PublishControl {
            channel: LOCAL_TOAST_CHANNEL.to_string(),
            body: serde_json::to_string(&ToastBody {
                v: CONTROL_PLANE_VERSION,
                severity: ToastSeverity::Warning,
                text: format!(
                    "{instance}: a queued publish batch was dropped — the surface has been \
                     offline too long"
                ),
                source: ToastSource::Kernel,
            })
            .expect("surface client: a toast body serializes"),
        }
    }

    /// Compose one `PublishBatch` frame and record it as outstanding — both in
    /// the correlation table (which answers "whose result is this?") and on the
    /// instance (which answers "is this instance's wire free?").
    fn batch_frame(
        &mut self,
        instance: &str,
        entries: Vec<BatchEntry>,
        ops: Vec<BatchDeferredOp>,
    ) -> Effect {
        let correlation = self.next_batch_correlation;
        self.next_batch_correlation += 1;
        let reg = self
            .registered
            .get_mut(instance)
            .expect("surface client: sending a flush for an unregistered instance");
        assert!(
            reg.batch_in_flight.is_none(),
            "surface client: {instance} already has a flush on the wire"
        );
        reg.batch_in_flight = Some(correlation);
        self.pending_batches.insert(
            correlation,
            PendingBatch {
                instance: instance.to_string(),
                entries: entries.clone(),
                ops: ops.clone(),
            },
        );
        Effect::SendFrame(ClientFrame::PublishBatch {
            instance: instance.to_string(),
            correlation,
            publishes: entries,
            deferred_ops: ops,
        })
    }

    /// Re-validate every instance's outbox against the new `Welcome` and start it
    /// draining, oldest first.
    ///
    /// A parked batch was validated against the *previous* connection's contract,
    /// and a reconnect can hand the page a different one. Every gate the server
    /// answers with a violation is therefore re-checked here against the new
    /// `Welcome`, and a batch that would fail one is dropped whole — counted and
    /// toasted like a cap drop:
    ///
    /// - **A port the new bindings no longer carry**, which the server sees as an
    ///   unbound port.
    /// - **A body over the new `max_body_bytes`**, which an operator can lower on
    ///   a restart with no build change and so no forced reload.
    ///
    /// Either would take a protocol death — and feed fail2ban — for honestly
    /// replaying what the page buffered under the contract in force when it
    /// buffered it. Batches that clear both gates stay queued. Both gates apply to
    /// the batch's control ops as well: an op names a port, and an edit carries a
    /// body.
    ///
    /// A held op's `message_id` needs no re-validation. It came from a view scoped
    /// to this instance's own sender, and the sender identity does not depend on
    /// the connection — so across a reconnect the id either still names that
    /// sender's parked message or names one that released, which is the benign race
    /// the server logs and counts.
    ///
    /// Only the surviving head goes out here: the outbox is ordered and carries
    /// at most one flush per instance on the wire, so the rest leave as each
    /// result comes back.
    fn send_parked_batches(&mut self, bindings: &SurfaceBindings, now: Millis) -> Vec<Effect> {
        let mut instances: Vec<String> = self.registered.keys().cloned().collect();
        instances.sort();
        // The *new* connection's cap, already stamped from this `Welcome` by the
        // time parked batches are replayed — which is the whole point of checking
        // it here rather than trusting the check the batch passed when it was
        // buffered.
        let max_body_bytes = self.max_body_bytes;
        let mut effects = Vec::new();
        for instance in instances {
            let parked: Vec<ParkedBatch> = self
                .registered
                .get_mut(&instance)
                .expect("surface client: instance from this map")
                .parked
                .drain(..)
                .collect();
            for batch in parked {
                // The port set and the body sizes are read off the entries at
                // check time rather than carried alongside them: both are derived
                // from what the batch names, and a stored copy is a second truth
                // to keep in step.
                let bound = |port: &str| {
                    bindings
                        .outputs
                        .iter()
                        .any(|b| b.instance == instance && b.port == port)
                };
                let survives =
                    batch.entries.iter().all(|entry| {
                        bound(&entry.port) && entry.body.len() as u64 <= max_body_bytes
                    }) && batch.ops.iter().all(|op| {
                        bound(&op.port)
                            && match &op.op {
                                DeferredOpKind::Edit {
                                    body: Some(body), ..
                                } => body.len() as u64 <= max_body_bytes,
                                _ => true,
                            }
                    });
                let reg = self
                    .registered
                    .get_mut(&instance)
                    .expect("surface client: instance from this map");
                if survives {
                    reg.parked.push_back(batch);
                } else {
                    reg.parked_dropped += 1;
                    effects.push(Self::parked_drop_toast(&instance));
                }
            }
            effects.extend(self.pump_outbox(&instance));
        }
        effects.extend(self.retry_wakeup(now));
        effects
    }

    /// Route a publish on a `local:` channel: mint the envelope, assign the
    /// position, retain, and fan out — the page-local twin of what the server's
    /// store does for `ephemeral:`, and the reason a `local:` publish never
    /// touches the wire.
    ///
    /// Seq assignment, store append, and fan-out are one synchronous step with no
    /// await between them, so the store and the delivered order can never diverge
    /// — the single-router property that buys `local:` its freedom from the echo
    /// and dual-position problems.
    ///
    /// The publish succeeds except where a plane guard refuses the body: there is
    /// no server to reject it, no budget to exhaust (nothing leaves the page),
    /// and no connection to be down. Fan-out pushes into bounded per-port queues,
    /// where a slow port's overflow is drop-oldest-and-count, the one overflow
    /// policy every class runs — a per-port concern that never fails the
    /// publisher. The one exception is [`LOCAL_OVERLAY_STATE_CHANNEL`], whose
    /// guard drops a body the kernel would otherwise have to stand behind; its
    /// publisher is answered `Refused`, because a `PublishResult` is the
    /// publisher's only word on whether its message landed.
    ///
    /// Fold-to-latest corollary: on a last-value-semantics plane (a consumer
    /// that folds each message against current state, so only the newest matters)
    /// drop-oldest overflow loses intermediate history, never the final state —
    /// the delivered tail still converges to the most recently published value.
    fn route_local_publish(
        &mut self,
        intent: PublishIntent,
        channel: String,
        urgency: Urgency,
    ) -> Vec<Effect> {
        let PublishIntent {
            correlation,
            instance,
            port,
            body,
            stamp,
            // The caller resolved the override against the port's configured
            // default and passes the answer as `urgency`; the raw override has no
            // further use here.
            urgency: _,
            // A `local:` publish drops it: this router mints its own envelope and
            // already knows which instance published, from its own port wiring.
            subject_instance: _,
        } = intent;
        let mint = self.mint_and_route_local(
            &channel,
            LocalOrigin::Instance(&instance),
            body,
            stamp,
            urgency,
            // The single-frame path carries no release time: a deferred publish is
            // always a buffered one, and a buffered publish always flushes.
            None,
        );
        let status = match mint {
            LocalMint::Routed(_) => PublishStatus::Ok,
            LocalMint::Refused(_) => PublishStatus::Refused,
        };
        let mut effects = mint.into_effects();
        effects.push(Effect::EmitEvent(Event::PublishResult {
            instance,
            port,
            correlation,
            status,
        }));
        effects
    }

    /// Publish one of the kernel's own reserved control planes
    /// ([`LOCAL_LINK_STATE_CHANNEL`] and friends): mint, retain, fan out.
    ///
    /// The kernel grain, not a component's: the surface model defines exactly
    /// two identity grains, and these messages are the kernel acting on nobody's
    /// behalf, so they carry the bare `surface:<slug>` platform identity. There
    /// is no instance to attribute them to and inventing one would fake a
    /// component the config never declared.
    ///
    /// Fire-and-forget: no correlation, no `PublishResult`. The kernel is not a
    /// component awaiting an answer, and there is no server to answer — the
    /// publish cannot fail, because fan-out into bounded port queues is a
    /// per-port drop-oldest concern that never reaches the publisher.
    ///
    /// Dropped, not panicked, before the first `Welcome`: the surface's
    /// participant id arrives with it, so until then the kernel has no identity
    /// to publish under, and an unattributable envelope is precisely what the
    /// identity model exists to prevent. Nothing is lost by it — no chrome can
    /// have mounted that early either (the instance set rides the same
    /// `Welcome`), and the kernel's own pre-chrome indicator owns that window
    /// rather than this plane. The first post-`Welcome` transition
    /// publishes, and the depth-1 store replays it to whatever attaches later.
    fn on_publish_control(
        &mut self,
        channel: String,
        body: String,
        stamp: MessageStamp,
    ) -> Vec<Effect> {
        let reserved = reserved_local_channel(&channel).unwrap_or_else(|| {
            panic!("surface client: control publish on non-reserved channel {channel}")
        });
        // The kernel publishing a plane it does not own would be the same
        // authority confusion the boot-time output-binding rejection prevents
        // for components; this is that rule, enforced on the one party boot
        // cannot check.
        assert!(
            reserved.kernel_publish_only,
            "surface client: control publish on component-producer plane {channel}"
        );
        // Not gated on `local_router_live()`: a control publish routes even in a
        // terminal state, because the terminal transition's own link-state
        // notification (`fatal` / `reloading`) is exactly the message chrome needs
        // to draw the death banner, and the router's rings and chrome's mount both
        // outlive that transition. The one drop remains identity: before the first
        // `Welcome` the kernel has no `surface:<slug>` to publish under.
        if self.participant_id.is_empty() {
            return Vec::new();
        }
        // Inert: urgency decides whether a parked row is worth waking a consumer
        // for, and page-local delivery wakes on every arrival regardless. Normal is
        // the honest value — the kernel states no preference, and there is no
        // operator knob on a contract-defined plane to resolve one from.
        self.mint_and_route_local(
            &channel,
            LocalOrigin::Kernel,
            body,
            stamp,
            Urgency::Normal,
            // The kernel states its control planes now or not at all: a plane whose
            // reader mounts later reads the retained value, so there is nothing a
            // schedule would buy.
            None,
        )
        .into_effects()
    }

    // TODO(attach-cutover): this plane guard block — overlay validation, the
    // takeover stamp in `guard_local_body`, and `record_overlay_state` — is
    // duplicated by `crate::planes::SurfacePlanes`, which states the same rules
    // as the policy the attach crate's router is constructed with. Delete it
    // here when the kernel cuts over.

    /// Judge a publish on [`LOCAL_OVERLAY_STATE_CHANNEL`] against the plane's
    /// publisher rules, without recording anything.
    ///
    /// `Some(effect)` means refused: the message is neither retained nor
    /// delivered, and the effect reports the violation. `None` means the body may
    /// become a message on the plane (which keeps its depth-1 ring so a
    /// page-local consumer can read the same value).
    ///
    /// Three refusals, all of them "the kernel would otherwise report something
    /// it cannot stand behind": a publisher that is not this surface's chrome (a
    /// component cannot speak for chrome's overlay — `chrome = true` is unique
    /// per surface, so any other publisher is an operator wiring a lie); a body
    /// that does not parse; and a `holder` naming an instance the surface does
    /// not declare, which the server would treat as a protocol violation and
    /// kill the session over.
    ///
    /// Who holds the overlay is recorded separately, by
    /// [`Self::record_overlay_state`], at the moment the message reaches the
    /// plane.
    ///
    /// # Panics
    ///
    /// If the kernel itself publishes here. The kernel holds no overlay and
    /// renders none; a kernel-minted overlay report would be the kernel
    /// inventing telemetry about a component's state.
    fn validate_overlay_state(&self, origin: LocalOrigin<'_>, body: &str) -> Option<Effect> {
        let instance = match origin {
            LocalOrigin::Instance(instance) => instance,
            LocalOrigin::Kernel => panic!(
                "surface client: the kernel does not publish on {LOCAL_OVERLAY_STATE_CHANNEL}"
            ),
        };
        let bindings = self
            .bindings
            .as_ref()
            .expect("surface client: a resolved local publish implies bindings");
        let refuse = |reason: String| {
            Some(Effect::EmitEvent(Event::OverlayStateRejected {
                instance: instance.to_string(),
                reason,
            }))
        };
        if bindings.chrome_instance.is_empty() || bindings.chrome_instance != instance {
            return refuse("only the surface's chrome instance may publish it".to_string());
        }
        let parsed: OverlayStateBody = match serde_json::from_str(body) {
            Ok(parsed) => parsed,
            Err(err) => return refuse(format!("unparseable body: {err}")),
        };
        if let Some(holder) = &parsed.holder
            && !bindings.components.iter().any(|c| c.instance == *holder)
        {
            return refuse(format!("holder {holder:?} is not a declared instance"));
        }
        None
    }

    /// The reserved confined planes' rules for one component-authored body about
    /// to become a message on `channel`.
    ///
    /// `Carry(body)` is the body to use — the takeover plane rewrites it, every
    /// other channel passes it through. `Refused(effect)` means the body was
    /// rejected, and the effect reports the violation.
    ///
    /// **Every** path that puts a component-authored body onto a confined channel
    /// runs this: the immediate mint, the park, and the rewrite a deferred edit
    /// makes. A guard the edit skipped would be one a component walks around by
    /// scheduling a well-formed message and rewriting it before it releases.
    fn guard_local_body(
        &self,
        channel: &str,
        origin: LocalOrigin<'_>,
        body: String,
    ) -> GuardedBody {
        if channel == LOCAL_OVERLAY_STATE_CHANNEL
            && let Some(effect) = self.validate_overlay_state(origin, &body)
        {
            return GuardedBody::Refused(effect);
        }
        // The takeover plane's payload carries a request/deny/release identity
        // that the consumer (chrome) trusts; overwrite it with the authenticated
        // publishing instance so a component cannot forge another's takeover.
        // Derived from the router's own port wiring, exactly like `sender` — the
        // component names only its port.
        if channel != LOCAL_TAKEOVER_CHANNEL {
            return GuardedBody::Carry(body);
        }
        match origin {
            LocalOrigin::Instance(instance) => {
                GuardedBody::Carry(inject_takeover_instance(body, instance))
            }
            // The kernel has no takeover identity to stamp, and an unattributable
            // takeover message is precisely what the identity model forbids. A
            // future kernel-emitted takeover (a forced release on instance
            // failure, say) must carry an explicit attributed mechanism rather
            // than ride an anonymous body through the mint.
            LocalOrigin::Kernel => {
                panic!("surface client: the kernel does not publish on {channel}")
            }
        }
    }

    /// Record who holds the overlay, from a message that has just reached
    /// [`LOCAL_OVERLAY_STATE_CHANNEL`].
    ///
    /// Called where the message becomes observable — an immediate append and a
    /// release alike — rather than where it was minted. A schedule that is
    /// parked, then cancelled or refused by the deferred cap, never reaches a
    /// consumer, so recording it at the mint would leave the kernel reporting an
    /// overlay no page consumer ever saw and that will never exist.
    ///
    /// A holder the surface no longer declares is skipped: a `Status` frame
    /// naming an unconfigured instance is a protocol violation the server kills
    /// the session over, and a `Welcome` can retire an instance between a park
    /// and its release.
    fn record_overlay_state(&mut self, envelope: &MessageEnvelope) {
        if envelope.channel != LOCAL_OVERLAY_STATE_CHANNEL {
            return;
        }
        let parsed: OverlayStateBody = serde_json::from_str(&envelope.body)
            .expect("surface client: a body on the overlay plane parsed when it was accepted");
        let declared = parsed.holder.as_ref().is_none_or(|holder| {
            self.bindings
                .as_ref()
                .is_some_and(|b| b.components.iter().any(|c| c.instance == *holder))
        });
        if !declared {
            return;
        }
        self.overlay = parsed.holder.map(|holder| OverlayReport {
            holder,
            since: envelope.publish_ts,
        });
    }

    /// Mint a `local:` envelope, assign its position, retain it, and fan it out
    /// to every port bound to the channel.
    ///
    /// The whole of what the router does, shared by its callers so the component
    /// grain and the kernel grain cannot drift in position assignment,
    /// retention, or fan-out. They differ in exactly one value — the identity
    /// derived from `origin` — and no other.
    ///
    /// This is the single point every `local:` publish passes through — the
    /// gesture path and the activation-buffer flush both arrive here — so it is
    /// where the reserved planes' guard ([`Self::guard_local_body`]) runs for a
    /// publish. A deferred edit rewrites a body without passing through here, so
    /// it runs the same guard itself.
    ///
    /// Returns [`LocalMint::Refused`] when a plane guard rejects the body, so a
    /// caller with a publisher to answer can say so rather than report a
    /// delivery that did not happen.
    fn mint_and_route_local(
        &mut self,
        channel: &str,
        origin: LocalOrigin<'_>,
        body: String,
        stamp: MessageStamp,
        urgency: Urgency,
        deliver_after: Option<u64>,
    ) -> LocalMint {
        let body = match self.guard_local_body(channel, origin, body) {
            GuardedBody::Carry(body) => body,
            GuardedBody::Refused(effect) => return LocalMint::Refused(vec![effect]),
        };
        // Resolved before the store is borrowed: both read `self`.
        let sender = match origin {
            LocalOrigin::Instance(instance) => self.local_sender(instance),
            LocalOrigin::Kernel => self.participant_id.clone(),
        };
        let source = self.participant_id.clone();
        let channel = channel.to_string();
        let envelope = MessageEnvelope {
            message_id: stamp.message_id,
            // Provenance. On the wire this is the server's origin, because the
            // server is the instance that produced the message; page-local
            // traffic is produced by the page, so the surface's own identity is
            // the honest answer — and the only one available, since no server
            // origin reaches the client.
            source,
            channel: channel.clone(),
            // The sender's identity, at whichever of the two grains applies:
            // `surface:<slug>#<instance>` when a component published, the bare
            // `surface:<slug>` when the kernel did. Derived by the router from
            // its own wiring in both cases — for a wire publish the server
            // derives this from the instance its declaration set admits and the
            // client asserts nothing; no server sees this message, so the router
            // derives it the same way, never from anything the component said.
            // The component names only its port, so it can forge neither this
            // nor `source`.
            sender,
            publish_ts: stamp.publish_ts,
            body,
            reply_to: None,
            delivery_deadline: None,
            // Always `None`, even for the parked message below: a schedule is the
            // channel's, held in its deferred set until it releases, and a released
            // message is an ordinary arrival nobody is owed a release time for.
            deliver_after: None,
            // A page-local publish carries no user-interaction authority: the
            // component names only its port, and nothing in the browser is in a
            // position to assert a gesture on the operator's behalf.
            impetus: None,
            // The caller's override, else the port's configured default —
            // resolved by the caller, since this core is the router and no
            // server downstream will apply the default for it.
            //
            // Inert for waking: urgency decides whether a parked row is worth
            // waking a consumer for, and page-local delivery wakes on every
            // arrival regardless (the fan-out below is synchronous and
            // unconditional). Carried honestly anyway: the field exists on the
            // envelope, so it should report what the sender and the operator
            // actually said rather than a hard-coded value a reader would mistake
            // for one of them.
            urgency,
            envelope_type: ChannelScheme::Local,
        };
        // A release time still ahead of the mint holds the message out of
        // retention until it arrives; one already past publishes immediately, which
        // is the contract every host gives a `deliver_after` in the past.
        if let Some(release_at) = deliver_after.filter(|at| *at > epoch_ms(stamp.publish_ts)) {
            let LocalOrigin::Instance(instance) = origin else {
                // The kernel's own planes are stated now or not at all, so no
                // kernel publish carries a release time; one that did would be a
                // schedule with no component to account it to.
                panic!("surface client: the kernel does not park on {channel}")
            };
            return self.park_local(instance, &channel, envelope, release_at);
        }
        // The plane's state is recorded here rather than at the guard above,
        // because here is where the message actually reaches its readers.
        self.record_overlay_state(&envelope);
        // The store is the channel's retention and the source of every window
        // assembled off it. The page is the authority for a confined channel, so
        // this append *is* the delivery: every instance bound to the channel is
        // woken by it — there is no per-instance subscription to scope a confined
        // publish to — and no per-message effect is emitted, because that batching
        // is the delivery model.
        let overflow = self
            .stores
            .get_mut(&StoreKey::Confined(channel.clone()))
            .expect(
                "surface client: every routable confined channel has a store (reserved planes \
                 seeded at construction, the rest proven by on_welcome)",
            )
            .append_minted(envelope);
        LocalMint::Routed(self.enact_overflow(&channel, overflow))
    }

    /// Hold a minted confined envelope in its channel's deferred set until
    /// `release_at`.
    ///
    /// Nothing is retained, nothing is woken, and no effect is emitted: a parked
    /// message is not on the channel yet. The release timer the caller's input
    /// re-arms is what brings it back.
    ///
    /// **Quota exhaustion is normal operation, not an error.** The schedule is
    /// dropped, logged, and counted against the instance — never reported to the
    /// component, because a post-activation flush has no error channel back to a
    /// guest that already returned.
    fn park_local(
        &mut self,
        instance: &str,
        channel: &str,
        envelope: MessageEnvelope,
        release_at: u64,
    ) -> LocalMint {
        let sender = envelope.sender.clone();
        let parked = self
            .stores
            .get_mut(&StoreKey::Confined(channel.to_string()))
            .expect(
                "surface client: every routable confined channel has a store (reserved planes \
                 seeded at construction, the rest proven by on_welcome)",
            )
            .park(&sender, envelope, release_at);
        if let Err(brenn_queue::QuotaExceeded { cap }) = parked {
            tracing::warn!(
                instance,
                %channel,
                cap,
                release_at,
                "surface client: deferred set full, dropping the schedule"
            );
            if let Some(reg) = self.registered.get_mut(instance) {
                reg.deferred_dropped += 1;
            }
        }
        LocalMint::Routed(Vec::new())
    }

    /// Enact the loudness ladder for the positions one retirement outran — an
    /// arrival that evicted, or a depth shrink that trimmed.
    ///
    /// The loss is *counted* the moment it happens, against every binding
    /// retention was pushed past: a binding that lags is on the books whether or
    /// not it runs again, which is the whole reason the charge is here rather than
    /// only at the next window. The `alarm` rung's announcement is deferred to
    /// that window, where one alert names the whole delta instead of one per
    /// message. The `fatal` rung's is not deferrable — the kill means there is no
    /// next window — so a fatal charge announces here.
    ///
    /// The still-retained remainder is counted at the next window instead, so no
    /// span is counted twice.
    ///
    /// Sorted by binding so a page whose several bindings overflowed on one
    /// retirement emits its loud effects in a stable order — the bindings are
    /// independent, so any total order is correct and a stable one keeps the
    /// page's telemetry reproducible.
    fn enact_overflow(
        &mut self,
        channel: &str,
        overflow: Vec<CursorOverflow<BindingKey>>,
    ) -> Vec<Effect> {
        if overflow.is_empty() {
            return Vec::new();
        }
        let mut charged = overflow;
        charged.sort_by(|a, b| {
            (&a.subscriber.instance, &a.subscriber.port)
                .cmp(&(&b.subscriber.instance, &b.subscriber.port))
        });
        let mut effects = Vec::new();
        for CursorOverflow {
            subscriber,
            evicted,
        } in charged
        {
            let BindingKey { instance, port } = subscriber;
            // A position outliving its binding in the table cannot be charged
            // against a noise level, and there is one moment where that happens: a
            // `Welcome` reconciles the stores — trimming a shrunk one — before it
            // reconciles the positions, so a trim can outrun a position whose
            // binding this very `Welcome` removed. Nothing is owed a charge on a
            // binding the operator has unwired; the position is about to go with
            // it.
            let Some(noise) = self.binding_noise(&instance, &port) else {
                continue;
            };
            effects.extend(self.enact_drop_charges(
                &instance,
                vec![DropCharge {
                    port,
                    channel: channel.to_string(),
                    noise,
                    counted: evicted,
                    announced: if noise >= NoiseLevel::Fatal {
                        evicted
                    } else {
                        0
                    },
                }],
            ));
        }
        effects
    }

    /// The resolved noise level of one input binding, or `None` when the binding
    /// table does not hold it.
    fn binding_noise(&self, instance: &str, port: &str) -> Option<NoiseLevel> {
        self.bindings
            .as_ref()?
            .subscriptions
            .iter()
            .find(|b| b.instance == instance && b.port == port)
            .map(|b| b.noise)
    }

    /// The component sub-identity for a publish from `instance`:
    /// `surface:<slug>#<instance>`. The principal is the instance, so sibling
    /// instances of one kind are distinct senders. `#` is outside the
    /// slug/instance charset, so the form is unambiguous.
    ///
    /// The instance half is the `Welcome` instance map's own key — the same set
    /// the server admits a wire publish's instance against — so a component
    /// cannot claim an identity by naming one: it names only its own port, and
    /// the router takes the rest from its wiring. An instance absent from the map
    /// cannot reach here — the publish resolved a binding, and the handshake
    /// rejects a `Welcome` whose bindings name an undeclared instance — so its
    /// absence is a broken internal invariant rather than an anonymous fallback:
    /// attributing a message to nobody is exactly what the identity model exists
    /// to prevent.
    ///
    /// The form itself comes from [`surface_sub_identity`], the same helper the
    /// server's `ParticipantId::for_surface_component` composes with: two parties
    /// derive this identity independently, so the grammar has one home.
    fn local_sender(&self, instance: &str) -> String {
        let declared = self
            .bindings
            .as_ref()
            .expect("surface client: a resolved local publish implies bindings")
            .components
            .iter()
            .any(|c| c.instance == instance);
        assert!(
            declared,
            "surface client: local publish from instance {instance:?} absent from the Welcome \
             instance map"
        );
        surface_sub_identity(&self.participant_id, instance)
    }

    /// Set the liveness deadline to `now + liveness_ms` and return it.
    fn arm_liveness(&mut self, now: Millis) -> Millis {
        self.deadline = now.saturating_add_ms(self.liveness_ms);
        self.deadline
    }

    /// (Re)subscribe every channel that still has a subscribed instance.
    /// Transport close reset every channel's wire state to `Unsubscribed`, so on
    /// each `Welcome` of a connection every
    /// channel with a surviving refcount opens a fresh subscription, presenting
    /// its retained high-water resume token (if any) so an in-ring transport
    /// blip is lossless for the channel's continuously-subscribed instances.
    /// Called only from [`Self::on_welcome`], where the state is `Active`; a
    /// channel at refcount 0 (all its subscriptions dropped by reconcile) is
    /// left `Unsubscribed`, so no `Subscribe` is ever emitted for it — and its
    /// token was already discarded, so no orphaned token can leak onto the wire.
    fn resubscribe_survivors(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        for (sub, cs) in self.channels.iter_mut() {
            if cs.refcount > 0 && cs.wire == WireState::Unsubscribed {
                // Reset the span high-water and echo the stored cursor verbatim
                // (or `None` on a fresh attach). Class-blind: the server decides
                // what the cursor means, including a stale one.
                let resume = cs.prepare_subscribe();
                effects.push(Effect::SendFrame(ClientFrame::Subscribe {
                    channel: sub.channel.clone(),
                    instance: sub.instance.clone(),
                    resume,
                }));
            }
        }
        effects
    }

    /// A text frame arrived while `Active`. Any inbound text frame resets the
    /// liveness deadline; `Heartbeat` carries no other effect. A second
    /// `Welcome` is fatal. `SubscribeResult` drives the wire-state machine,
    /// `Deliver` fans out to bound ports, and `PublishResult` routes back to the
    /// publish that awaits it. Fatal frames go terminal and disarm the timer, so
    /// they do not reset liveness — the connection is dying regardless.
    fn on_text_active(&mut self, text: &str, now: Millis) -> Vec<Effect> {
        let frame = match serde_json::from_str::<ServerFrame>(text) {
            Ok(frame) => frame,
            Err(err) => return self.go_fatal(format!("unparseable server frame: {err}")),
        };
        match frame {
            ServerFrame::Heartbeat => vec![Effect::SetWakeup(Some(self.arm_liveness(now)))],
            ServerFrame::Welcome { .. } => self.go_fatal("second Welcome frame".to_string()),
            ServerFrame::SubscribeResult {
                channel,
                instance,
                outcome,
                gap,
                ..
            } => self.on_subscribe_result(SubKey { instance, channel }, outcome, gap, now),
            ServerFrame::Deliver {
                channel,
                envelope,
                targets,
            } => self.on_deliver_frame(channel, envelope, targets, now),
            ServerFrame::PublishResult {
                correlation,
                outcome,
            } => self.on_publish_result(correlation, outcome, now),
            ServerFrame::PublishBatchResult {
                correlation,
                outcome,
            } => self.on_publish_batch_result(correlation, outcome, now),
            ServerFrame::DeferredView {
                channel,
                instance,
                entries,
            } => self.on_deferred_view(channel, instance, entries, now),
        }
    }

    /// The backend restated what one component instance has parked on one
    /// transportable channel.
    ///
    /// A full snapshot, so it replaces the mirror wholesale — idempotent,
    /// last-writer-wins, and an empty one is a legitimate answer meaning the set
    /// is empty. Nothing is validated against the binding table: a view for a pair
    /// this page no longer binds is inert (no window reads it), and refusing it
    /// would make an ordinary reconnect race fatal.
    ///
    /// Wakes nobody. A schedule changing is not an arrival — only a release is,
    /// and a released message reaches the page as an ordinary `Deliver`.
    fn on_deferred_view(
        &mut self,
        channel: String,
        instance: String,
        entries: Vec<DeferredViewEntry>,
        now: Millis,
    ) -> Vec<Effect> {
        self.deferred_views.insert((channel, instance), entries);
        vec![Effect::SetWakeup(Some(self.arm_liveness(now)))]
    }

    /// The server answered one `PublishBatch`.
    ///
    /// A result for a correlation this core never sent, or already settled, is
    /// inexplicable — the correlation space is the kernel's own and monotone — so
    /// it is a fatal protocol error rather than a tolerated echo.
    ///
    /// A refused batch is **not** discarded. The activation returned ok, so the
    /// kernel's guarantee is "flushed, not discarded, up to a stated bound" — in
    /// the refusal case exactly as in the disconnect case, and a refusal is not
    /// even a failure: the server's backstop meters the wire rate, and being
    /// metered is what a backstop is for. So the batch goes back to the head of
    /// its instance's outbox (it is the oldest un-applied flush) and is retried
    /// on the timer. Nothing retries forever without evidence: a head the server
    /// keeps refusing converges to the outbox cap, and from there to counted,
    /// toasted drops.
    fn on_publish_batch_result(
        &mut self,
        correlation: u64,
        outcome: PublishBatchOutcome,
        now: Millis,
    ) -> Vec<Effect> {
        let Some(pending) = self.pending_batches.remove(&correlation) else {
            return self.go_fatal(format!(
                "PublishBatchResult for unknown correlation {correlation}"
            ));
        };
        let PendingBatch {
            instance,
            entries,
            ops,
        } = pending;
        // The instance can have deregistered under the outstanding frame; its
        // outbox went with it and there is nothing to clear or re-park.
        let registered = match self.registered.get_mut(&instance) {
            Some(reg) => {
                reg.batch_in_flight = None;
                true
            }
            None => false,
        };
        let mut effects = vec![Effect::SetWakeup(Some(self.arm_liveness(now)))];
        if !registered {
            // The instance deregistered under the outstanding frame. An `Ok` was
            // already applied server-side, but a `RateLimited` result drops ok'd
            // entries that were never applied — counted, announced, never silent.
            if matches!(outcome, PublishBatchOutcome::RateLimited) {
                tracing::warn!(
                    %instance,
                    entries = entries.len(),
                    "surface client: activation flush refused after its instance deregistered — \
                     ok'd entries dropped with the outbox"
                );
            }
            return effects;
        }
        match outcome {
            PublishBatchOutcome::Ok => {
                // The wire is free for this instance again: anything that queued
                // behind the frame goes out now rather than waiting a tick.
                effects.extend(self.pump_outbox(&instance));
            }
            PublishBatchOutcome::RateLimited => {
                let reg = self
                    .registered
                    .get_mut(&instance)
                    .expect("surface client: instance registered a line ago");
                reg.rate_limited_batches += 1;
                tracing::warn!(
                    %instance,
                    "surface client: server rate-limited an activation flush — parked at the \
                     head of the instance's outbox and retried"
                );
                effects.extend(self.park_batch(&instance, ParkedBatch { entries, ops }, true));
            }
        }
        effects.extend(self.retry_wakeup(now));
        effects
    }

    /// One `Deliver` frame arrived: one envelope on one channel, addressed to
    /// one or more of this connection's subscriptions of that channel.
    ///
    /// This is the kernel's fan-out site — the wire carries the payload once and
    /// the kernel delivers it to each named subscription, exactly as the
    /// backend's dispatcher fans one publish out to its consumers without the
    /// transport copying the body per subscriber. Each target is then handled by
    /// [`Self::on_deliver`] with its own per-subscription `(seq, cursor,
    /// dropped)`; nothing about a target's handling depends on how many targets
    /// shared its frame, so a single-target frame and a one-entry multi-target
    /// frame are the same thing.
    ///
    /// The frame is validated **whole before any target is delivered**, so a
    /// frame from a broken server is rejected rather than half-applied. Three
    /// frame-level protocol errors, each inexplicable from a correct server and
    /// therefore fatal:
    ///
    /// - empty `targets` — a delivery addressed to nobody;
    /// - two targets naming one subscription — which would ask that
    ///   subscription's span seq to both advance and regress within one frame;
    /// - a target naming a subscription never active on this connection —
    ///   acceptance follows the server's FIFO ordering, so it is inexplicable
    ///   (the same check a single-target frame has always made);
    /// - a target whose span seq does not exceed its own subscription's
    ///   high-water. Each target is checked against *its own* subscription's
    ///   span: sibling seq counters are unrelated, so one target's seq says
    ///   nothing about another's. Straggler targets are exempt, as they are on a
    ///   single-target frame — they advance no state, so they have no span to
    ///   regress.
    ///
    /// Liveness re-arms once per frame, not once per target: it is a property of
    /// inbound traffic, and the frame is one arrival however many subscriptions
    /// it feeds.
    fn on_deliver_frame(
        &mut self,
        channel: String,
        envelope: MessageEnvelope,
        targets: Vec<DeliverTarget>,
        now: Millis,
    ) -> Vec<Effect> {
        if targets.is_empty() {
            return self.go_fatal(format!("Deliver on {channel} with no targets"));
        }
        let mut seen: Vec<&String> = Vec::with_capacity(targets.len());
        for t in &targets {
            if seen.contains(&&t.instance) {
                return self.go_fatal(format!(
                    "Deliver on {channel} names subscription (instance {:?}) twice",
                    t.instance
                ));
            }
            seen.push(&t.instance);
            let sub = SubKey {
                instance: t.instance.clone(),
                channel: channel.clone(),
            };
            let cs = match self.channels.get(&sub) {
                Some(cs) if cs.has_been_active => cs,
                _ => {
                    return self.go_fatal(format!(
                        "Deliver for a subscription never active on this connection: {channel} \
                         (instance {:?})",
                        t.instance
                    ));
                }
            };
            // Class-blind continuity check: the server assigns `seq` strictly
            // increasing per subscription-span for both wire classes, minted at
            // the socket-write boundary, so a `seq` that does not exceed the
            // span high-water is a server bug — fatal, never tolerated. A
            // straggler advances no span, so it is checked against none.
            if cs.wire == WireState::Active
                && let Some(hw) = cs.span_hw
                && t.seq <= hw
            {
                return self.go_fatal(format!(
                    "Deliver seq regression on {channel}: {} not greater than {hw}",
                    t.seq
                ));
            }
        }
        let mut effects = vec![Effect::SetWakeup(Some(self.arm_liveness(now)))];
        for target in targets {
            let DeliverTarget {
                instance,
                seq,
                cursor,
                dropped,
            } = target;
            let sub = SubKey {
                instance,
                channel: channel.clone(),
            };
            // The envelope is cloned per target because each subscription's
            // retained store owns its entries. Fan-out cost is per-subscription
            // page state, which the design keeps; what the consolidation removes
            // is paying that cost N times on the wire.
            effects.extend(self.on_deliver(sub, envelope.clone(), seq, cursor, dropped));
        }
        effects
    }

    /// One target of a `Deliver` frame. Route the envelope into the
    /// subscription's store, from which the next activation window is assembled.
    ///
    /// The caller has already established that the subscription has been
    /// `Active` on this connection. A subscription that *has* been `Active` but
    /// is not *currently* `Active` (`Unsubscribed` after an `Unsubscribe`, or
    /// `Pending` on an immediate re-subscribe) is the one tolerated race: the
    /// target is a previous-span straggler and is discarded entirely — no
    /// routing, no token/seq effect.
    ///
    /// `dropped > 0` (server-side overflow since this subscription's
    /// previous accepted delivery) is accumulated as the loss counter the next
    /// window reports through `PortWindow::dropped`; there is no marker in the
    /// message stream. A discarded straggler advances no state and contributes
    /// no `dropped`. The discard itself is surfaced via
    /// [`Event::StragglerDiscarded`] (the first straggler per activation span),
    /// so it is diagnosable without changing the semantics.
    ///
    /// Liveness is the frame's business, not a target's: the caller arms it once
    /// per frame.
    fn on_deliver(
        &mut self,
        sub: SubKey,
        envelope: MessageEnvelope,
        seq: u64,
        cursor: Cursor,
        dropped: u64,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.channels.get(&sub).map(|cs| cs.wire) != Some(WireState::Active) {
            // Tolerated post-`Unsubscribe` straggler: discard, keep liveness
            // fresh, and — deliberately — leave the token untouched. Advancing
            // it from a discarded Deliver would resume past the retained latest
            // value and suppress its replay on the next fresh attach.
            //
            // Surface the discard once per activation span, gated on
            // `straggler_reported`. The emission is once-per-span (not
            // per-straggler) because the EventStream panics on overflow
            // (`Driver::emit`), so nothing server-paced may ride it unbounded.
            let cs = self.channels.get_mut(&sub).unwrap();
            if !cs.straggler_reported {
                cs.straggler_reported = true;
                effects.push(Effect::EmitEvent(Event::StragglerDiscarded {
                    channel: sub.channel,
                    seq,
                    dropped,
                }));
            }
            return effects;
        }
        // The caller checked this seq against this subscription's span
        // high-water before any target was applied. Advance the high-water and
        // store the latest accepted cursor (both are structurally monotone; the
        // cursor is opaque and echoed verbatim on reconnect).
        let cs = self
            .channels
            .get_mut(&sub)
            .expect("surface client: Active subscription has state");
        cs.span_hw = Some(seq);
        cs.token = Some(cursor);
        // Into the subscription's store, for **every** subscription uniformly —
        // registered instances or not, since retention is what makes a dropped
        // message recoverable and is not a fact about who is listening. Arrival
        // moves no position, which is what coalesces a turn's deliveries into one
        // activation; a binding whose position the entry outran is charged here,
        // at the arrival that caused it.
        //
        // The insert is idempotent by `message_id` because several reconnect paths
        // legitimately re-present what the store already holds.
        //
        // A subscription this frame reaches is `Active`, so it holds at least one
        // reference, and taking a reference is what created its store. A store-less
        // `Active` subscription is a broken invariant — fail fast rather than
        // silently deliver a message no window can ever show as context.
        let channel = sub.channel.clone();
        let overflow = match self.stores.get_mut(&StoreKey::Wire(sub.clone())) {
            Some(store) => {
                // The server's figure is the subscription's, so every binding
                // holding a position on it takes the full count: each of them
                // missed those messages, and no page-side arithmetic can see a loss
                // that happened upstream of the page.
                store.count_server_drops(dropped);
                store.insert(envelope)
            }
            None => {
                return self.go_fatal(format!(
                    "wire Deliver on a store-less subscription: {} (instance {:?})",
                    sub.channel, sub.instance
                ));
            }
        };
        effects.extend(self.enact_overflow(&channel, overflow));
        effects
    }

    /// Route a handle command.
    fn on_command(&mut self, cmd: Command) -> Vec<Effect> {
        match cmd {
            Command::Publish {
                correlation,
                instance,
                port,
                body,
                subject_instance,
                urgency,
                stamp,
            } => self.on_publish(PublishIntent {
                correlation,
                instance,
                port,
                body,
                subject_instance,
                urgency,
                stamp,
            }),
            Command::PublishControl {
                channel,
                body,
                stamp,
            } => self.on_publish_control(channel, body, stamp),
            Command::Alert {
                severity,
                title,
                body,
            } => self.on_alert(severity, title, body),
            Command::SendGeometry {
                width,
                height,
                device_pixel_ratio,
            } => self.on_send_geometry(width, height, device_pixel_ratio),
            Command::SendStatus {
                instances,
                uptime_secs,
                counters,
            } => self.on_send_status(instances, uptime_secs, counters),
            Command::Close => self.on_close(),
        }
    }

    /// Handle a kernel-requested orderly shutdown. Mirrors the teardown shape of
    /// `go_fatal`/`enter_reload_required` minus any surfaced event (the kernel
    /// asked for this, so it needs no notification): reset the bus plane, close
    /// the transport best-effort, fail any outstanding publishes with
    /// `ConnectionLost`, disarm the timer, and enter the terminal `Closed`
    /// state. No reconnect. `CloseTransport` is emitted unconditionally — the
    /// driver no-ops it when no transport is live, exactly as on the fatal path.
    fn on_close(&mut self) -> Vec<Effect> {
        self.reset_bus_plane();
        let mut effects = vec![Effect::CloseTransport];
        effects.extend(self.fail_pending_publishes());
        self.state = State::Closed;
        effects.push(Effect::SetWakeup(None));
        effects
    }

    /// Handle an `alert` command. Best-effort: the alert rides the
    /// same WS, so it is sent only while `Active` and silently dropped otherwise.
    /// Title and body are truncated to the proto caps on UTF-8 boundaries so a
    /// conforming client never sends an oversized `Alert` — which, unlike a
    /// `Log`, is a protocol violation the server kills the session over.
    ///
    /// The send is additionally gated on the surface's alert grant
    /// (`Welcome.alert_granted`): an ungranted `Alert` is a grant violation the
    /// server kills the session over, so the core drops it here rather than
    /// letting `ClientHandle::alert` reach the wire on an ungranted surface. The
    /// two drops are different in kind and handled separately: the not-`Active`
    /// drop is a benign liveness race (silent, shared with `log`); the ungranted
    /// drop is a capability refusal on a caller that failed to pre-gate, so it
    /// leaves a `warn!` breadcrumb rather than vanishing without a trace.
    fn on_alert(&mut self, severity: AlertSeverity, title: String, body: String) -> Vec<Effect> {
        if self.state != State::Active {
            return Vec::new();
        }
        if !self.alert_granted {
            tracing::warn!(
                "surface client: dropped Alert — surface has no alert grant; callers of \
                 ClientHandle::alert must pre-gate on Welcome.alert_granted"
            );
            return Vec::new();
        }
        vec![Effect::SendFrame(ClientFrame::Alert {
            severity,
            title: truncate_report_field(title, MAX_ALERT_TITLE_BYTES),
            body: truncate_report_field(body, MAX_ALERT_BODY_BYTES),
        })]
    }

    // TODO(attach-cutover): the two telemetry commands below carry the frames
    // that die with this wire; the documents replacing them are composed by
    // `crate::telemetry` and sent as ordinary publishes through
    // `crate::outbound`. Delete this path when the kernel cuts over.

    /// Handle a `SendGeometry` telemetry command. Best-effort like `on_alert`:
    /// the frame rides the same WS, so it is sent only while `Active`. The
    /// not-`Active` drop is the benign liveness race shared with the other
    /// best-effort planes and stays silent.
    fn on_send_geometry(
        &mut self,
        width: u32,
        height: u32,
        device_pixel_ratio: f64,
    ) -> Vec<Effect> {
        if self.state != State::Active {
            return Vec::new();
        }
        vec![Effect::SendFrame(ClientFrame::Geometry {
            width,
            height,
            device_pixel_ratio,
        })]
    }

    /// Handle a `SendStatus` telemetry command. Same best-effort discipline as
    /// [`Self::on_send_geometry`]: sent only while `Active`, silently dropped
    /// otherwise.
    fn on_send_status(
        &mut self,
        instances: Vec<InstanceReport>,
        uptime_secs: u64,
        counters: StatusCounters,
    ) -> Vec<Effect> {
        if self.state != State::Active {
            return Vec::new();
        }
        vec![Effect::SendFrame(ClientFrame::Status {
            instances,
            uptime_secs,
            counters,
            // The kernel's own field, not the DOM executor's: overlay state
            // reaches the core through the router, so the report carries what
            // the last chrome transition said and nothing the caller supplies.
            overlay: self.overlay.clone(),
        })]
    }

    /// Handle a publish command. Re-runs the handle's pre-validation
    /// authoritatively via the shared [`check_publish`] (the same check the
    /// handle's fast gate calls): a publish issued while unreachable, for an
    /// unbound `(instance, port)`, or with a body over the server's cap is never
    /// sent — the core answers it with an [`Event::PublishResult`] carrying the
    /// local status (this fires only around reconnects, when the handle's
    /// snapshot is a bindings-generation stale and the new bindings no longer
    /// bind the pair — an unbound pair under both generations is the ordinary
    /// reject, and a pair bound under both never rejects). An accepted publish
    /// is tracked by correlation and sent as a `Publish` frame.
    ///
    /// A publish whose output port resolves to a `local:` channel is routed by
    /// this core's own router instead ([`Self::route_local_publish`]) and never
    /// reaches the wire — including while the link is down, since "reachable" for
    /// a page-local port has nothing to do with the connection.
    ///
    /// The reserved `#brenn`/`error-reports` output port is treated as bound
    /// whenever the error-report floor is advertised (`Welcome.error_report_floor
    /// == Some`), even though it is absent from the bindings table: it is kernel
    /// infrastructure the server advertises via the floor, not component wiring.
    /// When the floor is `None` the reserved port is unbound like any other, so a
    /// publish to it is the ordinary unbound-port rejection.
    fn on_publish(&mut self, intent: PublishIntent) -> Vec<Effect> {
        let PublishIntent {
            correlation,
            instance,
            port,
            body,
            subject_instance,
            urgency,
            stamp,
        } = intent;
        // A local reject on the *live* reserved port (floor advertised) is
        // fire-and-forget: surfacing it re-enters the kernel's non-`Ok` breadcrumb
        // path, which would publish a fresh report about the failed report — the
        // self-amplifying loop the result-swallow closes. The kernel's pre-publish
        // console copy is the durable record. When the floor is *absent* the
        // reserved pair is an ordinary unbound port, so its `UnboundPort` reject
        // surfaces normally (a non-conforming client sent it).
        let swallow_reject = self.is_error_report_port(&instance, &port);
        let reject = |status| {
            if swallow_reject {
                return Vec::new();
            }
            vec![Effect::EmitEvent(Event::PublishResult {
                instance: instance.clone(),
                port: port.clone(),
                correlation,
                status,
            })]
        };
        // Resolve the output binding once: `Some` answers "is it bound", the
        // address answers "route it locally or put it on the wire", and its
        // `urgency` is the port's configured default. Absent bindings
        // (pre-first-`Welcome`) resolve to `None` — unbound, and no local channel
        // can exist yet either, since rings are built from `Welcome`.
        //
        // Borrowed, not cloned: locality is a `&str` question, and only the local
        // branch below needs an owned channel (to outlive the `&mut self` the
        // router takes). A wire publish — the common case — pays no allocation.
        let out = self
            .bindings
            .as_ref()
            .and_then(|b| resolve_output(b, &instance, &port));
        let local =
            out.is_some_and(|b| !channel_is_transportable(&b.channel)) && self.local_router_live();
        if let Err(r) = check_publish(
            self.state == State::Active || local,
            || self.is_error_report_port(&instance, &port) || out.is_some(),
            body.len() as u64,
            self.max_body_bytes,
        ) {
            return reject(PublishStatus::from(r));
        }
        if local {
            // Routed in-page and answered synchronously below, so it never joins
            // `pending_publishes`: that map exists to route a *server*
            // `PublishResult` back by correlation, and no server will answer this.
            //
            // Resolve urgency here rather than forwarding it: this core is the
            // router, so there is no server downstream to apply the port's
            // default. Inert for waking — page-local delivery wakes on every
            // arrival — but the envelope carries the field, and it should say what
            // the operator declared rather than a hard-coded `Normal`.
            let out = out.expect("surface client: local publish resolved an output binding");
            let channel = out.channel.clone();
            let urgency_sent = urgency.unwrap_or(out.urgency);
            return self.route_local_publish(
                PublishIntent {
                    correlation,
                    instance,
                    port,
                    body,
                    subject_instance,
                    urgency,
                    stamp,
                },
                channel,
                urgency_sent,
            );
        }
        // Correlations are handle-assigned and unique per connection; the core is
        // the authoritative layer, so a collision is a local bug, not a tolerable
        // race — panic rather than silently overwrite the routing entry (which
        // would misroute the first result and later fatal on the "unknown"
        // second one, blaming the server).
        let prev = self
            .pending_publishes
            .insert(correlation, (instance.clone(), port.clone()));
        assert!(
            prev.is_none(),
            "surface client: duplicate pending publish correlation {correlation}"
        );
        vec![Effect::SendFrame(ClientFrame::Publish {
            instance,
            port,
            body,
            correlation: Some(correlation),
            subject_instance,
            // Forwarded verbatim: `None` means "the port's default", and the
            // server holds the authoritative one. Substituting the binding's
            // advertised default here would put a stale value on the wire exactly
            // when the snapshot races a bindings change.
            urgency,
        })]
    }

    /// Whether the page-local router still routes. False once the core is
    /// terminal: `Fatal`/`ReloadRequired`/`Closed` have already answered every
    /// attached port `Closed`, so a fan-out then would push messages into queues
    /// that have delivered their terminal marker — and the kernel is quiescing
    /// toward a reload anyway. A local publish after that is answered
    /// `NotConnected` like any other, which is what keeps the terminal arm's
    /// "one result, no frame" contract intact.
    fn local_router_live(&self) -> bool {
        !matches!(
            self.state,
            State::Fatal | State::ReloadRequired | State::Closed
        )
    }

    /// Whether `(instance, port)` is the reserved error-report output port and the
    /// error-report floor is advertised (so the port is live this connection).
    fn is_error_report_port(&self, instance: &str, port: &str) -> bool {
        self.error_report_floor.is_some()
            && brenn_surface_contract::is_error_report_port(instance, port)
    }

    /// Release one refcount from a resolved port's channel and return the wire
    /// effect: an `Unsubscribe` when this was the last port on an `Active`
    /// channel, nothing otherwise. Shared by ordinary detach and the reconcile's
    /// force-detach — both drop exactly one attachment from a channel.
    ///
    /// The last reference off also **discards the subscription's mirror**. The
    /// mirror is a record of what this subscription delivered, in the same family
    /// of subscription state as the resume token dropped here: once no port is owed
    /// replay, the next attach is a fresh consumer, its `Subscribe` carries no
    /// resume, and the server replays the channel's retained tail. Keeping the old
    /// contents would let the store's dedup swallow that replay as
    /// re-presentation — the page would sit indefinitely on history it can no
    /// longer be told about, since the server is the catch-up authority on a
    /// transportable channel and its cursors are opaque to the page.
    fn release_channel_ref(&mut self, sub: SubKey) -> Vec<Effect> {
        // A confined port held no refcount and no `ChannelState` to release, and
        // there is no `Unsubscribe` to send — the channel keeps its store for the
        // page's life regardless of who is attached, so a later re-attach is primed
        // from it. Detaching is simply the removal from `attached` the caller
        // already did.
        if !channel_is_transportable(&sub.channel) {
            return Vec::new();
        }
        let cs = self
            .channels
            .get_mut(&sub)
            .expect("surface client: attached port's subscription state exists");
        // Refcount hit zero — ordinary detach, the last port off a still-
        // `Pending` re-subscribe (the refcount-zero-while-Pending edge), or a
        // durable force-detach: no port is owed replay, so `release_ref`
        // discards the resume token. A later fresh attach re-subscribes with
        // `resume: None` and receives the retained tail rather than resuming
        // past the latest value.
        if cs.release_ref() > 0 {
            return Vec::new();
        }
        let wire = cs.wire;
        if wire == WireState::Active {
            cs.wire = WireState::Unsubscribed;
        }
        // The mirror goes with the token, whatever the wire state was: all three
        // arms mean no port is owed replay on this subscription any more. Its
        // per-binding server-drop counts go with it, which is right — no binding
        // remains to report them to. Removal is idempotent: a `Welcome` that
        // un-declares a binding drops the store in `reconcile_stores` before the
        // force-detach releases the reference.
        self.stores.remove(&StoreKey::Wire(sub.clone()));
        match wire {
            WireState::Active => vec![Effect::SendFrame(ClientFrame::Unsubscribe {
                channel: sub.channel,
                instance: sub.instance,
            })],
            // Pending: defer — the SubscribeResult, arriving at refcount 0, sends
            // the deferred Unsubscribe. Unsubscribed: nothing.
            WireState::Pending | WireState::Unsubscribed => Vec::new(),
        }
    }

    /// A `SubscribeResult` arrived. It must be for a `Pending` channel (the
    /// server's FIFO writer orders the result before any replay, so a result for
    /// a non-`Pending` channel is inexplicable ⇒ fatal). `Ok` activates the
    /// channel — unless every reference was released while the `Subscribe` was in
    /// flight (refcount 0), in which case the deferred `Unsubscribe` is sent now.
    /// `Ok` is the only outcome: every config-bound subscribe class is supported,
    /// so a subscribe the server cannot honour is a violation that kills the
    /// connection, never a `SubscribeResult`.
    ///
    /// A `gap` on the result means replay could not cover the requested resume
    /// point (epoch change, a hole past the retained ring, or a durable resume
    /// beyond the retained window). It is a resume-layer fact and stops here: the
    /// kernel's answer is the re-resume it already performed, and the component
    /// sees at most a first-window-after-resubscribe, which the contract defines
    /// as unremarkable. The replayed `Deliver`s that follow flow through the
    /// normal path. `replay_count` is informational and left to the driver's
    /// logging, not tracked in the pure core.
    ///
    /// TODO(processor-typed-gaps): this classification exists only on the
    /// surface's resume layer. A wasmtime-hosted component gets no equivalent
    /// signal; backend adoption rides the next `processor.wit` world bump.
    fn on_subscribe_result(
        &mut self,
        sub: SubKey,
        outcome: SubscribeOutcome,
        _gap: Option<GapInfo>,
        now: Millis,
    ) -> Vec<Effect> {
        if self.channels.get(&sub).map(|cs| cs.wire) != Some(WireState::Pending) {
            return self.go_fatal(format!(
                "SubscribeResult for non-pending subscription: {} (instance {:?})",
                sub.channel, sub.instance
            ));
        }
        let mut effects = vec![Effect::SetWakeup(Some(self.arm_liveness(now)))];
        // A gap the server reports on a fresh or resumed subscribe is a real
        // staleness signal — a fresh attach receives the retained window with no
        // synthesized gap, so any gap here means the server could not cover a
        // resume point. It goes no further than this layer: the resubscribe that
        // carries the kernel past it has already happened, and there is no
        // component-visible gap vocabulary to fan it out to.
        match outcome {
            SubscribeOutcome::Ok => {
                // The subscription is acknowledged: the channel has now been
                // `Active` on this connection, even if the next line immediately
                // sends a deferred `Unsubscribe` (the momentary Active case).
                // This gates the Deliver straggler/never-active rule.
                let cs = self.channels.get_mut(&sub).unwrap();
                cs.has_been_active = true;
                // A new activation span opens: re-arm the straggler diagnostic
                // so a fresh post-`Active` window reports again.
                cs.straggler_reported = false;
                if cs.refcount == 0 {
                    // Every port detached while Pending: send the deferred
                    // Unsubscribe now that the subscription is acknowledged.
                    cs.wire = WireState::Unsubscribed;
                    effects.push(Effect::SendFrame(ClientFrame::Unsubscribe {
                        channel: sub.channel,
                        instance: sub.instance,
                    }));
                } else {
                    cs.wire = WireState::Active;
                }
            }
        }
        effects
    }

    /// A `PublishResult` arrived. It must carry a `correlation` that matches a
    /// still-pending publish (the server tags every result with the correlation
    /// the client sent); a missing or unknown correlation is inexplicable ⇒
    /// fatal. Otherwise the matched publish is completed: its
    /// `(instance, port)` is recovered and the wire outcome surfaced as an
    /// [`Event::PublishResult`]. Resets liveness like any inbound text frame.
    ///
    /// A result for the reserved error-report port is swallowed: the outcome is
    /// consumed (liveness reset, pending entry cleared) but no `Event` is
    /// emitted. An error report is a fire-and-forget self-publish whose console
    /// copy the kernel already wrote before publishing, so surfacing its result
    /// would re-enter the kernel's non-`Ok` breadcrumb path and publish a fresh
    /// report about the failed report — a self-amplifying loop. Dropping the
    /// result closes that loop; the report's record survives in the console.
    fn on_publish_result(
        &mut self,
        correlation: Option<u64>,
        outcome: PublishOutcome,
        now: Millis,
    ) -> Vec<Effect> {
        let Some(correlation) = correlation else {
            return self.go_fatal("PublishResult with no correlation".to_string());
        };
        let Some((instance, port)) = self.pending_publishes.remove(&correlation) else {
            return self.go_fatal(format!(
                "PublishResult with unknown correlation: {correlation}"
            ));
        };
        let mut effects = vec![Effect::SetWakeup(Some(self.arm_liveness(now)))];
        if brenn_surface_contract::is_error_report_port(&instance, &port) {
            return effects;
        }
        effects.push(Effect::EmitEvent(Event::PublishResult {
            instance,
            port,
            correlation,
            status: publish_outcome_to_status(outcome),
        }));
        effects
    }

    /// Complete every outstanding publish with `ConnectionLost` on transport
    /// teardown. The map is drained (it is per-connection: no correlation
    /// survives a reconnect), and the events are ordered by correlation so the
    /// effect stream is deterministic. Non-`Active` states never hold pending
    /// publishes, so this is a no-op there.
    ///
    /// Reserved error-report correlations are drained but emit no event: a
    /// `ConnectionLost` for a report would re-enter the kernel's non-`Ok`
    /// breadcrumb path (across the async event channel, possibly after a
    /// reconnect) and publish a fresh report about the failed report — the
    /// self-amplifying loop the result-swallow closes. The kernel's pre-publish
    /// console copy is the durable record.
    fn fail_pending_publishes(&mut self) -> Vec<Effect> {
        let mut pending: Vec<(u64, (String, String))> = std::mem::take(&mut self.pending_publishes)
            .into_iter()
            .collect();
        pending.sort_by_key(|(correlation, _)| *correlation);
        pending
            .into_iter()
            .filter(|(_, (instance, port))| {
                !brenn_surface_contract::is_error_report_port(instance, port)
            })
            .map(|(correlation, (instance, port))| {
                Effect::EmitEvent(Event::PublishResult {
                    instance,
                    port,
                    correlation,
                    status: PublishStatus::ConnectionLost,
                })
            })
            .collect()
    }

    /// Enter the terminal `Fatal` state: close the transport, fail any
    /// outstanding publishes, surface `Fatal`, and disarm the timer.
    ///
    /// A fatal protocol error is a dying connection, so the core itself
    /// publishes no error report: a reserved-port publish would race the
    /// transport close. The `Fatal` event carries `detail` to the kernel, which
    /// consoles it (and best-effort error-reports it) as a diagnostic
    /// breadcrumb. The server observes the disconnect directly.
    fn go_fatal(&mut self, detail: String) -> Vec<Effect> {
        self.state = State::Fatal;
        let mut effects = vec![Effect::CloseTransport];
        effects.extend(self.fail_pending_publishes());
        effects.push(Effect::EmitEvent(Event::Fatal { detail }));
        effects.push(Effect::SetWakeup(None));
        effects
    }
}

// Host-run protocol-core conformance suite; excluded from wasm builds (it plays
// the server in pure sync Rust and needs the native-only `test_support`).
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

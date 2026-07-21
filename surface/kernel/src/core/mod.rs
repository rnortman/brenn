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
//! It also owns the **`local:` router**: page-local pub/sub whose sole source of
//! truth is this core. A `local:` channel has no wire state at all — no
//! `Subscribe`, no refcount, no resume token — because no server mediates it.
//! The router assigns `LocalPos { epoch, seq }`, retains a bounded per-channel
//! ring, and routes a publish into the subscribed instances' pending queues
//! synchronously, so local delivery keeps working with the link down. See
//! [`LocalRing`].
//!
//! # Err consumes; retention is the recovery
//!
//! The messages an activation is assembled for are acked **at assembly**, on
//! both hostings. A failed activation is therefore never re-driven: returning
//! err or trapping discards the buffered publishes and nothing else, and the
//! messages that activation saw reappear only as retained context, in this or a
//! later window whose `retain_depth` still covers them. There is no gap-and-
//! replay choreography and no terminal port failure.
//!
//! Author rule: if you cannot afford to lose it on a failed activation, either
//! do not err after observing it, or give the port retention.
//!
//! # The loudness ladder
//!
//! The kernel is the single surface-side enforcement site for per-binding
//! overflow loudness. Drops from both queues in series — the kernel-side pending
//! queue and the server-side push window (reported on `Deliver` and folded in) —
//! surface as one per-binding drop delta at window assembly, and that delta is
//! the ladder's only input. Rungs are cumulative: `silent` does nothing beyond
//! the existing `dropped` accounting; `metered` adds kernel-internal per-binding
//! lifetime counters; `alarm` adds an `Alert` frame and a coalesced
//! `local:brenn/toast` (one per binding per activation); `fatal` adds the kill,
//! taking the same trap-terminal path an entry's own trap takes. Noise governs
//! loudness only — it never changes what happens to the data, which is always
//! the delivery class's own overflow behaviour.
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
use brenn_surface_contract::{Activation, ActivationError, PortWindow};
use brenn_surface_proto::{
    AlertSeverity, BatchEntry, CONTROL_PLANE_VERSION, ClientFrame, Cursor, DeliverTarget, GapInfo,
    InstanceReport, LOCAL_TAKEOVER_CHANNEL, LOCAL_TOAST_CHANNEL, LogLevel, MAX_ALERT_BODY_BYTES,
    MAX_ALERT_TITLE_BYTES, NoiseLevel, PublishBatchOutcome, PublishOutcome,
    RESERVED_LOCAL_CHANNELS, STALE_BUILD_CLOSE_CODE, ServerFrame, StatusCounters, SubscribeOutcome,
    SurfaceBindings, SurfaceDescription, TakeoverBody, ToastBody, ToastSeverity, ToastSource,
    reserved_local_channel,
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

mod activation;
mod publish_buffer;
mod util;

use activation::{LocalRing, ParkedBatch, PendingQueue, RegisteredInstance, RetainedRing};
/// Re-exported so the handle's `PublishGate` asks the same question the core's
/// wire paths do: a publish to a page-local port must not be pre-rejected as
/// `NotConnected` while the link is down.
pub(crate) use brenn_surface_proto::is_local_channel;
pub use publish_buffer::PublishBuffer;
use publish_buffer::{BufferedPublish, OutputSpec};
pub(crate) use util::truncate_report_field;
use util::*;

/// A monotonic timestamp in milliseconds, supplied by the driver on every
/// input. wasm32 has no working `std::time::Instant`, so the driver reads the
/// clock (`performance.now()` on wasm, `tokio::time::Instant` natively) and the
/// core only ever compares these values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Millis(pub u64);

impl Millis {
    fn saturating_add_ms(self, ms: u64) -> Millis {
        Millis(self.0.saturating_add(ms))
    }
}

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
    /// The armed timer fired.
    Tick,
    /// The armed outbox-retry timer fired: every instance whose outbox head is
    /// waiting on a refusal gets one more attempt.
    RetryTick,
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
}

/// How long the kernel waits before re-offering a refused outbox head.
///
/// A constant, not config. The server's backstop refill — 15s per publish by
/// default — is what decides when the head is admitted; this only decides how
/// promptly the kernel notices, and a 1s probe against that refill is idle-cheap
/// and costs nothing when no outbox is blocked (the timer is disarmed). A knob
/// here would be a number nobody can state a requirement for.
const RETRY_INTERVAL_MS: u64 = 1_000;

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
    /// attach subscribes with `resume: None` and receives the retained ring
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
    /// and receives the retained ring rather than resuming past the latest
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
    /// not the page's. Wire subscriptions only: a `local:` channel has no wire
    /// state and lives in [`Self::local_rings`] instead.
    channels: HashMap<SubKey, ChannelState>,
    /// The `local:` router's retained rings, keyed by channel address. Operator
    /// channels are rebuilt from `Welcome.bindings.local_channels` (their depths
    /// are resolvable nowhere else: `local:` channels have no `[[channel]]`
    /// block); the reserved `local:brenn/*` planes are seeded at construction at
    /// their contract-fixed depths and never rebuilt, because the contract, not
    /// a `Welcome`, is what declares them. Page-local state, so a reconnect
    /// preserves every existing ring — only a page reload clears them, and that
    /// mints a new `local_epoch` too.
    local_rings: HashMap<String, LocalRing>,
    /// Per-subscription retained rings for **wire** channels, keyed by the same
    /// [`SubKey`] the subscription table uses. Depth is the max fold over that
    /// instance's bindings' `retain_depth` on the channel (`reap_frontier`'s
    /// documented fold for capacities), and each binding reads its own depth back
    /// out of it.
    ///
    /// Fed by `on_deliver` for **every** subscription, uniformly — registered or
    /// not, and before any pending queue — because the ring is what makes a
    /// dropped message recoverable, and whether a component is activation- or
    /// dialect-delivered is not a fact about retention.
    ///
    /// `local:` channels are absent by construction: the router's own ring
    /// (`local_rings`) is their context source. A second ring on the same
    /// messages would be a mirror that could disagree with the router about what
    /// the page retained.
    wire_rings: HashMap<SubKey, RetainedRing>,
    /// Instances delivered by activation, keyed by instance id. Holds the pending
    /// queues, the scheduler flags, the sink carryover, and the parked flushes.
    /// Every `dom` and headless instance the page delivers to is in here — it is
    /// the only delivery model there is.
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
            // yet. Seeding them here is what "auto-bound by the kernel" means.
            local_rings: RESERVED_LOCAL_CHANNELS
                .iter()
                .map(|c| (c.address.to_string(), LocalRing::new(c.ring_depth)))
                .collect(),
            wire_rings: HashMap::new(),
            registered: HashMap::new(),
            local_epoch: config.local_epoch,
            participant_id: String::new(),
            jitter_rng: SplitMix64::new(config.backoff_jitter_seed),
        };
        let effects = core.begin_connect(now);
        (core, effects)
    }

    /// Feed one input at monotonic time `now`; returns the ordered effects.
    pub fn on_input(&mut self, input: Input, now: Millis) -> Vec<Effect> {
        match (self.state, input) {
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
        // events to a late chrome. Checked here so `reconcile_local_rings` can
        // hold reserved rings untouched: past this point a reserved entry agrees
        // with the ring already seeded at construction.
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
        // Before `reconcile_attached`, which may force-detach ports on local
        // channels this Welcome dropped, and before any attach resolves — a
        // ring must exist for every declared local channel before a port can
        // bind to it and replay it.
        self.reconcile_local_rings(&bindings);
        // The wire half of the same job, before anything can deliver into either.
        self.reconcile_wire_rings(&bindings);
        self.max_body_bytes = max_body_bytes;
        self.alert_granted = alert_granted;
        self.error_report_floor = error_report_floor;
        // Arm the liveness deadline in place of the now-satisfied handshake
        // deadline; any inbound text frame will push it out.
        let deadline = self.arm_liveness(now);
        let mut effects = vec![Effect::SetWakeup(Some(deadline))];
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

    /// Reconcile the `local:` router's rings against a just-received `Welcome`.
    ///
    /// Existing rings are **preserved across a reconnect**, contents and seq
    /// counter intact: the ring is page-local state and the page did not reload,
    /// so discarding it would manufacture a data loss the link never caused —
    /// exactly what `local:` exists to avoid (its whole point is that a dropped
    /// link does not interrupt page-local delivery). A channel absent from this
    /// `Welcome` is dropped: the operator un-declared it, and nothing may route
    /// on it again. An operator ring whose declared depth changed is re-trimmed
    /// in place.
    ///
    /// Reserved `local:brenn/*` planes are neither dropped nor retuned by any
    /// `Welcome`: the contract declares them and fixes their depths, so both
    /// halves below step over them. A `Welcome` that disagrees about a reserved
    /// depth never reaches here — [`Self::on_welcome`] fatals on it.
    ///
    /// Ports attached to a dropped local channel are force-detached by
    /// [`Self::reconcile_attached`], which runs off the same bindings.
    fn reconcile_local_rings(&mut self, bindings: &SurfaceBindings) {
        self.local_rings.retain(|channel, _| {
            // Reserved planes are the kernel's own, seeded at construction and
            // never dropped: they are contract-defined, so no `Welcome` declares
            // them into existence and none can un-declare them.
            reserved_local_channel(channel).is_some()
                || bindings
                    .local_channels
                    .iter()
                    .any(|lc| &lc.channel == channel)
        });
        for lc in &bindings.local_channels {
            // Reserved planes are left exactly as construction seeded them — the
            // other half of the retain rule above. A `Welcome` names one whenever
            // a component binds it, but the contract owns its depth, so there is
            // nothing here to apply: `on_welcome` already fataled on an entry
            // that disagreed, and re-applying an agreeing one would only make the
            // contract-fixed depth look server-supplied.
            if reserved_local_channel(&lc.channel).is_some() {
                continue;
            }
            self.local_rings
                .entry(lc.channel.clone())
                .or_insert_with(|| LocalRing::new(lc.ring_depth))
                .ring
                .set_depth(lc.ring_depth);
        }
    }

    /// Reconcile the per-subscription retained rings for wire channels against a
    /// just-received `Welcome`.
    ///
    /// Same rule as the `local:` rings, for the same reason: a ring is
    /// page-lifetime state, so a reconnect preserves it — discarding it would
    /// manufacture a loss the link never caused, and the ring is exactly what a
    /// post-reconnect window's context is read from. A subscription no binding
    /// names any more is dropped (nothing can route on it again); a surviving
    /// one is retuned in place if its fold changed.
    ///
    /// Depth is the **max** over that instance's bindings' `retain_depth` on the
    /// channel — the codebase's documented fold for capacities (`reap_frontier`).
    /// Two ports of one instance on one channel share one subscription and one
    /// ring, and each reads its own binding's depth back out of it, so the fold
    /// must cover the deepest reader.
    fn reconcile_wire_rings(&mut self, bindings: &SurfaceBindings) {
        let mut folded: HashMap<SubKey, u64> = HashMap::new();
        for b in &bindings.subscriptions {
            // `local:` bindings take no ring here: the router's per-channel ring
            // is their context source. A second ring over the same messages could
            // disagree with the router about what the page retained.
            if is_local_channel(&b.channel) {
                continue;
            }
            let key = SubKey::for_instance(&b.instance, &b.channel);
            let depth = folded.entry(key).or_insert(0);
            *depth = (*depth).max(b.retain_depth);
        }
        self.wire_rings.retain(|key, _| folded.contains_key(key));
        for (key, depth) in folded {
            match self.wire_rings.get_mut(&key) {
                Some(ring) => ring.set_depth(depth),
                None => {
                    self.wire_rings.insert(key, RetainedRing::new(depth));
                }
            }
        }
    }

    /// Rebuild every registered instance's pending queues and subscription
    /// references against `bindings`.
    ///
    /// Run at every `Welcome` and at each registration — the two moments either
    /// side of the relationship can change. It is idempotent, which is what lets
    /// both call it without either knowing about the other.
    ///
    /// Queues are per binding, so the binding table defines the set: a binding
    /// that vanished loses its queue (and the messages in it — nothing can
    /// deliver them to a port that no longer exists), a new one gains an empty
    /// queue, and a surviving one keeps its contents and its drop counters with
    /// its depth retuned. A registered instance whose bindings all vanish simply
    /// stops being activated; it is not failed and not deregistered — the
    /// operator un-wired it, which is not the component's fault.
    ///
    /// Depth-0 bindings get no queue — that is the mechanism of "never activates
    /// me", not an optimization — but they *do* take a subscription reference:
    /// they still see their channel.
    ///
    /// References are diffed rather than dropped-and-retaken. Releasing a
    /// surviving reference to zero would discard the subscription's resume token
    /// (that is what refcount zero means), so a reconnect would re-subscribe from
    /// scratch and re-replay the retained window — manufacturing exactly the
    /// duplicate delivery the ring's dedup exists to prevent.
    fn reconcile_registered(&mut self, bindings: &SurfaceBindings) -> Vec<Effect> {
        let mut instances: Vec<String> = self.registered.keys().cloned().collect();
        instances.sort();
        let mut release: Vec<SubKey> = Vec::new();
        let mut acquire: Vec<SubKey> = Vec::new();
        for instance in &instances {
            let mut queues: HashMap<String, usize> = HashMap::new();
            let mut wanted_subs: Vec<String> = Vec::new();
            for b in bindings
                .subscriptions
                .iter()
                .filter(|b| b.instance == *instance)
            {
                if !is_local_channel(&b.channel) {
                    wanted_subs.push(b.channel.clone());
                }
                if b.push_depth == 0 {
                    continue;
                }
                let capacity = usize::try_from(b.push_depth).expect(
                    "surface client: on_welcome proves every binding's push_depth fits a usize",
                );
                queues.insert(b.port.clone(), capacity);
            }
            let reg = self
                .registered
                .get_mut(instance)
                .expect("surface client: instance from this map");
            reg.queues.retain(|port, _| queues.contains_key(port));
            for (port, capacity) in queues {
                match reg.queues.get_mut(&port) {
                    Some(queue) => queue.set_capacity(capacity),
                    None => {
                        reg.queues.insert(port, PendingQueue::new(capacity));
                    }
                }
            }
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
    /// Its pending queues go with it (nothing will consume them). Its rings do
    /// not: rings belong to the subscription, not to the entry, and a re-register
    /// reads the same retained history a reconnect would have kept.
    ///
    /// Deregistering an unregistered instance is a caller bug and panics, exactly
    /// as detaching an unknown port is.
    fn on_activation_deregistered(&mut self, instance: &str) -> Vec<Effect> {
        let reg = self.registered.remove(instance).unwrap_or_else(|| {
            panic!("surface client: deregistration of unregistered instance {instance:?}")
        });
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
    pub fn take_ready_activation(&mut self) -> Option<ReadyActivation> {
        let mut ready: Vec<&String> = self
            .registered
            .iter()
            .filter(|(_, reg)| reg.ready())
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
        Some(self.dispatch_activation(instance))
    }

    /// Whether any instance could be dispatched right now — [`Self::
    /// take_ready_activation`] without the dispatch.
    ///
    /// The driver's select loop asks this so a component that re-readies itself
    /// synchronously (a `local:` publish onto a channel it reads) is dispatched
    /// from the loop, one activation per turn, instead of inside an unbounded
    /// drain that would never return to the transport.
    pub fn has_ready_activation(&self) -> bool {
        self.registered.values().any(|reg| reg.ready())
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

    /// Whether an instance is terminal (its activation entry trapped, or a
    /// `fatal`-rung binding overflowed). The driver consults this after a
    /// ready activation's loud-rung effects run: a killed instance is not
    /// invoked, and its assembled buffer is discarded.
    pub fn is_failed(&self, instance: &str) -> bool {
        self.registered.get(instance).is_some_and(|reg| reg.failed)
    }

    /// Assemble one activation for a ready instance: ack, window, seed.
    fn dispatch_activation(&mut self, instance: String) -> ReadyActivation {
        // Only this instance's input bindings, and of those only what a window
        // needs. Lifting them out first is what lets the loop below borrow `self`
        // for the rings; cloning the whole table instead would make every
        // activation pay for every sibling component's config.
        let inputs: Vec<(String, String, u64, NoiseLevel)> = self
            .bindings
            .as_ref()
            .expect("surface client: a ready activation implies bindings")
            .subscriptions
            .iter()
            .filter(|b| b.instance == instance)
            .map(|b| (b.port.clone(), b.channel.clone(), b.retain_depth, b.noise))
            .collect();
        // 1. Ack every pending queue of the instance and snapshot each binding's
        //    drop delta. Ack-at-activation-start is backend parity, and it is
        //    what makes err/trap consume: the messages are gone from the queue
        //    before the entry ever sees them, and retention is their only
        //    recovery.
        let reg = self
            .registered
            .get_mut(&instance)
            .expect("surface client: dispatch of an unregistered instance");
        reg.in_flight = true;
        let mut acked: HashMap<String, (Vec<MessageEnvelope>, u64)> = HashMap::new();
        for (port, queue) in &mut reg.queues {
            acked.insert(port.clone(), queue.ack());
        }
        // 2. Window every bound input port, in config order, present or not:
        //    a port with nothing new is a pure-context window, and a component
        //    must be able to read every port's view on every activation.
        let mut ports = Vec::new();
        // The loud rungs, applied after the window loop: `context_for` borrows
        // `self` immutably, so both the `metered` counter mutation and the `fatal`
        // kill (which mutate `self.registered`) cannot ride inside the loop. The
        // `alarm` alert/toast effects read nothing on `self`, so they accumulate
        // freely here and are handed out on the `ReadyActivation`.
        let mut metered: Vec<(String, u64)> = Vec::new();
        let mut loud_effects: Vec<Effect> = Vec::new();
        let mut fatal_reason: Option<String> = None;
        for (port, channel, retain_depth, noise) in inputs {
            let (new, dropped) = acked.remove(&port).unwrap_or_else(|| (Vec::new(), 0));
            // Every rung acts on the same input: the binding's drop delta this
            // activation, from either origin (both fold into the one ack delta).
            // The ladder is cumulative — a louder rung performs everything below
            // it. `Silent` does nothing beyond the honest `dropped` accounting.
            if dropped > 0 {
                if noise >= NoiseLevel::Metered {
                    metered.push((port.clone(), dropped));
                }
                if noise >= NoiseLevel::Alarm {
                    // One coalesced backend alert + toast per binding per
                    // activation; the delta rides the text.
                    loud_effects.extend(loud_drop_effects(&instance, &channel, &port, dropped));
                }
                if noise >= NoiseLevel::Fatal && fatal_reason.is_none() {
                    fatal_reason = Some(format!(
                        "input overflow: {dropped} message(s) dropped on port {port:?} \
                         ({channel}) — binding noise is fatal"
                    ));
                }
            }
            let context = self.context_for(&instance, &channel, retain_depth, &new);
            let new_from = u32::try_from(context.len())
                .expect("surface client: context depth is a config-bounded page-memory value");
            let mut envelopes = context;
            envelopes.extend(new);
            ports.push(PortWindow {
                port,
                envelopes,
                new_from,
                dropped,
            });
        }
        // Apply the metered counts now that the immutable ring borrows are done.
        if !metered.is_empty() {
            let reg = self
                .registered
                .get_mut(&instance)
                .expect("surface client: dispatch of an unregistered instance");
            for (port, dropped) in metered {
                *reg.metered_drops.entry(port).or_insert(0) += dropped;
            }
        }
        // The `fatal` rung: the instance takes the trap-terminal path — the same
        // one an entry's own trap takes (delivery stops, pending queues and parked
        // flushes cleared, `InstanceFailed` for the kernel to error-card, report,
        // and republish `surface-state`). The reason names the binding and the
        // overflow. Its buffer, assembled just above, is discarded: the driver
        // sees `is_failed` and never invokes the entry, so nothing flushes.
        if let Some(reason) = fatal_reason {
            let reg = self
                .registered
                .get_mut(&instance)
                .expect("surface client: dispatch of an unregistered instance");
            reg.in_flight = false;
            reg.failed = true;
            reg.queues.clear();
            reg.parked.clear();
            loud_effects.push(Effect::EmitEvent(Event::InstanceFailed {
                instance: instance.clone(),
                reason,
            }));
        }
        // 3. Seed the publish buffer: the entry gets inline quota answers without
        //    the driver re-entering the core mid-handler.
        let buffer = self.seed_buffer(&instance, &ports);
        ReadyActivation {
            instance,
            activation: Activation { ports },
            buffer,
            effects: loud_effects,
        }
    }

    /// One port's retained context: the subscription's ring at **this binding's
    /// own** depth, deduped by `message_id` against the port's new rows.
    ///
    /// The dedup is what keeps `new_from` honest. A message that is both retained
    /// and newly delivered — the ordinary case, since the ring is fed by the same
    /// delivery that queued it — must appear once, after the boundary: it is new.
    /// Reporting it on both sides would tell the component it had already seen
    /// something it is being woken for.
    ///
    /// For `local:` channels the router's ring is the source (§4.6): the page has
    /// exactly one retained copy of page-local traffic, and it is the router's.
    fn context_for(
        &self,
        instance: &str,
        channel: &str,
        retain_depth: u64,
        new: &[MessageEnvelope],
    ) -> Vec<MessageEnvelope> {
        let ring = if is_local_channel(channel) {
            self.local_rings.get(channel).map(|r| &r.ring)
        } else {
            self.wire_rings
                .get(&SubKey::for_instance(instance, channel))
        };
        let Some(ring) = ring else {
            return Vec::new();
        };
        ring.recent(retain_depth)
            .filter(|(e, _)| !new.iter().any(|n| n.message_id == e.message_id))
            .map(|(e, _)| e.clone())
            .collect()
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
    fn seed_buffer(&self, instance: &str, ports: &[PortWindow]) -> PublishBuffer {
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
        PublishBuffer::new(outputs, sink_mt, self.max_body_bytes)
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
                let (entries, carry) = buffer.take();
                let reg = self.registered.get_mut(&instance).unwrap();
                reg.in_flight = false;
                reg.carry_mt = carry;
                self.flush(&instance, entries, stamps)
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
            ActivationOutcome::Trap(reason) => {
                let reg = self.registered.get_mut(&instance).unwrap();
                reg.in_flight = false;
                reg.failed = true;
                // Its queues die with it — nothing will ever consume them. Its
                // rings do not: they are per-subscription and page-lifetime, and
                // a failed instance never activates, so they are inert rather
                // than something to clean up. Its parked flushes die too: they
                // were produced by a component whose memory is now presumed
                // poisoned, and there is nobody left to answer for them.
                reg.queues.clear();
                reg.parked.clear();
                vec![Effect::EmitEvent(Event::InstanceFailed {
                    instance,
                    reason,
                })]
            }
        }
    }

    /// Flush one ok activation's buffer: `local:` entries through the router,
    /// wire entries as one `PublishBatch`.
    ///
    /// Both commit at this one point. Call order is preserved **within** each
    /// class — the router routes its entries in order, the frame carries its
    /// entries in order — but the two classes commit in different places (one in
    /// this page, one in the server), so their relative order is not guaranteed.
    /// That is contract text, not an implementation artifact.
    fn flush(
        &mut self,
        instance: &str,
        entries: Vec<BufferedPublish>,
        stamps: Vec<MessageStamp>,
    ) -> Vec<Effect> {
        assert_eq!(
            entries.len(),
            stamps.len(),
            "surface client: the driver stamps one envelope per buffered publish"
        );
        let mut effects = Vec::new();
        let mut wire: Vec<BatchEntry> = Vec::new();
        for (entry, stamp) in entries.into_iter().zip(stamps) {
            if is_local_channel(&entry.channel) {
                // The router commits it here and now: seq assigned, ring fed,
                // fan-out — synchronously, in call order, with no await between,
                // which is the single-router property `local:` rests on. It never
                // touches the wire, so a down link is no reason to delay it.
                let sender = self.local_sender(instance);
                effects.extend(self.mint_and_route_local(
                    &entry.channel,
                    sender,
                    entry.body,
                    stamp,
                    entry.urgency,
                ));
            } else {
                // The stamp is discarded, exactly as it is for a single wire
                // publish: the server mints the authoritative envelope. The raw
                // override rides the frame, not the resolved urgency — the
                // server holds the port's default and applies it, and echoing
                // back a possibly-stale advertised default would let the client
                // override the operator.
                wire.push(BatchEntry {
                    port: entry.port,
                    body: entry.body,
                    urgency: entry.urgency_override,
                });
            }
        }
        if !wire.is_empty() {
            effects.extend(self.send_or_park(instance, wire));
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
    fn send_or_park(&mut self, instance: &str, entries: Vec<BatchEntry>) -> Vec<Effect> {
        if self.wire_free_for(instance) {
            return vec![self.batch_frame(instance, entries)];
        }
        self.park_batch(instance, ParkedBatch { entries }, false)
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
        vec![self.batch_frame(instance, batch.entries)]
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
    fn batch_frame(&mut self, instance: &str, entries: Vec<BatchEntry>) -> Effect {
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
            },
        );
        Effect::SendFrame(ClientFrame::PublishBatch {
            instance: instance.to_string(),
            correlation,
            publishes: entries,
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
    /// buffered it. Batches that clear both gates stay queued.
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
                let survives = batch.entries.iter().all(|entry| {
                    bindings
                        .outputs
                        .iter()
                        .any(|b| b.instance == instance && b.port == entry.port)
                        && entry.body.len() as u64 <= max_body_bytes
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
    /// `EphemeralBus` does for `ephemeral:`, and the reason a `local:` publish
    /// never touches the wire.
    ///
    /// Seq assignment, ring append, and fan-out are one synchronous step with no
    /// await between them, so the ring and the delivered order can never diverge
    /// — the single-router property that buys `local:` its freedom from the echo
    /// and dual-position problems.
    ///
    /// The publish always succeeds: there is no server to reject it, no budget to
    /// exhaust (nothing leaves the page), and no connection to be down. Fan-out
    /// pushes into bounded per-port queues, where a slow port's overflow is
    /// drop-oldest-and-count, the one overflow policy every class runs — a
    /// per-port concern that never fails the publisher.
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
        // The takeover plane's payload carries a request/deny/release identity
        // that the consumer (chrome) trusts; overwrite it with the authenticated
        // publishing instance so a component cannot forge another's takeover.
        // Derived from the router's own port wiring, exactly like `sender` — the
        // component names only its port.
        let body = if channel == LOCAL_TAKEOVER_CHANNEL {
            inject_takeover_instance(body, &instance)
        } else {
            body
        };
        // Resolved before the ring is borrowed: both read `self`.
        let sender = self.local_sender(&instance);
        let mut effects = self.mint_and_route_local(&channel, sender, body, stamp, urgency);
        effects.push(Effect::EmitEvent(Event::PublishResult {
            instance,
            port,
            correlation,
            status: PublishStatus::Ok,
        }));
        effects
    }

    /// Publish one of the kernel's own reserved control planes
    /// ([`LOCAL_LINK_STATE_CHANNEL`] and friends): mint, retain, fan out.
    ///
    /// The kernel grain, not a component's: §7.1 defines exactly two identities
    /// on a surface, and these messages are the kernel acting on nobody's
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
    /// `Welcome`), and §6.1 gives that window to the kernel's own pre-chrome
    /// indicator rather than to this plane. The first post-`Welcome` transition
    /// publishes, and the depth-1 ring replays it to whatever attaches later.
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
        let sender = self.participant_id.clone();
        // Inert: urgency is wake economics and page-local delivery never parks.
        // Normal is the honest value — the kernel states no preference, and there
        // is no operator knob on a contract-defined plane to resolve one from.
        self.mint_and_route_local(&channel, sender, body, stamp, Urgency::Normal)
    }

    /// Mint a `local:` envelope, assign its position, retain it, and fan it out
    /// to every port bound to the channel.
    ///
    /// The whole of what the router does, shared by its two callers so the
    /// component grain and the kernel grain cannot drift in position assignment,
    /// retention, or fan-out. They differ in exactly one value — `sender` — which
    /// is the difference §7.1 says they have and no other.
    fn mint_and_route_local(
        &mut self,
        channel: &str,
        sender: String,
        body: String,
        stamp: MessageStamp,
        urgency: Urgency,
    ) -> Vec<Effect> {
        let source = self.participant_id.clone();
        let epoch = self.local_epoch;
        let channel = channel.to_string();

        let ring = self.local_rings.get_mut(&channel).expect(
            "surface client: every routable local channel has a ring (reserved planes \
                 seeded at construction, the rest proven by on_welcome)",
        );
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
            deliver_after: None,
            // The caller's override, else the port's configured default —
            // resolved by the caller, since this core is the router and no
            // server downstream will apply the default for it.
            //
            // Inert for delivery: urgency is wake economics — it decides whether
            // a parked row is worth waking a consumer for — and local delivery
            // never parks and never wakes anything (the fan-out below is
            // synchronous and unconditional). Carried honestly anyway: the field
            // exists on the envelope, so it should report what the sender and the
            // operator actually said rather than a hard-coded value a reader
            // would mistake for one of them.
            urgency,
            envelope_type: ChannelScheme::Local,
        };
        // The ring is the channel's retention and the context source for every
        // window assembled off it; the returned position is the router's own
        // ordering record, which nothing downstream of the ring reads.
        ring.ring.push(&envelope, epoch);
        // Bindings on this channel take the router's message into their pending
        // queues; the ring above is already their context source, so there is
        // nothing else for them here. No per-message effect — that batching *is*
        // the delivery model. Every instance bound to the channel, because a
        // `local:` channel has no per-instance subscription to scope a delivery
        // to: the router simply delivers to everyone bound.
        self.deliver_to_registered(&channel, None, &envelope, 0);
        Vec::new()
    }

    /// Push one delivered envelope, plus its share of any server-reported drops,
    /// into every activation-registered binding on `channel` — the batching
    /// point. No effect is emitted: the message sits in its pending queue until
    /// the driver next drains ready activations, which is what coalesces a turn's
    /// deliveries into one activation.
    ///
    /// `only_instance` scopes the delivery to one subscription's owner, which is
    /// the wire case: a `Deliver` belongs to `(channel, instance)`, and a sibling
    /// instance on the same channel has its own subscription and its own cursor.
    /// `None` is the `local:` router, where the channel *is* the unit — there is
    /// no subscription to own and no cursor to keep, so every bound instance gets
    /// it.
    ///
    /// `dropped` is the subscription-level count the server reported: every
    /// binding on that subscription takes the **full** count, because each of
    /// them missed those messages. Page-side evictions are different and are
    /// counted by the evicting queue alone.
    ///
    /// A depth-0 binding has no queue and is skipped: it never activates its
    /// instance and never carries new envelopes. The ring already fed it, as
    /// context — that is the whole of what depth 0 means. It takes no drop count
    /// either: ring displacement is retention, not push overflow.
    ///
    /// A `failed` instance is skipped too. Its queues are gone; its rings keep
    /// filling. Delivery stops without disturbing anything the subscription
    /// shares with a live sibling.
    fn deliver_to_registered(
        &mut self,
        channel: &str,
        only_instance: Option<&str>,
        envelope: &MessageEnvelope,
        dropped: u64,
    ) {
        // This is the busiest path the kernel has — every wire `Deliver` and every
        // `local:` publish — so it asks the cheap question before doing any work at
        // all. Nothing is registered until the components' elements mount.
        if self.registered.is_empty() {
            return;
        }
        let Some(bindings) = self.bindings.as_ref() else {
            return;
        };
        let registered = &self.registered;
        let targets: Vec<(String, String)> = bindings
            .subscriptions
            .iter()
            .filter(|b| b.channel == channel)
            .filter(|b| only_instance.is_none_or(|i| b.instance == i))
            // Filter before cloning: a binding of an instance that has not
            // registered yet (element not mounted) has no queue to push.
            .filter(|b| registered.contains_key(&b.instance))
            .map(|b| (b.instance.clone(), b.port.clone()))
            .collect();
        for (instance, port) in targets {
            if let Some(reg) = self.registered.get_mut(&instance)
                && !reg.failed
                && let Some(queue) = reg.queues.get_mut(&port)
            {
                queue.count_server_drops(dropped);
                queue.push(envelope.clone());
            }
        }
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
            ServerFrame::ReAnchor { channel, instance } => {
                self.on_re_anchor(SubKey { instance, channel }, now)
            }
            ServerFrame::PublishResult {
                correlation,
                outcome,
            } => self.on_publish_result(correlation, outcome, now),
            ServerFrame::PublishBatchResult {
                correlation,
                outcome,
            } => self.on_publish_batch_result(correlation, outcome, now),
        }
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
        let PendingBatch { instance, entries } = pending;
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
                effects.extend(self.park_batch(&instance, ParkedBatch { entries }, true));
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
            // retained ring owns its entries. Fan-out cost is per-subscription
            // page state, which the design keeps; what the consolidation removes
            // is paying that cost N times on the wire.
            effects.extend(self.on_deliver(sub, envelope.clone(), seq, cursor, dropped));
        }
        effects
    }

    /// One target of a `Deliver` frame. Route the envelope into the
    /// subscription's retained ring and pending queue, from which the next
    /// activation window is assembled.
    ///
    /// The caller has already established that the subscription has been
    /// `Active` on this connection. A subscription that *has* been `Active` but
    /// is not *currently* `Active` (`Unsubscribed` after an `Unsubscribe`, or
    /// `Pending` on an immediate re-subscribe) is the one tolerated race: the
    /// target is a previous-span straggler and is discarded entirely — no
    /// routing, no token/seq effect.
    ///
    /// `dropped > 0` (server-side ring overflow since this subscription's
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
        let epoch = self.local_epoch;
        // Feed the subscription's retained ring, for **every** subscription and
        // before any pending queue below. That independence is the recovery
        // model: a message the pending queue drops on overflow is still in the
        // ring, so it is still visible as context. The feed is idempotent by
        // `message_id` (`RetainedRing::push`) because several reconnect paths
        // legitimately re-deliver what the ring already holds.
        //
        // Into the subscription's retained ring first: the ring is the context
        // source every window assembled off this subscription reads, and it takes
        // the envelope before and independently of the pending queues below — that
        // ordering is what makes a message dropped from a queue still recoverable
        // as context.
        //
        // Every surface subscription is an instance's and is welcome-time seeded
        // with a ring. A ring-less subscription is a broken seeding invariant —
        // fail fast rather than silently deliver a message no window can ever show
        // as context.
        match self.wire_rings.get_mut(&sub) {
            Some(ring) => {
                ring.push(&envelope, epoch);
            }
            None => {
                return self.go_fatal(format!(
                    "wire Deliver on a ring-less subscription: {} (instance {:?})",
                    sub.channel, sub.instance
                ));
            }
        }
        // Then the pending queues of this subscription's owner — the batching
        // point for activation-delivered bindings.
        self.deliver_to_registered(&sub.channel, Some(&sub.instance), &envelope, dropped);
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
        let local = out.is_some_and(|b| is_local_channel(&b.channel)) && self.local_router_live();
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
            // default. Inert for delivery — page-local traffic never parks and
            // wakes nothing — but the envelope carries the field, and it should
            // say what the operator declared rather than a hard-coded `Normal`.
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
    fn release_channel_ref(&mut self, sub: SubKey) -> Vec<Effect> {
        // A local port held no refcount and no `ChannelState` to release, and
        // there is no `Unsubscribe` to send — the router keeps the ring for the
        // page's life regardless of who is attached, so a later re-attach replays
        // it. Detaching is simply the removal from `attached` the caller already
        // did.
        if is_local_channel(&sub.channel) {
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
        // `resume: None` and receives the retained ring rather than resuming
        // past the latest value.
        if cs.release_ref() > 0 {
            return Vec::new();
        }
        match cs.wire {
            WireState::Active => {
                cs.wire = WireState::Unsubscribed;
                vec![Effect::SendFrame(ClientFrame::Unsubscribe {
                    channel: sub.channel,
                    instance: sub.instance,
                })]
            }
            // Pending: defer — the SubscribeResult, arriving at refcount 0, sends
            // the deferred Unsubscribe. Unsubscribed: nothing.
            WireState::Pending | WireState::Unsubscribed => Vec::new(),
        }
    }

    /// The server asked for one subscription to be re-anchored: unsubscribe it
    /// and subscribe it again presenting the cursor this kernel holds.
    ///
    /// This is the reconnect path applied to a single subscription, disturbing
    /// no other principal. The stored cursor is echoed verbatim — the kernel
    /// never interprets it, here as anywhere — and the span high-water resets,
    /// the server restarting the span counter at 1 for the new subscription.
    /// Class-blind: nothing here reads the channel's wire class.
    ///
    /// Only an `Active` subscription is re-anchored. A `Pending` one already has
    /// a fresh span opening (that subscribe *is* the re-anchor); an
    /// `Unsubscribed` one is either torn down with the `Unsubscribe` crossing
    /// this frame in flight or awaiting reconcile's resubscribe at the next
    /// `Welcome`. All are benign crosses with a re-subscribe already accounted
    /// for, and doing nothing is correct in each. A subscription this kernel
    /// never held has no such explanation: a correct server only asks about
    /// subscriptions it acknowledged, so it is fatal.
    fn on_re_anchor(&mut self, sub: SubKey, now: Millis) -> Vec<Effect> {
        let Some(cs) = self.channels.get_mut(&sub) else {
            return self.go_fatal(format!(
                "ReAnchor for a subscription never held: {} (instance {:?})",
                sub.channel, sub.instance
            ));
        };
        if cs.wire != WireState::Active {
            return vec![Effect::SetWakeup(Some(self.arm_liveness(now)))];
        }
        let resume = cs.prepare_subscribe();
        let mut effects = vec![Effect::SetWakeup(Some(self.arm_liveness(now)))];
        effects.push(Effect::SendFrame(ClientFrame::Unsubscribe {
            channel: sub.channel.clone(),
            instance: sub.instance.clone(),
        }));
        effects.push(Effect::SendFrame(ClientFrame::Subscribe {
            channel: sub.channel,
            instance: sub.instance,
            resume,
        }));
        effects
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

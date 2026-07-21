//! The client's public front door: construction and the kernel-facing handle.
//!
//! [`new`] builds the three pieces the kernel holds — the [`ClientHandle`] it
//! drives, the [`EventStream`] it drains, and the [`Driver`] it spawns — wired
//! together by three bounded command channels and the event channel.
//! [`ClientHandle`] issues attach / detach / close commands over a bounded
//! *control* channel, panicking fail-fast if it is full (a kernel burst bug) or closed (the
//! driver is gone). Publishes ride a separate bounded *publish* channel so a
//! publish burst can never starve control: a full publish channel is answered
//! with a synchronous `Err(PublishReject::Busy)` (one broken instance must not
//! kill the surface), not a panic. Error reports ([`ClientHandle::report`]) —
//! whose producers are the (untrusted) surface components, not the kernel itself
//! — ride the same publish channel as ordinary publishes but fire-and-forget:
//! composed into a reserved-port publish and dropped on any rejection, so an
//! error-looping instance is contained exactly like a publish flood.
//! [`PublishGate`] is the handle-side snapshot
//! that rejects a doomed `publish` synchronously, before the command reaches the
//! driver or the wire, so a flooding instance cannot pressure the event stream;
//! the driver holds it as `Arc<Mutex<PublishGate>>` and refreshes it on each
//! connection-state transition.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use brenn_envelope::Urgency;
use futures_channel::mpsc;
use futures_util::Stream;

use serde::Serialize;

use crate::core::{CoreConfig, Event, PublishBuffer};
use crate::driver::Driver;
use crate::proto::{
    AlertSeverity, InstanceReport, LogLevel, MAX_LOG_MESSAGE_BYTES, MAX_LOG_SOURCE_BYTES,
    StatusCounters, SurfaceBindings,
};
use crate::transport::TransportConnector;
use brenn_surface_contract::Activation;
// Only the native entry names it: the wasm entry hands back an
// `ActivationOutcome`, which carries the err message itself.
#[cfg(not(target_arch = "wasm32"))]
use brenn_surface_contract::ActivationError;
use brenn_surface_contract::{ERROR_REPORT_INSTANCE, ERROR_REPORT_PORT};

/// The flat error-report body a surface publishes to the reserved
/// `#brenn`/`error-reports` port: the surface's own claims, honestly attributed
/// by the envelope sender the server binds. Opaque to the server, which
/// applies only the ordinary body cap.
#[derive(Serialize)]
struct ErrorReportBody<'a> {
    source: &'a str,
    message: &'a str,
    level: LogLevel,
}

/// EventStream capacity. Control-plane traffic is low-rate by
/// construction; an overflow is a kernel-not-draining bug the driver panics on.
const EVENT_CHANNEL_CAPACITY: usize = 256;
/// Control command channel capacity. A full channel means the kernel issued
/// an unbounded synchronous burst of attaches — a kernel bug the handle panics on.
/// Sized from the proto's per-surface subscription-binding bound so a
/// boot-accepted config's whole first-connect attach burst fits without panicking
/// (the two cannot drift: one bound, one home).
const CONTROL_CHANNEL_CAPACITY: usize = crate::proto::MAX_SURFACE_SUBSCRIPTION_BINDINGS;
/// Publish command channel capacity. A full channel means one instance out-ran
/// its own publishes; the handle answers `Busy` synchronously (containment), so
/// this is a backpressure bound, not a panic bound. Error reports
/// ([`ClientHandle::report`]) share this channel rather than an isolated one:
/// reports are ordinary durable publishes, so a report flood competes with
/// sibling publishes for these slots (each loser gets a synchronous `Busy`),
/// bounded per surface. Accepted so reports ride one publish path with one set of
/// server bounds; alerts keep their own channel because paging must not be
/// starved by a publish/report flood.
pub(crate) const PUBLISH_CHANNEL_CAPACITY: usize = 256;
/// Alert command channel capacity. Alerting is best-effort and its
/// producers include the (untrusted) components; a full channel silently drops
/// the alert, so a instance alert-loop can neither crash nor starve the client.
/// Smaller than the publish channel: alerts page a human and are the rarer event,
/// and the server rate-limits them tightly regardless.
pub(crate) const ALERT_CHANNEL_CAPACITY: usize = 16;
/// Telemetry command channel capacity. Geometry and status are platform
/// telemetry the kernel paces (debounced resize, a fixed status interval), so a
/// full channel silently drops the frame — best-effort like alerts, kept off the
/// fail-fast control plane so a resize storm can neither crash nor starve control.
/// Small: at most a handful can be in flight between driver polls.
pub(crate) const TELEMETRY_CHANNEL_CAPACITY: usize = 16;

/// Public construction config for the surface client.
///
/// `url` and `build_id` have no sensible default and must be set; the remaining
/// fields default to the values [`ClientConfig::default`] provides.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Bare `ws(s)://…/surface/<slug>/ws`, no query; the client appends `?build`.
    pub url: String,
    /// This build's id, sent as the `?build` query and matched against the
    /// server's; a mismatch closes the connection with the stale-build code.
    pub build_id: String,
    /// Initial reconnect backoff.
    pub initial_backoff: Duration,
    /// Reconnect backoff ceiling.
    pub max_backoff: Duration,
    /// Handshake timeout covering transport-open through `Welcome`-received.
    pub connect_timeout: Duration,
    /// Multiple of `heartbeat_secs` of inbound silence that marks the connection
    /// dead.
    pub liveness_multiplier: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            build_id: String::new(),
            initial_backoff: Duration::from_secs(3),
            max_backoff: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(15),
            liveness_multiplier: 3,
        }
    }
}

/// One instance's activation entry: the kernel calls this once per activation.
///
/// Synchronous by construction, not by convenience. The flush rule is "publishes
/// commit iff the handler returns ok", and a boundary you can `await` across is
/// not a boundary — the kernel would have to decide what a half-finished
/// activation's buffer means, and there is no honest answer. The backend's guest
/// call is synchronous for the same reason.
///
/// `Send` natively, not on wasm — the bound tracks the executor, not the seam.
/// The driver is `tokio::spawn`ed on a multi-threaded runtime natively, so
/// everything it holds must be `Send` (as the rest of it already is); on wasm it
/// is `spawn_local`ed on the single JS thread, where a DOM-touching closure can
/// never be `Send` and requiring it would forbid the Phase 2 SDK's entire
/// purpose. Same seam, two executors, one honest bound each.
#[cfg(not(target_arch = "wasm32"))]
pub type ActivationEntry =
    Box<dyn Fn(&Activation, &mut PublishBuffer) -> Result<(), ActivationError> + Send>;

/// The wasm build's entry. See the native definition above for the `Send` split.
///
/// Two further differences from the native shape, both forced by the JS boundary
/// this entry wraps:
///
/// - **No `&mut PublishBuffer` argument.** A `dom` component publishes by
///   dispatching [`brenn_surface_contract::PORT_PUBLISH`] from inside its entry;
///   that event surfaces on the kernel's root listener, a code path with no way to
///   reach a buffer sitting on the driver's stack. So the buffer moves into the
///   shared [`InFlightSlot`] for the duration of the call, and the kernel's route
///   borrows it from there.
/// - **`ActivationOutcome`, not `Result`.** The wrapper calls a JS function and
///   can see what it did: a returned string is an err, a *thrown* exception is a
///   trap. `catch_unwind` cannot observe a wasm panic, so this boundary is the
///   only place the two are distinguishable — collapsing them into `Result` would
///   throw the distinction away here and never get it back.
#[cfg(target_arch = "wasm32")]
pub type ActivationEntry = Box<dyn Fn(&Activation) -> crate::core::ActivationOutcome>;

/// The buffer of the activation currently on the stack, and whose it is.
///
/// Exists only so the kernel's `PORT_PUBLISH` route can tell a buffered publish
/// from a gesture one and reach the buffer for the former. Activations are
/// serialized per instance and synchronous on the one JS thread, so at most one
/// instance is ever mid-activation: a `PORT_PUBLISH` whose resolved instance
/// **is** this occupant is buffered; anything else is a gesture publish.
#[cfg(target_arch = "wasm32")]
pub struct InFlightPublish {
    /// The instance whose entry is on the stack.
    pub instance: String,
    /// That activation's buffer — the sole quota authority for the call.
    pub buffer: PublishBuffer,
}

/// The in-flight slot, shared between the driver (which installs the buffer for
/// exactly the duration of an entry invocation and takes it back on return) and
/// the handle (which the kernel's publish route asks).
///
/// `Rc<RefCell<…>>` and wasm-only: one JS thread, nothing to make `Send` for.
/// Borrow discipline is safe by construction — the entry is synchronous, and the
/// kernel's listener runs only via DOM dispatch from inside it, so the driver
/// never touches the cell while the entry is on the stack.
#[cfg(target_arch = "wasm32")]
pub(crate) type InFlightSlot = std::rc::Rc<std::cell::RefCell<Option<InFlightPublish>>>;

/// A command from the client handle to the driver, which resolves anything the
/// pure core cannot hold (an entry closure, a minted stamp) and then feeds it the
/// corresponding [`Command`].
pub(crate) enum HandleCommand {
    /// Publish one of the kernel's reserved `local:` control planes. Rides the
    /// control channel rather than the publish channel because that is what it
    /// is — kernel control-plane traffic, emitted on link and mount transitions,
    /// with the kernel as its only producer. The starvation argument that keeps
    /// component publishes off this channel does not apply to the party that
    /// owns it.
    PublishControl { channel: String, body: String },
    /// Register `instance`'s activation entry. The driver stores the callback and
    /// feeds the core `Input::ActivationRegistered`. Rides the control channel:
    /// registration is a mount-time lifecycle event, exactly like an attach, and
    /// takes the same fail-fast bound.
    RegisterActivation {
        instance: String,
        entry: ActivationEntry,
    },
    /// Withdraw `instance`'s activation entry.
    DeregisterActivation { instance: String },
    /// Orderly shutdown. The driver feeds the core `Command::Close`, executes the
    /// resulting teardown effects, and ends its run loop (no reconnect).
    Close,
}

/// A publish issued through the handle, carried on the dedicated publish channel
/// (kept off the control channel so a publish burst cannot starve control). The
/// driver feeds it to the core as `Command::Publish`; the `correlation` is
/// handle-assigned so `publish` can return it synchronously, and the core routes
/// the eventual `Event::PublishResult` back by it.
pub(crate) struct PublishCommand {
    pub correlation: u64,
    pub instance: String,
    pub port: String,
    pub body: String,
    /// The report subject for the reserved error-report port; `None` for every
    /// ordinary publish. See [`crate::proto::ClientFrame::Publish`].
    pub subject_instance: Option<String>,
    /// The caller's per-message urgency override; `None` ⇒ the port's configured
    /// default. See [`crate::proto::ClientFrame::Publish`].
    pub urgency: Option<Urgency>,
}

/// A best-effort `Alert` issued through the handle, carried on the dedicated
/// alert channel (kept off the control channel so
/// a instance alert-loop can neither crash nor starve the control plane). The
/// driver feeds it to the core as `Command::Alert`, which truncates the title
/// and body to the proto caps and sends the frame only while `Active` (silently
/// dropped otherwise). The grant is enforced server-side; a conforming kernel
/// only issues an alert on an alert-granted surface (`Welcome.alert_granted`).
pub(crate) struct AlertCommand {
    pub severity: AlertSeverity,
    pub title: String,
    pub body: String,
}

/// A best-effort telemetry frame issued through the handle, carried on the
/// dedicated telemetry channel (kept off the control channel so a resize storm
/// cannot starve control). The driver feeds it to the core as
/// `Command::SendGeometry`/`Command::SendStatus`, which sends the frame only while
/// `Active` (dropped otherwise).
pub(crate) enum TelemetryCommand {
    Geometry {
        width: u32,
        height: u32,
        device_pixel_ratio: f64,
    },
    Status {
        instances: Vec<InstanceReport>,
        uptime_secs: u64,
        counters: StatusCounters,
    },
}

/// The receiver ends the driver selects over: the control plane plus the
/// publish, alert, and telemetry command channels. Bundled into one value so
/// [`Driver::new`] stays under the argument-count bound as command channels are
/// added; the handle owns the matching sender ends.
pub(crate) struct DriverChannels {
    pub control_rx: mpsc::Receiver<HandleCommand>,
    pub publish_rx: mpsc::Receiver<PublishCommand>,
    pub alert_rx: mpsc::Receiver<AlertCommand>,
    pub telemetry_rx: mpsc::Receiver<TelemetryCommand>,
}

/// Build the client: the [`ClientHandle`] the kernel drives, the [`EventStream`]
/// it drains, and the [`Driver`] it spawns (`tokio::spawn` natively,
/// `spawn_local` on wasm). The driver connects on spawn.
pub fn new<C: TransportConnector>(
    config: ClientConfig,
    connector: C,
) -> (ClientHandle, EventStream, Driver<C>) {
    let ClientConfig {
        url,
        build_id,
        initial_backoff,
        max_backoff,
        connect_timeout,
        liveness_multiplier,
    } = config;
    // `url` and `build_id` have no sensible default; a caller that leaves them
    // empty (e.g. a bare `ClientConfig::default()`) would otherwise loop
    // connect-fail → backoff forever with only debug logs to show for it. Fail
    // fast at construction instead.
    assert!(!url.is_empty(), "surface client: ClientConfig.url is empty");
    assert!(
        !build_id.is_empty(),
        "surface client: ClientConfig.build_id is empty"
    );
    let core_config = CoreConfig {
        url,
        build_id,
        initial_backoff,
        max_backoff,
        connect_timeout,
        liveness_multiplier,
        // Seed the core's backoff jitter from per-target entropy, distinct per
        // client, so a fleet reconnecting in lockstep after a deploy restart
        // decorrelates its reconnects. `ClientConfig` deliberately carries no seed
        // field: a caller-facing seed would be filled with boilerplate and a
        // forgotten constant would silently reintroduce lockstep.
        backoff_jitter_seed: crate::transport::entropy::seed(),
        // Mint the page-load epoch here, at the same edge and for the same
        // reason as the jitter seed: the core reads no entropy itself. Not from
        // `transport::entropy` — that source is deliberately non-cryptographic
        // and documented as unfit for anything but load-spreading. `Uuid::new_v4`
        // reads the platform CSPRNG (`crypto.getRandomValues` on wasm via the
        // `js` feature), which is what a page-lifetime identifier should be. Like
        // the seed, deliberately absent from `ClientConfig`: a caller-facing
        // epoch invites a forgotten constant, and two pages sharing an epoch
        // would make their `Pos::Local`s indistinguishable.
        local_epoch: uuid::Uuid::new_v4(),
    };
    let (events_tx, events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let (control_tx, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
    let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_CHANNEL_CAPACITY);
    let (alert_tx, alert_rx) = mpsc::channel(ALERT_CHANNEL_CAPACITY);
    let (telemetry_tx, telemetry_rx) = mpsc::channel(TELEMETRY_CHANNEL_CAPACITY);
    // The publish gate is shared: the driver refreshes it on every connection
    // transition, the handle reads it to pre-validate a publish.
    let gate = Arc::new(Mutex::new(PublishGate::default()));
    // The in-flight slot is shared the same way: the driver fills it for the
    // duration of an entry call, the handle's buffered-publish route reads it.
    #[cfg(target_arch = "wasm32")]
    let in_flight: InFlightSlot = Default::default();
    let driver = Driver::new(
        core_config,
        connector,
        events_tx,
        DriverChannels {
            control_rx,
            publish_rx,
            alert_rx,
            telemetry_rx,
        },
        gate.clone(),
        #[cfg(target_arch = "wasm32")]
        in_flight.clone(),
    );
    let handle = ClientHandle {
        control_tx: Mutex::new(control_tx),
        publish_tx: Mutex::new(publish_tx),
        alert_tx: Mutex::new(alert_tx),
        telemetry_tx: Mutex::new(telemetry_tx),
        gate,
        #[cfg(target_arch = "wasm32")]
        in_flight,
        next_correlation: AtomicU64::new(0),
    };
    (handle, EventStream(events_rx), driver)
}

/// The kernel's handle to a running surface client. Issues commands to the driver
/// over a bounded control channel.
pub struct ClientHandle {
    /// One long-lived sender behind a mutex. A bounded `futures` mpsc sender is
    /// only reported `Full` once *its own* guaranteed slot is taken, so the
    /// sender must persist across calls for the full-channel fail-fast to fire —
    /// cloning a fresh sender per send would hand out a new guaranteed slot every
    /// time and the bound would never be reached. The mutex is uncontended on
    /// wasm's single thread.
    control_tx: Mutex<mpsc::Sender<HandleCommand>>,
    /// The publish channel sender, held behind a mutex for the same
    /// per-call-persistence reason as `control_tx` (a fresh clone would hand out
    /// a new guaranteed slot and the `Busy` bound would never be reached). A full
    /// publish channel is answered `Busy`, not panicked on.
    publish_tx: Mutex<mpsc::Sender<PublishCommand>>,
    /// The alert channel sender, held behind a mutex for the same
    /// per-call-persistence reason as `control_tx`. Best-effort, exactly like
    /// `log_tx`: a full or closed channel silently drops the alert so a
    /// instance alert-loop can neither crash nor starve the control plane.
    alert_tx: Mutex<mpsc::Sender<AlertCommand>>,
    /// The telemetry channel sender, held behind a mutex for the same
    /// per-call-persistence reason as `control_tx`. Best-effort, exactly like
    /// `alert_tx`: a full or closed channel silently drops the geometry/status
    /// frame so a resize storm can neither crash nor starve the control plane.
    telemetry_tx: Mutex<mpsc::Sender<TelemetryCommand>>,
    /// The handle-side publish pre-validation snapshot, shared with the driver
    /// (which refreshes it). Read synchronously in [`ClientHandle::publish`].
    gate: Arc<Mutex<PublishGate>>,
    /// The in-flight activation's buffer, shared with the driver. Read
    /// synchronously by [`ClientHandle::try_buffered_publish`].
    #[cfg(target_arch = "wasm32")]
    in_flight: InFlightSlot,
    /// The next publish correlation to assign; monotonic per handle. The core
    /// routes each `Event::PublishResult` back by it.
    next_correlation: AtomicU64,
}

impl ClientHandle {
    /// Register `instance`'s activation entry: the kernel invokes it once per
    /// activation with every bound input port windowed, buffers the publishes it
    /// makes, and flushes them atomically iff it returns `Ok`.
    ///
    /// This is the delivery seam. `entry` is called synchronously on the driver's
    /// task — wasm is single-threaded and the flush rule needs a return value, not
    /// a future — and invocations for one instance never overlap: anything
    /// delivered while it runs coalesces into the next activation.
    ///
    /// Contract for the entry:
    ///
    /// - **Return `Ok`** and every publish it buffered is flushed, in call order.
    /// - **Return `Err`** and the buffer is discarded, a failure is counted, and
    ///   the instance keeps running. The messages it was activated for are
    ///   consumed regardless — they were acked when the activation was assembled
    ///   — and reappear only as retained context.
    /// - **Panic** and the instance is terminal: the buffer is discarded and
    ///   nothing is delivered to it again. Never page death; a trap has exactly
    ///   one subject.
    ///
    /// A `dom` component reaches this by dispatching `ACTIVATION_REGISTER` from its
    /// element's first `connectedCallback`; the kernel resolves the instance from
    /// the element and forwards. Headless consumers call it directly.
    pub fn register_activation(&self, instance: &str, entry: ActivationEntry) {
        self.send(HandleCommand::RegisterActivation {
            instance: instance.to_owned(),
            entry,
        });
    }

    /// Withdraw `instance`'s activation entry. Its pending queues go with it; its
    /// subscription rings do not (they are the subscription's, not the entry's).
    ///
    /// Deregistering an instance that never registered is a caller bug and panics
    /// in the core, exactly as detaching an unknown port does. A registered
    /// instance that trapped is *not* deregistered — it is terminal, which is a
    /// different state: it keeps its identity and its counters, and simply never
    /// activates again.
    pub fn deregister_activation(&self, instance: &str) {
        self.send(HandleCommand::DeregisterActivation {
            instance: instance.to_owned(),
        });
    }

    /// Publish one of the kernel's reserved `local:` control planes — link
    /// state, surface state, toasts.
    ///
    /// **Kernel-only.** This is not a component-facing API and there is no
    /// component-facing route to it: a component reaches a control plane by
    /// declaring a binding and publishing on its own port, which is what makes
    /// the grant checkable at boot. Calling this with a channel that is not a
    /// kernel-publish-only reserved plane panics in the core.
    ///
    /// Takes no instance: these carry the bare `surface:<slug>` platform
    /// identity, because the kernel acts on nobody's behalf. Best-effort and
    /// unacknowledged — page-local delivery cannot fail, and a pre-`Welcome`
    /// publish is dropped for want of an identity (see `on_publish_control`).
    pub fn publish_control(&self, channel: &str, body: String) {
        self.send(HandleCommand::PublishControl {
            channel: channel.to_owned(),
            body,
        });
    }

    /// Publish `body` from the output `(instance, port)`. Returns `Ok(correlation)`
    /// once the frame is queued to the driver — the eventual server disposition
    /// arrives later as an `Event::PublishResult` carrying the same correlation.
    ///
    /// Rejection is synchronous and local: an unbound pair, an oversized body, or
    /// a disconnected client is caught by the handle's [`PublishGate`] snapshot and
    /// returned as `Err` without touching the wire, so a flooding instance pays
    /// its own cost. A full publish channel returns `Err(PublishReject::Busy)` — a
    /// synchronous publish burst out-ran its bounded channel; the burst is
    /// contained to the offending instance and never a panic (unlike the control
    /// channel, whose fill is a kernel bug). The gate can be a bindings-generation
    /// stale across a reconnect; a publish that slips past it is caught by the
    /// core's authoritative check and answered with an `Event::PublishResult`.
    pub fn publish(&self, instance: &str, port: &str, body: String) -> Result<u64, PublishReject> {
        // A component publishes on its own ports and never names a report
        // subject: the subject field belongs to the reserved error-report port,
        // which only `report` writes to. Keeping it off this signature is what
        // stops a component from spelling one at all.
        self.publish_inner(instance, port, body, None, None)
    }

    /// Publish at an explicit urgency, overriding the port's configured default
    /// for this one message.
    ///
    /// The counterpart of the backend guest's `publish-with-urgency`: same
    /// sender-intent-plus-configured-default model, so a component's publish
    /// semantics do not change with its hosting.
    pub fn publish_with_urgency(
        &self,
        instance: &str,
        port: &str,
        body: String,
        urgency: Urgency,
    ) -> Result<u64, PublishReject> {
        self.publish_inner(instance, port, body, None, Some(urgency))
    }

    /// The publish path [`ClientHandle::publish`],
    /// [`ClientHandle::publish_with_urgency`] and [`ClientHandle::report`]
    /// share, differing only in whether a report subject or an urgency override
    /// rides along.
    fn publish_inner(
        &self,
        instance: &str,
        port: &str,
        body: String,
        subject_instance: Option<String>,
        urgency: Option<Urgency>,
    ) -> Result<u64, PublishReject> {
        self.gate
            .lock()
            .expect("surface client: publish gate mutex poisoned")
            .check(instance, port, &body)?;
        let correlation = self.next_correlation.fetch_add(1, Ordering::Relaxed);
        let command = PublishCommand {
            correlation,
            instance: instance.to_owned(),
            port: port.to_owned(),
            body,
            subject_instance,
            urgency,
        };
        let mut publish_tx = self
            .publish_tx
            .lock()
            .expect("surface client: publish sender mutex poisoned");
        match publish_tx.try_send(command) {
            Ok(()) => Ok(correlation),
            // The instance out-ran its own publishes: contained backpressure, not
            // a panic. The assigned correlation is simply unused — correlations
            // need only be unique, and a gap is harmless.
            Err(err) if err.is_full() => Err(PublishReject::Busy),
            Err(_) => panic!("surface client: driver is gone (publish channel closed)"),
        }
    }

    /// Route a publish into the in-flight activation's buffer, if it belongs
    /// there.
    ///
    /// `Some(status)` — `instance` is the one whose entry is on the stack, so this
    /// is a **buffered** publish: it was offered to that activation's
    /// [`PublishBuffer`] (the sole quota authority for the call) and answered
    /// inline. Nothing reaches the router or the wire until the entry returns ok.
    ///
    /// `None` — no activation is in flight, or a different instance's is. That is
    /// a **gesture publish**: the caller takes the immediate path
    /// ([`publish`](Self::publish)), drawing the port's sink bucket with no refill
    /// event. Reachable for a component only by dispatching against another
    /// instance's host, which the kernel's mounted-target resolution already
    /// treats as the contract violation it is.
    ///
    /// wasm-only: `dom` components are the only callers that can be mid-activation
    /// and dispatch an event at the same time.
    ///
    /// TODO(buffered-publish-routing-test): the match / mismatch / no-flight
    /// routing here and the driver's slot install/take are wasm-only and have no
    /// direct test — the client crate has no browser-test harness yet. Covered
    /// behaviorally via component-support's fake kernel only.
    #[cfg(target_arch = "wasm32")]
    pub fn try_buffered_publish(
        &self,
        instance: &str,
        port: &str,
        body: &str,
        urgency: Option<Urgency>,
    ) -> Option<Result<(), brenn_surface_contract::PublishError>> {
        // A short synchronous borrow that calls out to nothing: `publish_inner`
        // on the buffer touches only the buffer. The driver cannot be holding
        // this cell — it installed the buffer and is blocked in the entry call
        // this dispatch came from.
        //
        // `body` is borrowed and only owned once the in-flight instance matches:
        // the common gesture publish (no activation in flight, or a different
        // instance's) returns `None` after the instance compare without paying
        // the body's allocation.
        let mut slot = self.in_flight.borrow_mut();
        let in_flight = slot.as_mut()?;
        if in_flight.instance != instance {
            return None;
        }
        let body = body.to_owned();
        Some(match urgency {
            Some(urgency) => in_flight.buffer.publish_with_urgency(port, body, urgency),
            None => in_flight.buffer.publish(port, body),
        })
    }

    /// Publish a surface error report at `level`, best-effort — the wire side of
    /// the kernel's log path. Composes the flat `{source, message, level}` body
    /// (the surface's own claims, honestly attributed by the envelope sender the
    /// server binds) and publishes it to the reserved `#brenn`/`error-reports`
    /// output port, but only when the report floor is advertised
    /// (`Welcome.error_report_floor == Some(floor)`) and `level >= floor`. Below
    /// the floor, or when no error channel is configured (`None`), nothing is
    /// published — the caller's console copy is the only sink, by design.
    ///
    /// Fire-and-forget: it rides the ordinary publish plumbing (gate pre-check,
    /// per-connection publish bucket, the surface send budget), so a full publish
    /// channel or a gate rejection silently drops the report rather than
    /// surfacing to the caller. The report's `Event::PublishResult` is swallowed
    /// by the core (it would otherwise re-enter the non-`Ok` breadcrumb path and
    /// publish a report about the failed report). Callers write the console copy
    /// before calling this, so a dropped report is never lost.
    ///
    /// `subject_instance` names the component the report is *about*: it becomes
    /// the report's sender sub-identity once the server admits it, so the report
    /// is attributed to that component and draws down that component's send
    /// budget rather than its neighbours'. `None` for the kernel's own breadcrumbs (no
    /// component subject exists — the report carries the bare surface identity).
    /// It must name a declared instance: the server validates it against the
    /// declared set and kills the connection on an unknown one, so it is never a
    /// free-form label. The body's `source` is the human-readable twin and stays
    /// untrusted detail; this field is the machine one.
    ///
    /// The `source`/`message` fields are truncated to the proto caps on UTF-8
    /// boundaries, so a conforming kernel's report never trips the server's body
    /// cap (the boot validator proves `max_body_bytes` clears the worst case).
    pub fn report(
        &self,
        level: LogLevel,
        source: &str,
        message: &str,
        subject_instance: Option<&str>,
    ) {
        let floor = self
            .gate
            .lock()
            .expect("surface client: publish gate mutex poisoned")
            .error_report_floor();
        let Some(floor) = floor else { return };
        if level < floor {
            return;
        }
        // Shared with the alert path's field truncation: same UTF-8-boundary snap
        // and `…[truncated]` marker, so a subscriber can tell a cut report from a
        // complete one. The headroom validator budgets the marker's bytes.
        let source = crate::core::truncate_report_field(source.to_owned(), MAX_LOG_SOURCE_BYTES);
        let message = crate::core::truncate_report_field(message.to_owned(), MAX_LOG_MESSAGE_BYTES);
        let body = ErrorReportBody {
            source: &source,
            message: &message,
            level,
        };
        let body = serde_json::to_string(&body).expect("error report body serializes to JSON");
        // Best-effort: an unbound reserved port (floor withdrawn on a stale gate),
        // a full publish channel (a component error-loop out-running the driver),
        // or a closed one (the driver is gone) all drop the report. The console
        // copy the caller wrote is the durable record; the result is swallowed by
        // the core regardless, so there is nothing to observe here.
        // No urgency override: the reserved error-report port takes its
        // configured default like any other output. How loudly a surface's
        // reports should wake anyone is the operator's call on that channel, not
        // a constant the client bakes in.
        let _ = self.publish_inner(
            ERROR_REPORT_INSTANCE,
            ERROR_REPORT_PORT,
            body,
            subject_instance.map(str::to_owned),
            None,
        );
    }

    /// Emit a client-side alert to page an operator, best-effort. The title and
    /// body are truncated to the proto caps by the core before they reach the
    /// wire, and the frame is sent only while the connection is `Active` —
    /// silently dropped otherwise, since alerting rides the same WS and there is
    /// no other sink when it is down. Fire-and-forget: no result and no
    /// synchronous rejection.
    ///
    /// Routed over a dedicated best-effort alert channel, not the control
    /// channel, for the same reason [`report`](Self::report) is fire-and-forget:
    /// its producers include the (untrusted) surface components, whose rate the
    /// kernel does not bound, so a full or closed channel silently drops the alert
    /// rather than panicking (alert keeps its own channel so a page-the-operator
    /// event is not starved by a publish/report flood). The
    /// server enforces the alert grant and rate-limits paging; a conforming kernel
    /// only calls this on an alert-granted surface (`Welcome.alert_granted`).
    ///
    /// Contract: callers MUST pre-gate on the surface's alert grant. An `alert`
    /// raised on an ungranted surface is dropped by the core (it would otherwise
    /// be a grant violation the server kills the session over) and leaves only a
    /// `warn!` breadcrumb — no frame, no result. Out-of-tree kernels built on this
    /// client are responsible for the same pre-gating the in-tree kernel does.
    pub fn alert(&self, severity: AlertSeverity, title: &str, body: &str) {
        let command = AlertCommand {
            severity,
            title: title.to_owned(),
            body: body.to_owned(),
        };
        let mut alert_tx = self
            .alert_tx
            .lock()
            .expect("surface client: alert sender mutex poisoned");
        // Best-effort: a full channel (a instance alert-loop
        // out-running the driver) or a closed one (the driver is gone) drops the
        // alert. It is already best-effort — dropped when the connection is not
        // `Active` — so dropping it under backpressure or teardown is the same
        // contract, and it keeps an alert flood off the fail-fast control plane.
        if let Ok(()) = alert_tx.try_send(command) {}
    }

    /// Report the browser viewport, best-effort. Routed over the dedicated
    /// telemetry channel (not the control channel), so a resize storm cannot
    /// starve attach/detach; the core sends the frame only while `Active`,
    /// dropping it otherwise. Fire-and-forget: a full or closed channel silently
    /// drops the frame, the same contract as [`alert`](Self::alert).
    pub fn send_geometry(&self, width: u32, height: u32, device_pixel_ratio: f64) {
        self.send_telemetry(TelemetryCommand::Geometry {
            width,
            height,
            device_pixel_ratio,
        });
    }

    /// Report a per-instance mount-status snapshot, best-effort. Same routing and
    /// best-effort contract as [`send_geometry`](Self::send_geometry): the
    /// dedicated telemetry channel, sent only while `Active`, silently dropped
    /// otherwise.
    pub fn send_status(
        &self,
        instances: Vec<InstanceReport>,
        uptime_secs: u64,
        counters: StatusCounters,
    ) {
        self.send_telemetry(TelemetryCommand::Status {
            instances,
            uptime_secs,
            counters,
        });
    }

    /// Queue a telemetry command, best-effort. A full channel (a resize/status
    /// burst out-running the driver) or a closed one (the driver is gone) drops
    /// the frame — telemetry is already best-effort (dropped when not `Active`),
    /// so dropping under backpressure or teardown is the same contract, and it
    /// keeps a telemetry flood off the fail-fast control plane.
    fn send_telemetry(&self, command: TelemetryCommand) {
        let mut telemetry_tx = self
            .telemetry_tx
            .lock()
            .expect("surface client: telemetry sender mutex poisoned");
        if let Ok(()) = telemetry_tx.try_send(command) {}
    }

    /// Shut the client down in an orderly way: the driver closes the transport,
    /// fails any outstanding publishes with `ConnectionLost`, and ends its run
    /// loop with no reconnect. For test teardown and page unload. Routed over the
    /// control channel with the same full/closed fail-fast as `attach`/`detach`.
    ///
    /// After the driver has wound down, the control channel is closed:
    /// `attach`/`detach`/`close` panic (driver gone). `publish` does not panic —
    /// the disconnected gate rejects it with `Err(NotConnected)` before it reaches
    /// the closed channel — and `report` is silently dropped (best-effort).
    pub fn close(&self) {
        self.send(HandleCommand::Close);
    }

    /// Send a command to the driver. A full control channel is a kernel bug (an
    /// unbounded synchronous burst); a closed one means the driver is gone. Both
    /// are unrecoverable and panic per the house fail-fast rules.
    fn send(&self, command: HandleCommand) {
        let mut control_tx = self
            .control_tx
            .lock()
            .expect("surface client: control sender mutex poisoned");
        match control_tx.try_send(command) {
            Ok(()) => {}
            Err(err) if err.is_full() => {
                panic!(
                    "surface client: control command channel full (kernel issued an unbounded synchronous burst)"
                )
            }
            Err(_) => panic!("surface client: driver is gone (control channel closed)"),
        }
    }
}

/// The control-plane event stream the kernel drains. A bounded receiver; the
/// driver panics if it overflows (a kernel-not-draining bug).
pub struct EventStream(mpsc::Receiver<Event>);

impl Stream for EventStream {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

/// The local reason a `publish` was rejected synchronously by the handle, before
/// any frame reached the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishReject {
    /// `(instance, port)` is not a bound output in the current bindings.
    UnboundPort,
    /// The body exceeds the connection's `max_body_bytes`.
    BodyTooLarge {
        /// The rejected body's length in bytes.
        len: u64,
        /// The connection's advertised cap.
        max: u64,
    },
    /// The connection is not `Active`.
    NotConnected,
    /// The publish command channel is full: a synchronous publish burst exceeded
    /// its capacity. Contained to the offending instance — one broken instance
    /// must not kill the surface.
    Busy,
}

impl From<crate::core::PublishCheckReject> for PublishReject {
    fn from(reject: crate::core::PublishCheckReject) -> Self {
        use crate::core::PublishCheckReject;
        match reject {
            PublishCheckReject::NotConnected => PublishReject::NotConnected,
            PublishCheckReject::UnboundPort => PublishReject::UnboundPort,
            PublishCheckReject::BodyTooLarge { len, max } => {
                PublishReject::BodyTooLarge { len, max }
            }
        }
    }
}

/// A handle-side snapshot of the publish pre-validation inputs: the current
/// `outputs` table, the connection's `max_body_bytes`, and whether the
/// connection is `Active`.
///
/// Shared as `Arc<Mutex<PublishGate>>` between the handle and the driver — the
/// driver refreshes it on `Welcome` and on connection-state transitions, the
/// handle reads it to pre-validate a publish. The snapshot can be one
/// bindings-generation stale across a reconnect (the driver updates it a beat
/// after the core reconciles); a publish that slips past a stale gate is caught
/// by the core's authoritative copy of the same checks and answered with a
/// `PublishResult`. Such a reject only lands when the new bindings no longer
/// bind the pair — a pair bound under both generations never rejects, so an
/// ordinary reconnect drops nothing. This is a fast local reject, never the
/// source of truth.
#[derive(Debug, Default)]
pub struct PublishGate {
    /// The bound output pairs and whether each is page-local, as a small `Vec`
    /// scanned linearly rather than a `HashSet`: `check` runs on every publish
    /// (the flood path the gate exists to keep cheap) and holds `&str`s, so a
    /// `HashSet` probe would allocate an owned `(String, String)` per call. The
    /// table is a handful of ports; a linear scan of borrowed keys is
    /// allocation-free and, at this size, faster.
    ///
    /// The locality flag is resolved once at `Welcome` rather than by re-parsing
    /// the channel address on every publish — the gate exists to be cheap. Only
    /// the flag is kept, not the address: locality is all this layer needs to
    /// know, and the core does the routing.
    outputs: Vec<(String, String, bool)>,
    max_body_bytes: u64,
    connected: bool,
    /// The surface error-report floor from the latest `Welcome`. `Some(floor)`
    /// means the reserved `#brenn`/`error-reports` output port is live (treated
    /// as bound by [`Self::check`]) and [`ClientHandle::report`] publishes at
    /// `floor` and above; `None` means no reserved port (reports stay
    /// console-only), so a publish to it is the ordinary unbound-port rejection.
    error_report_floor: Option<LogLevel>,
}

impl PublishGate {
    /// Pre-validate a publish of `body` to `(instance, port)` against the
    /// current snapshot. Delegates to `core::check_publish` so the predicate set
    /// and order (`NotConnected`, then `UnboundPort`, then `BodyTooLarge`) live in
    /// one place shared with the core's authoritative recheck. The reserved
    /// error-report port counts as bound whenever the floor is advertised.
    ///
    /// A publish to a page-local output is reachable regardless of `connected`:
    /// it never touches the wire, so the gate must not pre-reject it while the
    /// link is down — that would defeat `local:`'s offline correctness before the
    /// router ever saw the publish. The core re-decides authoritatively; this
    /// only has to avoid rejecting what the core would route.
    pub fn check(&self, instance: &str, port: &str, body: &str) -> Result<(), PublishReject> {
        // One scan answers both questions the checks below ask of the table —
        // "is this pair bound?" and "is it page-local?" — on the flood path this
        // gate exists to keep cheap.
        let found = self
            .outputs
            .iter()
            .find(|(c, p, _)| c == instance && p == port);
        let local = found.is_some_and(|(_, _, local)| *local);
        crate::core::check_publish(
            self.connected || local,
            || {
                found.is_some()
                    || (self.error_report_floor.is_some()
                        && brenn_surface_contract::is_error_report_port(instance, port))
            },
            body.len() as u64,
            self.max_body_bytes,
        )
        .map_err(Into::into)
    }

    /// The advertised error-report floor, read by [`ClientHandle::report`] to
    /// decide whether a report at a given level is published or kept
    /// console-only.
    pub fn error_report_floor(&self) -> Option<LogLevel> {
        self.error_report_floor
    }

    /// Refresh the snapshot from a just-received `Welcome`: rebuild the outputs
    /// table, store the connection's `max_body_bytes` and error-report floor, and
    /// mark it `Active`.
    pub fn on_welcome(
        &mut self,
        bindings: &SurfaceBindings,
        max_body_bytes: u64,
        error_report_floor: Option<LogLevel>,
    ) {
        self.outputs = bindings
            .outputs
            .iter()
            .map(|b| {
                (
                    b.instance.clone(),
                    b.port.clone(),
                    crate::core::is_local_channel(&b.channel),
                )
            })
            .collect();
        self.max_body_bytes = max_body_bytes;
        self.error_report_floor = error_report_floor;
        self.connected = true;
    }

    /// Mark the connection no longer `Active` (transport close, backoff, fatal).
    /// The outputs table and cap are left in place: the wire-bound entries become
    /// unreachable until the next `Welcome` replaces them wholesale, but the
    /// page-local ones stay reachable — a dropped link does not stop page-local
    /// delivery, and the router on the other side of this gate keeps routing.
    pub fn on_disconnected(&mut self) {
        self.connected = false;
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::proto::OutputBinding;

    fn output(instance: &str, port: &str) -> OutputBinding {
        OutputBinding {
            channel: format!("ephemeral:{instance}.{port}"),
            instance: instance.to_owned(),
            port: port.to_owned(),
            urgency: Urgency::Normal,
            fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
        }
    }

    /// An output bound to a page-local channel.
    fn local_output(instance: &str, port: &str) -> OutputBinding {
        OutputBinding {
            channel: "local:brenn/theme".to_owned(),
            instance: instance.to_owned(),
            port: port.to_owned(),
            urgency: Urgency::Normal,
            fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
        }
    }

    fn bindings(outputs: Vec<OutputBinding>) -> SurfaceBindings {
        SurfaceBindings {
            components: vec![],
            subscriptions: vec![],
            outputs,
            local_channels: vec![],
            chrome_instance: String::new(),
        }
    }

    #[test]
    fn fresh_gate_rejects_not_connected() {
        let gate = PublishGate::default();
        assert_eq!(
            gate.check("comp", "out", "hi"),
            Err(PublishReject::NotConnected)
        );
    }

    #[test]
    fn a_disconnected_gate_still_admits_a_page_local_publish() {
        // The gate must not pre-reject what the core would route: `local:`
        // traffic never touches the wire, so the link being down is no reason to
        // stop it. A `NotConnected` here would defeat the class's offline
        // correctness before the router ever saw the publish.
        let mut gate = PublishGate::default();
        gate.on_welcome(
            &bindings(vec![local_output("comp", "out"), output("comp", "wire")]),
            64,
            None,
        );
        gate.on_disconnected();
        assert_eq!(gate.check("comp", "out", "hi"), Ok(()));
        // Its wire-bound sibling on the same disconnected gate still fails — the
        // exemption is scoped to the local port, not to the gate.
        assert_eq!(
            gate.check("comp", "wire", "hi"),
            Err(PublishReject::NotConnected)
        );
    }

    #[test]
    fn a_disconnected_local_publish_is_still_checked_for_size_and_binding() {
        // Reachability is the only check locality relaxes.
        let mut gate = PublishGate::default();
        gate.on_welcome(&bindings(vec![local_output("comp", "out")]), 4, None);
        gate.on_disconnected();
        assert_eq!(
            gate.check("comp", "out", "way too long"),
            Err(PublishReject::BodyTooLarge { len: 12, max: 4 })
        );
        assert_eq!(
            gate.check("comp", "nope", "hi"),
            Err(PublishReject::NotConnected)
        );
    }

    #[test]
    fn not_connected_takes_priority_over_unbound_and_oversized() {
        // A gate that once saw bindings but is now disconnected rejects on the
        // connection check first, regardless of port/size.
        let mut gate = PublishGate::default();
        gate.on_welcome(&bindings(vec![output("comp", "out")]), 4, None);
        gate.on_disconnected();
        assert_eq!(
            gate.check("other", "nope", "way too long"),
            Err(PublishReject::NotConnected)
        );
    }

    #[test]
    fn connected_unbound_port_rejected() {
        let mut gate = PublishGate::default();
        gate.on_welcome(&bindings(vec![output("comp", "out")]), 64, None);
        assert_eq!(
            gate.check("comp", "other", "hi"),
            Err(PublishReject::UnboundPort)
        );
        assert_eq!(
            gate.check("other", "out", "hi"),
            Err(PublishReject::UnboundPort)
        );
    }

    #[test]
    fn connected_oversized_body_rejected_with_len_and_max() {
        let mut gate = PublishGate::default();
        gate.on_welcome(&bindings(vec![output("comp", "out")]), 4, None);
        assert_eq!(
            gate.check("comp", "out", "hello"),
            Err(PublishReject::BodyTooLarge { len: 5, max: 4 })
        );
    }

    #[test]
    fn body_exactly_at_cap_is_accepted() {
        let mut gate = PublishGate::default();
        gate.on_welcome(&bindings(vec![output("comp", "out")]), 4, None);
        assert_eq!(gate.check("comp", "out", "four"), Ok(()));
    }

    #[test]
    fn bound_port_within_cap_accepted() {
        let mut gate = PublishGate::default();
        gate.on_welcome(&bindings(vec![output("comp", "out")]), 64, None);
        assert_eq!(gate.check("comp", "out", "hi"), Ok(()));
    }

    #[test]
    fn reserved_error_report_port_bound_only_when_floor_advertised() {
        let mut gate = PublishGate::default();
        // No floor: the reserved port is unbound like any other pair.
        gate.on_welcome(&bindings(vec![output("comp", "out")]), 64, None);
        assert_eq!(
            gate.check("#brenn", "error-reports", "{}"),
            Err(PublishReject::UnboundPort)
        );
        // Floor advertised: the reserved port counts as bound.
        gate.on_welcome(
            &bindings(vec![output("comp", "out")]),
            64,
            Some(LogLevel::Warn),
        );
        assert_eq!(gate.check("#brenn", "error-reports", "{}"), Ok(()));
        assert_eq!(gate.error_report_floor(), Some(LogLevel::Warn));
    }

    #[test]
    fn welcome_replaces_outputs_wholesale() {
        let mut gate = PublishGate::default();
        gate.on_welcome(&bindings(vec![output("comp", "out")]), 64, None);
        assert_eq!(gate.check("comp", "out", "hi"), Ok(()));
        // A second Welcome without the old port makes it unbound.
        gate.on_welcome(&bindings(vec![output("comp", "other")]), 64, None);
        assert_eq!(
            gate.check("comp", "out", "hi"),
            Err(PublishReject::UnboundPort)
        );
        assert_eq!(gate.check("comp", "other", "hi"), Ok(()));
    }

    /// Executable proof of the server-side headroom derivation. The boot
    /// validator asserts `max_body_bytes >= SURFACE_ERROR_BODY_MAX_BYTES`
    /// (`6*(MAX_LOG_MESSAGE_BYTES + MAX_LOG_SOURCE_BYTES) + 256`) so a conforming
    /// report is never `BodyTooLarge` — but that bound is only a prose
    /// derivation. This builds the *real* composed body at its worst case (every
    /// field byte a `U+0001` control char, the maximum six-byte JSON escape, each
    /// field truncated to its proto cap exactly as `report` composes it) and
    /// proves it fits. If the marker, the body shape, or a field is ever changed
    /// so the derivation rots, this fails instead of silently stranding reports
    /// (an overflow on the reserved port is swallowed by the core, so it would
    /// vanish with only the console copy).
    #[test]
    fn worst_case_report_body_fits_under_headroom_bound() {
        // The same formula the server-side SURFACE_ERROR_BODY_MAX_BYTES equals;
        // both derive from these proto caps.
        let bound = 6 * (MAX_LOG_MESSAGE_BYTES + MAX_LOG_SOURCE_BYTES) + 256;
        // Sized to the cap exactly, so no cheap ASCII truncation marker displaces
        // the maximally-expanding control chars.
        let source = crate::core::truncate_report_field(
            "\u{1}".repeat(MAX_LOG_SOURCE_BYTES),
            MAX_LOG_SOURCE_BYTES,
        );
        let message = crate::core::truncate_report_field(
            "\u{1}".repeat(MAX_LOG_MESSAGE_BYTES),
            MAX_LOG_MESSAGE_BYTES,
        );
        for level in [
            LogLevel::Trace,
            LogLevel::Debug,
            LogLevel::Info,
            LogLevel::Warn,
            LogLevel::Error,
        ] {
            let body = ErrorReportBody {
                source: &source,
                message: &message,
                level,
            };
            let serialized =
                serde_json::to_string(&body).expect("error report body serializes to JSON");
            assert!(
                serialized.len() <= bound,
                "worst-case report body {} exceeds headroom bound {bound} (level {level:?})",
                serialized.len(),
            );
        }
    }
}

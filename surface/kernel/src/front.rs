//! The front door: what the platform half holds, and what it may ask for
//! without waiting.
//!
//! [`crate::turn`] says what happens to a page and [`crate::runner`] performs it,
//! but neither can be called from the DOM listener, the log path or the resize
//! observer that actually produce the asking. Those run on the platform half's
//! own stack, synchronously, at rates the kernel does not bound. So the seam
//! between them is a set of channels and a snapshot: [`SurfaceHandle`] composes
//! one command and hands it over, and [`SurfaceGate`] answers the questions a
//! publish can be refused for before it costs anything.
//!
//! # Four channels, three contracts
//!
//! - **Control** — mounts, unmounts, the kernel's own confined planes, the close.
//!   Kernel-produced and low-rate, so a full channel is a kernel bug and panics.
//! - **Publish** — one component's publish, answered later by
//!   [`crate::session::Event::PublishResult`]. A full channel is answered
//!   synchronously with [`PublishReject::Busy`]: one component out-running its own
//!   publishes must not kill the page.
//! - **Alert** and **telemetry** — best-effort. A full or closed channel drops the
//!   command silently, because both already drop when there is no attachment, and
//!   a paging event must not be starved by a publish flood.
//!
//! Error reports ride the publish channel: a report *is* an ordinary publish on
//! an ordinary channel, and giving it its own channel would give an error-looping
//! component a second budget to flood from.
//!
//! # Why there is a gate at all
//!
//! The page answers every publish authoritatively, and it will refuse an unbound
//! port, an oversized body and an unconfigured attachment whether or not this
//! layer looks first. The gate exists so a component flooding refusals pays for
//! them on its own stack instead of filling the publish channel and the event
//! sink with doomed traffic. It is a snapshot, re-taken from the page rather than
//! composed here, and it is never the source of truth: a publish that slips past
//! a stale one is refused by the page and answered like any other.
//!
//! # The fifth seam: the in-flight buffer
//!
//! A `dom` component publishes by dispatching an event, which surfaces on the
//! kernel's root listener — a stack with no way to reach the buffer the runner is
//! holding across the entry call. So the buffer is shared through
//! [`InFlightSlot`]: the runner fills it for exactly the duration of an
//! invocation, and [`SurfaceHandle::try_buffered_publish`] and its siblings route
//! into it. Wasm-only, for the same reason: nothing else can be mid-activation
//! and dispatching an event at the same time.

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use brenn_attach_proto::AlertSeverity;
use brenn_envelope::Urgency;
use brenn_surface_schema::{InstanceReport, LogLevel, MAX_SURFACE_COMPONENTS, StatusCounters};
use futures_channel::mpsc;
use futures_util::Stream;
use serde_json::Number;

use crate::core::{PublishCheckReject, channel_is_transportable, check_publish};
use crate::page::SurfacePage;
#[cfg(target_arch = "wasm32")]
use crate::publish_buffer::PublishBuffer;
use crate::runner::RunnerCommand;
use crate::session::Event;

/// The event sink's capacity. This traffic is low-rate by construction — an
/// attachment's lifecycle, a component's fate, an answer to something asked for —
/// so a full sink is a platform half that stopped draining, which the runner
/// panics on.
pub const EVENT_CHANNEL_CAPACITY: usize = 256;

/// What the largest correct mount burst leaves room for beyond the mounts
/// themselves: the kernel's own control-plane statements in the same synchronous
/// stretch — one per reserved `local:brenn/*` plane, with room to spare — and the
/// close.
const CONTROL_BURST_HEADROOM: usize = 16;

/// The control channel's capacity, sized off the one per-surface bound the
/// operator's configuration carries.
///
/// A full control channel is a kernel bug and panics, so the number has to cover
/// the largest burst a *correct* kernel produces: every component of a
/// boot-accepted surface registering its activation entry in one synchronous
/// stretch, plus the headroom above. Assumes the peer boot-validates the
/// component count against the same shared bound; a config the peer accepted
/// must not out-run this channel.
pub const CONTROL_CHANNEL_CAPACITY: usize = MAX_SURFACE_COMPONENTS + CONTROL_BURST_HEADROOM;

/// The publish channel's capacity. A backpressure bound, not a panic bound: a
/// full channel is one component out-running its own publishes, and the answer is
/// a synchronous [`PublishReject::Busy`] to that component alone.
pub const PUBLISH_CHANNEL_CAPACITY: usize = 256;

/// The alert channel's capacity. Small on purpose: an alert pages a human, the
/// peer rate-limits paging tightly, and its own channel is what keeps it off the
/// publish backlog.
pub const ALERT_CHANNEL_CAPACITY: usize = 16;

/// The telemetry channel's capacity. The kernel paces both documents itself — a
/// debounced resize, a fixed status interval — so only a handful can be in flight
/// between turns.
pub const TELEMETRY_CHANNEL_CAPACITY: usize = 16;

/// One component's publish, on its way to the page.
///
/// The correlation is assigned here so [`SurfaceHandle::publish`] can hand it back
/// before the page has seen anything; the page mints its own wire correlation and
/// carries this one on the answer.
pub struct PublishCommand {
    pub correlation: u64,
    pub instance: String,
    pub port: String,
    pub body: String,
    /// The caller's per-message urgency override, or `None` for the port's
    /// configured default.
    pub urgency: Option<Urgency>,
}

/// One report from the kernel's log path, on its way to the page.
///
/// Rides the publish channel rather than the control channel: it is an ordinary
/// publish on an ordinary channel, and its producers include the components.
pub struct ReportCommand {
    pub level: LogLevel,
    pub source: String,
    pub message: String,
    /// The component the report is about, or `None` for the kernel's own
    /// breadcrumbs.
    pub subject: Option<String>,
}

/// What the publish channel carries: a component's publish, or a report.
pub enum PublishSlot {
    Publish(PublishCommand),
    Report(ReportCommand),
}

/// One alert, on its way to the page. Best-effort end to end.
pub struct AlertCommand {
    pub severity: AlertSeverity,
    pub title: String,
    pub body: String,
}

/// One of the two documents the surface writes about itself.
///
/// The viewport's device-pixel ratio is already a JSON number here: a reading that
/// is not one is not a reading, and refusing it at the boundary is what keeps the
/// page's input vocabulary totally comparable.
pub enum TelemetryCommand {
    Geometry {
        width: u32,
        height: u32,
        device_pixel_ratio: Number,
    },
    Status {
        instances: Vec<InstanceReport>,
        uptime_secs: u64,
        counters: StatusCounters,
    },
}

/// The buffer of the activation currently on the stack, and whose it is.
///
/// Exists only so the kernel's publish route can tell a buffered publish from a
/// gesture one and reach the buffer for the former. Activations are serialized
/// per instance and synchronous on the one JS thread, so at most one instance is
/// ever mid-activation: a publish whose resolved instance **is** this occupant is
/// buffered; anything else is a gesture publish.
#[cfg(target_arch = "wasm32")]
pub struct InFlightPublish {
    /// The instance whose entry is on the stack.
    pub instance: String,
    /// That activation's buffer — the sole quota authority for the call.
    pub buffer: PublishBuffer,
}

/// The in-flight slot, shared between the runner (which installs the buffer for
/// exactly the duration of an entry invocation and takes it back on return) and
/// the handle (which the kernel's publish route asks).
///
/// `Rc<RefCell<…>>` and wasm-only: one JS thread, nothing to make `Send` for.
/// Borrow discipline is safe by construction — the entry is synchronous, and the
/// kernel's listener runs only via DOM dispatch from inside it, so the runner
/// never touches the cell while the entry is on the stack.
#[cfg(target_arch = "wasm32")]
pub type InFlightSlot = std::rc::Rc<std::cell::RefCell<Option<InFlightPublish>>>;

/// The other side of the front door: everything the layer that owns the page
/// serves it through.
///
/// Bundled rather than passed one by one so a channel added later does not change
/// every constructor's shape. The gate rides along because it has two owners with
/// opposite jobs — the handle only reads it, this side only refreshes it — and
/// neither half is derivable from the other. The in-flight slot rides along for
/// the same reason, with the halves the other way round.
pub struct FrontChannels {
    pub events_tx: mpsc::Sender<Event>,
    pub control_rx: mpsc::Receiver<RunnerCommand>,
    pub publish_rx: mpsc::Receiver<PublishSlot>,
    pub alert_rx: mpsc::Receiver<AlertCommand>,
    pub telemetry_rx: mpsc::Receiver<TelemetryCommand>,
    #[cfg(target_arch = "wasm32")]
    pub in_flight: InFlightSlot,
    pub gate: Arc<Mutex<SurfaceGate>>,
}

/// Build the front door: the handle the platform half holds, the events it
/// drains, and everything the layer that owns the page serves them with.
pub fn new() -> (SurfaceHandle, EventStream, FrontChannels) {
    let (events_tx, events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let (control_tx, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
    let (publish_tx, publish_rx) = mpsc::channel(PUBLISH_CHANNEL_CAPACITY);
    let (alert_tx, alert_rx) = mpsc::channel(ALERT_CHANNEL_CAPACITY);
    let (telemetry_tx, telemetry_rx) = mpsc::channel(TELEMETRY_CHANNEL_CAPACITY);
    let gate = Arc::new(Mutex::new(SurfaceGate::default()));
    #[cfg(target_arch = "wasm32")]
    let in_flight: InFlightSlot = Default::default();
    let handle = SurfaceHandle {
        control_tx: Mutex::new(control_tx),
        publish_tx: Mutex::new(publish_tx),
        alert_tx: Mutex::new(alert_tx),
        telemetry_tx: Mutex::new(telemetry_tx),
        gate: gate.clone(),
        #[cfg(target_arch = "wasm32")]
        in_flight: in_flight.clone(),
        next_correlation: AtomicU64::new(0),
    };
    (
        handle,
        EventStream(events_rx),
        FrontChannels {
            events_tx,
            control_rx,
            publish_rx,
            alert_rx,
            telemetry_rx,
            #[cfg(target_arch = "wasm32")]
            in_flight,
            gate,
        },
    )
}

/// The platform half's handle on a running page.
///
/// Every sender is one long-lived value behind a mutex rather than a clone per
/// call. A bounded `futures` sender reports `Full` only once *its own* guaranteed
/// slot is taken, so a fresh clone per send would hand out a new slot every time
/// and neither the panic bound nor the [`Busy`](PublishReject::Busy) bound would
/// ever be reached. The mutex is uncontended on the one browser thread.
pub struct SurfaceHandle {
    control_tx: Mutex<mpsc::Sender<RunnerCommand>>,
    publish_tx: Mutex<mpsc::Sender<PublishSlot>>,
    alert_tx: Mutex<mpsc::Sender<AlertCommand>>,
    telemetry_tx: Mutex<mpsc::Sender<TelemetryCommand>>,
    /// The publish pre-check, refreshed by the layer that owns the page.
    gate: Arc<Mutex<SurfaceGate>>,
    /// The in-flight activation's buffer, filled by the layer that owns the page
    /// and read synchronously by [`try_buffered_publish`](Self::try_buffered_publish).
    #[cfg(target_arch = "wasm32")]
    in_flight: InFlightSlot,
    /// The caller-facing correlation space, monotone for the handle's life. Only
    /// uniqueness is asked of it, so an unused one — a publish the gate refused —
    /// costs nothing.
    next_correlation: AtomicU64,
}

impl SurfaceHandle {
    /// Register `instance`'s activation entry: the page invokes it once per
    /// activation with every bound input port windowed, buffers the publishes it
    /// makes, and flushes them atomically iff it returns ok.
    ///
    /// Admitted before the page has any wiring — a component can mount while the
    /// document is still in flight — and wired in by the reconcile that document
    /// runs.
    ///
    /// # Panics
    ///
    /// If the control channel is full (an unbounded synchronous burst) or closed
    /// (the run is over).
    pub fn register_activation(&self, instance: &str, entry: crate::handle::ActivationEntry) {
        self.control(RunnerCommand::RegisterActivation {
            instance: instance.to_owned(),
            entry,
        });
    }

    /// Withdraw `instance`'s activation entry. Its positions, its subscription
    /// references and its outbox go with it; the channels' stores do not.
    ///
    /// # Panics
    ///
    /// As [`register_activation`](Self::register_activation).
    pub fn deregister_activation(&self, instance: &str) {
        self.control(RunnerCommand::DeregisterActivation {
            instance: instance.to_owned(),
        });
    }

    /// State one of the kernel's own confined planes — link state, surface state,
    /// theme, a toast.
    ///
    /// **Kernel-only, and there is no component-facing route to it.** A component
    /// reaches a plane by declaring an output binding and publishing on its own
    /// port, which is what makes the grant checkable at boot. Naming a channel the
    /// kernel does not own panics inside the page's router.
    ///
    /// Unacknowledged: a confined append is the delivery, so there is nothing to
    /// wait for.
    ///
    /// # Panics
    ///
    /// As [`register_activation`](Self::register_activation).
    pub fn publish_control(&self, channel: &str, body: String) {
        self.control(RunnerCommand::PublishControl {
            channel: channel.to_owned(),
            body,
        });
    }

    /// Publish `body` from the output `(instance, port)`, at the port's configured
    /// urgency.
    ///
    /// `Ok(correlation)` means the publish is on its way; the disposition arrives
    /// later as [`Event::PublishResult`](crate::session::Event::PublishResult)
    /// carrying the same correlation. Every `Err` is answered here, on the
    /// caller's own stack, with nothing queued and nothing to wait for.
    pub fn publish(&self, instance: &str, port: &str, body: String) -> Result<u64, PublishReject> {
        self.queue_publish(instance, port, body, None)
    }

    /// Publish at an explicit urgency, overriding the port's configured default
    /// for this one message.
    ///
    /// The counterpart of the backend guest's `publish-with-urgency`: the same
    /// sender-intent-over-configured-default model, so a component's publish
    /// semantics do not change with where it is hosted.
    pub fn publish_with_urgency(
        &self,
        instance: &str,
        port: &str,
        body: String,
        urgency: Urgency,
    ) -> Result<u64, PublishReject> {
        self.queue_publish(instance, port, body, Some(urgency))
    }

    /// The path both publish entry points share: pre-check, take a correlation,
    /// queue.
    ///
    /// # Panics
    ///
    /// If the publish channel is closed — the run is over and the caller is
    /// holding a handle to nothing. A *full* channel is not a panic: that is the
    /// [`Busy`](PublishReject::Busy) answer.
    fn queue_publish(
        &self,
        instance: &str,
        port: &str,
        body: String,
        urgency: Option<Urgency>,
    ) -> Result<u64, PublishReject> {
        self.gate
            .lock()
            .expect("surface client: the publish gate mutex is poisoned")
            .check(instance, port, &body)?;
        let correlation = self.next_correlation.fetch_add(1, Ordering::Relaxed);
        let command = PublishSlot::Publish(PublishCommand {
            correlation,
            instance: instance.to_owned(),
            port: port.to_owned(),
            body,
            urgency,
        });
        match self.publish_sender().try_send(command) {
            Ok(()) => Ok(correlation),
            Err(err) if err.is_full() => Err(PublishReject::Busy),
            Err(_) => panic!("surface client: the run is over (publish channel closed)"),
        }
    }

    /// Report something at `level`, best-effort — the wire half of the kernel's
    /// log path.
    ///
    /// Published only when the wiring states an error-report floor and this report
    /// clears it; below the floor, or on a surface that declares no error channel,
    /// the caller's console copy is the only record, by design. `subject` names the
    /// component the report is *about* and becomes its sender sub-identity, so it
    /// draws down that component's budget rather than its neighbours'; `None` is
    /// the kernel's own breadcrumb.
    ///
    /// Fire-and-forget in every direction: a floor that has moved, a full publish
    /// channel or a finished run all drop it silently, and its own outcome is
    /// answered to nobody — a report about a failed report is the loop that
    /// swallowing closes.
    pub fn report(&self, level: LogLevel, source: &str, message: &str, subject: Option<&str>) {
        let floor = self
            .gate
            .lock()
            .expect("surface client: the publish gate mutex is poisoned")
            .error_report_floor();
        // No floor means the surface declares no error channel, so the page would
        // drop it: refusing here keeps a log-looping component off the publish
        // channel entirely.
        let Some(floor) = floor else { return };
        if level < floor {
            return;
        }
        let command = PublishSlot::Report(ReportCommand {
            level,
            source: source.to_owned(),
            message: message.to_owned(),
            subject: subject.map(str::to_owned),
        });
        let _ = self.publish_sender().try_send(command);
    }

    /// Page an operator, best-effort.
    ///
    /// **Callers must pre-gate on the attachment's alert grant**
    /// ([`Event::Connected`](crate::session::Event::Connected)'s `alert_granted`).
    /// An ungranted alert is dropped by the page with a breadcrumb — the peer
    /// would close the attachment over the frame — and one raised while detached
    /// is dropped silently, since the alert rides the same socket as everything
    /// else and there is no other sink for it.
    pub fn alert(&self, severity: AlertSeverity, title: &str, body: &str) {
        let command = AlertCommand {
            severity,
            title: title.to_owned(),
            body: body.to_owned(),
        };
        let _ = self
            .alert_tx
            .lock()
            .expect("surface client: the alert sender mutex is poisoned")
            .try_send(command);
    }

    /// Report the viewport, best-effort.
    ///
    /// A device-pixel ratio that is not a JSON number — infinite, or NaN — is not
    /// a reading of a physical display, so it is refused here rather than carried
    /// into a document. The page refuses implausible-but-representable readings
    /// itself; this is only the boundary that keeps the vocabulary comparable.
    pub fn send_geometry(&self, width: u32, height: u32, device_pixel_ratio: f64) {
        let Some(device_pixel_ratio) = Number::from_f64(device_pixel_ratio) else {
            tracing::warn!(
                device_pixel_ratio,
                "surface client: dropped a viewport reading whose device-pixel ratio is not a number"
            );
            return;
        };
        self.send_telemetry(TelemetryCommand::Geometry {
            width,
            height,
            device_pixel_ratio,
        });
    }

    /// Report the mount-status snapshot the platform half keeps, best-effort.
    ///
    /// The health summary and the overlay are deliberately not parameters: the
    /// page derives the first from its own wiring and records the second from its
    /// own overlay plane, so a reporter can assert neither.
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

    /// Shut the page down in an orderly way: the attachment closes, every caller
    /// awaiting a publish is answered, and nothing reconnects.
    ///
    /// # Panics
    ///
    /// As [`register_activation`](Self::register_activation).
    pub fn close(&self) {
        self.control(RunnerCommand::Close);
    }

    /// Route a publish into the in-flight activation's buffer, if it belongs
    /// there.
    ///
    /// `Some(result)` — the caller is the instance whose entry is on the stack, so
    /// this is a **buffered** publish: it was offered to that activation's buffer
    /// (the sole quota authority for the call) and answered inline. Nothing
    /// reaches the router or the wire until the entry returns ok.
    ///
    /// `None` — no activation is in flight, or a different instance's is. That is
    /// a **gesture publish**: the caller takes the immediate path
    /// ([`publish`](Self::publish)), drawing the port's sink bucket with no refill
    /// event. Reachable for a component only by dispatching against another
    /// instance's host, which the kernel's mounted-target resolution already
    /// treats as the contract violation it is.
    ///
    /// TODO(buffered-publish-routing-test): the match / mismatch / no-flight
    /// routing here and the runner's slot install/take are wasm-only and have no
    /// direct test — the browser suites drive the DOM seam, not the slot. Covered
    /// behaviorally via component-support's fake kernel only.
    #[cfg(target_arch = "wasm32")]
    pub fn try_buffered_publish(
        &self,
        instance: &str,
        port: &str,
        body: &str,
        urgency: Option<Urgency>,
    ) -> Option<Result<(), brenn_surface_contract::PublishError>> {
        // `body` is borrowed and only owned once the in-flight instance matches:
        // the common gesture publish (no activation in flight, or a different
        // instance's) returns `None` after the instance compare without paying
        // the body's allocation.
        self.with_in_flight(instance, |buffer| match urgency {
            Some(urgency) => buffer.publish_with_urgency(port, body.to_owned(), urgency),
            None => buffer.publish(port, body.to_owned()),
        })
    }

    /// Route a deferred publish into the in-flight activation's buffer, if it
    /// belongs there. `deliver_after` is epoch milliseconds UTC.
    ///
    /// Same routing rule and same `None` meaning as
    /// [`try_buffered_publish`](Self::try_buffered_publish): only the instance
    /// whose entry is on the stack can buffer, and there is no unbuffered fallback
    /// — a schedule laundered onto the gesture path would escape the flush-iff-ok
    /// rule that makes an err schedule nothing.
    #[cfg(target_arch = "wasm32")]
    pub fn try_buffered_publish_deferred(
        &self,
        instance: &str,
        port: &str,
        body: &str,
        deliver_after: u64,
    ) -> Option<Result<(), brenn_surface_contract::PublishError>> {
        self.with_in_flight(instance, |buffer| {
            buffer.publish_deferred(port, body.to_owned(), deliver_after)
        })
    }

    /// Route a cancel of one of this instance's parked messages into the in-flight
    /// activation's buffer, if it belongs there. `index` names the message by its
    /// position in the deferred window this activation delivered for `port`.
    #[cfg(target_arch = "wasm32")]
    pub fn try_buffered_defer_cancel(
        &self,
        instance: &str,
        port: &str,
        index: u32,
    ) -> Option<Result<(), brenn_surface_contract::DeferError>> {
        self.with_in_flight(instance, |buffer| buffer.defer_cancel(port, index))
    }

    /// Route an edit of one of this instance's parked messages into the in-flight
    /// activation's buffer, if it belongs there. `body` and `deliver_after` are
    /// each `Some` to change and `None` to leave alone.
    #[cfg(target_arch = "wasm32")]
    pub fn try_buffered_defer_edit(
        &self,
        instance: &str,
        port: &str,
        index: u32,
        body: Option<String>,
        deliver_after: Option<u64>,
    ) -> Option<Result<(), brenn_surface_contract::DeferError>> {
        self.with_in_flight(instance, |buffer| {
            buffer.defer_edit(port, index, body, deliver_after)
        })
    }

    /// Run `f` against the in-flight activation's buffer, but only when the
    /// activation on the stack is `instance`'s.
    ///
    /// A short synchronous borrow that calls out to nothing: every buffer method
    /// touches only the buffer. The runner cannot be holding this cell — it
    /// installed the buffer and is blocked in the entry call this dispatch came
    /// from.
    #[cfg(target_arch = "wasm32")]
    fn with_in_flight<R>(
        &self,
        instance: &str,
        f: impl FnOnce(&mut PublishBuffer) -> R,
    ) -> Option<R> {
        let mut slot = self.in_flight.borrow_mut();
        let in_flight = slot.as_mut()?;
        if in_flight.instance != instance {
            return None;
        }
        Some(f(&mut in_flight.buffer))
    }

    /// The publish sender, held across the call for the reason the struct's own
    /// doc gives.
    fn publish_sender(&self) -> std::sync::MutexGuard<'_, mpsc::Sender<PublishSlot>> {
        self.publish_tx
            .lock()
            .expect("surface client: the publish sender mutex is poisoned")
    }

    /// Queue a best-effort telemetry document. A full channel (a resize storm
    /// out-running the page) or a closed one (the run is over) drops it: the
    /// document is latest-wins, so the next tick or resize states a fresh one.
    fn send_telemetry(&self, command: TelemetryCommand) {
        let _ = self
            .telemetry_tx
            .lock()
            .expect("surface client: the telemetry sender mutex is poisoned")
            .try_send(command);
    }

    /// Queue a control command.
    ///
    /// # Panics
    ///
    /// If the channel is full — the kernel issued an unbounded synchronous burst
    /// — or closed, meaning the run is over. Both are unrecoverable: this plane
    /// carries the mounts every later delivery depends on, so a dropped command
    /// would leave a component that believes itself mounted receiving nothing.
    fn control(&self, command: RunnerCommand) {
        let mut control_tx = self
            .control_tx
            .lock()
            .expect("surface client: the control sender mutex is poisoned");
        match control_tx.try_send(command) {
            Ok(()) => {}
            Err(err) if err.is_full() => panic!(
                "surface client: the control channel is full (the kernel issued an unbounded \
                 synchronous burst)"
            ),
            Err(_) => panic!("surface client: the run is over (control channel closed)"),
        }
    }
}

/// The event stream the platform half drains.
///
/// Dropping it is how the platform half says it has left: the layer that owns the
/// page reads exactly that off its own sink and winds the run down.
pub struct EventStream(mpsc::Receiver<Event>);

impl Stream for EventStream {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

/// Why a publish was refused here rather than by the page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishReject {
    /// `(instance, port)` is not a bound output of the wiring in force.
    UnboundPort,
    /// The body is over the cap the most recent attachment stated.
    BodyTooLarge {
        /// The rejected body's length in bytes.
        len: u64,
        /// The attachment's advertised cap.
        max: u64,
    },
    /// A transportable port with no configured attachment to publish on.
    NotConnected,
    /// The publish channel is full: this component out-ran its own publishes.
    /// Contained to it — one component must not kill the page.
    Busy,
}

/// Spell the page's own refusal in the vocabulary a caller of this layer holds.
///
/// A free function rather than a `From` impl: the page's reject type is
/// crate-private and a trait impl over it would put a private type in this
/// module's public surface.
fn reject(reject: PublishCheckReject) -> PublishReject {
    match reject {
        PublishCheckReject::NotConnected => PublishReject::NotConnected,
        PublishCheckReject::UnboundPort => PublishReject::UnboundPort,
        PublishCheckReject::BodyTooLarge { len, max } => PublishReject::BodyTooLarge { len, max },
    }
}

/// The handle-side snapshot of everything a publish can be refused for.
///
/// Shared as `Arc<Mutex<SurfaceGate>>`: the handle reads it on every publish, the
/// layer that owns the page refreshes it from the page. Every field is the page's
/// own answer to the same question, taken wholesale in [`refresh`](Self::refresh)
/// rather than assembled here — two derivations of one authority would be two
/// things to keep in agreement.
///
/// It can be one turn stale against a page another task is driving. That is
/// inherent to a cross-thread snapshot and it is safe in one direction only: a
/// publish it wrongly *admits* is refused by the page and answered like any other,
/// where one it wrongly refuses is simply lost. So every predicate here is the
/// page's own, spelled the same way and in the same order.
#[derive(Debug, Default)]
pub struct SurfaceGate {
    /// Each bound output pair and whether its channel is confined, as a small
    /// `Vec` scanned linearly rather than a hashed set. `check` runs on the flood
    /// path this gate exists to keep cheap and holds borrowed keys; a hashed probe
    /// would allocate an owned pair per call, and the table is a handful of ports.
    ///
    /// Confinement is resolved once here rather than by re-parsing the address per
    /// publish. Only the answer is kept, not the address: the page does the
    /// routing.
    outputs: Vec<(String, String, bool)>,
    /// The publish body cap the most recent attachment stated. Retained across a
    /// detach exactly as the page retains it — a component's body-size contract
    /// must not change because the link dropped.
    body_cap: u64,
    /// Whether a configured attachment is in force. Not "attached": a page between
    /// phase 1 and phase 2 has an attachment but not the wiring its peer is
    /// judging against, and a publish composed out of the previous document is
    /// exactly what must not go out.
    configured: bool,
    /// The wiring's error-report floor, read by [`SurfaceHandle::report`]. `None`
    /// means the surface declares no error channel and publishes no reports.
    error_report_floor: Option<LogLevel>,
}

impl SurfaceGate {
    /// Pre-check a publish of `body` to `(instance, port)`.
    ///
    /// Delegates to the page's own [`check_publish`] so the predicate set and its
    /// order — reachable, then bound, then within the cap — have one spelling.
    ///
    /// A confined port is reachable whether or not the link is: its traffic never
    /// touches the wire, so refusing it while detached would defeat the offline
    /// correctness of the class before the page's router ever saw it.
    pub fn check(&self, instance: &str, port: &str, body: &str) -> Result<(), PublishReject> {
        // One scan answers both questions asked of the table — is this pair bound,
        // and is it confined — on the path this gate exists to keep cheap.
        let bound = self
            .outputs
            .iter()
            .find(|(i, p, _)| i == instance && p == port);
        let confined = bound.is_some_and(|(_, _, confined)| *confined);
        check_publish(
            self.configured || confined,
            || bound.is_some(),
            body.len() as u64,
            self.body_cap,
        )
        .map_err(reject)
    }

    /// The floor a report must clear to be published at all.
    pub fn error_report_floor(&self) -> Option<LogLevel> {
        self.error_report_floor
    }

    /// Re-take the snapshot from the page.
    ///
    /// The outputs table is read off the wiring *in force*, which outlives a
    /// detach, while reachability is read off the *configured* wiring, which does
    /// not — the same split the page's own publish path makes, so a page-local
    /// publish stays admitted through an outage and a wire publish does not.
    pub fn refresh(&mut self, page: &SurfacePage) {
        self.body_cap = page.body_cap;
        self.configured = page.connect.configured_bindings().is_some();
        let Some(bindings) = page.connect.bindings() else {
            self.outputs.clear();
            self.error_report_floor = None;
            return;
        };
        self.outputs.clear();
        self.outputs
            .extend(bindings.document().outputs.iter().map(|binding| {
                (
                    binding.instance.clone(),
                    binding.port.clone(),
                    !channel_is_transportable(&binding.channel),
                )
            }));
        self.error_report_floor = bindings.platform().error_report_floor;
    }
}

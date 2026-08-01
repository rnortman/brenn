//! What the platform half asks of a page.
//!
//! [`crate::inbound`] is the half a peer drives and [`crate::outward`] the half
//! the page drives itself. This is the third: a component published, the log path
//! has something to report, an operator should be paged, chrome's link-state
//! plane needs restating, the viewport moved, the status cadence came round, the
//! page is going away. Each is one [`Command`], and each is one pass over the
//! page that answers what it produced rather than enacting it — the same seam
//! every surface-side module answers across.
//!
//! # A port on the way in, a channel on the way out
//!
//! A component states a publish as `(instance, port)`; where that goes, with what
//! urgency and by which authority, is a lookup in the wiring. A port bound to a
//! confined channel commits here and now, through the page's own router, and its
//! caller is answered synchronously; a port bound to a transportable one composes
//! a frame and waits for the peer's answer. That split is why a publish carries an
//! envelope stamp it may never use: only this layer knows which side of it the
//! port falls on, and the layer that reads clocks and entropy has already gone by.
//!
//! # Everything but a publish is best-effort
//!
//! A report, an alert and a telemetry document are fire-and-forget: nobody is
//! awaiting them, and a page with no attachment, no wiring, or no grant drops them
//! rather than queueing them. The reporter's console copy, the next status tick and
//! the next resize are what make that affordable. A component's publish is the one
//! command that is always answered, refusal included.
//!
//! # The one thing a page must not publish around
//!
//! A status document that contradicts the wiring it was assembled from is this
//! build disagreeing with itself — the table, the counters and the wiring all
//! descend from one document — so it is the caller's fatal rather than something
//! to drop. A viewport reading outside the plausible range is the opposite: the
//! page misread a physical display, and the reading is refused so the retained
//! window keeps the last one that made sense.

#[cfg(test)]
mod tests;

use brenn_attach_client::publish::OutboxSteps;
use brenn_attach_client::router::{MessageStamp, Origin, RouteOutcome, RouteRequest};
use brenn_attach_proto::{AlertSeverity, ClientFrame, MAX_ALERT_BODY_BYTES, MAX_ALERT_TITLE_BYTES};
use brenn_envelope::Urgency;
use brenn_queue::CursorOverflow;
use brenn_surface_schema::{InstanceReport, LogLevel, StatusCounters};
use serde_json::Number;

use crate::activation::{DropVerdicts, Schedules};
use crate::bindings::AppliedBindings;
use crate::core::{PublishStatus, channel_is_transportable, check_publish, truncate_report_field};
use crate::flush::PlaneRefusal;
use crate::outbound::{
    ErrorReport, PortPublish, PublishAnswer, ResolvedOutput, TelemetryKind, resolve_output,
};
use crate::page::{Detached, SurfacePage};
use crate::registry::BindingKey;
use crate::telemetry::{self, StatusReport};

/// The urgency the kernel states on its own control planes.
///
/// Inert for waking — a confined append *is* the delivery, so every reader on the
/// channel is woken whatever this says — and stated honestly anyway: the kernel
/// has no preference, and a contract-defined plane carries no operator knob to
/// resolve one from.
const CONTROL_URGENCY: Urgency = Urgency::Normal;

/// Something the platform half asks the page to do.
///
/// `Eq`, like every other input: the one value in this vocabulary with no total
/// equality is the viewport's device-pixel ratio, and it arrives as the JSON
/// number the document carries rather than as a float.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// One component's publish on one of its own output ports, with the envelope
    /// identity the driver minted for it.
    ///
    /// The stamp is spent only when the port resolves to a confined channel,
    /// where this page is the router; a transportable publish discards it,
    /// because the peer mints the authoritative envelope. It is minted for both
    /// because only this layer holds the wiring that says which is which.
    Publish {
        publish: PortPublish,
        stamp: MessageStamp,
    },
    /// One error report from the kernel's log path.
    ///
    /// Whether it is published at all is the wiring's: a surface with no error
    /// channel publishes none, and one with a floor publishes only what reaches
    /// it.
    Report {
        level: LogLevel,
        /// The human-readable producer, e.g. `"component:<kind>"`. Untrusted
        /// detail beside the machine-readable [`subject`](Self::Report::subject).
        source: String,
        message: String,
        /// The component the report is *about*, which becomes its sender
        /// sub-identity; `None` for the kernel's own breadcrumbs.
        subject: Option<String>,
    },
    /// Page an operator. Best-effort and ungated at the caller: the page drops it
    /// when there is no attachment to send it on and when the attachment carries
    /// no alert grant.
    Alert {
        severity: AlertSeverity,
        title: String,
        body: String,
    },
    /// State one of the kernel's own confined control planes — link-state,
    /// surface-state, theme, toast.
    ///
    /// Carries no correlation: the kernel is not a component awaiting an answer,
    /// and no peer answers a page-local publish. The channel must be a plane the
    /// kernel owns; anything else is a page bug and panics inside the router.
    PublishControl {
        channel: String,
        body: String,
        stamp: MessageStamp,
    },
    /// The viewport, as the page's DOM half read it.
    ///
    /// The device-pixel ratio arrives as a JSON number because that is what the
    /// document carries it as: a reading that is not a JSON number is not a
    /// reading, and refusing it at the boundary is what keeps this vocabulary
    /// totally comparable.
    Geometry {
        width: u32,
        height: u32,
        device_pixel_ratio: Number,
    },
    /// The mount-status snapshot the platform half keeps.
    ///
    /// The health summary and the overlay are *not* carried: the page derives the
    /// first from its own wiring and records the second from its own overlay
    /// plane, so a reporter cannot assert either.
    Status {
        instances: Vec<InstanceReport>,
        uptime_secs: u64,
        counters: StatusCounters,
    },
    /// Orderly shutdown: the attachment closes, every caller awaiting a publish is
    /// answered, and nothing reconnects.
    Close,
}

/// A confined plane refused the body of one component's publish.
///
/// Unlike a refusal at flush, the publisher *is* told — its caller's answer is
/// `Refused`. This is the operator-facing half: a plane's status enum carries no
/// reason, and the reason is the only thing that says which rule was broken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedPublish {
    /// The publisher.
    pub instance: String,
    pub refusal: PlaneRefusal,
}

/// What one command produced.
#[derive(Debug, Default, PartialEq)]
pub struct CommandOutcome {
    /// Frames to send, in composition order.
    pub frames: Vec<ClientFrame>,
    /// Callers owed an answer: a publish refused before it went anywhere, one
    /// committed to a confined channel, or every outstanding one at a close.
    pub answers: Vec<PublishAnswer>,
    /// The loudness ladder's verdicts for positions a confined append evicted.
    pub drops: DropVerdicts,
    /// The one plane refusal a single publish can produce.
    pub refusal: Option<RefusedPublish>,
    /// The outboxes' answer to a close: the retry deadline disarmed. A queued
    /// flush is not sent — there is no wire left to send it on.
    pub steps: OutboxSteps<String>,
    /// Whether the page asked for the attachment to be closed.
    pub close: bool,
    /// A composition that contradicts the page's own state — the caller's fatal.
    pub fatal: Option<String>,
}

/// Run one command against the page.
///
/// Reads no clock: a command that mints an envelope carries the stamp the driver
/// read for it, and nothing else here resolves against time.
///
/// # Panics
///
/// If a component's confined publish reaches a page with no identity, or if one of
/// the page's own planes refuses a body the kernel wrote. Both are unreachable by
/// construction — a bound port implies a document, which implies an attachment
/// whose principal outlives it; and a plane the kernel may write has no rule
/// against what the kernel writes — and both would otherwise mint an
/// unattributable or silently discarded message.
pub fn on_command(page: &mut SurfacePage, command: Command) -> CommandOutcome {
    match command {
        Command::Publish { publish, stamp } => on_publish(page, publish, stamp),
        Command::Report {
            level,
            source,
            message,
            subject,
        } => on_report(page, level, &source, &message, subject.as_deref()),
        Command::Alert {
            severity,
            title,
            body,
        } => on_alert(page, severity, title, body),
        Command::PublishControl {
            channel,
            body,
            stamp,
        } => on_publish_control(page, &channel, body, stamp),
        Command::Geometry {
            width,
            height,
            device_pixel_ratio,
        } => on_geometry(page, width, height, &device_pixel_ratio),
        Command::Status {
            instances,
            uptime_secs,
            counters,
        } => on_status(page, &instances, uptime_secs, &counters),
        Command::Close => on_close(page),
    }
}

/// One component's publish: resolved, checked, and then committed by whichever
/// authority owns its channel.
///
/// The three refusals are the page's own, answered without anything leaving it: a
/// port the wiring does not bind, a body over the attachment's cap, and — for a
/// transportable port only — no *configured* attachment to send it on. A confined
/// port is reachable whether or not the link is, which is what keeps a page usable
/// offline; a transportable one needs the wiring the peer on this socket is
/// judging against, which is the document of this attachment and not the last
/// one's.
fn on_publish(page: &mut SurfacePage, publish: PortPublish, stamp: MessageStamp) -> CommandOutcome {
    let resolved = page.connect.bindings().and_then(|bindings| {
        resolve_output(bindings, &publish.instance, &publish.port, publish.urgency)
            .map(|out| (out.channel.to_string(), out.urgency))
    });
    let confined = resolved
        .as_ref()
        .is_some_and(|(channel, _)| !channel_is_transportable(channel));
    let reject = check_publish(
        page.connect.configured_bindings().is_some() || confined,
        || resolved.is_some(),
        publish.body.len() as u64,
        page.body_cap,
    );
    if let Err(reject) = reject {
        return CommandOutcome {
            answers: vec![PublishAnswer::Port {
                instance: publish.instance,
                port: publish.port,
                correlation: publish.correlation,
                status: PublishStatus::from(reject),
            }],
            ..CommandOutcome::default()
        };
    }
    let (channel, urgency) = resolved.expect("surface client: an unbound port was refused above");
    if confined {
        route_publish(page, publish, channel, urgency, stamp)
    } else {
        wire_publish(page, publish, &channel, urgency)
    }
}

/// Compose the frame for a publish the peer will route and answer.
///
/// The resolution the caller already made is handed down rather than repeated:
/// two lookups that had to agree would be coupled by nothing but an `expect`.
fn wire_publish(
    page: &mut SurfacePage,
    publish: PortPublish,
    channel: &str,
    urgency: Urgency,
) -> CommandOutcome {
    let frame = page
        .outbound
        .publish_port(ResolvedOutput { channel, urgency }, publish);
    CommandOutcome {
        frames: vec![frame],
        ..CommandOutcome::default()
    }
}

/// Commit a publish on a confined channel: minted, retained, and by that one
/// append every reader on the channel woken.
///
/// Answered synchronously, because there is no peer to answer it: this page is the
/// authority, so the outcome is known before the call returns. It states no
/// release time — a deferred publish is always a buffered one, and a buffered
/// publish always flushes.
fn route_publish(
    page: &mut SurfacePage,
    publish: PortPublish,
    channel: String,
    urgency: Urgency,
    stamp: MessageStamp,
) -> CommandOutcome {
    let PortPublish {
        instance,
        port,
        body,
        urgency: _,
        correlation,
    } = publish;
    let SurfacePage {
        connect,
        stores,
        schedules,
        router,
        ..
    } = page;
    let outcome = router.route(
        stores,
        RouteRequest {
            channel: &channel,
            origin: Origin::Sub(&instance),
            body,
            stamp,
            urgency,
            deliver_after: None,
        },
    );
    let mut answered = CommandOutcome::default();
    let status = match outcome {
        RouteOutcome::Routed { overflow } => {
            answered.drops = charge(connect.bindings(), schedules, &channel, overflow);
            PublishStatus::Ok
        }
        RouteOutcome::Refused { reason } => {
            answered.refusal = Some(RefusedPublish {
                instance: instance.clone(),
                refusal: PlaneRefusal {
                    port: port.clone(),
                    channel,
                    reason,
                },
            });
            PublishStatus::Refused
        }
        RouteOutcome::NoIdentity => panic!(
            "surface client: {instance} published on {channel} before the page had an identity"
        ),
        RouteOutcome::Parked { .. } | RouteOutcome::ScheduleDropped { .. } => {
            unreachable!("surface client: a single publish states no release time")
        }
    };
    answered.answers.push(PublishAnswer::Port {
        instance,
        port,
        correlation,
        status,
    });
    answered
}

/// One error report, if the wiring publishes any and this one clears its floor.
///
/// Dropped while the attachment is not configured rather than queued: a report is
/// a breadcrumb about something that already happened, the console copy is the
/// durable record, and a page reconnecting after an outage has nothing to gain
/// from a backlog of stale ones. Its outcome is answered to nobody — a report
/// about a failed report is the loop the swallow closes.
fn on_report(
    page: &mut SurfacePage,
    level: LogLevel,
    source: &str,
    message: &str,
    subject: Option<&str>,
) -> CommandOutcome {
    let SurfacePage {
        connect, outbound, ..
    } = page;
    let Some(bindings) = connect.configured_bindings() else {
        return CommandOutcome::default();
    };
    let frame = outbound.report(
        bindings,
        ErrorReport {
            level,
            source,
            message,
            subject,
        },
    );
    CommandOutcome {
        frames: frame.into_iter().collect(),
        ..CommandOutcome::default()
    }
}

/// One alert, if there is an attachment to send it on and it is granted.
///
/// The two drops differ in kind. No attachment is a benign liveness race — the
/// alert rides the same socket as everything else and there is no other sink for
/// it — and stays silent. No grant is a capability refusal against a caller that
/// failed to pre-gate on [`Event::Connected`](crate::session::Event::Connected)'s
/// `alert_granted`, and the peer would close the attachment over the frame, so it
/// leaves a breadcrumb.
fn on_alert(
    page: &mut SurfacePage,
    severity: AlertSeverity,
    title: String,
    body: String,
) -> CommandOutcome {
    let Some(facts) = page.connect.facts() else {
        return CommandOutcome::default();
    };
    if !facts.alert_granted {
        tracing::warn!(
            "surface client: dropped an alert — this surface has no alert grant; callers must \
             pre-gate on the attachment's grant"
        );
        return CommandOutcome::default();
    }
    CommandOutcome {
        frames: vec![ClientFrame::Alert {
            severity,
            title: truncate_report_field(title, MAX_ALERT_TITLE_BYTES),
            body: truncate_report_field(body, MAX_ALERT_BODY_BYTES),
        }],
        ..CommandOutcome::default()
    }
}

/// State one of the kernel's own planes.
///
/// Honoured whether or not the link is up, and whether or not a document is in
/// force: the plane a page draws its own death banner from is exactly the one that
/// must still work when everything else has stopped. The single precondition is
/// identity — before the first attachment the page has no principal to attribute
/// an envelope to, and an unattributable envelope is what the identity model
/// exists to prevent. Nothing is lost by that drop: a depth-1 plane replays its
/// retained value to whatever mounts later.
fn on_publish_control(
    page: &mut SurfacePage,
    channel: &str,
    body: String,
    stamp: MessageStamp,
) -> CommandOutcome {
    let SurfacePage {
        connect,
        stores,
        schedules,
        router,
        ..
    } = page;
    let outcome = router.route(
        stores,
        RouteRequest {
            channel,
            origin: Origin::Attacher,
            body,
            stamp,
            urgency: CONTROL_URGENCY,
            // The kernel states its planes now or not at all: a plane's reader
            // mounts to the retained value, so a schedule would buy nothing.
            deliver_after: None,
        },
    );
    match outcome {
        RouteOutcome::Routed { overflow } => CommandOutcome {
            drops: charge(connect.bindings(), schedules, channel, overflow),
            ..CommandOutcome::default()
        },
        RouteOutcome::NoIdentity => {
            tracing::debug!(
                %channel,
                "surface client: dropped a control publish — the page has no identity yet"
            );
            CommandOutcome::default()
        }
        RouteOutcome::Refused { reason } => {
            panic!("surface client: {channel} refused a body the kernel wrote for it: {reason}")
        }
        RouteOutcome::Parked { .. } | RouteOutcome::ScheduleDropped { .. } => {
            unreachable!("surface client: a control publish states no release time")
        }
    }
}

/// The viewport document.
///
/// Needs both halves of an attachment's context: the session id the document
/// self-attributes with, and the wiring *this* attachment put in force, which
/// names the channel it goes on. A reading taken before either is dropped rather
/// than held — the next resize, or the platform half's own post-attach reading, is
/// a fresher fact than a stale one.
fn on_geometry(
    page: &mut SurfacePage,
    width: u32,
    height: u32,
    device_pixel_ratio: &Number,
) -> CommandOutcome {
    let SurfacePage {
        connect, outbound, ..
    } = page;
    let (Some(facts), Some(bindings)) = (connect.facts(), connect.configured_bindings()) else {
        return CommandOutcome::default();
    };
    let ratio = device_pixel_ratio
        .as_f64()
        .expect("surface client: a JSON number reads back as a double");
    let body = match telemetry::geometry_body(&facts.session_id, width, height, ratio) {
        Ok(body) => body,
        Err(err) => {
            tracing::warn!(
                %err,
                "surface client: refused a viewport reading outside the plausible range"
            );
            return CommandOutcome::default();
        }
    };
    CommandOutcome {
        frames: vec![outbound.publish_telemetry(bindings, TelemetryKind::Geometry, body)],
        ..CommandOutcome::default()
    }
}

/// The mount-status document, with the health summary derived here, the overlay
/// read off the page's own plane and the refused-telemetry total stated from the
/// page's own count.
///
/// Dropped until the attachment is configured, exactly as the viewport is: the
/// channel it goes on and the wiring the health summary is derived from must both
/// be this attachment's.
fn on_status(
    page: &mut SurfacePage,
    instances: &[InstanceReport],
    uptime_secs: u64,
    counters: &StatusCounters,
) -> CommandOutcome {
    let SurfacePage {
        connect,
        outbound,
        router,
        ..
    } = page;
    let (Some(facts), Some(bindings)) = (connect.facts(), connect.configured_bindings()) else {
        return CommandOutcome::default();
    };
    let report = StatusReport {
        instances,
        uptime_secs,
        counters,
        telemetry_dropped: outbound.telemetry_dropped(),
        overlay: router.policy().overlay(),
    };
    match telemetry::status_body(&facts.session_id, bindings, &report) {
        Ok(body) => CommandOutcome {
            frames: vec![outbound.publish_telemetry(bindings, TelemetryKind::Status, body)],
            ..CommandOutcome::default()
        },
        // Every principal the report names descends from the same document the
        // health summary was derived from, so a contradiction is this build
        // disagreeing with itself rather than a snapshot to publish around.
        Err(err) => CommandOutcome {
            fatal: Some(format!("status document contradicts the wiring: {err}")),
            ..CommandOutcome::default()
        },
    }
}

/// Wind the page down: the attachment's own state goes exactly as it does on a
/// detach, every caller awaiting a publish is answered `ConnectionLost`, and the
/// close is asked for.
///
/// The page itself is left intact — its stores, positions and registrations
/// outlive the attachment — because the caller owns what happens next, and nothing
/// here can reload or drop a page.
fn on_close(page: &mut SurfacePage) -> CommandOutcome {
    let Detached { answers, steps } = page.on_detached();
    CommandOutcome {
        answers,
        steps,
        close: true,
        ..CommandOutcome::default()
    }
}

/// Charge one confined append's evictions to the readers that lost them.
///
/// A page with no document in force has no reader on any channel: positions are
/// attached by the reconcile of a document, so an overflow without one is the page
/// disagreeing with itself.
fn charge(
    bindings: Option<&AppliedBindings>,
    schedules: &mut Schedules,
    channel: &str,
    overflow: Vec<CursorOverflow<BindingKey>>,
) -> DropVerdicts {
    match bindings {
        Some(bindings) => schedules.charge_overflow(bindings, channel, overflow),
        None => {
            assert!(
                overflow.is_empty(),
                "surface client: {channel} evicted a position with no document in force"
            );
            DropVerdicts::default()
        }
    }
}

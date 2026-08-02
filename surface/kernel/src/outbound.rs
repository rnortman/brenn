//! What a surface writes: its components' publishes addressed onto channels, the
//! documents the kernel writes about itself, and the per-instance outboxes that
//! carry an activation's whole flush.
//!
//! A component states a publish the way it is wired — an `(instance, port)` pair
//! and a body — and the wire carries a channel address, a concrete urgency and an
//! opaque sub-identity. Turning the first into the second is this module's whole
//! job, and it is a lookup in the bindings document: the port's binding names the
//! channel and the urgency the operator configured for it, and the instance id
//! *is* the attribution the peer validates against its own declared set.
//!
//! Three writers share that path, and the difference between them is only which
//! answer they are owed:
//!
//! - **A component's publish** is answered to its caller, by the caller's own
//!   token.
//! - **An error report** is answered to nobody. A report whose own failure
//!   produced a report would be a loop, so the outcome is consumed and dropped —
//!   the console copy the reporter already wrote is the durable record.
//! - **A telemetry document** is answered to a counter. The peer refusing a
//!   snapshot costs staleness until the next tick, so a refusal is counted and
//!   never retried; anything that is not a refusal a conforming peer can produce
//!   is a broken invariant and the caller's fatal.
//!
//! **Correlations.** The wire correlation is minted here, and the caller's token
//! rides the pending entry rather than the frame. That is what lets the kernel's
//! own writers — which have no caller and no token — use the same table as a
//! component's publish without two correlation spaces colliding on it.

#[cfg(test)]
mod tests;

use brenn_attach_client::Millis;
use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::publish::{
    BatchAnswer, Discarded, FlushBatch, OutboxSteps, Outboxes, PendingPublishes, PublishRequest,
};
use brenn_attach_proto::{ClientFrame, PublishBatchOutcome, PublishOutcome, check_batch_legality};
use brenn_envelope::Urgency;
use brenn_surface_schema::telemetry::ErrorReportDocument;
use brenn_surface_schema::{LogLevel, MAX_LOG_MESSAGE_BYTES, MAX_LOG_SOURCE_BYTES};

use crate::bindings::AppliedBindings;

/// The urgency the kernel states on the documents it writes about itself.
///
/// The platform channels are named by the wiring rather than bound through an
/// output block, so there is no operator urgency knob on them to read: they take
/// the same `normal` an unset one resolves to. Widening this to a configurable
/// knob is additive.
const PLATFORM_URGENCY: Urgency = Urgency::Normal;

/// The disposition of a publish, as its caller is answered. It unifies the
/// peer's wire [`PublishOutcome`] with the page-side rejections and the
/// link-drop signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishStatus {
    /// The peer accepted the publish.
    Ok,
    /// The peer rate-limited it (its per-connection token bucket). The page does
    /// not retry — that is the instance's business.
    RateLimited,
    /// The body exceeded the peer's cap. Carries the peer's reported `len`/`max`
    /// when the peer rejected it, or the page's own view when the page rejected
    /// it before sending (a stale-snapshot race).
    BodyTooLarge { len: u64, max: u64 },
    /// `(instance, port)` is not a bound output in the wiring in force: the page
    /// refused to send an unbound-port publish (a stale-snapshot race).
    UnboundPort,
    /// There was no configured attachment when the command reached the page (a
    /// stale-snapshot race): the publish was not sent.
    NotConnected,
    /// The attachment ended with this publish's answer still outstanding.
    ConnectionLost,
    /// The peer accepted the frame but the durable publish failed on a path that
    /// must not kill the attachment. Client-facing meaning is "it did not land";
    /// the page does not retry.
    Failed,
    /// A `local:` plane guard refused the body: the page-local router minted
    /// nothing, so the message was neither retained nor delivered. The violation
    /// itself is reported separately, attributed to the publisher.
    Refused,
}

/// The three publish pre-check rejections, in authoritative check order. Single
/// source of truth shared by the front door's fast gate and the page's
/// authoritative recheck; each caller converts into its own reject vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishCheckReject {
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

/// The publish pre-check: predicates and their order live here and only here.
/// `output_bound` is lazy, so a caller may resolve it however it likes.
///
/// `reachable` is "there is a configured attachment, **or** the target is a
/// `local:` port": page-local traffic never touches the wire, so the link being
/// down is no reason to reject it — that offline-correctness is the whole point
/// of the class (the kiosk that must still accept a takeover with the network
/// out). Callers compute it; the distinction cannot be made here, where no
/// wiring is in scope.
///
/// The body cap applies to confined publishes too, deliberately. It is nominally
/// a peer ingress limit, but ports are ports: a component's body-size contract
/// must not silently change because an operator rebound its output port from
/// `brenn:` to `local:`. It also bounds the router's rings, which are page
/// memory.
pub fn check_publish(
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

/// Cap a report field (`message` or `source`) at its schema byte limit,
/// truncating on a UTF-8 boundary and appending a marker so the receiver sees the
/// value was cut. Asserts `cap` exceeds the marker length rather than trusting it
/// by prose: a smaller cap would underflow the subtraction below (a release-mode
/// wrap into a hang), so it dies loudly instead.
pub fn truncate_report_field(value: String, cap: usize) -> String {
    const MARKER: &str = "…[truncated]";
    assert!(
        cap > MARKER.len(),
        "surface client: report-field cap {cap} is smaller than the truncation marker"
    );
    if value.len() <= cap {
        return value;
    }
    let mut end = cap - MARKER.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = value;
    out.truncate(end);
    out.push_str(MARKER);
    out
}

/// One caller's publish, at the grain the caller states it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortPublish {
    pub instance: String,
    pub port: String,
    pub body: String,
    /// The caller's override, or `None` for the port's configured default.
    pub urgency: Option<Urgency>,
    /// The caller's own token, carried back on this publish's answer. Distinct
    /// from the wire correlation, which this layer mints.
    pub correlation: u64,
}

/// Where an output port publishes, and with what urgency.
///
/// Both fields are resolved here rather than left to the peer: the wire carries a
/// concrete urgency, because the page needs the port's default anyway to stamp
/// the envelopes it routes itself, and a client that can state one value there
/// cannot honestly state a different one on a channel the peer routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedOutput<'a> {
    pub channel: &'a str,
    pub urgency: Urgency,
}

/// Resolve `(instance, port)` against the wiring in force, or `None` for a port
/// this surface does not bind.
///
/// `None` is also the answer for an unknown instance: an unwired port and an
/// unwired component are the same thing to a publisher, and the caller's
/// rejection reads the same either way.
pub fn resolve_output<'a>(
    bindings: &'a AppliedBindings,
    instance: &str,
    port: &str,
    urgency: Option<Urgency>,
) -> Option<ResolvedOutput<'a>> {
    let binding = bindings.output(instance, port)?;
    Some(ResolvedOutput {
        channel: &binding.channel,
        urgency: urgency.unwrap_or(binding.urgency),
    })
}

/// Whether `instance` still publishes on `channel` under the wiring in force.
///
/// The question a queued flush is re-checked with: its entries name channels, and
/// what the peer admits is the sender's own output set.
fn publishes_on(bindings: &AppliedBindings, instance: &str, channel: &str) -> bool {
    bindings.outputs_of(instance).any(|b| b.channel == channel)
}

/// One error report as the kernel's log path states it.
pub struct ErrorReport<'a> {
    pub level: LogLevel,
    /// The human-readable producer, e.g. `"component:<kind>"`. Untrusted detail;
    /// the machine-readable attribution is [`subject`](Self::subject).
    pub source: &'a str,
    pub message: &'a str,
    /// The component the report is *about*, which becomes the report's sender
    /// sub-identity. `None` for the kernel's own breadcrumbs, which carry the
    /// bare surface identity. It must name a declared instance — the peer
    /// validates it against the set its own configuration declares and closes the
    /// attachment on an unknown one, so it is never a free-form label.
    pub subject: Option<&'a str>,
}

/// A flush nobody is left to answer for.
///
/// Two paths produce one: a refusal the peer answered after the outbox closed, and
/// an outbox a bindings document closed with a queue still in it. Either way the
/// entries were ok'd by the activation that wrote them and never applied, so the
/// loss is real — and it is reported with the instance it belonged to rather than
/// counted on a registrant that no longer exists.
#[derive(Debug, Clone, PartialEq)]
pub struct LostFlush {
    /// Whose flush it was, by the instance id its outbox was keyed on.
    pub instance: String,
    pub batch: FlushBatch,
}

/// Which document a telemetry publish carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryKind {
    Geometry,
    Status,
}

impl TelemetryKind {
    /// The channel this document is published on, from the wiring's platform
    /// section. Explicit there rather than derived here: the page cannot know the
    /// operator's channel prefix.
    fn channel(self, bindings: &AppliedBindings) -> &str {
        match self {
            Self::Geometry => &bindings.platform().geometry_channel,
            Self::Status => &bindings.platform().status_channel,
        }
    }
}

/// What a publish this layer sent is waiting to answer.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PublishTag {
    /// A component's publish, to be answered to its caller.
    Port {
        instance: String,
        port: String,
        correlation: u64,
    },
    /// An error report. Its outcome is consumed and dropped.
    ErrorReport,
    /// One of the surface's own documents.
    Telemetry(TelemetryKind),
}

/// What one settled `PublishResult` is owed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishAnswer {
    /// Hand `status` back to the caller that issued `correlation`.
    Port {
        instance: String,
        port: String,
        correlation: u64,
        status: PublishStatus,
    },
    /// The peer refused a document the surface writes about itself. Counted, not
    /// retried: the next tick or resize publishes a fresh snapshot, so a dropped
    /// latest-wins document costs staleness only.
    TelemetryDropped {
        kind: TelemetryKind,
        outcome: PublishOutcome,
    },
}

/// The surface's outbound side: what it has sent and is owed an answer for, and
/// what each component has queued behind the wire.
///
/// Holds no wiring of its own — every address, urgency and depth it needs is read
/// off the [`AppliedBindings`] passed in at the call — so a new document changes
/// what this layer resolves without invalidating what it is holding.
pub struct SurfaceOutbound {
    /// Single publishes on this attachment, by wire correlation.
    pending: PendingPublishes<PublishTag>,
    /// One outbox per registered component instance, keyed by instance id.
    outboxes: Outboxes<String>,
    /// The wire correlation space, monotone for the life of the page. Per-page
    /// rather than per-attachment because uniqueness is all the peer asks of it,
    /// and a table cleared at every detach cannot collide across one.
    next_correlation: u64,
    /// Telemetry documents the peer refused. Straggler diagnostics: nothing acts
    /// on it, and the status document carries it to whoever wants the total.
    telemetry_dropped: u64,
}

impl Default for SurfaceOutbound {
    fn default() -> Self {
        Self {
            pending: PendingPublishes::new(),
            outboxes: Outboxes::new(),
            next_correlation: 0,
            telemetry_dropped: 0,
        }
    }
}

impl SurfaceOutbound {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compose one component publish and record what its answer is owed to.
    ///
    /// Takes the port's resolution rather than resolving it: the caller has
    /// already asked [`resolve_output`] which channel this port lands on, because
    /// the channel's class is what decides whether the publish reaches the wire at
    /// all. Nothing here can refuse — an unbound port, an unreachable one and an
    /// oversized body are all answered before this layer is asked to spend a
    /// correlation on the publish.
    pub fn publish_port(&mut self, out: ResolvedOutput<'_>, req: PortPublish) -> ClientFrame {
        let request = PublishRequest {
            channel: out.channel.to_string(),
            attribution: Some(req.instance.clone()),
            body: req.body,
            urgency: out.urgency,
        };
        let tag = PublishTag::Port {
            instance: req.instance,
            port: req.port,
            correlation: req.correlation,
        };
        self.send(tag, request)
    }

    /// Compose one error report, or `None` when the surface publishes none.
    ///
    /// Two ways to answer `None`, and they are the same answer to the reporter:
    /// the wiring declares no error channel at all, or it declares one with a
    /// floor this report does not reach. Either way the console copy the caller
    /// wrote is the only sink, by design.
    ///
    /// The two text fields are truncated on UTF-8 boundaries to the schema's
    /// caps, so a conforming kernel's report never trips the peer's body cap —
    /// boot proves `max_body_bytes` clears the worst case.
    pub fn report(
        &mut self,
        bindings: &AppliedBindings,
        report: ErrorReport<'_>,
    ) -> Option<ClientFrame> {
        let platform = bindings.platform();
        let channel = platform.error_channel.as_deref()?;
        let floor = platform
            .error_report_floor
            .expect("surface client: an error channel states a floor");
        if report.level < floor {
            return None;
        }
        let body = ErrorReportDocument {
            source: truncate_report_field(report.source.to_owned(), MAX_LOG_SOURCE_BYTES),
            message: truncate_report_field(report.message.to_owned(), MAX_LOG_MESSAGE_BYTES),
            level: report.level,
        }
        .to_body();
        let request = PublishRequest {
            channel: channel.to_string(),
            // The subject is the report's sender sub-identity, so the report is
            // attributed to the component it is about and draws down that
            // component's send budget rather than its neighbours'.
            attribution: report.subject.map(str::to_owned),
            body,
            urgency: PLATFORM_URGENCY,
        };
        Some(self.send(PublishTag::ErrorReport, request))
    }

    /// Compose one of the surface's own documents.
    ///
    /// Unattributed: the kernel observes the viewport and owns the mount table,
    /// so there is no component on whose behalf it acts — and the bare identity
    /// is the only writer the peer admits on these channels, which is what keeps
    /// their single-writer property true at runtime.
    pub fn publish_telemetry(
        &mut self,
        bindings: &AppliedBindings,
        kind: TelemetryKind,
        body: String,
    ) -> ClientFrame {
        let request = PublishRequest {
            channel: kind.channel(bindings).to_string(),
            attribution: None,
            body,
            urgency: PLATFORM_URGENCY,
        };
        self.send(PublishTag::Telemetry(kind), request)
    }

    /// Mint a wire correlation, record the tag, and compose the frame.
    fn send(&mut self, tag: PublishTag, request: PublishRequest) -> ClientFrame {
        let correlation = self.next_correlation;
        self.next_correlation += 1;
        self.pending.send(correlation, tag, request)
    }

    /// Settle one `PublishResult`.
    ///
    /// `Ok(None)` is a publish that is owed nobody an answer: an accepted
    /// telemetry document, or an error report whatever its outcome.
    ///
    /// `Err` is unreconcilable and the caller's fatal — a correlation this
    /// attachment never sent, and a telemetry document refused for a reason a
    /// conforming peer cannot produce on a channel boot declared and granted.
    pub fn on_publish_result(
        &mut self,
        correlation: Option<u64>,
        outcome: PublishOutcome,
    ) -> Result<Option<PublishAnswer>, String> {
        let tag = self.pending.on_result(correlation)?;
        match tag {
            PublishTag::Port {
                instance,
                port,
                correlation,
            } => Ok(Some(PublishAnswer::Port {
                instance,
                port,
                correlation,
                status: publish_status(outcome),
            })),
            PublishTag::ErrorReport => Ok(None),
            PublishTag::Telemetry(kind) => match outcome {
                PublishOutcome::Ok => Ok(None),
                // A refusal the peer's own metering produces. Never fatal and
                // never retried.
                PublishOutcome::RateLimited | PublishOutcome::BodyTooLarge { .. } => {
                    self.telemetry_dropped += 1;
                    Ok(Some(PublishAnswer::TelemetryDropped { kind, outcome }))
                }
                // The peer reports this only where publishing failed on a path it
                // declares non-fatal, which its profile does for a diagnostics
                // channel and not for these. Reaching it means the channel the
                // wiring names is not the one boot declared.
                PublishOutcome::Failed => Err(format!(
                    "PublishResult refused a {kind:?} document the wiring names: {outcome:?}"
                )),
            },
        }
    }

    /// Fail every outstanding publish: the attachment went away, so no answer is
    /// coming for any of them.
    ///
    /// Only a component's publish is answered. A report's failure must not become
    /// a report, and a telemetry document lost with its connection is not a
    /// refusal to count — the next attachment publishes a fresh snapshot.
    pub fn fail_pending(&mut self) -> Vec<PublishAnswer> {
        self.pending
            .fail_all()
            .into_iter()
            .filter_map(|(_, tag)| match tag {
                PublishTag::Port {
                    instance,
                    port,
                    correlation,
                } => Some(PublishAnswer::Port {
                    instance,
                    port,
                    correlation,
                    status: PublishStatus::ConnectionLost,
                }),
                PublishTag::ErrorReport | PublishTag::Telemetry(_) => None,
            })
            .collect()
    }

    /// Telemetry documents the peer has refused on this page.
    pub fn telemetry_dropped(&self) -> u64 {
        self.telemetry_dropped
    }

    /// Whether `instance` holds an outbox.
    pub fn is_registered(&self, instance: &str) -> bool {
        self.outboxes.is_registered(instance)
    }

    /// Open `instance`'s outbox at the depth its component entry declares,
    /// publishing under its own instance id.
    ///
    /// # Panics
    ///
    /// If the wiring declares no such component. Every registered instance is one
    /// the document names — that is what a registration is — so an unknown one is
    /// this build disagreeing with itself.
    pub fn register(&mut self, instance: &str, bindings: &AppliedBindings) {
        let depth = bindings
            .component(instance)
            .unwrap_or_else(|| {
                panic!("surface client: no component entry for registered instance {instance:?}")
            })
            .parked_batch_depth;
        self.outboxes
            .register(instance.to_string(), Some(instance.to_string()), depth);
    }

    /// Close `instance`'s outbox, answering the flushes that die with it.
    ///
    /// An instance holding no outbox answers nothing: registration before the
    /// page's first document is allowed, and until a document declares the
    /// instance there is no depth to open an outbox at and so no queue it could
    /// be owed.
    pub fn deregister(&mut self, instance: &str) -> Vec<FlushBatch> {
        if !self.outboxes.is_registered(instance) {
            return Vec::new();
        }
        self.outboxes.deregister(instance)
    }

    /// Bring the open outboxes into line with the instances currently registered:
    /// open one for every registered instance the document declares, close the
    /// outbox of everything no longer registered, and answer the flushes that died.
    ///
    /// This is what carries a registration made before the page's first document
    /// — where there was no depth to open an outbox at — into a page that has one.
    ///
    /// Custody follows **registration**, not declaration. A document that stops
    /// declaring a registered instance leaves it registered and un-wired (that is
    /// the registration table's rule — the operator un-wired it, which is not the
    /// component's fault), so its outbox stays open too: closing it here would
    /// leave one half of the page saying "still registered" while a flush or an
    /// unmount of that instance found no outbox. What the un-wiring does cost it is
    /// its queue's contents, which no longer name a channel it publishes on and are
    /// dropped as such at the next attachment.
    ///
    /// An open outbox keeps the depth it was opened at even if a later document
    /// declares a different one. A changed document reloads the page, so the
    /// window is the one between the delivery and the reload, and re-opening the
    /// outbox inside it would discard exactly the queue the depth governs.
    pub fn reconcile<'a>(
        &mut self,
        bindings: &AppliedBindings,
        registered: impl Iterator<Item = &'a str>,
    ) -> Vec<LostFlush> {
        let registered: Vec<String> = registered.map(str::to_string).collect();
        let stale: Vec<String> = self
            .outboxes
            .registrants()
            .filter(|open| !registered.contains(open))
            .cloned()
            .collect();
        let mut lost = Vec::new();
        for instance in stale {
            // Paired with the instance here, where it is still in hand: the batch
            // itself carries no attribution, and a report that cannot name whose
            // committed writes vanished is no use during a config-change incident.
            lost.extend(
                self.outboxes
                    .deregister(&instance)
                    .into_iter()
                    .map(|batch| LostFlush {
                        instance: instance.clone(),
                        batch,
                    }),
            );
        }
        for instance in registered {
            if !self.outboxes.is_registered(&instance) && bindings.is_declared_instance(&instance) {
                self.register(&instance, bindings);
            }
        }
        lost
    }

    /// Offer one completed activation's flush to its instance's outbox, unless the
    /// contract in force refuses it.
    ///
    /// The gate is the one [`Self::on_attached`] runs over a queued flush, asked
    /// here of a flush just composed — because a buffered publish carries the
    /// channel its port resolved to *when the component published*, and the
    /// activation that wrote it can outlive that resolution: a reconnect or a
    /// second document applies new wiring, and an operator can lower the body cap
    /// across a restart. Without the gate such a flush goes straight to a free
    /// wire, and the peer answers a channel outside the sender's set with a
    /// protocol close and a fail2ban signal charged against a legitimate user. A
    /// refused flush is dropped whole and counted, exactly as an attachment-time
    /// refusal is.
    ///
    /// `facts` is `None` while detached, where the two caps have no value to be
    /// judged against — the flush queues, and the attachment that eventually
    /// carries it re-validates it against its own contract.
    ///
    /// # Panics
    ///
    /// If the instance holds no outbox. Every registered instance a document has
    /// ever declared holds one, and an instance no document ever declared has no
    /// bindings — so nothing activates it and it has no flush to offer.
    pub fn flush(
        &mut self,
        bindings: &AppliedBindings,
        facts: Option<&AttachmentFacts>,
        instance: &str,
        batch: FlushBatch,
        now: Millis,
    ) -> OutboxSteps<String> {
        if !survives(bindings, instance, &batch, facts) {
            return self.outboxes.refuse_flush(instance, now);
        }
        self.outboxes.flush(instance, batch, now)
    }

    /// Throw away every flush `instance` has queued, answering how many died.
    ///
    /// The kill path: a component whose memory is presumed poisoned wrote these,
    /// and nobody is left to answer for them.
    ///
    /// # Panics
    ///
    /// If the instance holds no outbox, for the same reason [`Self::flush`] does.
    pub fn discard_parked(&mut self, instance: &str, now: Millis) -> Discarded {
        self.outboxes.discard_parked(instance, now)
    }

    /// The attachment came up: re-validate every queued flush against the
    /// contract this one states, then start the outboxes draining.
    ///
    /// A queued flush was composed under the previous attachment's contract, and a
    /// reconnect can hand the page a different one. Every gate the peer answers
    /// with a violation rather than an outcome is re-checked here, and there are
    /// three: a channel the new wiring no longer binds to this instance, the
    /// batch legality law against the new `max_body_bytes`, and the composed
    /// frame against the new `max_frame_bytes`. The caps are one operator knob —
    /// the frame cap is derived from the body cap — which an operator can lower on
    /// a restart with no build change and so no forced reload. The channel gate
    /// applies to the flush's control ops as well, and the legality law charges
    /// them: an op names a channel, and an edit carries a body. A flush that fails
    /// any of them is dropped whole rather than sent into a protocol death for
    /// honestly replaying what the page buffered under the contract in force when
    /// it buffered it.
    ///
    /// A held op's `message_id` needs no re-validation. It came from a view scoped
    /// to this instance's own sender, and that identity does not depend on the
    /// attachment — so across a reconnect the id either still names that sender's
    /// parked message or names one that released, which is the benign race the
    /// peer logs and counts.
    pub fn on_attached(
        &mut self,
        bindings: &AppliedBindings,
        facts: &AttachmentFacts,
        now: Millis,
    ) -> OutboxSteps<String> {
        self.outboxes
            .on_attached(facts.max_body_bytes as usize, now, |instance, batch| {
                survives(bindings, instance, batch, Some(facts))
            })
    }

    /// The attachment went away: outstanding flushes die with it, queued ones
    /// stay queued.
    pub fn on_detached(&mut self) -> OutboxSteps<String> {
        self.outboxes.on_detached()
    }

    /// Settle one `PublishBatchResult`. `Err` is unreconcilable and the caller's
    /// fatal, exactly as for a single publish.
    pub fn on_batch_result(
        &mut self,
        correlation: u64,
        outcome: PublishBatchOutcome,
        now: Millis,
    ) -> Result<BatchAnswer<String>, String> {
        self.outboxes.on_batch_result(correlation, outcome, now)
    }

    /// The retry timer fired: offer every blocked outbox's head once more.
    pub fn on_retry_tick(&mut self, now: Millis) -> OutboxSteps<String> {
        self.outboxes.on_retry_tick(now)
    }

    /// Flushes `instance` has dropped at its outbox cap.
    pub fn dropped_count(&self, instance: &str) -> u64 {
        self.outboxes.dropped_count(instance)
    }

    /// Flushes the peer has metered for `instance`.
    pub fn rate_limited_count(&self, instance: &str) -> u64 {
        self.outboxes.rate_limited_count(instance)
    }
}

/// Whether a flush passes every gate the peer answers with a violation. See
/// [`SurfaceOutbound::on_attached`] for why these three and no others.
///
/// `facts` of `None` asks the wiring half alone, which is the only half that has
/// an answer while detached.
fn survives(
    bindings: &AppliedBindings,
    instance: &str,
    batch: &FlushBatch,
    facts: Option<&AttachmentFacts>,
) -> bool {
    let admits = |channel: &str| publishes_on(bindings, instance, channel);
    batch.entries.iter().all(|entry| admits(&entry.channel))
        && batch.ops.iter().all(|op| admits(&op.channel))
        && facts.is_none_or(|facts| fits_the_caps(instance, batch, facts))
}

/// Whether a flush clears the attachment's two size caps: the protocol's batch
/// legality law against `max_body_bytes`, which charges every channel and body
/// the flush carries, and the composed frame against the `max_frame_bytes` the
/// peer advertised.
///
/// The measured frame is checked as well as the law, though the law is derived
/// to bound it: the number that governs is the one the peer stated on this
/// attachment, and a peer built against a different derivation would state a
/// different one.
fn fits_the_caps(instance: &str, batch: &FlushBatch, facts: &AttachmentFacts) -> bool {
    check_batch_legality(&batch.entries, &batch.ops, facts.max_body_bytes as usize).is_ok()
        // Last, because it is the one gate that has to serialize the flush to
        // answer.
        && batch.frame_bytes(Some(instance)) as u64 <= facts.max_frame_bytes
}

/// The caller-facing status of one wire outcome.
fn publish_status(outcome: PublishOutcome) -> PublishStatus {
    match outcome {
        PublishOutcome::Ok => PublishStatus::Ok,
        PublishOutcome::RateLimited => PublishStatus::RateLimited,
        PublishOutcome::BodyTooLarge { len, max } => PublishStatus::BodyTooLarge { len, max },
        PublishOutcome::Failed => PublishStatus::Failed,
    }
}

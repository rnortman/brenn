//! The vocabulary the surface kernel's platform half speaks, and the one place a
//! page's answers become it.
//!
//! Every layer below this one answers with data and enacts nothing: a pass hands
//! back frames to send, callers owed a publish outcome, the loudness ladder's
//! verdicts, an outbox's timer instruction, a document it applied. Something has
//! to decide what each of those *is* — which frame goes out, which event the
//! platform half reads, which loss kills a component. That is this module, and
//! [`Reactions`] is where one turn's answers accumulate in the order they were
//! produced.
//!
//! # Two vocabularies, one direction each
//!
//! [`Event`] goes up, to the platform half that mounts components and draws the
//! page: the attachment came up carrying this wiring, this component died, this
//! publish landed. [`Effect`] goes out, to the layer that owns the driver: send
//! this frame, arm this deadline, mint an envelope for this control publish, take
//! the connection fatal. An event travels *inside* an effect
//! ([`Effect::EmitEvent`]) so that one ordered list is the whole account of a
//! turn — an event raised before a frame stays before it.
//!
//! # What is not decided here
//!
//! No clock is read, no socket is touched, and no envelope is minted: a confined
//! publish this layer composes leaves as [`Effect::PublishControl`] for the layer
//! that reads clocks and entropy to stamp and route. That keeps the whole
//! surface-side stack testable against fixed inputs, which is the same seam every
//! module since the page aggregate answers across.
//!
//! # Two things the fold must know
//!
//! - **A frame needs a live attachment.** The driver refuses to write to a socket
//!   that is not there, and the loudness ladder can fire while the link is down —
//!   a confined append evicts a position whether or not a peer exists. So an
//!   `Alert` composed while detached is dropped, best-effort, exactly as the
//!   alert command has always been; its toast still goes out, because the page's
//!   own planes work offline.
//! - **A loss can name a component that has left.** The ladder announces a loud
//!   rung even for a binding whose instance deregistered, deliberately — an
//!   operator-visible loss must not become invisible because the component that
//!   suffered it went away. A kill, however, has nothing left to kill, so the
//!   `fatal` rung is enacted only against an instance the page still holds.

#[cfg(test)]
mod tests;

use brenn_attach_client::Millis;
use brenn_attach_client::conn::{ConnEvent, DetachReason};
use brenn_attach_client::publish::{OutboxSteps, TimerChange};
use brenn_attach_client::router::ReleaseTimer;
use brenn_attach_proto::{ClientFrame, VersionRange};
use brenn_surface_schema::bindings::BindingsDocument;

use crate::activation::{ActivationOutcome, DropVerdicts};
use crate::command::CommandOutcome;
use crate::flush::Killed;
use crate::inbound::Inbound;
use crate::outbound::{PublishAnswer, PublishStatus};
use crate::outward::{self, Completion, ControlPublish, Released};
use crate::page::{Configured, Detached, SurfacePage};

/// Something the platform half must know: an attachment's lifecycle, a
/// component's fate, or the answer to something it asked for.
///
/// Diagnostics that no consumer acts on are not here — a subscription gap and a
/// refused telemetry document are logged where they are folded, because inventing
/// an event nobody reads would be an API the platform half has to match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The attachment is configured: its wiring has been applied and the page is
    /// usable. Emitted once per attachment, at phase 2.
    ///
    /// Carries the parsed wiring rather than a frame's payload — the wiring is
    /// retained bus state now.
    Connected {
        bindings: BindingsDocument,
        /// This attachment's principal, `surface:<slug>`.
        participant_id: String,
        /// The peer-minted id of this attachment, which the surface's own
        /// documents are self-attributed with.
        session_id: String,
        /// The publish body cap this attachment states.
        max_body_bytes: u64,
        /// Whether the peer will accept an `Alert` from this attachment.
        alert_granted: bool,
    },
    /// The delivered wiring differs from what the page was running on. The
    /// platform half reloads (capped) — a page cannot re-wire itself in place,
    /// and the operator changed the configuration under it.
    ///
    /// Its own event rather than a field of [`Connected`](Event::Connected)
    /// because the two are independent: a reconnect can deliver a changed
    /// document (both fire), and a second document mid-attachment changes the
    /// wiring without the page attaching again (only this one fires).
    WiringChanged,
    /// The live attachment went away. Reconnection proceeds on the client's
    /// backoff schedule; the platform half surfaces it (a banner, the link-state
    /// plane).
    Disconnected { reason: DetachReason },
    /// A protocol contract the page could not reconcile. Terminal — no
    /// reconnect.
    ///
    /// `detail` is a diagnostic string that embeds peer-supplied text: the channel
    /// addresses, close reasons and gap descriptions the frames it names carried.
    /// Render it as text only — never interpolate it into markup or a URL.
    Fatal { detail: String },
    /// The two ends speak no version in common. Terminal, and deliberately not a
    /// [`Fatal`](Event::Fatal): the peer conformed by stating what it speaks. For
    /// a page served by the peer it names, stale assets are the likeliest cause.
    ///
    /// `theirs` is peer-stated, as numbers rather than text. Render it as text
    /// only, on the same rule the rest of this vocabulary's peer-supplied fields
    /// carry.
    Incompatible {
        ours: VersionRange,
        theirs: VersionRange,
    },
    /// The peer closed with the code this page declared terminal: it is running
    /// against an older build than the peer now serves. Terminal — the platform
    /// half performs the (capped) reload; nothing here reloads anything.
    ///
    /// `server_build` is opaque peer-supplied text from the close reason, never
    /// validated against any build-id shape. Render it as text only — never
    /// interpolate it into markup or a URL.
    ReloadRequired { server_build: String },
    /// The outcome of a publish issued through the handle, routed back by the
    /// `correlation` the caller was given.
    PublishResult {
        instance: String,
        port: String,
        correlation: u64,
        status: PublishStatus,
    },
    /// A delivery from a span the page had already left was discarded. Diagnostic
    /// only, and reported once per span, so its rate is page-paced rather than
    /// peer-paced.
    StragglerDiscarded {
        channel: String,
        seq: u64,
        dropped: u64,
    },
    /// An activation entry returned err. Diagnostic: its buffer was discarded and
    /// a failure counted, but the instance is alive and still being delivered.
    ActivationFailed { instance: String, message: String },
    /// An instance is terminal — it trapped, or a binding of its own overflowed
    /// past the rung that kills. Nothing further is delivered to it and its
    /// queued flushes are gone. Never page death, and never a sibling's problem.
    InstanceFailed { instance: String, reason: String },
    /// One of the page's own confined planes refused a body a component wrote:
    /// the message was neither retained nor delivered, and whatever the plane
    /// tracks is unchanged. The publisher is not told — it got its answer at
    /// buffer time — so this is the only account of a wiring fault an operator
    /// has to see.
    PlaneRefused {
        /// The publisher.
        instance: String,
        /// Its own output port, so the report names what a component author would
        /// recognize.
        port: String,
        channel: String,
        /// The plane's own words.
        reason: String,
    },
}

/// Something the layer that owns the driver must do, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Send a client frame on the live attachment.
    SendFrame(ClientFrame),
    /// Hand an event to the platform half.
    EmitEvent(Event),
    /// Publish a body on one of the page's confined planes. Not routed here:
    /// minting a confined envelope needs a stamp only the layer that reads clocks
    /// and entropy can supply, and routing one can itself evict a reader's
    /// position — another ladder charge, which belongs to that layer's own pass.
    PublishControl { channel: String, body: String },
    /// Re-arm or disarm the outbox-retry deadline. Emitted only when it changed:
    /// re-arming an already-armed timer on every input would let unrelated
    /// traffic push a blocked head's deadline out indefinitely.
    SetRetryWakeup(TimerChange),
    /// Re-arm or disarm the confined-release deadline, in wall-clock epoch
    /// milliseconds — the currency a release time is stated in. Emitted only when
    /// it changed, and stated once per turn rather than at each site that could
    /// move it.
    SetReleaseWakeup(ReleaseTimer),
    /// Take the attachment fatal with this diagnosis. The connection answers with
    /// its own terminal event, which is where [`Event::Fatal`] is minted — a
    /// fatal is one thing however it was diagnosed.
    GoFatal { detail: String },
    /// Close the attachment for good: the page asked to shut down. Terminal and
    /// silent — no reconnect, and no event, because the platform half is the party
    /// that asked. The callers the close stranded are answered beside it.
    Close,
}

/// One turn's effects, accumulated in the order the passes produced them.
///
/// A turn is one input: an attachment event, a routed frame, a command, a
/// completion, a timer fire. The caller runs the passes the input calls for,
/// folds each answer in here, and closes the turn with [`end_turn`](Self::end_turn).
#[derive(Debug, Default)]
pub struct Reactions {
    effects: Vec<Effect>,
}

impl Reactions {
    /// An empty turn.
    pub fn new() -> Self {
        Self::default()
    }

    /// What the turn asks for, in order.
    pub fn into_effects(self) -> Vec<Effect> {
        self.effects
    }

    /// What the turn asks for so far, for a caller that folds and inspects in one
    /// pass.
    pub fn effects(&self) -> &[Effect] {
        &self.effects
    }

    /// Close the turn: state the confined-release deadline if the soonest parked
    /// message moved.
    ///
    /// Stated here rather than at every site that could move one — a park at
    /// flush, a release sweep, a control op and a discarded store all move it —
    /// because one restatement over the whole page cannot be forgotten at a new
    /// site the way per-site arming can.
    pub fn end_turn(&mut self, page: &mut SurfacePage) {
        if let Some(timer) = outward::release_wakeup(page) {
            self.effects.push(Effect::SetReleaseWakeup(timer));
        }
    }

    /// Fold one of the connection's own events.
    ///
    /// Four of the five are terminal or diagnostic and reach the platform half
    /// unchanged. `Attached` is phase 1: the page takes the attachment's identity
    /// and subscribes the channel its wiring is retained on, which is the one
    /// frame this phase sends. No [`Event::Connected`] is minted yet — the page
    /// is not usable until its document arrives.
    ///
    /// # Panics
    ///
    /// If an attachment is already live when `Attached` arrives — see
    /// [`SurfacePage::on_attached`].
    pub fn conn_event(&mut self, page: &mut SurfacePage, event: ConnEvent) {
        match event {
            ConnEvent::Attached(facts) => {
                let frames = page.on_attached(facts);
                self.frames(page, frames);
            }
            // The loss is reported before what died with it: a caller reading
            // `ConnectionLost` off a publish should not be the first thing the
            // platform half hears about a link that is down.
            ConnEvent::Detached { reason } => {
                self.emit(Event::Disconnected { reason });
                self.detach(page);
            }
            ConnEvent::Fatal { detail } => {
                self.emit(Event::Fatal { detail });
                self.detach(page);
            }
            ConnEvent::Incompatible { ours, theirs } => {
                self.emit(Event::Incompatible { ours, theirs });
                self.detach(page);
            }
            ConnEvent::PeerClosedTerminal { code, reason } => {
                tracing::warn!(code, %reason, "surface client: the peer closed with the terminal code");
                self.emit(Event::ReloadRequired {
                    server_build: reason,
                });
                self.detach(page);
            }
        }
    }

    /// Fold what one routed server frame settled.
    ///
    /// The order is the order the frame produced it: the subscription frames it
    /// enacts, then everything a document it carried applied, then the callers it
    /// answered, then the losses it caused.
    pub fn inbound(&mut self, page: &mut SurfacePage, inbound: Inbound, now: Millis, now_ms: u64) {
        let Inbound {
            frames,
            answers,
            drops,
            steps,
            configured,
            straggler,
            gap,
            lost_flushes,
        } = inbound;
        self.frames(page, frames);
        for configured in configured {
            self.configured(page, configured, now, now_ms);
        }
        self.answers(answers);
        self.steps(page, steps);
        self.verdicts(page, drops, now, now_ms);
        if let Some(straggler) = straggler {
            self.emit(Event::StragglerDiscarded {
                channel: straggler.channel,
                seq: straggler.seq,
                dropped: straggler.dropped,
            });
        }
        // A gap goes no further than a log line: the subscribe that carries the
        // page past it has already happened, and no component-visible vocabulary
        // states one — a component sees a first-window-after-resume, which its
        // contract calls unremarkable.
        if let Some(gap) = gap {
            tracing::warn!(
                channel = %gap.channel,
                replay_count = gap.replay_count,
                gap = ?gap.gap,
                "surface client: the peer could not replay to the resume point"
            );
        }
        for lost in lost_flushes {
            tracing::warn!(
                instance = %lost.instance,
                entries = lost.batch.entries.len(),
                ops = lost.batch.ops.len(),
                "surface client: a refused flush outlived the outbox that owed it"
            );
        }
    }

    /// Fold what putting a bindings document in force produced — phase 2.
    ///
    /// [`Event::Connected`] is minted for the first document of an attachment,
    /// which is the moment the page becomes usable, and
    /// [`Event::WiringChanged`] whenever the document differs from what the page
    /// was running on. Both, one, or neither can fire.
    ///
    /// # Panics
    ///
    /// If no attachment is live — a document arrives on a subscription that only
    /// exists while attached.
    pub fn configured(
        &mut self,
        page: &mut SurfacePage,
        configured: Configured,
        now: Millis,
        now_ms: u64,
    ) {
        let Configured {
            first_of_attachment,
            wiring_changed,
            frames,
            steps,
            drops,
            lost_flushes,
        } = configured;
        if first_of_attachment {
            self.emit(connected(page));
        }
        if wiring_changed {
            self.emit(Event::WiringChanged);
        }
        self.frames(page, frames);
        self.steps(page, steps);
        self.verdicts(page, drops, now, now_ms);
        for lost in lost_flushes {
            tracing::warn!(
                instance = %lost.instance,
                entries = lost.batch.entries.len(),
                ops = lost.batch.ops.len(),
                "surface client: a queued flush died with the outbox this document closed"
            );
        }
    }

    /// Fold what one command the platform half asked for produced.
    ///
    /// The order is the order a caller needs it in: the frames the command
    /// composed, then the close it asked for, then the diagnostic for a plane that
    /// refused it, then the callers it answered, then the losses it caused. A
    /// refusal is reported ahead of the answer that carries it, so the reason
    /// reaches the log before the bare status reaches the publisher.
    pub fn command(
        &mut self,
        page: &mut SurfacePage,
        outcome: CommandOutcome,
        now: Millis,
        now_ms: u64,
    ) {
        let CommandOutcome {
            frames,
            answers,
            drops,
            refusal,
            steps,
            close,
            fatal,
        } = outcome;
        self.frames(page, frames);
        if close {
            self.effects.push(Effect::Close);
        }
        if let Some(refused) = refusal {
            self.emit(Event::PlaneRefused {
                instance: refused.instance,
                port: refused.refusal.port,
                channel: refused.refusal.channel,
                reason: refused.refusal.reason,
            });
        }
        self.answers(answers);
        self.steps(page, steps);
        self.verdicts(page, drops, now, now_ms);
        if let Some(detail) = fatal {
            self.go_fatal(detail);
        }
    }

    /// Fold one activation's completion.
    ///
    /// The instance and the outcome ride the answer rather than being restated
    /// here: the pass that produced it was handed both, and a caller holding its own
    /// copies could name one component on the failure while the kill inside landed
    /// on another.
    ///
    /// An absorbed completion — its instance deregistered or already terminal — asks
    /// for nothing at all, the failure included: there is no longer anybody to
    /// attribute it to.
    pub fn completion(
        &mut self,
        page: &mut SurfacePage,
        completion: Completion,
        now: Millis,
        now_ms: u64,
    ) {
        let Completion {
            instance,
            outcome,
            steps,
            drops,
            refusals,
            killed,
            absorbed,
        } = completion;
        if absorbed {
            return;
        }
        self.steps(page, steps);
        for refusal in refusals {
            self.emit(Event::PlaneRefused {
                instance: instance.clone(),
                port: refusal.port,
                channel: refusal.channel,
                reason: refusal.reason,
            });
        }
        match outcome {
            ActivationOutcome::Ok => {}
            ActivationOutcome::Err(err) => self.emit(Event::ActivationFailed {
                instance: instance.clone(),
                message: err.message,
            }),
            ActivationOutcome::Trap(reason) => {
                let killed = killed.expect("surface client: a trap takes its instance terminal");
                self.killed(&instance, reason, killed);
            }
        }
        self.verdicts(page, drops, now, now_ms);
    }

    /// Fold what a confined-release pass produced.
    ///
    /// A release is an ordinary arrival, so its only reaction is the ladder's.
    pub fn released(
        &mut self,
        page: &mut SurfacePage,
        released: Released,
        now: Millis,
        now_ms: u64,
    ) {
        self.verdicts(page, released.drops, now, now_ms);
    }

    /// Fold an outbox pass's answer: the frames it freed, a toast per whole flush
    /// it lost, and the retry deadline when it moved.
    ///
    /// The toast plane and not an alert: a flush can be lost while the link is
    /// down, where an alert composed for one would be written to a socket that is
    /// gone. The per-instance counter carries the total for whoever reconnects
    /// and asks.
    pub fn steps(&mut self, page: &SurfacePage, steps: OutboxSteps<String>) {
        let OutboxSteps {
            frames,
            dropped,
            retry_wakeup,
        } = steps;
        self.frames(page, frames);
        for toast in outward::parked_drop_notices(&dropped) {
            self.control(toast);
        }
        if let Some(change) = retry_wakeup {
            self.effects.push(Effect::SetRetryWakeup(change));
        }
    }

    /// Fold the loudness ladder's verdicts: what the page says about a loss, and
    /// the one loss that kills.
    ///
    /// An announcement becomes one `Alert` for the operator and one toast on the
    /// page's own plane, both carrying the same sentence the kill reason does. Each
    /// `fatal` rung takes its own instance terminal — unless the page no longer
    /// holds it, in which case the announcement above is the whole account of the
    /// loss. Every kill in the set is enacted: one retirement can evict several
    /// instances' positions at once, and each of them was configured to die of it.
    pub fn verdicts(
        &mut self,
        page: &mut SurfacePage,
        verdicts: DropVerdicts,
        now: Millis,
        now_ms: u64,
    ) {
        if verdicts.is_quiet() {
            return;
        }
        let notices = outward::drop_notices(&verdicts);
        for alert in notices.alerts {
            self.send_frame(page, alert);
        }
        for toast in notices.toasts {
            self.control(toast);
        }
        for fatal in verdicts.fatal {
            if !page.registrations.is_registered(&fatal.instance)
                || !page.schedules.is_tracked(&fatal.instance)
            {
                tracing::warn!(
                    instance = %fatal.instance,
                    "surface client: a fatal overflow named an instance the page no longer holds"
                );
                continue;
            }
            let killed = outward::kill(page, &fatal.instance, now, now_ms);
            self.killed(&fatal.instance, fatal.describe(), killed);
        }
    }

    /// Fold the answers a settled publish owes its callers.
    pub fn answers(&mut self, answers: Vec<PublishAnswer>) {
        for answer in answers {
            match answer {
                PublishAnswer::Port {
                    instance,
                    port,
                    correlation,
                    status,
                } => self.emit(Event::PublishResult {
                    instance,
                    port,
                    correlation,
                    status,
                }),
                // Nothing is owed and nothing is retried: the next timer tick or
                // resize publishes a fresh snapshot, so a dropped latest-wins
                // document costs staleness only. The count rides the status
                // document for whoever wants the total.
                PublishAnswer::TelemetryDropped { kind, outcome } => tracing::debug!(
                    ?kind,
                    ?outcome,
                    "surface client: the peer refused a telemetry document"
                ),
            }
        }
    }

    /// Send frames a pass composed, in order.
    ///
    /// See [`send_frame`](Self::send_frame) for what happens to one composed
    /// against a dead link.
    pub fn frames(&mut self, page: &SurfacePage, frames: Vec<ClientFrame>) {
        for frame in frames {
            self.send_frame(page, frame);
        }
    }

    /// Ask the connection to go fatal. The terminal event comes back from it.
    pub fn go_fatal(&mut self, detail: String) {
        self.effects.push(Effect::GoFatal { detail });
    }

    /// Tear the page's half of an attachment down and answer what died with it.
    ///
    /// Shared by the ordinary detach and by each of the three terminal verdicts,
    /// which end an attachment just as finally: after any of them there is no
    /// wire, so a caller still awaiting a publish is owed `ConnectionLost` now
    /// rather than never, and a frame composed against a page that still believed
    /// itself attached would have nowhere to go. Idempotent — a fatal raised
    /// while already detached asks for nothing.
    fn detach(&mut self, page: &mut SurfacePage) {
        let Detached { answers, steps } = page.on_detached();
        self.answers(answers);
        self.steps(page, steps);
    }

    /// What a terminal instance asks for: the timer its discarded queue freed,
    /// and the failure — once, however many rungs or traps named it.
    fn killed(&mut self, instance: &str, reason: String, killed: Killed) {
        if let Some(change) = killed.retry_wakeup {
            self.effects.push(Effect::SetRetryWakeup(change));
        }
        if killed.discarded > 0 {
            tracing::warn!(
                instance,
                discarded = killed.discarded,
                "surface client: discarded the queued flushes of a terminal instance"
            );
        }
        if killed.first {
            self.emit(Event::InstanceFailed {
                instance: instance.to_string(),
                reason,
            });
        }
    }

    /// Send a frame, if there is an attachment to send it on.
    ///
    /// Every frame a pass composes for the wire is composed against a live
    /// attachment except one: an `Alert` the ladder raised, which can fire on a
    /// confined append while the link is down. Dropping it is the best-effort
    /// posture the alert path has always had, and the toast beside it is what
    /// reaches the page either way.
    ///
    /// # Panics
    ///
    /// On any other frame class composed while detached. A subscription frame or a
    /// flush that evaporated here would be an absence of behaviour — a channel that
    /// never resubscribes, an outbox that believes its batch is on the wire — and
    /// silently dropping it is exactly the state that must not be carried on from.
    fn send_frame(&mut self, page: &SurfacePage, frame: ClientFrame) {
        if page.connect.facts().is_none() {
            assert!(
                matches!(frame, ClientFrame::Alert { .. }),
                "surface client: composed {} with no attachment to send it on",
                frame_class(&frame)
            );
            tracing::debug!("surface client: dropped an alert composed with no attachment");
            return;
        }
        self.effects.push(Effect::SendFrame(frame));
    }

    fn control(&mut self, publish: ControlPublish) {
        self.effects.push(Effect::PublishControl {
            channel: publish.channel,
            body: publish.body,
        });
    }

    fn emit(&mut self, event: Event) {
        self.effects.push(Effect::EmitEvent(event));
    }
}

/// A client frame's own name, for the diagnostic that refuses to compose one
/// against a dead link.
fn frame_class(frame: &ClientFrame) -> &'static str {
    match frame {
        ClientFrame::Hello { .. } => "Hello",
        ClientFrame::Subscribe { .. } => "Subscribe",
        ClientFrame::Unsubscribe { .. } => "Unsubscribe",
        ClientFrame::Publish { .. } => "Publish",
        ClientFrame::PublishBatch { .. } => "PublishBatch",
        ClientFrame::Alert { .. } => "Alert",
    }
}

/// The event a configured attachment is announced with, read off the page.
///
/// # Panics
///
/// If no attachment is live or no document is in force — both hold at phase 2 by
/// construction.
fn connected(page: &SurfacePage) -> Event {
    let facts = page
        .connect
        .facts()
        .expect("surface client: a configured page has a live attachment");
    let bindings = page
        .bindings()
        .expect("surface client: a configured page has a document in force");
    Event::Connected {
        bindings: bindings.document().clone(),
        participant_id: facts.participant_id.clone(),
        session_id: facts.session_id.clone(),
        max_body_bytes: facts.max_body_bytes,
        alert_granted: facts.alert_granted,
    }
}

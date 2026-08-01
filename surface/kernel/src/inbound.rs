//! The inbound half of a turn: what a server frame the attachment handed back
//! does to the page.
//!
//! The generic client consumes the frames that are the *connection's* — the
//! version handshake, the attachment contract, the liveness beat — and hands
//! everything else up. Five frames reach the surface layer, and each of them
//! lands in a table the page already holds:
//!
//! - **`SubscribeResult`** settles a channel's wire state. On the config channel
//!   it is also the answer the two-phase connect judges: an empty replay means
//!   the peer retains no wiring for this surface.
//! - **`Deliver`** is either a message a component reads — into the channel's
//!   store, where the next activation windows it — or, on the config channel, the
//!   bindings document itself, which is phase 2.
//! - **`PublishResult`** settles one single publish: a caller's answer, a
//!   swallowed error report, or a telemetry document counted and dropped.
//! - **`PublishBatchResult`** settles one activation's flush, freeing its
//!   instance's outbox or re-parking the refusal.
//! - **`DeferredView`** restates what one of the page's senders has parked on one
//!   transportable channel.
//!
//! # One frame, one pass
//!
//! [`on_server_frame`] takes exactly one frame and answers [`Inbound`] — frames
//! to send, callers owed an answer, the loudness ladder's verdicts, the outboxes'
//! timer instruction, and the document application when the frame carried one.
//! Nothing is enacted here: which event an answer becomes, whether a changed
//! document reloads the page, and how a `fatal` rung's kill is carried out are
//! the caller's, the same seam every surface-side module answers across.
//!
//! # What is fatal
//!
//! An `Err` is a peer contract the page cannot reconcile, and the caller's cue to
//! go fatal on its connection: a delivery on a channel this attachment never had
//! open, a span sequence that does not advance, a correlation nothing sent, a
//! config channel with no document on it, a document this build cannot apply. The
//! page takes its whole configuration from this peer on faith, so a peer that
//! contradicts the contract is not something to carry on from.
//!
//! A *straggler* is not fatal and not an error: a delivery from a span the page
//! has already left, in flight when its `Unsubscribe` crossed. It is discarded,
//! reported once per span, and advances nothing.

#[cfg(test)]
mod tests;

use brenn_attach_client::Millis;
use brenn_attach_client::publish::{BatchAnswer, OutboxSteps};
use brenn_attach_client::subs::DeliverDisposition;
use brenn_attach_proto::{
    ClientFrame, DeferredViewEntry, GapInfo, PublishBatchOutcome, PublishOutcome, ServerFrame,
    SubscribeOutcome,
};
use brenn_envelope::MessageEnvelope;

use crate::activation::DropVerdicts;
use crate::outbound::{LostFlush, PublishAnswer};
use crate::page::{Configured, SurfacePage};

/// A delivery discarded as a previous span's straggler.
///
/// Reported once per post-`Active` window rather than once per straggler:
/// stragglers are peer-paced, so nothing may ride an unbounded diagnostic on
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Straggler {
    pub channel: String,
    /// The discarded delivery's span sequence.
    pub seq: u64,
    /// The loss the peer reported with it, which is discarded along with the
    /// delivery: a straggler advances no position, so there is nothing for the
    /// count to be charged against.
    pub dropped: u64,
}

/// A subscription the peer could not replay to the resume point it was given.
///
/// Diagnostic only. The page's answer to a gap is the subscribe it has already
/// performed: the messages between the cursor and the retained window are gone,
/// and a component sees a first-window-after-resume, which its contract calls
/// unremarkable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelGap {
    pub channel: String,
    /// What the peer is about to replay in place of the resumed stream.
    pub replay_count: u32,
    pub gap: GapInfo,
}

/// What one inbound frame produced.
///
/// Every field defaults to nothing, so a frame answers only what it actually
/// settled — `DeferredView` produces an empty `Inbound`, and no caller has to
/// read a field a frame never touches.
#[derive(Debug, Default, PartialEq)]
pub struct Inbound {
    /// Frames to send, in order: the `Unsubscribe`/`Subscribe` pair a
    /// `SubscribeResult` enacts, and — through
    /// [`configured`](Self::configured) — everything phase 2 composed.
    pub frames: Vec<ClientFrame>,
    /// Callers owed the answer to a publish, and telemetry documents the peer
    /// refused.
    pub answers: Vec<PublishAnswer>,
    /// The loudness ladder's verdicts for positions this frame's arrival
    /// overflowed. An arrival is as accountable a cause of loss as a depth
    /// shrink, and it is charged where it happens.
    pub drops: DropVerdicts,
    /// The outboxes' answer to a settled flush: what goes out now that the
    /// instance's wire is free again, what its cap dropped, and the retry timer.
    pub steps: OutboxSteps<String>,
    /// The bindings document this frame carried, applied — phase 2, whose own
    /// frames, steps and verdicts it carries.
    pub configured: Option<Configured>,
    /// A delivery discarded as a straggler, the first of its span.
    pub straggler: Option<Straggler>,
    /// A gap on a subscription that presented a resume claim.
    pub gap: Option<ChannelGap>,
    /// Flushes lost with the outbox that owed them.
    pub lost_flushes: Vec<LostFlush>,
}

/// Route one server frame the connection handed back.
///
/// `now` is the driver's monotonic reading, which the outboxes' retry deadline is
/// stated against.
///
/// # Panics
///
/// If handed a frame the connection consumes itself — `Hello`, `Welcome` or
/// `Heartbeat`. Those never reach an embedder: this build disagreeing with itself
/// about the client crate's contract is not a state to carry on from.
pub fn on_server_frame(
    page: &mut SurfacePage,
    frame: ServerFrame,
    now: Millis,
) -> Result<Inbound, String> {
    match frame {
        ServerFrame::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => on_subscribe_result(page, channel, outcome, replay_count, gap),
        ServerFrame::Deliver {
            channel,
            envelope,
            seq,
            cursor,
            dropped,
        } => on_deliver(
            page,
            Delivery {
                channel,
                envelope,
                seq,
            },
            cursor,
            dropped,
            now,
        ),
        ServerFrame::PublishResult {
            correlation,
            outcome,
        } => on_publish_result(page, correlation, outcome),
        ServerFrame::PublishBatchResult {
            correlation,
            outcome,
        } => on_batch_result(page, correlation, outcome, now),
        ServerFrame::DeferredView {
            channel,
            attribution,
            entries,
        } => Ok(on_deferred_view(page, channel, attribution, entries)),
        ServerFrame::Hello { .. } => unreachable_frame("Hello"),
        ServerFrame::Welcome { .. } => unreachable_frame("Welcome"),
        ServerFrame::Heartbeat => unreachable_frame("Heartbeat"),
    }
}

fn unreachable_frame(name: &str) -> ! {
    panic!("surface client: the connection consumes {name} itself and routes it to nobody")
}

/// What a `Deliver` says about the message it carries, minus the resume state.
///
/// Bundled because the three travel together and a positional list of a `String`,
/// an envelope and a `u64` beside a `Cursor` and another `u64` is a transposition
/// waiting to typecheck.
struct Delivery {
    channel: String,
    envelope: MessageEnvelope,
    seq: u64,
}

/// Settle one channel's `SubscribeResult`.
///
/// The wire half first, then the config channel's own rule: the acknowledgement
/// is what tells a peer retaining a document from one retaining none, and an
/// empty replay on a cursorless subscription is the latter. An application
/// channel's gap is handed back as a diagnostic.
///
/// A gap means replay could not cover the requested resume point (epoch change,
/// a hole past the retained ring, or a durable resume beyond the retained
/// window). It is a resume-layer fact and stops here: the page's answer is the
/// re-resume it already performed, and the component sees at most a
/// first-window-after-resubscribe, which the contract defines as unremarkable.
///
/// TODO(processor-typed-gaps): this classification exists only on the surface's
/// resume layer. A wasmtime-hosted component gets no equivalent signal; backend
/// adoption rides the next `processor.wit` world bump.
fn on_subscribe_result(
    page: &mut SurfacePage,
    channel: String,
    outcome: SubscribeOutcome,
    replay_count: u32,
    gap: Option<GapInfo>,
) -> Result<Inbound, String> {
    let ack = page
        .subs
        .on_subscribe_result(&channel, outcome, replay_count, gap)?;
    if page.connect.is_config_channel(&channel) {
        // Errors before the gap is reported below: on this channel a gap answers
        // a resume claim the page never made, which is a broken peer rather than
        // something to describe.
        page.on_config_ack(&ack)?;
    }
    Ok(Inbound {
        frames: ack.frames,
        gap: ack.gap.map(|gap| ChannelGap {
            channel,
            replay_count: ack.replay_count,
            gap,
        }),
        ..Inbound::default()
    })
}

/// Take one delivered envelope.
///
/// Three outcomes. A straggler is discarded and reported. The config channel's
/// delivery is the page's wiring and runs phase 2. Anything else goes into the
/// channel's store — for every subscribed channel uniformly, registered readers
/// or not, because retention is what makes a message recoverable and is not a
/// fact about who is listening.
///
/// Arrival moves no position, which is what coalesces a turn's deliveries into
/// one activation. A position the arrival outran is charged here, at the arrival
/// that caused it, rather than at a window the binding may never reach.
///
/// # Panics
///
/// If an accepted delivery names a channel with no store, or arrives with no
/// wiring in force. Both are unreachable through the page's own passes: a channel
/// is subscribed only by the registration reconcile, which runs after the store
/// pass of the document that named it.
fn on_deliver(
    page: &mut SurfacePage,
    delivery: Delivery,
    cursor: brenn_attach_proto::Cursor,
    dropped: u64,
    now: Millis,
) -> Result<Inbound, String> {
    let Delivery {
        channel,
        envelope,
        seq,
    } = delivery;
    let accepted = match page.subs.on_deliver(&channel, seq, cursor, dropped)? {
        DeliverDisposition::Accept { dropped } => dropped,
        DeliverDisposition::Discard { first } => {
            return Ok(Inbound {
                straggler: first.then_some(Straggler {
                    channel,
                    seq,
                    dropped,
                }),
                ..Inbound::default()
            });
        }
    };
    if page.connect.is_config_channel(&channel) {
        // The page's own wiring, not a message any component reads: it has no
        // store and no reader, and a loss the peer reports on it is a superseded
        // document rolling out of a one-deep retained window — the delivery in
        // hand is the current one either way.
        let configured = page.apply_config(&envelope.body, now)?;
        return Ok(Inbound {
            configured: Some(configured),
            ..Inbound::default()
        });
    }
    let overflow = {
        let store = page.stores.get_mut(&channel).unwrap_or_else(|| {
            panic!("surface client: delivery on {channel:?}, which the page retains nothing for")
        });
        // The peer's figure is the channel's, so every position on it takes the
        // full count: each of them missed those messages, and no page-side
        // arithmetic can see a loss that happened upstream of the page.
        store.count_server_drops(accepted);
        // Idempotent by `message_id`: several legitimate paths re-present what
        // the store already holds, a resubscribed channel replaying into a store
        // the page kept most of all.
        store.insert(envelope)
    };
    let bindings = page
        .connect
        .bindings()
        .expect("surface client: a subscribed channel implies a document in force");
    let drops = page.schedules.charge_overflow(bindings, &channel, overflow);
    Ok(Inbound {
        drops,
        ..Inbound::default()
    })
}

/// Settle one single publish.
fn on_publish_result(
    page: &mut SurfacePage,
    correlation: Option<u64>,
    outcome: PublishOutcome,
) -> Result<Inbound, String> {
    let answer = page.outbound.on_publish_result(correlation, outcome)?;
    Ok(Inbound {
        answers: answer.into_iter().collect(),
        ..Inbound::default()
    })
}

/// Settle one activation's flush.
///
/// An `Ok` frees the instance's wire and pumps whatever queued behind the frame;
/// a refusal re-parks the flush at the head of its outbox and arms the retry. A
/// refusal answered after the outbox closed is the one loss that has nowhere to
/// wait, and it is reported.
fn on_batch_result(
    page: &mut SurfacePage,
    correlation: u64,
    outcome: PublishBatchOutcome,
    now: Millis,
) -> Result<Inbound, String> {
    let BatchAnswer {
        steps,
        registrant,
        lost,
    } = page.outbound.on_batch_result(correlation, outcome, now)?;
    Ok(Inbound {
        steps,
        lost_flushes: lost
            .into_iter()
            .map(|batch| LostFlush {
                instance: registrant.clone(),
                batch,
            })
            .collect(),
        ..Inbound::default()
    })
}

/// Take one restatement of what a sender has parked on a transportable channel.
///
/// A full snapshot, so it replaces the mirror wholesale — idempotent,
/// last-writer-wins, and an empty one legitimately means the set is empty.
/// Nothing is validated against the wiring: a view for a pair the page no longer
/// binds is inert, since no activation reads it, and refusing it would make an
/// ordinary reconnect race fatal.
///
/// Wakes nobody. A schedule changing is not an arrival — only its release is, and
/// a released message reaches the page as an ordinary `Deliver`.
fn on_deferred_view(
    page: &mut SurfacePage,
    channel: String,
    attribution: Option<String>,
    entries: Vec<DeferredViewEntry>,
) -> Inbound {
    page.views.on_view(channel, attribution, entries);
    Inbound::default()
}

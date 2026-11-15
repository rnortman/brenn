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
//! - **`Deliver`** is one delivery pass on one channel: rows a component reads —
//!   into the channel's store, where the turn's single activation windows the
//!   whole pass at once — or, on the config channel, the bindings document
//!   itself, which is phase 2.
//! - **`PublishResult`** settles one single publish: a caller's answer, a
//!   swallowed error report, or a telemetry document counted and dropped.
//! - **`PublishBatchResult`** settles one activation's flush, freeing its
//!   instance's outbox or re-parking the refusal.
//! - **`DeferredView`** restates what one of the page's senders has parked on one
//!   transportable channel.
//!
//! The blob answers are settled once on the page's plane and split by who
//! asked: a fetch's go to the page's fetcher ([`crate::fetch`]), which also
//! takes every application row a `Deliver` carries — it stores a row at once,
//! or holds it in the channel's arrival hold until the attachments the page
//! fetches of it have settled — and an upload's go to the stage
//! ([`crate::uploads`]).
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
//! open, a pass with no rows, a span sequence that does not advance, a
//! correlation nothing sent, a config channel with no document on it. The page
//! takes its whole configuration from this peer on faith, so a peer that
//! contradicts the contract is not something to carry on from.
//!
//! A document this build cannot apply is not an `Err`: it is peer skew — the
//! page's assets are older than the server — and is carried as
//! [`Inbound::unusable_document`] for the caller to reload on.
//!
//! A *straggler* is not fatal and not an error: a pass from a span the page has
//! already left, in flight when its `Unsubscribe` crossed. It is discarded whole,
//! reported once per span, and advances nothing.

#[cfg(test)]
mod tests;

use brenn_attach_client::Millis;
use brenn_attach_client::publish::{BatchAnswer, OutboxSteps};
use brenn_attach_client::subs::{DeliverDisposition, SubscribeSettlement};
use brenn_attach_proto::{
    ClientFrame, DeferredViewEntry, DeliverRow, GapInfo, PublishBatchOutcome, PublishOutcome,
    ServerFrame, SubscribeOutcome,
};

use crate::activation::DropVerdicts;
use crate::bindings::channel_carries;
use crate::blob_plane::PlaneAnswer;
use crate::fetch::{self, FetchReport, Fetched};
use crate::outbound::{LostFlush, PublishAnswer};
use crate::page::{Configured, HandOver, SurfacePage};
use crate::uploads::Lost;

/// A delivery pass discarded as a previous span's straggler.
///
/// Reported once per post-`Active` window rather than once per straggler:
/// stragglers are peer-paced, so nothing may ride an unbounded diagnostic on
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Straggler {
    pub channel: String,
    /// The span sequence of the pass's first discarded row. The pass is
    /// discarded whole, so this names where it started rather than everything it
    /// carried.
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
    /// The bindings documents this frame carried, applied in frame order —
    /// phase 2, whose own frames, steps and verdicts each one carries.
    pub configured: Vec<Configured>,
    /// A bindings document this build cannot apply, with the reason
    /// [`SurfacePage::apply_config`] gave. It follows every document in
    /// [`configured`](Self::configured), and nothing after it in the pass was
    /// applied.
    pub unusable_document: Option<String>,
    /// A delivery pass discarded as a straggler, the first of its span.
    pub straggler: Option<Straggler>,
    /// A gap on a subscription that presented a resume claim.
    pub gap: Option<ChannelGap>,
    /// Flushes lost with the outbox that owed them.
    pub lost_flushes: Vec<LostFlush>,
    /// What the fetcher reports for the page's error channel.
    pub reports: Vec<FetchReport>,
    /// Attachment flushes the stage lost to this frame: a refused upload or
    /// open, or a flush handed over that the contract in force refused.
    pub staged_lost: Vec<Lost>,
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
        ServerFrame::Deliver { channel, rows } => on_deliver(page, channel, rows, now),
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
        frame @ (ServerFrame::BlobCreateResult { .. }
        | ServerFrame::BlobSealResult { .. }
        | ServerFrame::BlobOpenResult { .. }
        | ServerFrame::BlobReadResult { .. }) => on_blob_frame(page, frame, now),
    }
}

fn unreachable_frame(name: &str) -> ! {
    panic!("surface client: the connection consumes {name} itself and routes it to nobody")
}

/// Settle one blob answer on the page's plane and hand each event to whoever
/// asked: the fetcher, then the stage. An answer the plane is not owed is
/// fatal on the connection, and so is one the stage cannot reconcile with the
/// reference the server itself delivered.
///
/// The stage moves on every blob answer, whoever it was for: a fetch that
/// finished may have freed the room an upload is waiting for.
fn on_blob_frame(
    page: &mut SurfacePage,
    frame: ServerFrame,
    now: Millis,
) -> Result<Inbound, String> {
    let PlaneAnswer {
        frames,
        fetch,
        upload,
    } = page.plane.on_frame(frame)?;
    let Fetched {
        frames,
        drops,
        reports,
    } = fetch::on_blob_events(page, frames, fetch, now)?;
    let mut inbound = Inbound {
        frames,
        drops,
        reports,
        ..Inbound::default()
    };
    let SurfacePage {
        connect,
        outbound,
        stage,
        plane,
        ..
    } = page;
    let Some(bindings) = connect.bindings() else {
        assert!(
            upload.is_empty(),
            "surface client: an upload answered before any document was in force"
        );
        return Ok(inbound);
    };
    HandOver {
        stage,
        plane,
        outbound,
        bindings,
        facts: connect.facts(),
        now,
    }
    .try_drive(
        &mut inbound.steps,
        &mut inbound.staged_lost,
        |stage, plane, gate| stage.on_events(plane, upload, gate),
    )?;
    Ok(inbound)
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
    let settlement = page
        .subs
        .on_subscribe_result(&channel, outcome, replay_count, gap)?;
    let ack = match settlement {
        SubscribeSettlement::Opened(ack) => ack,
        // Every channel a surface subscribes is provisioned before the backend
        // accepts a connection and named by the wiring it delivered. The server
        // answers `Unavailable` for a non-text channel only to a v4 session,
        // and for a channel absent from its directory, which a reload never
        // leaves it while a session that may name the channel is attached. So
        // it is an answer this route cannot legally produce — a broken peer,
        // like any other.
        SubscribeSettlement::Unavailable => {
            return Err(format!(
                "the peer answered Unavailable on {channel}, a channel its own wiring names"
            ));
        }
    };
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

/// Take one delivery pass — every row one `Deliver` carried on one channel.
///
/// Three outcomes. A straggling pass is discarded whole and reported once. The
/// config channel's rows are the page's wiring and each runs phase 2. Anything
/// else is checked against the channel's declaration — a non-text row on a
/// channel the wiring declares text-only is the peer's fault and an `Err`,
/// before any row is stored — and goes into the channel's store — for every subscribed channel uniformly,
/// registered readers or not, because retention is what makes a message
/// recoverable and is not a fact about who is listening.
///
/// **A pass lands before anything windows it.** Arrival moves no position, so
/// inserting N rows leaves every binding's cursor where it was and the turn's
/// single activation pass windows the whole pass at once — which is what caps a
/// catch-up's new slice at the binding's `push_depth` instead of presenting each
/// row as its own arrival. A position the pass outran is charged here, at the
/// arrival that caused it, rather than at a window the binding may never reach.
///
/// # Panics
///
/// If an accepted delivery names a channel with no store, or arrives with no
/// wiring in force. Both are unreachable through the page's own passes: a channel
/// is subscribed only by the registration reconcile, which runs after the store
/// pass of the document that named it.
fn on_deliver(
    page: &mut SurfacePage,
    channel: String,
    rows: Vec<DeliverRow>,
    now: Millis,
) -> Result<Inbound, String> {
    let accepted = match page.subs.on_deliver(&channel, &rows)? {
        DeliverDisposition::Accept { dropped } => dropped,
        DeliverDisposition::Discard { first, dropped } => {
            let head = rows
                .first()
                .expect("attach client: an empty pass is refused before it is dispositioned");
            return Ok(Inbound {
                straggler: first.then_some(Straggler {
                    channel,
                    seq: head.seq,
                    dropped,
                }),
                ..Inbound::default()
            });
        }
    };
    if page.connect.is_config_channel(&channel) {
        // The page's own wiring, not a message any component reads: it has no
        // store and no reader, and a loss the peer reports on it is a superseded
        // document rolling out of a one-deep retained window — the newest
        // document of the pass is the current one either way.
        //
        // Rows apply in order and the first one this build cannot apply ends
        // the pass: the rows before it are folded as usual and the refusal
        // rides behind them, so its reload is the last word on the pass. A
        // refused document is peer skew, not a broken contract, so it is
        // carried rather than returned as an `Err`.
        //
        // Each applied document reconciles the fetcher too: a gated channel
        // the attachment opened no blob door for is a broken peer, and a
        // channel the document dropped takes its hold and cache with it.
        let mut configured = Vec::new();
        let mut unusable_document = None;
        let mut reports = Vec::new();
        for row in &rows {
            let applied = match row.envelope.body.as_text() {
                Some(text) => page.apply_config(text, now),
                None => Err("the configuration document is not text".to_string()),
            };
            match applied {
                Ok(mut applied) => {
                    let fetched = fetch::on_config(page, now)?;
                    applied.frames.extend(fetched.frames);
                    applied.drops.merge(fetched.drops);
                    reports.extend(fetched.reports);
                    configured.push(applied);
                }
                Err(detail) => {
                    unusable_document = Some(detail);
                    break;
                }
            }
        }
        return Ok(Inbound {
            configured,
            unusable_document,
            reports,
            ..Inbound::default()
        });
    }
    // The server builds the document and its directory from one plan, and a
    // reload that changes a declaration closes the session first, so a non-text
    // row on a channel the document declares text-only is a broken peer.
    let declared_non_text = page
        .connect
        .bindings()
        .expect("surface client: a subscribed channel implies a document in force")
        .carries_non_text(&channel);
    if let Some(row) = rows
        .iter()
        .find(|row| !channel_carries(declared_non_text, &row.envelope.body))
    {
        return Err(format!(
            "the peer delivered a non-text body ({}) on {channel}, which its own wiring declares \
             text-only",
            row.envelope.body.summary()
        ));
    }
    // The peer's figure is the channel's, so every position on it takes the
    // full count: each of them missed those messages, and no page-side
    // arithmetic can see a loss that happened upstream of the page.
    page.stores
        .get_mut(&channel)
        .unwrap_or_else(|| {
            panic!("surface client: delivery on {channel:?}, which the page retains nothing for")
        })
        .count_server_drops(accepted);
    // Inserted by the fetcher, which stores a row in this pass unless the
    // channel's arrival hold is holding rows or the row names an attachment the
    // page fetches. Insertion is idempotent by `message_id`: several legitimate
    // paths re-present what the store already holds, a resubscribed channel
    // replaying into a store the page kept most of all.
    let Fetched {
        frames,
        drops,
        reports,
    } = fetch::on_rows(
        page,
        &channel,
        rows.into_iter().map(|row| row.envelope).collect(),
        now,
    );
    Ok(Inbound {
        frames,
        drops,
        reports,
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
    // The surface route answers a non-text body offered to a text-only channel
    // with a protocol close, never with that outcome; and a batch names only
    // handles the server answered held on this attachment before it was sent,
    // so the server has no cause to answer that one. A peer that sends either
    // is not the server this page was built against.
    if matches!(
        outcome,
        PublishBatchOutcome::TextOnlyChannel | PublishBatchOutcome::AttachmentNotHeld
    ) {
        return Err(format!(
            "PublishBatchResult {correlation} answered {outcome:?}, which the surface route \
             never sends"
        ));
    }
    let BatchAnswer {
        steps,
        registrant,
        lost,
        outcome: _,
    } = page.outbound.on_batch_result(correlation, outcome, now)?;
    let mut inbound = Inbound {
        steps,
        lost_flushes: lost
            .into_iter()
            .map(|batch| LostFlush {
                instance: registrant.clone(),
                batch,
            })
            .collect(),
        ..Inbound::default()
    };
    // The answer finishes or re-parks a handed attachment flush, and frees the
    // instance's outbox for the next one its stage holds.
    let SurfacePage {
        connect,
        outbound,
        stage,
        plane,
        ..
    } = page;
    let bindings = connect
        .bindings()
        .expect("surface client: a batch was sent under a document in force");
    HandOver {
        stage,
        plane,
        outbound,
        bindings,
        facts: connect.facts(),
        now,
    }
    .drive(
        &mut inbound.steps,
        &mut inbound.staged_lost,
        |stage, plane, gate| stage.on_batch_answer(&registrant, outcome, plane, gate),
    );
    Ok(inbound)
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

//! The attachment's subscription plane: what an attacher may open, what it is
//! owed once open, and how each delivery reaches the socket.
//!
//! Everything here is keyed by channel and nothing else. An attachment holds at
//! most one subscription per channel, so a delivery is a per-channel fact of the
//! connection: one `Deliver` per (attachment, channel, delivery pass), whose rows
//! carry that channel's own span seqs, resume cursors, and drop count. Whatever
//! sits behind the channel on the attacher's side — one binding, six bindings, a
//! daemon's own bookkeeping — is the attacher's fan-out and is invisible here.
//!
//! A **pass** is everything one run of one send path produced: a subscribe's
//! replay, a drain's suffix, a single live row. One pass is one frame, so a
//! multi-row catch-up reaches the attacher as one delivery point rather than as
//! N — which is what makes the attacher's window arithmetic cap the whole
//! catch-up at its `push_depth` instead of presenting every row as its own
//! arrival.
//!
//! One code path serves every transportable channel: the only channel
//! characteristic this module reads is what the profile's fold says about it,
//! because only transportable channels cross the websocket at all.

#![allow(dead_code)]

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brenn_attach_proto::{
    Cursor, DeliverRow, GapInfo, GapReason as ProtoGapReason, ServerFrame, SubscribeOutcome,
};
use brenn_lib::messaging::store::{ResumeCursor, gap_suffix};
use brenn_lib::messaging::{GapReason as BusGapReason, MessageEnvelope, Replay};
use brenn_lib::token_bucket::{TokenBucket, TokenBucketOutcome};
use tracing::{debug, warn};
use uuid::Uuid;

use super::cursor::{self, CursorState};
use super::profile::{AttachProfile, SubscriptionFacts};
use super::registry::{DeferredViewPush, LiveDelivery, SessionPush};
use super::session::{AttachSessionCtx, FrameOutcome, SessionCounters, sanitize_client_detail};

/// One `Subscribe`/`Unsubscribe` token refilled per this interval.
pub const SUBSCRIBE_REFILL: Duration = Duration::from_secs(1);

/// The per-connection `Subscribe`/`Unsubscribe` bucket, sized from the profile's
/// own burst policy. Starts full, so an attacher's first-connect reconcile is
/// admitted before limiting begins.
pub fn subscribe_bucket(profile: &dyn AttachProfile) -> TokenBucket {
    TokenBucket::new(profile.subscribe_burst(), SUBSCRIBE_REFILL, 1)
}

/// The connection's active subscriptions — every one of them, whichever store
/// holds the channel's retention, each with the facts its `Subscribe` resolved.
///
/// `active` (the local mirror) and the registry-shared `shared` set move
/// strictly together through [`activate`](Self::activate) /
/// [`deactivate`](Self::deactivate) — the two-set sync discipline lives here and
/// nowhere else, so no handler can update one set and forget the other.
///
/// Duplicate suppression is not held here: a subscription's position
/// ([`WireCursors::pos_of`]) is the only record of what this connection has
/// sent, so a second copy of a position — the replay racing the live fan-out —
/// is at or below it and dropped.
pub struct ActiveChannels {
    active: HashMap<String, SubscriptionFacts>,
    shared: Arc<Mutex<HashSet<String>>>,
}

impl ActiveChannels {
    pub fn new(shared: Arc<Mutex<HashSet<String>>>) -> Self {
        Self {
            active: HashMap::new(),
            shared,
        }
    }

    /// Insert the channel into both the local and registry-shared active sets,
    /// recording the facts its subscribe resolved. Inserting into `shared` is
    /// what makes the router start queuing live rows, so callers activate before
    /// the store read.
    pub fn activate(&mut self, channel: &str, facts: SubscriptionFacts) {
        self.shared
            .lock()
            .expect("active_channels poisoned")
            .insert(channel.to_string());
        self.active.insert(channel.to_string(), facts);
    }

    /// Remove the channel from both active sets. Returns whether it was active —
    /// the Unsubscribe-of-non-active violation check.
    pub fn deactivate(&mut self, channel: &str) -> bool {
        let was_active = self.active.remove(channel).is_some();
        if was_active {
            self.shared
                .lock()
                .expect("active_channels poisoned")
                .remove(channel);
        }
        was_active
    }

    /// The facts this connection's subscription on `channel` resolved, or `None`
    /// if it holds none.
    pub fn facts(&self, channel: &str) -> Option<SubscriptionFacts> {
        self.active.get(channel).copied()
    }

    pub fn is_active(&self, channel: &str) -> bool {
        self.active.contains_key(channel)
    }

    /// Every active channel, for the drain sweep.
    fn channels(&self) -> Vec<String> {
        self.active.keys().cloned().collect()
    }
}

/// Session-owned per-channel wire position state: the delivery-time span seq
/// counters and the store positions cursors are minted from. There is one
/// serialized writer per connection, so this state needs no locking.
///
/// A span seq is a per-channel counter reset to 0 at each `Subscribe` (the span
/// its `SubscribeResult` opens), incremented per `Deliver`, so the first delivery
/// on a span carries seq 1. Minting at the socket-write boundary makes per-span
/// monotonicity structural: nothing the router queues or a delayed release
/// re-orders can produce a wire regression.
///
/// A channel's position is `max(position presented at the resume anchor,
/// positions delivered this connection)`. A cursor is minted from the position
/// *after* advancing it to `max(position, this row's seq)`, so a delayed-release
/// row below the position leaves it unmoved and repeats the unmoved cursor — no
/// duplicate replay next reconnect — while its wire seq is still the next
/// monotone span seq.
pub struct WireCursors {
    span_seq: HashMap<String, u64>,
    /// The store's boot counter, resolved once at boot — a per-process constant
    /// minted into every cursor. It catches the one staleness a store position's
    /// epoch cannot: a backup restore that keeps epochs but rolls positions
    /// backwards.
    incarnation: i64,
    /// The connection-resident position of every active subscription, in the
    /// store's own resume shape. Epoch and seq travel together by construction,
    /// so a position can never be minted against a numbering domain that did not
    /// assign it.
    pos: HashMap<String, ResumeCursor>,
}

impl WireCursors {
    pub fn new(incarnation: i64) -> Self {
        Self {
            span_seq: HashMap::new(),
            incarnation,
            pos: HashMap::new(),
        }
    }

    /// The store incarnation every cursor on this connection is stamped with.
    pub fn incarnation(&self) -> i64 {
        self.incarnation
    }

    /// Reset the span counter for `channel` to 0 and anchor its position at
    /// `(epoch, anchor)`. Called at every successful `Subscribe`, before the
    /// `SubscribeResult` and replay, so the span's first `Deliver` mints seq 1.
    pub fn start_span(&mut self, channel: &str, epoch: Uuid, anchor: u64) {
        self.span_seq.insert(channel.to_string(), 0);
        self.pos
            .insert(channel.to_string(), ResumeCursor { epoch, seq: anchor });
    }

    /// Drop all wire state for `channel` (unsubscribe / teardown).
    pub fn clear(&mut self, channel: &str) {
        self.span_seq.remove(channel);
        self.pos.remove(channel);
    }

    /// The channel's current position — the newest retention position written to
    /// this socket for it, in the numbering domain that assigned it. A send at or
    /// below it is a second copy of a position the client already has (the replay
    /// racing the live fan-out) and is dropped. `None` if no span was anchored
    /// (no `Subscribe` yet).
    pub fn pos_of(&self, channel: &str) -> Option<ResumeCursor> {
        self.pos.get(channel).copied()
    }

    /// The next span seq for `channel`. Panics if no span was started — every
    /// `Deliver` follows a `Subscribe` that started one.
    fn next_seq(&mut self, channel: &str) -> u64 {
        let seq = self
            .span_seq
            .get_mut(channel)
            .expect("attach session: Deliver on a channel with no started span");
        *seq += 1;
        *seq
    }

    /// The `(span seq, cursor)` for a `Deliver` of the row at retention position
    /// `retained_seq`. Advances the position to `max(position, retained_seq)` and
    /// mints the cursor from it.
    pub fn next(&mut self, channel: &str, retained_seq: u64) -> (u64, Cursor) {
        let seq = self.next_seq(channel);
        let incarnation = self.incarnation;
        let pos = self
            .pos
            .get_mut(channel)
            .expect("attach session: Deliver on a channel with no anchored position");
        pos.seq = pos.seq.max(retained_seq);
        (seq, cursor::mint(incarnation, channel, *pos))
    }
}

/// One row of a delivery pass, before its wire facts are minted.
pub struct PassRow {
    pub envelope: MessageEnvelope,
    /// The channel's loss count charged to this row. The loss belongs to the
    /// subscription rather than to a message, so only the first row that follows
    /// it carries it and the rest of the pass carries `0`.
    pub dropped: u64,
    /// The row's position in the channel's retention, which its cursor is minted
    /// from.
    pub retained_seq: u64,
}

/// Mint each row's span seq and cursor and write the whole pass as one
/// `Deliver`.
///
/// The single socket-write boundary for deliveries, and the one place span seqs
/// are assigned, so per-span monotonicity is structural — within a pass as well
/// as across passes.
///
/// A pass with no rows writes no frame: the wire's `rows` list is never empty,
/// and a send path that found nothing to serve has nothing to say.
pub async fn send_pass(
    ctx: &AttachSessionCtx,
    cursors: &mut WireCursors,
    channel: &str,
    pass: Vec<PassRow>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    if pass.is_empty() {
        return FrameOutcome::Continue;
    }
    let rows = pass
        .into_iter()
        .map(
            |PassRow {
                 envelope,
                 dropped,
                 retained_seq,
             }| {
                let (seq, cursor) = cursors.next(channel, retained_seq);
                DeliverRow {
                    envelope,
                    seq,
                    cursor,
                    dropped,
                }
            },
        )
        .collect();
    let frame = ServerFrame::Deliver {
        channel: channel.to_string(),
        rows,
    };
    super::session::send_frame(&ctx.tx, frame, counters).await
}

/// Write one deferred-view snapshot to this connection.
pub async fn send_deferred_view(
    ctx: &AttachSessionCtx,
    view: DeferredViewPush,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let DeferredViewPush {
        channel,
        attribution,
        entries,
    } = view;
    let frame = ServerFrame::DeferredView {
        channel,
        attribution,
        entries,
    };
    super::session::send_frame(&ctx.tx, frame, counters).await
}

/// Write one turn's worth of pushes, splitting the two planes that share the
/// session's push queue.
///
/// The retained rows go first, as one coalesced pass, so the sequencing
/// [`send_live`] does is not broken up by an interleaved view. The views follow
/// in arrival order, which is emission order — each is a full replacement, so the
/// last one for a `(channel, attribution)` is the answer the attacher keeps.
pub async fn send_session_pushes(
    ctx: &AttachSessionCtx,
    active: &ActiveChannels,
    cursors: &mut WireCursors,
    pushes: Vec<SessionPush>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let mut rows = Vec::new();
    let mut views = Vec::new();
    for push in pushes {
        match push {
            SessionPush::Live(delivery) => rows.push(delivery),
            SessionPush::DeferredView(view) => views.push(view),
        }
    }
    if !rows.is_empty()
        && let FrameOutcome::Disconnect = send_live(ctx, active, cursors, rows, counters).await
    {
        return FrameOutcome::Disconnect;
    }
    for view in views {
        if let FrameOutcome::Disconnect = send_deferred_view(ctx, view, counters).await {
            return FrameOutcome::Disconnect;
        }
    }
    FrameOutcome::Continue
}

/// Send one turn's worth of live router deliveries, in queue order. A row whose
/// channel this connection does not hold, or whose channel the delivery floor
/// denies, is skipped before any of the decisions below.
///
/// No coalescing across targets: the attachment holds one subscription per
/// channel, so one message reaches it once and a `Deliver` names a channel and
/// nothing else. The channel's position decides each copy, at send time, because
/// a send inside this batch moves it:
///
/// - at or below it — a second copy of a position this connection already wrote
///   (the fan-out racing the subscribe replay or a drain). Dropped: the client's
///   cursor already covers it.
/// - exactly one above it — the contiguous next position. Sent.
/// - further above it — something below this position never reached the wire
///   (a quiet row nobody woke for, a frame this session's queue was too full to
///   take). Sending it alone would strand the interior span under a position
///   that had moved past it, so the live copy is dropped and the channel is
///   served its whole suffix from retention instead.
pub async fn send_live(
    ctx: &AttachSessionCtx,
    active: &ActiveChannels,
    cursors: &mut WireCursors,
    batch: Vec<LiveDelivery>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    for LiveDelivery {
        envelope,
        retained_seq,
    } in batch
    {
        let channel = envelope.channel.as_str();
        if !active.is_active(channel) {
            debug!(
                channel,
                retained_seq, "live delivery for inactive subscription; dropping"
            );
            continue;
        }
        // Floor parity with the subscribe replay and the drain: all three send
        // paths ask the same question, so a policy that denies the channel
        // suppresses the whole subscription rather than whichever rows happened
        // to arrive contiguously.
        if !ctx.policy.allows_channel_access(channel) {
            warn!(
                channel,
                "attach live delivery: delivery floor denied; sending nothing"
            );
            continue;
        }
        let pos = cursors
            .pos_of(channel)
            .expect(
                "attach session: live delivery on a channel with no anchored position — \
                 activation anchors one",
            )
            .seq;
        if retained_seq <= pos {
            debug!(
                channel,
                retained_seq,
                position = pos,
                "live delivery at or below the channel position; dropping the duplicate"
            );
        } else if retained_seq == pos + 1 {
            // A live row is its own pass: distinct live publishes are distinct
            // delivery points, and coalescing them would merge arrivals the bus
            // kept apart.
            let pass = vec![PassRow {
                envelope: (*envelope).clone(),
                dropped: 0,
                retained_seq,
            }];
            if let FrameOutcome::Disconnect = send_pass(ctx, cursors, channel, pass, counters).await
            {
                return FrameOutcome::Disconnect;
            }
        } else {
            debug!(
                channel,
                retained_seq,
                position = pos,
                "live delivery above the contiguous next position; serving the channel its \
                 suffix from retention instead"
            );
            if let FrameOutcome::Disconnect =
                drain_channel(ctx, active, cursors, channel, counters).await
            {
                return FrameOutcome::Disconnect;
            }
        }
    }
    FrameOutcome::Continue
}

/// Charge one token for a `Subscribe`/`Unsubscribe` frame. An exhausted bucket is
/// a protocol violation, not a silent drop: dropping a Subscribe would desync the
/// attacher's subscription state machine, and a subscribe storm is not something
/// a correct client produces — the posture treats it as fail2ban signal. The
/// bucket starts full and admits the profile's whole burst, so an honest
/// maximum-size attacher's first-connect reconcile plus one detach/re-attach
/// cycle never trips it.
pub fn charge_subscribe_token(
    ctx: &AttachSessionCtx,
    subscribe_bucket: &mut TokenBucket,
) -> Result<(), FrameOutcome> {
    match subscribe_bucket.try_consume() {
        TokenBucketOutcome::Granted | TokenBucketOutcome::GrantedAfterSuppression { .. } => Ok(()),
        TokenBucketOutcome::Denied { .. } => {
            Err(ctx.violation("Subscribe/Unsubscribe rate exceeded"))
        }
    }
}

/// Handle an `Unsubscribe` frame.
///
/// Fire-and-forget: an active subscription is removed — clearing the
/// shared/local sets stops the router fan-out — with no response frame. A
/// channel with no active subscription is a violation: only
/// `SubscribeOutcome::Ok` creates one, and a correct client tracks that.
///
/// `Deliver` frames for the removed channel may still sit in the outbound queue
/// or the session's live push queue and arrive after this; the client contract
/// (proto crate docs) is to discard them. The removal also clears the channel's
/// span — so a live copy still queued from this span is dropped rather than
/// delivered. A re-subscribe re-anchors from the client's echoed cursor and is
/// served from retention, so nothing carries across the cycle server-side.
pub fn handle_unsubscribe(
    ctx: &AttachSessionCtx,
    active: &mut ActiveChannels,
    cursors: &mut WireCursors,
    channel: &str,
) -> FrameOutcome {
    // Unknown, unsubscribable, and never-active channels are indistinguishable
    // on the wire (no existence oracle): all violate.
    if active.deactivate(channel) {
        cursors.clear(channel);
        return FrameOutcome::Continue;
    }
    ctx.violation(format!(
        "Unsubscribe of non-active subscription {}",
        sanitize_client_detail(channel)
    ))
}

/// Parse an echoed resume [`Cursor`] and confirm it belongs to `channel`,
/// mapping either failure to the protocol violation it is. A conforming client
/// cannot produce one — cursors are minted by this server, per channel, and
/// echoed verbatim — so both kill the connection and log for fail2ban.
///
/// The channel check is not redundant with the store's epoch check: every ring
/// store in the process numbers under one shared epoch, so a position minted on
/// one ephemeral channel would otherwise resolve as a position in another's
/// numbering — silently repositioning a subscription and reporting no gap.
///
/// The parse cause names the offending token, so it is client-influenced content
/// and takes the same sanitizer every other violation detail does: the security
/// log line and the operator alert it feeds are bounded by the connection, not by
/// how much a hostile client chose to put in a field.
fn parse_resume_cursor(
    ctx: &AttachSessionCtx,
    cursor: &Cursor,
    channel: &str,
) -> Result<CursorState, FrameOutcome> {
    let state = cursor::parse(cursor).map_err(|detail| {
        ctx.violation(format!(
            "unparseable resume cursor on {}: {}",
            sanitize_client_detail(channel),
            sanitize_client_detail(&detail),
        ))
    })?;
    if state.channel != channel {
        // The cursor's own channel is not echoed: it is a channel this attacher
        // may hold a cursor for, and naming it back would confirm which.
        return Err(ctx.violation(format!(
            "resume cursor minted for another channel, echoed on {}",
            sanitize_client_detail(channel),
        )));
    }
    Ok(state)
}

/// The one mapping from a store's replay decision to the wire's answer: the
/// `SubscribeResult` gap, and the position the channel's span anchors at.
///
/// The anchor rule is part of the contract. `UpToDate` and `Exact` keep the
/// echoed position — everything at or below it the client already holds.
/// `Fresh` and every gap anchor at 0, below every assigned position, because in
/// all of them the client's mirror is discarded and the whole answer is new.
///
/// `ResumeAhead` — a position above anything the channel ever assigned — is
/// answered as a fresh attach, never a kill. An honest client reaches it: a
/// store restored from backup re-climbs its positions under a cursor the
/// attacher is still holding. The answer costs nothing a bare re-subscribe would
/// not also give (the retained window), the parse gate still kills a malformed
/// token, and the `warn!` is the observability signal — worded as a candidate
/// cause, because the position inside the cursor is client-echoed and the
/// encoding is opaque, not authenticated.
///
/// `echoed_seq` is the position the client echoed, absent on a first subscribe.
/// `epoch` is the numbering domain the store answered in.
fn resume_answer(
    decision: Replay,
    echoed_seq: Option<u64>,
    channel: &str,
    epoch: Uuid,
) -> (Option<GapInfo>, u64) {
    match decision {
        Replay::Fresh => (None, 0),
        Replay::UpToDate | Replay::Exact => (None, echoed_seq.unwrap_or_default()),
        Replay::Gap(BusGapReason::BeyondRetained) => (
            Some(GapInfo {
                reason: ProtoGapReason::BeyondRetained,
            }),
            0,
        ),
        Replay::Gap(BusGapReason::EpochChanged) => (
            Some(GapInfo {
                reason: ProtoGapReason::EpochChanged,
            }),
            0,
        ),
        Replay::Gap(BusGapReason::ResumeAhead) => {
            warn!(
                channel,
                cursor_seq = ?echoed_seq,
                store_epoch = %epoch,
                "attach resume: echoed cursor above the channel high-water; the store may have \
                 been restored from backup, or the attacher echoed a position this channel never \
                 assigned"
            );
            (
                Some(GapInfo {
                    reason: ProtoGapReason::EpochChanged,
                }),
                0,
            )
        }
    }
}

/// One inbound `Subscribe`'s fields, borrowed for the duration of the handler.
///
/// Bundled rather than passed positionally because the two depths are both
/// `u64` and a transposition would typecheck — and they mean opposite things:
/// one is what wakes the attacher, the other what it may see.
pub struct SubscribeRequest<'a> {
    pub channel: &'a str,
    pub push_depth: u64,
    pub retain_depth: u64,
    /// The cursor the attacher echoed for this channel, or `None` to start from
    /// the channel's retained tail.
    pub resume: Option<Cursor>,
}

/// Handle a `Subscribe` frame — one handler for every transportable channel,
/// whichever store holds its retention.
///
/// Validates the channel against the profile and the active subscription set,
/// clamps the client's stated depths to the boot-resolved fold, activates the
/// subscription, anchors its position at the echoed cursor (0 on a fresh
/// attach), and replays what retention holds above it as one pass — the attach
/// is one delivery point, so its replay is one frame. The echoed cursor is the
/// subscription's whole delivery state, so a live row racing the activation is
/// either above the anchor — and delivered once, by whichever path reaches the
/// socket first — or at or below it, and dropped as a duplicate. The FIFO writer
/// queue serializes `SubscribeResult` → replay → live deliveries, so ordering
/// holds by construction.
pub async fn handle_subscribe(
    ctx: &AttachSessionCtx,
    active: &mut ActiveChannels,
    cursors: &mut WireCursors,
    request: SubscribeRequest<'_>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let SubscribeRequest {
        channel,
        push_depth,
        retain_depth,
        resume,
    } = request;
    // Unknown channels and channels this attacher may not subscribe are
    // indistinguishable on the wire (no existence oracle): the same violation.
    let Some(bound) = ctx.profile.subscribable(channel) else {
        return ctx.violation(format!(
            "Subscribe to unsubscribable channel {}",
            sanitize_client_detail(channel)
        ));
    };

    if active.is_active(channel) {
        return ctx.violation(format!(
            "duplicate Subscribe to active subscription {}",
            sanitize_client_detail(channel)
        ));
    }

    // The client states both knobs and the server clamps them to what boot
    // resolved for the channel. A conforming attacher echoes the depths its own
    // configuration gave it and sees no difference; a client asking for a wider
    // window than the operator declared gets the operator's answer, and the
    // replay per Subscribe stays config-bounded — which is what keeps a subscribe
    // storm from being amplified into a DoS.
    let facts = SubscriptionFacts {
        push_depth: push_depth.min(bound.push_depth),
        retain_depth: retain_depth.min(bound.retain_depth),
    };

    // A subscription that neither wakes nor sees is meaningless, and servicing
    // it would wedge the channel: its replay clamp is empty, so the position
    // never advances past the first non-contiguous row and every later row on
    // the channel drains nothing while looking healthy on the wire. A conforming
    // attacher echoes depths its operator wrote, which never clamp to this.
    if facts.push_depth == 0 && facts.retain_depth == 0 {
        return ctx.violation(format!(
            "Subscribe with neither a push nor a retain window on {}",
            sanitize_client_detail(channel)
        ));
    }

    let echoed = match resume {
        None => None,
        Some(cursor) => match parse_resume_cursor(ctx, &cursor, channel) {
            Ok(state) => Some(state),
            Err(outcome) => return outcome,
        },
    };

    // Activate before the store read: from here the router queues live rows. A
    // row the replay below also serves arrives at the live arm at or below the
    // position this subscribe anchors, so the handoff race closes on the cursor.
    active.activate(channel, facts);

    // The connection's boot incarnation (see `WireCursors::incarnation` for the
    // staleness check it feeds). A stale cursor is conforming, so it is answered
    // as a fresh attach, never a violation.
    let incarnation = cursors.incarnation();
    let stale = echoed
        .as_ref()
        .is_some_and(|state| state.incarnation > incarnation);
    if let Some(state) = &echoed
        && stale
    {
        warn!(
            channel,
            cursor_incarnation = state.incarnation,
            store_incarnation = incarnation,
            "attach resume: cursor minted under a boot this store never counted; answering as \
             fresh attach"
        );
    }
    let store_cursor = match (&echoed, stale) {
        (Some(state), false) => Some(state.resume),
        _ => None,
    };

    // The subscription's whole delivery state: the client's own cursor, answered
    // from retention.
    let replay = ctx
        .messenger
        .store_for_address(channel)
        .replay_from(store_cursor, facts.replay_clamp())
        .await;

    let (mut gap, anchor) = resume_answer(
        replay.decision,
        store_cursor.map(|cursor| cursor.seq),
        channel,
        replay.epoch,
    );
    if stale {
        // A stale-store cursor forces the `EpochChanged` gap over whatever the
        // (fresh-attach) store answer concluded.
        gap = Some(GapInfo {
            reason: ProtoGapReason::EpochChanged,
        });
    }

    // Anchor the position and reset the span before the SubscribeResult, so the
    // replay rows mint seqs 1..N and the position starts at the (non-stale)
    // resume cursor, or 0 on a fresh or stale-store attach.
    cursors.start_span(channel, replay.epoch, anchor);

    // Floor parity: this gates every session-side replay send. Policies are
    // boot-static, so a deny is fail-closed hygiene, not a feature.
    let floor_ok = ctx.policy.allows_channel_access(channel);
    let window = replay.messages;
    let replay_count = if floor_ok { window.len() as u32 } else { 0 };

    let result = ServerFrame::SubscribeResult {
        channel: channel.to_string(),
        outcome: SubscribeOutcome::Ok,
        replay_count,
        gap,
    };
    if let FrameOutcome::Disconnect = super::session::send_frame(&ctx.tx, result, counters).await {
        return FrameOutcome::Disconnect;
    }

    if !floor_ok {
        warn!(
            channel,
            "attach subscribe: delivery floor denied; sending no replay"
        );
        return FrameOutcome::Continue;
    }

    // The whole replay is one pass, hence one frame: the attacher is owed one
    // delivery point for the attach, not one per retained row.
    let pass = window
        .into_iter()
        .map(|retained| PassRow {
            envelope: (*retained.message).clone(),
            dropped: 0,
            retained_seq: retained.seq,
        })
        .collect();
    send_pass(ctx, cursors, channel, pass, counters).await
}

/// Drain every active subscription's unseen suffix — the eager-wake nudge path.
/// Stops and reports `Disconnect` if any send finds the writer gone.
pub async fn drain_all(
    ctx: &AttachSessionCtx,
    active: &ActiveChannels,
    cursors: &mut WireCursors,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    for channel in active.channels() {
        if let FrameOutcome::Disconnect =
            drain_channel(ctx, active, cursors, &channel, counters).await
        {
            return FrameOutcome::Disconnect;
        }
    }
    FrameOutcome::Continue
}

/// Send one channel's unseen suffix in seq order, as one pass: everything the
/// channel retains above the position this connection has written, in one
/// frame. That position is the
/// subscription's whole delivery state, so a drain racing the live fan-out
/// re-reads what the fan-out already advanced past and finds nothing. A span
/// retention no longer holds is reported as `dropped` on the first delivery that
/// follows it. The delivery floor gates the send.
pub async fn drain_channel(
    ctx: &AttachSessionCtx,
    active: &ActiveChannels,
    cursors: &mut WireCursors,
    channel: &str,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    // An active subscription always has an anchored position — the Subscribe
    // that activated it anchored one before returning.
    let cursor = cursors.pos_of(channel).expect(
        "attach session: drain on a channel with no anchored position — activation anchors one",
    );
    let facts = active
        .facts(channel)
        .expect("attach session: drain on a channel with no active subscription");
    let replay = ctx
        .messenger
        .store_for_address(channel)
        .replay_from(Some(cursor), facts.replay_clamp())
        .await;
    // A `Gap` answer means retention no longer covers the whole span above the
    // position — evicted, or longer than the subscription's clamp — and its
    // window is the channel's newest rows rather than a suffix of the cursor.
    // Sending it whole would re-send positions already written and, worse, move
    // the position past the interior span with no signal, which no later
    // Subscribe could then report. So the window is cut to the suffix and the
    // seqs between the position and its oldest entry ride the first delivery's
    // `dropped`, which is the wire's own field for exactly this loss — on a
    // subscription that has a push window at all. A context feed has none, so
    // nothing may be reported as dropped on it: the loss is still logged, but the
    // wire field describes an overflow the attacher cannot have had.
    let (window, lost) = match replay.decision {
        Replay::Gap(reason) => {
            let (suffix, lost) = gap_suffix(replay.messages, cursor.seq);
            warn!(
                channel,
                ?reason,
                position = cursor.seq,
                dropped = lost,
                serving = suffix.len(),
                "attach drain: retention no longer covers the span above the position; the \
                 subscription loses the interior span"
            );
            (suffix, if facts.push_enabled() { lost } else { 0 })
        }
        _ => (replay.messages, 0),
    };
    if window.is_empty() {
        return FrameOutcome::Continue;
    }
    if !ctx.policy.allows_channel_access(channel) {
        warn!(
            channel,
            "attach drain: delivery floor denied; sending nothing"
        );
        return FrameOutcome::Continue;
    }
    // The loss belongs to the subscription, not to a message: it rides the first
    // delivery that follows it and is not repeated on the rest.
    let mut dropped = lost;
    // One drain is one pass, hence one frame: the suffix a behind subscription is
    // served is one catch-up, not one arrival per row.
    let pass = window
        .into_iter()
        .map(|retained| PassRow {
            envelope: (*retained.message).clone(),
            dropped: std::mem::take(&mut dropped),
            retained_seq: retained.seq,
        })
        .collect();
    send_pass(ctx, cursors, channel, pass, counters).await
}

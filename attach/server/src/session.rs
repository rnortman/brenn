//! One attachment, from upgrade to teardown: the context a frame handler reads,
//! the loop that reads the socket, the dispatch that routes each frame to its
//! plane, and the alert plane itself.
//!
//! Nothing here is application-shaped. The context holds a profile, not a
//! surface; an account, not a browser session; and its violation helper spells
//! the attacher by its principal, so one log format serves a page and a daemon
//! alike. The loop is generic over the socket, so the whole lifecycle is
//! exercised without one.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::Message;
use brenn_attach_proto::{
    AlertSeverity as ProtoAlertSeverity, ClientFrame, MAX_ALERT_BODY_BYTES, MAX_ALERT_TITLE_BYTES,
    ServerFrame,
};
use brenn_common::sanitize_untrusted_str;
use brenn_lib::access::AppPolicy;
use brenn_lib::token_bucket::{TokenBucket, TokenBucketOutcome};
use brenn_messaging::Messenger;
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity as NativeAlertSeverity};
use brenn_obs::security::{SecurityEventType, log_and_alert_security_event};
use futures::{Sink, Stream, StreamExt};
use tokio::sync::{Notify, mpsc};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{Instrument, info, info_span, warn};
use uuid::Uuid;

use super::profile::AttachProfile;
use super::publish::{
    PublishBatchRequest, PublishRequest, handle_publish, handle_publish_batch, seed_deferred_views,
};
use super::registry::{AttachRegistry, AttachSessionGuard, SessionPush};
use super::socket::{
    HELLO_TIMEOUT, Handshake, InboundError, classify_read_error, read_client_hello, server_hello,
    welcome, writer_task,
};
use super::subscription::{
    ActiveChannels, SubscribeRequest, WireCursors, charge_subscribe_token, drain_all,
    handle_subscribe, handle_unsubscribe, send_session_pushes, subscribe_bucket,
};

/// Outbound frame queue depth. A slow reader fills this, then the delivery path
/// blocks (backpressure at the socket) rather than dropping control frames.
pub const OUTBOUND_QUEUE_FRAMES: usize = 256;

/// Immutable per-connection context every frame handler reads but none mutates.
/// Built once when the session starts and passed as `&AttachSessionCtx`, so
/// handler signatures carry one shared reference rather than a positional list
/// of same-typed identity params (`account`, `ip`, …) a caller could transpose.
/// The genuinely mutable per-session state — the subscription map, the wire
/// cursors, the rate buckets, the counters — is threaded separately as `&mut`.
pub struct AttachSessionCtx {
    /// The authority half of this attachment: which channels it may subscribe
    /// and publish, which sub-identities it may act as. Boot-built by the route
    /// and shared by every session of the attacher.
    pub profile: Arc<dyn AttachProfile>,
    /// The bus. Every store read and every publish this session makes goes
    /// through it.
    pub messenger: Arc<Messenger>,
    /// The attacher's resolved access policy. The session consults it as a
    /// delivery floor on the send path — boot already proved every subscribable
    /// channel granted, so a deny here is fail-closed hygiene, not a feature.
    pub policy: Arc<AppPolicy>,
    /// Process alert dispatcher, cloned once at attach. Two planes reach it: the
    /// `Alert` frame pages through it, and every protocol violation on this
    /// attachment is logged and alerted through it as a security event.
    pub alert_dispatcher: AlertDispatcher,
    /// The attached-attachment registry. Reached for the planes whose subject is
    /// the attacher rather than the connection — a parked set belongs to a
    /// sub-identity every attachment shares, so a change one of them causes is
    /// pushed at all of them.
    pub registry: AttachRegistry,
    /// The largest message body this attachment may publish, advertised in
    /// `Welcome`. A transport contract fact of the attachment, not application
    /// config: the frame cap the attacher enforces is derived from it.
    pub max_body_bytes: usize,
    /// Per-connection id, minted at attach and advertised in `Welcome` so the
    /// attacher can self-attribute the documents it authors.
    pub session_id: Uuid,
    /// The authenticated account behind this attachment. Held for log
    /// attribution only: authority comes from the profile, never from here.
    pub account: String,
    pub ip: IpAddr,
    /// Outbound frame sender to the writer task. Owning it here means dropping
    /// the context at teardown closes the channel and exits the writer.
    pub tx: mpsc::Sender<ServerFrame>,
}

impl AttachSessionCtx {
    /// A protocol violation on this attachment, named by attacher and account.
    ///
    /// One spelling for every plane, so the security log line that feeds
    /// fail2ban has a stable prefix whatever the frame was. `detail` names the
    /// violated rule and must not echo unsanitized client payload — see
    /// `sanitize_client_detail`.
    pub fn violation(&self, detail: impl Display) -> FrameOutcome {
        FrameOutcome::Violation(self.violation_detail(detail))
    }

    /// The rendered security-event detail for a violation, for the paths that
    /// hold one without a frame to answer — the handshake, and the socket-level
    /// frame classes the dispatch never sees.
    pub fn violation_detail(&self, detail: impl Display) -> String {
        format!(
            "attacher {} account {}: {detail}",
            self.profile.attacher().as_str(),
            self.account,
        )
    }
}

/// What the session loop does after a dispatched inbound frame.
pub enum FrameOutcome {
    /// Frame handled; keep the session running.
    Continue,
    /// Protocol violation: the caller logs+alerts it as a security event and
    /// tears the session down. The detail names the attacher, the account, and
    /// the violated rule, and never echoes the client payload.
    Violation(String),
    /// The writer is gone (socket died mid-send): tear the session down without
    /// a security event.
    Disconnect,
}

/// Per-session counters folded into the single disconnect line. Frame counts
/// cover the application frames the session task processes and enqueues; the
/// writer's liveness `Ping`/`Heartbeat` frames are transport plumbing and are
/// not counted here.
#[derive(Default)]
pub struct SessionCounters {
    /// Inbound text (application) frames dispatched. Binary-frame and
    /// cap-overflow violations tear down before this counts them.
    pub frames_in: u64,
    /// Server frames the session task enqueued to the writer.
    pub frames_out: u64,
    /// Publishes that reached the bus with an `Ok` outcome.
    pub publishes: u64,
    /// Publishes denied by either rate gate — the connection bucket or the
    /// bus-level per-sender gate.
    pub publish_rate_limited: u64,
    /// Publishes rejected for an oversized body at the transport pre-check.
    /// Drives the first-occurrence warn and the escalation-to-violation count.
    pub publish_body_too_large: u64,
    /// Publishes where the transport pre-check admitted a body the bus then
    /// rejected as oversized — a config-wiring bug (both caps derive from one
    /// `max_body_bytes`). Each such arm already `error!`s; this counter keeps
    /// them out of the transport-reject count so escalation is not conflated
    /// with an internal disagreement.
    pub publish_body_cap_disagreement: u64,
    /// `Alert` frames dispatched to the process alert dispatcher (granted, and
    /// within the per-connection alert bucket) — the operator's count of how
    /// many times this attachment paged.
    pub alerts_dispatched: u64,
    /// `Alert` frames dropped by the per-connection alert bucket. Not a kill (a
    /// noisy but legitimate attacher must not lose its session); the
    /// process-wide alert rate limiter bounds total paging downstream.
    pub alerts_suppressed: u64,
    /// Per-attribution publish breakdown — the same grain the send budget meters
    /// and the sender identity carries, so "which sub-identity drained its
    /// budget?" is answerable from the disconnect line without correlating
    /// against the bus.
    ///
    /// **Does not sum to `publishes`/`publish_rate_limited`**, by construction:
    /// the attacher's own publishes name no attribution and have no column here.
    /// The totals are the session's; this is the attributable part of them.
    pub by_attribution: BTreeMap<String, AttributionPublishCounters>,
}

/// One sub-identity's publish outcomes within a session ([`SessionCounters`]).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AttributionPublishCounters {
    /// Publishes this sub-identity landed on the bus.
    pub publishes: u64,
    /// Publishes denied by either rate gate — the connection bucket or this
    /// sub-identity's own send budget. A component looping on retries shows up
    /// here, under its own name.
    pub publish_rate_limited: u64,
}

impl SessionCounters {
    /// Count one publish that reached the bus, attachment-wide and against the
    /// sub-identity that made it.
    ///
    /// `attribution` is `None` for a publish by the attacher itself, which has
    /// no column — see `by_attribution`. Both counters move together here rather
    /// than at each call site so the breakdown cannot silently stop tracking the
    /// total it decomposes.
    pub fn publish_ok(&mut self, attribution: Option<&str>) {
        self.publishes += 1;
        if let Some(name) = attribution {
            self.by_attribution
                .entry(name.to_string())
                .or_default()
                .publishes += 1;
        }
    }

    /// Count one publish denied by a rate gate, attachment-wide and against the
    /// sub-identity that made it. See [`SessionCounters::publish_ok`].
    pub fn publish_rate_limited(&mut self, attribution: Option<&str>) {
        self.publish_rate_limited += 1;
        if let Some(name) = attribution {
            self.by_attribution
                .entry(name.to_string())
                .or_default()
                .publish_rate_limited += 1;
        }
    }
}

/// Render a client-supplied string for inclusion in a security-event detail.
///
/// Truncates to a short prefix and control-character-escapes the result, so a
/// hostile client cannot inject unbounded length or raw newline/escape bytes
/// into the security log line or the phone-alert body.
pub fn sanitize_client_detail(s: &str) -> String {
    const MAX_CHARS: usize = 128;
    let mut rendered: String = s
        .chars()
        .take(MAX_CHARS)
        .flat_map(char::escape_debug)
        .collect();
    if s.chars().nth(MAX_CHARS).is_some() {
        rendered.push_str("...");
    }
    rendered
}

/// Send one `ServerFrame` to the writer, counting it and mapping a closed
/// channel (writer gone) to `Disconnect`.
pub async fn send_frame(
    tx: &mpsc::Sender<ServerFrame>,
    frame: ServerFrame,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    match tx.send(frame).await {
        Ok(()) => {
            counters.frames_out += 1;
            FrameOutcome::Continue
        }
        Err(_) => FrameOutcome::Disconnect,
    }
}

/// `Alert` frame rate-limit burst — deliberately tighter than the publish bucket
/// because alerts page a human. Beyond-burst alerts are dropped and counted,
/// never a kill: a legitimately unhealthy attacher must not lose its attachment
/// for being noisy.
pub const ALERT_BURST: u32 = 5;

/// One `Alert` token refilled per this interval under sustained load.
const ALERT_REFILL: Duration = Duration::from_secs(300);

/// The per-connection rate buckets, grouped so frame handlers take one bundle
/// instead of a growing list of parallel `&mut TokenBucket` params.
///
/// Each starts full, so an attachment's first burst is admitted before limiting
/// begins. What each bucket costs when exhausted differs and is stated at its
/// charge site: `subscribe` violates, `publish` answers an outcome, `alert`
/// drops.
pub struct SessionBuckets {
    /// Gates `Subscribe` and `Unsubscribe` of both channel classes — metering
    /// only durable would leak the class distinction to a probe.
    pub subscribe: TokenBucket,
    /// Gates single publishes. Trips ahead of the bus-level per-sender gate,
    /// which is the principal's rather than the connection's.
    pub publish: TokenBucket,
    /// Gates the paging plane, tighter than the rest.
    pub alert: TokenBucket,
}

impl SessionBuckets {
    /// The three buckets one attachment starts with, sized from its profile.
    pub fn new(profile: &dyn AttachProfile) -> Self {
        let rate = profile.publish_rate();
        Self {
            subscribe: subscribe_bucket(profile),
            publish: TokenBucket::new(rate.burst, Duration::from_secs(1), rate.per_sec),
            alert: TokenBucket::new(ALERT_BURST, ALERT_REFILL, 1),
        }
    }
}

/// The mutable half of one attachment: the state the loop owns and every plane
/// mutates, bundled so a new per-connection item widens one struct instead of
/// every signature between the loop and the plane that needs it.
///
/// Separate from [`AttachSessionCtx`], which is the immutable half: a handler
/// takes one shared reference and one exclusive one, and neither carries a
/// positional list a caller could transpose.
pub struct SessionState {
    /// Which channels this attachment currently subscribes, and at what fold.
    pub active: ActiveChannels,
    /// Per-channel span and position state, and the cursors minted from it.
    pub cursors: WireCursors,
    pub buckets: SessionBuckets,
    pub counters: SessionCounters,
}

impl SessionState {
    /// The state one attachment starts with. `active_channels` is the set shared
    /// with the registry handle, and `store_incarnation` is the boot counter
    /// stamped into every cursor this attachment mints.
    pub fn new(
        profile: &dyn AttachProfile,
        active_channels: Arc<Mutex<HashSet<String>>>,
        store_incarnation: i64,
    ) -> Self {
        Self {
            active: ActiveChannels::new(active_channels),
            cursors: WireCursors::new(store_incarnation),
            buckets: SessionBuckets::new(profile),
            counters: SessionCounters::default(),
        }
    }
}

/// Everything the route's upgrade callback hands to one attachment's task.
///
/// Generic over the socket so the whole lifecycle — handshake, `Welcome`,
/// dispatch, liveness, teardown — is driven against a plain duplex of websocket
/// messages. The route passes the real `WebSocket`.
pub struct AttachSessionParams<S> {
    /// The authority half of this attachment, boot-built by the route.
    pub profile: Arc<dyn AttachProfile>,
    pub messenger: Arc<Messenger>,
    pub policy: Arc<AppPolicy>,
    pub registry: AttachRegistry,
    /// Releases this attachment's registry slot on drop, even on panic.
    pub guard: AttachSessionGuard,
    pub session_id: Uuid,
    pub account: String,
    pub ip: IpAddr,
    pub max_body_bytes: usize,
    /// The cadence the writer probes on and the attacher's inbound-silence rule
    /// is measured in, advertised in `Welcome`.
    pub heartbeat_secs: u32,
    /// The store's boot counter, stamped into every cursor this attachment
    /// mints.
    pub store_incarnation: i64,
    /// This build's free-form identifier, for the peer's logs only.
    pub ident: String,
    pub alert_dispatcher: AlertDispatcher,
    /// Live rows and deferred-view snapshots pushed at this attachment from
    /// outside its task (paired with the `push_tx` in its registry handle).
    pub push_rx: mpsc::Receiver<SessionPush>,
    /// Active subscriptions, shared with the registry handle so the router can
    /// see which channels this attachment covers. Written only by this task.
    pub active_channels: Arc<Mutex<HashSet<String>>>,
    /// Drain nudge, notified by the router to flush parked/quiet rows.
    pub drain_notify: Arc<Notify>,
    pub socket: S,
}

/// What the route learns from a finished attachment.
///
/// Deliberately thin: the transport reports facts, and what a route *does* with
/// them — an application-layer teardown document, an audit row, nothing at all —
/// stays outside the session, which knows no application layer to write one for.
pub struct AttachSessionOutcome {
    /// Whether this was the attacher's last attachment: its registration was
    /// removed and no other remained. Decided atomically at removal, because two
    /// concurrent closers each reading a count while still registered would both
    /// see the other and neither would be last.
    pub last_detach: bool,
    /// Whether the attachment ended on a protocol violation. Part of the
    /// terminal disposition a route may act on; neither route acts on it, since
    /// the session already logs the violation as a security event, so only the
    /// session suite reads it.
    #[allow(dead_code)]
    pub violation: bool,
}

/// Run one attachment to completion, inside a `tracing` span that carries
/// per-attachment attribution on every log line.
pub async fn run_attach_session<S>(params: AttachSessionParams<S>) -> AttachSessionOutcome
where
    S: Stream<Item = Result<Message, axum::Error>> + Sink<Message> + Unpin + Send + 'static,
    <S as Sink<Message>>::Error: Display + Send,
{
    let span = info_span!(
        "attach_session",
        attacher = %params.profile.attacher().as_str(),
        session_id = %params.session_id,
        account = %params.account,
        ip = %params.ip,
    );
    attach_session(params).instrument(span).await
}

/// How the opening sequence ended: attached and ready to read frames, or closed
/// before that, carrying a rendered security detail iff it was a violation.
enum Opening {
    Attached,
    Closed(Option<String>),
}

async fn attach_session<S>(params: AttachSessionParams<S>) -> AttachSessionOutcome
where
    S: Stream<Item = Result<Message, axum::Error>> + Sink<Message> + Unpin + Send + 'static,
    <S as Sink<Message>>::Error: Display + Send,
{
    let AttachSessionParams {
        profile,
        messenger,
        policy,
        registry,
        guard,
        session_id,
        account,
        ip,
        max_body_bytes,
        heartbeat_secs,
        store_incarnation,
        ident,
        alert_dispatcher,
        mut push_rx,
        active_channels,
        drain_notify,
        socket,
    } = params;
    let heartbeat = Duration::from_secs(u64::from(heartbeat_secs));

    let (sink, mut ws_stream) = socket.split();
    let (tx, rx) = mpsc::channel::<ServerFrame>(OUTBOUND_QUEUE_FRAMES);
    // Instrument the writer with the session span so its logs carry the same
    // attacher/session/account/ip attribution as this task's.
    let writer =
        tokio::spawn(writer_task(sink, rx, heartbeat).instrument(tracing::Span::current()));

    // The shared per-connection context. It owns `tx`, so dropping it at teardown
    // closes the writer channel and exits the writer.
    let ctx = AttachSessionCtx {
        profile,
        messenger,
        policy,
        alert_dispatcher,
        registry,
        max_body_bytes,
        session_id,
        account,
        ip,
        tx,
    };

    let mut state = SessionState::new(ctx.profile.as_ref(), active_channels, store_incarnation);

    let violation_detail = match open_attachment(
        &ctx,
        &mut ws_stream,
        heartbeat_secs,
        &ident,
        &mut state.counters,
    )
    .await
    {
        Opening::Attached => {
            read_frames(
                &ctx,
                &mut ws_stream,
                &mut push_rx,
                &drain_notify,
                heartbeat,
                &mut state,
            )
            .await
        }
        Opening::Closed(detail) => detail,
    };

    // One security event per attachment, whatever detected the violation: the
    // handshake, the dispatch, or a socket-level frame class the dispatch never
    // sees. A violation is terminal, so there is never a second.
    //
    // TODO(bridge-violation-close-code): teardown below is by drop, so a
    // violated attachment gets no close frame and the attacher cannot tell a
    // refusal from a network blip.
    if let Some(detail) = &violation_detail {
        log_and_alert_security_event(
            &ctx.alert_dispatcher,
            SecurityEventType::AttachProtocolViolation,
            ctx.ip,
            detail,
        );
    }

    // Remove this attachment's registration and read the post-removal count in
    // one step, before the context drops, so the route's terminal action has an
    // uncontested answer to "was that the last one".
    let remaining = guard.unregister_returning_remaining();

    // Single teardown path: drop the context (its `tx` is the writer sender, so
    // the writer exits and the socket closes), await the writer, drop the guard
    // (slot released even on panic). The active set needs no explicit teardown:
    // the fan-out reaches this attachment only through its registry handle, and
    // unregistering above is what removes it.
    drop(ctx);
    writer.await.expect("attach writer task panicked");
    drop(guard);

    let violation = violation_detail.is_some();
    info!(
        violation,
        frames_in = state.counters.frames_in,
        frames_out = state.counters.frames_out,
        publishes = state.counters.publishes,
        publish_rate_limited = state.counters.publish_rate_limited,
        publish_body_too_large = state.counters.publish_body_too_large,
        publish_body_cap_disagreement = state.counters.publish_body_cap_disagreement,
        alerts_dispatched = state.counters.alerts_dispatched,
        alerts_suppressed = state.counters.alerts_suppressed,
        // Rendered via `Debug` on a `BTreeMap`, so the breakdown is one
        // deterministically-ordered field on the existing line rather than N
        // extra lines per detach. Keys are declared attributions, not client
        // strings.
        by_attribution = ?state.counters.by_attribution,
        "attachment detached"
    );
    AttachSessionOutcome {
        last_detach: remaining == 0,
        violation,
    }
}

/// The opening sequence: `Hello` out, `Hello` in, `Welcome`, parked-set mirrors.
///
/// The server's `Hello` goes first and without waiting, so a peer that reads
/// before it writes still makes progress. `Welcome` and the seeded mirrors are
/// enqueued before any inbound frame is dispatched: by then they already sit
/// ahead of every response in the FIFO writer queue, so "a response before
/// `Welcome`" is unrepresentable and the attacher's mirrors are refilled before
/// it can act on them.
async fn open_attachment<St>(
    ctx: &AttachSessionCtx,
    ws_stream: &mut St,
    heartbeat_secs: u32,
    ident: &str,
    counters: &mut SessionCounters,
) -> Opening
where
    St: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    if let FrameOutcome::Disconnect = send_frame(&ctx.tx, server_hello(ident), counters).await {
        return Opening::Closed(None);
    }

    let version = match read_client_hello(ws_stream, HELLO_TIMEOUT).await {
        Handshake::Agreed(version) => version,
        Handshake::Incompatible(peer) => {
            // Both ranges have already crossed, so each end closes on its own
            // arithmetic and neither owes the other a refusal frame.
            info!(
                peer_min = peer.min,
                peer_max = peer.max,
                "attachment closed: no version in common"
            );
            return Opening::Closed(None);
        }
        Handshake::Violation(rule) => return Opening::Closed(Some(ctx.violation_detail(rule))),
        Handshake::Disconnect => return Opening::Closed(None),
    };

    match send_frame(&ctx.tx, welcome(ctx, version, heartbeat_secs), counters).await {
        FrameOutcome::Continue => {}
        _ => return Opening::Closed(None),
    }
    match seed_deferred_views(ctx, counters).await {
        FrameOutcome::Continue => {}
        // The only non-Continue return is a writer that is already gone.
        _ => return Opening::Closed(None),
    }

    info!(version, "attachment connected");
    Opening::Attached
}

/// Read the socket until the attachment ends, servicing the three other things
/// that can wake this task: pushes from the router, the eager-wake drain nudge,
/// and the liveness tick.
///
/// Returns the rendered security detail iff the attachment ended on a violation.
async fn read_frames<St>(
    ctx: &AttachSessionCtx,
    ws_stream: &mut St,
    push_rx: &mut mpsc::Receiver<SessionPush>,
    drain_notify: &Notify,
    heartbeat: Duration,
    state: &mut SessionState,
) -> Option<String>
where
    St: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    // Liveness is inbound-silence, measured against the same cadence the writer
    // probes on: an attacher that has answered nothing for three probes is gone
    // whatever the socket still claims. The clock is tokio's, so the reap runs on
    // the same time source the tick does.
    let reap_after = heartbeat * 3;
    let mut last_inbound = Instant::now();
    let mut liveness = tokio::time::interval(heartbeat);
    liveness.set_missed_tick_behavior(MissedTickBehavior::Delay);
    liveness.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            // A push from outside this task: a live retained row or a
            // deferred-view snapshot.
            Some(push) = push_rx.recv() => {
                // Take every co-available push before writing: the router queues
                // one message's sibling rows back to back, so they coalesce.
                let mut pushes = vec![push];
                while let Ok(next) = push_rx.try_recv() {
                    pushes.push(next);
                }
                if let FrameOutcome::Disconnect = send_session_pushes(
                    ctx,
                    &state.active,
                    &mut state.cursors,
                    pushes,
                    &mut state.counters,
                )
                .await
                {
                    return None;
                }
            }
            // Eager-wake nudge: serve every active subscription its suffix. The
            // router queues its live copy before firing the nudge, so anything
            // already in hand goes out first — as the frame the fan-out composed,
            // not as a retention read that would beat it to the position.
            () = drain_notify.notified() => {
                let mut pushes = Vec::new();
                while let Ok(next) = push_rx.try_recv() {
                    pushes.push(next);
                }
                if !pushes.is_empty()
                    && let FrameOutcome::Disconnect = send_session_pushes(
                        ctx,
                        &state.active,
                        &mut state.cursors,
                        pushes,
                        &mut state.counters,
                    )
                    .await
                {
                    return None;
                }
                if let FrameOutcome::Disconnect = drain_all(
                    ctx,
                    &state.active,
                    &mut state.cursors,
                    &mut state.counters,
                )
                .await
                {
                    return None;
                }
            }
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        state.counters.frames_in += 1;
                        match handle_client_frame(ctx, text.as_str(), state).await {
                            FrameOutcome::Continue => last_inbound = Instant::now(),
                            FrameOutcome::Violation(detail) => return Some(detail),
                            // Writer gone (socket died mid-send): tear down.
                            FrameOutcome::Disconnect => return None,
                        }
                    }
                    // The protocol is JSON text in both directions, so a binary
                    // frame is not a frame this attachment could have meant.
                    Some(Ok(Message::Binary(_))) => {
                        return Some(ctx.violation_detail("binary frame"));
                    }
                    // Inbound pings are auto-ponged by axum; an inbound pong is
                    // the peer answering our liveness probe.
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                        last_inbound = Instant::now();
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("attachment closed by peer");
                        return None;
                    }
                    Some(Err(e)) => {
                        return match classify_read_error(e) {
                            InboundError::Oversized => {
                                Some(ctx.violation_detail("inbound frame exceeds size cap"))
                            }
                            InboundError::Transport(detail) => {
                                warn!("attachment WS read error: {detail}");
                                None
                            }
                        };
                    }
                }
            }
            _ = liveness.tick() => {
                if last_inbound.elapsed() > reap_after {
                    info!("attachment reaped: no inbound liveness within 3x heartbeat");
                    return None;
                }
            }
        }
    }
}

/// Parse and dispatch one inbound frame.
///
/// Unparseable input — malformed JSON, an unknown `type`, a field the negotiated
/// schema does not have — is a violation: both ends agreed which schema is in
/// force, so unparseable traffic is a bug or tampering, never skew. Each plane
/// owns what its own frames cost; the two this function decides are the second
/// `Hello` (there is no re-negotiation) and the `Subscribe`/`Unsubscribe` token,
/// charged here so both frames draw one bucket.
async fn handle_client_frame(
    ctx: &AttachSessionCtx,
    text: &str,
    state: &mut SessionState,
) -> FrameOutcome {
    let Ok(frame) = serde_json::from_str::<ClientFrame>(text) else {
        return ctx.violation("unparseable client frame");
    };
    match frame {
        // The handshake happens once, before any other frame; a version in force
        // is in force for the connection's life.
        ClientFrame::Hello { .. } => ctx.violation("Hello after the handshake"),
        ClientFrame::Subscribe {
            channel,
            push_depth,
            retain_depth,
            resume,
        } => {
            if let Err(violation) = charge_subscribe_token(ctx, &mut state.buckets.subscribe) {
                return violation;
            }
            handle_subscribe(
                ctx,
                &mut state.active,
                &mut state.cursors,
                SubscribeRequest {
                    channel: &channel,
                    push_depth,
                    retain_depth,
                    resume,
                },
                &mut state.counters,
            )
            .await
        }
        ClientFrame::Unsubscribe { channel } => {
            if let Err(violation) = charge_subscribe_token(ctx, &mut state.buckets.subscribe) {
                return violation;
            }
            handle_unsubscribe(ctx, &mut state.active, &mut state.cursors, &channel)
        }
        ClientFrame::Publish {
            channel,
            attribution,
            body,
            urgency,
            correlation,
        } => {
            handle_publish(
                ctx,
                &mut state.buckets.publish,
                PublishRequest {
                    channel: &channel,
                    attribution: attribution.as_deref(),
                    body: &body,
                    urgency,
                    correlation,
                },
                &mut state.counters,
            )
            .await
        }
        ClientFrame::PublishBatch {
            attribution,
            correlation,
            publishes,
            deferred_ops,
        } => {
            handle_publish_batch(
                ctx,
                PublishBatchRequest {
                    attribution: attribution.as_deref(),
                    correlation,
                    publishes: &publishes,
                    deferred_ops: &deferred_ops,
                },
                &mut state.counters,
            )
            .await
        }
        ClientFrame::Alert {
            severity,
            title,
            body,
        } => handle_alert(
            ctx,
            &mut state.buckets.alert,
            &mut state.counters,
            severity,
            &title,
            &body,
        ),
    }
}

/// Map a proto [`AlertSeverity`](ProtoAlertSeverity) to the native
/// [`AlertSeverity`](NativeAlertSeverity), 1:1. Both share the WIT
/// `alert.severity` vocabulary; this bridge keeps the wire crate free of a
/// host-only dependency.
fn map_alert_severity(severity: ProtoAlertSeverity) -> NativeAlertSeverity {
    match severity {
        ProtoAlertSeverity::Info => NativeAlertSeverity::Info,
        ProtoAlertSeverity::Warning => NativeAlertSeverity::Warning,
        ProtoAlertSeverity::Critical => NativeAlertSeverity::Critical,
    }
}

/// Handle an `Alert` frame — the grant-gated paging plane.
///
/// 1. Grant check first: an alert from an attacher without the grant is a
///    violation. The grant is advertised in `Welcome` and a conforming attacher
///    suppresses ungranted alerts itself, so the frame reaches here only from a
///    non-conforming one.
/// 2. Size caps: an oversized title or body is a violation — the plane is opt-in
///    and its client is expected to conform. The payload is never echoed into the
///    security detail.
/// 3. Per-connection alert bucket: beyond-burst alerts are dropped, counted, and
///    warned — not a kill. The process-wide alert rate limiter bounds total
///    paging downstream.
/// 4. Dispatch: title and body are sanitized, attribution is appended to the
///    body, and the title is prefixed
///    with the attacher's principal so an attachment cannot impersonate a host,
///    app, or WASM alert source.
/// 5. Record: one `warn!` — the operator's durable record of who paged. Alerts
///    are not republished onto any channel; the plane exists precisely so paging
///    does not depend on the bus it may be reporting on.
fn handle_alert(
    ctx: &AttachSessionCtx,
    alert_bucket: &mut TokenBucket,
    counters: &mut SessionCounters,
    severity: ProtoAlertSeverity,
    title: &str,
    body: &str,
) -> FrameOutcome {
    let attacher = ctx.profile.attacher().as_str().to_string();

    // 1. Grant — deny-by-default.
    if !ctx.profile.alert_granted() {
        return ctx.violation("Alert from an attacher without the alert grant");
    }

    // 2. Size caps, without echoing the payload.
    if title.len() > MAX_ALERT_TITLE_BYTES || body.len() > MAX_ALERT_BODY_BYTES {
        return ctx.violation(format!(
            "Alert field exceeds size cap (title {}/{MAX_ALERT_TITLE_BYTES}, body \
             {}/{MAX_ALERT_BODY_BYTES})",
            title.len(),
            body.len(),
        ));
    }

    // 3. Per-connection bucket. Beyond-bucket is dropped, counted, warned.
    match alert_bucket.try_consume() {
        TokenBucketOutcome::Granted => {}
        TokenBucketOutcome::GrantedAfterSuppression { suppressed } => {
            warn!(
                suppressed,
                "attachment Alert rate limit lifted, alerts were suppressed"
            );
        }
        TokenBucketOutcome::Denied { first } => {
            counters.alerts_suppressed += 1;
            if first {
                warn!("rate-limiting Alert frames from this attachment");
            }
            return FrameOutcome::Continue;
        }
    }

    // 4. Dispatch.
    let title = sanitize_untrusted_str(title, MAX_ALERT_TITLE_BYTES);
    let body = sanitize_untrusted_str(body, MAX_ALERT_BODY_BYTES);
    let severity = map_alert_severity(severity);
    let attributed_body = format!(
        "{body}\nattacher={attacher} account={} session={}",
        ctx.account, ctx.session_id
    );
    ctx.alert_dispatcher.alert(
        severity,
        format!("Attacher {attacher}: {title}"),
        attributed_body,
    );

    // 5. Record.
    counters.alerts_dispatched += 1;
    warn!(severity = %severity, title = %title, "attachment alert dispatched");
    FrameOutcome::Continue
}

//! Per-connection surface WS session machinery.
//!
//! `run_surface_session` owns one connection: a writer task drives the sink
//! (idle `Heartbeat`s, native pings, a write-progress watchdog) while the main
//! task sends `Welcome`, reads inbound frames, and reaps dead connections.
//!
//! Inbound text frames are parsed as `ClientFrame` and dispatched:
//! unparseable payloads (malformed JSON, unknown `type`) are protocol
//! violations. `Log` is the one lenient frame — size-capped, rate-limited,
//! and logged at its declared level, never a violation. `Subscribe` attaches an
//! ephemeral subscription (durable channels answer `Unsupported` until durable
//! projection lands) whose live deliveries flow through a `StreamMap` over
//! `SubscriptionStream`. `Publish` resolves `(instance, port)` to a bound
//! output and publishes behind the per-connection rate bucket — an ephemeral
//! output onto its channel's ring store, a durable output through
//! `Messenger::publish_from_surface` (oversized bodies answer `BodyTooLarge`).
//! `Unsubscribe` removes an active subscription (fire-and-
//! forget, no ack); unsubscribing a channel with no active subscription is a
//! violation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket};
use brenn_lib::access::AppCapability;
use brenn_lib::messaging::store::{DeferralOutcome, DeferredMessage, QuotaExceeded, ResumeCursor};
use brenn_lib::messaging::{
    EphemeralDelivery, EphemeralEvent, EphemeralReceiver, EphemeralResume,
    GapReason as BusGapReason, MessageEnvelope, Messenger, ParkedSet, ParticipantId, PrepaidEntry,
    PublishResult, Replay, SurfaceBatchPublish, SurfaceSendVerdict, Urgency, db, utc_from_epoch_ms,
};
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity as NativeAlertSeverity};
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::token_bucket::{TokenBucket, TokenBucketOutcome};
use futures::SinkExt;
use futures::stream::{self, SplitSink, Stream, StreamExt};
use tokio::sync::{Notify, mpsc};
use tokio::time::MissedTickBehavior;
use tokio_stream::StreamMap;
use tracing::{Instrument, debug, error, info, info_span, warn};
use uuid::Uuid;

use brenn_budget::{MAX_PUBLISH_BYTES_PER_ACTIVATION, MAX_PUBLISHES_PER_ACTIVATION};
use brenn_common::sanitize_untrusted_str;
use brenn_surface_proto::{
    AlertSeverity as ProtoAlertSeverity, BatchDeferredOp, BatchEntry, ClientFrame, Cursor,
    DeferredOpKind, DeferredViewEntry, DeliverTarget, GapInfo, GapReason as ProtoGapReason,
    InstanceReport, MAX_ALERT_BODY_BYTES, MAX_ALERT_TITLE_BYTES, PublishBatchOutcome,
    PublishOutcome, ServerFrame, SubscribeOutcome, SurfaceDescription,
};

use super::cursor::{self, CursorState};
use chrono::{DateTime, Utc};

use super::telemetry::{self, Health};

use super::registry::{
    DeferredViewPush, DurableDelivery, SessionPush, SurfaceRegistry, SurfaceSessionGuard,
};
use super::{DeliveryClass, OutputPort, SubKey, SurfaceRuntime, sanitize_client_detail};

/// Outbound frame queue depth. A slow reader fills this, then the delivery path
/// blocks (backpressure) rather than dropping control frames.
const OUTBOUND_QUEUE_FRAMES: usize = 256;

/// `Alert` frame rate-limit burst — deliberately tighter than the publish bucket
/// because alerts page a human. Beyond-burst alerts are dropped and counted,
/// never a kill (a legitimately unhealthy surface must not lose its session for
/// being noisy).
pub(super) const ALERT_BURST: u32 = 5;

/// One `Alert` token refilled per this interval under sustained load.
const ALERT_REFILL: Duration = Duration::from_secs(300);

/// `Subscribe`/`Unsubscribe` rate-limit burst, derived — never a literal — from
/// the boot-enforced maximum binding count so the two can never drift. Parent
/// D10's reconnect-reconcile sends one `Subscribe` per bound channel in a single
/// first-connect burst, so any literal below the maximum would turn a boot-valid
/// 33-plus-binding surface into a deterministic connect → violation → fail2ban
/// loop. `3×` admits the first-connect reconcile (MAX subscribes) plus one full
/// detach/re-attach cycle of a maximum-size surface (MAX unsubscribes + MAX
/// subscribes); churn beyond that is throttled to one token/sec.
const SUBSCRIBE_BURST: u32 = 3 * brenn_surface_proto::MAX_SURFACE_SUBSCRIPTION_BINDINGS as u32;

/// One `Subscribe`/`Unsubscribe` token refilled per this interval.
const SUBSCRIBE_REFILL: Duration = Duration::from_secs(1);

/// The Nth transport-side `BodyTooLarge` reject on a connection is a protocol
/// violation (kill); the first N-1 are answered with `BodyTooLarge` outcome
/// frames. A correct shell learns `max_body_bytes` from `Welcome` and derives
/// the same cap, so it produces ~0; the outcome frames give even a
/// buggy-but-honest component feedback to stop before this trips.
const BODY_TOO_LARGE_VIOLATION_THRESHOLD: u64 = 8;

/// Everything the upgrade callback hands to the session task.
pub(crate) struct SurfaceSessionParams {
    pub runtime: Arc<SurfaceRuntime>,
    pub session_id: Uuid,
    pub username: String,
    pub ip: IpAddr,
    pub guard: SurfaceSessionGuard,
    /// The attached-session registry, for reaching **every** session of this
    /// surface. A deferred view is the sender's, not the connection's — one
    /// surface's tabs share the sub-identity that owns the parked set — so a
    /// change this session causes must reach its siblings too.
    pub registry: SurfaceRegistry,
    pub heartbeat_secs: u32,
    pub alert_dispatcher: AlertDispatcher,
    /// Live rows and deferred-view snapshots pushed at this session from outside
    /// its task (paired with the `push_tx` in this session's registry handle).
    pub push_rx: mpsc::Receiver<SessionPush>,
    /// Active durable subscriptions, shared with the registry handle so the
    /// router can see which of them this session covers. Written only by the
    /// session task.
    pub durable_subs: Arc<Mutex<HashSet<SubKey>>>,
    /// Drain nudge, notified by the router (eager wake / per-delivery) to flush
    /// parked/quiet durable rows.
    pub drain_notify: Arc<Notify>,
    pub socket: WebSocket,
}

/// Run one surface WS connection to completion, inside a `tracing` span that
/// carries per-session attribution on every log line.
pub(crate) async fn run_surface_session(params: SurfaceSessionParams) {
    let span = info_span!(
        "surface_session",
        surface = %params.runtime.resolved.slug,
        session_id = %params.session_id,
        user = %params.username,
        ip = %params.ip,
    );
    session_loop(params).instrument(span).await;
}

async fn session_loop(params: SurfaceSessionParams) {
    let SurfaceSessionParams {
        runtime,
        session_id,
        username,
        ip,
        guard,
        registry,
        heartbeat_secs,
        alert_dispatcher,
        mut push_rx,
        durable_subs,
        drain_notify,
        socket,
    } = params;
    let heartbeat = Duration::from_secs(u64::from(heartbeat_secs));

    let (sink, mut ws_stream) = socket.split();
    let (tx, rx) = mpsc::channel::<ServerFrame>(OUTBOUND_QUEUE_FRAMES);
    // Instrument the writer with the session span so its logs carry the same
    // surface/session_id/user/ip attribution as the session task's.
    let writer =
        tokio::spawn(writer_task(sink, rx, heartbeat).instrument(tracing::Span::current()));

    // The shared per-session context. It owns `tx`, so dropping it at teardown
    // closes the writer channel. `slug` is kept as a local for this function's
    // own log formatting; it lives on `ctx.runtime.resolved.slug`.
    let ctx = SessionCtx {
        runtime,
        session_id,
        username,
        ip,
        alert_dispatcher,
        registry,
        tx,
    };
    let slug = ctx.runtime.resolved.slug.clone();

    let mut counters = SessionCounters::default();

    // Welcome is enqueued before any inbound frame is read: by the time a frame
    // is dispatched, Welcome already sits ahead of every response in the FIFO
    // writer queue, so a "frame before Welcome" class is unrepresentable.
    let welcome = ServerFrame::Welcome {
        surface: slug.clone(),
        participant_id: ctx.runtime.participant.as_str().to_string(),
        heartbeat_secs,
        max_body_bytes: ctx.runtime.max_body_bytes as u64,
        alert_granted: ctx.runtime.policy.grants.has(AppCapability::SurfaceAlert),
        takeover_granted: ctx
            .runtime
            .policy
            .grants
            .has(AppCapability::SurfaceTakeover),
        // Error-report floor: `Some(floor)` when `surface_error_channel` is
        // configured (the reserved port is bound), else `None` (console-only).
        error_report_floor: ctx.runtime.error_report_floor,
        // The heartbeat cadence the shell reports status on. The operator tunes
        // it; the shell never guesses.
        surface_description: SurfaceDescription {
            status_interval_secs: ctx.runtime.description.status_interval_secs,
        },
        bindings: ctx.runtime.bindings.clone(),
    };
    if let FrameOutcome::Disconnect = send_frame(&ctx.tx, welcome, &mut counters).await {
        // Writer already exited (socket died at upgrade): tear down.
        drop(ctx);
        writer.await.expect("surface writer task panicked");
        drop(guard);
        return;
    }
    // Behind `Welcome` in the same FIFO writer queue, before any inbound frame is
    // read: the page cleared its mirrors at `Welcome`, so these frames are what
    // refills them and their absence is what says a set is empty.
    if let FrameOutcome::Disconnect = seed_deferred_views(&ctx, &mut counters).await {
        drop(ctx);
        writer.await.expect("surface writer task panicked");
        drop(guard);
        return;
    }
    info!("surface session connected");

    let reap_after = heartbeat * 3;
    let mut last_inbound = Instant::now();
    let mut liveness = tokio::time::interval(heartbeat);
    liveness.set_missed_tick_behavior(MissedTickBehavior::Delay);
    liveness.tick().await; // consume the immediate first tick

    // Per-connection rate buckets, grouped so frame handlers take one bundle
    // rather than a growing list of parallel `&mut TokenBucket` params. Each
    // starts full, so the first burst is admitted before limiting begins.
    //   - `subscribe` gates both Subscribe and Unsubscribe of both classes
    //     (metering only durable would leak the class distinction to a probe);
    //     beyond-bucket is a protocol violation.
    //   - `alert` is tighter than `publish` since alerts page a human.
    //   - `publish` caps from this surface's config and trips before the
    //     bus-level per-sender gate (defense in depth).
    let mut buckets = SessionBuckets {
        subscribe: TokenBucket::new(SUBSCRIBE_BURST, SUBSCRIBE_REFILL, 1),
        alert: TokenBucket::new(ALERT_BURST, ALERT_REFILL, 1),
        publish: TokenBucket::new(
            ctx.runtime.resolved.publish_burst,
            Duration::from_secs(1),
            ctx.runtime.resolved.publish_per_sec,
        ),
    };

    // Active ephemeral subscriptions, keyed by (instance, channel) — the
    // subscribing principal's grain, so sibling instances on one channel are
    // separate entries rather than a duplicate. The map *is* the subscription
    // table: `contains_key` answers "already active", `insert` is Subscribe, and
    // dropping a value is the bus detach.
    let mut subscriptions: StreamMap<SubKey, SubscriptionStream> = StreamMap::new();

    // Durable subscription state: the local active mirror, the registry-shared
    // active set (read by the router fan-out), and the connection-lifetime
    // per-channel replay-dedup sets, kept in sync inside `DurableSessionState`.
    let mut durable = DurableSessionState::new(durable_subs);

    // Per-subscription wire position state: span seqs and durable high-waters.
    let mut spans = WireSpans::new();

    // Most recent shell-reported instance list, retained so the teardown terminal
    // `disconnected` snapshot can carry the last-known instances (empty if
    // the shell never reported a status this session).
    let mut last_status_instances: Vec<InstanceReport> = Vec::new();

    let mut violation = false;
    loop {
        tokio::select! {
            // A live delivery from any active subscription. Guarded because an
            // empty `StreamMap` yields `None` immediately (busy-loop otherwise).
            maybe_delivery = subscriptions.next(), if !subscriptions.is_empty() => {
                if let Some((sub, item)) = maybe_delivery {
                    // Deliberate: this arm and the ephemeral replay loop in
                    // handle_subscribe deep-clone the envelope (body up to
                    // max_body_bytes) per delivery per session rather than
                    // threading the Arc<EphemeralDelivery> to the writer. The
                    // clone is a small fraction of the serialize+socket-write
                    // that immediately follows on the same bytes; removing it
                    // would change the writer's payload type for an unmeasured
                    // win. Accepted cost; revisit only with profiling data.
                    // A context feed has no push window for `dropped` to
                    // describe: its rows are the page ring's diet, and the page
                    // keeps no queue behind them to overflow. Broadcast-lag loss
                    // can still happen on the bus — on a context-only
                    // subscription it surfaces, if at all, as thinner retained
                    // context, never as a drop counter.
                    let dropped = if ctx.runtime.push_enabled(&sub) {
                        item.dropped
                    } else {
                        0
                    };
                    let epoch = ctx.runtime.messenger().ring_epoch();
                    let ring_seq = item.delivery.seq;
                    let mut targets = vec![mint_target(
                        &mut spans,
                        &sub,
                        dropped,
                        DeliverKind::Ephemeral { epoch, ring_seq },
                    )];
                    // Coalesce the same publish's copies on this connection's
                    // sibling subscriptions of the channel: one broadcast send
                    // puts the message into every sibling stream atomically, so a
                    // sibling with no backlog has it at its head right now. A
                    // sibling holding older traffic ahead of its copy stays out —
                    // its own order wins over coalescing.
                    for (other, stream) in subscriptions.iter_mut() {
                        if other == &sub || other.channel != sub.channel {
                            continue;
                        }
                        let head_matches = stream
                            .head_now()
                            .is_some_and(|h| h.delivery.envelope.message_id == item.delivery.envelope.message_id);
                        if !head_matches {
                            continue;
                        }
                        let sibling_item = stream.take_head().expect("head_now reported an item");
                        let sibling_dropped = if ctx.runtime.push_enabled(other) {
                            sibling_item.dropped
                        } else {
                            0
                        };
                        targets.push(mint_target(
                            &mut spans,
                            other,
                            sibling_dropped,
                            DeliverKind::Ephemeral { epoch, ring_seq: sibling_item.delivery.seq },
                        ));
                    }
                    if let FrameOutcome::Disconnect = send_multi_deliver(
                        &ctx, sub.channel.clone(), item.delivery.envelope.as_ref().clone(), targets, &mut counters,
                    ).await {
                        break;
                    }
                }
            }
            // A push from outside this task: a live durable row or a deferred-view
            // snapshot.
            Some(push) = push_rx.recv() => {
                // Take every co-available push before writing: the router queues one
                // message's sibling rows back to back, so they coalesce into one
                // frame.
                let mut pushes = vec![push];
                while let Ok(next) = push_rx.try_recv() {
                    pushes.push(next);
                }
                if let FrameOutcome::Disconnect =
                    send_session_pushes(&ctx, &durable, &mut spans, pushes, &mut counters).await
                {
                    break;
                }
            }
            // Eager-wake nudge: serve every active durable channel its suffix.
            // The router queues its live copy before firing the nudge, so anything
            // already in hand goes out first — as the frame the fan-out composed,
            // not as a retention read that would beat it to the position.
            _ = drain_notify.notified() => {
                let mut pushes = Vec::new();
                while let Ok(next) = push_rx.try_recv() {
                    pushes.push(next);
                }
                if !pushes.is_empty()
                    && let FrameOutcome::Disconnect =
                        send_session_pushes(&ctx, &durable, &mut spans, pushes, &mut counters).await
                {
                    break;
                }
                if let FrameOutcome::Disconnect =
                    drain_all_durable(&ctx, &durable, &mut spans, &mut counters).await
                {
                    break;
                }
            }
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        counters.frames_in += 1;
                        match handle_client_frame(
                            &ctx,
                            text.as_str(),
                            &mut subscriptions,
                            &mut durable,
                            &mut spans,
                            &mut buckets,
                            &mut counters,
                            &mut last_status_instances,
                        )
                        .await
                        {
                            FrameOutcome::Continue => last_inbound = Instant::now(),
                            FrameOutcome::Violation(detail) => {
                                log_and_alert_security_event(
                                    &ctx.alert_dispatcher,
                                    SecurityEventType::SurfaceProtocolViolation,
                                    ctx.ip,
                                    &detail,
                                );
                                violation = true;
                                break;
                            }
                            // Writer gone (socket died mid-send): tear down.
                            FrameOutcome::Disconnect => break,
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        log_and_alert_security_event(
                            &ctx.alert_dispatcher,
                            SecurityEventType::SurfaceProtocolViolation,
                            ctx.ip,
                            &format!("surface {slug} user {}: binary frame", ctx.username),
                        );
                        violation = true;
                        break;
                    }
                    // axum auto-pongs inbound pings; an inbound Pong is the
                    // client answering our liveness probe.
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                        last_inbound = Instant::now();
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        info!("surface session closed by client");
                        break;
                    }
                    Some(Err(e)) => {
                        // axum wraps the underlying tungstenite error and exposes
                        // it via `into_inner`, so a downcast is deterministic —
                        // provided our direct `tungstenite` dep stays version-
                        // unified with axum's tokio-tungstenite (Cargo.toml notes
                        // this; `surface_ws_oversized_frame_is_violation_and_kills`
                        // fails if they drift). A
                        // frame-cap overflow (Capacity(MessageTooLong), the
                        // `max_message_size` cap firing) is a protocol violation:
                        // no config-legal frame can exceed the derived cap, so it
                        // is tampering or a serious client bug. Every other read
                        // error (TCP resets, proxy framing) tears down without a
                        // security event.
                        let inner = e.into_inner();
                        let oversized = inner
                            .downcast_ref::<tungstenite::Error>()
                            .is_some_and(|te| {
                                matches!(
                                    te,
                                    tungstenite::Error::Capacity(
                                        tungstenite::error::CapacityError::MessageTooLong { .. }
                                    )
                                )
                            });
                        if oversized {
                            log_and_alert_security_event(
                                &ctx.alert_dispatcher,
                                SecurityEventType::SurfaceProtocolViolation,
                                ctx.ip,
                                &format!(
                                    "surface {slug} user {}: inbound frame exceeds size cap",
                                    ctx.username
                                ),
                            );
                            violation = true;
                        } else {
                            warn!("surface WS read error: {inner}");
                        }
                        break;
                    }
                }
            }
            _ = liveness.tick() => {
                if last_inbound.elapsed() > reap_after {
                    info!("surface session reaped: no inbound liveness within 3x heartbeat");
                    break;
                }
            }
        }
    }

    // Terminal disconnected snapshot: when this is the last session attached to
    // the slug, write a
    // `disconnected` status document so the retained value itself says the surface
    // is down without timestamp math. "Last" is decided atomically by removing our
    // own registration and reading the post-removal count: consulting the count
    // while still registered races two concurrent closers into both seeing the
    // other and both skipping the stamp. `guard`'s own `Drop` (below) becomes a
    // no-op after this. Runs before `drop(ctx)` because it publishes through
    // `ctx.runtime`; not writing while another session survives prevents a
    // departing device from overwriting a live one's health.
    //
    // The atomicity covers concurrent *closers* only. A new session that
    // registers and publishes its first status between this removal and this
    // stamp landing (a browser reload closing the old socket as the new page
    // connects) can be transiently overwritten by this `disconnected` row; the
    // new session's next heartbeat corrects it within one `status_interval_secs`
    // — the same staleness-bounded convergence the retained-status model relies
    // on as its fallback disconnect signal.
    let remaining_sessions = guard.unregister_returning_remaining();
    if remaining_sessions == 0 {
        let description = &ctx.runtime.description;
        let session = ctx.session_id.simple().to_string();
        let epoch = ctx.runtime.messenger().ring_epoch();
        let body = telemetry::disconnected_body(
            &slug,
            Some(&session),
            epoch,
            "session closed",
            &last_status_instances,
        );
        // Same platform publish + panic discipline as the runtime telemetry path;
        // the connection is being torn down, so a Disconnect outcome is moot.
        publish_platform_telemetry(&ctx, &description.status_channel, &body, "terminal status")
            .await;
    }

    // Single teardown path: drop the subscription map (detaching every receiver
    // from the bus), drop the context (its `tx` is the writer sender, so the
    // writer exits and the socket closes), await it, drop the registry guard
    // (slot released even on panic).
    drop(subscriptions);
    drop(ctx);
    writer.await.expect("surface writer task panicked");
    drop(guard);
    info!(
        violation,
        frames_in = counters.frames_in,
        frames_out = counters.frames_out,
        publishes = counters.publishes,
        publish_rate_limited = counters.publish_rate_limited,
        publish_body_too_large = counters.publish_body_too_large,
        publish_body_cap_disagreement = counters.publish_body_cap_disagreement,
        alerts_dispatched = counters.alerts_dispatched,
        alerts_suppressed = counters.alerts_suppressed,
        // Rendered via `Debug` on a `BTreeMap`, so the breakdown is one
        // deterministically-ordered field on the existing line rather than N
        // extra lines per disconnect. Keys are boot-declared instance ids, not
        // client strings.
        by_instance = ?counters.by_instance,
        "surface session disconnected"
    );
}

/// Per-session counters folded into the single disconnect `info!` line. Frame
/// counts cover the application frames the session task processes and enqueues;
/// the writer's liveness `Ping`/`Heartbeat` frames are transport plumbing and
/// are not counted here.
#[derive(Default)]
struct SessionCounters {
    /// Inbound text (application) frames dispatched. Binary-frame and
    /// cap-overflow violations tear down before this counts them.
    frames_in: u64,
    /// Server frames the session task enqueued to the writer.
    frames_out: u64,
    /// Publishes that reached the bus with an `Ok` outcome.
    publishes: u64,
    /// Publishes denied by either rate gate — the connection bucket or the
    /// bus-level per-sender gate.
    publish_rate_limited: u64,
    /// Publishes rejected for an oversized body at the transport pre-check.
    /// Drives the first-occurrence warn and the escalation-to-violation count.
    publish_body_too_large: u64,
    /// Publishes where the transport pre-check admitted a body the bus then
    /// rejected as oversized — a config-wiring bug (both caps derive from
    /// `config.messaging.max_body_bytes`). Each such arm already `error!`s; this
    /// counter keeps them out of the transport-reject count so escalation is not
    /// conflated with an internal disagreement.
    publish_body_cap_disagreement: u64,
    /// `Alert` frames dispatched to the process `AlertDispatcher` (granted, and
    /// within the per-connection alert bucket) — the operator's count of how many
    /// times this session paged.
    alerts_dispatched: u64,
    /// `Alert` frames dropped by the per-connection alert bucket. Not a kill (a
    /// noisy but legitimate surface must not lose its session); the process-wide
    /// alert rate limiter bounds total paging downstream.
    alerts_suppressed: u64,
    /// Per-principal publish breakdown, keyed by component instance — the same
    /// grain the send budget meters and the sender identity carries, so the
    /// question "which component drained its budget?" is answerable from the
    /// disconnect line without correlating against the bus.
    ///
    /// **Does not sum to `publishes`/`publish_rate_limited`**, by construction:
    /// the kernel's own publishes (an error report with no subject) carry the
    /// bare surface identity and have no instance column. The totals are the
    /// session's; this is the attributable part of them.
    by_instance: BTreeMap<String, InstancePublishCounters>,
}

/// One principal's publish outcomes within a session ([`SessionCounters`]).
#[derive(Debug, Default, PartialEq, Eq)]
struct InstancePublishCounters {
    /// Publishes this principal landed on the bus.
    publishes: u64,
    /// Publishes denied by either rate gate — the connection bucket or this
    /// principal's own send budget. A component looping on retries shows up
    /// here, under its own name.
    publish_rate_limited: u64,
}

impl SessionCounters {
    /// Count one publish that reached the bus, surface-wide and against the
    /// principal that made it.
    ///
    /// `principal` is `None` for a kernel-grain publish (a self-report with no
    /// subject component), which has no instance column — see `by_instance`.
    /// Both counters move together here rather than at each call site so the
    /// breakdown cannot silently stop tracking the total it decomposes.
    fn publish_ok(&mut self, principal: Option<&str>) {
        self.publishes += 1;
        if let Some(instance) = principal {
            self.by_instance
                .entry(instance.to_string())
                .or_default()
                .publishes += 1;
        }
    }

    /// Count one publish denied by a rate gate, surface-wide and against the
    /// principal that made it. See [`SessionCounters::publish_ok`].
    fn publish_rate_limited(&mut self, principal: Option<&str>) {
        self.publish_rate_limited += 1;
        if let Some(instance) = principal {
            self.by_instance
                .entry(instance.to_string())
                .or_default()
                .publish_rate_limited += 1;
        }
    }
}

/// Immutable per-session context every frame handler reads but none mutates.
/// Built once in `session_loop` and passed as `&SessionCtx`, so handler
/// signatures carry one shared reference rather than a positional list of
/// same-typed identity params (`slug`, `username`, `ip`, …) a caller could
/// transpose. The genuinely mutable per-session state — the subscription map,
/// the rate buckets, the counters — is threaded separately as `&mut`. `slug`
/// is not stored: it is `runtime.resolved.slug`.
struct SessionCtx {
    runtime: Arc<SurfaceRuntime>,
    session_id: Uuid,
    username: String,
    ip: IpAddr,
    /// Process alert dispatcher, cloned once from `AppState` at session start.
    /// Read-only per-session handle: `handle_alert` pages through it and the
    /// session loop routes `SurfaceProtocolViolation` security events through it.
    alert_dispatcher: AlertDispatcher,
    /// The attached-session registry. Reached for the planes whose subject is the
    /// surface rather than the connection — a deferred view belongs to a
    /// sub-identity every tab shares, so a change one session causes is pushed at
    /// all of them.
    registry: SurfaceRegistry,
    /// Outbound frame sender to the writer task. Owning it here means dropping
    /// the context at teardown closes the channel and exits the writer.
    tx: mpsc::Sender<ServerFrame>,
}

/// One inbound [`ClientFrame::Publish`]'s fields, borrowed for the duration of
/// the handler. Bundled rather than passed positionally because `instance`,
/// `port`, `body`, and `subject_instance` are all `&str`-ish and a transposition
/// would typecheck — `instance`/`subject_instance` especially, where swapping
/// them would silently misattribute a publish's identity.
struct PublishRequest<'a> {
    instance: &'a str,
    port: &'a str,
    body: &'a str,
    correlation: Option<u64>,
    /// The report subject for the reserved error-report port; `None` otherwise.
    /// See [`ClientFrame::Publish`].
    subject_instance: Option<&'a str>,
    /// The component's per-message urgency override; `None` ⇒ the bound port's
    /// configured default. See [`ClientFrame::Publish`].
    urgency: Option<Urgency>,
}

/// What the session loop does after a dispatched inbound frame.
enum FrameOutcome {
    /// Frame handled; keep the session running.
    Continue,
    /// Protocol violation: the caller logs+alerts it as a
    /// `SurfaceProtocolViolation` and tears the session down. The detail names
    /// the surface, user, and violated rule, and never echoes the client
    /// payload.
    Violation(String),
    /// The writer is gone (socket died mid-send): tear the session down without
    /// a security event.
    Disconnect,
}

/// Session-local durable-subscription state. `active` (the local mirror) and the
/// registry-shared `shared` set move strictly together through
/// [`activate`](Self::activate)/[`deactivate`](Self::deactivate) — the two-set
/// sync discipline lives here and nowhere else, so no handler can update one set
/// and forget the other.
///
/// Duplicate suppression is not held here: a subscription's durable high-water
/// ([`WireSpans::durable_high_water_of`]) is the only record of what this
/// connection has sent, so a second copy of a position — the replay racing the
/// live fan-out — is at or below it and dropped.
struct DurableSessionState {
    active: HashSet<SubKey>,
    shared: Arc<Mutex<HashSet<SubKey>>>,
}

impl DurableSessionState {
    fn new(shared: Arc<Mutex<HashSet<SubKey>>>) -> Self {
        Self {
            active: HashSet::new(),
            shared,
        }
    }

    /// Insert the subscription into both the local and registry-shared active
    /// sets. Inserting into `shared` is what makes the router start queuing live
    /// rows, so callers activate before the store read.
    fn activate(&mut self, sub: &SubKey) {
        self.shared
            .lock()
            .expect("durable_subs poisoned")
            .insert(sub.clone());
        self.active.insert(sub.clone());
    }

    /// Remove the subscription from both active sets. Returns whether it was
    /// active — the Unsubscribe-of-non-active violation check.
    fn deactivate(&mut self, sub: &SubKey) -> bool {
        let was_active = self.active.remove(sub);
        if was_active {
            self.shared
                .lock()
                .expect("durable_subs poisoned")
                .remove(sub);
        }
        was_active
    }

    fn is_active(&self, sub: &SubKey) -> bool {
        self.active.contains(sub)
    }
}

/// Session-owned per-subscription wire position state: the delivery-time span
/// seq counters and the durable high-waters cursors are minted from. There is
/// one serialized writer per connection, so this state needs no locking.
///
/// A span seq is a per-subscription counter reset to 0 at each `Subscribe` (the
/// span its `SubscribeResult` opens), incremented per `Deliver`, so the first
/// delivery on a span carries seq 1. Minting at the socket-write boundary makes
/// per-span monotonicity structural: nothing the router queues or a delayed
/// release re-orders can produce a wire regression.
///
/// A durable high-water is `max(rowid presented at the resume anchor, rowids
/// delivered this connection)`. A durable cursor is minted from the high-water
/// *after* advancing it to `max(high_water, this row's id)`, so a delayed-release
/// row below the high-water leaves it unmoved and repeats the unmoved cursor —
/// no duplicate replay next reconnect — while its wire seq is still the next
/// monotone span seq.
struct WireSpans {
    span_seq: HashMap<SubKey, u64>,
    durable_high_water: HashMap<SubKey, u64>,
    /// The store's boot `incarnation`, read once from the DB at the first durable
    /// `Subscribe` on the connection and stamped into every cursor minted
    /// thereafter. Constant for the connection's life (the store cannot re-boot
    /// under a live page), so a single read suffices. Catches the one staleness
    /// case the store cursor's epoch cannot: a backup restore that keeps epochs
    /// but rolls the high-water backwards.
    incarnation: Option<i64>,
    /// Per-durable-subscription numbering domain: the channel's `resume_epoch`,
    /// read from the store at `Subscribe` and stamped into every cursor minted for
    /// that subscription. Per channel, not per connection — each durable channel
    /// mints its own epoch with its row.
    durable_epoch: HashMap<SubKey, Uuid>,
}

impl WireSpans {
    fn new() -> Self {
        Self {
            span_seq: HashMap::new(),
            durable_high_water: HashMap::new(),
            incarnation: None,
            durable_epoch: HashMap::new(),
        }
    }

    /// Record the store's boot incarnation, read at a durable `Subscribe`.
    /// Idempotent: a second durable subscribe on the same connection re-reads the
    /// same value.
    fn set_incarnation(&mut self, incarnation: i64) {
        self.incarnation = Some(incarnation);
    }

    /// The store incarnation every cursor on this connection is stamped with.
    /// Returns `0` before any durable subscribe has read the value — vacuous on a
    /// ring (whose per-boot epoch already catches cross-boot cursors).
    fn incarnation(&self) -> i64 {
        self.incarnation.unwrap_or(0)
    }

    /// Reset the span counter for `sub` to 0. Called at every successful
    /// ephemeral `Subscribe`, before the `SubscribeResult` and replay, so the
    /// span's first `Deliver` mints seq 1.
    fn start_span(&mut self, sub: &SubKey) {
        self.span_seq.insert(sub.clone(), 0);
    }

    /// Reset the span counter for `sub` to 0, anchor its durable high-water at
    /// `anchor_high_water`, and record the channel's numbering domain `epoch`.
    fn start_durable_span(&mut self, sub: &SubKey, epoch: Uuid, anchor_high_water: u64) {
        self.span_seq.insert(sub.clone(), 0);
        self.durable_high_water
            .insert(sub.clone(), anchor_high_water);
        self.durable_epoch.insert(sub.clone(), epoch);
    }

    /// Drop all wire state for `sub` (unsubscribe / teardown).
    fn clear(&mut self, sub: &SubKey) {
        self.span_seq.remove(sub);
        self.durable_high_water.remove(sub);
        self.durable_epoch.remove(sub);
    }

    /// The subscription's current durable high-water — the newest retention
    /// position written to this socket for it. A durable send at or below it is a
    /// second copy of a position the client already has (the replay racing the
    /// live fan-out) and is dropped. `None` if no durable span was anchored (no
    /// durable `Subscribe` yet).
    fn durable_high_water_of(&self, sub: &SubKey) -> Option<u64> {
        self.durable_high_water.get(sub).copied()
    }

    /// The numbering domain the subscription's positions are minted in. `None`
    /// if no durable span was anchored.
    fn durable_epoch_of(&self, sub: &SubKey) -> Option<Uuid> {
        self.durable_epoch.get(sub).copied()
    }

    /// The next span seq for `sub`. Panics if no span was started — every
    /// `Deliver` follows a `Subscribe` that started one.
    fn next_seq(&mut self, sub: &SubKey) -> u64 {
        let seq = self
            .span_seq
            .get_mut(sub)
            .expect("surface session: Deliver on a subscription with no started span");
        *seq += 1;
        *seq
    }

    /// The `(span seq, cursor)` for an ephemeral `Deliver` of the row at
    /// `(epoch, ring_seq)`.
    fn next_ephemeral(&mut self, sub: &SubKey, epoch: Uuid, ring_seq: u64) -> (u64, Cursor) {
        let seq = self.next_seq(sub);
        let cursor = cursor::mint(self.incarnation(), epoch, ring_seq);
        (seq, cursor)
    }

    /// The `(span seq, cursor)` for a durable `Deliver` of the row at retention
    /// position `retained_seq`. Advances the high-water to
    /// `max(high_water, retained_seq)` and mints the cursor from it.
    fn next_durable(&mut self, sub: &SubKey, retained_seq: u64) -> (u64, Cursor) {
        let seq = self.next_seq(sub);
        let incarnation = self.incarnation.expect(
            "surface session: durable Deliver before the store incarnation was read at Subscribe",
        );
        let epoch = *self
            .durable_epoch
            .get(sub)
            .expect("surface session: durable Deliver on a subscription with no anchored epoch");
        let hw = self.durable_high_water.get_mut(sub).expect(
            "surface session: durable Deliver on a subscription with no anchored high-water",
        );
        *hw = (*hw).max(retained_seq);
        let hw_value = *hw;
        (seq, cursor::mint(incarnation, epoch, hw_value))
    }
}

/// Which class of `Deliver` [`send_deliver`] is minting position for.
enum DeliverKind {
    Ephemeral { epoch: Uuid, ring_seq: u64 },
    Durable { retained_seq: u64 },
}

/// Mint the span seq + cursor for one subscription's share of a `Deliver` at the
/// single socket-write boundary. The one place span seqs are assigned, so
/// per-span monotonicity is structural.
///
/// Minting is per-`SubKey` and stays that way: a frame carrying several targets
/// is a pure encoding change over per-subscription state, never a shared window
/// or a shared cursor.
fn mint_target(
    spans: &mut WireSpans,
    sub: &SubKey,
    dropped: u64,
    kind: DeliverKind,
) -> DeliverTarget {
    let (seq, cursor) = match kind {
        DeliverKind::Ephemeral { epoch, ring_seq } => spans.next_ephemeral(sub, epoch, ring_seq),
        DeliverKind::Durable { retained_seq } => spans.next_durable(sub, retained_seq),
    };
    DeliverTarget {
        instance: sub.instance.clone(),
        seq,
        cursor,
        dropped,
    }
}

/// Mint one subscription's position and write it as a single-target `Deliver`.
///
/// A single-target frame is the honest encoding wherever targets legitimately
/// diverge — replay, subscribe-time context, a sibling subscribed mid-stream or
/// lagging behind its own backlog. Live fan-out of one publish to sibling
/// subscriptions coalesces into one multi-target frame instead; that is the
/// caller's decision, made where co-availability is known.
async fn send_deliver(
    ctx: &SessionCtx,
    spans: &mut WireSpans,
    sub: &SubKey,
    envelope: MessageEnvelope,
    dropped: u64,
    kind: DeliverKind,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let target = mint_target(spans, sub, dropped, kind);
    send_multi_deliver(ctx, sub.channel.clone(), envelope, vec![target], counters).await
}

/// Write one `Deliver` carrying the envelope once for every target that shares
/// it. Targets are already minted from their own subscription's state; this is
/// the encoding boundary and nothing else.
async fn send_multi_deliver(
    ctx: &SessionCtx,
    channel: String,
    envelope: MessageEnvelope,
    targets: Vec<DeliverTarget>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    assert!(
        !targets.is_empty(),
        "surface session: Deliver frame with no targets"
    );
    let frame = ServerFrame::Deliver {
        channel,
        envelope,
        targets,
    };
    send_frame(&ctx.tx, frame, counters).await
}

/// Write one turn's worth of pushes, splitting the two planes that share the
/// session's push queue.
///
/// The durable rows go first, as one coalesced pass, so the batching
/// [`send_durable_live`] depends on is not broken up by an interleaved view.
/// The views follow in arrival order, which is emission order — each is a full
/// replacement, so the last one for a `(channel, instance)` is the answer the
/// page keeps.
async fn send_session_pushes(
    ctx: &SessionCtx,
    durable: &DurableSessionState,
    spans: &mut WireSpans,
    pushes: Vec<SessionPush>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let mut rows = Vec::new();
    let mut views = Vec::new();
    for push in pushes {
        match push {
            SessionPush::Durable(delivery) => rows.push(delivery),
            SessionPush::DeferredView(view) => views.push(view),
        }
    }
    if !rows.is_empty()
        && let FrameOutcome::Disconnect =
            send_durable_live(ctx, durable, spans, rows, counters).await
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

/// Write one deferred-view snapshot to this connection.
async fn send_deferred_view(
    ctx: &SessionCtx,
    view: DeferredViewPush,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let DeferredViewPush {
        channel,
        instance,
        entries,
    } = view;
    let frame = ServerFrame::DeferredView {
        channel,
        instance,
        entries,
    };
    send_frame(&ctx.tx, frame, counters).await
}

/// Send one turn's worth of live durable router deliveries, coalescing the rows
/// of one message across this connection's sibling subscriptions into one frame.
///
/// Groups by (channel, retention position) in first-appearance order, which
/// preserves each subscription's own delivery order: a subscription appears in at
/// most one group per position.
///
/// The subscription's high-water decides each copy, at send time, because a
/// send inside this batch moves it:
///
/// - at or below it — a second copy of a position this connection already wrote
///   (the fan-out racing the subscribe replay or a drain). Dropped: the client's
///   cursor already covers it.
/// - exactly one above it — the contiguous next position. Sent.
/// - further above it — something below this position never reached the wire
///   (a quiet row nobody woke for, a frame this session's queue was too full to
///   take). Sending it alone would strand the interior span under a high-water
///   that had moved past it, so the live copy is dropped and the subscription is
///   served its whole suffix from retention instead.
async fn send_durable_live(
    ctx: &SessionCtx,
    durable: &DurableSessionState,
    spans: &mut WireSpans,
    batch: Vec<DurableDelivery>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    /// One row's deliveries: the envelope, and every subscription it is bound for.
    struct RowGroup {
        channel: String,
        retained_seq: u64,
        envelope: Arc<MessageEnvelope>,
        subs: Vec<SubKey>,
    }
    let mut groups: Vec<RowGroup> = Vec::new();
    for DurableDelivery {
        envelope,
        retained_seq,
        sub,
    } in batch
    {
        if !durable.is_active(&sub) {
            debug!(
                channel = %sub.channel,
                instance = ?sub.instance,
                retained_seq,
                "durable live delivery for inactive subscription; dropping"
            );
            continue;
        }
        match groups
            .iter_mut()
            .find(|g| g.retained_seq == retained_seq && g.channel == sub.channel)
        {
            Some(group) => group.subs.push(sub),
            None => groups.push(RowGroup {
                channel: sub.channel.clone(),
                retained_seq,
                envelope,
                subs: vec![sub],
            }),
        }
    }
    for RowGroup {
        channel,
        retained_seq,
        envelope,
        subs,
    } in groups
    {
        let mut targets = Vec::with_capacity(subs.len());
        let mut behind = Vec::new();
        for sub in &subs {
            let hw = spans.durable_high_water_of(sub).expect(
                "surface session: live durable delivery for a subscription with no anchored \
                 high-water — activation anchors one",
            );
            if retained_seq <= hw {
                debug!(
                    channel = %sub.channel,
                    instance = ?sub.instance,
                    retained_seq,
                    high_water = hw,
                    "durable live delivery at or below the subscription high-water; dropping \
                     the duplicate"
                );
            } else if retained_seq == hw + 1 {
                targets.push(mint_target(
                    spans,
                    sub,
                    0,
                    DeliverKind::Durable { retained_seq },
                ));
            } else {
                debug!(
                    channel = %sub.channel,
                    instance = ?sub.instance,
                    retained_seq,
                    high_water = hw,
                    "durable live delivery above the contiguous next position; serving the \
                     subscription its suffix from retention instead"
                );
                behind.push(sub.clone());
            }
        }
        if !targets.is_empty()
            && let FrameOutcome::Disconnect =
                send_multi_deliver(ctx, channel, (*envelope).clone(), targets, counters).await
        {
            return FrameOutcome::Disconnect;
        }
        for sub in &behind {
            if let FrameOutcome::Disconnect = drain_durable_channel(ctx, spans, sub, counters).await
            {
                return FrameOutcome::Disconnect;
            }
        }
    }
    FrameOutcome::Continue
}

/// The per-connection rate buckets, grouped so frame handlers take one bundle
/// instead of a growing list of parallel `&mut TokenBucket` params.
struct SessionBuckets {
    subscribe: TokenBucket,
    publish: TokenBucket,
    alert: TokenBucket,
}

/// Parse and dispatch one inbound frame.
///
/// Unparseable input — malformed JSON or an unknown `type` — is a violation:
/// the build-ID handshake guarantees a live client is never a version behind,
/// so unparseable traffic is a bug or tampering. `Alert` is grant-gated and
/// `Publish`-disciplined: ungranted or oversized is a violation, beyond-bucket
/// is dropped, and an admitted alert dispatches to the process
/// `AlertDispatcher`. `Subscribe` attaches an ephemeral or durable
/// subscription. `Publish` resolves a bound output — or the reserved
/// error-report port — and publishes onto the bus behind the connection rate
/// bucket. `Unsubscribe` removes an active subscription (ephemeral or durable);
/// unsubscribing a non-active channel is a violation.
#[allow(clippy::too_many_arguments)]
async fn handle_client_frame(
    ctx: &SessionCtx,
    text: &str,
    subscriptions: &mut StreamMap<SubKey, SubscriptionStream>,
    durable: &mut DurableSessionState,
    spans: &mut WireSpans,
    buckets: &mut SessionBuckets,
    counters: &mut SessionCounters,
    last_status_instances: &mut Vec<InstanceReport>,
) -> FrameOutcome {
    let frame = match serde_json::from_str::<ClientFrame>(text) {
        Ok(frame) => frame,
        Err(_) => {
            return FrameOutcome::Violation(format!(
                "surface {} user {}: unparseable client frame",
                ctx.runtime.resolved.slug, ctx.username
            ));
        }
    };
    match frame {
        ClientFrame::PublishBatch {
            instance,
            correlation,
            publishes,
            deferred_ops,
        } => {
            handle_publish_batch(
                ctx,
                &instance,
                correlation,
                &publishes,
                &deferred_ops,
                counters,
            )
            .await
        }
        ClientFrame::Subscribe {
            channel,
            instance,
            resume,
        } => {
            if let Err(violation) = charge_subscribe_token(ctx, &mut buckets.subscribe) {
                return violation;
            }
            handle_subscribe(
                ctx,
                subscriptions,
                durable,
                spans,
                SubKey { instance, channel },
                resume,
                counters,
            )
            .await
        }
        ClientFrame::Publish {
            instance,
            port,
            body,
            correlation,
            subject_instance,
            urgency,
        } => {
            handle_publish(
                ctx,
                &mut buckets.publish,
                PublishRequest {
                    instance: &instance,
                    port: &port,
                    body: &body,
                    correlation,
                    subject_instance: subject_instance.as_deref(),
                    urgency,
                },
                counters,
            )
            .await
        }
        ClientFrame::Alert {
            severity,
            title,
            body,
        } => handle_alert(ctx, &mut buckets.alert, counters, severity, &title, &body),
        ClientFrame::Unsubscribe { channel, instance } => {
            if let Err(violation) = charge_subscribe_token(ctx, &mut buckets.subscribe) {
                return violation;
            }
            handle_unsubscribe(
                ctx,
                subscriptions,
                durable,
                spans,
                SubKey { instance, channel },
            )
        }
        ClientFrame::Geometry {
            width,
            height,
            device_pixel_ratio,
        } => handle_geometry(ctx, &mut buckets.publish, width, height, device_pixel_ratio).await,
        ClientFrame::Status {
            instances,
            uptime_secs,
            counters,
            overlay,
        } => {
            handle_status(
                ctx,
                &mut buckets.publish,
                &telemetry::StatusReport {
                    instances: &instances,
                    uptime_secs,
                    counters: &counters,
                    overlay: overlay.as_ref(),
                },
                last_status_instances,
            )
            .await
        }
    }
}

/// Handle a `Geometry` telemetry frame.
///
/// An out-of-bounds value is a protocol violation — the browser is untrusted even
/// when authenticated, and the log feeds fail2ban. Otherwise the frame is counted
/// against the per-connection
/// publish bucket, wrapped into a server-stamped document, and published to the
/// surface's derived geometry channel via the platform-telemetry path (exempt
/// from the per-surface send budget). Telemetry has no wire ack.
async fn handle_geometry(
    ctx: &SessionCtx,
    publish_bucket: &mut TokenBucket,
    width: u32,
    height: u32,
    device_pixel_ratio: f64,
) -> FrameOutcome {
    let slug = &ctx.runtime.resolved.slug;
    let username = &ctx.username;
    let description = &ctx.runtime.description;
    if let Err(rule) = telemetry::validate_geometry(width, height, device_pixel_ratio) {
        return FrameOutcome::Violation(format!("surface {slug} user {username}: {rule}"));
    }
    // Count against the per-connection publish bucket; a denied telemetry frame is
    // dropped (no ack), never a kill — a legitimate resize storm is debounced
    // shell-side and this is defense in depth.
    if telemetry_bucket_denied(publish_bucket) {
        return FrameOutcome::Continue;
    }
    let session = ctx.session_id.simple().to_string();
    let body = telemetry::geometry_body(slug, &session, width, height, device_pixel_ratio);
    publish_platform_telemetry(ctx, &description.geometry_channel, &body, "geometry").await
}

/// Handle a `Status` telemetry frame.
///
/// An instance the surface does not configure, or an over-long `reason`, is a
/// protocol violation. The server derives the health summary from the reported facts (the shell is
/// untrusted; it reports raw states), wraps the snapshot, and publishes it to the
/// derived status channel via the platform-telemetry path.
async fn handle_status(
    ctx: &SessionCtx,
    publish_bucket: &mut TokenBucket,
    report: &telemetry::StatusReport<'_>,
    last_status_instances: &mut Vec<InstanceReport>,
) -> FrameOutcome {
    let slug = &ctx.runtime.resolved.slug;
    let username = &ctx.username;
    let description = &ctx.runtime.description;
    // Configured instance → kind, and the pump count each instance should have,
    // both precomputed once at boot on the description runtime (boot-constant, so
    // not rebuilt per frame). The shell may report only configured instances.
    if let Err(rule) = telemetry::validate_status(report, &description.configured_kinds) {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: {}",
            sanitize_client_detail(&rule)
        ));
    }
    // Retain the validated report so a teardown terminal snapshot can carry
    // the last-known instances. Recorded even if the publish bucket later denies
    // this frame — the report itself is a truthful, well-formed observation.
    *last_status_instances = report.instances.to_vec();

    let health = telemetry::derive_health(report.instances, &description.expected_pumps);
    debug_assert!(
        health != Health::Disconnected,
        "live report never disconnected"
    );

    if telemetry_bucket_denied(publish_bucket) {
        return FrameOutcome::Continue;
    }
    let session = ctx.session_id.simple().to_string();
    let body = telemetry::status_body(
        slug,
        &session,
        ctx.runtime.messenger().ring_epoch(),
        health,
        report,
    );
    publish_platform_telemetry(ctx, &description.status_channel, &body, "status").await
}

/// Charge one token for a telemetry frame against the per-connection publish
/// bucket. Returns `true` when the bucket denied it (the frame is dropped, no
/// ack). A denied telemetry frame is never a kill.
fn telemetry_bucket_denied(publish_bucket: &mut TokenBucket) -> bool {
    match publish_bucket.try_consume() {
        TokenBucketOutcome::Granted | TokenBucketOutcome::GrantedAfterSuppression { .. } => false,
        TokenBucketOutcome::Denied { first } => {
            if first {
                warn!("rate-limiting surface telemetry from this connection");
            }
            true
        }
    }
}

/// Publish a server-constructed telemetry document via the platform path (exempt
/// from the per-surface send budget). Every non-`Ok` outcome except `BodyTooLarge`
/// is a broken boot invariant — the geometry/status channel is boot-declared,
/// single-writer, and covered by the surface's injected grant — so it panics.
/// `BodyTooLarge` is a late-discovered config error on a bounded, server-built
/// body: `error!` + continue rather than kill the connection over telemetry.
/// `BudgetExhausted` is unreachable on the exempt path (panic).
async fn publish_platform_telemetry(
    ctx: &SessionCtx,
    channel: &str,
    body: &str,
    kind: &str,
) -> FrameOutcome {
    let slug = &ctx.runtime.resolved.slug;
    let messenger = ctx.runtime.messenger.as_ref().unwrap_or_else(|| {
        panic!(
            "surface {slug}: {kind} telemetry publish but runtime has no Messenger — a durable \
             derived channel implies messaging configured implies Some(messenger)"
        )
    });
    match messenger
        .publish_from_surface_platform(slug, channel, body, Urgency::Normal)
        .await
    {
        PublishResult::Ok { .. } => FrameOutcome::Continue,
        PublishResult::BodyTooLarge { len, max } => {
            error!(
                surface = %slug,
                channel = %channel,
                len,
                max,
                "surface {kind} telemetry publish rejected as oversized — the server-built body \
                 exceeds max_body_bytes; dropping this snapshot"
            );
            FrameOutcome::Continue
        }
        other => panic!(
            "surface {slug}: {kind} telemetry publish to {channel} did not succeed ({other:?}) — \
             the derived channel is boot-declared, single-writer, and covered by the surface's \
             injected geometry/status grant, and the platform path is send-budget exempt, so any \
             failure is a broken boot invariant"
        ),
    }
}

/// Charge one token for a `Subscribe`/`Unsubscribe` frame. An exhausted bucket is
/// a protocol violation, not a silent drop: dropping a Subscribe would desync the
/// client's subscription state machine, and a subscribe storm is not something a
/// correct client produces — the posture treats it as fail2ban signal. The bucket
/// starts full and admits `SUBSCRIBE_BURST` frames (see the constant), so an
/// honest maximum-size surface's first-connect reconcile plus one detach/re-attach
/// cycle never trips it.
fn charge_subscribe_token(
    ctx: &SessionCtx,
    subscribe_bucket: &mut TokenBucket,
) -> Result<(), FrameOutcome> {
    match subscribe_bucket.try_consume() {
        TokenBucketOutcome::Granted | TokenBucketOutcome::GrantedAfterSuppression { .. } => Ok(()),
        TokenBucketOutcome::Denied { .. } => Err(FrameOutcome::Violation(format!(
            "surface {} user {}: Subscribe/Unsubscribe rate exceeded",
            ctx.runtime.resolved.slug, ctx.username
        ))),
    }
}

/// Handle an `Unsubscribe` frame.
///
/// Fire-and-forget: an active subscription is removed (dropping the ephemeral
/// receiver is the bus detach; for durable, clearing the shared/local sets stops
/// the router fan-out), with no response frame. A channel with no active
/// subscription is a violation: only `SubscribeOutcome::Ok` creates one, and a
/// correct client tracks that.
///
/// `Deliver` frames for the removed channel may still sit in the outbound queue
/// (ephemeral) or the durable live queue and arrive after this; the client
/// contract (proto crate docs) is to discard them. A durable removal clears the
/// channel from both active sets, which stops the router fanning out to it, and
/// clears the subscription's span — so a live copy still queued from this span
/// is dropped rather than delivered. A re-subscribe re-anchors from the client's
/// echoed cursor and is served from retention, so nothing carries across the
/// cycle server-side.
fn handle_unsubscribe(
    ctx: &SessionCtx,
    subscriptions: &mut StreamMap<SubKey, SubscriptionStream>,
    durable: &mut DurableSessionState,
    spans: &mut WireSpans,
    sub: SubKey,
) -> FrameOutcome {
    // `remove` returns the removed stream (dropped here = bus detach) or `None`
    // when nothing was active. Unknown, unbound, and never-active subscriptions
    // are indistinguishable on the wire (no existence oracle): all violate. That
    // includes unsubscribing a *sibling's* live subscription: the key does not
    // match this instance's, so it is simply not active for the asker.
    if subscriptions.remove(&sub).is_some() {
        spans.clear(&sub);
        return FrameOutcome::Continue;
    }
    if durable.deactivate(&sub) {
        spans.clear(&sub);
        return FrameOutcome::Continue;
    }
    FrameOutcome::Violation(format!(
        "surface {} user {}: Unsubscribe of non-active subscription {} (instance {})",
        ctx.runtime.resolved.slug,
        ctx.username,
        sanitize_client_detail(&sub.channel),
        sanitize_client_detail(&sub.instance),
    ))
}

/// Parse an echoed resume [`Cursor`], mapping an unparseable one to the protocol
/// violation it is. A conforming client cannot produce one — cursors live only in
/// page memory and the build gate forces a reload before a stale-format page
/// reconnects — so an unparseable cursor kills the connection and logs for
/// fail2ban. A cursor minted for a *different* subscription is not unparseable;
/// it resolves to an epoch mismatch (a gap) at the store.
fn parse_resume_cursor(
    cursor: &Cursor,
    slug: &str,
    username: &str,
    channel: &str,
) -> Result<CursorState, FrameOutcome> {
    cursor::parse(cursor).map_err(|detail| {
        FrameOutcome::Violation(format!(
            "surface {slug} user {username}: unparseable resume cursor on {channel}: {detail}"
        ))
    })
}

/// Handle a `Subscribe` frame.
///
/// Validates the channel against the surface's config bindings and both active
/// subscription sets (ephemeral + durable), then dispatches on delivery class.
/// Durable channels project the backlog and (on resume) the retained window;
/// ephemeral channels attach the broadcast stream. The FIFO writer queue
/// serializes `SubscribeResult` → replay → live deliveries, so ordering holds by
/// construction.
// TODO(surface-wire-cursors): the echoed resume token, `replay_from`'s five-way
// decision, `WireSpans` and `GapInfo` are a parallel implementation of
// `SubscriberCursor` + window/advance. Re-grounding them in that vocabulary is
// its own design cycle.
async fn handle_subscribe(
    ctx: &SessionCtx,
    subscriptions: &mut StreamMap<SubKey, SubscriptionStream>,
    durable: &mut DurableSessionState,
    spans: &mut WireSpans,
    sub: SubKey,
    resume: Option<Cursor>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let runtime = &ctx.runtime;
    let slug = &ctx.runtime.resolved.slug;
    let username = &ctx.username;
    // Unknown channels, channels this surface does not bind, and channels bound
    // by a *different* instance are all indistinguishable on the wire (no
    // existence oracle): all the same violation. Keying the gate on the whole
    // subscription is what makes the third case a violation rather than a
    // silently mis-attributed subscription — the map holds exactly the
    // (instance, channel) pairs boot declared, so an instance cannot subscribe
    // on a sibling's binding.
    let class = match runtime.subscription_channels.get(&sub) {
        Some(facts) => facts.class,
        None => {
            return FrameOutcome::Violation(format!(
                "surface {slug} user {username}: Subscribe to unbound subscription {} \
                 (instance {})",
                sanitize_client_detail(&sub.channel),
                sanitize_client_detail(&sub.instance),
            ));
        }
    };

    // A duplicate Subscribe is a client bug (the client refcount table dedupes).
    // The check spans both subscription tables.
    if subscriptions.contains_key(&sub) || durable.is_active(&sub) {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: duplicate Subscribe to active subscription {} \
             (instance {:?})",
            sub.channel, sub.instance
        ));
    }

    match class {
        DeliveryClass::Durable => {
            return handle_durable_subscribe(ctx, durable, spans, sub, resume, counters).await;
        }
        // `local:` traffic never crosses the wire: the page-local router is its
        // sole source of truth, so the server never subscribes to one. Not
        // attacker-reachable — `class` is looked up from the boot-resolved
        // subscription map, which excludes `local:` bindings by construction
        // (`SurfaceRuntime::build`), so a client naming a local channel was
        // already killed by the unbound-channel violation above. Broken boot
        // invariant: die naming the real bug rather than falling into the
        // ephemeral arm, whose rejected-bound-channel panic would misdiagnose it
        // as missing boot ACL coverage.
        DeliveryClass::Local => panic!(
            "broken boot invariant: surface {slug} resolved a local: channel {} into the wire \
             subscription map; page-local channels are never subscribed over the wire",
            sub.channel
        ),
        DeliveryClass::Ephemeral => {}
    }

    let tx = &ctx.tx;
    let resume = match resume {
        None => None,
        Some(cursor) => match parse_resume_cursor(&cursor, slug, username, &sub.channel) {
            Ok(state) => Some(EphemeralResume {
                epoch: state.epoch,
                seq: state.seq,
            }),
            Err(outcome) => return outcome,
        },
    };

    let subscription = match runtime.messenger().attach_live(
        // The subscribing principal, at the grain it subscribed: an ephemeral
        // subscription opens no push window and keeps no cursor, so nothing here
        // is keyed by it — but the attach's ACL check and its own attribution
        // should name the principal that actually asked, not the page it rode in
        // on.
        sub.participant(slug),
        runtime.policy.clone(),
        &sub.channel,
        resume,
    ) {
        Ok(subscription) => subscription,
        // Boot validation proved every bound channel exists and is policy-covered
        // and policies are boot-static, so a denial here is a broken boot
        // invariant, not attacker-reachable (the only client influence — an
        // unbound channel name — was already killed as a violation above).
        Err(err) => panic!(
            "surface {slug}: live attach rejected bound channel {}: {err:?} — boot validation \
             guarantees every bound channel exists and is policy-covered",
            sub.channel
        ),
    };

    // A matching-epoch resume seq the store never assigned is impossible for an
    // honest client: escalate to a violation, sending nothing first.
    if let Replay::Gap(BusGapReason::ResumeAhead) = subscription.decision {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: resume seq ahead of assigned range on {}",
            sub.channel
        ));
    }

    let gap = match subscription.decision {
        Replay::Fresh | Replay::UpToDate | Replay::Exact => None,
        Replay::Gap(BusGapReason::EpochChanged) => Some(GapInfo {
            reason: ProtoGapReason::EpochChanged,
        }),
        Replay::Gap(BusGapReason::BeyondRetained) => Some(GapInfo {
            reason: ProtoGapReason::BeyondRetained,
        }),
        Replay::Gap(BusGapReason::ResumeAhead) => {
            unreachable!("ResumeAhead escalated to a violation above")
        }
    };

    let epoch = runtime.messenger().ring_epoch();
    let replay_count = subscription.replay.len() as u32;

    // Reset the span before the SubscribeResult so the replay rows mint seqs
    // 1..N.
    spans.start_span(&sub);

    let result = ServerFrame::SubscribeResult {
        channel: sub.channel.clone(),
        instance: sub.instance.clone(),
        outcome: SubscribeOutcome::Ok,
        replay_count,
        gap,
    };
    if let FrameOutcome::Disconnect = send_frame(tx, result, counters).await {
        spans.clear(&sub);
        return FrameOutcome::Disconnect;
    }

    for delivery in subscription.replay {
        // Deliberate clone; see the delivery-clone rationale at the live
        // select! arm in session_loop.
        let kind = DeliverKind::Ephemeral {
            epoch,
            ring_seq: delivery.seq,
        };
        if let FrameOutcome::Disconnect = send_deliver(
            ctx,
            spans,
            &sub,
            delivery.envelope.as_ref().clone(),
            0,
            kind,
            counters,
        )
        .await
        {
            spans.clear(&sub);
            return FrameOutcome::Disconnect;
        }
    }

    subscriptions.insert(sub, SubscriptionStream::new(subscription.receiver));
    FrameOutcome::Continue
}

/// Handle a `Subscribe` to a durable (`brenn:`) channel.
///
/// Activates the subscription, anchors its high-water at the echoed cursor (0 on
/// a fresh attach), and replays what retention holds above it. The echoed cursor
/// is the subscription's whole delivery state, so a live row racing the
/// activation is either above the anchor — and delivered once, by whichever path
/// reaches the socket first — or at or below it, and dropped as a duplicate.
async fn handle_durable_subscribe(
    ctx: &SessionCtx,
    durable: &mut DurableSessionState,
    spans: &mut WireSpans,
    sub: SubKey,
    resume: Option<Cursor>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let slug = &ctx.runtime.resolved.slug;
    let username = &ctx.username;

    let echoed = match resume {
        None => None,
        Some(cursor) => match parse_resume_cursor(&cursor, slug, username, &sub.channel) {
            Ok(state) => Some(state),
            Err(outcome) => return outcome,
        },
    };

    // The resolved subscription carries the channel uuid and the retain clamp.
    // Boot classified this (instance, channel) Durable, so it must be present.
    let resolved = ctx.runtime.durable_subscription(&sub);
    let clamp = resolved.retain_depth;
    let messenger = ctx.runtime.messenger.as_ref().unwrap_or_else(|| {
        panic!(
            "surface {slug}: durable subscribe on {} but no Messenger — \
             SurfaceRuntime::build should have rejected this at boot",
            sub.channel
        )
    });

    // Activate before the store read: from here the router queues live rows. A row
    // the replay below also serves arrives at the live arm at or below the
    // high-water this subscribe anchors, so the handoff race closes on the cursor.
    durable.activate(&sub);

    // Read the store's boot incarnation (see `WireSpans::incarnation` for the
    // staleness check it feeds). A stale cursor is conforming, so it is answered
    // as a fresh attach, never a violation.
    let incarnation = {
        let conn = messenger.db().lock().await;
        db::read_store_identity(&conn).incarnation
    };
    spans.set_incarnation(incarnation);
    let stale = echoed
        .as_ref()
        .is_some_and(|state| state.incarnation > incarnation);
    if let Some(state) = &echoed
        && stale
    {
        warn!(
            channel = %sub.channel,
            instance = ?sub.instance,
            cursor_incarnation = state.incarnation,
            store_incarnation = incarnation,
            "surface durable resume: cursor minted under a boot this store never counted; \
             answering as fresh attach"
        );
    }
    let store_cursor = match (&echoed, stale) {
        (Some(state), false) => Some(ResumeCursor {
            epoch: state.epoch,
            seq: state.seq,
        }),
        _ => None,
    };

    // The subscription's whole delivery state: the client's own cursor, answered
    // from retention. The subscribe rate is bucketed in `handle_client_frame` and
    // boot proved a durable surface binding's `retain_depth` is bounded, so the
    // replay per Subscribe is config-bounded and cannot be amplified into a DoS.
    let replay = messenger
        .store_for_address(&sub.channel)
        .replay_from(store_cursor, clamp)
        .await;

    // A resumable cursor keeps its position — everything at or below it the client
    // already holds; every other answer anchors at 0 (below every assigned
    // position — a fresh attach).
    let (mut gap, anchor) = match replay.decision {
        Replay::Fresh => (None, 0),
        Replay::UpToDate | Replay::Exact => (
            None,
            store_cursor.map(|cursor| cursor.seq).unwrap_or_default(),
        ),
        Replay::Gap(BusGapReason::EpochChanged) => (
            Some(GapInfo {
                reason: ProtoGapReason::EpochChanged,
            }),
            0,
        ),
        Replay::Gap(BusGapReason::BeyondRetained) => (
            Some(GapInfo {
                reason: ProtoGapReason::BeyondRetained,
            }),
            0,
        ),
        // Reachable on a durable channel by an honest client whose store was
        // restored from backup and re-climbed its high-water past the cursor
        // before this resume — the incarnation check above catches only cursors
        // from boots the restore lost. Escalate loudly, answer as a fresh attach,
        // and never kill: the client conformed. (The ring maps the same decision
        // to a violation, there being no ring restore.)
        Replay::Gap(BusGapReason::ResumeAhead) => {
            warn!(
                channel = %sub.channel,
                instance = ?sub.instance,
                cursor_seq = ?store_cursor.map(|cursor| cursor.seq),
                store_epoch = %replay.epoch,
                "surface durable resume: cursor above the channel high-water; the store may \
                 have been restored from backup"
            );
            (
                Some(GapInfo {
                    reason: ProtoGapReason::EpochChanged,
                }),
                0,
            )
        }
    };
    if stale {
        // A stale-store cursor forces the `EpochChanged` gap over whatever the
        // (fresh-attach) store answer concluded.
        gap = Some(GapInfo {
            reason: ProtoGapReason::EpochChanged,
        });
    }

    // Anchor the durable high-water and reset the span before the
    // SubscribeResult, so the replay rows mint seqs 1..N and the high-water
    // starts at the (non-stale) resume cursor, or 0 on a fresh or stale-store
    // attach.
    spans.start_durable_span(&sub, replay.epoch, anchor);

    // Floor parity: this gates every session-side durable send. Policies are
    // boot-static, so a deny is fail-closed hygiene, not a feature.
    let floor_ok = ctx.runtime.policy.allows_channel_access(&sub.channel);
    let window = replay.messages;
    let replay_count = if floor_ok { window.len() as u32 } else { 0 };

    let result = ServerFrame::SubscribeResult {
        channel: sub.channel.clone(),
        instance: sub.instance.clone(),
        outcome: SubscribeOutcome::Ok,
        replay_count,
        gap,
    };
    if let FrameOutcome::Disconnect = send_frame(&ctx.tx, result, counters).await {
        return FrameOutcome::Disconnect;
    }

    if !floor_ok {
        warn!(
            channel = %sub.channel,
            instance = ?sub.instance,
            "surface durable subscribe: delivery floor denied; sending no replay"
        );
        return FrameOutcome::Continue;
    }

    for retained in window {
        if let FrameOutcome::Disconnect = send_deliver(
            ctx,
            spans,
            &sub,
            (*retained.message).clone(),
            0,
            DeliverKind::Durable {
                retained_seq: retained.seq,
            },
            counters,
        )
        .await
        {
            return FrameOutcome::Disconnect;
        }
    }

    FrameOutcome::Continue
}

/// Drain every active durable channel's unseen suffix — the eager-wake nudge
/// path. Stops and reports `Disconnect` if any send finds the writer gone.
async fn drain_all_durable(
    ctx: &SessionCtx,
    durable: &DurableSessionState,
    spans: &mut WireSpans,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let subs: Vec<SubKey> = durable.active.iter().cloned().collect();
    for sub in &subs {
        if let FrameOutcome::Disconnect = drain_durable_channel(ctx, spans, sub, counters).await {
            return FrameOutcome::Disconnect;
        }
    }
    FrameOutcome::Continue
}

/// Send one durable subscription's unseen suffix in seq order: everything the
/// channel retains above the high-water this connection has written. That
/// high-water is the subscription's whole delivery state, so a drain racing the
/// live fan-out re-reads what the fan-out already advanced past and finds
/// nothing. A span retention no longer holds is reported as `dropped` on the
/// first delivery that follows it. The delivery floor gates the send.
async fn drain_durable_channel(
    ctx: &SessionCtx,
    spans: &mut WireSpans,
    sub: &SubKey,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let channel = sub.channel.as_str();
    let messenger = ctx.runtime.messenger.as_ref().unwrap_or_else(|| {
        panic!(
            "surface {}: durable drain on {channel} but no Messenger — boot invariant violated",
            ctx.runtime.resolved.slug
        )
    });
    // An active durable subscription always has an anchored span and epoch — the
    // Subscribe that activated it anchored both before returning.
    let cursor = ResumeCursor {
        epoch: spans.durable_epoch_of(sub).expect(
            "surface session: durable drain on a subscription with no anchored epoch — \
             activation anchors one",
        ),
        seq: spans.durable_high_water_of(sub).expect(
            "surface session: durable drain on a subscription with no anchored high-water — \
             activation anchors one",
        ),
    };
    let clamp = ctx.runtime.durable_subscription(sub).retain_depth;
    let replay = messenger
        .store_for_address(channel)
        .replay_from(Some(cursor), clamp)
        .await;
    // A `Gap` answer means retention no longer covers the whole span above the
    // high-water — evicted, or longer than the subscription's clamp — and its
    // window is the channel's newest rows rather than a suffix of the cursor.
    // Sending it whole would re-send positions already written and, worse, move
    // the high-water past the interior span with no signal, which no later
    // Subscribe could then report. So the window is cut to the suffix and the
    // seqs between the high-water and its oldest entry ride the first delivery's
    // `dropped`, which is the wire's own field for exactly this loss.
    let (window, lost) = match replay.decision {
        Replay::Gap(reason) => {
            let suffix: Vec<_> = replay
                .messages
                .into_iter()
                .filter(|retained| retained.seq > cursor.seq)
                .collect();
            let lost = suffix
                .first()
                .map_or(0, |retained| retained.seq - cursor.seq - 1);
            warn!(
                %channel,
                instance = ?sub.instance,
                ?reason,
                high_water = cursor.seq,
                dropped = lost,
                serving = suffix.len(),
                "surface durable drain: retention no longer covers the span above the \
                 high-water; the subscription loses the interior span"
            );
            (suffix, lost)
        }
        _ => (replay.messages, 0),
    };
    if window.is_empty() {
        return FrameOutcome::Continue;
    }
    if !ctx.runtime.policy.allows_channel_access(channel) {
        warn!(%channel, "surface durable drain: delivery floor denied; sending nothing");
        return FrameOutcome::Continue;
    }
    // The loss belongs to the subscription, not to a message: it rides the first
    // delivery that follows it and is not repeated on the rest.
    let mut dropped = lost;
    for retained in window {
        if let FrameOutcome::Disconnect = send_deliver(
            ctx,
            spans,
            sub,
            (*retained.message).clone(),
            std::mem::take(&mut dropped),
            DeliverKind::Durable {
                retained_seq: retained.seq,
            },
            counters,
        )
        .await
        {
            return FrameOutcome::Disconnect;
        }
    }
    FrameOutcome::Continue
}

/// Handle a `Publish` frame.
///
/// Resolves `(instance, port)` against the surface's config-bound outputs, then
/// answers on the wire. Validation happens before the rate bucket (a publish
/// that cannot succeed consumes no token): an unbound port is a violation; an
/// oversized body answers `BodyTooLarge` (a correct shell can produce it, so it
/// is metered — warned once — up to a per-connection threshold, then policed:
/// the Nth reject on one connection escalates to a violation and kills).
/// Otherwise the connection bucket
/// gates the publish; on grant the message routes by delivery class — an
/// `Ephemeral` output onto its channel's ring store, a `Durable` (`brenn:`) output
/// through `Messenger::publish_from_surface`. Both classes flow through the same
/// body-cap and connection-bucket gates; class no longer precedes them.
///
/// The frame's fields travel as one [`PublishRequest`] rather than as a
/// positional run of same-typed `&str`s a caller could transpose — the same
/// reason [`SessionCtx`] exists.
async fn handle_publish(
    ctx: &SessionCtx,
    publish_bucket: &mut TokenBucket,
    req: PublishRequest<'_>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let PublishRequest {
        instance,
        port,
        body,
        correlation,
        subject_instance,
        urgency,
    } = req;
    let runtime = &ctx.runtime;
    let tx = &ctx.tx;
    let slug = &ctx.runtime.resolved.slug;
    let username = &ctx.username;
    // 1. (instance, port) must be a config-bound output. Unknown and unbound
    //    are indistinguishable on the wire (no existence oracle): both violate.
    //    Linear scan over the handful of bound outputs — allocation-free, unlike
    //    building an owned tuple key to probe the map on every frame.
    let Some(out) = runtime
        .output_ports
        .iter()
        .find(|(key, _)| key.0.as_str() == instance && key.1.as_str() == port)
        .map(|(_, value)| value)
    else {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: Publish to unbound port {}/{}",
            sanitize_client_detail(instance),
            sanitize_client_detail(port),
        ));
    };

    // 1b. Resolve the publishing principal's component grain. The principal is
    //     the instance, so the grain is the `instance` the frame named, admitted
    //     against the server's boot-resolved declaration set — the client
    //     supplies a value the operator must have written, never an identity it
    //     spells. An ordinary publish is attributed to the instance it came from;
    //     a report on the reserved `#brenn` port is attributed to its
    //     `subject_instance`, because `#brenn` is by construction outside the
    //     declared set and names no component.
    //
    //     Both checks treat an unknown instance as a violation, not a fallback
    //     identity: silently demoting to the bare surface identity would let a
    //     non-conforming client launder a component's publishes onto the
    //     surface's own budget, which is exactly the blast-radius scoping this
    //     grain exists to enforce.
    let is_error_report = brenn_surface_contract::is_error_report_port(instance, port);
    if subject_instance.is_some() && !is_error_report {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: Publish to {}/{} carries subject_instance, which \
             only the reserved error-report port may name",
            sanitize_client_detail(instance),
            sanitize_client_detail(port),
        ));
    }
    let component = if is_error_report {
        // `None` is legitimate here and only here: a kernel self-report has no
        // component subject and carries the bare surface identity.
        match subject_instance {
            Some(subject) if runtime.is_declared_instance(subject) => Some(subject),
            Some(subject) => {
                return FrameOutcome::Violation(format!(
                    "surface {slug} user {username}: error report names undeclared \
                     subject_instance {}",
                    sanitize_client_detail(subject),
                ));
            }
            None => None,
        }
    } else {
        // A bound output port implies a declared instance (boot resolves bindings
        // against the declaration set), so a miss is a broken boot invariant
        // rather than client input — the unbound-port arm above already killed
        // every client-reachable path to an undeclared instance.
        assert!(
            runtime.is_declared_instance(instance),
            "surface {slug}: bound output port {instance}/{port} names an instance absent from \
             the resolved component set — boot validation resolves every binding against that \
             set, so this is a broken boot invariant"
        );
        Some(instance)
    };

    // 2. Body size, before the bucket. Reachable by a correct client, so an
    //    outcome — but metered (first-occurrence warn) up to a threshold, then
    //    policed: no rate token is spent here, so without an escalation an
    //    authenticated client could sustain an unthrottled parse-and-respond
    //    flood in the (body-cap, frame-cap] window. After
    //    BODY_TOO_LARGE_VIOLATION_THRESHOLD transport rejects on one connection
    //    it becomes a violation (kill), mirroring the subscribe-rate breach. No
    //    token is consumed on this path; the escalation is a counter threshold,
    //    not a token spend, so the "doomed publish spends no token" rule holds.
    if body.len() > runtime.max_body_bytes {
        // First occurrence gates the warn, keyed off the transport counter (no
        // parallel flag to drift): only this arm bumps it, so the warn fires
        // once per session.
        if counters.publish_body_too_large == 0 {
            warn!(
                len = body.len(),
                max = runtime.max_body_bytes,
                "surface Publish body exceeds max_body_bytes; rejecting"
            );
        }
        counters.publish_body_too_large += 1;
        if counters.publish_body_too_large >= BODY_TOO_LARGE_VIOLATION_THRESHOLD {
            return FrameOutcome::Violation(format!(
                "surface {slug} user {username}: persistent oversized Publish bodies ({} rejects \
                 this connection)",
                counters.publish_body_too_large
            ));
        }
        let frame = ServerFrame::PublishResult {
            correlation,
            outcome: PublishOutcome::BodyTooLarge {
                len: body.len() as u64,
                max: runtime.max_body_bytes as u64,
            },
        };
        return send_frame(tx, frame, counters).await;
    }

    // 3. Connection rate bucket: the first gate, trips before the bus-level
    //    per-sender gate. Denied is not a kill — a legitimate component retry
    //    loop can reach it. Attribution (surface/user/ip) rides the session span.
    match publish_bucket.try_consume() {
        TokenBucketOutcome::Granted => {}
        TokenBucketOutcome::GrantedAfterSuppression { suppressed } => {
            warn!(
                suppressed,
                "surface Publish rate limit lifted, publishes were suppressed"
            );
        }
        TokenBucketOutcome::Denied { first } => {
            counters.publish_rate_limited(component);
            if first {
                warn!("rate-limiting surface Publish from this connection");
            }
            let frame = ServerFrame::PublishResult {
                correlation,
                outcome: PublishOutcome::RateLimited,
            };
            return send_frame(tx, frame, counters).await;
        }
    }

    // 4. Publish, routed by delivery class. The sender identity + policy are the
    //    boot-resolved surface principal, per the bus/messenger caller invariant.
    //
    //    Urgency is the component's stated intent, else the port's boot-resolved
    //    default — the same override-else-configured-default rule a backend guest
    //    gets from `publish-with-urgency`, so a component's publish semantics do
    //    not change with its hosting. The default is read from the server's own
    //    output map, never from the frame: the frame says only what the component
    //    chose, and a client that stayed quiet gets the operator's value even if
    //    its `Welcome` snapshot has gone stale under a reconnect.
    //
    //    Unlike the instance fields this needs no validation past the enum:
    //    urgency is sender intent on a port the sender is already bound to
    //    publish on, and what bounds the traffic is the send budget, not the
    //    rung it asks for.
    let urgency = urgency.unwrap_or(out.default_urgency);
    let OutputPort { address, class, .. } = out;
    // `local:` traffic never crosses the wire: the page-local router is its sole
    // source of truth, so a `local:` publish must never reach the server. Not
    // attacker-reachable — `class` comes from the boot-resolved output map, which
    // excludes `local:` bindings by construction (`SurfaceRuntime::build`), so a
    // client naming a local output port was already killed by the unbound-port
    // violation above. Die rather than route page-local traffic onto the bus.
    assert!(
        !matches!(class, DeliveryClass::Local),
        "surface {slug}: output {address} classified Local — local: channels never reach the \
         server (the output map excludes them); this is a broken boot invariant"
    );
    // One publish path for every class the wire can carry: the channel's
    // capabilities decide where the message lands, inside the pipeline. A bound
    // output implies a directory channel implies messaging configured implies
    // `Some(messenger)`, so `None` here is a broken boot invariant, not
    // attacker-reachable. (`SurfaceRuntime::build` asserts the same
    // Messenger-present invariant for subscriptions; the output direction is
    // enforced fail-fast here at first use — see its comment.)
    let messenger = runtime.messenger.as_ref().unwrap_or_else(|| {
        panic!(
            "surface {slug}: output {address} bound but runtime has no Messenger — a bound \
             output implies a directory channel implies messaging configured implies \
             Some(messenger)"
        )
    });
    // The reserved error-report port rides the ordinary publish path but has two
    // distinct postures below: on success an audit emit restoring the
    // user/session correlation the report body omits, and on the
    // broken-boot-invariant outcomes an `error!` carrying the report body instead
    // of the bound-output panic — killing the server over its own diagnostics
    // channel, on an attacker-sendable frame path, inverts priorities.
    // A report about a component is published under that component's sub-identity
    // (resolved at step 1b): attribution lands on the component the report is
    // about, and a crash-looping component's report flood draws down its own
    // budget rather than its neighbours'. A kernel self-report carries the bare
    // surface identity.
    let outcome = match messenger
        .publish_from_surface(slug, component, address, body, urgency)
        .await
    {
        PublishResult::Ok { .. } => {
            counters.publish_ok(component);
            if is_error_report {
                // The auth layer attests user/session; the report body does not
                // carry them (server-attested facts do not belong in a
                // surface-attributed body). Keyed by the session span +
                // publish_ts, this restores the correlation.
                info!(
                    target: "surface_report",
                    surface = %slug,
                    session_id = %ctx.session_id,
                    user = %username,
                    "surface error report published"
                );
            }
            PublishOutcome::Ok
        }
        // Reaching this means the transport pre-check (step 2) and the pipeline
        // disagree on body size — a config-wiring bug, since both derive from
        // config.messaging.max_body_bytes. Not panicked (body is client-controlled
        // input), but it must scream: a bare counter bump would fold silently into
        // the routine transport-rejection count.
        PublishResult::BodyTooLarge { len, max } => {
            error!(
                len,
                max,
                transport_max = runtime.max_body_bytes,
                "surface Publish: transport and messenger body-size caps disagree"
            );
            counters.publish_body_cap_disagreement += 1;
            PublishOutcome::BodyTooLarge {
                len: len as u64,
                max: max as u64,
            }
        }
        // The output binding, its publish ACL coverage, and the channel's
        // existence are all boot-validated and boot-static, so a denial here is a
        // broken boot invariant — not attacker-reachable (the only client
        // influence, an unbound port, was killed above). `UnsupportedOption` joins
        // them: this call passes no option fields at all, so producing one would
        // mean the pipeline invented one. For an ordinary bound output that means
        // panic (a user-visible publish silently failing is doing the wrong
        // thing). On the reserved error-report port the same outcomes instead
        // `error!` the full report body and return `Failed`: this branch handles
        // attacker-adjacent input on a diagnostics channel, so it fails
        // loud-and-closed rather than trusting the invariant with the process's
        // life. The report is preserved in the `error!` line (and the shell
        // console-logged it before publishing).
        // `DeferredQuotaExceeded` joins them for the same reason as
        // `UnsupportedOption`: it can only arise from a future `deliver_after`,
        // an option field this call never passes, so producing one means the
        // pipeline invented it. Surface deferral rides the batch path — a
        // component's deferred publish is buffered, and buffered publishes flush
        // as batches — where a full deferred set is per-entry normal operation
        // rather than an outcome this ladder could carry.
        other @ (PublishResult::MissingSender
        | PublishResult::AclDenied(_)
        | PublishResult::UnknownChannel(_)
        | PublishResult::MalformedAddress(_)
        | PublishResult::UnsupportedOption { .. }
        | PublishResult::DeferredQuotaExceeded { .. }) => {
            if is_error_report {
                error!(
                    surface = %slug,
                    session_id = %ctx.session_id,
                    user = %username,
                    channel = %address,
                    outcome = ?other,
                    // Client-composed content: render via `Debug` so embedded
                    // newlines / ANSI escapes are escaped rather than forging or
                    // mangling lines in the operator's primary diagnostic stream.
                    body = ?body,
                    "surface error report publish failed on the reserved port; report preserved \
                     in this log line only"
                );
                PublishOutcome::Failed
            } else {
                panic!(
                    "surface {slug}: publish_from_surface rejected bound output {address}: \
                     {other:?} — boot validation guarantees every bound output exists and is \
                     policy-covered"
                )
            }
        }
        // The per-surface send budget (durable targets) and the per-(sender,
        // channel) send-rate gate (every target) can both deny. Client-facing
        // meaning is identical — slow down — so both map to the RateLimited wire
        // outcome and counter; each gate already emitted its own first-denial warn.
        PublishResult::BudgetExhausted | PublishResult::RateLimited => {
            counters.publish_rate_limited(component);
            PublishOutcome::RateLimited
        }
    };
    let frame = ServerFrame::PublishResult {
        correlation,
        outcome,
    };
    send_frame(tx, frame, counters).await
}

/// Handle a `PublishBatch` frame — one activation's flush, applied whole or not
/// at all.
///
/// The discipline differs from `handle_publish` on purpose. A single `Publish` is
/// the v0 path, where the page has no kernel-side gate contract, so a body the
/// server rejects is an ordinary outcome a correct-but-buggy component can
/// produce. A batch is different: every entry in it already passed the kernel's
/// own buffer-time gates — bound port, body cap, sink budget — which answered the
/// component the `processor.wit` error triple inline and never buffered a publish
/// that failed one. So an entry arriving broken here says the kernel did not run,
/// which means the client is not the kernel. That is fail2ban signal, not a soft
/// outcome, and every per-entry check below is therefore violation-grade.
///
/// Order:
/// 1. `instance` names a declared component — the sub-identity is *derived* from
///    it, never claimed. Unknown → violation, exactly the single-publish rule.
/// 2. Batch shape: non-empty (in *either* list — a flush that only cancels a
///    schedule carries no publishes), each list within the per-activation cap the
///    kernel buffers against, and the publishes within the per-activation *byte*
///    cap it buffers against. All of them bound the work steps 3–5 do before any
///    budget is consulted.
/// 3. Per entry: a bound output port of *this* instance, a body within the cap,
///    and a representable `deliver_after`. `local:` targets fall out of the port
///    check for free — the boot-resolved output map excludes them by
///    construction, so a page-local address is an unbound port here and dies as
///    one. The release time is judged against one clock read for the whole flush,
///    so a time already past becomes an immediate publish for every entry
///    identically. Control ops resolve the same way: bound port, an edit body
///    within the cap, a representable release time.
/// 4. The instance's send budget, drawn once for the whole batch as N tokens (see
///    [`Messenger::draw_surface_send_budget_for_batch`]). Denial is
///    `RateLimited` — never a violation, never a kill: it is the honest answer
///    when the two budget tiers disagree, and the kernel's tier is the binding
///    one for any non-malicious page. Drawn before anything is applied, because a
///    refused batch is re-parked and retried whole by the kernel: an op applied
///    ahead of the draw would apply again on the retry.
/// 5. Apply the control ops, ahead of the publishes: an op names a message an
///    earlier activation parked, so applying it first keeps this activation's own
///    publishes out of its way. An applied op re-states the sender's view and kicks the
///    dispatcher (an edit can move a release earlier than the sweep's current
///    sleep target). A `NotDeferred` is the benign drain-vs-release race: logged
///    and counted, never a kill. A `WrongSender` is a violation — a conforming
///    kernel can only name what a sender-scoped view showed it, so naming another
///    sender's parked message is a client reaching past every window it was ever
///    offered.
/// 6. Apply: durable entries in one transaction, ephemeral entries fanned out, both
///    in call order. Cross-class relative order is not guaranteed — one class
///    commits in the server's DB and the other in its bus. A deferred entry parks
///    at its class's retention authority instead of committing, and a full
///    deferred set drops that entry's schedule — a warn and a counter, never a
///    wire error and never the batch: the component already returned, so there is
///    nothing left to answer, and the entries it published unconditionally must
///    not be lost to one it merely scheduled.
///
/// The per-connection publish bucket does not gate this frame: it meters whole
/// publishes and a batch is one frame carrying up to
/// `MAX_PUBLISHES_PER_ACTIVATION` of them, so drawing one token would under-count
/// it and drawing N would starve any batch wider than the burst. The pipe is
/// bounded here by the WS frame cap; the *principal* is bounded by step 4, which
/// is the bound this path needs.
async fn handle_publish_batch(
    ctx: &SessionCtx,
    instance: &str,
    correlation: u64,
    publishes: &[BatchEntry],
    deferred_ops: &[BatchDeferredOp],
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let runtime = &ctx.runtime;
    let slug = &runtime.resolved.slug;
    let username = &ctx.username;

    // 1. Derive the principal. An undeclared instance is a violation rather than a
    //    demotion to the bare surface identity: demoting would let a
    //    non-conforming client launder a flush onto the surface's own budget,
    //    which is the blast-radius scoping this grain exists to enforce. The
    //    reserved error-report port dies here too — its instance is outside the
    //    declared set by construction, and a batch is an activation's flush, not
    //    the kernel's breadcrumb path.
    if !runtime.is_declared_instance(instance) {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: PublishBatch from undeclared instance {}",
            sanitize_client_detail(instance),
        ));
    }

    // 2. Batch shape. A conforming kernel never flushes an empty buffer (it sends
    //    no frame at all) and never buffers past the cap (it answers the component
    //    `quota-exceeded` at the cap instead), so both are non-kernel signal.
    if publishes.is_empty() && deferred_ops.is_empty() {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: empty PublishBatch from instance {}",
            sanitize_client_detail(instance),
        ));
    }
    if publishes.len() > MAX_PUBLISHES_PER_ACTIVATION {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: PublishBatch from instance {} carries {} entries, \
             over the {MAX_PUBLISHES_PER_ACTIVATION} per-activation cap",
            sanitize_client_detail(instance),
            publishes.len(),
        ));
    }
    // The ops have their own ceiling on the kernel side, the same number, counted
    // separately — so it is mirrored the same way and for the same reason.
    if deferred_ops.len() > MAX_PUBLISHES_PER_ACTIVATION {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: PublishBatch from instance {} carries {} control \
             ops, over the {MAX_PUBLISHES_PER_ACTIVATION} per-activation cap",
            sanitize_client_detail(instance),
            deferred_ops.len(),
        ));
    }
    // The kernel's third buffer-time gate, mirrored for the same reason as the
    // other two: it refuses the publish that would cross this total at buffer
    // time, so a batch over it is a batch no kernel produced. Without this arm the
    // entry-count cap alone lets a hostile client hand the server 256 max-size
    // bodies — durable rows and their push fan-out — in one frame, on a path whose
    // whole doctrine is that kernel-impossible input is fail2ban signal.
    let total_bytes: usize = publishes.iter().map(|e| e.body.len()).sum();
    if total_bytes > MAX_PUBLISH_BYTES_PER_ACTIVATION {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: PublishBatch from instance {} carries {total_bytes} \
             body bytes, over the {MAX_PUBLISH_BYTES_PER_ACTIVATION}-byte per-activation cap",
            sanitize_client_detail(instance),
        ));
    }

    // 3. Resolve every entry before applying any of them: the batch is atomic, so
    //    a check that runs per entry as it applies could kill the connection with
    //    a prefix already committed.
    //
    //    One clock read serves the whole batch's park-vs-immediate decisions, so
    //    every entry — both substrates — is judged against the same instant, and
    //    a release time already past becomes an ordinary immediate publish here
    //    rather than at each substrate's own reading of "now".
    let flush_now = Utc::now();
    let mut resolved: Vec<ResolvedBatchEntry<'_>> = Vec::with_capacity(publishes.len());
    for entry in publishes {
        let Some(out) = runtime
            .output_ports
            .iter()
            .find(|(key, _)| key.0.as_str() == instance && key.1.as_str() == entry.port)
            .map(|(_, value)| value)
        else {
            return FrameOutcome::Violation(format!(
                "surface {slug} user {username}: PublishBatch entry names unbound port {}/{}",
                sanitize_client_detail(instance),
                sanitize_client_detail(&entry.port),
            ));
        };
        if entry.body.len() > runtime.max_body_bytes {
            return FrameOutcome::Violation(format!(
                "surface {slug} user {username}: PublishBatch entry on port {}/{} carries a {}-byte \
                 body, over the {}-byte cap the kernel enforces at buffer time",
                sanitize_client_detail(instance),
                sanitize_client_detail(&entry.port),
                entry.body.len(),
                runtime.max_body_bytes,
            ));
        }
        // A release time chrono cannot carry is violation-grade like every other
        // per-entry check here: the kernel refuses one at buffer time with
        // `invalid-payload`, so it never reaches the wire from a kernel. Left
        // unchecked it would collapse into an immediate publish, silently turning
        // a component's schedule into a now.
        let deliver_after = match entry.deliver_after {
            None => None,
            Some(ms) => {
                let Some(at) = utc_from_epoch_ms(ms) else {
                    return FrameOutcome::Violation(format!(
                        "surface {slug} user {username}: PublishBatch entry on port {}/{} carries \
                         an unrepresentable deliver_after of {ms} ms, which the kernel refuses at \
                         buffer time",
                        sanitize_client_detail(instance),
                        sanitize_client_detail(&entry.port),
                    ));
                };
                Some(at).filter(|at| *at > flush_now)
            }
        };
        // Sender intent, else the port's boot-resolved default — read from the
        // server's own output map, never echoed from the frame, so a client whose
        // `Welcome` snapshot went stale still gets the operator's value.
        resolved.push(ResolvedBatchEntry {
            out,
            body: entry.body.as_str(),
            urgency: entry.urgency.unwrap_or(out.default_urgency),
            deliver_after,
        });
    }
    let resolved_ops = match resolve_batch_deferred_ops(ctx, instance, deferred_ops) {
        Ok(ops) => ops,
        Err(violation) => return violation,
    };

    // 4. The instance's send budget, one all-or-nothing draw. A `brenn:` output
    //    implies a Messenger (the boot invariant `handle_publish` documents), and
    //    the budget map is keyed by principal for every declared instance
    //    regardless of class, so an ephemeral-only batch draws it too — the budget
    //    meters the principal's WS-ingress traffic, not one delivery class.
    let messenger = runtime.messenger.as_ref().unwrap_or_else(|| {
        panic!(
            "surface {slug}: PublishBatch from declared instance {instance} but the runtime has \
             no Messenger — boot installs a send budget per declared instance on the Messenger, \
             so there is no budget to draw without one"
        )
    });
    // One token per publish, and one for a batch that publishes nothing: an
    // ops-only flush is still a frame the principal sent and work the server did,
    // and a path that draws zero is a path a client can ride for free.
    //
    // The control ops themselves are unpriced, which is deliberate for now and not
    // symmetric: each applied durable op is its own DB write, so a flush carrying
    // the op cap costs far more server work than the one token it draws.
    // TODO(surface-op-send-budget): price control ops in the send budget.
    let draw =
        u32::try_from(resolved.len().max(1)).expect("batch length is capped well below u32::MAX");
    if messenger.draw_surface_send_budget_for_batch(slug, instance, draw)
        == SurfaceSendVerdict::Denied
    {
        // Not a kill and not a retry prompt: the kernel logs, counts, and drops
        // the batch. Its activation's guarantee was "flushed, not discarded" *by
        // the kernel*, and it was flushed.
        for _ in 0..resolved.len() {
            counters.publish_rate_limited(Some(instance));
        }
        let frame = ServerFrame::PublishBatchResult {
            correlation,
            outcome: PublishBatchOutcome::RateLimited,
        };
        return send_frame(&ctx.tx, frame, counters).await;
    }

    // 5. The control ops, ahead of the publishes. A violation here can leave
    //    earlier ops of the same batch applied: whether a message is someone
    //    else's is only knowable by asking the store, and each ask is its own
    //    round trip. The ops that landed were legitimate ones; the connection dies
    //    on the one that was not.
    //
    //    The channels every op named are collected rather than restated here, so
    //    the one view-emission pass at the end of the batch covers ops and
    //    schedules together — a channel this flush both edited and parked on is
    //    restated once, from the truth after both. An op that lost the race to a
    //    release is collected too: it changed nothing, but it is evidence that
    //    some page named a schedule the backend does not hold.
    let mut op_channels: Vec<&str> = Vec::with_capacity(resolved_ops.len());
    let mut applied_any = false;
    for op in &resolved_ops {
        match apply_batch_deferred_op(ctx, messenger, instance, op, flush_now).await {
            Ok(effect) => {
                applied_any |= matches!(effect, OpEffect::Applied);
                op_channels.push(op.out.address.as_str());
            }
            Err(violation) => {
                // This connection dies, so the end-of-batch emission pass never
                // runs — but the ops that landed before the violating one changed
                // a set every session of the surface mirrors. Restate those here
                // or a sibling tab keeps a schedule that no longer exists: if the
                // op emptied the set, no release and no later change will ever
                // push a correcting view.
                if applied_any {
                    messenger.dispatch_kick();
                }
                emit_op_views(ctx, messenger, instance, &op_channels, flush_now).await;
                return violation;
            }
        }
    }
    // The release sweep sleeps to the earliest deadline it last computed, so an
    // edit that moved a release earlier has to wake it or the message waits out the
    // poll interval. A lost race moved no deadline, so it does not kick.
    if applied_any {
        messenger.dispatch_kick();
    }

    // 6. Stamp every wire entry, in call order, in one pass across the whole
    //    batch — before the substrate split, so call order is visible *across*
    //    the class boundary and not merely within each half. Each entry takes
    //    max(prev + 1, now), so the stamps are strictly increasing whatever the
    //    clock does. The delivered envelope's `publish_ts` carries this at ns
    //    precision; it is the ordering contract's only observable.
    let mut prev_ts: Option<i64> = None;
    let stamps: Vec<i64> = resolved
        .iter()
        .map(|_| {
            let now_ns = brenn_lib::messaging::db::utc_to_ns(Utc::now());
            let ts = match prev_ts {
                None => now_ns,
                Some(prev) => std::cmp::max(prev + 1, now_ns),
            };
            prev_ts = Some(ts);
            ts
        })
        .collect();

    // 7. Apply. Durable first as one transaction, then the ephemeral fan-out; each
    //    class in call order, with no order promised between them — the guarantee
    //    is the position assignment above plus per-session Deliver sequencing,
    //    never a shared commit instant.
    let durable: Vec<SurfaceBatchPublish<'_>> = resolved
        .iter()
        .zip(&stamps)
        .filter(|(entry, _)| matches!(entry.out.class, DeliveryClass::Durable))
        .map(|(entry, ts)| SurfaceBatchPublish {
            channel_address: entry.out.address.as_str(),
            body: entry.body,
            urgency: entry.urgency,
            publish_ts_ns: *ts,
            deliver_after: entry.deliver_after,
        })
        .collect();
    let durable_count = durable.len();
    // Entries whose schedule the cap refused published nothing, so they reduce
    // the publish count. No wire error: the guest has no error channel left.
    let schedules_dropped = messenger
        .publish_batch_from_surface(slug, instance, &durable)
        .await;
    for _ in 0..(durable_count - schedules_dropped) {
        counters.publish_ok(Some(instance));
    }

    let mut parked_ephemeral = false;
    for (entry, ts) in resolved
        .iter()
        .zip(&stamps)
        .filter(|(entry, _)| !matches!(entry.out.class, DeliveryClass::Durable))
    {
        parked_ephemeral |= publish_batch_ephemeral(
            ctx,
            messenger,
            instance,
            EphemeralBatchPublish {
                out: entry.out,
                body: entry.body,
                urgency: entry.urgency,
                publish_ts_ns: *ts,
                deliver_after: entry.deliver_after,
            },
            counters,
        );
    }
    // The release sweep keeps the earliest deadline it last computed and sleeps
    // to it, so a park that lands afterwards must wake it or the schedule waits
    // out the poll interval. The durable half kicks from inside
    // `publish_batch_from_surface`, which runs before this loop — too early to
    // cover a park made here, and skipped entirely for a batch with no durable
    // entries.
    if parked_ephemeral {
        messenger.dispatch_kick();
    }

    // Re-state the sender's parked view on every channel this batch scheduled
    // against or aimed a control op at, whichever half carried it and whether or
    // not the park or the op was admitted: the view is recomputed from the store,
    // so a refused schedule and a lost race both land on the truth. Judged at the
    // flush's one clock read, the same instant that decided park-vs-immediate.
    let mut scheduled: Vec<&str> = resolved
        .iter()
        .filter(|entry| entry.deliver_after.is_some())
        .map(|entry| entry.out.address.as_str())
        .chain(op_channels)
        .collect();
    scheduled.sort_unstable();
    scheduled.dedup();
    for channel in scheduled {
        broadcast_deferred_view(messenger, &ctx.registry, slug, instance, channel, flush_now).await;
    }

    let frame = ServerFrame::PublishBatchResult {
        correlation,
        outcome: PublishBatchOutcome::Ok,
    };
    send_frame(&ctx.tx, frame, counters).await
}

/// One control op of a `PublishBatch`, resolved against the server's own
/// boot-resolved output map before any of the batch is applied.
///
/// Resolution is complete for the same reason [`ResolvedBatchEntry`]'s is: the
/// batch is atomic, so every check that can kill the connection runs before
/// anything is applied.
struct ResolvedDeferredOp<'a> {
    out: &'a OutputPort,
    /// The port name, for the log line a benign race writes — the address alone
    /// does not say which of an instance's ports named it.
    port: &'a str,
    message_id: Uuid,
    /// `None` for a cancel; the edit's two halves otherwise, `release_at` already
    /// converted from the wire's epoch milliseconds.
    edit: Option<(Option<String>, Option<DateTime<Utc>>)>,
}

/// Resolve every control op of a `PublishBatch`, or the violation that kills the
/// connection.
///
/// Each check mirrors one the kernel already made at buffer time, so a failure
/// here says the client is not the kernel — the same doctrine the publish entries
/// are held to.
fn resolve_batch_deferred_ops<'a>(
    ctx: &'a SessionCtx,
    instance: &'a str,
    ops: &'a [BatchDeferredOp],
) -> Result<Vec<ResolvedDeferredOp<'a>>, FrameOutcome> {
    let slug = &ctx.runtime.resolved.slug;
    let username = &ctx.username;
    let mut resolved = Vec::with_capacity(ops.len());
    for op in ops {
        let Some(out) = ctx
            .runtime
            .output_ports
            .iter()
            .find(|(key, _)| key.0.as_str() == instance && key.1.as_str() == op.port)
            .map(|(_, value)| value)
        else {
            return Err(FrameOutcome::Violation(format!(
                "surface {slug} user {username}: PublishBatch control op names unbound port {}/{}",
                sanitize_client_detail(instance),
                sanitize_client_detail(&op.port),
            )));
        };
        let edit = match &op.op {
            DeferredOpKind::Cancel => None,
            DeferredOpKind::Edit {
                body,
                deliver_after,
            } => {
                if let Some(body) = body
                    && body.len() > ctx.runtime.max_body_bytes
                {
                    return Err(FrameOutcome::Violation(format!(
                        "surface {slug} user {username}: PublishBatch control op on port {}/{} \
                         carries a {}-byte edit body, over the {}-byte cap the kernel enforces at \
                         buffer time",
                        sanitize_client_detail(instance),
                        sanitize_client_detail(&op.port),
                        body.len(),
                        ctx.runtime.max_body_bytes,
                    )));
                }
                let release_at = match deliver_after {
                    None => None,
                    Some(ms) => {
                        let Some(at) = utc_from_epoch_ms(*ms) else {
                            return Err(FrameOutcome::Violation(format!(
                                "surface {slug} user {username}: PublishBatch control op on port \
                                 {}/{} carries an unrepresentable deliver_after of {ms} ms, which \
                                 the kernel refuses at buffer time",
                                sanitize_client_detail(instance),
                                sanitize_client_detail(&op.port),
                            )));
                        };
                        Some(at)
                    }
                };
                Some((body.clone(), release_at))
            }
        };
        resolved.push(ResolvedDeferredOp {
            out,
            port: op.port.as_str(),
            message_id: op.message_id,
            edit,
        });
    }
    Ok(resolved)
}

/// What one control op left behind.
enum OpEffect {
    /// The sender's parked set changed: the sweep needs waking and the view needs
    /// restating.
    Applied,
    /// The message released between the snapshot the component read and this
    /// frame. Nothing changed, but the view is restated anyway — see
    /// [`apply_batch_deferred_op`].
    Raced,
}

/// Apply one resolved control op under the batch's sub-identity. The caller
/// restates the view for the channel on either outcome.
///
/// The three outcomes:
///
/// - **Applied** — the sender's parked set changed, so its view owes a restatement.
/// - **`NotDeferred`** — the message released between the snapshot the component
///   read and this frame. Logged and counted, never punished: a conforming
///   component can always lose that race, and the page has no way to have known.
///   The view is restated regardless, because a mirror is the only place an op's
///   id can come from: an op naming something the backend does not hold is the one
///   event a *wrong* mirror reliably provokes — one whose emission was dropped on
///   a full push queue, say — and it would otherwise be the single set-touching
///   event that pushes nothing, leaving the phantom entry to be cancelled over and
///   over. A recompute is idempotent, so restating on a genuine race costs one
///   redundant snapshot.
/// - **`WrongSender`** — a violation. The ids a conforming kernel can name come
///   from a sender-scoped view, so this is a client naming a schedule no window
///   ever offered it. Reported rather than panicked precisely because it is
///   client-reachable: a panic here would be a remote kill switch.
async fn apply_batch_deferred_op(
    ctx: &SessionCtx,
    messenger: &Messenger,
    instance: &str,
    op: &ResolvedDeferredOp<'_>,
    now: DateTime<Utc>,
) -> Result<OpEffect, FrameOutcome> {
    let slug = &ctx.runtime.resolved.slug;
    let sender = ParticipantId::for_surface_component(slug, instance);
    let channel = op.out.address.as_str();
    let outcome = match &op.edit {
        None => {
            messenger
                .cancel_deferred_for_sender(channel, sender.as_str(), op.message_id, now)
                .await
        }
        Some((body, release_at)) => {
            messenger
                .edit_deferred_for_sender(
                    channel,
                    sender.as_str(),
                    op.message_id,
                    body.clone(),
                    *release_at,
                    now,
                )
                .await
        }
    };
    match outcome {
        DeferralOutcome::Applied => Ok(OpEffect::Applied),
        DeferralOutcome::NotDeferred => {
            messenger.record_deferred_control_race(sender.as_str(), channel);
            info!(
                surface = slug,
                instance,
                port = op.port,
                channel,
                "surface deferred control op is a no-op — the message released between the \
                 activation's snapshot and the flush"
            );
            Ok(OpEffect::Raced)
        }
        DeferralOutcome::WrongSender => Err(FrameOutcome::Violation(format!(
            "surface {slug} user {}: PublishBatch control op from instance {} names message {} on \
             {channel}, parked by another sender",
            ctx.username,
            sanitize_client_detail(instance),
            op.message_id,
        ))),
    }
}

/// Restate the sender's parked view on every channel the batch's control ops
/// named.
///
/// The violation exit from the op pass, where the connection dies before the
/// end-of-batch emission runs. The normal exit folds these channels into that one
/// pass instead, so a channel a flush both edited and parked on is restated once,
/// from the truth after both.
async fn emit_op_views(
    ctx: &SessionCtx,
    messenger: &Messenger,
    instance: &str,
    op_channels: &[&str],
    now: DateTime<Utc>,
) {
    if op_channels.is_empty() {
        return;
    }
    let mut channels = op_channels.to_vec();
    channels.sort_unstable();
    channels.dedup();
    for channel in channels {
        broadcast_deferred_view(
            messenger,
            &ctx.registry,
            &ctx.runtime.resolved.slug,
            instance,
            channel,
            now,
        )
        .await;
    }
}

/// Every set a deferred view can be seeded for: each declared instance crossed
/// with the transportable channels its bound output ports publish onto, deduped
/// (two ports may share a channel) and sorted so the seeding order is the same on
/// every attach.
///
/// The boot-resolved output map holds no `local:` address — a page-local channel
/// is the page's own retention authority and the backend parks nothing on it —
/// so every address here is transportable by construction.
///
/// Every address here also has a store: boot refuses to start a surface whose
/// transportable output names an undeclared channel
/// (`bootstrap::messaging::surfaces`), and the directory carries every declared
/// channel. An unresolvable address is a state boot cannot produce; the store
/// lookup behind the recompute panics on it.
fn deferred_view_targets(runtime: &SurfaceRuntime) -> Vec<ParkedSet> {
    let mut targets: Vec<ParkedSet> = runtime
        .output_ports
        .iter()
        .filter(|((instance, _), _)| runtime.is_declared_instance(instance))
        .map(|((instance, _), out)| ParkedSet {
            channel: out.address.clone(),
            instance: instance.clone(),
        })
        .collect();
    targets.sort();
    targets.dedup();
    targets
}

/// The wire form of one sender's parked messages: the identity both authorities
/// know each message by, its body, and its release time in epoch milliseconds
/// UTC — the units the page's clock reads.
pub(crate) fn deferred_view_entries(parked: &[DeferredMessage]) -> Vec<DeferredViewEntry> {
    parked
        .iter()
        .map(|message| DeferredViewEntry {
            message_id: message.envelope.message_id,
            body: message.envelope.body.clone(),
            deliver_after: u64::try_from(message.release_at.timestamp_millis()).expect(
                "a surface component's release time was admitted from epoch milliseconds, so it \
                 is at or after the epoch",
            ),
        })
        .collect()
}

/// Recompute one component instance's parked view on `channel` and push it at
/// every attached session of the surface.
///
/// Recompute and push run under the messenger's deferred-view gate, so this
/// emission and the release sweep's reach a page in the order they read the
/// store. Without it the two can invert — a snapshot carries no version, so the
/// page would keep the older one and mirror a schedule that has already released,
/// with no further emission owed if that release was the set's last change.
async fn broadcast_deferred_view(
    messenger: &Messenger,
    registry: &SurfaceRegistry,
    slug: &str,
    instance: &str,
    channel: &str,
    now: DateTime<Utc>,
) {
    let sender = ParticipantId::for_surface_component(slug, instance);
    let _order = messenger.lock_deferred_view_gate().await;
    let entries = deferred_view_entries(
        &messenger
            .deferred_view_for_sender(channel, sender.as_str(), now)
            .await,
    );
    registry.push_deferred_view(
        slug,
        &DeferredViewPush {
            channel: channel.to_string(),
            instance: instance.to_string(),
            entries,
        },
    );
}

/// Seed this connection's deferred-view mirrors, immediately behind `Welcome`.
///
/// One frame per `(instance, channel)` whose parked set is nonempty. The page
/// clears every mirror at `Welcome`, so an absent frame means an empty set —
/// which is also what makes a set that emptied while the page was away arrive
/// correctly empty.
///
/// This connection only: the frames ride the same FIFO writer queue `Welcome`
/// just entered, which is what puts them behind it.
async fn seed_deferred_views(ctx: &SessionCtx, counters: &mut SessionCounters) -> FrameOutcome {
    let Some(messenger) = ctx.runtime.messenger.as_ref() else {
        return FrameOutcome::Continue;
    };
    let slug = &ctx.runtime.resolved.slug;
    let now = Utc::now();
    let targets = deferred_view_targets(&ctx.runtime);
    for target in &targets {
        let sender = ParticipantId::for_surface_component(slug, &target.instance);
        let entries = deferred_view_entries(
            &messenger
                .deferred_view_for_sender(&target.channel, sender.as_str(), now)
                .await,
        );
        if entries.is_empty() {
            continue;
        }
        let frame = ServerFrame::DeferredView {
            channel: target.channel.clone(),
            instance: target.instance.clone(),
            entries,
        };
        if let FrameOutcome::Disconnect = send_frame(&ctx.tx, frame, counters).await {
            return FrameOutcome::Disconnect;
        }
    }
    for orphan in orphaned_parked_sets(messenger, slug, &targets, now).await {
        warn!(
            surface = slug,
            instance = orphan.instance,
            channel = orphan.channel,
            "parked messages this page cannot see: the sender has a schedule on a channel no \
             declared instance binds an output onto. They release normally; nothing on the page \
             can view, edit, or cancel them until the config declares that instance and binding \
             again"
        );
    }
    FrameOutcome::Continue
}

/// The parked sets of `slug` that seeding cannot reach — the ones outside
/// `targets`.
///
/// A set goes orphaned when the config that would have named it goes away: an
/// instance a `Welcome` no longer declares, or one whose output binding on that
/// channel is gone. The entries release on the backend regardless — a durable
/// schedule outliving its author's binding is part of what durable parking is
/// for — so nothing is lost and no ladder is charged. What is gone is the page's
/// ability to see them, and that is an operator's decision to have made, so it
/// is reported rather than repaired here.
async fn orphaned_parked_sets(
    messenger: &Messenger,
    slug: &str,
    targets: &[ParkedSet],
    now: DateTime<Utc>,
) -> Vec<ParkedSet> {
    messenger
        .parked_surface_components(slug, now)
        .await
        .into_iter()
        .filter(|parked| !targets.contains(parked))
        .collect()
}

/// One admitted entry of a `PublishBatch`, resolved against the server's own
/// boot-resolved output map before any of the batch is applied.
///
/// Resolution is complete: nothing below re-reads the frame. The port, the
/// urgency the operator's config or the sender chose, and the park-vs-immediate
/// verdict are all settled here, so the two substrate halves apply the same
/// decisions and neither re-derives one.
struct ResolvedBatchEntry<'a> {
    out: &'a OutputPort,
    body: &'a str,
    urgency: Urgency,
    /// The release time this entry parks until, or `None` for an immediate
    /// publish — already judged against the flush's single clock read, so a time
    /// in the past arrives here as `None`.
    deliver_after: Option<DateTime<Utc>>,
}

/// One ephemeral entry of an admitted `PublishBatch`, borrowed for the duration
/// of [`publish_batch_ephemeral`] — the non-durable peer of the durable half's
/// `SurfaceBatchPublish`.
struct EphemeralBatchPublish<'a> {
    out: &'a OutputPort,
    body: &'a str,
    urgency: Urgency,
    publish_ts_ns: i64,
    /// See [`ResolvedBatchEntry::deliver_after`].
    deliver_after: Option<DateTime<Utc>>,
}

/// Apply one ephemeral entry of an admitted `PublishBatch`.
///
/// Routes through the **prepaid** entry point, which never consults the
/// per-sender wall-clock gate: the batch already paid, whole, at step 4, and the
/// client has been promised `Ok` for all of it. A second, independently-keyed
/// bucket metering per entry after admission could only lose a wide flush's tail
/// under an answer that said it landed. Ad-hoc (gesture) ephemeral publishes
/// still route through the publish ladder and its gate — that is where the
/// wall-clock tier belongs.
///
/// The prepaid entry point panics rather than returning: every client-reachable
/// failure was already answered as a violation by the handler's per-entry
/// resolve, so nothing is left here that a conforming boot can produce.
///
/// The append's overflow goes straight to the noise ladder. A ring charges an
/// eviction as reported at the moment it overwrites an unread position, so a
/// drop this publish caused is escalated here or nowhere — no later consumer
/// take carries it.
///
/// A deferred entry parks instead of appending, against the channel's own
/// deferred cap. Exhaustion drops that entry's schedule with a warn and a
/// counter and is not counted as a publish — normal operation on a full deferred
/// set, because the component already returned and there is nothing to answer.
///
/// Returns whether a schedule landed, so the caller can wake the release sweep
/// once for the whole ephemeral half.
#[must_use]
fn publish_batch_ephemeral(
    ctx: &SessionCtx,
    messenger: &Messenger,
    instance: &str,
    publish: EphemeralBatchPublish<'_>,
    counters: &mut SessionCounters,
) -> bool {
    let runtime = &ctx.runtime;
    let address = publish.out.address.as_str();
    let publish_ts = brenn_lib::messaging::db::ns_to_utc(publish.publish_ts_ns);
    // The sender is the deferred set's ownership key: a parked entry must carry
    // the identity that will later see it in its deferred view and name it in a
    // control op.
    let sender = ParticipantId::for_surface_component(&runtime.resolved.slug, instance);
    let prepaid = || PrepaidEntry {
        sender: &sender,
        policy: &runtime.policy,
        channel_address: address,
        body: publish.body,
        urgency: publish.urgency,
        publish_ts,
    };
    if let Some(release_at) = publish.deliver_after {
        return match messenger.park_prepaid(prepaid(), release_at) {
            Ok(_) => {
                counters.publish_ok(Some(instance));
                true
            }
            Err(QuotaExceeded { cap }) => {
                messenger.record_dropped_deferred(sender.as_str(), address);
                warn!(
                    instance,
                    channel = address,
                    cap,
                    "surface deferred publish dropped — channel deferred set at its retain_depth \
                     cap"
                );
                false
            }
        };
    }
    let appended = messenger.publish_prepaid(prepaid());
    messenger.enact_overflow_for_channel(address, &appended.overflow);
    counters.publish_ok(Some(instance));
    false
}

/// Send one `ServerFrame` to the writer, counting it and mapping a closed
/// channel (writer gone) to `Disconnect`.
async fn send_frame(
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

/// Handle an `Alert` frame — the grant-gated paging plane, disciplined like
/// `Publish`, not lenient like `Log`.
///
/// 1. Grant check first: an `Alert` from a surface without the alert grant is a
///    protocol violation. A conforming shell learns the grant at attach time and
///    suppresses ungranted alerts client-side, so this frame reaches the server
///    only from a non-conforming client.
/// 2. Size caps: oversized title/body is a violation — the alert plane is opt-in
///    and its client is expected to conform. The payload is never echoed into the
///    security detail.
/// 3. Per-connection alert bucket: beyond-burst alerts are dropped, counted, and
///    warned — not a kill. The process-wide alert rate limiter bounds total
///    paging downstream, as it does for WASM and native alerts.
/// 4. Dispatch: title/body are sanitized (same discipline as the WASM alert
///    host), attribution is appended to the body, and the title is host-prefixed
///    `Surface {slug}: ` so a surface cannot impersonate a host, app, or WASM
///    alert source. Severity maps 1:1 to native.
/// 5. Record: one `warn!` line — the operator's durable record of who paged.
///    Alerts do not republish onto `surface_error_channel` (its body contract is
///    single-shape log records; alert durability is fire-and-forget).
fn handle_alert(
    ctx: &SessionCtx,
    alert_bucket: &mut TokenBucket,
    counters: &mut SessionCounters,
    severity: ProtoAlertSeverity,
    title: &str,
    body: &str,
) -> FrameOutcome {
    let slug = &ctx.runtime.resolved.slug;
    let username = &ctx.username;
    let session_id = ctx.session_id;

    // 1. Grant check — deny-by-default.
    if !ctx.runtime.policy.grants.has(AppCapability::SurfaceAlert) {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: Alert on a surface without the alert grant"
        ));
    }

    // 2. Size caps — a violation on the granted plane, unlike the lenient Log
    //    floor. The payload is never echoed.
    if title.len() > MAX_ALERT_TITLE_BYTES || body.len() > MAX_ALERT_BODY_BYTES {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: Alert field exceeds size cap \
             (title {}/{MAX_ALERT_TITLE_BYTES}, body {}/{MAX_ALERT_BODY_BYTES})",
            title.len(),
            body.len(),
        ));
    }

    // 3. Per-connection alert bucket. Beyond-bucket is dropped, counted, warned —
    //    never a violation.
    match alert_bucket.try_consume() {
        TokenBucketOutcome::Granted => {}
        TokenBucketOutcome::GrantedAfterSuppression { suppressed } => {
            warn!(
                suppressed,
                "surface Alert rate limit lifted, alerts were suppressed"
            );
        }
        TokenBucketOutcome::Denied { first } => {
            counters.alerts_suppressed += 1;
            if first {
                warn!("rate-limiting surface Alert frames from this connection");
            }
            return FrameOutcome::Continue;
        }
    }

    // 4. Dispatch. Sanitize, append attribution, host-prefix the title.
    let title = sanitize_untrusted_str(title, MAX_ALERT_TITLE_BYTES);
    let body = sanitize_untrusted_str(body, MAX_ALERT_BODY_BYTES);
    let severity = map_alert_severity(severity);
    let attributed_body = format!("{body}\nsurface={slug} user={username} session={session_id}");
    ctx.alert_dispatcher.alert(
        severity,
        format!("Surface {slug}: {title}"),
        attributed_body,
    );

    // 5. Record.
    counters.alerts_dispatched += 1;
    warn!(severity = %severity, title = %title, "surface alert dispatched");
    FrameOutcome::Continue
}

/// Owns the WS sink. Serializes outbound frames, emits the server-side liveness
/// probe (native `Ping`) every `heartbeat`, adds an idle `Heartbeat` frame when
/// nothing else was written since the last tick, and bounds every write with a
/// stalled-reader watchdog. Exits (dropping `rx`, which tears the session down)
/// on any sink error, watchdog timeout, or sender drop.
async fn writer_task(
    mut sink: SplitSink<WebSocket, Message>,
    mut rx: mpsc::Receiver<ServerFrame>,
    heartbeat: Duration,
) {
    let watchdog = heartbeat * 3;
    let mut ticker = tokio::time::interval(heartbeat);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // consume the immediate first tick
    let mut wrote_frame_since_tick = false;

    loop {
        tokio::select! {
            maybe_frame = rx.recv() => {
                match maybe_frame {
                    Some(frame) => {
                        let json = serde_json::to_string(&frame)
                            .expect("ServerFrame serialization");
                        if !write_with_watchdog(&mut sink, Message::Text(json.into()), watchdog)
                            .await
                        {
                            return;
                        }
                        wrote_frame_since_tick = true;
                    }
                    // Session task dropped the sender: teardown.
                    None => return,
                }
            }
            _ = ticker.tick() => {
                if !write_with_watchdog(&mut sink, Message::Ping(Vec::new().into()), watchdog).await
                {
                    return;
                }
                if !wrote_frame_since_tick {
                    let json = serde_json::to_string(&ServerFrame::Heartbeat)
                        .expect("ServerFrame serialization");
                    if !write_with_watchdog(&mut sink, Message::Text(json.into()), watchdog).await {
                        return;
                    }
                }
                wrote_frame_since_tick = false;
            }
        }
    }
}

/// One watchdog-bounded sink write. Returns `false` (caller must exit) on sink
/// error or on a stalled reader that keeps a write pending past the watchdog.
///
/// Attribution (surface/session_id/user/ip) comes from the session span the
/// writer task is instrumented with, so the `warn!`s below need no explicit
/// fields.
async fn write_with_watchdog(
    sink: &mut SplitSink<WebSocket, Message>,
    msg: Message,
    watchdog: Duration,
) -> bool {
    match tokio::time::timeout(watchdog, sink.send(msg)).await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            warn!("surface WS write failed: {e}");
            false
        }
        Err(_) => {
            warn!("surface WS writer stalled (reader not draining); tearing down");
            false
        }
    }
}

/// One wire-ready delivery: the message plus the count of messages dropped on
/// this channel since the previous delivery on this connection.
///
/// The session task maps this to a `Deliver` frame, reading `delivery.envelope`
/// and `delivery.seq` and forwarding `dropped`. Carrying the `Arc` rather than a
/// cloned envelope keeps fan-out to one allocation per message.
#[derive(Debug, Clone)]
pub struct DeliveryItem {
    /// The delivered message and its per-channel sequence number.
    pub delivery: Arc<EphemeralDelivery>,
    /// Messages lost to broadcast overflow on this channel since the previous
    /// `DeliveryItem`. `0` when none were dropped.
    pub dropped: u64,
}

/// A single ephemeral subscription rendered as a stream of wire-ready deliveries.
///
/// Folds the live event stream so a `Dropped(n)` overflow signal is never yielded
/// alone: its count accumulates into the `dropped` field of the next delivery.
/// A `Dropped(n)` is emitted only on fan-out lag — which means the ring is
/// full, so a delivery is immediately available behind it — so the pending count
/// lives for one poll. If the subscription tears down while a count is pending,
/// that count dies with it, which is correct: the client no longer holds the
/// subscription it described.
pub struct SubscriptionStream {
    inner: Pin<Box<dyn Stream<Item = DeliveryItem> + Send>>,
    /// An item polled out of `inner` by [`head_now`](Self::head_now) and not yet
    /// yielded. Held so a co-availability check never consumes a delivery it
    /// declines to coalesce.
    head: Option<DeliveryItem>,
    /// Set once `inner` has yielded its terminating `None` (the store dropped at
    /// shutdown). `inner` is an `unfold`, which panics if polled after it
    /// returns `None`, so once seen the terminator is remembered and `inner` is
    /// never polled again — `head_now`, which polls off the `StreamMap`'s real
    /// waker, would otherwise eat the `None` and leave the completed stream to be
    /// re-polled.
    done: bool,
}

impl SubscriptionStream {
    /// Wrap a live subscription receiver as a delivery stream.
    pub fn new(receiver: EphemeralReceiver) -> Self {
        Self {
            inner: Box::pin(delivery_stream(receiver_events(receiver))),
            head: None,
            done: false,
        }
    }

    /// The item at the head of this subscription's stream if one is available
    /// without waiting, else `None`.
    ///
    /// Polls with a no-op waker: a `Pending` result registers nothing, which is
    /// sound only because the session loop re-polls the whole `StreamMap` — with
    /// its real waker — on every turn, so a wakeup this poll would have armed is
    /// re-armed immediately. A `Ready(None)` terminator is recorded, not
    /// discarded, so the `StreamMap` still observes the completion and `inner` is
    /// never polled past it.
    fn head_now(&mut self) -> Option<&DeliveryItem> {
        if self.head.is_none() && !self.done {
            let mut cx = Context::from_waker(Waker::noop());
            match self.inner.as_mut().poll_next(&mut cx) {
                Poll::Ready(Some(item)) => self.head = Some(item),
                Poll::Ready(None) => self.done = true,
                Poll::Pending => {}
            }
        }
        self.head.as_ref()
    }

    /// Take the item [`head_now`](Self::head_now) reported.
    fn take_head(&mut self) -> Option<DeliveryItem> {
        self.head.take()
    }
}

impl Stream for SubscriptionStream {
    type Item = DeliveryItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(item) = self.head.take() {
            return Poll::Ready(Some(item));
        }
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

/// Drive an `EphemeralReceiver` as a stream of raw live events, ending when the
/// store's fan-out closes at shutdown.
fn receiver_events(receiver: EphemeralReceiver) -> impl Stream<Item = EphemeralEvent> + Send {
    stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|event| (event, receiver))
    })
}

/// Fold raw live events into wire-ready `DeliveryItem`s: accumulate `Dropped(n)`
/// counts into a pending total and attach it to the next `Delivery`.
fn delivery_stream(
    events: impl Stream<Item = EphemeralEvent> + Send + 'static,
) -> impl Stream<Item = DeliveryItem> + Send + 'static {
    stream::unfold(
        (Box::pin(events), 0u64),
        |(mut events, mut pending)| async move {
            loop {
                match events.next().await {
                    Some(EphemeralEvent::Dropped(n)) => pending += n,
                    Some(EphemeralEvent::Delivery(delivery)) => {
                        let item = DeliveryItem {
                            delivery,
                            dropped: pending,
                        };
                        return Some((item, (events, 0)));
                    }
                    None => return None,
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use brenn_lib::access::acl::ChannelMatcher;
    use brenn_lib::access::{AppCapability, AppPolicy};
    use brenn_lib::messaging::config::Depth;
    use brenn_lib::messaging::store::RingStores;
    use brenn_lib::messaging::store::ring::RING_FAN_OUT_CAPACITY;
    use brenn_lib::messaging::testutils::ephemeral_channel_entry;
    use brenn_lib::messaging::{
        EphemeralEvent, MessagingDirectory, MessagingGlobalConfig, ParticipantId, Urgency,
        WakeRouter,
    };

    use super::super::test_fixtures::{
        TEST_MAX_BODY_BYTES, TEST_ORIGIN, durable_resume, durable_resume_at,
    };
    use super::*;

    const CHANNEL: &str = "ephemeral:protobar";
    /// Bare channel name the `ephemeral_subscribe`/`ephemeral_publish` matchers
    /// key on (the scheme prefix is stripped before matching).
    const CHANNEL_NAME: &str = "protobar";

    /// [`handle_publish_batch`] for a flush carrying no control ops — the shape
    /// every test about the publish half wants.
    async fn flush_batch(
        ctx: &SessionCtx,
        instance: &str,
        correlation: u64,
        publishes: &[BatchEntry],
        counters: &mut SessionCounters,
    ) -> FrameOutcome {
        handle_publish_batch(ctx, instance, correlation, publishes, &[], counters).await
    }

    /// A `Messenger` over one `ephemeral:` channel — everything a live attach
    /// needs and nothing else.
    fn ring_messenger(retain_depth: u64, fan_out_capacity: u32) -> Arc<Messenger> {
        let entry = ephemeral_channel_entry(CHANNEL_NAME, retain_depth);
        Messenger::new(
            brenn_lib::db::init_db_memory(),
            Arc::new(MessagingDirectory::with_entries(vec![entry.clone()])),
            Arc::from("test-source"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_ring_stores(Arc::new(RingStores::build_with_fan_out_capacity(
            &[entry],
            fan_out_capacity,
        )))
    }

    fn subscriber_policy() -> Arc<AppPolicy> {
        let mut p = AppPolicy::default();
        p.grants.insert(AppCapability::EphemeralSubscribe);
        p.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(CHANNEL_NAME.to_string())];
        Arc::new(p)
    }

    fn publish_n(messenger: &Messenger, n: usize) {
        let sender = ParticipantId::for_surface("deskbar");
        for _ in 0..n {
            crate::routes::surface::test_fixtures::commit_eph(
                messenger.ring_stores(),
                CHANNEL,
                &sender,
                "hi",
            );
        }
    }

    fn stream_for(messenger: &Messenger) -> SubscriptionStream {
        let sub = messenger
            .attach_live(
                ParticipantId::for_surface("deskbar"),
                subscriber_policy(),
                CHANNEL,
                None,
            )
            .expect("attach");
        SubscriptionStream::new(sub.receiver)
    }

    #[tokio::test]
    async fn undropped_deliveries_carry_zero_dropped_in_seq_order() {
        let messenger = ring_messenger(8, RING_FAN_OUT_CAPACITY);
        let mut stream = stream_for(&messenger);
        publish_n(&messenger, 3);

        for expected_seq in 1..=3 {
            let item = stream.next().await.expect("delivery");
            assert_eq!(item.delivery.seq, expected_seq);
            assert_eq!(item.dropped, 0);
        }
    }

    #[tokio::test]
    async fn dropped_count_rides_the_next_delivery() {
        // Overrun the broadcast ring by 3 with no interleaved poll: the receiver
        // lags by 3 (the 3 oldest seqs overwritten). The fold never yields the
        // drop alone — it rides the first surviving delivery.
        const CAPACITY: u32 = 4;
        const OVERSHOOT: u64 = 3;
        let flood = CAPACITY as usize + OVERSHOOT as usize;
        let messenger = ring_messenger(0, CAPACITY);
        let mut stream = stream_for(&messenger);
        publish_n(&messenger, flood);

        let first = stream.next().await.expect("delivery");
        assert_eq!(first.delivery.seq, OVERSHOOT + 1);
        assert_eq!(first.dropped, OVERSHOOT);

        let second = stream.next().await.expect("delivery");
        assert_eq!(second.delivery.seq, OVERSHOOT + 2);
        assert_eq!(second.dropped, 0);
    }

    #[tokio::test]
    async fn consecutive_drops_accumulate_onto_one_delivery() {
        // The live stream never emits two drops back-to-back (a delivery always
        // sits behind a lag), so drive the fold directly with synthetic events to
        // pin the accumulation arithmetic. Reuse a real delivery Arc to avoid
        // hand-building an envelope.
        let messenger = ring_messenger(1, RING_FAN_OUT_CAPACITY);
        let mut seed = stream_for(&messenger);
        publish_n(&messenger, 1);
        let delivery = seed.next().await.expect("delivery").delivery;

        let events = stream::iter(vec![
            EphemeralEvent::Dropped(2),
            EphemeralEvent::Dropped(3),
            EphemeralEvent::Delivery(delivery.clone()),
        ]);
        let mut folded = Box::pin(delivery_stream(events));

        let item = folded.next().await.expect("delivery");
        assert_eq!(item.dropped, 5);
        assert_eq!(item.delivery.seq, delivery.seq);
    }

    #[tokio::test]
    async fn store_teardown_ends_the_stream() {
        let messenger = ring_messenger(8, RING_FAN_OUT_CAPACITY);
        let mut stream = stream_for(&messenger);
        publish_n(&messenger, 2);

        assert!(stream.next().await.is_some());
        assert!(stream.next().await.is_some());

        // Dropping the last store handle closes the fan-out: the stream ends.
        drop(messenger);
        assert!(stream.next().await.is_none());
    }

    /// The shared test [`SessionCtx`] builder: a `deskbar` surface with the
    /// standard fixture identity (nil session, `dev`, localhost), optionally
    /// carrying the `SurfaceAlert` grant, owning the given dispatcher clone.
    fn alert_ctx(granted: bool, alert_dispatcher: AlertDispatcher) -> SessionCtx {
        use std::net::{IpAddr, Ipv4Addr};

        use brenn_lib::messaging::config::ResolvedSurface;

        let mut policy = AppPolicy::default();
        if granted {
            policy.grants.insert(AppCapability::SurfaceAlert);
        }
        let resolved = ResolvedSurface {
            slug: "deskbar".to_string(),
            skin: "bench".to_string(),
            components: vec![brenn_lib::messaging::config::ResolvedComponent {
                instance: "chrome".to_string(),
                kind: "chrome".to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: brenn_lib::messaging::config::SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: true,
            }],
            subscriptions: vec![],
            durable_subscriptions: vec![],
            local_channels: vec![],
            outputs: vec![],
            policy,
            allowed_users: vec![],
            publish_burst: 60,
            publish_per_sec: 1,
        };
        let runtime = Arc::new(SurfaceRuntime::build(
            resolved,
            None,
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        ));
        let (tx, _rx) = mpsc::channel(16);
        SessionCtx {
            runtime,
            session_id: Uuid::nil(),
            username: "dev".to_string(),
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            alert_dispatcher,
            registry: SurfaceRegistry::default(),
            tx,
        }
    }

    #[test]
    fn map_alert_severity_is_one_to_one() {
        assert_eq!(
            map_alert_severity(ProtoAlertSeverity::Info).to_string(),
            "info"
        );
        assert_eq!(
            map_alert_severity(ProtoAlertSeverity::Warning).to_string(),
            "warning"
        );
        assert_eq!(
            map_alert_severity(ProtoAlertSeverity::Critical).to_string(),
            "critical"
        );
    }

    #[tokio::test]
    async fn alert_without_grant_is_violation() {
        let (dispatcher, handle) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let ctx = alert_ctx(false, dispatcher);
        let mut bucket = TokenBucket::new(ALERT_BURST, ALERT_REFILL, 1);
        let mut counters = SessionCounters::default();

        let outcome = handle_alert(
            &ctx,
            &mut bucket,
            &mut counters,
            ProtoAlertSeverity::Warning,
            "t",
            "b",
        );

        assert!(matches!(outcome, FrameOutcome::Violation(_)));
        assert_eq!(counters.alerts_dispatched, 0);
        drop(ctx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn alert_oversized_field_is_violation() {
        let (dispatcher, handle) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let ctx = alert_ctx(true, dispatcher);
        let mut bucket = TokenBucket::new(ALERT_BURST, ALERT_REFILL, 1);
        let mut counters = SessionCounters::default();

        let big_title = "x".repeat(MAX_ALERT_TITLE_BYTES + 1);
        let outcome = handle_alert(
            &ctx,
            &mut bucket,
            &mut counters,
            ProtoAlertSeverity::Warning,
            &big_title,
            "b",
        );

        assert!(matches!(outcome, FrameOutcome::Violation(_)));
        assert_eq!(counters.alerts_dispatched, 0);
        drop(ctx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn alert_bucket_drops_beyond_burst_without_kill() {
        let (dispatcher, handle) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let ctx = alert_ctx(true, dispatcher);
        let mut bucket = TokenBucket::new(ALERT_BURST, ALERT_REFILL, 1);
        let mut counters = SessionCounters::default();

        // Burst of ALERT_BURST admitted, all Continue.
        for _ in 0..ALERT_BURST {
            let outcome = handle_alert(
                &ctx,
                &mut bucket,
                &mut counters,
                ProtoAlertSeverity::Warning,
                "t",
                "b",
            );
            assert!(matches!(outcome, FrameOutcome::Continue));
        }
        // The next one is dropped (not a violation) and counted.
        let outcome = handle_alert(
            &ctx,
            &mut bucket,
            &mut counters,
            ProtoAlertSeverity::Warning,
            "t",
            "b",
        );
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert_eq!(counters.alerts_dispatched, u64::from(ALERT_BURST));
        assert_eq!(counters.alerts_suppressed, 1);

        drop(ctx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn alert_dispatches_with_host_prefix_and_attribution() {
        use brenn_lib::obs::alerting::make_capturing_alerter;

        let (dispatcher, captured, handle) = make_capturing_alerter();
        let ctx = alert_ctx(true, dispatcher);
        let mut bucket = TokenBucket::new(ALERT_BURST, ALERT_REFILL, 1);
        let mut counters = SessionCounters::default();

        let outcome = handle_alert(
            &ctx,
            &mut bucket,
            &mut counters,
            ProtoAlertSeverity::Warning,
            "component panic: protobar",
            "the panic detail",
        );
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert_eq!(counters.alerts_dispatched, 1);

        // Drop the ctx (its dispatcher clone) so the alert mpsc closes, then drain.
        drop(ctx);
        handle.await.unwrap();

        let cap = captured.lock().unwrap();
        assert_eq!(cap.len(), 1);
        assert_eq!(cap[0].0, "Surface deskbar: component panic: protobar");
        assert!(
            cap[0].1.starts_with("the panic detail"),
            "body should lead with the sanitized client body, got {:?}",
            cap[0].1
        );
        assert!(
            cap[0]
                .1
                .contains("surface=deskbar user=dev session=00000000-0000-0000-0000-000000000000"),
            "body should carry server-attested attribution, got {:?}",
            cap[0].1
        );
    }

    #[tokio::test]
    async fn alert_hostile_title_dispatched_bounded_and_escaped() {
        use brenn_lib::obs::alerting::make_capturing_alerter;

        let (dispatcher, captured, handle) = make_capturing_alerter();
        let ctx = alert_ctx(true, dispatcher);
        let mut bucket = TokenBucket::new(ALERT_BURST, ALERT_REFILL, 1);
        let mut counters = SessionCounters::default();

        // Exactly MAX_ALERT_TITLE_BYTES raw ESC bytes: passes the raw-length gate
        // (not `>`), then each '\x1b' escapes to `\u{1b}` (6×) — an unbounded sanitizer
        // would push ~1.5 KiB into the ntfy Title header. Assert the dispatched title is
        // escaped and output-bounded at this browser-reachable sink.
        let hostile_title = "\x1b".repeat(MAX_ALERT_TITLE_BYTES);
        let outcome = handle_alert(
            &ctx,
            &mut bucket,
            &mut counters,
            ProtoAlertSeverity::Warning,
            &hostile_title,
            "b",
        );
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert_eq!(counters.alerts_dispatched, 1);

        drop(ctx);
        handle.await.unwrap();

        let cap = captured.lock().unwrap();
        assert_eq!(cap.len(), 1);
        let prefix = "Surface deskbar: ";
        assert!(
            cap[0].0.starts_with(prefix),
            "dispatched title must carry the host prefix, got {:?}",
            cap[0].0
        );
        assert!(
            !cap[0].0.contains('\x1b'),
            "raw ESC control char must be escaped in the dispatched title, got {:?}",
            cap[0].0
        );
        let sanitized = &cap[0].0[prefix.len()..];
        assert!(
            sanitized.len() <= MAX_ALERT_TITLE_BYTES + brenn_common::TRUNCATION_MARKER.len(),
            "sanitized title must be bounded to cap + marker, got {} bytes",
            sanitized.len()
        );
    }

    // ── Durable projection ────────────────────────────────────────────────

    const DURABLE_ADDR: &str = "brenn:durable-demo";

    /// The instance every durable fixture binding below belongs to.
    const DURABLE_INSTANCE: &str = "protobar";

    /// The one subscription `durable_ctx` declares — `DURABLE_INSTANCE`'s
    /// binding on `DURABLE_ADDR`. Subscriptions are per (instance, channel), so
    /// every handler below is driven with the whole key, never a bare channel.
    fn durable_sub() -> SubKey {
        SubKey {
            instance: DURABLE_INSTANCE.to_string(),
            channel: DURABLE_ADDR.to_string(),
        }
    }

    /// A durable-capable [`SessionCtx`]: a `deskbar` surface bound to one durable
    /// `brenn:` channel, backed by a real in-memory `Messenger` whose directory
    /// declares that channel (retain clamp `retain_depth`). Returns the ctx, the
    /// outbound-frame receiver (to read the frames the durable handlers enqueue),
    /// and the channel uuid (to seed rows).
    async fn durable_ctx(
        db: &brenn_lib::db::Db,
        retain_depth: Depth,
    ) -> (SessionCtx, mpsc::Receiver<ServerFrame>, Uuid) {
        durable_ctx_for(db, retain_depth, &[DURABLE_INSTANCE]).await
    }

    /// [`durable_ctx`] whose surface declares one durable subscription per named
    /// instance — sibling principals on the one channel and the one session, each
    /// with its own subscription state.
    async fn durable_ctx_for(
        db: &brenn_lib::db::Db,
        retain_depth: Depth,
        instances: &[&str],
    ) -> (SessionCtx, mpsc::Receiver<ServerFrame>, Uuid) {
        use brenn_lib::access::acl::ChannelMatcher;
        use brenn_lib::messaging::config::{
            ChannelConfigRaw, MessagingGlobalConfig, NoiseLevel, ResolvedSubscription,
            build_channel_entries,
        };
        use brenn_lib::messaging::{
            MessagingDirectory, Messenger, WakeMin, WakeRouter, query::NoopWakeRouter,
        };

        let raw = ChannelConfigRaw {
            send_rate: None,
            uuid: Some(Uuid::new_v4().to_string()),
            address: "durable-demo".to_string(),
            description: None,
            push_depth: None,
            retain_depth: None,
            standing_retain_depth: None,
            noise: None,
            sink: None,
            wake_min: None,
        };
        let entry = build_channel_entries(&[raw], &MessagingGlobalConfig::default())
            .pop()
            .expect("one channel entry");
        let channel_uuid = entry.uuid;
        {
            let conn = db.lock().await;
            brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&entry));
        }
        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![entry])),
            Arc::from(TEST_ORIGIN),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );

        let mut policy = AppPolicy::default();
        policy.grants.insert(AppCapability::MessagingSubscribe);
        policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact("durable-demo".to_string())];

        let mut fixture = crate::test_support::surface::SurfaceFixture::new("deskbar", "protobar")
            .subscribe(DURABLE_ADDR, "protobar", "messages")
            .policy(policy);
        for instance in instances {
            fixture = fixture.durable_subscribe(
                instance,
                ResolvedSubscription {
                    channel_uuid,
                    channel_address: DURABLE_ADDR.to_string(),
                    push_depth: Depth::Bounded(64),
                    retain_depth,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
            );
        }
        let resolved = fixture.build();
        let runtime = SurfaceRuntime::build(
            resolved,
            Some(messenger),
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );
        let (alert_dispatcher, _drainer) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let (tx, rx) = mpsc::channel::<ServerFrame>(64);
        let ctx = SessionCtx {
            runtime: Arc::new(runtime),
            session_id: Uuid::nil(),
            username: "dev".to_string(),
            ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            alert_dispatcher,
            registry: SurfaceRegistry::default(),
            tx,
        };
        (ctx, rx, channel_uuid)
    }

    // ── Reserved error-report port: backstop + audit ─────────────────────────

    /// A [`SessionCtx`] whose runtime binds the reserved `#brenn`/`error-reports`
    /// output port to `brenn:surface-errors`, backed by a real in-memory
    /// `Messenger`. When `has_grant` the surface policy carries the
    /// substrate-injected error-channel publish grant (so a report publishes
    /// `Ok`); when not, the policy lacks it (so `publish_from_surface` returns
    /// `AclDenied` — the broken-boot-invariant outcome the backstop arm handles).
    /// Returns the ctx and the outbound-frame receiver.
    async fn report_ctx(
        db: &brenn_lib::db::Db,
        has_grant: bool,
    ) -> (SessionCtx, mpsc::Receiver<ServerFrame>) {
        use brenn_lib::messaging::config::{
            ChannelConfigRaw, MessagingGlobalConfig, SurfaceSendBudget, build_channel_entries,
        };
        use brenn_lib::messaging::testutils::surface_registrations;
        use brenn_lib::messaging::{
            MessagingDirectory, Messenger, WakeRouter, query::NoopWakeRouter,
        };

        let raw = ChannelConfigRaw {
            send_rate: None,
            uuid: Some(Uuid::new_v4().to_string()),
            address: "surface-errors".to_string(),
            description: None,
            push_depth: None,
            retain_depth: None,
            standing_retain_depth: None,
            noise: None,
            sink: None,
            wake_min: None,
        };
        let entry = build_channel_entries(&[raw], &MessagingGlobalConfig::default())
            .pop()
            .expect("one channel entry");
        {
            let conn = db.lock().await;
            brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&entry));
        }

        let mut policy = AppPolicy::default();
        policy.grants.insert(AppCapability::MessagingPublish);
        if has_grant {
            policy
                .acls
                .brenn_publish
                .push(ChannelMatcher::Exact("surface-errors".to_string()));
        }
        let mut surface_policies = std::collections::HashMap::new();
        surface_policies.insert("deskbar".to_string(), policy.clone());

        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![entry])),
            Arc::from(TEST_ORIGIN),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(surface_registrations(surface_policies))
        // deskbar declares one component kind (`protobar`, per the fixture below),
        // so both grains it can publish under are budgeted: its kernel identity
        // (a self-report) and `protobar` (a report about that component).
        .with_surface_send_budgets([(
            "deskbar".to_string(),
            vec![
                (None, SurfaceSendBudget::default()),
                (Some("protobar".to_string()), SurfaceSendBudget::default()),
            ],
        )]);

        let resolved = crate::test_support::surface::SurfaceFixture::new("deskbar", "protobar")
            .policy(policy)
            .build();
        let mut runtime = SurfaceRuntime::build(
            resolved,
            Some(messenger),
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );
        // Wire the reserved port + floor exactly as `build_surface_runtimes` does.
        runtime.output_ports.insert(
            (
                brenn_surface_contract::ERROR_REPORT_INSTANCE.to_string(),
                brenn_surface_contract::ERROR_REPORT_PORT.to_string(),
            ),
            OutputPort {
                address: "brenn:surface-errors".to_string(),
                class: DeliveryClass::Durable,
                default_urgency: Urgency::Normal,
            },
        );
        runtime.error_report_floor = Some(brenn_surface_proto::LogLevel::Warn);

        let (alert_dispatcher, _drainer) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let (tx, rx) = mpsc::channel::<ServerFrame>(64);
        let ctx = SessionCtx {
            runtime: Arc::new(runtime),
            session_id: Uuid::nil(),
            username: "dev".to_string(),
            ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            alert_dispatcher,
            registry: SurfaceRegistry::default(),
            tx,
        };
        (ctx, rx)
    }

    const REPORT_BODY: &str =
        r#"{"source":"component:echo-stub","message":"boom","level":"error"}"#;

    /// A [`PublishRequest`] to the reserved error-report port, subject varying —
    /// the one axis the report tests actually differ on.
    fn report_request(subject_instance: Option<&str>) -> PublishRequest<'_> {
        PublishRequest {
            instance: brenn_surface_contract::ERROR_REPORT_INSTANCE,
            port: brenn_surface_contract::ERROR_REPORT_PORT,
            body: REPORT_BODY,
            correlation: Some(3),
            subject_instance,
            urgency: None,
        }
    }

    /// The §8 backstop: a broken-boot-invariant outcome (`AclDenied` here) on the
    /// reserved error-report port must `error!` the report body and answer
    /// `Failed` — **never panic**. Killing the server over its own diagnostics
    /// channel, on an attacker-adjacent frame path, inverts priorities.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn report_backstop_acl_denied_answers_failed_without_panic() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = report_ctx(&db, false).await;
        let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
        let mut counters = SessionCounters::default();

        let outcome = handle_publish(&ctx, &mut bucket, report_request(None), &mut counters).await;

        // No panic; the session stays live (the frame was enqueued).
        assert!(matches!(outcome, FrameOutcome::Continue));
        match rx.try_recv().expect("PublishResult frame") {
            ServerFrame::PublishResult {
                correlation,
                outcome,
            } => {
                assert_eq!(correlation, Some(3));
                assert!(
                    matches!(outcome, PublishOutcome::Failed),
                    "reserved-port failure must answer Failed, got {outcome:?}"
                );
            }
            other => panic!("expected PublishResult, got {other:?}"),
        }
        // The report is preserved in the error! line (body included).
        assert!(
            logs_contain("boom"),
            "the error! backstop must carry the report body"
        );
        assert!(logs_contain("report preserved"));
    }

    /// The same broken-boot-invariant outcome on an *ordinary* bound output still
    /// panics — the backstop is scoped to the reserved port alone.
    #[tokio::test]
    #[should_panic(expected = "rejected bound output")]
    async fn ordinary_bound_output_acl_denied_still_panics() {
        let db = brenn_lib::db::init_db_memory();
        let (mut ctx, _rx) = report_ctx(&db, false).await;
        // Re-bind the reserved pair's address under an ordinary (non-reserved)
        // port so the same AclDenied hits the panic branch, not the backstop.
        let runtime = Arc::get_mut(&mut ctx.runtime).expect("uniquely owned in test");
        runtime.output_ports.insert(
            ("protobar".to_string(), "out".to_string()),
            OutputPort {
                address: "brenn:surface-errors".to_string(),
                class: DeliveryClass::Durable,
                default_urgency: Urgency::Normal,
            },
        );
        let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
        let mut counters = SessionCounters::default();
        let _ = handle_publish(
            &ctx,
            &mut bucket,
            PublishRequest {
                instance: "protobar",
                port: "out",
                body: REPORT_BODY,
                correlation: Some(4),
                subject_instance: None,
                urgency: None,
            },
            &mut counters,
        )
        .await;
    }

    /// A successful report emits exactly one `surface_report` audit record
    /// carrying surface/session/user — the only server-side correlation for a
    /// report, since §5 strips those from the surface-attributed body.
    ///
    /// Uses a buffer-capturing subscriber rather than `tracing_test`: the audit
    /// emit rides the custom `surface_report` target, which `tracing_test`'s
    /// crate-scoped env filter (`brenn_server=trace`) drops. A current-thread
    /// runtime under `with_default` keeps the in-task emit on the subscriber.
    #[test]
    fn report_success_emits_audit_record() {
        use std::io::Write;
        use std::sync::{Arc as StdArc, Mutex as StdMutex};

        #[derive(Clone)]
        struct VecWriter(StdArc<StdMutex<Vec<u8>>>);
        impl Write for VecWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buf: StdArc<StdMutex<Vec<u8>>> = StdArc::new(StdMutex::new(Vec::new()));
        let writer_buf = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || VecWriter(writer_buf.clone()))
            .with_ansi(false)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let db = brenn_lib::db::init_db_memory();
                let (ctx, mut rx) = report_ctx(&db, true).await;
                let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
                let mut counters = SessionCounters::default();

                let outcome =
                    handle_publish(&ctx, &mut bucket, report_request(None), &mut counters).await;

                assert!(matches!(outcome, FrameOutcome::Continue));
                match rx.try_recv().expect("PublishResult frame") {
                    ServerFrame::PublishResult { outcome, .. } => assert!(
                        matches!(outcome, PublishOutcome::Ok),
                        "granted report must answer Ok, got {outcome:?}"
                    ),
                    other => panic!("expected PublishResult, got {other:?}"),
                }
            });
        });

        let logs = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("surface error report published"),
            "the audit info! must fire on a successful report: {logs}"
        );
        assert!(
            logs.contains("surface_report"),
            "the audit emit uses the surface_report target: {logs}"
        );
        assert!(
            logs.contains("surface=deskbar"),
            "audit names the surface: {logs}"
        );
        assert!(logs.contains("user=dev"), "audit names the user: {logs}");
    }

    // ── Per-component identity derivation ────────────────────────────────────

    /// A report naming a declared `subject_instance` is stored under that
    /// component's sub-identity — attribution lands on the component the report
    /// is about, not on the surface.
    #[tokio::test]
    async fn report_with_subject_stamps_component_sub_identity() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = report_ctx(&db, true).await;
        let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
        let mut counters = SessionCounters::default();

        let outcome = handle_publish(
            &ctx,
            &mut bucket,
            report_request(Some("protobar")),
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        let conn = db.lock().await;
        let sender: String = conn
            .query_row("SELECT sender FROM messaging_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            sender, "surface:deskbar#protobar",
            "a report about a component is attributed to that component"
        );
    }

    /// The session's per-instance breakdown attributes a publish to the same
    /// principal the sender identity and the send budget use — the subject
    /// component, not the frame's reserved `#brenn` instance.
    ///
    /// This is the property that makes the breakdown worth having: if it keyed
    /// off the frame's `instance` field it would file every error report under
    /// `#brenn` and answer nothing.
    #[tokio::test]
    async fn session_counters_attribute_a_publish_to_its_principal() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = report_ctx(&db, true).await;
        let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
        let mut counters = SessionCounters::default();

        let _ = handle_publish(
            &ctx,
            &mut bucket,
            report_request(Some("protobar")),
            &mut counters,
        )
        .await;

        assert_eq!(counters.publishes, 1, "the session-wide total");
        assert_eq!(
            counters.by_instance.get("protobar"),
            Some(&InstancePublishCounters {
                publishes: 1,
                publish_rate_limited: 0,
            }),
            "attributed to the subject component, not to the reserved instance"
        );
        assert!(
            !counters
                .by_instance
                .contains_key(brenn_surface_contract::ERROR_REPORT_INSTANCE),
            "the frame's reserved instance is not a principal and gets no column"
        );
    }

    /// A kernel-grain publish (a self-report with no subject) moves the total but
    /// takes no instance column — it has no instance. Pinned so the breakdown's
    /// documented "does not sum to the total" property is a decision, not a bug
    /// someone later "fixes" by inventing a `#brenn` row.
    #[tokio::test]
    async fn session_counters_leave_a_kernel_publish_unattributed() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = report_ctx(&db, true).await;
        let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
        let mut counters = SessionCounters::default();

        let _ = handle_publish(&ctx, &mut bucket, report_request(None), &mut counters).await;

        assert_eq!(counters.publishes, 1);
        assert!(
            counters.by_instance.is_empty(),
            "a kernel publish is attributable to no component: {:?}",
            counters.by_instance
        );
    }

    /// A rate-limited publish is attributed too, and lands in its own column: the
    /// operator question the breakdown answers is "which component is being
    /// throttled?", which an ok-only counter cannot answer.
    #[tokio::test]
    async fn session_counters_attribute_a_rate_limited_publish() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = report_ctx(&db, true).await;
        // An empty bucket: the connection rate gate denies before the bus is
        // reached, which is the earliest of the counted sites.
        let mut bucket = TokenBucket::new(0, std::time::Duration::from_secs(60), 0);
        let mut counters = SessionCounters::default();

        let _ = handle_publish(
            &ctx,
            &mut bucket,
            report_request(Some("protobar")),
            &mut counters,
        )
        .await;

        assert_eq!(
            counters.by_instance.get("protobar"),
            Some(&InstancePublishCounters {
                publishes: 0,
                publish_rate_limited: 1,
            }),
        );
        assert_eq!(counters.publish_rate_limited, 1, "and the session total");
    }

    /// A kernel self-report (no subject) carries the bare surface identity.
    #[tokio::test]
    async fn report_without_subject_stamps_bare_surface_identity() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = report_ctx(&db, true).await;
        let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
        let mut counters = SessionCounters::default();

        let _ = handle_publish(&ctx, &mut bucket, report_request(None), &mut counters).await;

        let conn = db.lock().await;
        let sender: String = conn
            .query_row("SELECT sender FROM messaging_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            sender, "surface:deskbar",
            "a report with no component subject is the kernel's own"
        );
    }

    /// The claim surface, closed: a subject naming an instance outside the
    /// declared set is a protocol violation (kill + log), not a fallback to the
    /// bare surface identity — which would let a non-conforming client launder a
    /// component's reports onto the surface's own budget.
    #[tokio::test]
    async fn report_with_undeclared_subject_is_a_violation() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = report_ctx(&db, true).await;
        let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
        let mut counters = SessionCounters::default();

        let outcome = handle_publish(
            &ctx,
            &mut bucket,
            report_request(Some("never-declared")),
            &mut counters,
        )
        .await;
        assert!(
            matches!(outcome, FrameOutcome::Violation(_)),
            "an undeclared subject_instance must kill the connection"
        );

        // Nothing was published: the violation precedes the publish.
        let conn = db.lock().await;
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "a violating frame must publish nothing");
    }

    /// `subject_instance` is meaningless on an ordinary output port — a claim
    /// with nothing to claim — so it is a violation rather than a silently
    /// ignored field.
    #[tokio::test]
    async fn subject_instance_on_an_ordinary_port_is_a_violation() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = report_ctx(&db, true).await;
        let mut bucket = TokenBucket::new(60, std::time::Duration::from_secs(1), 60);
        let mut counters = SessionCounters::default();

        let outcome = handle_publish(
            &ctx,
            &mut bucket,
            PublishRequest {
                instance: "protobar",
                port: "out",
                body: REPORT_BODY,
                correlation: Some(4),
                subject_instance: Some("protobar"),
                urgency: None,
            },
            &mut counters,
        )
        .await;
        assert!(
            matches!(outcome, FrameOutcome::Violation(_)),
            "subject_instance outside the reserved port must violate"
        );
    }

    /// Insert one message on `channel_uuid`. Returns the message id.
    async fn seed_message(
        db: &brenn_lib::db::Db,
        channel_uuid: Uuid,
        body: &str,
        ts_ns: i64,
    ) -> i64 {
        use brenn_lib::messaging::db::insert_message;
        use brenn_lib::messaging::{ChannelScheme, Urgency};
        let conn = db.lock().await;
        insert_message(
            &conn,
            channel_uuid,
            "test",
            "sender",
            body,
            Urgency::Normal,
            ChannelScheme::Brenn,
            None,
            None,
            None,
            ts_ns,
        )
        .id
    }

    /// A durable `Deliver` carries a delivery-time span `seq` on the wire and its
    /// retention position inside the opaque `cursor` (the subscription
    /// high-water). The tests assert the delivered row by parsing that position,
    /// which for an in-order delivery is the delivered row's own.
    ///
    /// These fixtures seed one channel per DB, so a message's rowid and its
    /// retention position coincide and callers can name the row by either.
    fn expect_deliver(frame: ServerFrame, want_id: i64) {
        let target = sole_target(frame);
        match cursor::parse(&target.cursor) {
            Ok(state) => assert_eq!(state.seq, want_id as u64, "Deliver cursor high-water"),
            other => panic!("expected a parseable cursor for id {want_id}, got {other:?}"),
        }
    }

    /// The one target of a `Deliver` these tests expect. Each drives a single
    /// subscription, so a frame carrying more than one target would mean the
    /// coalescer folded in a subscription the test never opened.
    fn sole_target(frame: ServerFrame) -> DeliverTarget {
        match frame {
            ServerFrame::Deliver { mut targets, .. } => {
                assert_eq!(
                    targets.len(),
                    1,
                    "expected a single-target Deliver, got {targets:?}"
                );
                targets.remove(0)
            }
            other => panic!("expected a Deliver, got {other:?}"),
        }
    }

    /// The wire span `seq` of a `Deliver`, for the tests that assert per-span
    /// monotonicity of the delivery-boundary counter directly.
    fn expect_deliver_seq(frame: ServerFrame) -> u64 {
        sole_target(frame).seq
    }

    /// A durable row released below the current high-water advances the wire span
    /// seq but neither regresses the minted cursor nor moves the high-water — so
    /// the next reconnect resumes from the true high-water, not a below-water
    /// floor that would replay already-seen rows.
    #[test]
    fn next_durable_below_high_water_holds_cursor_but_advances_span_seq() {
        let mut spans = WireSpans::new();
        spans.set_incarnation(0);
        let sub = durable_sub();
        spans.start_durable_span(&sub, Uuid::nil(), 0);

        let (seq_hi, cursor_hi) = spans.next_durable(&sub, 10);
        assert_eq!(seq_hi, 1, "first durable span seq is 1");
        assert!(
            matches!(cursor::parse(&cursor_hi), Ok(state) if state.seq == 10),
            "cursor high-water tracks the delivered row",
        );

        let (seq_lo, cursor_lo) = spans.next_durable(&sub, 7);
        assert_eq!(seq_lo, 2, "span seq is monotone regardless of row order");
        assert!(
            matches!(cursor::parse(&cursor_lo), Ok(state) if state.seq == 10),
            "a below-water row keeps the cursor at the prior high-water",
        );
    }

    /// One live batch, three decisions, each made against the subscription's own
    /// high-water: the caught-up sibling takes the contiguous position live, the
    /// lagging one has its live copy dropped and is served its whole suffix from
    /// retention, and a position already written is dropped as the duplicate it
    /// is. Driven directly, so no drain nudge can heal a decision the arm got
    /// wrong.
    #[tokio::test]
    async fn a_live_batch_decides_each_sibling_against_its_own_high_water() {
        const BEHIND: &str = "protobar";
        const CURRENT: &str = "agenda";
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx_for(&db, Depth::Bounded(8), &[BEHIND, CURRENT]).await;
        let behind = SubKey {
            instance: BEHIND.to_string(),
            channel: DURABLE_ADDR.to_string(),
        };
        let current = SubKey {
            instance: CURRENT.to_string(),
            channel: DURABLE_ADDR.to_string(),
        };

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs);
        let mut counters = SessionCounters::default();
        let mut spans = WireSpans::new();

        // The laggard subscribes to an empty channel: its high-water anchors at 0
        // and nothing moves it.
        handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            behind.clone(),
            None,
            &mut counters,
        )
        .await;
        let _ = rx.try_recv().expect("the laggard's SubscribeResult");

        let m1 = seed_message(&db, uuid, "one", 100).await;
        let m2 = seed_message(&db, uuid, "two", 200).await;
        // The sibling subscribes after: its replay carries both rows, so its
        // high-water sits one below what comes next.
        handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            current.clone(),
            None,
            &mut counters,
        )
        .await;
        let _ = rx.try_recv().expect("the sibling's SubscribeResult");
        expect_deliver(rx.try_recv().expect("replay of one"), m1);
        expect_deliver(rx.try_recv().expect("replay of two"), m2);

        let m3 = seed_message(&db, uuid, "three", 300).await;
        let envelope = |body: &str| {
            Arc::new(MessageEnvelope {
                message_id: Uuid::new_v4(),
                source: "test".into(),
                channel: DURABLE_ADDR.to_string(),
                sender: "sender".into(),
                publish_ts: Utc::now(),
                body: body.to_string(),
                reply_to: None,
                delivery_deadline: None,
                deliver_after: None,
                urgency: Urgency::Normal,
                envelope_type: brenn_lib::messaging::ChannelScheme::Brenn,
            })
        };
        let outcome = send_durable_live(
            &ctx,
            &durable,
            &mut spans,
            vec![
                DurableDelivery {
                    envelope: envelope("three"),
                    retained_seq: m3 as u64,
                    sub: behind.clone(),
                },
                DurableDelivery {
                    envelope: envelope("three"),
                    retained_seq: m3 as u64,
                    sub: current.clone(),
                },
                DurableDelivery {
                    envelope: envelope("one"),
                    retained_seq: m1 as u64,
                    sub: current.clone(),
                },
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        // The contiguous sibling takes the live copy, under its own name.
        match rx.try_recv().expect("the live copy") {
            ServerFrame::Deliver {
                envelope, targets, ..
            } => {
                assert_eq!(envelope.body, "three");
                assert_eq!(
                    targets.len(),
                    1,
                    "only the caught-up sibling is a live target"
                );
                assert_eq!(targets[0].instance, CURRENT);
                assert!(
                    matches!(cursor::parse(&targets[0].cursor), Ok(state) if state.seq == m3 as u64)
                );
            }
            other => panic!("expected a Deliver, got {other:?}"),
        }
        // The laggard is served every position above its own high-water, in order.
        for (want_body, want_seq) in [("one", m1), ("two", m2), ("three", m3)] {
            match rx.try_recv().expect("the laggard's suffix") {
                ServerFrame::Deliver {
                    envelope, targets, ..
                } => {
                    assert_eq!(envelope.body, want_body);
                    assert_eq!(targets.len(), 1);
                    assert_eq!(targets[0].instance, BEHIND);
                    assert_eq!(
                        targets[0].dropped, 0,
                        "retention covered the whole suffix, so nothing was lost"
                    );
                    assert!(
                        matches!(cursor::parse(&targets[0].cursor), Ok(state) if state.seq == want_seq as u64)
                    );
                }
                other => panic!("expected a Deliver, got {other:?}"),
            }
        }
        assert!(
            rx.try_recv().is_err(),
            "the below-water copy is dropped as a duplicate, not re-sent"
        );
    }

    /// **One turn of the push queue becomes frames: durable rows first as one
    /// coalesced pass, then the views in arrival order.** The middle link of the
    /// view plane — the registry test stops at the queue and the kernel tests start
    /// at a serialized frame, so without this a view that never became a frame would
    /// leave every page mirror frozen at whatever the seeding pass supplied, with
    /// both neighbouring tests still green. The order is load-bearing too: an
    /// interleaved view would split one message's sibling rows across frames and
    /// break the coalescing `send_durable_live` exists to do.
    #[tokio::test]
    async fn a_turn_of_pushes_writes_the_rows_first_then_every_view_in_order() {
        const BEHIND: &str = "protobar";
        const CURRENT: &str = "agenda";
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx_for(&db, Depth::Bounded(8), &[BEHIND, CURRENT]).await;
        let behind = SubKey {
            instance: BEHIND.to_string(),
            channel: DURABLE_ADDR.to_string(),
        };
        let current = SubKey {
            instance: CURRENT.to_string(),
            channel: DURABLE_ADDR.to_string(),
        };

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs);
        let mut counters = SessionCounters::default();
        let mut spans = WireSpans::new();
        for sub in [&behind, &current] {
            handle_durable_subscribe(
                &ctx,
                &mut durable,
                &mut spans,
                sub.clone(),
                None,
                &mut counters,
            )
            .await;
            let _ = rx.try_recv().expect("SubscribeResult");
        }

        let seq = seed_message(&db, uuid, "shared", 100).await;
        let envelope = Arc::new(MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: "test".into(),
            channel: DURABLE_ADDR.to_string(),
            sender: "sender".into(),
            publish_ts: Utc::now(),
            body: "shared".to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: brenn_lib::messaging::ChannelScheme::Brenn,
        });
        let view = |body: &str| {
            SessionPush::DeferredView(DeferredViewPush {
                channel: DURABLE_ADDR.to_string(),
                instance: BEHIND.to_string(),
                entries: vec![DeferredViewEntry {
                    message_id: Uuid::nil(),
                    body: body.to_string(),
                    deliver_after: 1_700_000_000_000,
                }],
            })
        };

        // A view arrives between the two sibling rows of one message: exactly the
        // interleaving that must not reach the writer.
        let outcome = send_session_pushes(
            &ctx,
            &durable,
            &mut spans,
            vec![
                SessionPush::Durable(DurableDelivery {
                    envelope: Arc::clone(&envelope),
                    retained_seq: seq as u64,
                    sub: behind.clone(),
                }),
                view("first"),
                SessionPush::Durable(DurableDelivery {
                    envelope: Arc::clone(&envelope),
                    retained_seq: seq as u64,
                    sub: current.clone(),
                }),
                view("second"),
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        match rx.try_recv().expect("the coalesced delivery") {
            ServerFrame::Deliver {
                envelope, targets, ..
            } => {
                assert_eq!(envelope.body, "shared");
                let mut named: Vec<String> = targets.iter().map(|t| t.instance.clone()).collect();
                named.sort();
                assert_eq!(
                    named,
                    vec![CURRENT.to_string(), BEHIND.to_string()],
                    "both sibling rows rode one frame: the view did not split them"
                );
            }
            other => panic!("expected the rows first, got {other:?}"),
        }
        for want in ["first", "second"] {
            match rx.try_recv().expect("a view frame") {
                ServerFrame::DeferredView {
                    channel,
                    instance,
                    entries,
                } => {
                    assert_eq!(channel, DURABLE_ADDR);
                    assert_eq!(instance, BEHIND);
                    assert_eq!(
                        entries.iter().map(|e| e.body.as_str()).collect::<Vec<_>>(),
                        vec![want],
                        "views follow the rows, in arrival order — the last one is what \
                         the page keeps"
                    );
                }
                other => panic!("expected a DeferredView, got {other:?}"),
            }
        }
        assert!(
            rx.try_recv().is_err(),
            "one turn, four pushes, three frames"
        );
    }

    /// An unparseable resume cursor is a protocol violation whose detail names the
    /// parse cause — the fail2ban-relevant mapping the class-mismatch tests do not
    /// exercise.
    #[test]
    fn parse_resume_cursor_unparseable_is_violation_with_cause() {
        let bogus: Cursor =
            serde_json::from_value(serde_json::Value::String("not-a-cursor".into())).unwrap();
        match parse_resume_cursor(&bogus, "slug", "user", "chan") {
            Err(FrameOutcome::Violation(detail)) => {
                assert!(
                    detail.contains("unparseable resume cursor"),
                    "violation names the cause: {detail}"
                );
            }
            Err(_) => panic!("expected a Violation outcome, got a different outcome"),
            Ok(state) => panic!("expected a Violation, parsed to {state:?}"),
        }
    }

    /// The durable delivery path mints a per-span `seq` that starts at 1 and
    /// strictly increases, minted at the socket-write boundary like the ephemeral
    /// path — a constant or zero durable span seq is a bug this pins.
    #[tokio::test]
    async fn durable_deliver_span_seq_starts_at_one_and_is_monotone() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let _ = seed_message(&db, uuid, "one", 100).await;
        let _ = seed_message(&db, uuid, "two", 200).await;

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs.clone());
        let mut counters = SessionCounters::default();
        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            None,
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        let _ = rx.try_recv().expect("SubscribeResult");
        let s1 = expect_deliver_seq(rx.try_recv().expect("first delivery"));
        let s2 = expect_deliver_seq(rx.try_recv().expect("second delivery"));
        assert_eq!(s1, 1, "durable span seq starts at 1");
        assert!(s2 > s1, "durable span seq strictly increases across replay");
    }

    /// A fresh durable subscribe replays the retained window in seq order behind
    /// a `SubscribeResult{Ok}` and activates the shared/local sets. It writes
    /// nothing: the client's cursor, minted onto each `Deliver`, is the whole of
    /// the subscription's delivery state.
    #[tokio::test]
    async fn durable_subscribe_fresh_replays_the_retained_window() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let m2 = seed_message(&db, uuid, "two", 200).await;

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs.clone());
        let mut counters = SessionCounters::default();

        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            None,
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        match rx.try_recv().expect("SubscribeResult") {
            ServerFrame::SubscribeResult {
                outcome,
                replay_count,
                gap,
                ..
            } => {
                assert!(matches!(outcome, SubscribeOutcome::Ok));
                assert_eq!(replay_count, 2);
                assert!(gap.is_none());
            }
            other => panic!("expected SubscribeResult, got {other:?}"),
        }
        expect_deliver(rx.try_recv().expect("first delivery"), m1);
        expect_deliver(rx.try_recv().expect("second delivery"), m2);
        assert!(rx.try_recv().is_err(), "no frames beyond the backlog");

        // Activation is visible to the router (shared set) and the local mirror.
        assert!(durable.is_active(&durable_sub()));
        assert!(durable_subs.lock().unwrap().contains(&durable_sub()));
        // The replay reads retention and writes nothing at all.
        let conn = db.lock().await;
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM messaging_pending_pushes", [], |row| {
                row.get(0)
            })
            .expect("count pending pushes");
        assert_eq!(rows, 0, "a durable subscribe writes no row");
    }

    /// `Resume::Durable` replays the retained window (`id > last_seq`) with no gap
    /// when the window covers, oldest-first.
    #[tokio::test]
    async fn durable_subscribe_resume_replays_retained_window() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let m2 = seed_message(&db, uuid, "two", 200).await;
        let m3 = seed_message(&db, uuid, "three", 300).await;

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs.clone());
        let mut counters = SessionCounters::default();

        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            Some(durable_resume(&db, m1).await),
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        match rx.try_recv().expect("SubscribeResult") {
            ServerFrame::SubscribeResult {
                replay_count, gap, ..
            } => {
                assert_eq!(replay_count, 2, "m2 and m3 re-sent");
                assert!(gap.is_none(), "window covered last_seq = m1");
            }
            other => panic!("expected SubscribeResult, got {other:?}"),
        }
        expect_deliver(rx.try_recv().expect("m2"), m2);
        expect_deliver(rx.try_recv().expect("m3"), m3);
        assert!(rx.try_recv().is_err());
    }

    /// A retain clamp that truncates the resumable suffix yields a
    /// `BeyondRetained` gap alongside the (clamped) replay.
    #[tokio::test]
    async fn durable_subscribe_resume_truncated_window_gaps() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(1)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let _m2 = seed_message(&db, uuid, "two", 200).await;
        let m3 = seed_message(&db, uuid, "three", 300).await;

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs.clone());
        let mut counters = SessionCounters::default();

        // Resume from m1: the store answers `Exact` with the two rows above it, and
        // the binding's clamp of 1 drops m2 — rows this subscription is owed and
        // will not receive, which is what the gap reports.
        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            Some(durable_resume(&db, m1).await),
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        match rx.try_recv().expect("SubscribeResult") {
            ServerFrame::SubscribeResult {
                replay_count, gap, ..
            } => {
                assert_eq!(replay_count, 1, "clamp keeps newest 1");
                assert_eq!(
                    gap.expect("truncation gap").reason,
                    ProtoGapReason::BeyondRetained
                );
            }
            other => panic!("expected SubscribeResult, got {other:?}"),
        }
        expect_deliver(rx.try_recv().expect("m3"), m3);
        assert!(rx.try_recv().is_err());
    }

    /// A durable cursor anchored at position 0 on a non-empty channel is a
    /// resumable position — retention positions start at 1, so the whole window is
    /// its suffix. It replays that window clamped to `retain_depth`, and the
    /// truncation is what reports `BeyondRetained`.
    #[tokio::test]
    async fn durable_subscribe_snapshot_last_seq_zero_replays_window_with_gap() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(1)).await;
        let _m1 = seed_message(&db, uuid, "one", 100).await;
        let m2 = seed_message(&db, uuid, "two", 200).await;

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs.clone());
        let mut counters = SessionCounters::default();

        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            Some(durable_resume_at(&db, uuid, 0).await),
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        match rx.try_recv().expect("SubscribeResult") {
            ServerFrame::SubscribeResult {
                replay_count, gap, ..
            } => {
                assert_eq!(replay_count, 1, "clamp to retain_depth = 1 keeps newest");
                assert_eq!(
                    gap.expect("the clamp drops m1, which the cursor was owed")
                        .reason,
                    ProtoGapReason::BeyondRetained
                );
            }
            other => panic!("expected SubscribeResult, got {other:?}"),
        }
        expect_deliver(rx.try_recv().expect("m2"), m2);
        assert!(rx.try_recv().is_err());
    }

    /// A position-0 resume anchor on an *empty* channel (fresh install) is
    /// `UpToDate`, not a gap: the channel has assigned no position, so 0 is
    /// exactly its high-water and the client has missed nothing. No rows, no
    /// gap, so a brand-new bar shows no false staleness warning.
    #[tokio::test]
    async fn durable_subscribe_snapshot_last_seq_zero_empty_channel_is_uptodate() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(1)).await;

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs.clone());
        let mut counters = SessionCounters::default();

        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            Some(durable_resume_at(&db, uuid, 0).await),
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        match rx.try_recv().expect("SubscribeResult") {
            ServerFrame::SubscribeResult {
                replay_count, gap, ..
            } => {
                assert_eq!(replay_count, 0, "empty channel replays nothing");
                assert!(
                    gap.is_none(),
                    "position 0 is the empty channel's high-water, so nothing was missed"
                );
            }
            other => panic!("expected SubscribeResult, got {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    /// Read the store identity for a test db.
    async fn store_id(db: &brenn_lib::db::Db) -> db::StoreIdentity {
        let conn = db.lock().await;
        db::read_store_identity(&conn)
    }

    /// Drive a durable subscribe with `resume` and assert it was answered as a
    /// fresh attach against the retained window plus an `EpochChanged` gap — the
    /// stale-store answer. Returns the replayed retention positions in delivery
    /// order.
    async fn assert_stale_store_fresh_attach(
        ctx: &SessionCtx,
        rx: &mut mpsc::Receiver<ServerFrame>,
        resume: Cursor,
        want_replay: u32,
    ) -> Vec<u64> {
        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs);
        let mut counters = SessionCounters::default();
        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            Some(resume),
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        match rx.try_recv().expect("SubscribeResult") {
            ServerFrame::SubscribeResult {
                replay_count, gap, ..
            } => {
                assert_eq!(replay_count, want_replay, "retained-window replay count");
                assert_eq!(
                    gap.expect("stale-store cursor gaps").reason,
                    ProtoGapReason::EpochChanged,
                    "a stale-store cursor is answered as a fresh attach + EpochChanged",
                );
            }
            other => panic!("expected SubscribeResult, got {other:?}"),
        }
        let mut ids = Vec::new();
        while let Ok(ServerFrame::Deliver { targets, .. }) = rx.try_recv() {
            for target in targets {
                match cursor::parse(&target.cursor) {
                    Ok(state) => ids.push(state.seq),
                    other => panic!("expected a parseable Deliver cursor, got {other:?}"),
                }
            }
        }
        ids
    }

    /// A cursor bearing an epoch this channel never minted (the messaging DB was
    /// replaced under a live page, so every channel row — and every epoch — is
    /// new) is answered as a fresh attach + `EpochChanged`, never silence, never a
    /// violation. The store answers it from the cursor alone: no wire-layer
    /// generation field is involved.
    #[tokio::test]
    async fn durable_resume_foreign_epoch_is_fresh_attach() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let _m1 = seed_message(&db, uuid, "one", 100).await;
        let _m2 = seed_message(&db, uuid, "two", 200).await;
        let incarnation = store_id(&db).await.incarnation;

        // A cursor at a real position, in a numbering domain this store never
        // assigned.
        let stale = cursor::mint(incarnation, Uuid::new_v4(), 2);
        // retain_depth 8 covers both seeded rows.
        let ids = assert_stale_store_fresh_attach(&ctx, &mut rx, stale, 2).await;
        assert_eq!(ids, vec![1, 2], "fresh window replays the retained rows");
    }

    /// A cursor whose incarnation is above the store's current one — the DB was
    /// restored from backup and the cursor was minted under a boot the restored
    /// store never counted, so its positions may name different messages now — is
    /// answered as a fresh attach. The one staleness question the store cursor
    /// cannot answer, which is why the session keeps it.
    #[tokio::test]
    async fn durable_resume_incarnation_above_store_is_fresh_attach() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let incarnation = store_id(&db).await.incarnation;
        let epoch = {
            let conn = db.lock().await;
            db::channel_resume_epoch(&conn, uuid)
        };
        let _ = m1;

        let stale = cursor::mint(incarnation + 1, epoch, 1);
        let ids = assert_stale_store_fresh_attach(&ctx, &mut rx, stale, 1).await;
        assert_eq!(ids, vec![1]);
    }

    /// A cursor above the channel's high-water — the store was restored from
    /// backup and re-climbed its sequence past the cursor before this resume, so
    /// the incarnation check does not catch it — is escalated as a fresh attach +
    /// `EpochChanged`. The connection survives: an honest client reaches this.
    #[tokio::test]
    async fn durable_resume_above_high_water_is_fresh_attach_not_a_kill() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let _ = m1;

        let ahead = durable_resume_at(&db, uuid, 1_000_000).await;
        let ids = assert_stale_store_fresh_attach(&ctx, &mut rx, ahead, 1).await;
        assert_eq!(ids, vec![1]);
    }

    /// An ordinary messenger teardown/rebuild on the same DB bumps the incarnation
    /// exactly once and trips no staleness arm: a cursor minted before the rebuild
    /// carries an incarnation *below* the store's new one, so it resumes normally.
    #[tokio::test]
    async fn durable_resume_after_ordinary_rebuild_is_not_stale() {
        use brenn_lib::messaging::config::MessagingGlobalConfig;
        use brenn_lib::messaging::{
            MessagingDirectory, Messenger, WakeRouter, query::NoopWakeRouter,
        };

        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let m2 = seed_message(&db, uuid, "two", 200).await;
        // The cursor the live page holds, minted at the current (pre-rebuild)
        // incarnation.
        let id_before = store_id(&db).await;
        let resume = durable_resume(&db, m1).await;

        // Simulate a server restart on the same DB: a second Messenger boot bumps
        // the incarnation once, generation unchanged.
        let _rebuilt = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![])),
            Arc::from(TEST_ORIGIN),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        let id_after = store_id(&db).await;
        assert_eq!(
            id_after.generation, id_before.generation,
            "generation stable"
        );
        assert_eq!(
            id_after.incarnation,
            id_before.incarnation + 1,
            "one rebuild bumps incarnation once",
        );

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs);
        let mut counters = SessionCounters::default();
        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            Some(resume),
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        match rx.try_recv().expect("SubscribeResult") {
            ServerFrame::SubscribeResult {
                replay_count, gap, ..
            } => {
                assert_eq!(replay_count, 1, "ordinary resume replays id > m1");
                assert!(
                    gap.is_none(),
                    "an ordinary rebuild is not a stale-store event",
                );
            }
            other => panic!("expected SubscribeResult, got {other:?}"),
        }
        expect_deliver(rx.try_recv().expect("m2"), m2);
        assert!(rx.try_recv().is_err());
    }

    // ── DurableSessionState ───────────────────────────────────────────────

    /// A `SubKey` for `instance`'s subscription on `brenn:c`.
    fn sk(instance: &str) -> SubKey {
        SubKey {
            instance: instance.to_string(),
            channel: "brenn:c".to_string(),
        }
    }

    /// `activate`/`deactivate` move the local and registry-shared active sets
    /// together, and `deactivate` returns whether the subscription was active
    /// (the Unsubscribe-of-non-active violation check).
    #[test]
    fn durable_session_state_activate_deactivate_syncs_shared_set() {
        let shared = Arc::new(Mutex::new(HashSet::new()));
        let mut st = DurableSessionState::new(shared.clone());
        assert!(!st.is_active(&sk("a")));

        st.activate(&sk("a"));
        assert!(st.is_active(&sk("a")));
        assert!(shared.lock().unwrap().contains(&sk("a")));

        assert!(st.deactivate(&sk("a")), "was active");
        assert!(!st.is_active(&sk("a")));
        assert!(!shared.lock().unwrap().contains(&sk("a")));

        // A second deactivate of a non-active subscription is false.
        assert!(!st.deactivate(&sk("a")));
    }

    /// Sibling instances on one channel are independent subscriptions: one's
    /// activation must not make the other's look active, and unsubscribing one
    /// must not tear down the other. Keyed by channel alone (the old shape),
    /// every assertion here inverts.
    #[test]
    fn durable_session_state_keeps_sibling_instances_independent() {
        let shared = Arc::new(Mutex::new(HashSet::new()));
        let mut st = DurableSessionState::new(shared.clone());

        st.activate(&sk("agenda-alice"));
        assert!(st.is_active(&sk("agenda-alice")));
        assert!(
            !st.is_active(&sk("agenda-bob")),
            "bob never subscribed; alice's subscription is not his"
        );
        assert!(
            !st.deactivate(&sk("agenda-bob")),
            "unsubscribing bob's non-existent subscription is not-active, not a silent hit on \
             alice's"
        );
        assert!(st.is_active(&sk("agenda-alice")), "alice survives");

        st.activate(&sk("agenda-bob"));
        assert_eq!(
            shared.lock().unwrap().len(),
            2,
            "two principals, two entries"
        );
    }

    // ── Subscribe/Unsubscribe rate bucket ─────────────────────────────────

    /// The Subscribe/Unsubscribe bucket admits exactly `SUBSCRIBE_BURST` frames —
    /// a maximum-size surface's first-connect reconcile plus one full
    /// detach/re-attach cycle — then trips a protocol violation on the next frame.
    #[tokio::test]
    async fn subscribe_bucket_admits_burst_then_violates() {
        let (dispatcher, _drainer) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let ctx = alert_ctx(false, dispatcher);
        let mut bucket = TokenBucket::new(SUBSCRIBE_BURST, SUBSCRIBE_REFILL, 1);

        for _ in 0..SUBSCRIBE_BURST {
            assert!(charge_subscribe_token(&ctx, &mut bucket).is_ok());
        }
        assert!(matches!(
            charge_subscribe_token(&ctx, &mut bucket),
            Err(FrameOutcome::Violation(_))
        ));
    }

    // ── PublishBatch: one activation's flush ────────────────────────────────

    /// The backend consumer registered on `ephemeral:batch-eph` in
    /// [`batch_ctx`], at `metered` noise: the subscriber a surface publish's ring
    /// eviction is charged and escalated against.
    const BATCH_EPH_CONSUMER: &str = "eph-lagger";

    /// A [`SessionCtx`] whose runtime declares instance `protobar` with a durable
    /// output (`out` → `brenn:batch-out`) and an ephemeral one (`eph` →
    /// `ephemeral:batch-eph`), backed by a real in-memory `Messenger` with a
    /// budget installed for both principal grains. The two classes together are
    /// what the batch's split-and-apply step exists for.
    async fn batch_ctx(db: &brenn_lib::db::Db) -> (SessionCtx, mpsc::Receiver<ServerFrame>) {
        batch_ctx_shaped(db, None, &[]).await
    }

    /// [`batch_ctx`] with an explicit `retain_depth` on the durable channel,
    /// which is also its deferred cap — the one fixture knob the schedule-refusal
    /// cases need. `None` leaves the resolved default.
    async fn batch_ctx_at_durable_retain(
        db: &brenn_lib::db::Db,
        durable_retain_depth: Option<u64>,
    ) -> (SessionCtx, mpsc::Receiver<ServerFrame>) {
        batch_ctx_shaped(db, durable_retain_depth, &[]).await
    }

    /// The address of [`batch_ctx_with_undeclared_output`]'s third output: a
    /// `brenn:` channel the fixture's directory does not hold.
    const UNDECLARED_OUT_ADDR: &str = "brenn:nonesuch";

    /// [`batch_ctx`] plus a third bound output on [`UNDECLARED_OUT_ADDR`], which
    /// the fixture's two-entry directory cannot resolve — the configuration a
    /// booted server refuses (`bootstrap::messaging::surfaces` asserts channel
    /// existence on every transportable output), reachable here because
    /// `SurfaceRuntime::build` validates no output address.
    ///
    /// It is the only shape that tells enumeration from resolution apart: every
    /// other fixture output resolves, so a filter over the directory would return
    /// the same targets as no filter at all.
    async fn batch_ctx_with_undeclared_output(
        db: &brenn_lib::db::Db,
    ) -> (SessionCtx, mpsc::Receiver<ServerFrame>) {
        batch_ctx_shaped(db, None, &[UNDECLARED_OUT_ADDR]).await
    }

    /// The assembly behind the `batch_ctx` family: `durable_retain_depth` tunes
    /// the durable channel's retain/deferred cap, and each address in
    /// `extra_outputs` binds one more `protobar` output port (`extra0`, `extra1`,
    /// …) without declaring a channel for it.
    async fn batch_ctx_shaped(
        db: &brenn_lib::db::Db,
        durable_retain_depth: Option<u64>,
        extra_outputs: &[&str],
    ) -> (SessionCtx, mpsc::Receiver<ServerFrame>) {
        use brenn_lib::messaging::config::{
            ChannelConfigRaw, Depth, MessagingGlobalConfig, NoiseLevel, SurfaceSendBudget,
            build_channel_entries,
        };
        use brenn_lib::messaging::store::RingStores;
        use brenn_lib::messaging::testutils::{ephemeral_channel_entry, surface_registrations};
        use brenn_lib::messaging::{
            MessagingDirectory, Messenger, SubscriberEntry, SubscriberEntryKind, WakeRouter,
            query::NoopWakeRouter,
        };

        let raw = ChannelConfigRaw {
            send_rate: None,
            uuid: Some(Uuid::new_v4().to_string()),
            address: "batch-out".to_string(),
            description: None,
            push_depth: None,
            retain_depth: durable_retain_depth.map(Depth::Bounded),
            standing_retain_depth: None,
            noise: None,
            sink: None,
            wake_min: None,
        };
        let entry = build_channel_entries(&[raw], &MessagingGlobalConfig::default())
            .pop()
            .expect("one channel entry");
        {
            let conn = db.lock().await;
            brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&entry));
        }

        let mut policy = AppPolicy::default();
        policy.grants.insert(AppCapability::MessagingPublish);
        policy.grants.insert(AppCapability::EphemeralPublish);
        // The test observes the ephemeral fan-out through an ordinary
        // subscription, which is its own grant + ACL pair.
        policy.grants.insert(AppCapability::EphemeralSubscribe);
        policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Prefix(String::new()));
        policy
            .acls
            .ephemeral_publish
            .push(ChannelMatcher::Prefix(String::new()));
        policy
            .acls
            .ephemeral_subscribe
            .push(ChannelMatcher::Prefix(String::new()));
        let mut surface_policies = std::collections::HashMap::new();
        surface_policies.insert("deskbar".to_string(), policy.clone());

        // Boot's shape: the ephemeral channel is a member of the one directory,
        // its store is the registry's, and it carries a backend consumer's
        // registration — the rung a ring eviction escalates against.
        let mut eph = ephemeral_channel_entry("batch-eph", 4);
        eph.subscribers.push(SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(BATCH_EPH_CONSUMER.to_string()),
            push_depth: Depth::Bounded(4),
            retain_depth: Depth::Bounded(4),
            noise: NoiseLevel::Metered,
            wake_min: None,
        });
        let ring_stores = Arc::new(RingStores::build(std::slice::from_ref(&eph)));

        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![entry, eph])),
            Arc::from(TEST_ORIGIN),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_ring_stores(ring_stores)
        .with_subscriber_registrations(surface_registrations(surface_policies))
        .with_surface_send_budgets([(
            "deskbar".to_string(),
            vec![
                (None, SurfaceSendBudget::default()),
                (Some("protobar".to_string()), SurfaceSendBudget::default()),
            ],
        )]);

        let mut fixture = crate::test_support::surface::SurfaceFixture::new("deskbar", "protobar")
            .output("brenn:batch-out", "protobar", "out")
            .output("ephemeral:batch-eph", "protobar", "eph");
        for (i, address) in extra_outputs.iter().enumerate() {
            fixture = fixture.output(address, "protobar", &format!("extra{i}"));
        }
        let resolved = fixture.policy(policy).build();
        let runtime = SurfaceRuntime::build(
            resolved,
            Some(Arc::clone(&messenger)),
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );

        let (alert_dispatcher, _drainer) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let (tx, rx) = mpsc::channel::<ServerFrame>(64);
        let ctx = SessionCtx {
            runtime: Arc::new(runtime),
            session_id: Uuid::nil(),
            username: "dev".to_string(),
            ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            alert_dispatcher,
            registry: SurfaceRegistry::default(),
            tx,
        };
        (ctx, rx)
    }

    /// One batch entry naming `port`, no urgency override, published now.
    fn entry(port: &str, body: &str) -> BatchEntry {
        BatchEntry {
            port: port.to_string(),
            body: body.to_string(),
            urgency: None,
            deliver_after: None,
        }
    }

    /// One batch entry naming `port`, scheduled for `deliver_after` epoch-ms.
    fn deferred_entry(port: &str, body: &str, deliver_after: u64) -> BatchEntry {
        BatchEntry {
            deliver_after: Some(deliver_after),
            ..entry(port, body)
        }
    }

    /// **The ordering contract, across the class boundary.** A mixed-class batch's
    /// entries carry strictly increasing publish timestamps in call order —
    /// observed where the contract is observable, on the delivered envelopes'
    /// `publish_ts` at ns precision, on both substrates. The stamps are assigned
    /// in one pass before the split, so this holds even though the two halves
    /// commit in different substrates at different instants; a stamp minted per
    /// substrate could order each half against itself and nothing more.
    #[tokio::test]
    async fn a_mixed_class_batch_is_stamped_in_call_order_across_the_boundary() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        let mut sub = ctx
            .runtime
            .messenger()
            .attach_live(
                ctx.runtime.participant.clone(),
                ctx.runtime.policy.clone(),
                "ephemeral:batch-eph",
                None,
            )
            .expect("ephemeral subscribe")
            .receiver;

        // Interleaved on purpose: the boundary is crossed twice, so a per-half
        // stamp would be caught in either direction.
        let outcome = flush_batch(
            &ctx,
            "protobar",
            1,
            &[
                entry("out", "d0"),
                entry("eph", "e1"),
                entry("out", "d2"),
                entry("eph", "e3"),
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        // Durable stamps, as persisted.
        let conn = db.lock().await;
        let durable: Vec<(String, i64)> = conn
            .prepare("SELECT body, publish_ts_ns FROM messaging_messages")
            .unwrap()
            .query_map([], |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        drop(conn);

        // Ephemeral stamps, off the delivered envelopes.
        let mut stamps: Vec<(String, i64)> = durable;
        for _ in 0..2 {
            let d = match sub.recv().await.expect("ephemeral delivery") {
                brenn_lib::messaging::EphemeralEvent::Delivery(d) => d,
                other => panic!("expected a delivery, got {other:?}"),
            };
            stamps.push((
                d.envelope.body.clone(),
                brenn_lib::messaging::db::utc_to_ns(d.envelope.publish_ts),
            ));
        }

        stamps.sort_by_key(|(_, ts)| *ts);
        let order: Vec<&str> = stamps.iter().map(|(b, _)| b.as_str()).collect();
        assert_eq!(
            order,
            vec!["d0", "e1", "d2", "e3"],
            "sorting the whole batch by publish_ts recovers call order across the classes"
        );
        let ts: Vec<i64> = stamps.iter().map(|(_, t)| *t).collect();
        assert!(
            ts.windows(2).all(|w| w[0] < w[1]),
            "strictly increasing, not merely non-decreasing: {ts:?}"
        );
    }

    /// **The send-rate gate is never consulted on the batch path.** The batch
    /// paid once, whole, at admission; a second bucket metering per entry
    /// afterwards could only lose a wide flush's tail under an `Ok`. Driven with
    /// a flush wider than the default burst — the case that loses entries if the
    /// gate is in the path at all — and pinned on the messenger's rate-limit
    /// counter, which is the gate's only fingerprint.
    #[tokio::test]
    async fn the_send_rate_gate_is_never_consulted_on_the_batch_path() {
        use brenn_lib::messaging::config::SendRate;

        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        // Wider than the default send-rate burst, and still a conforming flush
        // the instance's backstop admits whole.
        let n = SendRate::default().burst as usize + 8;
        assert!(
            n <= MAX_PUBLISHES_PER_ACTIVATION,
            "still a conforming flush"
        );
        let wide: Vec<BatchEntry> = (0..n).map(|_| entry("eph", "x")).collect();

        let outcome = flush_batch(&ctx, "protobar", 5, &wide, &mut counters).await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert!(
            matches!(
                rx.try_recv().expect("batch result"),
                ServerFrame::PublishBatchResult {
                    outcome: PublishBatchOutcome::Ok,
                    ..
                }
            ),
            "the flush is answered Ok"
        );
        assert_eq!(
            ctx.runtime
                .messenger
                .as_ref()
                .expect("batch fixture wires a messenger")
                .publish_rate_limited_count(ctx.runtime.participant.as_str()),
            0,
            "the gate's counter never moved — it was not in the path"
        );
        assert_eq!(
            counters.by_instance["protobar"].publishes, n as u64,
            "every entry landed; an Ok that lost its tail is the bug this rules out"
        );
        assert_eq!(
            counters.publish_rate_limited, 0,
            "nothing was rate-limited below the admission decision"
        );
    }

    /// A surface's bound-output publish is an eviction source like any other, and
    /// the ring books an eviction as reported the moment it happens. The batch
    /// path must therefore route its overflow to the noise ladder itself: a
    /// discarded outcome is a drop no consumer take will ever report, so a lagging
    /// backend consumer would be silently starved by a page.
    #[tokio::test]
    async fn publish_batch_enacts_the_ring_overflow_it_causes() {
        use brenn_lib::messaging::{ParticipantId, store::Priming};

        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        let messenger = ctx
            .runtime
            .messenger
            .as_ref()
            .expect("batch fixture wires a messenger");
        let consumer = ParticipantId::for_wasm(BATCH_EPH_CONSUMER);
        let channel = messenger
            .directory()
            .resolve("ephemeral:batch-eph")
            .expect("the ephemeral channel is a directory member");
        messenger.attach_ring_subscriber(&channel.uuid, &consumer, 4, Priming::Head);

        // Six into a depth-4 ring: the last two overwrite messages the consumer,
        // which never runs, is still owed.
        let flush: Vec<BatchEntry> = (0..6).map(|_| entry("eph", "x")).collect();
        let outcome = flush_batch(&ctx, "protobar", 9, &flush, &mut counters).await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert!(
            matches!(
                rx.try_recv().expect("batch result"),
                ServerFrame::PublishBatchResult {
                    outcome: PublishBatchOutcome::Ok,
                    ..
                }
            ),
            "the flush is answered Ok"
        );

        assert_eq!(
            messenger.drop_counter("ephemeral:batch-eph", &consumer),
            2,
            "both evicted-while-owed messages are metered — the ladder saw the \
             drops this publish caused"
        );
    }

    /// The happy path, both classes: durable entries commit in call order under
    /// the instance sub-identity, the ephemeral entry fans out, and the batch is
    /// answered `Ok` on its correlation.
    #[tokio::test]
    async fn publish_batch_applies_both_classes_and_answers_ok() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        let mut sub = ctx
            .runtime
            .messenger()
            .attach_live(
                ctx.runtime.participant.clone(),
                ctx.runtime.policy.clone(),
                "ephemeral:batch-eph",
                None,
            )
            .expect("ephemeral subscribe")
            .receiver;

        let outcome = flush_batch(
            &ctx,
            "protobar",
            77,
            &[entry("out", "a"), entry("eph", "e"), entry("out", "b")],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        match rx.try_recv().expect("PublishBatchResult frame") {
            ServerFrame::PublishBatchResult {
                correlation,
                outcome,
            } => {
                assert_eq!(correlation, 77, "the correlation round-trips");
                assert_eq!(outcome, PublishBatchOutcome::Ok);
            }
            other => panic!("expected PublishBatchResult, got {other:?}"),
        }

        // The durable half: both rows, in call order, under the sub-identity.
        let conn = db.lock().await;
        let rows: Vec<(String, String)> = conn
            .prepare("SELECT body, sender FROM messaging_messages ORDER BY publish_ts_ns")
            .unwrap()
            .query_map([], |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows,
            vec![
                ("a".to_string(), "surface:deskbar#protobar".to_string()),
                ("b".to_string(), "surface:deskbar#protobar".to_string()),
            ],
            "durable entries commit in call order under the instance sub-identity"
        );
        drop(conn);

        // The ephemeral half reached the bus.
        match sub.recv().await.expect("ephemeral delivery") {
            EphemeralEvent::Delivery(d) => assert_eq!(
                d.envelope.body, "e",
                "the ephemeral entry fanned out with its own body"
            ),
            other => panic!("expected a delivery, got {other:?}"),
        }

        assert_eq!(counters.publishes, 3, "all three entries counted Ok");
    }

    /// Ten minutes out, in the epoch-ms a `BatchEntry` carries.
    fn later_ms() -> u64 {
        u64::try_from((Utc::now() + chrono::Duration::minutes(10)).timestamp_millis())
            .expect("a positive epoch")
    }

    /// **Deferral crosses the wire and parks at each class's retention
    /// authority.** The durable entry's row carries a release time and no
    /// retention position, so nothing that reads retention can see it; the
    /// ephemeral entry is in its ring's deferred set, so the live consumer gets
    /// only the sibling that published now. Both are counted as publishes — the
    /// schedule landed, which is what the component asked for.
    #[tokio::test]
    async fn a_deferred_batch_entry_parks_on_both_classes_and_delivers_neither_now() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();

        let mut sub = ctx
            .runtime
            .messenger()
            .attach_live(
                ctx.runtime.participant.clone(),
                ctx.runtime.policy.clone(),
                "ephemeral:batch-eph",
                None,
            )
            .expect("ephemeral subscribe")
            .receiver;

        let outcome = flush_batch(
            &ctx,
            "protobar",
            9,
            &[
                deferred_entry("out", "d-later", later),
                deferred_entry("eph", "e-later", later),
                entry("eph", "e-now"),
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert!(matches!(
            rx.try_recv().expect("PublishBatchResult frame"),
            ServerFrame::PublishBatchResult {
                outcome: PublishBatchOutcome::Ok,
                ..
            }
        ));

        let conn = db.lock().await;
        let rows: Vec<(String, bool, Option<i64>)> = conn
            .prepare("SELECT body, deliver_after IS NOT NULL, retained_seq FROM messaging_messages")
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get(0).unwrap(),
                    r.get::<_, bool>(1).unwrap(),
                    r.get(2).unwrap(),
                ))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows,
            vec![("d-later".to_string(), true, None)],
            "the durable entry is parked: a release time and no retention position"
        );
        drop(conn);

        match sub.recv().await.expect("ephemeral delivery") {
            EphemeralEvent::Delivery(d) => assert_eq!(
                d.envelope.body, "e-now",
                "only the unscheduled ephemeral entry is on the channel"
            ),
            other => panic!("expected a delivery, got {other:?}"),
        }
        assert_eq!(counters.publishes, 3, "three entries landed");
    }

    /// Attach one session to `ctx`'s registry under the fixture slug, returning
    /// its guard (keep it alive — dropping it unregisters) and the push queue the
    /// deferred-view fan-out writes into.
    fn attach_view_session(
        ctx: &SessionCtx,
    ) -> (
        crate::routes::surface::registry::SurfaceSessionGuard,
        mpsc::Receiver<SessionPush>,
    ) {
        use crate::routes::surface::registry::{
            PUSH_QUEUE_FRAMES, SessionCaps, SurfaceSessionHandle,
        };

        let (push_tx, push_rx) = mpsc::channel(PUSH_QUEUE_FRAMES);
        let mut handle = SurfaceSessionHandle::for_test("dev");
        handle.push_tx = push_tx;
        let guard = ctx
            .registry
            .try_register(&ctx.runtime.resolved.slug, handle, SessionCaps::UNCAPPED)
            .expect("the fixture registry is uncapped");
        (guard, push_rx)
    }

    /// One drained deferred view: its channel, its instance, and each entry as
    /// `(body, deliver_after)` — an edit's two halves are exactly those fields.
    type ViewSnapshot = (String, String, Vec<(String, u64)>);

    /// The deferred views waiting in one session's push queue.
    fn drain_view_entries(rx: &mut mpsc::Receiver<SessionPush>) -> Vec<ViewSnapshot> {
        let mut views = Vec::new();
        while let Ok(push) = rx.try_recv() {
            if let SessionPush::DeferredView(view) = push {
                views.push((
                    view.channel,
                    view.instance,
                    view.entries
                        .into_iter()
                        .map(|e| (e.body, e.deliver_after))
                        .collect(),
                ));
            }
        }
        views
    }

    /// The deferred views waiting in one session's push queue, as
    /// `(channel, instance, bodies)`.
    fn drain_views(rx: &mut mpsc::Receiver<SessionPush>) -> Vec<(String, String, Vec<String>)> {
        let mut views = Vec::new();
        while let Ok(push) = rx.try_recv() {
            if let SessionPush::DeferredView(view) = push {
                views.push((
                    view.channel,
                    view.instance,
                    view.entries.into_iter().map(|e| e.body).collect(),
                ));
            }
        }
        views
    }

    /// **A park restates the sender's view to every session of the surface.** The
    /// parked set belongs to `surface:<slug>#<instance>`, which every tab shares,
    /// so both sessions are told — and told the same thing, on both classes.
    #[tokio::test]
    async fn a_parked_batch_entry_pushes_the_view_to_every_session_of_the_surface() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();

        let (_g1, mut first) = attach_view_session(&ctx);
        let (_g2, mut second) = attach_view_session(&ctx);

        let outcome = flush_batch(
            &ctx,
            "protobar",
            21,
            &[
                deferred_entry("out", "d-later", later),
                deferred_entry("eph", "e-later", later),
                entry("eph", "e-now"),
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        let expected = vec![
            (
                "brenn:batch-out".to_string(),
                "protobar".to_string(),
                vec!["d-later".to_string()],
            ),
            (
                "ephemeral:batch-eph".to_string(),
                "protobar".to_string(),
                vec!["e-later".to_string()],
            ),
        ];
        assert_eq!(
            drain_views(&mut first),
            expected,
            "one view per scheduled channel, in channel order"
        );
        assert_eq!(
            drain_views(&mut second),
            expected,
            "the sibling session is told the same thing"
        );
    }

    /// **A batch that scheduled nothing restates nothing.** The view is pushed at
    /// the change, not at every flush; a page that parked nothing is not handed a
    /// snapshot it already has.
    #[tokio::test]
    async fn a_batch_with_no_schedule_pushes_no_deferred_view() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let (_guard, mut pushes) = attach_view_session(&ctx);

        let outcome = flush_batch(
            &ctx,
            "protobar",
            22,
            // A release time already past is an immediate publish, so it parks
            // nothing and owes no view either.
            &[entry("out", "a"), deferred_entry("eph", "past", 1)],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert!(
            drain_views(&mut pushes).is_empty(),
            "nothing parked, so nothing to restate"
        );
    }

    /// **Seeding sends one frame per nonempty set, and nothing for an empty one.**
    /// The page clears its mirrors at `Welcome`, so silence about a channel is the
    /// statement that its set is empty — which is what makes a set that drained
    /// while the page was away arrive correctly empty.
    #[tokio::test]
    async fn seeding_frames_the_nonempty_sets_and_stays_silent_about_the_rest() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();

        // Park on the durable channel only; the ephemeral one stays empty.
        let outcome = flush_batch(
            &ctx,
            "protobar",
            23,
            &[
                deferred_entry("out", "survivor", later),
                entry("eph", "now"),
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        while rx.try_recv().is_ok() {}

        assert!(matches!(
            seed_deferred_views(&ctx, &mut counters).await,
            FrameOutcome::Continue
        ));

        let mut seeded = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            match frame {
                ServerFrame::DeferredView {
                    channel,
                    instance,
                    entries,
                } => seeded.push((
                    channel,
                    instance,
                    entries.into_iter().map(|e| e.body).collect::<Vec<_>>(),
                )),
                other => panic!("expected only DeferredView frames, got {other:?}"),
            }
        }
        assert_eq!(
            seeded,
            vec![(
                "brenn:batch-out".to_string(),
                "protobar".to_string(),
                vec!["survivor".to_string()],
            )],
            "the empty ephemeral set is stated by its absence"
        );
    }

    /// **Every bound output of a declared instance is a seeding target**, on both
    /// classes and whether or not the directory holds its channel. The enumeration
    /// is the config's alone — declared instance crossed with the channels its
    /// output ports name — so the third output here, whose address the fixture's
    /// directory does not carry, is a target like the other two. Reintroducing a
    /// directory filter over this set fails on that one.
    ///
    /// What an unseedable set must not do is vanish from the targets: absent from
    /// them, a set with something parked on it reports as an orphaned instance
    /// rather than as the config gap it is.
    #[tokio::test]
    async fn every_bound_output_of_a_declared_instance_is_a_seeding_target() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx_with_undeclared_output(&db).await;

        assert_eq!(
            deferred_view_targets(&ctx.runtime),
            vec![
                ParkedSet {
                    channel: "brenn:batch-out".to_string(),
                    instance: "protobar".to_string(),
                },
                ParkedSet {
                    channel: UNDECLARED_OUT_ADDR.to_string(),
                    instance: "protobar".to_string(),
                },
                ParkedSet {
                    channel: "ephemeral:batch-eph".to_string(),
                    instance: "protobar".to_string(),
                },
            ],
            "all three bound outputs, sorted, the undeclared one included"
        );
    }

    /// **Seeding an output the directory cannot resolve panics; it does not skip.**
    /// The seeding pass enumerates from the config and trusts boot's assertion that
    /// every transportable output names a declared channel, so the recompute's
    /// store lookup is where a violated invariant surfaces — loudly, on a state a
    /// booted server cannot reach. Skipping instead would turn an operator's config
    /// gap into a page mirror that is silently and permanently empty.
    #[tokio::test]
    #[should_panic(expected = "a bound output port must resolve")]
    async fn seeding_a_bound_output_the_directory_lacks_panics() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx_with_undeclared_output(&db).await;
        let mut counters = SessionCounters::default();

        let _outcome = seed_deferred_views(&ctx, &mut counters).await;
    }

    /// **A parked set no seeding target covers is reported, not seeded.** An
    /// instance the config no longer declares still holds its schedules on the
    /// backend, and they still release; what is gone is the page's view of them,
    /// so the seeding pass names the orphan for the operator and stays silent to
    /// the page about it.
    #[tokio::test]
    async fn a_parked_set_outside_the_seeding_targets_is_reported_as_orphaned() {
        use brenn_lib::messaging::store::NewMessage;

        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();
        let messenger = ctx.runtime.messenger.as_ref().expect("fixture messenger");

        // The declared instance parks through its bound output: a seedable set.
        let outcome = flush_batch(
            &ctx,
            "protobar",
            24,
            &[deferred_entry("out", "declared", later)],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        while rx.try_recv().is_ok() {}

        // A sub-identity the fixture's `Welcome` does not declare — the shape a
        // config change leaves behind — parks on a channel it once bound.
        let ghost = ParticipantId::for_surface_component(&ctx.runtime.resolved.slug, "ghost");
        let now = Utc::now();
        messenger
            .store_for_address("ephemeral:batch-eph")
            .park(
                NewMessage {
                    source: "test".to_string(),
                    sender: ghost.as_str().to_string(),
                    body: "orphaned".to_string(),
                    urgency: Urgency::Normal,
                    envelope_type: brenn_lib::messaging::ChannelScheme::Ephemeral,
                    reply_to_uuid: None,
                    delivery_deadline: None,
                    publish_ts_ns: now.timestamp_nanos_opt().unwrap(),
                },
                now + chrono::Duration::hours(1),
            )
            .await
            .expect("under the cap");

        let targets = deferred_view_targets(&ctx.runtime);
        assert_eq!(
            orphaned_parked_sets(messenger, &ctx.runtime.resolved.slug, &targets, now).await,
            vec![ParkedSet {
                channel: "ephemeral:batch-eph".to_string(),
                instance: "ghost".to_string(),
            }],
            "only the set no target covers is orphaned; the declared instance's is seeded"
        );

        // Seeding itself is unchanged by the orphan: the page hears about the
        // set it can act on and nothing about the one it cannot.
        assert!(matches!(
            seed_deferred_views(&ctx, &mut counters).await,
            FrameOutcome::Continue
        ));
        let mut seeded = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            match frame {
                ServerFrame::DeferredView {
                    channel, instance, ..
                } => seeded.push((channel, instance)),
                other => panic!("expected only DeferredView frames, got {other:?}"),
            }
        }
        assert_eq!(
            seeded,
            vec![("brenn:batch-out".to_string(), "protobar".to_string())]
        );
    }

    /// The identities of `protobar`'s parked messages on `channel`, in release
    /// order — the same view the page is pushed, which is where a conforming
    /// kernel's op ids come from.
    async fn parked_ids(ctx: &SessionCtx, channel: &str) -> Vec<Uuid> {
        let messenger = ctx.runtime.messenger.as_ref().expect("fixture messenger");
        let sender = ParticipantId::for_surface_component(&ctx.runtime.resolved.slug, "protobar");
        messenger
            .deferred_view_for_sender(channel, sender.as_str(), Utc::now())
            .await
            .into_iter()
            .map(|m| m.envelope.message_id)
            .collect()
    }

    /// The bodies of `protobar`'s parked messages on `channel`, in release order.
    async fn parked_bodies(ctx: &SessionCtx, channel: &str) -> Vec<String> {
        let messenger = ctx.runtime.messenger.as_ref().expect("fixture messenger");
        let sender = ParticipantId::for_surface_component(&ctx.runtime.resolved.slug, "protobar");
        messenger
            .deferred_view_for_sender(channel, sender.as_str(), Utc::now())
            .await
            .into_iter()
            .map(|m| m.envelope.body.clone())
            .collect()
    }

    /// One control op naming `port` and `message_id`.
    fn cancel_op(port: &str, message_id: Uuid) -> BatchDeferredOp {
        BatchDeferredOp {
            port: port.to_string(),
            message_id,
            op: DeferredOpKind::Cancel,
        }
    }

    /// **A cancel travelling the wire applies to the sender's own parked set, and
    /// the page is told.** The sub-identity is derived from the named instance
    /// exactly as a publish's is, so the op reaches the same schedule the view
    /// showed and nothing else.
    #[tokio::test]
    async fn a_wire_cancel_applies_under_the_sub_identity_and_restates_the_view() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();
        let (_guard, mut pushes) = attach_view_session(&ctx);

        assert!(matches!(
            flush_batch(
                &ctx,
                "protobar",
                31,
                &[
                    deferred_entry("out", "keep", later),
                    deferred_entry("out", "drop", later + 1_000),
                ],
                &mut counters,
            )
            .await,
            FrameOutcome::Continue
        ));
        let ids = parked_ids(&ctx, "brenn:batch-out").await;
        assert_eq!(ids.len(), 2, "both entries parked");
        let _ = drain_views(&mut pushes);

        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            32,
            &[],
            &[cancel_op("out", ids[1])],
            &mut counters,
        )
        .await;
        assert!(
            matches!(outcome, FrameOutcome::Continue),
            "an ops-only flush is a whole batch"
        );
        assert_eq!(
            parked_bodies(&ctx, "brenn:batch-out").await,
            vec!["keep".to_string()],
            "the cancel took exactly the message it named"
        );
        assert_eq!(
            drain_views(&mut pushes),
            vec![(
                "brenn:batch-out".to_string(),
                "protobar".to_string(),
                vec!["keep".to_string()],
            )],
            "the applied op restates the view"
        );
    }

    /// **The ops apply before the batch's publishes.** Observed through the
    /// deferred cap: with room for exactly one schedule, a batch that cancels the
    /// parked entry and parks a new one lands the new one. Publishes-first would
    /// meet a full set and drop the schedule.
    #[tokio::test]
    async fn control_ops_apply_before_the_batchs_publishes() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx_at_durable_retain(&db, Some(1)).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();

        assert!(matches!(
            flush_batch(
                &ctx,
                "protobar",
                33,
                &[deferred_entry("out", "old", later)],
                &mut counters,
            )
            .await,
            FrameOutcome::Continue
        ));
        let ids = parked_ids(&ctx, "brenn:batch-out").await;
        assert_eq!(ids.len(), 1, "the cap admitted one");

        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            34,
            &[deferred_entry("out", "new", later + 1_000)],
            &[cancel_op("out", ids[0])],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert_eq!(
            parked_bodies(&ctx, "brenn:batch-out").await,
            vec!["new".to_string()],
            "the cancel freed the slot the park then took"
        );
    }

    /// **An edit that moves a release earlier wakes the sweep, and restates the
    /// view it rewrote.** The sweep sleeps to the earliest deadline it last
    /// computed, so an edit is exactly as capable of stranding a schedule past its
    /// time as a park is — and both halves it rewrote are what the page must end up
    /// mirroring, since an edit is the one op that changes an entry rather than
    /// removing it.
    #[tokio::test]
    async fn an_applied_edit_wakes_the_release_sweep_and_restates_the_view() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();
        let (_guard, mut pushes) = attach_view_session(&ctx);

        assert!(matches!(
            flush_batch(
                &ctx,
                "protobar",
                35,
                &[deferred_entry("out", "distant", later + 3_600_000)],
                &mut counters,
            )
            .await,
            FrameOutcome::Continue
        ));
        let ids = parked_ids(&ctx, "brenn:batch-out").await;
        let _ = drain_view_entries(&mut pushes);
        // On a whole second: a durable row keeps its release time to the second, so
        // this is the value that survives the write and comes back in the view.
        let edited = later - later % 1_000;

        // The park above kicked too; consume that permit so the one asserted below
        // is unambiguously the edit's.
        let notify = ctx.runtime.messenger().dispatch_kick_notify();
        let _ = tokio::time::timeout(std::time::Duration::from_millis(20), notify.notified()).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), notify.notified())
                .await
                .is_err(),
            "no kick is outstanding before the edit, so the one below is the op's"
        );

        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            36,
            &[],
            &[BatchDeferredOp {
                port: "out".to_string(),
                message_id: ids[0],
                op: DeferredOpKind::Edit {
                    body: Some("rewritten".to_string()),
                    deliver_after: Some(edited),
                },
            }],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert_eq!(
            parked_bodies(&ctx, "brenn:batch-out").await,
            vec!["rewritten".to_string()],
            "both halves of the edit landed"
        );
        assert_eq!(
            drain_view_entries(&mut pushes),
            vec![(
                "brenn:batch-out".to_string(),
                "protobar".to_string(),
                vec![("rewritten".to_string(), edited)],
            )],
            "the pushed view is recomputed, so it carries the new body and the new \
             release time"
        );
        tokio::time::timeout(std::time::Duration::from_millis(20), notify.notified())
            .await
            .expect("the applied edit kicked the release sweep");
    }

    /// **A message that released between the snapshot and the frame is a benign
    /// race — and the view is restated anyway.** The component acted on the only
    /// truth it had, and the page could not have known better; the op is logged and
    /// counted, the batch continues, and the connection lives. The restatement is
    /// what closes the loop for a mirror that is wrong for any other reason: an op
    /// naming a schedule the backend does not hold is the one event a stale mirror
    /// reliably provokes, so it must not be the one set-touching event that pushes
    /// nothing.
    #[tokio::test]
    async fn a_control_op_naming_nothing_parked_is_a_counted_no_op() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let (_guard, mut pushes) = attach_view_session(&ctx);
        let messenger = ctx.runtime.messenger();
        let sender = ParticipantId::for_surface_component(&ctx.runtime.resolved.slug, "protobar");

        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            37,
            &[entry("out", "alongside")],
            &[cancel_op("out", Uuid::new_v4())],
            &mut counters,
        )
        .await;
        assert!(
            matches!(outcome, FrameOutcome::Continue),
            "a race is never a kill"
        );
        assert_eq!(
            messenger.deferred_control_race_count(sender.as_str(), "brenn:batch-out"),
            1,
            "the race is counted against the sender and channel"
        );
        assert_eq!(
            counters.publishes, 1,
            "the batch's publish landed regardless"
        );
        assert_eq!(
            drain_views(&mut pushes),
            vec![(
                "brenn:batch-out".to_string(),
                "protobar".to_string(),
                Vec::new(),
            )],
            "the lost race still restates the set — empty, which is exactly what the \
             page needed to hear"
        );
    }

    /// **Naming another sender's parked message is a protocol violation.** A
    /// conforming kernel can only name what a sender-scoped view showed it, so this
    /// is a client reaching for a schedule no window ever offered — fail2ban signal.
    /// Reported rather than panicked on purpose: the uuid comes off the wire, and a
    /// panic on client input is a remote kill switch.
    #[tokio::test]
    async fn a_control_op_naming_another_senders_message_is_a_violation() {
        use brenn_lib::messaging::store::NewMessage;

        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let messenger = ctx.runtime.messenger();
        let ghost = ParticipantId::for_surface_component(&ctx.runtime.resolved.slug, "ghost");
        let now = Utc::now();

        let parked = messenger
            .store_for_address("ephemeral:batch-eph")
            .park(
                NewMessage {
                    source: "test".to_string(),
                    sender: ghost.as_str().to_string(),
                    body: "not yours".to_string(),
                    urgency: Urgency::Normal,
                    envelope_type: brenn_lib::messaging::ChannelScheme::Ephemeral,
                    reply_to_uuid: None,
                    delivery_deadline: None,
                    publish_ts_ns: now.timestamp_nanos_opt().unwrap(),
                },
                now + chrono::Duration::hours(1),
            )
            .await
            .expect("under the cap");

        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            38,
            &[],
            &[cancel_op("eph", parked.message_uuid)],
            &mut counters,
        )
        .await;
        let FrameOutcome::Violation(detail) = outcome else {
            panic!("expected a violation for a foreign message id");
        };
        assert!(
            detail.contains("parked by another sender"),
            "the log line names the reason: {detail}"
        );
        assert_eq!(
            messenger
                .deferred_view_for_sender("ephemeral:batch-eph", ghost.as_str(), Utc::now())
                .await
                .len(),
            1,
            "the other sender's schedule is untouched"
        );
    }

    /// **A violation mid-batch still restates what the earlier ops changed.** The
    /// offending connection dies, but the applied ops were legitimate and the
    /// parked set belongs to the sub-identity every session of the surface shares.
    /// Left unsaid, a sibling tab would hold a cancelled schedule with nothing left
    /// to correct it: an emptied set has no release and no later change to push a
    /// view from.
    #[tokio::test]
    async fn a_violating_op_still_restates_the_sets_its_predecessors_changed() {
        use brenn_lib::messaging::store::NewMessage;

        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();
        let messenger = ctx.runtime.messenger();
        let now = Utc::now();

        // The sibling tab: it shares the sender, so it mirrors the same set and
        // survives the violating connection's death.
        let (_sibling, mut sibling) = attach_view_session(&ctx);

        assert!(matches!(
            flush_batch(
                &ctx,
                "protobar",
                41,
                &[deferred_entry("out", "only-one", later)],
                &mut counters,
            )
            .await,
            FrameOutcome::Continue
        ));
        let ids = parked_ids(&ctx, "brenn:batch-out").await;
        assert_eq!(ids.len(), 1);
        let _ = drain_views(&mut sibling);

        // Another sender's schedule, for the op that kills the connection.
        let ghost = ParticipantId::for_surface_component(&ctx.runtime.resolved.slug, "ghost");
        let foreign = messenger
            .store_for_address("ephemeral:batch-eph")
            .park(
                NewMessage {
                    source: "test".to_string(),
                    sender: ghost.as_str().to_string(),
                    body: "not yours".to_string(),
                    urgency: Urgency::Normal,
                    envelope_type: brenn_lib::messaging::ChannelScheme::Ephemeral,
                    reply_to_uuid: None,
                    delivery_deadline: None,
                    publish_ts_ns: now.timestamp_nanos_opt().unwrap(),
                },
                now + chrono::Duration::hours(1),
            )
            .await
            .expect("under the cap");

        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            42,
            &[],
            &[
                cancel_op("out", ids[0]),
                cancel_op("eph", foreign.message_uuid),
            ],
            &mut counters,
        )
        .await;
        assert!(
            matches!(outcome, FrameOutcome::Violation(_)),
            "the foreign id still kills the connection"
        );
        assert!(
            parked_bodies(&ctx, "brenn:batch-out").await.is_empty(),
            "the legitimate cancel stands"
        );
        assert_eq!(
            drain_views(&mut sibling),
            vec![(
                "brenn:batch-out".to_string(),
                "protobar".to_string(),
                Vec::<String>::new(),
            )],
            "the sibling is told the set is now empty, which nothing else would say"
        );
    }

    /// **Every per-op shape check the kernel makes at buffer time is a violation
    /// here.** An unbound port, an oversize edit body, an unrepresentable release
    /// time, an op list past the per-activation cap, and a frame with neither
    /// publishes nor ops are all things a kernel refuses before the wire, so each
    /// one arriving says the client is not the kernel.
    #[tokio::test]
    async fn malformed_control_ops_are_violations() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let id = Uuid::new_v4();

        let edit = |body: Option<String>, deliver_after: Option<u64>| BatchDeferredOp {
            port: "out".to_string(),
            message_id: id,
            op: DeferredOpKind::Edit {
                body,
                deliver_after,
            },
        };
        let cases: Vec<(&str, Vec<BatchDeferredOp>)> = vec![
            ("unbound port", vec![cancel_op("nope", id)]),
            (
                "oversize edit body",
                vec![edit(Some("x".repeat(TEST_MAX_BODY_BYTES + 1)), None)],
            ),
            (
                "unrepresentable release time",
                vec![edit(None, Some(u64::MAX))],
            ),
            (
                "over the op cap",
                (0..=MAX_PUBLISHES_PER_ACTIVATION)
                    .map(|_| cancel_op("out", id))
                    .collect(),
            ),
        ];
        for (name, ops) in cases {
            let outcome =
                handle_publish_batch(&ctx, "protobar", 39, &[], &ops, &mut counters).await;
            assert!(
                matches!(outcome, FrameOutcome::Violation(_)),
                "{name} must be violation-grade"
            );
        }
        assert!(
            matches!(
                handle_publish_batch(&ctx, "protobar", 40, &[], &[], &mut counters).await,
                FrameOutcome::Violation(_)
            ),
            "a frame carrying neither publishes nor ops is empty"
        );
    }

    /// A release time already past is an immediate publish, on both classes — the
    /// WIT's rule, decided against one clock read for the whole flush.
    #[tokio::test]
    async fn a_deliver_after_already_past_publishes_immediately() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        let mut sub = ctx
            .runtime
            .messenger()
            .attach_live(
                ctx.runtime.participant.clone(),
                ctx.runtime.policy.clone(),
                "ephemeral:batch-eph",
                None,
            )
            .expect("ephemeral subscribe")
            .receiver;

        let outcome = flush_batch(
            &ctx,
            "protobar",
            11,
            &[
                deferred_entry("out", "d-past", 1),
                deferred_entry("eph", "e-past", 1),
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        let conn = db.lock().await;
        let rows: Vec<(String, bool, Option<i64>)> = conn
            .prepare("SELECT body, deliver_after IS NOT NULL, retained_seq FROM messaging_messages")
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get(0).unwrap(),
                    r.get::<_, bool>(1).unwrap(),
                    r.get(2).unwrap(),
                ))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows,
            vec![("d-past".to_string(), false, Some(1))],
            "a past release time carries no schedule and takes a position"
        );
        drop(conn);

        match sub.recv().await.expect("ephemeral delivery") {
            EphemeralEvent::Delivery(d) => assert_eq!(d.envelope.body, "e-past"),
            other => panic!("expected a delivery, got {other:?}"),
        }
    }

    /// A release time chrono cannot carry is violation-grade like every other
    /// per-entry check: the kernel refuses one at buffer time, so it reaches the
    /// server only from a client that is not the kernel.
    #[tokio::test]
    async fn an_unrepresentable_deliver_after_is_a_violation() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        let outcome = flush_batch(
            &ctx,
            "protobar",
            13,
            &[
                entry("out", "sibling"),
                deferred_entry("out", "impossible", u64::MAX),
            ],
            &mut counters,
        )
        .await;
        match outcome {
            FrameOutcome::Violation(detail) => assert!(
                detail.contains("unrepresentable deliver_after"),
                "the detail names the refused field: {detail}"
            ),
            _ => panic!("an unrepresentable release time must kill the connection"),
        }

        let conn = db.lock().await;
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 0,
            "the batch resolves whole before it applies, so the sibling never committed"
        );
    }

    /// A full deferred set is normal operation, not an error: the cap refuses
    /// one schedule and the batch is still answered `Ok`.
    #[tokio::test]
    async fn a_full_ephemeral_deferred_set_drops_one_schedule_and_the_batch_is_still_ok() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();

        // The fixture channel retains 4, which is its deferred cap.
        let entries: Vec<BatchEntry> = (0..5)
            .map(|i| deferred_entry("eph", &format!("s{i}"), later))
            .collect();
        let outcome = flush_batch(&ctx, "protobar", 15, &entries, &mut counters).await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert!(matches!(
            rx.try_recv().expect("PublishBatchResult frame"),
            ServerFrame::PublishBatchResult {
                outcome: PublishBatchOutcome::Ok,
                ..
            }
        ));

        assert_eq!(
            counters.publishes, 4,
            "the refused schedule published nothing"
        );
        assert_eq!(
            ctx.runtime
                .messenger()
                .dropped_deferred_count("surface:deskbar#protobar", "ephemeral:batch-eph"),
            1,
            "the drop is counted against the component that asked for it"
        );
    }

    /// **A park must wake the release sweep.** The sweep sleeps to the earliest
    /// deadline it last computed, capped at the poll interval, so a schedule that
    /// lands afterwards is late by up to a whole poll unless the park kicks the
    /// loop. The durable half kicks from inside `publish_batch_from_surface`,
    /// which returns before it does anything for a batch with no durable entries
    /// — so the ephemeral half must kick for itself.
    #[tokio::test]
    async fn an_ephemeral_only_deferred_batch_wakes_the_release_sweep() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let notify = ctx.runtime.messenger().dispatch_kick_notify();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), notify.notified())
                .await
                .is_err(),
            "no kick is outstanding before the batch, so the one below is the park's"
        );

        let outcome = flush_batch(
            &ctx,
            "protobar",
            21,
            &[deferred_entry("eph", "e-later", later_ms())],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        tokio::time::timeout(std::time::Duration::from_millis(20), notify.notified())
            .await
            .expect("the ephemeral park kicked the release sweep");
    }

    /// The session's own use of the refused-schedule count: a durable schedule the
    /// cap turned away published nothing, so it must not be counted as a publish.
    /// The counter is what an operator reads for work the surface actually landed,
    /// and the subtraction that keeps it honest also underflows — panicking the
    /// session task on a client-reachable frame — if the returned count ever
    /// stops being a subset of the durable entries.
    #[tokio::test]
    async fn a_refused_durable_schedule_is_not_counted_as_a_publish() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx_at_durable_retain(&db, Some(1)).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();

        let outcome = flush_batch(
            &ctx,
            "protobar",
            23,
            &[
                deferred_entry("out", "d-first", later),
                deferred_entry("out", "d-refused", later),
                entry("out", "d-now"),
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        assert!(matches!(
            rx.try_recv().expect("PublishBatchResult frame"),
            ServerFrame::PublishBatchResult {
                outcome: PublishBatchOutcome::Ok,
                ..
            }
        ));

        assert_eq!(
            counters.publishes, 2,
            "the refused schedule published nothing, so only two entries landed"
        );
        assert_eq!(
            ctx.runtime
                .messenger()
                .dropped_deferred_count("surface:deskbar#protobar", "brenn:batch-out"),
            1,
            "the drop is counted against the component that asked for it"
        );

        let conn = db.lock().await;
        let bodies: Vec<String> = conn
            .prepare("SELECT body FROM messaging_messages ORDER BY publish_ts_ns")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            bodies,
            vec!["d-first".to_string(), "d-now".to_string()],
            "the refused entry left no row and the rest of the batch committed"
        );
    }

    /// A per-call urgency override wins over the port's configured default; an
    /// entry that states none takes the operator's value. The server reads the
    /// default from its own output map, never from the frame.
    #[tokio::test]
    async fn publish_batch_resolves_urgency_per_entry() {
        let db = brenn_lib::db::init_db_memory();
        let (mut ctx, _rx) = batch_ctx(&db).await;
        // Give the port a non-`Normal` default so "override wins" and "default
        // applies" are distinguishable from each other and from the enum default.
        let runtime = Arc::get_mut(&mut ctx.runtime).expect("uniquely owned in test");
        runtime
            .output_ports
            .get_mut(&("protobar".to_string(), "out".to_string()))
            .expect("the durable output")
            .default_urgency = Urgency::Low;
        let mut counters = SessionCounters::default();

        let outcome = flush_batch(
            &ctx,
            "protobar",
            1,
            &[
                entry("out", "defaulted"),
                BatchEntry {
                    port: "out".to_string(),
                    body: "overridden".to_string(),
                    urgency: Some(Urgency::High),
                    deliver_after: None,
                },
            ],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        let conn = db.lock().await;
        let rows: Vec<(String, String)> = conn
            .prepare("SELECT body, urgency FROM messaging_messages ORDER BY publish_ts_ns")
            .unwrap()
            .query_map([], |r| Ok((r.get(0).unwrap(), r.get(1).unwrap())))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows,
            vec![
                ("defaulted".to_string(), Urgency::Low.as_str().to_string()),
                ("overridden".to_string(), Urgency::High.as_str().to_string()),
            ],
            "absent urgency takes the port default; an override wins"
        );
    }

    /// An undeclared instance is a violation, not a demotion to the bare surface
    /// identity — demoting would let a non-conforming client launder a flush onto
    /// the surface's own budget.
    #[tokio::test]
    async fn publish_batch_from_an_undeclared_instance_is_a_violation() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        let outcome = flush_batch(&ctx, "ghost", 1, &[entry("out", "a")], &mut counters).await;
        assert!(
            matches!(outcome, FrameOutcome::Violation(_)),
            "an undeclared instance kills the connection"
        );
    }

    /// The reserved error-report port cannot ride a batch: its instance is outside
    /// the declared set by construction, so it dies on the same arm as any other
    /// undeclared instance. A batch is an activation's flush, not the kernel's
    /// breadcrumb path.
    #[tokio::test]
    async fn publish_batch_on_the_reserved_report_port_is_a_violation() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        let outcome = flush_batch(
            &ctx,
            brenn_surface_contract::ERROR_REPORT_INSTANCE,
            1,
            &[entry(
                brenn_surface_contract::ERROR_REPORT_PORT,
                REPORT_BODY,
            )],
            &mut counters,
        )
        .await;
        assert!(
            matches!(outcome, FrameOutcome::Violation(_)),
            "the reserved report port is not a batch target"
        );
    }

    /// **Per-entry validation is violation-grade.** The kernel gates every one of
    /// these at buffer time and answers the component the `processor.wit` triple
    /// inline, so an entry arriving broken means the client is not the kernel —
    /// fail2ban signal, never a soft outcome. Contrast single `Publish`, where an
    /// over-cap body is an outcome.
    #[tokio::test]
    async fn publish_batch_entry_violations_kill_the_connection() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;

        // An unbound port of a declared instance.
        let mut counters = SessionCounters::default();
        assert!(
            matches!(
                flush_batch(&ctx, "protobar", 1, &[entry("nope", "a")], &mut counters).await,
                FrameOutcome::Violation(_)
            ),
            "an unbound port is a violation"
        );

        // An over-cap body — an *outcome* on the single-publish path.
        let mut counters = SessionCounters::default();
        let oversized = "x".repeat(TEST_MAX_BODY_BYTES + 1);
        assert!(
            matches!(
                flush_batch(
                    &ctx,
                    "protobar",
                    1,
                    &[entry("out", &oversized)],
                    &mut counters
                )
                .await,
                FrameOutcome::Violation(_)
            ),
            "an over-cap entry body is a violation, not BodyTooLarge"
        );

        // More entries than the kernel can buffer in one activation.
        let mut counters = SessionCounters::default();
        let too_many: Vec<BatchEntry> = (0..MAX_PUBLISHES_PER_ACTIVATION + 1)
            .map(|_| entry("out", "x"))
            .collect();
        assert!(
            matches!(
                flush_batch(&ctx, "protobar", 1, &too_many, &mut counters).await,
                FrameOutcome::Violation(_)
            ),
            "a batch over the per-activation cap is a violation"
        );

        // An empty batch: a conforming kernel sends no frame at all.
        let mut counters = SessionCounters::default();
        assert!(
            matches!(
                flush_batch(&ctx, "protobar", 1, &[], &mut counters).await,
                FrameOutcome::Violation(_)
            ),
            "an empty batch is a violation"
        );
    }

    /// A batch under both the entry-count cap and the per-entry body cap can
    /// still be over the kernel's per-activation *byte* ceiling — and that is a
    /// batch no kernel produced, so it is a violation like the other two.
    ///
    /// This is the arm that closes the gap between the caps: without it a hostile
    /// client hands the server 256 legal maximum bodies in one frame — durable
    /// rows plus their push fan-out — on a single positive-balance debt draw,
    /// while every individual check says yes.
    #[tokio::test]
    async fn a_batch_over_the_per_activation_byte_cap_is_a_violation() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        // Each body is exactly the legal per-entry maximum and the count is far
        // under the 256 cap, so only the byte ceiling can refuse this.
        let body = "x".repeat(TEST_MAX_BODY_BYTES);
        let count = MAX_PUBLISH_BYTES_PER_ACTIVATION / TEST_MAX_BODY_BYTES + 1;
        assert!(count <= MAX_PUBLISHES_PER_ACTIVATION, "not the count arm");
        let batch: Vec<BatchEntry> = (0..count).map(|_| entry("out", &body)).collect();
        assert!(
            matches!(
                flush_batch(&ctx, "protobar", 1, &batch, &mut counters).await,
                FrameOutcome::Violation(_)
            ),
            "a batch over the per-activation byte cap is a violation"
        );

        // Nothing applied: the shape check runs before any entry is routed.
        let conn = db.lock().await;
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0, "a violating batch commits nothing");
    }

    /// A violating entry must not leave a prefix of the batch applied: the checks
    /// all run before any entry is routed, because the batch is atomic.
    #[tokio::test]
    async fn a_violating_entry_applies_none_of_its_batch() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, _rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        let outcome = flush_batch(
            &ctx,
            "protobar",
            1,
            &[entry("out", "would-have-landed"), entry("nope", "kills-it")],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Violation(_)));

        let conn = db.lock().await;
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "the valid prefix never reached the bus");
    }

    /// The send-budget backstop: a batch whose draw the balance cannot cover is
    /// answered `RateLimited` — logged, counted, dropped, connection healthy.
    /// Never a violation and never a kill (the two tiers disagreeing is not
    /// misbehaviour).
    #[tokio::test]
    async fn a_batch_refused_by_the_send_budget_is_rate_limited_not_killed() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        // Drain the bucket with one maximal conforming flush. The default burst
        // is the per-activation cap, so this is both the widest batch a kernel
        // can send and exactly the whole balance — it must be admitted, and it
        // must leave nothing.
        let wide: Vec<BatchEntry> = (0..MAX_PUBLISHES_PER_ACTIVATION)
            .map(|_| entry("out", "x"))
            .collect();
        assert!(matches!(
            flush_batch(&ctx, "protobar", 1, &wide, &mut counters).await,
            FrameOutcome::Continue
        ));
        assert!(
            matches!(
                rx.try_recv().expect("first batch result"),
                ServerFrame::PublishBatchResult {
                    outcome: PublishBatchOutcome::Ok,
                    ..
                }
            ),
            "a maximal conforming flush is admitted whole from a full bucket"
        );

        // The next batch finds an empty balance and is refused.
        let outcome = flush_batch(&ctx, "protobar", 2, &[entry("out", "b")], &mut counters).await;
        assert!(
            matches!(outcome, FrameOutcome::Continue),
            "a refused batch never kills the connection"
        );
        match rx.try_recv().expect("second batch result") {
            ServerFrame::PublishBatchResult {
                correlation,
                outcome,
            } => {
                assert_eq!(correlation, 2);
                assert_eq!(outcome, PublishBatchOutcome::RateLimited);
            }
            other => panic!("expected PublishBatchResult, got {other:?}"),
        }
        assert_eq!(
            counters.publish_rate_limited, 1,
            "the refused batch's entry is counted against the instance"
        );
        assert_eq!(
            counters.by_instance["protobar"].publish_rate_limited, 1,
            "attribution lands on the instance that flushed"
        );

        // The refused batch reached nothing: only the first batch's rows exist.
        let conn = db.lock().await;
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count,
            wide.len() as i64,
            "the refused batch applied nothing"
        );
    }

    /// **A batch that publishes nothing still draws a token.** The floor is what
    /// keeps an ops-only flush from being a path a client rides for free: it is a
    /// frame the principal sent and work the server did.
    #[tokio::test]
    async fn an_ops_only_flush_draws_the_one_token_floor() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();

        // Park one entry, then spend the bucket down to a single token.
        assert!(matches!(
            flush_batch(
                &ctx,
                "protobar",
                1,
                &[deferred_entry("out", "parked", later)],
                &mut counters,
            )
            .await,
            FrameOutcome::Continue
        ));
        let ids = parked_ids(&ctx, "brenn:batch-out").await;
        assert_eq!(ids.len(), 1, "the entry parked");
        let rest: Vec<BatchEntry> = (0..MAX_PUBLISHES_PER_ACTIVATION - 2)
            .map(|_| entry("out", "x"))
            .collect();
        assert!(matches!(
            flush_batch(&ctx, "protobar", 2, &rest, &mut counters).await,
            FrameOutcome::Continue
        ));
        while rx.try_recv().is_ok() {}

        // One token left, one ops-only flush: admitted, and it spends that token.
        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            3,
            &[],
            &[cancel_op("out", ids[0])],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        match rx.try_recv().expect("the ops-only batch result") {
            ServerFrame::PublishBatchResult { outcome, .. } => {
                assert_eq!(
                    outcome,
                    PublishBatchOutcome::Ok,
                    "the last token covered it"
                );
            }
            other => panic!("expected PublishBatchResult, got {other:?}"),
        }

        // The balance is gone, so a second ops-only flush is refused — one token per
        // ops-only flush, no more and no less.
        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            4,
            &[],
            &[cancel_op("out", Uuid::new_v4())],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        match rx.try_recv().expect("the second ops-only batch result") {
            ServerFrame::PublishBatchResult { outcome, .. } => {
                assert_eq!(
                    outcome,
                    PublishBatchOutcome::RateLimited,
                    "the floor really drew from the bucket"
                );
            }
            other => panic!("expected PublishBatchResult, got {other:?}"),
        }
    }

    /// **The draw comes before anything is applied.** A refused batch is re-parked
    /// and retried whole by the kernel, so an op applied ahead of the draw would
    /// apply twice — and the page would be told nothing landed while its mirror was
    /// restated behind its back.
    #[tokio::test]
    async fn a_rate_limited_ops_only_flush_applies_nothing_and_pushes_no_view() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let later = later_ms();
        let (_guard, mut pushes) = attach_view_session(&ctx);

        assert!(matches!(
            flush_batch(
                &ctx,
                "protobar",
                1,
                &[deferred_entry("out", "parked", later)],
                &mut counters,
            )
            .await,
            FrameOutcome::Continue
        ));
        let ids = parked_ids(&ctx, "brenn:batch-out").await;
        // Drain the rest of the bucket, then the views the parks pushed.
        let rest: Vec<BatchEntry> = (0..MAX_PUBLISHES_PER_ACTIVATION - 1)
            .map(|_| entry("out", "x"))
            .collect();
        assert!(matches!(
            flush_batch(&ctx, "protobar", 2, &rest, &mut counters).await,
            FrameOutcome::Continue
        ));
        while rx.try_recv().is_ok() {}
        let _ = drain_views(&mut pushes);

        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            3,
            &[],
            &[cancel_op("out", ids[0])],
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        match rx.try_recv().expect("the refused batch result") {
            ServerFrame::PublishBatchResult { outcome, .. } => {
                assert_eq!(outcome, PublishBatchOutcome::RateLimited)
            }
            other => panic!("expected PublishBatchResult, got {other:?}"),
        }
        assert_eq!(
            parked_bodies(&ctx, "brenn:batch-out").await,
            vec!["parked".to_string()],
            "the refused flush cancelled nothing"
        );
        assert!(
            drain_views(&mut pushes).is_empty(),
            "and restated nothing: a set that did not change owes no emission"
        );
    }

    /// A single `Publish` after a batch has spent the instance's balance is
    /// rejected with today's rate-limit outcome — one bucket, one principal, so a
    /// flush's spending is a real cost against the instance's own ordinary
    /// traffic rather than a separate allowance.
    #[tokio::test]
    async fn a_single_publish_after_a_batch_drains_the_budget_is_rate_limited() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();
        let mut bucket = TokenBucket::new(1_000, std::time::Duration::from_secs(1), 1_000);

        let wide: Vec<BatchEntry> = (0..MAX_PUBLISHES_PER_ACTIVATION)
            .map(|_| entry("out", "x"))
            .collect();
        assert!(matches!(
            flush_batch(&ctx, "protobar", 1, &wide, &mut counters).await,
            FrameOutcome::Continue
        ));
        let _ = rx.try_recv();

        let outcome = handle_publish(
            &ctx,
            &mut bucket,
            PublishRequest {
                instance: "protobar",
                port: "out",
                body: "single",
                correlation: Some(9),
                subject_instance: None,
                urgency: None,
            },
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));
        match rx.try_recv().expect("PublishResult frame") {
            ServerFrame::PublishResult {
                correlation,
                outcome,
            } => {
                assert_eq!(correlation, Some(9));
                assert!(
                    matches!(outcome, PublishOutcome::RateLimited),
                    "a single publish during debt is rate-limited, got {outcome:?}"
                );
            }
            other => panic!("expected PublishResult, got {other:?}"),
        }
    }
}

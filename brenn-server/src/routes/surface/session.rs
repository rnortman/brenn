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
//! output onto the `EphemeralBus`, a durable output through
//! `Messenger::publish_from_surface` (oversized bodies answer `BodyTooLarge`).
//! `Unsubscribe` removes an active subscription (fire-and-
//! forget, no ack); unsubscribing a channel with no active subscription is a
//! violation.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket};
use brenn_lib::access::AppCapability;
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{
    EphemeralDelivery, EphemeralEvent, EphemeralPublishResult, EphemeralReceiver, EphemeralResume,
    GapReason as BusGapReason, MessageEnvelope, Messenger, PublishResult, Replay,
    SurfaceBatchPublish, SurfaceSendVerdict, Urgency, db,
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
    AlertSeverity as ProtoAlertSeverity, BatchEntry, ClientFrame, Cursor, DeliverTarget, GapInfo,
    GapReason as ProtoGapReason, InstanceReport, MAX_ALERT_BODY_BYTES, MAX_ALERT_TITLE_BYTES,
    PublishBatchOutcome, PublishOutcome, ServerFrame, StatusCounters, SubscribeOutcome,
    SurfaceDescription,
};

use super::cursor::{self, CursorState};
use chrono::Utc;

use super::telemetry::{self, Health};

use super::registry::{DurableDelivery, SurfaceSessionGuard};
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

/// Hard cap on the per-channel `replay_sent` id set held for the connection
/// lifetime. A new id enters only when it is both published after a previous
/// replay's window and then replay-sent — so growth needs both re-subscribe churn
/// (bucket-bounded above) and new publishes; a client that stays subscribed adds
/// nothing after its initial replay, and each replay adds at most the resolved
/// `retain_depth`. The set is therefore not a config-derived constant over an
/// unbounded connection lifetime — a churning client on a busy channel accretes
/// ids — so the bound is enforced, not argued: past it the connection is torn
/// down (normal close, not a protocol violation — it is reachable by an honest
/// very-long-lived connection, and teardown is always safe: the client
/// reconnects, resumes, and starts a fresh set). 65 536 ids ≈ 512 KiB per
/// channel. Pruning below the DB-retained window was rejected as unsound: a live
/// copy still queued in `durable_tx` is process memory independent of DB
/// retention GC, so pruning a still-queued id re-opens the duplicate this cap and
/// the retained set together close.
const REPLAY_SENT_MAX: usize = 65_536;

/// Everything the upgrade callback hands to the session task.
pub(crate) struct SurfaceSessionParams {
    pub runtime: Arc<SurfaceRuntime>,
    pub session_id: Uuid,
    pub username: String,
    pub ip: IpAddr,
    pub guard: SurfaceSessionGuard,
    pub heartbeat_secs: u32,
    pub alert_dispatcher: AlertDispatcher,
    /// Live durable rows from the router fan-out (paired with the `durable_tx` in
    /// this session's registry handle).
    pub durable_rx: mpsc::Receiver<DurableDelivery>,
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
        heartbeat_secs,
        alert_dispatcher,
        mut durable_rx,
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
                    let epoch = ctx.runtime.bus.epoch();
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
                        &ctx, sub.channel.clone(), item.delivery.envelope.clone(), targets, &mut counters,
                    ).await {
                        break;
                    }
                }
            }
            // A live durable row from the router fan-out. Skipped (with a debug
            // log) when the channel is no longer active (unsubscribed while
            // queued) or when the subscribe replay already sent this seq.
            Some(delivery) = durable_rx.recv() => {
                // Take every co-available row before writing: the router queues one
                // message's sibling rows back to back, so they coalesce into one
                // frame.
                let mut batch = vec![delivery];
                while let Ok(next) = durable_rx.try_recv() {
                    batch.push(next);
                }
                if let FrameOutcome::Disconnect =
                    send_durable_live(&ctx, &durable, &mut spans, batch, &mut counters).await
                {
                    break;
                }
            }
            // Eager-wake nudge: drain every active durable channel's parked rows.
            _ = drain_notify.notified() => {
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
        let epoch = ctx.runtime.bus.epoch();
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
/// and forget the other. `replay_sent` records, per channel, the message ids the
/// subscribe replay already put on the wire, so a live row racing the replay is
/// delivered exactly once; its entries persist for the **connection** lifetime,
/// not the subscription span, so a re-subscribe merges into the retained set
/// rather than rebuilding it — closing the duplicate a queued live copy would
/// otherwise be across an unsubscribe/re-subscribe cycle. Each per-channel set is
/// hard-capped at [`REPLAY_SENT_MAX`]; see that constant for the bound argument.
struct DurableSessionState {
    active: HashSet<SubKey>,
    shared: Arc<Mutex<HashSet<SubKey>>>,
    replay_sent: HashMap<SubKey, HashSet<i64>>,
}

impl DurableSessionState {
    fn new(shared: Arc<Mutex<HashSet<SubKey>>>) -> Self {
        Self {
            active: HashSet::new(),
            shared,
            replay_sent: HashMap::new(),
        }
    }

    /// Insert the subscription into both the local and registry-shared active
    /// sets and ensure a `replay_sent` entry exists. Inserting into `shared` is
    /// what makes the router start claiming and queuing live rows, so callers
    /// activate before the drain lock. An existing `replay_sent` entry from an
    /// earlier subscription span is retained (merged into), never cleared.
    fn activate(&mut self, sub: &SubKey) {
        self.shared
            .lock()
            .expect("durable_subs poisoned")
            .insert(sub.clone());
        self.active.insert(sub.clone());
        self.replay_sent.entry(sub.clone()).or_default();
    }

    /// Remove the subscription from both active sets, **retaining** its
    /// `replay_sent` entry for the connection lifetime. Returns whether it was
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

    /// Record a replayed message id. Returns `false` when the per-channel set is
    /// already at [`REPLAY_SENT_MAX`] and this id is new — the caller tears the
    /// connection down. A repeat id is always accepted (it adds no growth).
    fn record_replayed(&mut self, sub: &SubKey, seq: i64) -> bool {
        let set = self.replay_sent.entry(sub.clone()).or_default();
        if set.contains(&seq) {
            return true;
        }
        if set.len() >= REPLAY_SENT_MAX {
            return false;
        }
        set.insert(seq);
        true
    }

    fn already_replayed(&self, sub: &SubKey, seq: i64) -> bool {
        self.replay_sent.get(sub).is_some_and(|s| s.contains(&seq))
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
/// Confirm-set depth past which the server asks the kernel to re-anchor the
/// subscription, so the reconcile that empties the set runs without waiting for
/// a reconnect. A trigger, not a gate: deliveries continue and the set keeps
/// absorbing entries while the ask is outstanding.
///
/// A constant, not config: no operator could state what a per-subscription
/// confirm-set depth means. Tune it here.
pub(super) const CONFIRM_SET_SOFT_CAP: usize = 64;

/// Confirm-set depth at which a client is judged to have ignored the re-anchor
/// ask. A conforming kernel resubscribes promptly, so reaching this is a
/// non-conforming client and takes the ordinary violation posture: kill + log.
///
/// Never silent truncation — dropping entries would silently convert delivered
/// rows into presumed-lost ones at the next reconcile.
///
/// TODO(confirm-set-hard-cap-e2e): the confirm-set-specific Violation wiring
/// (`add_confirm` → `ConfirmCapAction::Violation` → session kill) has only unit
/// coverage on `add_confirm`; no ws test drives a client past the hard cap and
/// asserts the kill. The parked-replay path does not appear to enforce the hard
/// cap (it keeps the connection alive), so a correct e2e test needs a live-send
/// scenario — and whether replay should enforce the cap at all wants a design
/// answer before the test is written.
const CONFIRM_SET_HARD_CAP: usize = 256;

const _: () = assert!(CONFIRM_SET_SOFT_CAP < CONFIRM_SET_HARD_CAP);

/// What recording a confirm-set entry obliges the caller to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfirmCapAction {
    /// Set is within the soft cap, or an ask is already outstanding.
    None,
    /// Set passed the soft cap with no ask outstanding: send one `ReAnchor`.
    ReAnchor,
    /// Set reached the hard cap: the client ignored the ask. Violation.
    Violation,
}

struct WireSpans {
    span_seq: HashMap<SubKey, u64>,
    durable_high_water: HashMap<SubKey, i64>,
    /// Per-durable-subscription below-water confirm set: the message ids
    /// of below-water rows written to the socket this connection, each carried in
    /// every durable cursor minted for the subscription until the client echoes a
    /// cursor confirming it at the next resume. Entries are only added within a
    /// connection (the confirm signal arrives via an echoed cursor at the *next*
    /// resume), so on a single never-reconnecting connection the set grows with the
    /// number of below-water sends since the last reconcile — bounded by the
    /// resolved `push_depth` at any moment a resume clears it. Past
    /// [`CONFIRM_SET_SOFT_CAP`] the server asks the kernel to re-anchor the
    /// subscription ([`ServerFrame::ReAnchor`]), whose resubscribe runs the
    /// reconcile and empties the set; at [`CONFIRM_SET_HARD_CAP`] a client that
    /// ignored the ask is non-conforming and the session is killed. See
    /// [`WireSpans::add_confirm`].
    confirm_set: HashMap<SubKey, BTreeSet<i64>>,
    /// Subscriptions with a `ReAnchor` sent and no resubscribe yet. Suppresses
    /// repeat asks while one is outstanding; cleared by [`WireSpans::clear`],
    /// which every (re)subscribe and teardown runs.
    reanchor_pending: HashSet<SubKey>,
    /// The store's durable identity `(generation, incarnation)`, read once from
    /// the DB at the first durable `Subscribe` on the connection and stamped into
    /// every durable cursor minted thereafter. Constant for the connection's life
    /// (the store cannot re-boot under a live page), so a single read suffices.
    store_identity: Option<db::StoreIdentity>,
}

impl WireSpans {
    fn new() -> Self {
        Self {
            span_seq: HashMap::new(),
            durable_high_water: HashMap::new(),
            confirm_set: HashMap::new(),
            reanchor_pending: HashSet::new(),
            store_identity: None,
        }
    }

    /// Record the store identity read at a durable `Subscribe`. Idempotent: a
    /// second durable subscribe on the same connection re-reads the same values.
    fn set_store_identity(&mut self, identity: db::StoreIdentity) {
        self.store_identity = Some(identity);
    }

    /// Reset the span counter for `sub` to 0. Called at every successful
    /// ephemeral `Subscribe`, before the `SubscribeResult` and replay, so the
    /// span's first `Deliver` mints seq 1.
    fn start_span(&mut self, sub: &SubKey) {
        self.span_seq.insert(sub.clone(), 0);
    }

    /// Reset the span counter for `sub` to 0 and anchor its durable high-water.
    /// Called at every successful durable `Subscribe`: a fresh attach anchors at
    /// 0, a resume anchors at the parsed cursor's high-water.
    fn start_durable_span(&mut self, sub: &SubKey, anchor_high_water: i64) {
        self.span_seq.insert(sub.clone(), 0);
        self.durable_high_water
            .insert(sub.clone(), anchor_high_water);
    }

    /// Drop all wire state for `sub` (unsubscribe / teardown).
    fn clear(&mut self, sub: &SubKey) {
        self.span_seq.remove(sub);
        self.durable_high_water.remove(sub);
        self.confirm_set.remove(sub);
        self.reanchor_pending.remove(sub);
    }

    /// The subscription's current durable high-water — the value a durable send
    /// is *below* when its row id is at or under it. Read before [`next_durable`]
    /// advances it, so a below-water send can be detected and stamped. `None` if
    /// no durable span was anchored (no durable `Subscribe` yet).
    fn durable_high_water_of(&self, sub: &SubKey) -> Option<i64> {
        self.durable_high_water.get(sub).copied()
    }

    /// Record `message_id` in the subscription's below-water confirm set, so
    /// every durable cursor minted thereafter carries it as ack evidence, and
    /// report what the resulting depth obliges the caller to do.
    ///
    /// The set only shrinks at a resume's reconcile, so on a connection that
    /// never reconnects it — and every cursor carrying it — grows with the
    /// below-water sends since the last reconcile. The caps bound that: past the
    /// soft cap the caller asks for a re-anchor (one ask per outstanding
    /// re-anchor, [`Self::reanchor_pending`]); at the hard cap the ask was
    /// ignored and the client is non-conforming.
    fn add_confirm(&mut self, sub: &SubKey, message_id: i64) -> ConfirmCapAction {
        let set = self.confirm_set.entry(sub.clone()).or_default();
        set.insert(message_id);
        let len = set.len();
        if len >= CONFIRM_SET_HARD_CAP {
            return ConfirmCapAction::Violation;
        }
        if len > CONFIRM_SET_SOFT_CAP && self.reanchor_pending.insert(sub.clone()) {
            return ConfirmCapAction::ReAnchor;
        }
        ConfirmCapAction::None
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
        (seq, cursor::mint_ephemeral(epoch, ring_seq))
    }

    /// The `(span seq, cursor)` for a durable `Deliver` of the row `row_id`. The
    /// high-water advances to `max(high_water, row_id)` and the cursor is minted
    /// from the advanced high-water.
    fn next_durable(&mut self, sub: &SubKey, row_id: i64) -> (u64, Cursor) {
        let seq = self.next_seq(sub);
        let identity = self.store_identity.expect(
            "surface session: durable Deliver before the store identity was read at Subscribe",
        );
        let hw = self.durable_high_water.get_mut(sub).expect(
            "surface session: durable Deliver on a subscription with no anchored high-water",
        );
        *hw = (*hw).max(row_id);
        let hw_value = *hw;
        let confirm: Vec<i64> = self
            .confirm_set
            .get(sub)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        (
            seq,
            cursor::mint_durable(identity.generation, identity.incarnation, hw_value, confirm),
        )
    }
}

/// Which class of `Deliver` [`send_deliver`] is minting position for.
enum DeliverKind {
    Ephemeral { epoch: Uuid, ring_seq: u64 },
    Durable { row_id: i64 },
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
        DeliverKind::Durable { row_id } => spans.next_durable(sub, row_id),
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

/// Send a durable `Deliver`, stamping the below-water ack first when the row
/// needs one.
///
/// A row needs the ack channel when it is at or below the subscription's current
/// high-water (`row_id <= hw`) *or* when `force_stamp` is set. `force_stamp` is
/// used by the subscribe replay for a row it just unclaimed from the tentative set:
/// such a row was below-water on an earlier connection and its fresh redelivery
/// carries no high-water ack (a fresh attach anchors the high-water at 0), so
/// without the re-stamp a second lost frame would leave it delivered-but-
/// unrecoverable. An above-water row that advances the high-water (the common case)
/// is sent unstamped: the minted cursor's high-water advance *is* its
/// acknowledgment, and WS frame ordering makes an echoed high-water covering the id
/// proof of receipt.
///
/// Before the socket write a stamped row sets `confirm_pending = 1` on the claimed
/// push row (a DB write ordered ahead of the send) and enters the id in the confirm
/// set every durable cursor then carries, giving the next resume's reconcile the
/// receipt evidence to redeliver a lost row exactly once.
async fn send_durable_deliver(
    ctx: &SessionCtx,
    spans: &mut WireSpans,
    sub: &SubKey,
    envelope: MessageEnvelope,
    row_id: i64,
    force_stamp: bool,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let cap = stamp_below_water(ctx, spans, sub, row_id, force_stamp).await;
    if cap == ConfirmCapAction::Violation {
        return hard_cap_violation(ctx, sub);
    }
    let outcome = send_deliver(
        ctx,
        spans,
        sub,
        envelope,
        0,
        DeliverKind::Durable { row_id },
        counters,
    )
    .await;
    // The ask rides behind the delivery it was triggered by: the delivery is
    // owed regardless (the cap is a trigger, not a gate), and the kernel's
    // re-anchor is correct whatever it just accepted.
    if matches!(outcome, FrameOutcome::Continue) && cap == ConfirmCapAction::ReAnchor {
        return send_re_anchor(ctx, sub, counters).await;
    }
    outcome
}

/// Stamp the below-water ack for one subscription's durable row when it needs
/// one — the DB write ordered ahead of the row's socket write. See
/// [`send_durable_deliver`] for the full rules. Returns what the stamped id's
/// confirm-set depth obliges the caller to do ([`WireSpans::add_confirm`]); a
/// send that stamps nothing obliges nothing.
async fn stamp_below_water(
    ctx: &SessionCtx,
    spans: &mut WireSpans,
    sub: &SubKey,
    row_id: i64,
    force_stamp: bool,
) -> ConfirmCapAction {
    let below_water = force_stamp
        || spans
            .durable_high_water_of(sub)
            .is_some_and(|hw| row_id <= hw);
    if below_water {
        let messenger = ctx.runtime.messenger.as_ref().unwrap_or_else(|| {
            panic!(
                "surface {}: below-water durable deliver on {} but no Messenger — \
                 boot invariant violated",
                ctx.runtime.resolved.slug, sub.channel
            )
        });
        let participant = sub.participant(&ctx.runtime.resolved.slug);
        // The stamp is one indexed single-row UPDATE per below-water send, taken
        // on the same code path for replay and live-drain sends alike. A resume
        // could in principle batch its below-water stamps under one lock, but the
        // count is bounded by the subscription's resolved push_depth (which is
        // never unbounded on a surface binding), so the uniform per-row path is
        // kept for simplicity over a reconnect-only batch fast path.
        //
        // The claim and this stamp are separate lock acquisitions (the claim runs
        // under the subscribe/drain lock or in the router; the stamp here), so the
        // hourly GC can evict the claimed row in the gap. A stamp that matches 0
        // rows is that eviction when the row is gone — an expected transient, not a
        // bug — and the row is genuinely unrecoverable then (its message aged out
        // of retention too), so the delivery goes out unstamped. A row still
        // present but unstamped is a real invariant break and panics.
        {
            let conn = messenger.db().lock().await;
            let stamped = db::stamp_confirm_pending(&conn, &participant, row_id);
            if stamped == 1 {
                return spans.add_confirm(sub, row_id);
            } else if db::pending_push_exists(&conn, &participant, row_id) {
                panic!(
                    "surface {}: below-water durable deliver on {} stamped {stamped} claimed rows \
                     for message {row_id} (participant {participant:?}); the push row is present \
                     but was not claimed — the ack channel's recovery evidence was not written",
                    ctx.runtime.resolved.slug, sub.channel
                );
            } else {
                warn!(
                    channel = %sub.channel,
                    instance = ?sub.instance,
                    message_id = row_id,
                    "surface durable below-water deliver: push row GC-evicted before the \
                     tentative stamp; delivering unstamped (row already past retention)"
                );
            }
        }
    }
    ConfirmCapAction::None
}

/// Send one turn's worth of live durable router deliveries, coalescing the rows
/// of one message across this connection's sibling subscriptions into one frame.
///
/// Groups by (channel, row id) in first-appearance order, which preserves each
/// subscription's own delivery order: a subscription appears in at most one group
/// per row, and the router never queues a row twice for it.
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
        row_id: i64,
        envelope: Arc<MessageEnvelope>,
        subs: Vec<SubKey>,
    }
    let mut groups: Vec<RowGroup> = Vec::new();
    for DurableDelivery { envelope, seq, sub } in batch {
        if !durable.is_active(&sub) {
            debug!(
                channel = %sub.channel,
                instance = ?sub.instance,
                seq,
                "durable live delivery for inactive subscription; dropping"
            );
            continue;
        }
        if durable.already_replayed(&sub, seq) {
            debug!(
                channel = %sub.channel,
                instance = ?sub.instance,
                seq,
                "durable live delivery already sent by replay; dropping"
            );
            continue;
        }
        match groups
            .iter_mut()
            .find(|g| g.row_id == seq && g.channel == sub.channel)
        {
            Some(group) => group.subs.push(sub),
            None => groups.push(RowGroup {
                channel: sub.channel.clone(),
                row_id: seq,
                envelope,
                subs: vec![sub],
            }),
        }
    }
    for RowGroup {
        channel,
        row_id,
        envelope,
        subs,
    } in groups
    {
        let mut targets = Vec::with_capacity(subs.len());
        let mut re_anchor = Vec::new();
        for sub in &subs {
            match stamp_below_water(ctx, spans, sub, row_id, false).await {
                ConfirmCapAction::Violation => return hard_cap_violation(ctx, sub),
                ConfirmCapAction::ReAnchor => re_anchor.push((*sub).clone()),
                ConfirmCapAction::None => {}
            }
            targets.push(mint_target(spans, sub, 0, DeliverKind::Durable { row_id }));
        }
        if let FrameOutcome::Disconnect =
            send_multi_deliver(ctx, channel, (*envelope).clone(), targets, counters).await
        {
            return FrameOutcome::Disconnect;
        }
        // The asks ride behind the delivery that triggered them: the delivery is
        // owed regardless (the cap is a trigger, not a gate), and the kernel's
        // re-anchor is correct whatever it just accepted.
        for sub in re_anchor {
            if let FrameOutcome::Disconnect = send_re_anchor(ctx, &sub, counters).await {
                return FrameOutcome::Disconnect;
            }
        }
    }
    FrameOutcome::Continue
}

/// Ask the kernel to re-anchor one subscription, so the resume reconcile that
/// empties its confirm set runs without waiting for a reconnect.
async fn send_re_anchor(
    ctx: &SessionCtx,
    sub: &SubKey,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    debug!(
        channel = %sub.channel,
        instance = ?sub.instance,
        "surface durable confirm set past the soft cap; asking the client to re-anchor"
    );
    send_frame(
        &ctx.tx,
        ServerFrame::ReAnchor {
            channel: sub.channel.clone(),
            instance: sub.instance.clone(),
        },
        counters,
    )
    .await
}

/// The confirm set reached the hard cap: the client was asked to re-anchor and
/// did not. A conforming kernel resubscribes promptly, so this is a
/// non-conforming client under the ordinary violation posture.
fn hard_cap_violation(ctx: &SessionCtx, sub: &SubKey) -> FrameOutcome {
    FrameOutcome::Violation(format!(
        "surface {}: confirm set for {} (instance {:?}) reached the hard cap of \
         {CONFIRM_SET_HARD_CAP} — the client ignored the ReAnchor sent past \
         {CONFIRM_SET_SOFT_CAP}",
        ctx.runtime.resolved.slug, sub.channel, sub.instance
    ))
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
        } => handle_publish_batch(ctx, &instance, correlation, &publishes, counters).await,
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
        } => {
            handle_status(
                ctx,
                &mut buckets.publish,
                &instances,
                uptime_secs,
                counters,
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
    instances: &[InstanceReport],
    uptime_secs: u64,
    counters: StatusCounters,
    last_status_instances: &mut Vec<InstanceReport>,
) -> FrameOutcome {
    let slug = &ctx.runtime.resolved.slug;
    let username = &ctx.username;
    let description = &ctx.runtime.description;
    // Configured instance → kind, and the pump count each instance should have,
    // both precomputed once at boot on the description runtime (boot-constant, so
    // not rebuilt per frame). The shell may report only configured instances.
    if let Err(rule) =
        telemetry::validate_status(instances, &counters, &description.configured_kinds)
    {
        return FrameOutcome::Violation(format!(
            "surface {slug} user {username}: {}",
            sanitize_client_detail(&rule)
        ));
    }
    // Retain the validated report so a teardown terminal snapshot can carry
    // the last-known instances. Recorded even if the publish bucket later denies
    // this frame — the report itself is a truthful, well-formed observation.
    *last_status_instances = instances.to_vec();

    let health = telemetry::derive_health(instances, &description.expected_pumps);
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
        ctx.runtime.bus.epoch(),
        health,
        uptime_secs,
        instances,
        counters,
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
/// channel from both active sets (so the router stops fanning out to it) but
/// **retains** its `replay_sent` set: a row still queued from this span that a
/// re-subscribe would otherwise re-deliver is dropped by the select loop's
/// `replay_sent` skip, which now spans subscription cycles. Any already-claimed
/// rows still queued stand (the client revoked interest — not a loss bug).
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

/// The wire class a subscribe expects its echoed resume [`Cursor`] to carry.
enum ExpectClass {
    Ephemeral,
    Durable,
}

/// Parse an echoed resume cursor for a subscribe of the given class, mapping both
/// failure shapes to the protocol violation they are: a class mismatch (a durable
/// cursor on an ephemeral channel, or the reverse) and an unparseable cursor. A
/// conforming client can produce neither — cursors live only in page memory and
/// the build gate forces a reload before a stale-format page reconnects — so both
/// kill the connection and log for fail2ban, the unparseable arm carrying the
/// parse cause. On `Ok` the returned [`CursorState`] is guaranteed to match
/// `expect`. One helper owns both violation messages so the two subscribe paths
/// cannot drift as `CursorState` grows.
fn parse_resume_cursor(
    cursor: &Cursor,
    expect: ExpectClass,
    slug: &str,
    username: &str,
    channel: &str,
) -> Result<CursorState, FrameOutcome> {
    match cursor::parse(cursor) {
        Ok(state @ CursorState::Ephemeral { .. }) if matches!(expect, ExpectClass::Ephemeral) => {
            Ok(state)
        }
        Ok(state @ CursorState::Durable { .. }) if matches!(expect, ExpectClass::Durable) => {
            Ok(state)
        }
        Ok(_) => {
            let (got, want) = match expect {
                ExpectClass::Ephemeral => ("durable", "ephemeral"),
                ExpectClass::Durable => ("ephemeral", "durable"),
            };
            Err(FrameOutcome::Violation(format!(
                "surface {slug} user {username}: {got} resume on {want} channel {channel}"
            )))
        }
        Err(detail) => Err(FrameOutcome::Violation(format!(
            "surface {slug} user {username}: unparseable resume cursor on {channel}: {detail}"
        ))),
    }
}

/// Handle a `Subscribe` frame.
///
/// Validates the channel against the surface's config bindings and both active
/// subscription sets (ephemeral + durable), then dispatches on delivery class.
/// Durable channels project the backlog and (on resume) the retained window;
/// ephemeral channels attach the broadcast stream. The FIFO writer queue
/// serializes `SubscribeResult` → replay → live deliveries, so ordering holds by
/// construction.
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
        // ephemeral arm, whose `EphemeralBus rejected bound channel` panic would
        // misdiagnose it as missing boot ACL coverage.
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
        Some(cursor) => {
            match parse_resume_cursor(
                &cursor,
                ExpectClass::Ephemeral,
                slug,
                username,
                &sub.channel,
            ) {
                Ok(CursorState::Ephemeral { epoch, seq }) => Some(EphemeralResume { epoch, seq }),
                Ok(CursorState::Durable { .. }) => {
                    unreachable!("parse_resume_cursor(Ephemeral) returns only an ephemeral state")
                }
                Err(outcome) => return outcome,
            }
        }
    };

    let subscription = match runtime.bus.subscribe(
        // The subscribing principal, at the grain it subscribed: an ephemeral
        // subscription opens no push window and keeps no cursor, so nothing here
        // is keyed by it — but the bus's ACL check and its own attribution
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
            "surface {slug}: EphemeralBus rejected bound channel {}: {err:?} — boot validation \
             guarantees every bound channel exists and is policy-covered",
            sub.channel
        ),
    };

    // A matching-epoch resume seq the bus never assigned is impossible for an
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
        Replay::Gap(BusGapReason::HoleExceedsRing) => Some(GapInfo {
            reason: ProtoGapReason::HoleExceedsRing,
        }),
        Replay::Gap(BusGapReason::ResumeAhead) => {
            unreachable!("ResumeAhead escalated to a violation above")
        }
    };

    let epoch = runtime.bus.epoch();
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
            delivery.envelope.clone(),
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

/// One entry in a durable subscribe's merged replay set: `(seq = message_id,
/// `Some(push_id)` for a claimed parked row or `None` for a retained re-send,
/// envelope)`. `push_id` distinguishes rows that must be un-claimed if the
/// session disconnects mid-replay from retained rows that own no push claim.
type DurableReplayRow = (i64, Option<i64>, MessageEnvelope);

/// Handle a `Subscribe` to a durable (`brenn:`) channel.
///
/// Activates the subscription (so the router routes live rows here), then under
/// one DB lock builds the replay set — claimed parked rows plus, on resume, the
/// retained window with `id > last_seq` — computes the [`GapReason::BeyondRetained`]
/// gap, and sends `SubscribeResult` followed by the merged replay in `m.id`
/// order. Every replayed id is recorded in `replay_sent` so a live row racing the
/// activation-to-lock window (queued to `durable_rx` *and* present in the retained
/// re-send) is dropped by the live arm, preserving at-most-once on the wire.
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
    // The principal whose push window this subscription drains. Every parked-row
    // claim below is made under it, so a sibling instance's rows on the same
    // channel are simply not this subscription's to claim.
    let participant = sub.participant(slug);

    // The echoed cursor, parsed to its `(generation, incarnation, high_water)` and
    // its below-water confirm set. Staleness against the current store
    // identity is decided under the DB lock below (it reads the store), because a
    // replaced/wiped/restored store makes a syntactically valid cursor point into a
    // store that no longer exists.
    let (resume_durable, echoed_confirm): (Option<(Uuid, i64, i64)>, HashSet<i64>) = match resume {
        None => (None, HashSet::new()),
        Some(cursor) => {
            match parse_resume_cursor(&cursor, ExpectClass::Durable, slug, username, &sub.channel) {
                Ok(CursorState::Durable {
                    generation,
                    incarnation,
                    high_water,
                    confirm,
                }) => (
                    Some((generation, incarnation, high_water)),
                    confirm.into_iter().collect(),
                ),
                Ok(CursorState::Ephemeral { .. }) => {
                    unreachable!("parse_resume_cursor(Durable) returns only a durable state")
                }
                Err(outcome) => return outcome,
            }
        }
    };

    // The resolved subscription carries the channel uuid and the retain clamp.
    // Boot classified this (instance, channel) Durable, so it must be present.
    let resolved = ctx.runtime.durable_subscription(&sub);
    let channel_uuid = resolved.channel_uuid;
    let clamp = resolved.retain_depth;
    let messenger = ctx.runtime.messenger.as_ref().unwrap_or_else(|| {
        panic!(
            "surface {slug}: durable subscribe on {} but no Messenger — \
             SurfaceRuntime::build should have rejected this at boot",
            sub.channel
        )
    });

    // Activate before the drain lock: from here the router claims and queues live
    // rows; anything it claims is excluded from the parked load by `delivered_at`,
    // and the retained re-send + `replay_sent` close the handoff race.
    durable.activate(&sub);

    // The subscribe rate is bucketed in `handle_client_frame`, and boot proved the
    // resolved `push_depth`/`retain_depth` of a durable surface binding are both
    // bounded, so the parked backlog and the retained re-send below are each
    // config-bounded — the load per Subscribe cannot be amplified into a DoS.
    //
    // Build the replay set + gap under one DB lock. `merged` entries are
    // `(seq = message_id, Some(push_id) for a claimed parked row | None for a
    // retained re-send, envelope)`.
    let (mut merged, gap, effective_anchor, unacked_message_ids): (
        Vec<DurableReplayRow>,
        Option<GapInfo>,
        i64,
        HashSet<i64>,
    ) = {
        let conn = messenger.db().lock().await;

        // Read the store identity once and stamp it into the connection's span
        // state so every durable cursor minted this connection carries it.
        let store = db::read_store_identity(&conn);
        spans.set_store_identity(store);

        // Apply the three stale-store arms. Each proves
        // the cursor was minted against a store that no longer exists — replaced,
        // wiped, or restored from backup — and is reachable by a conforming
        // client, so none is a violation: all are answered as a fresh attach
        // (full retained window from the tail) with an `EpochChanged` gap, the
        // same shape a stale-epoch ephemeral cursor already gets.
        let (last_seq, forced_gap): (Option<i64>, Option<GapInfo>) = match resume_durable {
            None => (None, None),
            Some((generation, incarnation, high_water)) => {
                let stale = if generation != store.generation {
                    // The messaging DB was replaced under a live page.
                    Some("generation mismatch")
                } else if incarnation > store.incarnation {
                    // The DB was restored from backup and the cursor was minted
                    // under a boot the restored store never counted.
                    Some("incarnation above store")
                } else if match db::channel_max_message_id(&conn, channel_uuid) {
                    Some(max) => high_water > max,
                    // An empty channel is stale only when the cursor claims to
                    // have seen a row (`hw >= 1`) that no longer exists; a
                    // never-delivered anchor (`hw == 0`) is an ordinary resume.
                    None => high_water > 0,
                } {
                    // The DB was restored and reconnected before new rows
                    // re-climbed the id space (subsumes the emptied-channel case).
                    Some("high-water above channel max")
                } else {
                    None
                };
                match stale {
                    Some(reason) => {
                        warn!(
                            channel = %sub.channel,
                            instance = ?sub.instance,
                            reason,
                            cursor_generation = %generation,
                            cursor_incarnation = incarnation,
                            cursor_high_water = high_water,
                            store_generation = %store.generation,
                            store_incarnation = store.incarnation,
                            "surface durable resume: stale-store cursor; answering as fresh attach"
                        );
                        (
                            None,
                            Some(GapInfo {
                                reason: ProtoGapReason::EpochChanged,
                            }),
                        )
                    }
                    None => (Some(high_water), None),
                }
            }
        };

        // Below-water ack reconcile, before the parked claim. Each
        // tentative (`confirm_pending = 1`) row for this participant is either
        // confirmed (in the echoed set → the client received it → clear the flag,
        // leave it delivered) or unclaimed (absent → never received → clear both
        // flag and `delivered_at` so the parked claim just below redelivers it
        // exactly once, excluded from the retained window by `parked_ids`). A fresh
        // attach or a stale-store cursor carries no valid evidence (`last_seq` is
        // None), so its set is discarded and every tentative row is unclaimed.
        let empty_confirm = HashSet::new();
        let effective_confirm = if last_seq.is_some() {
            &echoed_confirm
        } else {
            &empty_confirm
        };
        // The message ids of tentative rows this reconcile unclaimed for
        // redelivery — a previously below-water row whose receipt the echoed cursor
        // does not testify to. Their parked redelivery below is itself force-stamped
        // (they carry no fresh high-water ack — a fresh attach anchors at 0), so a
        // second lost frame is still recoverable.
        let mut unacked_message_ids: HashSet<i64> = HashSet::new();
        let tentative = db::load_confirm_pending_pushes(&conn, &participant, channel_uuid);
        if !tentative.is_empty() {
            // A row already sent on *this* connection is delivered: a same-connection
            // re-subscribe means the socket is still up, and WS/TCP delivered every
            // earlier frame in order. Treat it as confirmed even if the echoed cursor
            // predates the send — otherwise the unclaim below clears its evidence and
            // the replay-dedup drops the re-send, destroying recovery state with no
            // new delivery.
            let (confirmed, unacked): (Vec<_>, Vec<_>) =
                tentative.into_iter().partition(|(_, message_id)| {
                    effective_confirm.contains(message_id)
                        || durable.already_replayed(&sub, *message_id)
                });
            let confirmed_ids: Vec<i64> =
                confirmed.into_iter().map(|(push_id, _)| push_id).collect();
            let unacked_ids: Vec<i64> = unacked
                .into_iter()
                .map(|(push_id, message_id)| {
                    unacked_message_ids.insert(message_id);
                    push_id
                })
                .collect();
            db::confirm_pending_pushes(&conn, &confirmed_ids);
            db::unclaim_confirm_pending_pushes(&conn, &unacked_ids);
        }

        let parked = load_and_claim_parked(&conn, &participant, channel_uuid);
        let parked_ids: HashSet<i64> = parked
            .iter()
            .map(|(_, message_id, _)| *message_id)
            .collect();

        let (retained, window_gap) = match last_seq {
            // A fresh attach (no resume token) receives the channel's most recent
            // rows clamped to `retain_depth`, anchored at the window tail — the
            // same retained-window read as the resume arm, from message id 0.
            // Nothing was missed, so no gap is synthesized.
            None => {
                let window = db::load_channel_messages_after(&conn, channel_uuid, 0, clamp);
                let retained: Vec<(i64, MessageEnvelope)> = window
                    .into_iter()
                    .filter(|(message_id, _)| !parked_ids.contains(message_id))
                    .collect();
                (retained, None)
            }
            Some(ls) => {
                let window = db::load_channel_messages_after(&conn, channel_uuid, ls, clamp);
                // A full bounded window may have dropped older `id > last_seq`
                // rows (conservative — a false "may have missed" is honest).
                let truncated = matches!(clamp, Depth::Bounded(n) if window.len() as u64 == n);
                // `last_seq` below the oldest retained id ⇒ intervening rows
                // evicted; an empty channel ⇒ oldest is +∞ ⇒ any resume gaps.
                let oldest = db::channel_min_message_id(&conn, channel_uuid);
                let beyond = match oldest {
                    Some(min_id) => ls < min_id,
                    None => true,
                };
                let gap = (beyond || truncated).then_some(GapInfo {
                    reason: ProtoGapReason::BeyondRetained,
                });
                let retained: Vec<(i64, MessageEnvelope)> = window
                    .into_iter()
                    .filter(|(message_id, _)| !parked_ids.contains(message_id))
                    .collect();
                if gap.is_some() {
                    // The gap decision reads moving DB state (GC advances the
                    // retained window), so record its inputs — otherwise a "may
                    // have missed data" reported to the client is unreconstructible
                    // after the window has since moved. Fires at most once per
                    // Subscribe (rate-bucketed).
                    debug!(
                        channel = %sub.channel,
                        instance = ?sub.instance,
                        last_seq = ls,
                        oldest_retained = ?oldest,
                        truncated,
                        beyond,
                        parked = parked_ids.len(),
                        retained = retained.len(),
                        "surface durable resume: reporting BeyondRetained gap"
                    );
                }
                (retained, gap)
            }
        };

        // A stale-store cursor forces the `EpochChanged` gap over whatever the
        // (fresh-attach) window arm concluded; otherwise the window's own gap
        // decision stands.
        let gap = forced_gap.or(window_gap);

        let merged: Vec<DurableReplayRow> = parked
            .into_iter()
            .map(|(push_id, message_id, env)| (message_id, Some(push_id), env))
            .chain(
                retained
                    .into_iter()
                    .map(|(message_id, env)| (message_id, None, env)),
            )
            .collect();
        (merged, gap, last_seq.unwrap_or(0), unacked_message_ids)
    };
    merged.sort_by_key(|(message_id, _, _)| *message_id);

    // Anchor the durable high-water and reset the span before the
    // SubscribeResult, so the replay rows mint seqs 1..N and the high-water
    // starts at the (non-stale) resume cursor, or 0 on a fresh or stale-store
    // attach.
    spans.start_durable_span(&sub, effective_anchor);

    // At-most-once dedup: a row whose seq was already sent on this connection
    // (the retained `replay_sent` set) is already delivered — drop the copy and
    // leave any claimed push row claimed (retired), never re-send. This is the
    // same skip the live `durable_rx` arm applies, extended to the replay path
    // so a row unclaimed and re-claimed across a session/subscription cycle
    // cannot re-appear on the wire. Dropped parked rows stay claimed (they are
    // excluded from the unclaim-on-disconnect remainder below), so no later
    // drain resurrects them.
    merged.retain(|(seq, _push_id, _)| !durable.already_replayed(&sub, *seq));

    // Floor parity: this gates every session-side durable send — belt-and-
    // suspenders for parked rows, the only floor for the retained re-send.
    // Policies are boot-static, so a deny is fail-closed hygiene, not a feature.
    let floor_ok = ctx.runtime.policy.allows_channel_access(&sub.channel);
    let replay_count = if floor_ok { merged.len() as u32 } else { 0 };

    let result = ServerFrame::SubscribeResult {
        channel: sub.channel.clone(),
        instance: sub.instance.clone(),
        outcome: SubscribeOutcome::Ok,
        replay_count,
        gap,
    };
    if let FrameOutcome::Disconnect = send_frame(&ctx.tx, result, counters).await {
        unclaim_parked(messenger, merged.iter().map(|(_, push_id, _)| *push_id)).await;
        return FrameOutcome::Disconnect;
    }

    if !floor_ok {
        // Rows stay claimed (retired), matching the dispatcher floor's retire
        // semantics; the wire sees an empty replay.
        warn!(
            channel = %sub.channel,
            instance = ?sub.instance,
            "surface durable subscribe: delivery floor denied; retiring claimed rows"
        );
        return FrameOutcome::Continue;
    }

    for (i, (seq, _push_id, envelope)) in merged.iter().enumerate() {
        // A row this reconcile unclaimed from the tentative set is force-stamped on
        // redelivery: it was below-water on an earlier connection and carries no
        // fresh high-water ack here (a fresh attach anchors at 0), so without the
        // re-stamp a second lost frame would leave it delivered-but-unrecoverable.
        // Ordinary rows (first-time parked rows, retained re-sends) keep the
        // high-water ack — their delivery advances the cursor to cover them.
        let force_stamp = unacked_message_ids.contains(seq);
        if let FrameOutcome::Disconnect = send_durable_deliver(
            ctx,
            spans,
            &sub,
            envelope.clone(),
            *seq,
            force_stamp,
            counters,
        )
        .await
        {
            unclaim_parked(
                messenger,
                merged[i..].iter().map(|(_, push_id, _)| *push_id),
            )
            .await;
            return FrameOutcome::Disconnect;
        }
        if !durable.record_replayed(&sub, *seq) {
            // The per-channel replay-dedup set hit REPLAY_SENT_MAX: tear the
            // connection down (normal close, not a violation — reachable by an
            // honest very-long-lived connection). Row `i` was already sent; unclaim
            // the still-unsent remainder so a later drain re-sends it.
            warn!(
                channel = %sub.channel,
                instance = ?sub.instance,
                cap = REPLAY_SENT_MAX,
                "surface durable replay-dedup set full; tearing down connection"
            );
            unclaim_parked(
                messenger,
                merged[i + 1..].iter().map(|(_, push_id, _)| *push_id),
            )
            .await;
            return FrameOutcome::Disconnect;
        }
    }

    FrameOutcome::Continue
}

/// Un-claim the parked push rows among `push_ids` (the `Some` values) so they
/// re-park for a later drain. Used only by a claimer that disconnected before
/// handing off the rows it claimed.
async fn unclaim_parked(messenger: &Messenger, push_ids: impl Iterator<Item = Option<i64>>) {
    let remainder: Vec<i64> = push_ids.flatten().collect();
    if !remainder.is_empty() {
        let conn = messenger.db().lock().await;
        db::unclaim_pending_pushes(&conn, &remainder);
    }
}

/// Load the parked backlog for `(subscriber, channel)` and atomically claim it,
/// returning only the `(push_id, message_id, envelope)` rows this caller won.
/// Both durable send paths — the subscribe replay and the drain pass — share it
/// so the claim semantics the at-most-once guarantee depends on stay identical.
/// The caller holds the DB lock (`conn`) so load and claim are one atomic scope.
fn load_and_claim_parked(
    conn: &rusqlite::Connection,
    subscriber: &brenn_lib::messaging::ParticipantId,
    channel_uuid: Uuid,
) -> Vec<(i64, i64, MessageEnvelope)> {
    let rows = db::load_pending_pushes_for_channel(conn, subscriber, channel_uuid);
    let ids: Vec<i64> = rows.iter().map(|(push_id, _, _)| *push_id).collect();
    let won: HashSet<i64> = db::claim_pending_pushes(conn, &ids).into_iter().collect();
    rows.into_iter()
        .filter(|(push_id, _, _)| won.contains(push_id))
        .collect()
}

/// Drain every active durable channel's parked backlog — the eager-wake nudge
/// path. Stops and reports `Disconnect` if any send finds the writer gone.
async fn drain_all_durable(
    ctx: &SessionCtx,
    durable: &DurableSessionState,
    spans: &mut WireSpans,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    for sub in &durable.active {
        let uuid = ctx.runtime.durable_subscription(sub).channel_uuid;
        if let FrameOutcome::Disconnect =
            drain_durable_channel(ctx, durable, spans, sub, uuid, counters).await
        {
            return FrameOutcome::Disconnect;
        }
    }
    FrameOutcome::Continue
}

/// Claim and send one durable channel's parked backlog in seq order. Rows are
/// claimed under the same lock they are loaded, so the router fan-out and this
/// drain never double-send. The delivery floor gates the send; a deny retires
/// (leaves claimed) without delivering.
async fn drain_durable_channel(
    ctx: &SessionCtx,
    durable: &DurableSessionState,
    spans: &mut WireSpans,
    sub: &SubKey,
    channel_uuid: Uuid,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let channel = sub.channel.as_str();
    let messenger = ctx.runtime.messenger.as_ref().unwrap_or_else(|| {
        panic!(
            "surface {}: durable drain on {channel} but no Messenger — boot invariant violated",
            ctx.runtime.resolved.slug
        )
    });
    // Claims are made under this subscription's own principal, so the drain sees
    // exactly this instance's parked window — never a sibling's.
    let mut claimed: Vec<(i64, i64, MessageEnvelope)> = {
        let conn = messenger.db().lock().await;
        load_and_claim_parked(
            &conn,
            &sub.participant(&ctx.runtime.resolved.slug),
            channel_uuid,
        )
    };
    // At-most-once dedup: skip any row whose seq the replay path already put on
    // this connection's wire (leave it claimed = retired). Without this a row
    // unclaimed after a replay already sent its seq would drain as a duplicate.
    claimed.retain(|(_push_id, message_id, _)| !durable.already_replayed(sub, *message_id));
    if claimed.is_empty() {
        return FrameOutcome::Continue;
    }
    if !ctx.runtime.policy.allows_channel_access(channel) {
        warn!(%channel, "surface durable drain: delivery floor denied; retiring claimed rows");
        return FrameOutcome::Continue;
    }
    for (i, (_push_id, message_id, envelope)) in claimed.iter().enumerate() {
        // A live drain runs with the subscription's real high-water, so its
        // below-water detection (`id <= hw` inside `send_durable_deliver`) is
        // accurate — no force-stamp needed.
        if let FrameOutcome::Disconnect = send_durable_deliver(
            ctx,
            spans,
            sub,
            envelope.clone(),
            *message_id,
            false,
            counters,
        )
        .await
        {
            unclaim_parked(
                messenger,
                claimed[i..].iter().map(|(push_id, _, _)| Some(*push_id)),
            )
            .await;
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
/// `Ephemeral` output onto the `EphemeralBus`, a `Durable` (`brenn:`) output
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
    let outcome = match class {
        DeliveryClass::Ephemeral => match runtime.bus.publish(
            &runtime.participant,
            &runtime.policy,
            address,
            body,
            urgency,
        ) {
            EphemeralPublishResult::Ok { .. } => {
                counters.publish_ok(component);
                PublishOutcome::Ok
            }
            // Bus-level per-sender gate — the second rate-limit gate.
            EphemeralPublishResult::RateLimited => {
                counters.publish_rate_limited(component);
                PublishOutcome::RateLimited
            }
            // Reaching this means the transport pre-check (step 2) and the bus
            // disagree on body size — a boot/config-wiring bug, since both derive
            // from config.messaging.max_body_bytes. Not panicked (body is
            // client-controlled input), but it must scream: a bare counter bump
            // would fold silently into the routine transport-rejection count.
            EphemeralPublishResult::BodyTooLarge { len, max } => {
                error!(
                    len,
                    max,
                    transport_max = runtime.max_body_bytes,
                    "surface Publish: transport and bus body-size caps disagree"
                );
                counters.publish_body_cap_disagreement += 1;
                PublishOutcome::BodyTooLarge {
                    len: len as u64,
                    max: max as u64,
                }
            }
            // Boot validation proved every bound output exists and is policy-covered,
            // so these are broken boot invariants, not attacker-reachable (the only
            // client influence — an unbound port — was already killed above).
            other @ (EphemeralPublishResult::AclDenied(_)
            | EphemeralPublishResult::UnknownChannel(_)
            | EphemeralPublishResult::MalformedAddress(_)) => panic!(
                "surface {slug}: EphemeralBus rejected bound output {address}: {other:?} — boot \
                 validation guarantees every bound output exists and is policy-covered"
            ),
            // Dispatch-arm-only variants: never produced by EphemeralBus::publish.
            other @ (EphemeralPublishResult::MissingSender
            | EphemeralPublishResult::UnsupportedOption { .. }) => {
                unreachable!("EphemeralBus::publish never produces {other:?}")
            }
        },
        // Durable (`brenn:`) output: publish through the Messenger. A bound
        // `brenn:` output implies a directory channel implies messaging
        // configured implies `Some(messenger)`, so `None` here is a broken boot
        // invariant, not attacker-reachable. (`SurfaceRuntime::build` asserts the
        // same Messenger-present invariant for durable *subscriptions*; the output
        // direction is enforced fail-fast here at first use — see its comment.)
        DeliveryClass::Durable => {
            let messenger = runtime.messenger.as_ref().unwrap_or_else(|| {
                panic!(
                    "surface {slug}: durable output {address} bound but runtime has no Messenger \
                     — a bound `brenn:` output implies a directory channel implies messaging \
                     configured implies Some(messenger)"
                )
            });
            // The reserved error-report port rides the ordinary durable publish
            // path but has two distinct postures below: on success an audit emit
            // restoring the user/session correlation the report body omits, and on
            // the broken-boot-invariant outcomes an `error!` carrying the report
            // body instead of the bound-output panic — killing the server over its
            // own diagnostics channel, on an attacker-sendable frame path, inverts
            // priorities.
            // A report about a component is published under that component's
            // sub-identity (resolved at step 1b): attribution lands on the
            // component the report is about, and a crash-looping component's
            // report flood draws down its own budget rather than its neighbours'.
            // A kernel self-report carries the bare surface identity.
            match messenger
                .publish_from_surface(slug, component, address, body, urgency)
                .await
            {
                PublishResult::Ok { .. } => {
                    counters.publish_ok(component);
                    if is_error_report {
                        // The auth layer attests user/session; the report body
                        // does not carry them (server-attested facts do not belong
                        // in a surface-attributed body). Keyed by the session span
                        // + publish_ts, this restores the correlation.
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
                // Same transport-vs-bus body-cap skew as the ephemeral arm: both
                // caps derive from config.messaging.max_body_bytes, so a
                // disagreement is a config-wiring bug. Scream, don't panic (body
                // is client-controlled input) — surface it as an outcome.
                PublishResult::BodyTooLarge { len, max } => {
                    error!(
                        len,
                        max,
                        transport_max = runtime.max_body_bytes,
                        "surface durable Publish: transport and messenger body-size caps disagree"
                    );
                    counters.publish_body_cap_disagreement += 1;
                    PublishOutcome::BodyTooLarge {
                        len: len as u64,
                        max: max as u64,
                    }
                }
                // The output binding, its `brenn_publish` ACL coverage, and the
                // channel's existence are all boot-validated and boot-static, so a
                // denial here is a broken boot invariant — not attacker-reachable
                // (the only client influence, an unbound port, was killed above).
                // For an ordinary bound output that means panic (a user-visible
                // publish silently failing is doing the wrong thing). On the
                // reserved error-report port the same outcomes instead `error!`
                // the full report body and return `Failed`: this branch handles
                // attacker-adjacent input on a diagnostics channel, so it fails
                // loud-and-closed rather than trusting the invariant with the
                // process's life. The report is preserved in the `error!` line
                // (and the shell console-logged it before publishing).
                other @ (PublishResult::MissingSender
                | PublishResult::AclDenied(_)
                | PublishResult::UnknownChannel(_)
                | PublishResult::MalformedAddress(_)) => {
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
                            "surface error report publish failed on the reserved port; report \
                             preserved in this log line only"
                        );
                        PublishOutcome::Failed
                    } else {
                        panic!(
                            "surface {slug}: publish_from_surface rejected bound durable output \
                             {address}: {other:?} — boot validation guarantees every bound output \
                             exists and is policy-covered"
                        )
                    }
                }
                // The per-surface send budget can deny a durable surface
                // publish. Client-facing meaning is identical to any rate limit
                // — slow down — so it maps to the existing RateLimited wire
                // outcome and counter; the publish gate already emitted the
                // first-denial warn attributed to the slug.
                PublishResult::BudgetExhausted => {
                    counters.publish_rate_limited(component);
                    PublishOutcome::RateLimited
                }
            }
        }
        // `local:` traffic never crosses the wire: the page-local router is its
        // sole source of truth, so a `local:` publish must never reach the
        // server. Not attacker-reachable — `class` is looked up from the
        // boot-resolved output map, which excludes `local:` bindings by
        // construction (`SurfaceRuntime::build`), so a client naming a local
        // output port was already killed by the unbound-port violation above.
        // Broken boot invariant: die rather than route page-local traffic onto
        // the bus.
        DeliveryClass::Local => panic!(
            "surface {slug}: output {address} classified Local — local: channels never reach the \
             server (the output map excludes them); this is a broken boot invariant"
        ),
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
/// 2. Batch shape: non-empty, within the per-activation publish cap the kernel
///    buffers against, and within the per-activation *byte* cap it buffers
///    against. All three bound the work steps 3–5 do before any budget is
///    consulted.
/// 3. Per entry: a bound output port of *this* instance, and a body within the
///    cap. `local:` targets fall out of the port check for free — the boot-resolved
///    output map excludes them by construction, so a page-local address is an
///    unbound port here and dies as one.
/// 4. The instance's send budget, drawn once for the whole batch as N tokens (see
///    [`Messenger::draw_surface_send_budget_for_batch`]). Denial is
///    `RateLimited` — never a violation, never a kill: it is the honest answer
///    when the two budget tiers disagree, and the kernel's tier is the binding
///    one for any non-malicious page.
/// 5. Apply: durable entries in one transaction, ephemeral entries fanned out, both
///    in call order. Cross-class relative order is not guaranteed — one class
///    commits in the server's DB and the other in its bus.
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
    if publishes.is_empty() {
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
    let mut resolved: Vec<(&OutputPort, &str, Urgency)> = Vec::with_capacity(publishes.len());
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
        // Sender intent, else the port's boot-resolved default — read from the
        // server's own output map, never echoed from the frame, so a client whose
        // `Welcome` snapshot went stale still gets the operator's value.
        resolved.push((
            out,
            entry.body.as_str(),
            entry.urgency.unwrap_or(out.default_urgency),
        ));
    }

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
    let draw = u32::try_from(resolved.len()).expect("batch length is capped well below u32::MAX");
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

    // 5. Stamp every wire entry, in call order, in one pass across the whole
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

    // 6. Apply. Durable first as one transaction, then the ephemeral fan-out; each
    //    class in call order, with no order promised between them — the guarantee
    //    is the position assignment above plus per-session Deliver sequencing,
    //    never a shared commit instant.
    let durable: Vec<SurfaceBatchPublish<'_>> = resolved
        .iter()
        .zip(&stamps)
        .filter(|((out, _, _), _)| matches!(out.class, DeliveryClass::Durable))
        .map(|((out, body, urgency), ts)| SurfaceBatchPublish {
            channel_address: out.address.as_str(),
            body,
            urgency: *urgency,
            publish_ts_ns: *ts,
        })
        .collect();
    let durable_count = durable.len();
    messenger
        .publish_batch_from_surface(slug, instance, &durable)
        .await;
    for _ in 0..durable_count {
        counters.publish_ok(Some(instance));
    }

    for ((out, body, urgency), ts) in resolved
        .iter()
        .zip(&stamps)
        .filter(|((out, _, _), _)| !matches!(out.class, DeliveryClass::Durable))
    {
        publish_batch_ephemeral(ctx, instance, out, body, *urgency, *ts, counters);
    }

    let frame = ServerFrame::PublishBatchResult {
        correlation,
        outcome: PublishBatchOutcome::Ok,
    };
    send_frame(&ctx.tx, frame, counters).await
}

/// Apply one ephemeral entry of an admitted `PublishBatch`.
///
/// Routes through the bus's **prepaid** entry point, which never consults the
/// per-sender wall-clock gate: the batch already paid, whole, at step 4, and the
/// client has been promised `Ok` for all of it. A second, independently-keyed
/// bucket metering per entry after admission could only lose a wide flush's tail
/// under an answer that said it landed. Ad-hoc (gesture) ephemeral publishes
/// still route through `EphemeralBus::publish` and its gate — that is where the
/// wall-clock tier belongs.
///
/// The prepaid entry point panics rather than returning: every client-reachable
/// failure was already answered as a violation by the handler's per-entry
/// resolve, so nothing is left here that a conforming boot can produce.
fn publish_batch_ephemeral(
    ctx: &SessionCtx,
    instance: &str,
    out: &OutputPort,
    body: &str,
    urgency: Urgency,
    publish_ts_ns: i64,
    counters: &mut SessionCounters,
) {
    let runtime = &ctx.runtime;
    runtime.bus.publish_prepaid(
        &runtime.participant,
        &runtime.policy,
        out.address.as_str(),
        body,
        urgency,
        brenn_lib::messaging::db::ns_to_utc(publish_ts_ns),
    );
    counters.publish_ok(Some(instance));
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
/// Folds the bus event stream so a `Dropped(n)` overflow signal is never yielded
/// alone: its count accumulates into the `dropped` field of the next delivery.
/// A bus `Dropped(n)` is emitted only on broadcast lag — which means the ring is
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
    /// Set once `inner` has yielded its terminating `None` (the bus dropped at
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

/// Drive an `EphemeralReceiver` as a stream of raw bus events, ending when the
/// bus is dropped at shutdown.
fn receiver_events(receiver: EphemeralReceiver) -> impl Stream<Item = EphemeralEvent> + Send {
    stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|event| (event, receiver))
    })
}

/// Fold raw bus events into wire-ready `DeliveryItem`s: accumulate `Dropped(n)`
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
    use brenn_lib::messaging::testutils::ephemeral_channel_entry;
    use brenn_lib::messaging::{
        EphemeralBus, EphemeralEvent, EphemeralPublishResult, ParticipantId, Urgency,
    };

    use super::super::test_fixtures::{
        TEST_MAX_BODY_BYTES, TEST_ORIGIN, durable_resume, fixture_bus,
    };
    use super::*;

    const CHANNEL: &str = "ephemeral:protobar";
    /// Bare channel name the `ephemeral_subscribe`/`ephemeral_publish` matchers
    /// key on (the scheme prefix is stripped before matching).
    const CHANNEL_NAME: &str = "protobar";

    fn bus(retain_depth: u64, capacity: u32) -> Arc<EphemeralBus> {
        EphemeralBus::new(
            vec![ephemeral_channel_entry("protobar", retain_depth, capacity)],
            Arc::from("test-source"),
            1024,
        )
    }

    fn subscriber_policy() -> Arc<AppPolicy> {
        let mut p = AppPolicy::default();
        p.grants.insert(AppCapability::EphemeralSubscribe);
        p.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(CHANNEL_NAME.to_string())];
        Arc::new(p)
    }

    fn publisher_policy() -> AppPolicy {
        let mut p = AppPolicy::default();
        p.grants.insert(AppCapability::EphemeralPublish);
        p.acls.ephemeral_publish = vec![ChannelMatcher::Exact(CHANNEL_NAME.to_string())];
        p
    }

    fn publish_n(bus: &EphemeralBus, n: usize) {
        let sender = ParticipantId::for_surface("deskbar");
        let policy = publisher_policy();
        for _ in 0..n {
            // Assert success so fixture drift (ACL/scheme mismatch) fails loudly
            // here instead of hanging every downstream `stream.next().await`.
            assert!(matches!(
                bus.publish(&sender, &policy, CHANNEL, "hi", Urgency::Normal),
                EphemeralPublishResult::Ok { .. }
            ));
        }
    }

    fn stream_for(bus: &EphemeralBus) -> SubscriptionStream {
        let sub = bus
            .subscribe(
                ParticipantId::for_surface("deskbar"),
                subscriber_policy(),
                CHANNEL,
                None,
            )
            .expect("subscribe");
        SubscriptionStream::new(sub.receiver)
    }

    #[tokio::test]
    async fn undropped_deliveries_carry_zero_dropped_in_seq_order() {
        let bus = bus(8, 16);
        let mut stream = stream_for(&bus);
        publish_n(&bus, 3);

        for expected_seq in 1..=3 {
            let item = stream.next().await.expect("delivery");
            assert_eq!(item.delivery.seq, expected_seq);
            assert_eq!(item.dropped, 0);
        }
    }

    #[tokio::test]
    async fn dropped_count_rides_the_next_delivery() {
        // capacity 2, flood 5 with no interleaved poll: the receiver lags by 3
        // (seqs 1..3 overwritten), retaining the 2 newest (4, 5). The fold never
        // yields the drop alone — it rides the first surviving delivery.
        let bus = bus(0, 2);
        let mut stream = stream_for(&bus);
        publish_n(&bus, 5);

        let first = stream.next().await.expect("delivery");
        assert_eq!(first.delivery.seq, 4);
        assert_eq!(first.dropped, 3);

        let second = stream.next().await.expect("delivery");
        assert_eq!(second.delivery.seq, 5);
        assert_eq!(second.dropped, 0);
    }

    #[tokio::test]
    async fn consecutive_drops_accumulate_onto_one_delivery() {
        // The bus never emits two drops back-to-back (a delivery always sits
        // behind a lag), so drive the fold directly with synthetic events to pin
        // the accumulation arithmetic. Reuse a real delivery Arc to avoid
        // hand-building an envelope.
        let bus = bus(1, 8);
        let mut seed = stream_for(&bus);
        publish_n(&bus, 1);
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
    async fn bus_closure_ends_the_stream() {
        let bus = bus(8, 16);
        let mut stream = stream_for(&bus);
        publish_n(&bus, 2);

        assert!(stream.next().await.is_some());
        assert!(stream.next().await.is_some());

        // Dropping the bus closes the broadcast channel: the stream ends.
        drop(bus);
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
        let bus = fixture_bus(vec![]);
        let runtime = Arc::new(SurfaceRuntime::build(
            resolved,
            bus,
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
        use brenn_lib::access::acl::ChannelMatcher;
        use brenn_lib::messaging::config::{
            ChannelConfigRaw, MessagingGlobalConfig, NoiseLevel, ResolvedSubscription,
            build_channel_entries,
        };
        use brenn_lib::messaging::{
            MessagingDirectory, Messenger, WakeMin, WakeRouter, query::NoopWakeRouter,
        };

        let raw = ChannelConfigRaw {
            uuid: Uuid::new_v4().to_string(),
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

        let resolved = crate::test_support::surface::SurfaceFixture::new("deskbar", "protobar")
            .subscribe(DURABLE_ADDR, "protobar", "messages")
            .durable_subscribe(
                DURABLE_INSTANCE,
                ResolvedSubscription {
                    channel_uuid,
                    channel_address: DURABLE_ADDR.to_string(),
                    push_depth: Depth::Bounded(64),
                    retain_depth,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                },
            )
            .policy(policy)
            .build();
        let bus = fixture_bus(vec![]);
        let runtime = SurfaceRuntime::build(
            resolved,
            bus,
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
            uuid: Uuid::new_v4().to_string(),
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
        let bus = fixture_bus(vec![]);
        let mut runtime = SurfaceRuntime::build(
            resolved,
            bus,
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
    #[should_panic(expected = "rejected bound durable output")]
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

    /// Insert one message on `channel_uuid` with a pending push targeting
    /// `surface:deskbar`. Returns `(push_id, message_id)`.
    async fn seed_parked(
        db: &brenn_lib::db::Db,
        channel_uuid: Uuid,
        body: &str,
        ts_ns: i64,
    ) -> (i64, i64) {
        use brenn_lib::messaging::db::{PendingPushInsert, insert_message_with_pushes};
        use brenn_lib::messaging::{ChannelScheme, ParticipantId, Urgency};
        let conn = db.lock().await;
        // Targeted at the subscribing instance, as `resolve_push_targets` would:
        // the push window is the principal's, not the surface's.
        let subscriber = ParticipantId::for_surface_component("deskbar", DURABLE_INSTANCE);
        let push = PendingPushInsert {
            target_app_slug: subscriber.as_surface_subscriber_key().to_string(),
            target_subscriber: subscriber,
            eager_wake: true,
            release_after: None,
            delivery_deadline: None,
        };
        let msg = insert_message_with_pushes(
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
            &[push],
        );
        (msg.push_ids[0], msg.id)
    }

    /// Insert one message on `channel_uuid` with no pending push (an
    /// already-delivered / retained-only row). Returns the message id.
    async fn seed_message(
        db: &brenn_lib::db::Db,
        channel_uuid: Uuid,
        body: &str,
        ts_ns: i64,
    ) -> i64 {
        use brenn_lib::messaging::db::insert_message_with_pushes;
        use brenn_lib::messaging::{ChannelScheme, Urgency};
        let conn = db.lock().await;
        insert_message_with_pushes(
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
            &[],
        )
        .id
    }

    /// A durable `Deliver` carries a delivery-time span `seq` on the wire and its
    /// row identity inside the opaque `cursor` (the subscription high-water). The
    /// tests assert the delivered row by parsing the cursor's high-water, which
    /// for an in-order delivery is the delivered row's id.
    fn expect_deliver(frame: ServerFrame, want_id: i64) {
        let target = sole_target(frame);
        match cursor::parse(&target.cursor) {
            Ok(CursorState::Durable { high_water, .. }) => {
                assert_eq!(high_water, want_id, "Deliver cursor high-water");
            }
            other => panic!("expected a durable cursor for id {want_id}, got {other:?}"),
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

    /// Under the soft cap the confirm set asks for nothing: the set is doing its
    /// job and the cursors carrying it are small.
    #[test]
    fn confirm_set_under_the_soft_cap_asks_for_nothing() {
        let mut spans = WireSpans::new();
        let sub = durable_sub();
        for id in 0..CONFIRM_SET_SOFT_CAP {
            assert_eq!(
                spans.add_confirm(&sub, id as i64),
                ConfirmCapAction::None,
                "entry {id} is within the soft cap",
            );
        }
    }

    /// Past the soft cap the server asks once and only once: the ask is
    /// outstanding until the resubscribe, and deliveries continue meanwhile —
    /// the cap is a trigger, not a gate, so the set keeps absorbing entries.
    #[test]
    fn confirm_set_past_the_soft_cap_asks_exactly_once_while_pending() {
        let mut spans = WireSpans::new();
        let sub = durable_sub();
        for id in 0..CONFIRM_SET_SOFT_CAP {
            spans.add_confirm(&sub, id as i64);
        }
        assert_eq!(
            spans.add_confirm(&sub, CONFIRM_SET_SOFT_CAP as i64),
            ConfirmCapAction::ReAnchor,
            "the entry past the soft cap triggers the ask",
        );
        for id in CONFIRM_SET_SOFT_CAP + 1..CONFIRM_SET_HARD_CAP - 1 {
            assert_eq!(
                spans.add_confirm(&sub, id as i64),
                ConfirmCapAction::None,
                "entry {id} must not repeat the ask while one is outstanding",
            );
        }
        assert_eq!(
            spans.confirm_set[&sub].len(),
            CONFIRM_SET_HARD_CAP - 1,
            "entries keep being absorbed while the ask is outstanding",
        );
    }

    /// A re-anchor's resubscribe clears the set and the pending flag together, so
    /// a subscription that re-anchors is asked again the next time it needs to be
    /// — the trigger re-arms rather than firing once per connection.
    #[test]
    fn re_anchor_resubscribe_empties_the_set_and_re_arms_the_ask() {
        let mut spans = WireSpans::new();
        let sub = durable_sub();
        for id in 0..=CONFIRM_SET_SOFT_CAP {
            spans.add_confirm(&sub, id as i64);
        }
        assert!(spans.reanchor_pending.contains(&sub));

        // The kernel re-anchored: the resubscribe's clear is the reconcile.
        spans.clear(&sub);
        assert!(!spans.confirm_set.contains_key(&sub), "set emptied");
        assert!(
            !spans.reanchor_pending.contains(&sub),
            "ask no longer pending"
        );

        for id in 0..CONFIRM_SET_SOFT_CAP {
            assert_eq!(spans.add_confirm(&sub, id as i64), ConfirmCapAction::None);
        }
        assert_eq!(
            spans.add_confirm(&sub, CONFIRM_SET_SOFT_CAP as i64),
            ConfirmCapAction::ReAnchor,
            "the trigger re-arms after a reconcile",
        );
    }

    /// A client that ignores the ask until the hard cap is non-conforming. The set
    /// is never truncated to make room — truncating would silently convert
    /// delivered rows into presumed-lost ones at the next reconcile.
    #[test]
    fn confirm_set_at_the_hard_cap_is_a_violation_and_never_truncates() {
        let mut spans = WireSpans::new();
        let sub = durable_sub();
        for id in 0..CONFIRM_SET_HARD_CAP - 1 {
            assert_ne!(
                spans.add_confirm(&sub, id as i64),
                ConfirmCapAction::Violation,
                "entry {id} is below the hard cap",
            );
        }
        assert_eq!(
            spans.add_confirm(&sub, (CONFIRM_SET_HARD_CAP - 1) as i64),
            ConfirmCapAction::Violation,
            "the entry reaching the hard cap is a violation",
        );
        assert_eq!(
            spans.confirm_set[&sub].len(),
            CONFIRM_SET_HARD_CAP,
            "every recorded id survives; the set is never truncated",
        );
    }

    /// The caps are per subscription: one subscription's depth never triggers
    /// another's ask, exactly as the set itself is per-subscription state.
    #[test]
    fn confirm_caps_are_per_subscription() {
        let mut spans = WireSpans::new();
        let a = durable_sub();
        let b = SubKey {
            instance: "other".to_string(),
            channel: DURABLE_ADDR.to_string(),
        };
        for id in 0..=CONFIRM_SET_SOFT_CAP {
            spans.add_confirm(&a, id as i64);
        }
        assert!(spans.reanchor_pending.contains(&a));
        assert_eq!(
            spans.add_confirm(&b, 0),
            ConfirmCapAction::None,
            "a sibling's full set says nothing about this one",
        );
        assert!(!spans.reanchor_pending.contains(&b));
    }

    /// A durable row released below the current high-water advances the wire span
    /// seq but neither regresses the minted cursor nor moves the high-water — so
    /// the next reconnect resumes from the true high-water, not a below-water
    /// floor that would replay already-seen rows.
    #[test]
    fn next_durable_below_high_water_holds_cursor_but_advances_span_seq() {
        let mut spans = WireSpans::new();
        spans.set_store_identity(db::StoreIdentity {
            generation: Uuid::nil(),
            incarnation: 0,
        });
        let sub = durable_sub();
        spans.start_durable_span(&sub, 0);

        let (seq_hi, cursor_hi) = spans.next_durable(&sub, 10);
        assert_eq!(seq_hi, 1, "first durable span seq is 1");
        assert!(
            matches!(
                cursor::parse(&cursor_hi),
                Ok(CursorState::Durable { high_water: 10, .. })
            ),
            "cursor high-water tracks the delivered row",
        );

        let (seq_lo, cursor_lo) = spans.next_durable(&sub, 7);
        assert_eq!(seq_lo, 2, "span seq is monotone regardless of row order");
        assert!(
            matches!(
                cursor::parse(&cursor_lo),
                Ok(CursorState::Durable { high_water: 10, .. })
            ),
            "a below-water row keeps the cursor at the prior high-water",
        );
    }

    /// The below-water confirm set rides every durable cursor once a
    /// message id is recorded, and `durable_high_water_of` reads the pre-advance
    /// high-water a below-water send is detected against.
    #[test]
    fn confirm_set_rides_every_durable_cursor_after_being_recorded() {
        let mut spans = WireSpans::new();
        spans.set_store_identity(db::StoreIdentity {
            generation: Uuid::nil(),
            incarnation: 0,
        });
        let sub = durable_sub();
        spans.start_durable_span(&sub, 5);
        assert_eq!(spans.durable_high_water_of(&sub), Some(5));

        // A below-water row (7 is not below 5, but 3 is): record it, then every
        // cursor minted thereafter carries it.
        spans.add_confirm(&sub, 3);
        let (_seq, cursor) = spans.next_durable(&sub, 3);
        assert_eq!(
            cursor::parse(&cursor),
            Ok(cursor::CursorState::Durable {
                generation: Uuid::nil(),
                incarnation: 0,
                high_water: 5,
                confirm: vec![3],
            }),
            "the confirm set rides the cursor and the high-water stays put",
        );
        // A later above-water send still carries the accumulated set.
        let (_seq2, cursor2) = spans.next_durable(&sub, 9);
        assert!(matches!(
            cursor::parse(&cursor2),
            Ok(cursor::CursorState::Durable {
                high_water: 9,
                confirm,
                ..
            }) if confirm == vec![3]
        ));

        // Clearing the subscription drops the confirm set with the rest.
        spans.clear(&sub);
        assert_eq!(spans.durable_high_water_of(&sub), None);
    }

    /// An unparseable resume cursor is a protocol violation whose detail names the
    /// parse cause — the fail2ban-relevant mapping the class-mismatch tests do not
    /// exercise.
    #[test]
    fn parse_resume_cursor_unparseable_is_violation_with_cause() {
        let bogus: Cursor =
            serde_json::from_value(serde_json::Value::String("not-a-cursor".into())).unwrap();
        match parse_resume_cursor(&bogus, ExpectClass::Durable, "slug", "user", "chan") {
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
        let _ = seed_parked(&db, uuid, "one", 100).await;
        let _ = seed_parked(&db, uuid, "two", 200).await;

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

    /// A fresh durable subscribe drains the parked backlog in seq order behind a
    /// `SubscribeResult{Ok}`, activates the shared/local sets, and claims the rows.
    #[tokio::test]
    async fn durable_subscribe_fresh_drains_parked_backlog() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let (p1, m1) = seed_parked(&db, uuid, "one", 100).await;
        let (p2, m2) = seed_parked(&db, uuid, "two", 200).await;

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
        // Both rows are claimed (a re-claim finds nothing).
        let conn = db.lock().await;
        assert!(brenn_lib::messaging::db::claim_pending_pushes(&conn, &[p1, p2]).is_empty());
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

    /// A retain clamp that truncates the `id > last_seq` set yields a
    /// `BeyondRetained` gap alongside the (clamped) replay.
    #[tokio::test]
    async fn durable_subscribe_resume_truncated_window_gaps() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(1)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let m2 = seed_message(&db, uuid, "two", 200).await;

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs.clone());
        let mut counters = SessionCounters::default();

        // Resume from m1 (= oldest, so not "beyond"); the clamp of 1 drops nothing
        // below m2 here, but a full bounded window is reported conservatively.
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
        expect_deliver(rx.try_recv().expect("m2"), m2);
        assert!(rx.try_recv().is_err());
    }

    /// A durable cursor whose high-water is 0 on a non-empty channel replays the
    /// retained window (clamped to `retain_depth`) and reports a `BeyondRetained`
    /// gap: message ids start at 1, so the oldest retained id is always `> 0` and
    /// the resume floor of 0 is always "beyond" the retained window. Pins the
    /// server's replay + gap for a high-water-0 resume anchor.
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
            Some(durable_resume(&db, 0).await),
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
                    gap.expect("snapshot always gaps: oldest id > 0").reason,
                    ProtoGapReason::BeyondRetained
                );
            }
            other => panic!("expected SubscribeResult, got {other:?}"),
        }
        expect_deliver(rx.try_recv().expect("m2"), m2);
        assert!(rx.try_recv().is_err());
    }

    /// A high-water-0 resume anchor on an *empty* channel (fresh install)
    /// reports the `BeyondRetained` gap with no replayed rows, so a brand-new bar shows no
    /// false staleness warning.
    #[tokio::test]
    async fn durable_subscribe_snapshot_last_seq_zero_empty_channel_gaps_no_rows() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, _uuid) = durable_ctx(&db, Depth::Bounded(1)).await;

        let durable_subs = Arc::new(Mutex::new(HashSet::new()));
        let mut durable = DurableSessionState::new(durable_subs.clone());
        let mut counters = SessionCounters::default();

        let mut spans = WireSpans::new();
        let outcome = handle_durable_subscribe(
            &ctx,
            &mut durable,
            &mut spans,
            durable_sub(),
            Some(durable_resume(&db, 0).await),
            &mut counters,
        )
        .await;
        assert!(matches!(outcome, FrameOutcome::Continue));

        match rx.try_recv().expect("SubscribeResult") {
            ServerFrame::SubscribeResult {
                replay_count, gap, ..
            } => {
                assert_eq!(replay_count, 0, "empty channel replays nothing");
                assert_eq!(
                    gap.expect("empty channel still gaps from resume floor 0")
                        .reason,
                    ProtoGapReason::BeyondRetained
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
    /// stale-store answer. Returns the replayed row ids in delivery order.
    async fn assert_stale_store_fresh_attach(
        ctx: &SessionCtx,
        rx: &mut mpsc::Receiver<ServerFrame>,
        resume: Cursor,
        want_replay: u32,
    ) -> Vec<i64> {
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
                    Ok(CursorState::Durable { high_water, .. }) => ids.push(high_water),
                    other => panic!("expected durable Deliver, got {other:?}"),
                }
            }
        }
        ids
    }

    /// Arm 1 — a cursor whose generation does not match the store's (the
    /// messaging DB was replaced under a live page) is answered as a fresh
    /// attach + `EpochChanged`, never silence, never a violation.
    #[tokio::test]
    async fn durable_resume_generation_mismatch_is_fresh_attach() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let m2 = seed_message(&db, uuid, "two", 200).await;
        let id = store_id(&db).await;

        // A cursor pointing at a real row, but minted under a different store
        // generation than the one now on disk.
        let stale = cursor::mint_durable(Uuid::new_v4(), id.incarnation, m2, vec![]);
        // retain_depth 8 covers both seeded rows.
        let ids = assert_stale_store_fresh_attach(&ctx, &mut rx, stale, 2).await;
        assert_eq!(ids, vec![m1, m2], "fresh window replays the retained rows");
    }

    /// Arm 3 — a cursor whose incarnation is above the store's current one
    /// (the DB was restored from backup and the cursor was minted under a boot
    /// the restored store never counted) is answered as a fresh attach.
    #[tokio::test]
    async fn durable_resume_incarnation_above_store_is_fresh_attach() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let id = store_id(&db).await;

        let stale = cursor::mint_durable(id.generation, id.incarnation + 1, m1, vec![]);
        let ids = assert_stale_store_fresh_attach(&ctx, &mut rx, stale, 1).await;
        assert_eq!(ids, vec![m1]);
    }

    /// Arm 2 — a cursor whose high-water exceeds the channel's current max id
    /// (the DB was restored and reconnected before rows re-climbed the id space)
    /// is answered as a fresh attach. Same generation and a plausible incarnation,
    /// so the other two arms pass and this one alone catches it.
    #[tokio::test]
    async fn durable_resume_high_water_above_channel_max_is_fresh_attach() {
        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx, uuid) = durable_ctx(&db, Depth::Bounded(8)).await;
        let m1 = seed_message(&db, uuid, "one", 100).await;
        let id = store_id(&db).await;

        // high-water 1_000_000 is far above any seeded id.
        let stale = cursor::mint_durable(id.generation, id.incarnation, 1_000_000, vec![]);
        let ids = assert_stale_store_fresh_attach(&ctx, &mut rx, stale, 1).await;
        assert_eq!(ids, vec![m1]);
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
        let resume = cursor::mint_durable(id_before.generation, id_before.incarnation, m1, vec![]);

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

    /// The replay-dedup set is per subscription: a seq alice already received
    /// must not suppress bob's copy of the same message. They are separate
    /// principals with separate windows — the row is delivered once to each.
    #[test]
    fn durable_session_state_replay_dedup_is_per_instance() {
        let shared = Arc::new(Mutex::new(HashSet::new()));
        let mut st = DurableSessionState::new(shared);
        st.activate(&sk("agenda-alice"));
        st.activate(&sk("agenda-bob"));

        assert!(st.record_replayed(&sk("agenda-alice"), 42));
        assert!(st.already_replayed(&sk("agenda-alice"), 42));
        assert!(
            !st.already_replayed(&sk("agenda-bob"), 42),
            "alice's replay must not suppress bob's copy of seq 42"
        );
    }

    /// `replay_sent` survives an unsubscribe/re-subscribe cycle: a queued live
    /// copy of an already-replayed id is still skipped after a fresh re-subscribe,
    /// closing the stale-queue duplicate.
    #[test]
    fn durable_session_state_retains_replay_sent_across_deactivate() {
        let shared = Arc::new(Mutex::new(HashSet::new()));
        let mut st = DurableSessionState::new(shared);
        st.activate(&sk("a"));
        assert!(st.record_replayed(&sk("a"), 42));
        assert!(st.already_replayed(&sk("a"), 42));

        st.deactivate(&sk("a"));
        assert!(
            st.already_replayed(&sk("a"), 42),
            "replay_sent retained across unsubscribe"
        );

        st.activate(&sk("a"));
        assert!(
            st.already_replayed(&sk("a"), 42),
            "still retained after re-subscribe"
        );
    }

    /// `record_replayed` hard-caps each per-channel set at `REPLAY_SENT_MAX`: ids
    /// within the cap are admitted, a repeat is always accepted (no growth), and
    /// the cap-plus-one-th distinct id signals connection teardown (returns false).
    #[test]
    fn durable_session_state_record_replayed_caps_at_max() {
        let shared = Arc::new(Mutex::new(HashSet::new()));
        let mut st = DurableSessionState::new(shared);
        st.activate(&sk("a"));
        for seq in 0..REPLAY_SENT_MAX as i64 {
            assert!(st.record_replayed(&sk("a"), seq), "id {seq} within cap");
        }
        // A repeat id is accepted (adds no growth).
        assert!(st.record_replayed(&sk("a"), 0));
        // The REPLAY_SENT_MAX + 1-th *distinct* id signals teardown.
        assert!(!st.record_replayed(&sk("a"), REPLAY_SENT_MAX as i64));
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

    /// A [`SessionCtx`] whose runtime declares instance `protobar` with a durable
    /// output (`out` → `brenn:batch-out`) and an ephemeral one (`eph` →
    /// `ephemeral:batch-eph`), backed by a real in-memory `Messenger` with a
    /// budget installed for both principal grains. The two classes together are
    /// what the batch's split-and-apply step exists for.
    async fn batch_ctx(db: &brenn_lib::db::Db) -> (SessionCtx, mpsc::Receiver<ServerFrame>) {
        use brenn_lib::messaging::config::{
            ChannelConfigRaw, MessagingGlobalConfig, SurfaceSendBudget, build_channel_entries,
        };
        use brenn_lib::messaging::testutils::{ephemeral_channel_entry, surface_registrations};
        use brenn_lib::messaging::{
            MessagingDirectory, Messenger, WakeRouter, query::NoopWakeRouter,
        };

        let raw = ChannelConfigRaw {
            uuid: Uuid::new_v4().to_string(),
            address: "batch-out".to_string(),
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

        let messenger = Messenger::new(
            db.clone(),
            Arc::new(MessagingDirectory::with_entries(vec![entry])),
            Arc::from(TEST_ORIGIN),
            Arc::new(indexmap::IndexMap::new()),
            Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        )
        .with_subscriber_registrations(surface_registrations(surface_policies))
        .with_surface_send_budgets([(
            "deskbar".to_string(),
            vec![
                (None, SurfaceSendBudget::default()),
                (Some("protobar".to_string()), SurfaceSendBudget::default()),
            ],
        )]);

        let resolved = crate::test_support::surface::SurfaceFixture::new("deskbar", "protobar")
            .output("brenn:batch-out", "protobar", "out")
            .output("ephemeral:batch-eph", "protobar", "eph")
            .policy(policy)
            .build();
        let bus = fixture_bus(vec![ephemeral_channel_entry("batch-eph", 4, 16)]);
        let runtime = SurfaceRuntime::build(
            resolved,
            bus,
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
            tx,
        };
        (ctx, rx)
    }

    /// One batch entry naming `port`, no urgency override.
    fn entry(port: &str, body: &str) -> BatchEntry {
        BatchEntry {
            port: port.to_string(),
            body: body.to_string(),
            urgency: None,
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
            .bus
            .subscribe(
                ctx.runtime.participant.clone(),
                ctx.runtime.policy.clone(),
                "ephemeral:batch-eph",
                None,
            )
            .expect("ephemeral subscribe")
            .receiver;

        // Interleaved on purpose: the boundary is crossed twice, so a per-half
        // stamp would be caught in either direction.
        let outcome = handle_publish_batch(
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

    /// **The bus per-sender gate is never consulted on the batch path.** The
    /// batch paid once, whole, at admission; a second bucket metering per entry
    /// afterwards could only lose a wide flush's tail under an `Ok`. Driven with
    /// a flush wider than the bus burst — the case that loses entries if the gate
    /// is in the path at all — and pinned on the bus's own rate-limit counter,
    /// which is the gate's only fingerprint.
    #[tokio::test]
    async fn the_bus_per_sender_gate_is_never_consulted_on_the_batch_path() {
        use brenn_lib::messaging::EPHEMERAL_SENDER_BURST;

        let db = brenn_lib::db::init_db_memory();
        let (ctx, mut rx) = batch_ctx(&db).await;
        let mut counters = SessionCounters::default();

        // Wider than the bus per-sender burst, and still a conforming flush the
        // instance's backstop admits whole.
        let n = EPHEMERAL_SENDER_BURST as usize + 8;
        assert!(
            n <= MAX_PUBLISHES_PER_ACTIVATION,
            "still a conforming flush"
        );
        let wide: Vec<BatchEntry> = (0..n).map(|_| entry("eph", "x")).collect();

        let outcome = handle_publish_batch(&ctx, "protobar", 5, &wide, &mut counters).await;
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
                .bus
                .rate_limited_count(ctx.runtime.participant.as_str()),
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
            .bus
            .subscribe(
                ctx.runtime.participant.clone(),
                ctx.runtime.policy.clone(),
                "ephemeral:batch-eph",
                None,
            )
            .expect("ephemeral subscribe")
            .receiver;

        let outcome = handle_publish_batch(
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

        let outcome = handle_publish_batch(
            &ctx,
            "protobar",
            1,
            &[
                entry("out", "defaulted"),
                BatchEntry {
                    port: "out".to_string(),
                    body: "overridden".to_string(),
                    urgency: Some(Urgency::High),
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

        let outcome =
            handle_publish_batch(&ctx, "ghost", 1, &[entry("out", "a")], &mut counters).await;
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

        let outcome = handle_publish_batch(
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
                handle_publish_batch(&ctx, "protobar", 1, &[entry("nope", "a")], &mut counters)
                    .await,
                FrameOutcome::Violation(_)
            ),
            "an unbound port is a violation"
        );

        // An over-cap body — an *outcome* on the single-publish path.
        let mut counters = SessionCounters::default();
        let oversized = "x".repeat(TEST_MAX_BODY_BYTES + 1);
        assert!(
            matches!(
                handle_publish_batch(
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
                handle_publish_batch(&ctx, "protobar", 1, &too_many, &mut counters).await,
                FrameOutcome::Violation(_)
            ),
            "a batch over the per-activation cap is a violation"
        );

        // An empty batch: a conforming kernel sends no frame at all.
        let mut counters = SessionCounters::default();
        assert!(
            matches!(
                handle_publish_batch(&ctx, "protobar", 1, &[], &mut counters).await,
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
                handle_publish_batch(&ctx, "protobar", 1, &batch, &mut counters).await,
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

        let outcome = handle_publish_batch(
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
            handle_publish_batch(&ctx, "protobar", 1, &wide, &mut counters).await,
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
        let outcome =
            handle_publish_batch(&ctx, "protobar", 2, &[entry("out", "b")], &mut counters).await;
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
            handle_publish_batch(&ctx, "protobar", 1, &wide, &mut counters).await,
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

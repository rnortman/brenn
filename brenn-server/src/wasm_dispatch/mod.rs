//! Off-loop WASM consumer dispatch task (design §2.5).
//!
//! One task per `[[wasm_consumer]]`. Each task owns its consumer's
//! `ProcessorComponent` + a `Notify` clone and runs a serialized drain loop:
//!
//!   1. Startup sweep: run one drain step unconditionally (crash-recovery trigger,
//!      design §2.5 "Startup sweep").
//!   2. `loop { drain_fully(); notified.await; }` — coalesced wakes.
//!
//! The task never invokes the guest via the unified dispatcher (`dispatch_row`
//! gates Wasm rows to `spawn_eager_wake`, never calls `deliver` for them); it is
//! the sole owner of guest invocation for its slug, ensuring the serialized-drain
//! invariant (design §2.5).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use brenn_lib::messaging::config::{ActivationPacing, WasmInputPort, WasmOutputPort};
use brenn_lib::messaging::store::{DeferralOutcome, MessageSeq, instant_of, release_time_of};
use brenn_lib::messaging::{Messenger, ParticipantId, Urgency, WasmBatchFailure, WasmPublish};
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_lib::obs::security::{SecurityEventType, log_component_security_event};
use brenn_lib::token_bucket::{TokenBucket, TokenBucketOutcome};
use brenn_wasm::ProcessorDeferredOp;
use brenn_wasm::{
    GuestAlertSeverity, PROCESSOR_MAX_DIAG_BYTES, ProcessorActivation, ProcessorAlerter,
    ProcessorComponent, ProcessorDeferredEntry, ProcessorDeferredWindow, ProcessorOutcome,
    ProcessorPortWindow, ProcessorUrgency,
};
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Map a `ProcessorUrgency` (from `brenn-wasm`) to the messaging `Urgency` type.
///
/// 1:1 ladder mapping; exists to decouple the WASM-boundary enum from the
/// messaging enum so `brenn-wasm` does not depend on `brenn-lib`.
fn processor_urgency_to_messaging(u: ProcessorUrgency) -> Urgency {
    match u {
        ProcessorUrgency::VeryLow => Urgency::VeryLow,
        ProcessorUrgency::Low => Urgency::Low,
        ProcessorUrgency::Normal => Urgency::Normal,
        ProcessorUrgency::High => Urgency::High,
    }
}

/// Apply an activation's buffered deferred-message control ops (defer-cancel /
/// defer-edit).
///
/// Each op names a message by `(port, index)`. `index` resolves against
/// `deferred_ids` — the identities captured in this activation's drain-time
/// snapshot — so a message released between drain and flush is targeted by the
/// identity the guest saw, never retargeted by index. The guest host validated
/// the port binding and the index range at buffer time against the very window
/// captured here, so an unbound port or an index outside the captured snapshot is
/// a host invariant violation (the two disagree about the delivered window) and
/// panics. A [`DeferralOutcome::NotDeferred`] is the benign drain-vs-release race:
/// logged, not a failure.
async fn apply_deferred_ops(
    cfg: &WasmConsumerConfig,
    sender: &str,
    deferred_ids: &HashMap<String, Vec<Uuid>>,
    ops: &[ProcessorDeferredOp],
    now: DateTime<Utc>,
) {
    for op in ops {
        let (port, index) = match op {
            ProcessorDeferredOp::Cancel { port, index } => (port, *index),
            ProcessorDeferredOp::Edit { port, index, .. } => (port, *index),
        };
        let out = cfg
            .outputs
            .iter()
            .find(|o| &o.port == port)
            .unwrap_or_else(|| {
                panic!(
                    "wasm_dispatch: deferred op names unbound port {port:?} — the guest host must \
                 reject an unbound port at buffer time"
                )
            });
        let uuid = *deferred_ids
            .get(port)
            .and_then(|ids| ids.get(index as usize))
            .unwrap_or_else(|| {
                panic!(
                    "wasm_dispatch: deferred op index {index} out of range for port {port:?} \
                     snapshot — the guest host validated it against this same window"
                )
            });
        let outcome = match op {
            ProcessorDeferredOp::Cancel { .. } => {
                cfg.messenger
                    .cancel_deferred_for_sender(&out.channel_address, sender, uuid, now)
                    .await
            }
            ProcessorDeferredOp::Edit {
                payload,
                deliver_after,
                ..
            } => {
                let release_at = deliver_after.map(instant_of);
                cfg.messenger
                    .edit_deferred_for_sender(
                        &out.channel_address,
                        sender,
                        uuid,
                        payload.clone(),
                        release_at,
                        now,
                    )
                    .await
            }
        };
        if outcome == DeferralOutcome::NotDeferred {
            cfg.messenger
                .record_deferred_control_race(&cfg.slug, &out.channel_address);
            info!(
                slug = %cfg.slug,
                port = %port,
                "wasm_dispatch: deferred control op is a no-op — the message released between \
                 the activation snapshot and flush"
            );
        }
    }
}

/// Configuration for a single WASM consumer dispatch task.
pub(crate) struct WasmConsumerConfig {
    pub slug: String,
    pub component: Arc<ProcessorComponent>,
    pub notify: Arc<Notify>,
    pub messenger: Arc<Messenger>,
    pub alert_dispatcher: AlertDispatcher,
    /// Resolved input ports for this consumer (one per subscribed channel).
    pub inputs: Vec<WasmInputPort>,
    /// Resolved output ports for this consumer (one per bound publish channel).
    pub outputs: Vec<WasmOutputPort>,
    /// Per-component activation pacing. The consumer task builds its
    /// `ActivationPacer` (a `TokenBucket` over activations) from this and gates
    /// every drain step through it.
    pub activation_pacing: ActivationPacing,
}

/// Per-consumer activation pacing gate.
///
/// Wraps a `TokenBucket` over *activations* (capacity = `burst`, one token
/// refilled per `min_period`) plus episode-based throttle hysteresis owned here —
/// not the bucket's own per-window signals, which would close and reopen on every
/// single paced activation under a sustained flood and spam the logs.
///
/// The gate **delays** activations, it never drops them: when the bucket is empty
/// `admit` sleeps one `min_period` and then proceeds. It is the sole owner of the
/// bucket (task-local, no locking); nothing else consumes tokens.
struct ActivationPacer {
    bucket: TokenBucket,
    pacing: ActivationPacing,
    /// Consumer slug — attributes the throttle security event/alert. Owned by the
    /// pacer (captured at construction) rather than read from the config on every
    /// `admit`, so the gate needs no `WasmConsumerConfig` reference: the only
    /// state it touches is its own.
    slug: String,
    /// Alert sink for the once-per-process throttle-entry page (design §4).
    /// A cheap `AlertDispatcher` clone; shares the process-lifetime dedup set.
    alert_dispatcher: AlertDispatcher,
    /// `Some` while activations are being delayed (a throttle episode is open);
    /// `None` when unthrottled. Drives entry/exit logging exactly once per episode.
    episode: Option<ThrottleEpisode>,
}

/// State for one open throttle episode (unthrottled → throttled → unthrottled).
struct ThrottleEpisode {
    /// Number of activations delayed since this episode opened.
    delayed: u64,
    /// When this episode opened (tokio clock, for paused-time compatibility).
    started: Instant,
}

impl ActivationPacer {
    fn new(pacing: ActivationPacing, slug: String, alert_dispatcher: AlertDispatcher) -> Self {
        // capacity = burst, one token per min_period. Config resolve validates
        // both `burst >= 1` and `min_period >= 1ms`, but enforce both invariants
        // at the value's own boundary too, so a construction that ever bypasses
        // resolve fails fast *clearly* here: a zero interval panics in
        // `TokenBucket::new`, and a zero capacity would make the bucket unable to
        // ever grant — every `try_consume` denies, so `admit` would sleep and then
        // fire its post-sleep assert with a misleading "invariant violation"
        // message that blames the soundness argument rather than the bad config.
        assert!(
            pacing.burst >= 1,
            "ActivationPacer::new: burst must be >= 1 (got {}) for slug {slug:?}",
            pacing.burst,
        );
        Self {
            bucket: TokenBucket::new(pacing.burst, pacing.min_period, 1),
            pacing,
            slug,
            alert_dispatcher,
            episode: None,
        }
    }

    /// Admit one activation, delaying (never dropping) when the bucket is empty.
    /// Called before every `drain_step` (startup sweep + each notified wake).
    /// Blocks for at most ~`min_period` per call.
    async fn admit(&mut self) {
        match self.bucket.try_consume() {
            TokenBucketOutcome::Granted | TokenBucketOutcome::GrantedAfterSuppression { .. } => {
                // Admitted without delay. If a throttle episode was open, close it
                // (this is the first unthrottled activation after the flood). The
                // bucket's own suppression signals are ignored — we track episodes
                // ourselves (design §4).
                self.close_episode();
            }
            TokenBucketOutcome::Denied { .. } => {
                // Bucket empty: this activation is delayed. Open a throttle episode
                // (once per unthrottled → throttled transition; logs + alerts), then
                // sleep one refill interval and retry.
                self.open_episode();
                // `open_episode` leaves an episode open unconditionally; a missing
                // one is a broken invariant, not a stat to silently undercount —
                // fail fast (matches the same-function post-sleep assert below).
                self.episode
                    .as_mut()
                    .expect("open_episode leaves an episode open")
                    .delayed += 1;
                tokio::time::sleep(self.pacing.min_period).await;
                // The post-sleep consume MUST grant: `refill_amount = 1`, a full
                // `refill_interval` has elapsed on the same `tokio::time` clock the
                // bucket reads, and this bucket is task-local (nothing else consumes
                // from it). See the three-step soundness argument in design §2.1. A
                // denial here is an invariant violation — fail-fast (panic).
                let retry = self.bucket.try_consume();
                assert!(
                    matches!(
                        retry,
                        TokenBucketOutcome::Granted
                            | TokenBucketOutcome::GrantedAfterSuppression { .. }
                    ),
                    "ActivationPacer::admit: post-sleep try_consume denied ({retry:?}) for \
                     slug {:?} — the bucket refills 1 token per min_period, a full interval \
                     elapsed on the tokio clock, and the bucket is task-local; a denial is an \
                     invariant violation (mqtt-wasm-republish-pacing design §2.1)",
                    self.slug,
                );
            }
        }
    }

    /// Open a throttle episode on the unthrottled → throttled transition:
    /// `warn!` + component security event + a once-per-process phone alert
    /// (design §4). No-op if an episode is already open.
    fn open_episode(&mut self) {
        if self.episode.is_some() {
            return;
        }
        let burst = self.pacing.burst;
        let min_period_ms = self.pacing.min_period.as_millis() as u64;
        warn!(
            slug = %self.slug,
            burst,
            min_period_ms,
            "wasm_dispatch: activation pacing engaged — consumer is being throttled"
        );
        // Component-attributed security event (no `ip`; fail2ban never matches —
        // the "attacker" is an out-of-tree guest, not a bannable peer, design §4).
        let detail = format!("burst={burst} min_period_ms={min_period_ms}");
        log_component_security_event(
            SecurityEventType::WasmActivationThrottled,
            &self.slug,
            &detail,
        );
        // Phone alert once per process per slug. Dedup key is namespaced
        // `component:<slug>:...`, matching the existing component-attributed alert
        // convention (`signal_publish_denial`, `obs::security`), so component
        // dedup keys share one shape. Title must be stable
        // (`Security: wasm_activation_throttled`) so the per-slug key keys the slot
        // correctly (design §4). A runaway loop alerts exactly once; the security
        // log still records each episode.
        self.alert_dispatcher.alert_once_per_process(
            AlertSeverity::Warning,
            format!("Security: {}", SecurityEventType::WasmActivationThrottled),
            &format!("component:{}:activation_throttled", self.slug),
            format!(
                "WASM consumer {} is being activation-throttled: sustained activation rate \
                 capped at 1 per {min_period_ms} ms after a burst of {burst}. Likely a \
                 self-echo/runaway loop or an over-active consumer. Deliveries are delayed, \
                 not dropped.",
                self.slug
            ),
        );
        self.episode = Some(ThrottleEpisode {
            delayed: 0,
            started: Instant::now(),
        });
    }

    /// Close an open throttle episode on the first unthrottled activation after a
    /// flood: `info!` with the delayed count + episode duration. No-op if no
    /// episode is open. Fires on the next wake after the flood stops, which may be
    /// much later — the entry alert is the actionable signal (design §4).
    fn close_episode(&mut self) {
        if let Some(ep) = self.episode.take() {
            info!(
                slug = %self.slug,
                delayed = ep.delayed,
                episode_ms = ep.started.elapsed().as_millis() as u64,
                "wasm_dispatch: activation pacing episode ended"
            );
        }
    }
}

/// Bridge from `ProcessorAlerter` to the host's `AlertDispatcher`.
///
/// Wraps a per-component child `AlertDispatcher` (pre-seeded with `wasm_slug`
/// context) and the component slug. Title is host-prefixed so a guest cannot
/// impersonate another component or a host alert source.
pub(crate) struct DispatcherAlerter {
    dispatcher: AlertDispatcher,
    slug: String,
}

impl DispatcherAlerter {
    pub(crate) fn new(dispatcher: AlertDispatcher, slug: String) -> Self {
        Self { dispatcher, slug }
    }
}

impl ProcessorAlerter for DispatcherAlerter {
    fn alert(&self, severity: GuestAlertSeverity, title: &str, body: &str) {
        let alert_severity = match severity {
            GuestAlertSeverity::Info => AlertSeverity::Info,
            GuestAlertSeverity::Warning => AlertSeverity::Warning,
            GuestAlertSeverity::Critical => AlertSeverity::Critical,
        };
        // Title is host-prefixed so a guest cannot impersonate another component
        // or a host alert source. `alert()` (not `try_alert()`) panics on a dead
        // alert task — invariant violation; fail-fast preserved.
        self.dispatcher.alert(
            alert_severity,
            format!("WASM {}: {title}", self.slug),
            body.to_string(),
        );
    }
}

/// Spawn the off-loop dispatch task for one WASM consumer.
///
/// Returns a `tokio::task::JoinHandle`. The caller drops the handle (process-lifetime task).
/// Same lifecycle/supervision policy as the deadline and deliver-after tasks: panics are
/// logged + Critical-alerted by the global panic hook (`brenn-lib/src/obs/panic_hook.rs`);
/// manual restart is the decided mitigation. Do NOT add per-task supervision.
pub(crate) fn spawn_wasm_consumer_task(cfg: WasmConsumerConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { run_consumer(cfg).await })
}

/// Main body of the consumer task. Runs the startup sweep then enters the drain loop.
async fn run_consumer(cfg: WasmConsumerConfig) {
    let subscriber = ParticipantId::for_wasm(&cfg.slug);

    // Per-component activation pacer. Every drain step — startup sweep and
    // each notified wake — is admitted through this single gate, so the
    // startup sweep and external eager wakes are all paced. The bucket starts
    // full, so the startup sweep and any burst below `burst` never delay.
    let mut pacer = ActivationPacer::new(
        cfg.activation_pacing,
        cfg.slug.clone(),
        cfg.alert_dispatcher.clone(),
    );

    // Startup sweep: crash-recovery re-dispatch trigger.
    // Runs before the first `notified.await` so undelivered rows left by a prior crash
    // are re-loaded and re-invoked on restart, not waiting for a new wake.
    pacer.admit().await;
    drain_step(&cfg, &subscriber).await;

    // Serialized drain loop, woken by external eager wakes (`spawn_eager_wake`).
    // A wake sets a one-permit flag; any wakes that arrive during a drain step
    // coalesce into one pending permit and the next iteration consumes it. Two
    // drain steps for the same consumer never overlap.
    loop {
        cfg.notify.notified().await;
        pacer.admit().await;
        drain_step(&cfg, &subscriber).await;
    }
}

/// One drain step: assemble a multi-port activation snapshot → invoke guest once
/// → dispose.
///
/// Returns immediately (no-op) when `load_activation_snapshot` returns `None`
/// (no triggering port has pending rows).
///
/// Single-scan design: `load_activation_snapshot` performs one subscriber-scoped
/// pending-push scan under one DB lock hold covering all K input ports (AC 7).
pub(in crate::wasm_dispatch) async fn drain_step(
    cfg: &WasmConsumerConfig,
    subscriber: &ParticipantId,
) {
    // Step 1: assemble multi-port snapshot (single scan, T₀ hermetic).
    // Returns None when no triggering input has pending rows → no activation.
    let Some(snapshots) = cfg
        .messenger
        .load_activation_snapshot(subscriber, &cfg.inputs)
        .await
    else {
        return;
    };

    debug_assert!(
        snapshots.iter().any(|s| s.new_len() > 0),
        "drain_step: snapshot is Some but no port has new messages — invariant violated"
    );
    debug_assert_eq!(
        snapshots.len(),
        cfg.inputs.len(),
        "drain_step: snapshot len {} != inputs len {}",
        snapshots.len(),
        cfg.inputs.len()
    );

    // Step 2: advance every port's position over the window it just served,
    // BEFORE the guest executes. At-most-once; a crash between here and the
    // guest completing means the batch is gone (decided semantics).
    //
    // Each channel advances on its own, so the grain is per channel, not per
    // activation: a crash between two iterations leaves the advanced ports
    // passed and the rest unseen, and the next activation carries only the
    // remainder. The guest has not run at this point, so nothing is delivered
    // twice — a multi-port batch can simply arrive split.
    //
    // What the advance passed unserved is this port's `dropped` figure, so the
    // count the guest reads names exactly what its own window skipped — nothing
    // arrives an activation late and there is no delta to keep.
    let mut dropped_per_port: Vec<u64> = Vec::with_capacity(snapshots.len());
    for (snap, input) in snapshots.iter().zip(&cfg.inputs) {
        let Some((through, seen_floor)) = snap.advance_span() else {
            dropped_per_port.push(0);
            continue;
        };
        let outcome = cfg
            .messenger
            .advance_subscriber(
                &snap.channel_address,
                subscriber,
                through,
                seen_floor,
                input.sub.noise,
            )
            .await;
        dropped_per_port.push(outcome.dropped);
    }

    // Step 3: assemble ProcessorPortWindow per snapshot.
    let ports: Vec<ProcessorPortWindow> = snapshots
        .iter()
        .zip(&dropped_per_port)
        .map(|(snap, dropped)| {
            let envelopes: Vec<String> = snap
                .entries
                .iter()
                .map(|(_, env)| {
                    serde_json::to_string(env)
                        .unwrap_or_else(|e| panic!("wasm_dispatch: serialize MessageEnvelope: {e}"))
                })
                .collect();

            ProcessorPortWindow {
                port: snap.port.clone(),
                envelopes,
                new_from: snap.new_from as u32,
                dropped: *dropped,
            }
        })
        .collect();

    // One clock read for both the deferred-view boundary and the guest's `now`,
    // so a component's view of what is still deferred agrees with the clock it is
    // handed to compute new release times against.
    let now = Utc::now();
    // A guest has no clock; it computes deliver_after from this value.
    let now_ms =
        u64::try_from(now.timestamp_millis()).expect("system clock is before the Unix epoch");

    // Scoped to this component's `wasm:<slug>` sender identity: a shared output
    // channel still shows only this component's schedule.
    let sender = subscriber.as_str();
    let mut deferred: Vec<ProcessorDeferredWindow> = Vec::with_capacity(cfg.outputs.len());
    // Per output port, the message identities behind each deferred-window entry,
    // in the same index order the guest sees. Captured at drain so a
    // `defer-cancel`/`defer-edit` names the identity the guest actually saw — a
    // message that releases between here and flush cannot be retargeted by index.
    let mut deferred_ids: HashMap<String, Vec<Uuid>> = HashMap::with_capacity(cfg.outputs.len());
    // TODO(substrate-deferred-view-count-shortcut): this queries every bound
    // output port's deferred view per activation; for a durable port that is a
    // SQL read under the db mutex, paid even when nothing is parked. Short-circuit
    // the empty case with a maintained per-channel deferred count.
    for out in &cfg.outputs {
        let parked = cfg
            .messenger
            .deferred_view_for_sender(&out.channel_address, sender, now)
            .await;
        let entries: Vec<ProcessorDeferredEntry> = parked
            .iter()
            .enumerate()
            .map(|(i, dm)| ProcessorDeferredEntry {
                index: i as u32,
                payload: dm.envelope.body.clone(),
                deliver_after: release_time_of(dm.release_at),
            })
            .collect();
        deferred_ids.insert(
            out.port.clone(),
            parked.iter().map(|dm| dm.message_uuid()).collect(),
        );
        deferred.push(ProcessorDeferredWindow {
            port: out.port.clone(),
            entries,
        });
    }

    let activation = ProcessorActivation {
        ports,
        deferred,
        now: Some(now_ms),
    };

    // Step 4: invoke the guest. CPU-bound → spawn_blocking.
    let component = cfg.component.clone();
    let join_result = tokio::task::spawn_blocking(move || component.handle(activation)).await;
    let outcome = match join_result {
        Ok(outcome) => outcome,
        Err(join_err) => {
            // Format the channel list for the JoinError context (best-effort, pre-panic).
            let channel_list: Vec<&str> = cfg
                .inputs
                .iter()
                .map(|inp| inp.sub.channel_address.as_str())
                .collect();
            error!(
                slug = %cfg.slug,
                channels = ?channel_list,
                %join_err,
                "wasm_dispatch: spawn_blocking task died (JoinError)"
            );
            cfg.alert_dispatcher.try_alert(
                AlertSeverity::Critical,
                format!("WASM consumer {} task died", cfg.slug),
                format!(
                    "Consumer handle task panicked (JoinError). \
                     slug={} channels={channel_list:?}\n{join_err}",
                    cfg.slug
                ),
            );
            panic!("wasm_dispatch: spawn_blocking join error: {join_err}");
        }
    };

    // Step 5: disposition (activation-scoped). Positions already advanced above.
    match outcome {
        ProcessorOutcome::Ok {
            publishes,
            deferred_ops,
        } => {
            // `now` is the same drain-time instant the deferred view was taken
            // against, keeping the snapshot boundary consistent.
            apply_deferred_ops(cfg, sender, &deferred_ids, &deferred_ops, now).await;
            // A defer-edit can move a release time earlier than the dispatcher's
            // current sleep target (computed before this flush); wake it so an
            // edited-to-now message releases immediately rather than at the next
            // unrelated kick or the poll interval. Kicks are cheap and coalesced,
            // so an unconditional wake whenever any op ran is fine.
            if !deferred_ops.is_empty() {
                cfg.messenger.dispatch_kick();
            }
            if !publishes.is_empty() {
                let wasm_publishes: Vec<WasmPublish<'_>> = publishes
                    .iter()
                    .map(|p| WasmPublish {
                        channel_address: &p.channel_address,
                        body: &p.payload,
                        urgency: processor_urgency_to_messaging(p.urgency),
                        reply_to: p.reply_to.as_deref(),
                        // Representability was validated at buffer time
                        // (`do_publish`), where the guest still held the error
                        // channel, so a non-representable value here is a host
                        // invariant violation, not guest input.
                        deliver_after: p.deliver_after.map(|ms| {
                            DateTime::<Utc>::from_timestamp_millis(
                                i64::try_from(ms)
                                    .expect("deliver_after was range-validated at buffer time"),
                            )
                            .expect("deliver_after was range-validated at buffer time")
                        }),
                    })
                    .collect();
                cfg.messenger
                    .publish_from_wasm(&cfg.slug, &wasm_publishes)
                    .await;
            }
            // Log per-port batch sizes on success.
            let port_batches: Vec<(&str, usize)> = cfg
                .inputs
                .iter()
                .zip(snapshots.iter())
                .map(|(inp, snap)| (inp.port.as_str(), snap.new_len()))
                .collect();
            info!(
                slug = %cfg.slug,
                ports = ?port_batches,
                publish_count = publishes.len(),
                "wasm_dispatch: activation consumed successfully"
            );
        }
        ProcessorOutcome::Err(err) => {
            let diag =
                brenn_common::sanitize_untrusted_str(&format!("{err:?}"), PROCESSOR_MAX_DIAG_BYTES);
            let triggering_summary = format_triggering_summary(&snapshots);
            warn!(
                slug = %cfg.slug,
                triggering_ports = ?triggering_summary,
                diagnostic = %diag,
                "wasm_dispatch: guest returned error — quarantining activation"
            );
            cfg.alert_dispatcher.alert(
                AlertSeverity::Warning,
                format!("WASM consumer {} activation failed (err)", cfg.slug),
                format!("{}\ndiagnostic={diag}", triggering_summary.join("\n")),
            );
            let backing = collect_failure_backing(&snapshots);
            debug_assert!(
                !backing.is_empty(),
                "drain_step: collect_failure_backing returned empty for Some snapshot \
                 — invariant violated (snapshot Some implies at least one port has new messages)"
            );
            let failures = build_activation_failure_refs(&backing, subscriber, "err", &diag);
            cfg.messenger
                .record_wasm_activation_failure(&failures)
                .await;
        }
        ProcessorOutcome::Trap(msg) => {
            let diag = brenn_common::sanitize_untrusted_str(&msg, PROCESSOR_MAX_DIAG_BYTES);
            let triggering_summary = format_triggering_summary(&snapshots);
            warn!(
                slug = %cfg.slug,
                triggering_ports = ?triggering_summary,
                trap = %diag,
                "wasm_dispatch: guest trapped — quarantining activation"
            );
            cfg.alert_dispatcher.alert(
                AlertSeverity::Warning,
                format!("WASM consumer {} activation trapped", cfg.slug),
                format!("{}\ntrap={diag}", triggering_summary.join("\n")),
            );
            let backing = collect_failure_backing(&snapshots);
            debug_assert!(
                !backing.is_empty(),
                "drain_step: collect_failure_backing returned empty for Some snapshot \
                 — invariant violated (snapshot Some implies at least one port has new messages)"
            );
            let failures = build_activation_failure_refs(&backing, subscriber, "trap", &diag);
            cfg.messenger
                .record_wasm_activation_failure(&failures)
                .await;
        }
    }
}

fn format_triggering_summary(snapshots: &[brenn_lib::messaging::PortSnapshot]) -> Vec<String> {
    snapshots
        .iter()
        .filter(|s| s.new_len() > 0)
        .map(|s| {
            let new = s.new_entries();
            let first = new
                .first()
                .map(|(_, e)| e.message_id.to_string())
                .unwrap_or_default();
            let last = new
                .last()
                .map(|(_, e)| e.message_id.to_string())
                .unwrap_or_default();
            format!(
                "channel={} batch={} first={first} last={last}",
                s.channel_address,
                new.len()
            )
        })
        .collect()
}

/// Owned backing for one per-port failure record; WasmBatchFailure borrows from this.
struct PortFailureBacking {
    channel: String,
    first_message_id: String,
    last_message_id: String,
    /// The retention seqs this port's batch spanned, oldest first.
    seq_span: (MessageSeq, MessageSeq),
}

/// Build owned backing + `WasmBatchFailure` slices for all triggering ports in a
/// failed activation. The backing Vec must stay alive as long as the returned
/// `WasmBatchFailure` refs are used — both are returned together and consumed
/// in the same call to `record_wasm_activation_failure`.
fn build_activation_failure_refs<'a>(
    backing: &'a [PortFailureBacking],
    subscriber: &'a ParticipantId,
    outcome: &'static str,
    diagnostic: &'a str,
) -> Vec<WasmBatchFailure<'a>> {
    backing
        .iter()
        .map(|b| WasmBatchFailure {
            channel: &b.channel,
            subscriber,
            first_message_id: &b.first_message_id,
            last_message_id: &b.last_message_id,
            seq_span: b.seq_span,
            outcome,
            diagnostic,
        })
        .collect()
}

fn collect_failure_backing(
    snapshots: &[brenn_lib::messaging::PortSnapshot],
) -> Vec<PortFailureBacking> {
    snapshots
        .iter()
        .filter(|s| s.new_len() > 0)
        .map(|s| {
            let new = s.new_entries();
            let fid = new
                .first()
                .map(|(_, e)| e.message_id.to_string())
                .unwrap_or_default();
            let lid = new
                .last()
                .map(|(_, e)| e.message_id.to_string())
                .unwrap_or_default();
            PortFailureBacking {
                channel: s.channel_address.clone(),
                first_message_id: fid,
                last_message_id: lid,
                seq_span: s
                    .new_seq_span()
                    .expect("wasm_dispatch: a triggering port has a new span"),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests;

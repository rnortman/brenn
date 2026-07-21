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

use brenn_lib::messaging::config::{ActivationPacing, WasmInputPort};
use brenn_lib::messaging::{Messenger, ParticipantId, Urgency, WasmBatchFailure, WasmPublish};
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_lib::obs::security::{SecurityEventType, log_component_security_event};
use brenn_lib::token_bucket::{TokenBucket, TokenBucketOutcome};
use brenn_wasm::{
    GuestAlertSeverity, PROCESSOR_MAX_DIAG_BYTES, ProcessorActivation, ProcessorAlerter,
    ProcessorComponent, ProcessorOutcome, ProcessorPortWindow, ProcessorUrgency,
};
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{error, info, warn};

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

/// Configuration for a single WASM consumer dispatch task.
pub(crate) struct WasmConsumerConfig {
    pub slug: String,
    pub component: Arc<ProcessorComponent>,
    pub notify: Arc<Notify>,
    pub messenger: Arc<Messenger>,
    pub alert_dispatcher: AlertDispatcher,
    /// Resolved input ports for this consumer (one per subscribed channel).
    pub inputs: Vec<WasmInputPort>,
    /// Per-component activation pacing (mqtt-wasm-republish-pacing design §2).
    /// The consumer task builds its `ActivationPacer` (a `TokenBucket` over
    /// activations) from this and gates every drain step through it.
    pub activation_pacing: ActivationPacing,
}

/// Per-consumer activation pacing gate (mqtt-wasm-republish-pacing design §2).
///
/// Wraps a `TokenBucket` over *activations* (capacity = `burst`, one token
/// refilled per `min_period`) plus episode-based throttle hysteresis owned here —
/// not the bucket's own per-window signals, which would close and reopen on every
/// single paced activation under a sustained flood and spam the logs (design §4).
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

    // Per-channel last-seen drop counter (design §2.5 step 3b: delta tracking).
    // Seeded at 0 on startup; advances at ack time (before the guest runs) so
    // a dropped gap is reported exactly once — in the activation that observed it.
    // Failed/trapped activations are never re-driven; the advance-at-ack contract
    // means the same gap is never re-reported.
    // Across a host restart both last_seen and the drop counter reset to 0, so
    // dropped=0 is reported after a restart even if overflow occurred before the crash
    // — best-effort, since-boot, per design §2.5.
    let mut last_seen_drop: HashMap<String, u64> = HashMap::new();

    // Per-component activation pacer (mqtt-wasm-republish-pacing design §2). Every
    // drain step — startup sweep and each notified wake — is admitted through this
    // single gate, so external eager wakes, deadline wakes, clamp self-renotify
    // chains, and the startup sweep are all paced. The bucket starts full, so the
    // startup sweep and any burst below `burst` never delay.
    let mut pacer = ActivationPacer::new(
        cfg.activation_pacing,
        cfg.slug.clone(),
        cfg.alert_dispatcher.clone(),
    );

    // Startup sweep (crash-recovery re-dispatch trigger, design §2.7).
    // Runs before the first `notified.await` so undelivered rows left by a prior crash
    // are re-loaded and re-invoked on restart, not waiting for a new wake.
    pacer.admit().await;
    drain_step(&cfg, &subscriber, &mut last_seen_drop).await;

    // Serialized drain loop. Notify sources: external eager wakes (`spawn_eager_wake`)
    // and clamp self-renotify (`drain_step` fires `notify_one` when a port's backlog
    // exceeded its activation cap). Either sets a one-permit flag; any wakes that arrive
    // during a drain step coalesce into one pending permit and the next iteration consumes
    // it. Two drain steps for the same consumer never overlap.
    loop {
        cfg.notify.notified().await;
        pacer.admit().await;
        drain_step(&cfg, &subscriber, &mut last_seen_drop).await;
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
    last_seen_drop: &mut HashMap<String, u64>,
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
        snapshots.iter().any(|s| !s.new_rows.is_empty()),
        "drain_step: snapshot is Some but no port has new_rows — invariant violated"
    );
    debug_assert_eq!(
        snapshots.len(),
        cfg.inputs.len(),
        "drain_step: snapshot len {} != inputs len {}",
        snapshots.len(),
        cfg.inputs.len()
    );

    // Any port whose pending backlog exceeded this activation's cap leaves leftover
    // rows undelivered. Self-notify so the loop drains them without waiting for the
    // next external wake. Decided at snapshot time (before the guest runs): leftover
    // rows are real backlog regardless of the guest outcome. The single-permit Notify
    // coalesces this with any concurrent external wake into one follow-up drain step,
    // and every step acks its rows before the guest runs, so the chain is bounded by
    // real backlog — ceil(backlog / cap) steps absent new publishes.
    let clamped: Vec<(&str, usize)> = snapshots
        .iter()
        .filter(|s| s.clamped_leftover > 0)
        .map(|s| (s.port.as_str(), s.clamped_leftover))
        .collect();
    if !clamped.is_empty() {
        info!(
            slug = %cfg.slug,
            ports = ?clamped,
            "wasm_dispatch: activation clamped; self-notifying to drain leftover rows"
        );
        cfg.notify.notify_one();
    }

    // Step 2: assemble ProcessorPortWindow per snapshot, computing per-port drop delta.
    // Collect all push_ids across triggering ports for the combined ack.
    let mut all_push_ids: Vec<i64> = Vec::new();
    // Track current drop counter per channel (advance at ack time, step 3).
    let mut current_drops: Vec<(String, u64)> = Vec::with_capacity(snapshots.len());

    let ports: Vec<ProcessorPortWindow> = snapshots
        .iter()
        .map(|snap| {
            let context_envelopes: Vec<String> = snap
                .context
                .iter()
                .map(|env| {
                    serde_json::to_string(env).unwrap_or_else(|e| {
                        panic!("wasm_dispatch: serialize context envelope: {e}")
                    })
                })
                .collect();
            let new_envelopes: Vec<String> = snap
                .new_rows
                .iter()
                .map(|(_, env)| {
                    serde_json::to_string(env)
                        .unwrap_or_else(|e| panic!("wasm_dispatch: serialize MessageEnvelope: {e}"))
                })
                .collect();
            let new_from = context_envelopes.len() as u32;

            // Accumulate push_ids for the combined ack.
            for (id, _) in &snap.new_rows {
                all_push_ids.push(*id);
            }

            // Compute `dropped` delta: drop counter read inside load_activation_snapshot
            // (while the db lock was held) minus last_seen. Using the snapshot value
            // rather than a live read prevents a concurrent publish from evicting a row
            // that is present in this snapshot's new_rows while being counted as dropped
            // (correctness-1). Advance last_seen_drop at ack time (step 3) for EVERY
            // included channel — defensive uniformity (design §2.4 notes).
            let current_drop = snap.drop_counter_snapshot;
            let last_seen = last_seen_drop
                .get(&snap.channel_address)
                .copied()
                .unwrap_or(0);
            let dropped = current_drop.saturating_sub(last_seen);
            current_drops.push((snap.channel_address.clone(), current_drop));

            let mut envelopes = context_envelopes;
            envelopes.extend(new_envelopes);

            ProcessorPortWindow {
                port: snap.port.clone(),
                envelopes,
                new_from,
                dropped,
            }
        })
        .collect();

    debug_assert!(
        !all_push_ids.is_empty(),
        "drain_step: all_push_ids empty — snapshot Some but no triggering rows"
    );

    let activation = ProcessorActivation { ports };

    // Step 3 (ack-at-activation-start): mark all push rows across all triggering ports
    // delivered BEFORE the guest executes. At-most-once; crash between here and guest
    // completing means the batch is gone (decided semantics).
    cfg.messenger.mark_pushes_delivered(&all_push_ids).await;
    // Advance last_seen_drop for EVERY included channel at ack time (defensive uniformity).
    for (channel_address, current_drop) in &current_drops {
        last_seen_drop.insert(channel_address.clone(), *current_drop);
    }

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

    // Step 5: disposition (activation-scoped). Push rows already acked above.
    match outcome {
        ProcessorOutcome::Ok(publishes) => {
            // Flush buffered publishes atomically (all-or-nothing, design §2.3).
            if !publishes.is_empty() {
                let wasm_publishes: Vec<WasmPublish<'_>> = publishes
                    .iter()
                    .map(|p| WasmPublish {
                        channel_address: &p.channel_address,
                        body: &p.payload,
                        urgency: processor_urgency_to_messaging(p.urgency),
                        reply_to: p.reply_to.as_deref(),
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
                .map(|(inp, snap)| (inp.port.as_str(), snap.new_rows.len()))
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
                 — invariant violated (snapshot Some ⟹ ≥1 port has new_rows)"
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
                 — invariant violated (snapshot Some ⟹ ≥1 port has new_rows)"
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
        .filter(|s| !s.new_rows.is_empty())
        .map(|s| {
            let first = s
                .new_rows
                .first()
                .map(|(_, e)| e.message_id.to_string())
                .unwrap_or_default();
            let last = s
                .new_rows
                .last()
                .map(|(_, e)| e.message_id.to_string())
                .unwrap_or_default();
            format!(
                "channel={} batch={} first={first} last={last}",
                s.channel_address,
                s.new_rows.len()
            )
        })
        .collect()
}

/// Owned backing for one per-port failure record; WasmBatchFailure borrows from this.
struct PortFailureBacking {
    channel: String,
    first_message_id: String,
    last_message_id: String,
    push_ids: Vec<i64>,
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
            push_ids: &b.push_ids,
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
        .filter(|s| !s.new_rows.is_empty())
        .map(|s| {
            let fid = s
                .new_rows
                .first()
                .map(|(_, e)| e.message_id.to_string())
                .unwrap_or_default();
            let lid = s
                .new_rows
                .last()
                .map(|(_, e)| e.message_id.to_string())
                .unwrap_or_default();
            PortFailureBacking {
                channel: s.channel_address.clone(),
                first_message_id: fid,
                last_message_id: lid,
                push_ids: s.new_rows.iter().map(|(id, _)| *id).collect(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests;

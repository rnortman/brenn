//! The attachment's publish plane: what an attacher may write, under which
//! sub-identity, and the sender-scoped mirror of what it has parked.
//!
//! Two questions and nothing else reach the profile here — *who is sending*
//! (the attribution, admitted against the route's declared set and minted into
//! the envelope's sender) and *may that sender write this channel*. Neither
//! names a component, a port, or an application concept: an attribution is an
//! opaque string the operator must already have written, and a channel is an
//! address. Where the message lands is the channel's own business, decided
//! inside the publish pipeline.
//!
//! The parked-set mirror lives here for the same reason: a parked message
//! belongs to the sender that parked it, so a deferred view is cut at
//! `(attribution, channel)` — the publish plane's own grain.

#![allow(dead_code)]

#[cfg(test)]
mod tests;

use brenn_attach_proto::{
    BatchDeferredOp, BatchEntry, DeferredOpKind, DeferredViewEntry, PublishBatchOutcome,
    PublishOutcome, ServerFrame, Urgency,
};
use brenn_budget::{MAX_PUBLISH_BYTES_PER_ACTIVATION, MAX_PUBLISHES_PER_ACTIVATION};
use brenn_lib::messaging::store::{DeferralOutcome, DeferredMessage};
use brenn_lib::messaging::{
    ParticipantId, PublishResult, SurfaceBatchPublish, SurfaceSendVerdict, utc_from_epoch_ms,
};
use brenn_lib::token_bucket::{TokenBucket, TokenBucketOutcome};
use chrono::{DateTime, Utc};
use tracing::{error, info, warn};
use uuid::Uuid;

use super::profile::{DeferredTarget, PublishPosture};
use super::registry::DeferredViewPush;
use super::session::{AttachSessionCtx, FrameOutcome, SessionCounters, sanitize_client_detail};

/// Transport-level oversized-body rejections one connection may accumulate
/// before the next one is a violation.
///
/// An oversized body is an outcome, not a kill: a correct-but-buggy sender can
/// produce one, and it is answered rather than punished. But no rate token is
/// spent on that path, so without a ceiling an authenticated attacher could
/// sustain an unthrottled parse-and-respond flood in the (body cap, frame cap]
/// window. The Nth reject on one connection escalates to a violation.
pub const BODY_TOO_LARGE_VIOLATION_THRESHOLD: u64 = 8;

/// One inbound [`ClientFrame::Publish`]'s fields, borrowed for the duration of
/// the handler.
///
/// Bundled rather than passed positionally because `channel`, `attribution`,
/// and `body` are all `&str`-ish and a transposition would typecheck —
/// `attribution` especially, where swapping it for anything else would
/// misattribute the publish's identity.
///
/// [`ClientFrame::Publish`]: brenn_attach_proto::ClientFrame::Publish
pub struct PublishRequest<'a> {
    pub channel: &'a str,
    /// The sub-identity sending, or `None` for the attacher itself.
    pub attribution: Option<&'a str>,
    pub body: &'a str,
    /// Sender intent, concrete on the wire — the client resolved the channel's
    /// configured default before sending, because it needs that default anyway
    /// to stamp the envelopes it routes without a server.
    pub urgency: Urgency,
    pub correlation: Option<u64>,
}

/// Handle a `Publish` frame — one immediate message, addressed by channel.
///
/// Order, and why: the sub-identity is admitted first (a sender must be
/// somebody before it can act), then the channel against that sender's
/// publishable set. Both are violations — an undeclared attribution and an
/// unpublishable channel are alike things a conforming attacher cannot produce,
/// and unknown is indistinguishable from unauthorized on the wire (no existence
/// oracle). Neither spends a rate token: a publish that cannot succeed consumes
/// nothing.
///
/// Then the body cap (an outcome, metered and escalating — see
/// [`BODY_TOO_LARGE_VIOLATION_THRESHOLD`]), then the connection's rate bucket
/// (an outcome; a legitimate retry loop reaches it), then the bus. What each of
/// the bus's own refusals means is the profile's call: on a channel boot proved
/// reachable and policy-covered an invariant-excluded outcome is a broken
/// server, and on a diagnostics channel the same outcome is reported instead —
/// see [`PublishPosture`].
pub async fn handle_publish(
    ctx: &AttachSessionCtx,
    publish_bucket: &mut TokenBucket,
    request: PublishRequest<'_>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let PublishRequest {
        channel,
        attribution,
        body,
        urgency,
        correlation,
    } = request;

    // 1. Who is sending. An unknown attribution is a violation, never a silent
    //    demotion to the bare identity: demoting would let a non-conforming
    //    client launder a sub-identity's traffic onto the attacher's own budget,
    //    which is exactly the blast-radius scoping this grain exists to enforce.
    if ctx.profile.admit_attribution(attribution).is_none() {
        return ctx.violation(format!(
            "Publish under undeclared attribution {}",
            sanitize_client_detail(attribution.unwrap_or_default()),
        ));
    }

    // 2. May that sender write here.
    if let Err(violation) = assert_publishable(ctx, attribution, channel, "Publish") {
        return violation;
    }

    // 3. Body size, before the bucket. See the threshold constant for why this
    //    is an outcome that nonetheless escalates.
    if body.len() > ctx.max_body_bytes {
        // First occurrence gates the warn, keyed off the counter itself (no
        // parallel flag to drift): only this arm bumps it.
        if counters.publish_body_too_large == 0 {
            warn!(
                len = body.len(),
                max = ctx.max_body_bytes,
                "attach Publish body exceeds max_body_bytes; rejecting"
            );
        }
        counters.publish_body_too_large += 1;
        if counters.publish_body_too_large >= BODY_TOO_LARGE_VIOLATION_THRESHOLD {
            return ctx.violation(format!(
                "persistent oversized Publish bodies ({} rejects this connection)",
                counters.publish_body_too_large
            ));
        }
        let frame = ServerFrame::PublishResult {
            correlation,
            outcome: PublishOutcome::BodyTooLarge {
                len: body.len() as u64,
                max: ctx.max_body_bytes as u64,
            },
        };
        return super::session::send_frame(&ctx.tx, frame, counters).await;
    }

    // 4. The connection's rate bucket — the first gate, tripping before the
    //    bus-level per-sender gate. Denied is not a kill.
    match publish_bucket.try_consume() {
        TokenBucketOutcome::Granted => {}
        TokenBucketOutcome::GrantedAfterSuppression { suppressed } => {
            warn!(
                suppressed,
                "attach Publish rate limit lifted, publishes were suppressed"
            );
        }
        TokenBucketOutcome::Denied { first } => {
            counters.publish_rate_limited(attribution);
            if first {
                warn!("rate-limiting attach Publish from this connection");
            }
            let frame = ServerFrame::PublishResult {
                correlation,
                outcome: PublishOutcome::RateLimited,
            };
            return super::session::send_frame(&ctx.tx, frame, counters).await;
        }
    }

    // 5. Publish. The budget bucket is `(scope, attribution)`: a sub-identity's
    //    retry loop drains its own allowance and leaves its siblings and the
    //    attacher's own writes able to publish.
    let posture = ctx.profile.publish_posture(channel);
    let scope = ctx.profile.send_budget_scope();
    let outcome = match ctx
        .messenger
        .publish_from_surface(scope, attribution, channel, body, urgency)
        .await
    {
        PublishResult::Ok { .. } => {
            counters.publish_ok(attribution);
            if matches!(posture, PublishPosture::Diagnostic) {
                // The auth layer attests account and session; a report body
                // carries neither, because a server-attested fact does not
                // belong in an attacher-authored body. Keyed by this record and
                // the envelope's `publish_ts`, the correlation is restored.
                info!(
                    target: "attach_report",
                    attacher = %ctx.profile.attacher().as_str(),
                    session_id = %ctx.session_id,
                    account = %ctx.account,
                    channel,
                    "attach diagnostic report published"
                );
            }
            PublishOutcome::Ok
        }
        // Reaching this means the transport pre-check (step 3) and the pipeline
        // disagree on body size — a config-wiring bug, since both derive from
        // one `max_body_bytes`. Not panicked (the body is client-controlled
        // input), but it must scream: a bare counter bump would fold silently
        // into the routine transport-rejection count.
        PublishResult::BodyTooLarge { len, max } => {
            error!(
                len,
                max,
                transport_max = ctx.max_body_bytes,
                "attach Publish: transport and messenger body-size caps disagree"
            );
            counters.publish_body_cap_disagreement += 1;
            PublishOutcome::BodyTooLarge {
                len: len as u64,
                max: max as u64,
            }
        }
        // The channel's existence, its publish ACL coverage, and this sender's
        // authority over it are all boot-resolved and boot-static, so a denial
        // here is a broken boot invariant — and not attacker-reachable, the one
        // client influence having been killed at step 2. `DeferredQuotaExceeded`
        // joins them: it can only arise from a future release time, which this
        // single-publish path never passes. So does `ImpetusUnauthorized`: the
        // attachment frames carry no impetus.
        other @ (PublishResult::MissingSender
        | PublishResult::AclDenied(_)
        | PublishResult::UnknownChannel(_)
        | PublishResult::MalformedAddress(_)
        | PublishResult::DeferredQuotaExceeded { .. }
        | PublishResult::ImpetusUnauthorized) => match posture {
            PublishPosture::Diagnostic => {
                error!(
                    attacher = %ctx.profile.attacher().as_str(),
                    session_id = %ctx.session_id,
                    account = %ctx.account,
                    channel,
                    outcome = ?other,
                    // Client-composed content: rendered via `Debug` so embedded
                    // newlines and escapes cannot forge or mangle lines in the
                    // operator's primary diagnostic stream.
                    body = ?body,
                    "attach diagnostic report publish failed; the report is preserved in this log \
                     line only"
                );
                PublishOutcome::Failed
            }
            PublishPosture::Invariant => panic!(
                "attach session: publish onto {channel} rejected: {other:?} — the profile admits \
                 only boot-validated channels, so every one of these outcomes is a broken boot \
                 invariant"
            ),
        },
        // The sender's send budget and the per-(sender, channel) send-rate gate
        // can both deny. What they mean to a client is identical — slow down —
        // so both map to one wire outcome; each gate emitted its own first-denial
        // warn.
        PublishResult::BudgetExhausted | PublishResult::RateLimited => {
            counters.publish_rate_limited(attribution);
            PublishOutcome::RateLimited
        }
    };
    let frame = ServerFrame::PublishResult {
        correlation,
        outcome,
    };
    super::session::send_frame(&ctx.tx, frame, counters).await
}

// ---------------------------------------------------------------------------
// The atomic batch flush
// ---------------------------------------------------------------------------

/// One inbound [`ClientFrame::PublishBatch`]'s fields, borrowed for the duration
/// of the handler.
///
/// Bundled because the two lists are both slices of batch members and mean
/// opposite things about ordering — the ops run first, the publishes second — so
/// a transposition would typecheck and silently invert the flush.
///
/// [`ClientFrame::PublishBatch`]: brenn_attach_proto::ClientFrame::PublishBatch
pub struct PublishBatchRequest<'a> {
    /// The sub-identity whose activation produced this flush, or `None` for the
    /// attacher itself.
    pub attribution: Option<&'a str>,
    pub correlation: u64,
    pub publishes: &'a [BatchEntry],
    pub deferred_ops: &'a [BatchDeferredOp],
}

/// One admitted entry of a batch, resolved against the profile and the flush's
/// single clock read before any of the batch is applied.
struct ResolvedBatchEntry<'a> {
    channel: &'a str,
    body: &'a str,
    urgency: Urgency,
    /// The release time, or `None` for an immediate publish — including a time
    /// the flush's clock read has already passed, which is decided here so every
    /// entry of one flush answers park-vs-immediate at the same instant.
    deliver_after: Option<DateTime<Utc>>,
}

/// One control op of a batch, resolved before any of it is applied: the batch is
/// atomic, so every check that can kill the connection runs first.
struct ResolvedDeferredOp<'a> {
    channel: &'a str,
    message_id: Uuid,
    /// `None` for a cancel; the edit's two halves otherwise, the release time
    /// already converted from the wire's epoch milliseconds.
    edit: Option<(Option<String>, Option<DateTime<Utc>>)>,
}

/// What one control op left behind.
enum OpEffect {
    /// The sender's parked set changed: the release sweep needs waking and the
    /// view needs restating.
    Applied,
    /// The message released between the snapshot the attacher acted on and this
    /// frame. Nothing changed, but the view is restated anyway so a wrong mirror
    /// converges.
    Raced,
}

/// Handle a `PublishBatch` frame — one activation's flush, applied whole or not
/// at all.
///
/// Every entry in a batch already passed the attacher's own buffer-time gates —
/// publishable channel, body cap, per-activation caps — so an entry arriving
/// broken here means the client is not a conforming attacher. That is fail2ban
/// signal, not a soft outcome, and every per-entry check is therefore
/// violation-grade.
///
/// The per-connection publish bucket does not gate this frame: it meters whole
/// publishes and a batch is one frame carrying up to
/// [`MAX_PUBLISHES_PER_ACTIVATION`] of them, so drawing one token would
/// under-count it and drawing N would starve any batch wider than the burst. The
/// pipe is bounded by the frame cap; the *principal* is bounded by the sender's
/// send budget (step 4).
pub async fn handle_publish_batch(
    ctx: &AttachSessionCtx,
    request: PublishBatchRequest<'_>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let PublishBatchRequest {
        attribution,
        correlation,
        publishes,
        deferred_ops,
    } = request;

    // 1. Who is sending. An undeclared attribution is a violation rather than a
    //    demotion to the bare identity: demoting would let a non-conforming
    //    client launder a flush onto the attacher's own budget, which is the
    //    blast-radius scoping this grain exists to enforce.
    let Some(sender) = ctx.profile.admit_attribution(attribution) else {
        return ctx.violation(format!(
            "PublishBatch under undeclared attribution {}",
            sanitize_client_detail(attribution.unwrap_or_default()),
        ));
    };

    // 2. Batch shape. A conforming attacher never flushes an empty buffer (it
    //    sends no frame at all) and never buffers past a cap (it answers its
    //    caller at the cap instead), so each is non-conforming signal.
    if publishes.is_empty() && deferred_ops.is_empty() {
        return ctx.violation("empty PublishBatch");
    }
    if publishes.len() > MAX_PUBLISHES_PER_ACTIVATION {
        return ctx.violation(format!(
            "PublishBatch carries {} entries, over the {MAX_PUBLISHES_PER_ACTIVATION} \
             per-activation cap",
            publishes.len(),
        ));
    }
    if deferred_ops.len() > MAX_PUBLISHES_PER_ACTIVATION {
        return ctx.violation(format!(
            "PublishBatch carries {} control ops, over the {MAX_PUBLISHES_PER_ACTIVATION} \
             per-activation cap",
            deferred_ops.len(),
        ));
    }
    // Without this arm the entry-count cap alone lets a hostile client hand the
    // server a full batch of max-size bodies — retained rows and their push
    // fan-out — in one frame, on a path whose whole doctrine is that
    // non-conforming input is fail2ban signal. Edit bodies are summed with them:
    // an edit rewrites a parked row, so a frame of max-size edits is the same
    // work behind the same one-token draw.
    let publish_bytes: usize = publishes.iter().map(|entry| entry.body.len()).sum();
    let edit_bytes: usize = deferred_ops
        .iter()
        .map(|op| match &op.op {
            DeferredOpKind::Cancel => 0,
            DeferredOpKind::Edit { body, .. } => body.as_ref().map_or(0, String::len),
        })
        .sum();
    let total_bytes = publish_bytes + edit_bytes;
    if total_bytes > MAX_PUBLISH_BYTES_PER_ACTIVATION {
        return ctx.violation(format!(
            "PublishBatch carries {total_bytes} body bytes, over the \
             {MAX_PUBLISH_BYTES_PER_ACTIVATION}-byte per-activation cap",
        ));
    }

    // 3. Resolve everything before applying any of it: the batch is atomic, so a
    //    check that ran per entry as it applied could kill the connection with a
    //    prefix already committed.
    //
    //    One clock read serves the whole batch's park-vs-immediate decisions, so
    //    every entry is judged against the same instant.
    let flush_now = Utc::now();
    let resolved = match resolve_batch_entries(ctx, attribution, publishes, flush_now) {
        Ok(resolved) => resolved,
        Err(violation) => return violation,
    };
    let resolved_ops = match resolve_deferred_ops(ctx, attribution, deferred_ops) {
        Ok(ops) => ops,
        Err(violation) => return violation,
    };

    // 4. The sender's send budget, one all-or-nothing draw. One token per
    //    publish, and one for a batch that publishes nothing: an ops-only flush
    //    is still a frame the principal sent and work the server did, and a path
    //    that draws zero is a path a client can ride for free.
    //
    //    The control ops themselves are unpriced, which is deliberate and not
    //    symmetric: each applied durable op is its own store write, so a flush
    //    carrying the op cap costs far more server work than the one token it
    //    draws.
    //    TODO(surface-op-send-budget): price control ops in the send budget.
    let draw =
        u32::try_from(resolved.len().max(1)).expect("batch length is capped well below u32::MAX");
    if ctx.messenger.draw_surface_send_budget_for_batch(
        ctx.profile.send_budget_scope(),
        attribution,
        draw,
    ) == SurfaceSendVerdict::Denied
    {
        // Not a kill: the attacher re-parks the batch and retries on the next
        // refill. Count each entry that did not publish.
        for _ in 0..resolved.len() {
            counters.publish_rate_limited(attribution);
        }
        let frame = ServerFrame::PublishBatchResult {
            correlation,
            outcome: PublishBatchOutcome::RateLimited,
        };
        return super::session::send_frame(&ctx.tx, frame, counters).await;
    }

    // 5. The control ops, ahead of the publishes. A violation here can leave
    //    earlier ops of the same batch applied: whether a message is someone
    //    else's is only knowable by asking the store, and each ask is its own
    //    round trip. The ops that landed were legitimate ones; the connection
    //    dies on the one that was not.
    //
    //    Op channels are collected for the one view-emission pass at the end, so a
    //    channel this flush both edited and parked on is restated once, from the
    //    truth after both. A lost race collects its channel too: the restatement
    //    corrects a wrong mirror that could otherwise provoke infinite cancel loops.
    let mut op_channels: Vec<&str> = Vec::with_capacity(resolved_ops.len());
    let mut applied_any = false;
    for op in &resolved_ops {
        match apply_deferred_op(ctx, &sender, attribution, op, flush_now).await {
            Ok(effect) => {
                applied_any |= matches!(effect, OpEffect::Applied);
                op_channels.push(op.channel);
            }
            Err(violation) => {
                // This connection dies, so the end-of-batch emission pass never
                // runs — but the ops that landed before the violating one changed
                // a set every attachment of this attacher mirrors. Restate those
                // here or a sibling attachment keeps a schedule that no longer
                // exists: if the op emptied the set, no release and no later
                // change will ever push a correcting view.
                if applied_any {
                    ctx.messenger.dispatch_kick();
                }
                emit_views(ctx, attribution, op_channels, flush_now).await;
                return violation;
            }
        }
    }
    // The release sweep sleeps to the earliest deadline it last computed, so an
    // edit that moved a release earlier has to wake it or the message waits out
    // the poll interval. A lost race moved no deadline, so it does not kick.
    if applied_any {
        ctx.messenger.dispatch_kick();
    }

    // 6. Stamp every entry, in call order, in one pass across the whole batch —
    //    before the substrate split, so call order is visible *across* the class
    //    boundary and not merely within each half. Each entry takes
    //    max(prev + 1, now), so the stamps are strictly increasing whatever the
    //    clock does. The delivered envelope's `publish_ts` carries this at ns
    //    precision; it is the ordering contract's only observable.
    let mut prev_ts: Option<i64> = None;
    let batch: Vec<SurfaceBatchPublish<'_>> = resolved
        .iter()
        .map(|entry| {
            let now_ns = brenn_lib::messaging::db::utc_to_ns(Utc::now());
            let ts = match prev_ts {
                None => now_ns,
                Some(prev) => std::cmp::max(prev + 1, now_ns),
            };
            prev_ts = Some(ts);
            SurfaceBatchPublish {
                channel_address: entry.channel,
                body: entry.body,
                urgency: entry.urgency,
                publish_ts_ns: ts,
                deliver_after: entry.deliver_after,
            }
        })
        .collect();

    // 7. Apply. Entries whose schedule the channel's deferred cap refused
    //    published nothing, so they reduce the publish count. No wire error: the
    //    activation already returned, so there is nothing left to answer, and the
    //    entries it published unconditionally must not be lost to one it merely
    //    scheduled.
    let schedules_dropped = ctx
        .messenger
        .publish_batch_from_surface(ctx.profile.send_budget_scope(), attribution, &batch)
        .await;
    for _ in 0..(batch.len() - schedules_dropped) {
        counters.publish_ok(attribution);
    }

    // Re-state the sender's parked view on every channel this batch scheduled
    // against or aimed a control op at, whichever half carried it and whether or
    // not the park or the op was admitted: the view is recomputed from the store,
    // so a refused schedule and a lost race both land on the truth. Judged at the
    // flush's one clock read, the same instant that decided park-vs-immediate.
    let touched: Vec<&str> = resolved
        .iter()
        .filter(|entry| entry.deliver_after.is_some())
        .map(|entry| entry.channel)
        .chain(op_channels)
        .collect();
    emit_views(ctx, attribution, touched, flush_now).await;

    let frame = ServerFrame::PublishBatchResult {
        correlation,
        outcome: PublishBatchOutcome::Ok,
    };
    super::session::send_frame(&ctx.tx, frame, counters).await
}

/// Resolve every publish entry of a batch, or the violation that kills the
/// connection.
fn resolve_batch_entries<'a>(
    ctx: &AttachSessionCtx,
    attribution: Option<&str>,
    publishes: &'a [BatchEntry],
    flush_now: DateTime<Utc>,
) -> Result<Vec<ResolvedBatchEntry<'a>>, FrameOutcome> {
    let mut resolved = Vec::with_capacity(publishes.len());
    for entry in publishes {
        assert_publishable(ctx, attribution, &entry.channel, "PublishBatch entry")?;
        if entry.body.len() > ctx.max_body_bytes {
            return Err(ctx.violation(format!(
                "PublishBatch entry on channel {} carries a {}-byte body, over the {}-byte cap",
                sanitize_client_detail(&entry.channel),
                entry.body.len(),
                ctx.max_body_bytes,
            )));
        }
        // Left unchecked, an unrepresentable release time collapses into an
        // immediate publish, silently turning a schedule into a now.
        let deliver_after = match entry.deliver_after {
            None => None,
            Some(ms) => {
                let Some(at) = utc_from_epoch_ms(ms) else {
                    return Err(ctx.violation(format!(
                        "PublishBatch entry on channel {} carries an unrepresentable deliver_after \
                         of {ms} ms",
                        sanitize_client_detail(&entry.channel),
                    )));
                };
                Some(at).filter(|at| *at > flush_now)
            }
        };
        resolved.push(ResolvedBatchEntry {
            channel: entry.channel.as_str(),
            body: entry.body.as_str(),
            urgency: entry.urgency,
            deliver_after,
        });
    }
    Ok(resolved)
}

/// Resolve every control op of a batch, or the violation that kills the
/// connection.
fn resolve_deferred_ops<'a>(
    ctx: &AttachSessionCtx,
    attribution: Option<&str>,
    ops: &'a [BatchDeferredOp],
) -> Result<Vec<ResolvedDeferredOp<'a>>, FrameOutcome> {
    let mut resolved = Vec::with_capacity(ops.len());
    for op in ops {
        // A parked set belongs to a sender on a channel, so the authority to
        // touch one is the authority to write that channel — the same question,
        // asked of the same seam.
        assert_publishable(ctx, attribution, &op.channel, "PublishBatch control op")?;
        let edit = match &op.op {
            DeferredOpKind::Cancel => None,
            DeferredOpKind::Edit {
                body,
                deliver_after,
            } => {
                if let Some(body) = body
                    && body.len() > ctx.max_body_bytes
                {
                    return Err(ctx.violation(format!(
                        "PublishBatch control op on channel {} carries a {}-byte edit body, over \
                         the {}-byte cap",
                        sanitize_client_detail(&op.channel),
                        body.len(),
                        ctx.max_body_bytes,
                    )));
                }
                let release_at = match deliver_after {
                    None => None,
                    Some(ms) => {
                        let Some(at) = utc_from_epoch_ms(*ms) else {
                            return Err(ctx.violation(format!(
                                "PublishBatch control op on channel {} carries an unrepresentable \
                                 deliver_after of {ms} ms",
                                sanitize_client_detail(&op.channel),
                            )));
                        };
                        Some(at)
                    }
                };
                Some((body.clone(), release_at))
            }
        };
        resolved.push(ResolvedDeferredOp {
            channel: op.channel.as_str(),
            message_id: op.message_id,
            edit,
        });
    }
    Ok(resolved)
}

/// The publishable check every write path runs, with the frame element named
/// for the security log.
///
/// An unpublishable channel is a violation (not an outcome — this path has none to
/// answer with), and a page-local channel — which no profile's publishable set may
/// contain — is an assert.
fn assert_publishable(
    ctx: &AttachSessionCtx,
    attribution: Option<&str>,
    channel: &str,
    what: &str,
) -> Result<(), FrameOutcome> {
    if !ctx.profile.publishable(attribution, channel) {
        return Err(ctx.violation(format!(
            "{what} names unpublishable channel {}",
            sanitize_client_detail(channel),
        )));
    }
    assert!(
        !brenn_envelope::is_local_channel(channel),
        "attach session: publishable channel {channel} is page-local — a profile's publishable \
         sets must exclude local addresses, which never cross the wire"
    );
    Ok(())
}

/// Apply one resolved control op under the batch's sender. The caller restates
/// the view for the channel on either outcome.
///
/// The three outcomes:
///
/// - **Applied** — the parked set changed, so its view owes a restatement.
/// - **`NotDeferred`** — the message released between the snapshot the attacher
///   acted on and this frame. Logged and counted, never punished: a conforming
///   attacher can always lose that race. The view is restated regardless: a wrong
///   mirror (dropped emission, say) can provoke ops naming schedules the server
///   does not hold, and without a restatement the phantom entry would be
///   cancelled over and over. A recompute is idempotent, so restating on a
///   genuine race costs one redundant snapshot.
/// - **`WrongSender`** — a violation. The ids a conforming attacher can name come
///   from a sender-scoped view, so this is a client naming a schedule no window
///   ever offered it. Reported rather than panicked precisely because it is
///   client-reachable: a panic here would be a remote kill switch.
async fn apply_deferred_op(
    ctx: &AttachSessionCtx,
    sender: &ParticipantId,
    attribution: Option<&str>,
    op: &ResolvedDeferredOp<'_>,
    now: DateTime<Utc>,
) -> Result<OpEffect, FrameOutcome> {
    let outcome = match &op.edit {
        None => {
            ctx.messenger
                .cancel_deferred_for_sender(op.channel, sender.as_str(), op.message_id, now)
                .await
        }
        Some((body, release_at)) => {
            ctx.messenger
                .edit_deferred_for_sender(
                    op.channel,
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
            ctx.messenger
                .record_deferred_control_race(sender.as_str(), op.channel);
            info!(
                attacher = %ctx.profile.attacher().as_str(),
                attribution = attribution.unwrap_or("<attacher>"),
                channel = op.channel,
                "attach deferred control op is a no-op — the message released between the \
                 activation's snapshot and the flush"
            );
            Ok(OpEffect::Raced)
        }
        DeferralOutcome::WrongSender => Err(ctx.violation(format!(
            "PublishBatch control op names message {} on {}, parked by another sender",
            op.message_id,
            sanitize_client_detail(op.channel),
        ))),
    }
}

/// Restate the sender's parked view on each named channel, once per channel.
///
/// Deduped because one flush can park on and aim an op at the same channel, and
/// the view is recomputed from the store — so the second emission would carry the
/// same snapshot the first did.
async fn emit_views(
    ctx: &AttachSessionCtx,
    attribution: Option<&str>,
    channels: Vec<&str>,
    now: DateTime<Utc>,
) {
    let mut channels = channels;
    channels.sort_unstable();
    channels.dedup();
    for channel in channels {
        broadcast_deferred_view(ctx, attribution, channel, now).await;
    }
}

// ---------------------------------------------------------------------------
// The parked-set mirror
// ---------------------------------------------------------------------------

/// The wire form of one sender's parked messages: the identity both authorities
/// know each message by, its body, and its release time in epoch milliseconds
/// UTC — the units an attacher's own clock reads.
pub fn deferred_view_entries(parked: &[DeferredMessage]) -> Vec<DeferredViewEntry> {
    parked
        .iter()
        .map(|message| DeferredViewEntry {
            message_id: message.envelope.message_id,
            body: message.envelope.body.clone(),
            deliver_after: u64::try_from(message.release_at.timestamp_millis()).expect(
                "a parked release time was admitted from epoch milliseconds, so it is at or after \
                 the epoch",
            ),
        })
        .collect()
}

/// The sender one attribution's parked set belongs to.
///
/// Minting is the profile's, so a parked set is only ever read or written under
/// an identity the route's own configuration named.
///
/// # Panics
///
/// If `attribution` is not one the profile declares. Callers must pass
/// server-side data only (an already-admitted attribution, or the profile's own
/// target list), so an undeclared one is a broken invariant, never client input.
fn parked_sender(ctx: &AttachSessionCtx, attribution: Option<&str>) -> ParticipantId {
    ctx.profile
        .admit_attribution(attribution)
        .unwrap_or_else(|| {
            panic!(
                "attach session: parked set for attribution {attribution:?} the profile does not \
                 declare — every caller admits it first, and a seeding target is the profile's own"
            )
        })
}

/// Recompute one sub-identity's parked view on `channel` and push it at every
/// attachment of this attacher.
///
/// Every attachment, not just the one whose action changed the set: the parked
/// set belongs to the sub-identity, which every attachment of the attacher
/// shares, so a view held by only one of them would be a second answer to a
/// question that has one.
///
/// Recompute and push run under the messenger's deferred-view gate, so this
/// emission and the release sweep's reach an attacher in the order they read the
/// store. Without it the two can invert — a snapshot carries no version, so the
/// attacher would keep the older one and mirror a schedule that has already
/// released, with no further emission owed if that release was the set's last
/// change.
///
/// # Panics
///
/// If `attribution` is not one the profile declares. Callers reach here from a
/// frame whose attribution was already admitted, or from the profile's own
/// target list, so an undeclared one is a broken invariant rather than client
/// input.
pub async fn broadcast_deferred_view(
    ctx: &AttachSessionCtx,
    attribution: Option<&str>,
    channel: &str,
    now: DateTime<Utc>,
) {
    let sender = parked_sender(ctx, attribution);
    let _order = ctx.messenger.lock_deferred_view_gate().await;
    let entries = deferred_view_entries(
        &ctx.messenger
            .deferred_view_for_sender(channel, sender.as_str(), now)
            .await,
    );
    ctx.registry.push_deferred_view(
        ctx.profile.attacher().as_str(),
        &DeferredViewPush {
            channel: channel.to_string(),
            attribution: attribution.map(str::to_string),
            entries,
        },
    );
}

/// Seed this connection's parked-set mirrors, immediately behind `Welcome`.
///
/// One frame per `(attribution, channel)` whose parked set is nonempty. The
/// attacher clears every mirror at `Welcome`, so an absent frame means an empty
/// set — which is also what makes a set that emptied while the attacher was away
/// arrive correctly empty.
///
/// This connection only: the frames ride the same FIFO writer queue `Welcome`
/// just entered, which is what puts them behind it. The seeding is deliberately
/// not sequenced behind any application-config delivery: the mirrors are plain
/// data keyed `(channel, attribution)`, so an attacher can absorb them before it
/// has configured itself.
pub async fn seed_deferred_views(
    ctx: &AttachSessionCtx,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let now = Utc::now();
    let targets = ctx.profile.deferred_view_targets();
    for target in targets {
        let sender = parked_sender(ctx, target.attribution.as_deref());
        let entries = deferred_view_entries(
            &ctx.messenger
                .deferred_view_for_sender(&target.channel, sender.as_str(), now)
                .await,
        );
        if entries.is_empty() {
            continue;
        }
        let frame = ServerFrame::DeferredView {
            channel: target.channel.clone(),
            attribution: target.attribution.clone(),
            entries,
        };
        if let FrameOutcome::Disconnect = super::session::send_frame(&ctx.tx, frame, counters).await
        {
            return FrameOutcome::Disconnect;
        }
    }
    for orphan in orphaned_parked_sets(ctx, targets, now).await {
        warn!(
            attacher = %ctx.profile.attacher().as_str(),
            attribution = orphan.attribution.as_deref().unwrap_or("<attacher>"),
            channel = orphan.channel,
            "parked messages this attachment cannot see: the sender holds a schedule on a channel \
             outside its seeding targets. They release normally; nothing on the attacher can view, \
             edit, or cancel them until the config names that sub-identity and channel again"
        );
    }
    FrameOutcome::Continue
}

/// The parked sets of this attacher that seeding cannot reach — the ones outside
/// `targets`.
///
/// A set goes orphaned when the config that would have named it goes away: a
/// sub-identity the profile no longer declares, or one whose authority over that
/// channel is gone. The entries release on the server regardless — a durable
/// schedule outliving its author's binding is part of what durable parking is
/// for — so nothing is lost and no ladder is charged. What is gone is the
/// attacher's ability to see them, and that is an operator's decision to have
/// made, so it is reported rather than repaired here.
///
/// Sub-identities only: bare-identity sets are outside what this sweep can
/// report. Nothing is lost — those entries release the same way — but an orphaned
/// bare-identity set is silent.
async fn orphaned_parked_sets(
    ctx: &AttachSessionCtx,
    targets: &[DeferredTarget],
    now: DateTime<Utc>,
) -> Vec<DeferredTarget> {
    ctx.messenger
        .parked_surface_components(ctx.profile.send_budget_scope(), now)
        .await
        .into_iter()
        .map(|parked| DeferredTarget {
            channel: parked.channel,
            attribution: Some(parked.instance),
        })
        .filter(|parked| !targets.contains(parked))
        .collect()
}

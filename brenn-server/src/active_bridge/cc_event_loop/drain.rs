//! Startup event/messaging-push drain: repo-sync staleness filter, per-slug
//! collapse, combined render, send + mark-delivered.

use tracing::{info, warn};

use super::super::{ActiveBridge, emit_prerendered_summary};

/// Drain pending events from the event queue and deliver as a batch message.
///
/// Called at the start of cc_event_loop for singleton apps. Events are
/// formatted as a single internal user message, delivered to CC, then marked
/// delivered in the DB. At-least-once: if the send fails or the process
/// crashes before marking, events stay pending for next wake.
///
/// Extensions for repo-sync (see `docs/designs/repo-sync.md`):
/// - **Staleness filter**: `repo_sync:*` events for conversations whose
///   `updated_at` is older than `stale_conversation_days` are marked
///   delivered immediately without injection. Other event sources are
///   unaffected. Resume-time sync-manager pokes re-synthesize fresh state.
/// - **Per-slug collapser**: multiple `repo_sync:pulled`/`:conflict` rows
///   for the same slug fold into a single synthesized `repo_sync:summary`
///   entry appended to the batch. The originals get marked delivered
///   alongside the synthesized batch.
pub(in crate::active_bridge) async fn drain_pending_events(bridge: &ActiveBridge) {
    // Held until this drain has advanced past what it sent: a concurrent
    // backlog delivery would otherwise read the same unseen suffix and send it
    // a second time.
    let _delivering = bridge.bus_delivery.lock().await;

    // Two sources, two delivery models. Ingress events are channel-less rows
    // handed to this conversation directly; bus messages are what the
    // conversation's positions on its subscribed channels are holding for it.
    let (ingress_events, bus_delivery) = if let Some(messenger) = &bridge.messenger {
        let subscriber =
            brenn_lib::messaging::ParticipantId::for_conversation(bridge.conversation_id);
        (
            messenger
                .load_pending_ingress(&subscriber)
                .await
                .into_iter()
                .map(|(_, ev)| ev)
                .collect::<Vec<_>>(),
            messenger
                .conversation_delivery(bridge.conversation_id)
                .await,
        )
    } else {
        (Vec::new(), Default::default())
    };

    // Check for repo_sync rows to fetch the conversation's updated_at.
    let conv_updated_at_str = if ingress_events
        .iter()
        .any(|e| brenn_messaging::is_repo_sync_source(&e.source))
    {
        let conn = bridge.db.lock().await;
        Some(brenn_db::conversation::get_updated_at(
            &conn,
            bridge.conversation_id,
        ))
    } else {
        None
    };

    if ingress_events.is_empty() && bus_delivery.is_empty() {
        return;
    }

    // Staleness filter — drain-time, not enqueue-time (see design). We only
    // apply it to repo_sync:* rows; cron/discord/pfin have their own
    // semantics. Drop stale repo_sync rows silently, mark delivered.
    //
    // All ingress rows are in the unified store; stale push IDs are marked
    // delivered via mark_pending_pushes_delivered.
    let (kept, stale) = if ingress_events.is_empty() {
        (ingress_events, Vec::new())
    } else if let Some(updated_at_str) = conv_updated_at_str {
        let conv_updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|e| {
                panic!(
                    "conversation {} updated_at {:?} is not RFC3339: {e}",
                    bridge.conversation_id, updated_at_str
                )
            });
        let staleness = brenn_messaging::repo_sync_staleness_days();
        brenn_messaging::split_stale_repo_sync(
            ingress_events,
            conv_updated_at,
            chrono::Utc::now(),
            staleness,
        )
    } else {
        (ingress_events, Vec::new())
    };
    if kept.is_empty() && stale.is_empty() && bus_delivery.is_empty() {
        return;
    }

    // Stale rows get marked delivered immediately (drop-silently semantics
    // per design). Their push IDs are carried in stale[].id (Event.id == push_id
    // by construction in row_to_drain_push).
    if !stale.is_empty() {
        let stale_push_ids: Vec<i64> = stale
            .iter()
            .filter(|e| e.id != brenn_messaging::SYNTHETIC_EVENT_ID)
            .map(|e| e.id)
            .collect();
        if !stale_push_ids.is_empty() {
            let conn = bridge.db.lock().await;
            brenn_messaging_store::db::mark_pending_pushes_delivered(&conn, &stale_push_ids);
        }
        info!(
            conversation_id = bridge.conversation_id,
            dropped = stale.len(),
            "repo_sync: dropped stale events at drain (conversation idle too long)"
        );
    }

    // Collapse per-slug repo_sync events into a single summary entry.
    let collapsed = brenn_messaging::collapse_repo_sync(kept);

    // Pre-render the system-message card (collapsed <details> card in chat
    // history). `render_combined_drain` is the single producer of
    // (text, rendered_html, messaging_card_html) — `drain_pending_events`
    // does not call the formatters itself. Rendering is pure (markdown →
    // HTML, no I/O); doing it before the send + mark sequence means a
    // future panic in the renderer can't strand already-delivered rows.
    // The bus half is turn-provoking work, and the conversation's impetus pool
    // pays for it. Impetus carried by anything in the batch restores the pool
    // before the turn it buys; an empty pool holds the batch — nothing injected,
    // positions left owed, delivered after the next refill. The self-echo filter
    // ran upstream, so a batch of nothing but the conversation's own utterances
    // never reaches the pool at all.
    let bus_held = if bus_delivery.messages.is_empty() {
        false
    } else {
        bridge.redeem_batch_impetus(&bus_delivery.messages).await;
        !bridge.impetus_pool_has_room().await
    };
    if bus_held {
        info!(
            conversation_id = bridge.conversation_id,
            held = bus_delivery.messages.len(),
            "impetus pool empty — holding the bus batch until the conversation is attended"
        );
    }
    // The ingress half draws nothing and delivers regardless, so a held bus
    // batch renders as an events-only drain rather than as nothing.
    let no_messages: [brenn_lib::messaging::MessageEnvelope; 0] = [];
    let to_inject: &[brenn_lib::messaging::MessageEnvelope] = if bus_held {
        &no_messages
    } else {
        &bus_delivery.messages
    };
    let delivered_message_count = to_inject.len();

    // `render_combined_drain` returns `None` only when both event and
    // messaging slices are empty. That is reachable with positions still owed:
    // a bus batch consisting entirely of this conversation's own utterances is
    // filtered away by `conversation_delivery` and leaves nothing to inject.
    // The positions must still move, or every wake re-serves the same batch —
    // unless the batch is held, where leaving them owed is the whole point.
    let Some(system_render) =
        brenn_render::system_message::render_combined_drain(&collapsed.events, to_inject)
    else {
        if let Some(messenger) = &bridge.messenger
            && !bus_held
        {
            messenger
                .advance_conversation(bridge.conversation_id, bus_delivery)
                .await;
        }
        return;
    };

    // Ingress push IDs to mark delivered (survived staleness filter).
    // collapsed.events carries the surviving ingress rows; their ids are push_ids
    // from the unified store (SYNTHETIC_EVENT_ID rows have no push to mark).
    let ingress_push_ids_to_mark: Vec<i64> = collapsed
        .events
        .iter()
        .filter(|e| e.id != brenn_messaging::SYNTHETIC_EVENT_ID)
        .map(|e| e.id)
        .chain(
            collapsed
                .original_repo_sync_ids
                .iter()
                .filter(|id| **id != brenn_messaging::SYNTHETIC_EVENT_ID)
                .copied(),
        )
        .collect::<std::collections::HashSet<i64>>()
        .into_iter()
        .collect();

    info!(
        conversation_id = bridge.conversation_id,
        event_count = collapsed.events.len(),
        ingress_ids = ingress_push_ids_to_mark.len(),
        "draining queued events into CC"
    );

    // Take the messaging-card HTML out of the render before consuming it
    // by `send_system_message`; we still need it for the dual
    // `ToolUseSummary` broadcast below. `Option::take` avoids cloning.
    let mut system_render = system_render;
    let messaging_card_html = system_render.messaging_card_html.take();

    // Deliver the batch. If send fails (CC died between init and now),
    // events stay pending — at-least-once semantics. Stale rows stay too,
    // but split_stale_repo_sync is idempotent so the next drain re-filters.
    //
    // send_system_message awaits a flush ack:
    // it returns Ok only after the message has been flushed to CC's stdin.
    // A failure (broken pipe, writer exited) leaves rows delivered_at IS NULL
    // so the next drain will retry. Without the flush ack, rows could be
    // marked delivered after mpsc-enqueue but before the OS-pipe flush.
    if let Err(e) = bridge.send_system_message(system_render, None).await {
        warn!(
            conversation_id = bridge.conversation_id,
            error = %e,
            "failed to drain events — will retry on next wake"
        );
        return;
    }

    // A flush failure leaves both untouched; the batch re-serves next wake.
    if !ingress_push_ids_to_mark.is_empty() {
        let conn = bridge.db.lock().await;
        brenn_messaging_store::db::mark_pending_pushes_delivered(&conn, &ingress_push_ids_to_mark);
    }
    if !bus_held {
        if let Some(messenger) = &bridge.messenger {
            messenger
                .advance_conversation(bridge.conversation_id, bus_delivery)
                .await;
        }
        // One turn, one unit, whatever the batch's size — and nothing at all for
        // an events-only render, which the pool does not meter.
        if delivered_message_count > 0 {
            bridge.draw_impetus_pool().await;
        }
    }

    // Emit ToolUseSummary card for received messages (the dual-broadcast).
    if let Some(html) = messaging_card_html {
        emit_prerendered_summary(
            bridge,
            brenn_render::tools::messaging::MCP_MESSAGE_RECEIVED_PSEUDO_TOOL,
            html,
            format!("{delivered_message_count} bus message(s) delivered"),
        )
        .await;
    }
}

/// Deliver whatever this conversation's channels are holding for it into a live
/// bridge, then advance past it — the drain's bus half, on its own, for the
/// wake path that finds the conversation already awake.
///
/// One delivery serves every channel's unseen suffix, in publish order, so a
/// wake that arrives per message still renders each message once: the second
/// wake finds the position past it and sends nothing. `Ok` therefore means "this
/// conversation is served up to date", which is what the caller retires its
/// delivery record on; `Err` means the send failed and the position did not
/// move, so the next wake re-serves the batch.
///
/// Read, send, and advance run under the bridge's delivery lock, so "the second
/// wake finds the position past it" holds for two wakes that arrive at once as
/// well as for two that arrive in sequence.
///
/// The batch draws one unit from the conversation's impetus pool, after carried
/// impetus has restored it. An empty pool holds the batch: nothing is injected,
/// the positions stay owed, and this still reports `Ok` — "served for now", so
/// the wake record retires instead of spinning. The next refill delivers it.
pub(crate) async fn deliver_conversation_backlog(bridge: &ActiveBridge) -> Result<(), String> {
    let Some(messenger) = &bridge.messenger else {
        return Ok(());
    };
    let _delivering = bridge.bus_delivery.lock().await;
    let delivery = messenger
        .conversation_delivery(bridge.conversation_id)
        .await;
    if delivery.is_empty() {
        return Ok(());
    }
    if delivery.messages.is_empty() {
        // Positions are owed but there is nothing to say: the batch was all
        // self-echo. Advance past it and report served, or the wake pass hands
        // it back on every pass.
        messenger
            .advance_conversation(bridge.conversation_id, delivery)
            .await;
        return Ok(());
    }
    bridge.redeem_batch_impetus(&delivery.messages).await;
    if !bridge.impetus_pool_has_room().await {
        info!(
            conversation_id = bridge.conversation_id,
            held = delivery.messages.len(),
            "impetus pool empty — holding the bus batch until the conversation is attended"
        );
        return Ok(());
    }
    // The live path renders messages only — no event drain rides with it, and
    // (unlike the startup drain) no dual `ToolUseSummary` broadcast.
    let mut render = brenn_render::system_message::render_combined_drain(&[], &delivery.messages)
        .expect("non-empty messages: render_combined_drain must produce a render");
    render.messaging_card_html = None;
    bridge.send_system_message(render, None).await?;
    messenger
        .advance_conversation(bridge.conversation_id, delivery)
        .await;
    bridge.draw_impetus_pool().await;
    Ok(())
}

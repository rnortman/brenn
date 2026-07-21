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
    // Fetch pending messaging pushes — single unified source for both ingress
    // and bus messages. Returns `IngressOrBus`-tagged payloads.
    let messaging_pushes: Vec<(i64, brenn_lib::messaging::IngressOrBus)> =
        if let Some(messenger) = &bridge.messenger {
            messenger
                .load_pending_pushes(&brenn_lib::messaging::ParticipantId::for_conversation(
                    bridge.conversation_id,
                ))
                .await
        } else {
            Vec::new()
        };

    // Partition into ingress events (kind='ingress') and bus envelopes
    // (kind='brenn'). Ingress rows go through the event drain path; bus
    // rows go through the messaging path.
    let mut ingress_events: Vec<brenn_lib::messaging::ingress::Event> = Vec::new();
    let mut bus_envelopes: Vec<brenn_lib::messaging::MessageEnvelope> = Vec::new();
    let mut bus_push_ids: Vec<i64> = Vec::new();
    for (push_id, payload) in messaging_pushes {
        match payload {
            brenn_lib::messaging::IngressOrBus::Ingress(ev) => {
                ingress_events.push(ev);
            }
            brenn_lib::messaging::IngressOrBus::Bus(env) => {
                bus_push_ids.push(push_id);
                bus_envelopes.push(env);
            }
        }
    }

    // Check for repo_sync rows to fetch the conversation's updated_at.
    let conv_updated_at_str = if ingress_events
        .iter()
        .any(|e| brenn_lib::messaging::is_repo_sync_source(&e.source))
    {
        let conn = bridge.db.lock().await;
        Some(brenn_lib::conversation::get_updated_at(
            &conn,
            bridge.conversation_id,
        ))
    } else {
        None
    };

    if ingress_events.is_empty() && bus_envelopes.is_empty() {
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
        let staleness = brenn_lib::messaging::repo_sync_staleness_days();
        brenn_lib::messaging::split_stale_repo_sync(
            ingress_events,
            conv_updated_at,
            chrono::Utc::now(),
            staleness,
        )
    } else {
        (ingress_events, Vec::new())
    };
    if kept.is_empty() && stale.is_empty() && bus_envelopes.is_empty() {
        return;
    }

    // Stale rows get marked delivered immediately (drop-silently semantics
    // per design). Their push IDs are carried in stale[].id (Event.id == push_id
    // by construction in row_to_drain_push).
    if !stale.is_empty() {
        let stale_push_ids: Vec<i64> = stale
            .iter()
            .filter(|e| e.id != brenn_lib::messaging::SYNTHETIC_EVENT_ID)
            .map(|e| e.id)
            .collect();
        if !stale_push_ids.is_empty() {
            let conn = bridge.db.lock().await;
            brenn_lib::messaging::db::mark_pending_pushes_delivered(&conn, &stale_push_ids);
        }
        info!(
            conversation_id = bridge.conversation_id,
            dropped = stale.len(),
            "repo_sync: dropped stale events at drain (conversation idle too long)"
        );
    }

    // Collapse per-slug repo_sync events into a single summary entry.
    let collapsed = brenn_lib::messaging::collapse_repo_sync(kept);

    // Collect bus envelopes for the messaging renderer. Push IDs are already
    // collected above in bus_push_ids.
    let messaging_envelopes = bus_envelopes;
    let messaging_push_ids = bus_push_ids;

    // Pre-render the system-message card (collapsed <details> card in chat
    // history). `render_combined_drain` is the single producer of
    // (text, rendered_html, messaging_card_html) — `drain_pending_events`
    // does not call the formatters itself. Rendering is pure (markdown →
    // HTML, no I/O); doing it before the send + mark sequence means a
    // future panic in the renderer can't strand already-delivered rows.
    // `render_combined_drain` returns `None` only when both event and
    // messaging slices are empty — which is the early-exit case here.
    let Some(system_render) =
        crate::system_message::render_combined_drain(&collapsed.events, &messaging_envelopes)
    else {
        return;
    };

    // Ingress push IDs to mark delivered (survived staleness filter).
    // collapsed.events carries the surviving ingress rows; their ids are push_ids
    // from the unified store (SYNTHETIC_EVENT_ID rows have no push to mark).
    let ingress_push_ids_to_mark: Vec<i64> = collapsed
        .events
        .iter()
        .filter(|e| e.id != brenn_lib::messaging::SYNTHETIC_EVENT_ID)
        .map(|e| e.id)
        .chain(
            collapsed
                .original_repo_sync_ids
                .iter()
                .filter(|id| **id != brenn_lib::messaging::SYNTHETIC_EVENT_ID)
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
    // send_system_message now awaits a flush ack (design §2.2, D1 fix):
    // it returns Ok only after the message has been flushed to CC's stdin.
    // A failure (broken pipe, writer exited) leaves rows delivered_at IS NULL
    // so the next drain will retry. This closes the D1 window where rows were
    // marked delivered after mpsc-enqueue but before the OS-pipe flush.
    if let Err(e) = bridge.send_system_message(system_render, None).await {
        warn!(
            conversation_id = bridge.conversation_id,
            error = %e,
            "failed to drain events — will retry on next wake"
        );
        return;
    }

    // Mark all push IDs (ingress + bus) delivered in a single critical section.
    // Runs only after a confirmed flush (send_system_message returned Ok),
    // so all rows in this batch are marked all-or-nothing: a flush failure
    // above leaves all of them parked for redelivery (correct at-least-once).
    {
        let conn = bridge.db.lock().await;
        let all_push_ids_to_mark: Vec<i64> = ingress_push_ids_to_mark
            .into_iter()
            .chain(messaging_push_ids)
            .collect();
        if !all_push_ids_to_mark.is_empty() {
            brenn_lib::messaging::db::mark_pending_pushes_delivered(&conn, &all_push_ids_to_mark);
        }
    }

    // Emit ToolUseSummary card for received messages (the dual-broadcast).
    if let Some(html) = messaging_card_html {
        emit_prerendered_summary(
            bridge,
            crate::tools::messaging::MCP_MESSAGE_RECEIVED_PSEUDO_TOOL,
            html,
        )
        .await;
    }
}

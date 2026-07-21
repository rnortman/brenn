//! Compaction-status lifecycle handlers: `handle_status_change` (StatusChange)
//! and `handle_compact_boundary` (CompactBoundary). Both concern the
//! compaction-status lifecycle and touch `CompactionPhase`.

use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::ws_types::{CcState, WsServerMessage};
use tracing::{debug, info, warn};

use super::super::bridge_io::persist_incoming_message;
use super::super::{ActiveBridge, CompactionPhase};

pub(super) async fn handle_status_change(
    bridge: &ActiveBridge,
    status: Option<&str>,
    compact_result: Option<&str>,
    alert_dispatcher: &AlertDispatcher,
) {
    match status {
        Some("compacting") => {
            info!(conversation_id = bridge.conversation_id, "CC compacting");
            bridge.broadcast(WsServerMessage::Status {
                state: CcState::Compacting,
            });
        }
        Some("requesting") => {
            // CC is making an API request (fires per-turn in CC 2.1.112+).
            // Strict subset of Thinking; `set_cc_busy` already broadcasts
            // Thinking on Idle→busy, so no state change needed here.
            debug!(
                conversation_id = bridge.conversation_id,
                "CC requesting (API call in flight)"
            );
        }
        None => {
            // Status cleared. When compact_result is present this is the
            // end-of-compaction signal — broadcast Idle immediately so the
            // UI doesn't stay stuck on "Compacting context..." waiting for
            // the subsequent TurnCompleted.
            if let Some(cr) = compact_result {
                // compact_result should only arrive while phase is Compacting.
                // Gate on the current phase so a duplicate/replayed message
                // outside a compaction cycle does not silently abort in-flight
                // PendingTurnCompletion or PersistingState flows.
                let phase_is_compacting = matches!(
                    bridge.compaction.lock().await.phase,
                    CompactionPhase::Compacting
                );
                if !phase_is_compacting {
                    warn!(
                        conversation_id = bridge.conversation_id,
                        compact_result = cr,
                        "CC sent compact_result on status:null while not Compacting — \
                         possible CC protocol drift; ignoring state mutation"
                    );
                    alert_dispatcher.alert_once_per_process(
                        brenn_lib::obs::alerting::AlertSeverity::Warning,
                        "Unexpected compact_result outside Compacting phase".into(),
                        cr,
                        format!(
                            "CC sent compact_result {:?} on status:null but bridge was not \
                             in Compacting phase. This likely indicates CC protocol drift.",
                            cr
                        ),
                    );
                } else {
                    match cr {
                        "success" => {
                            info!(
                                conversation_id = bridge.conversation_id,
                                "CC compaction succeeded — broadcasting Idle"
                            );
                            bridge.broadcast(WsServerMessage::Status {
                                state: CcState::Idle,
                            });
                            // Reset the state machine so TurnCompleted proceeds normally.
                            bridge.reset_compaction_state().await;
                        }
                        "failure" => {
                            warn!(
                                conversation_id = bridge.conversation_id,
                                "CC compaction failed — broadcasting Idle and surfacing failure card"
                            );
                            bridge.broadcast(WsServerMessage::Status {
                                state: CcState::Idle,
                            });
                            // Reset state machine so subsequent TurnCompleted doesn't
                            // stay stuck in StayCompacting forever.
                            bridge.reset_compaction_state().await;
                            let render = crate::system_message::render_compaction_failed();
                            bridge
                                .persist_and_broadcast_system_message(render, None)
                                .await;
                        }
                        other => {
                            // Unrecognized compact_result — treat as end-of-compaction
                            // defensively. Warn so we notice CC protocol evolution.
                            warn!(
                                conversation_id = bridge.conversation_id,
                                compact_result = other,
                                "CC status cleared with unrecognized compact_result — broadcasting Idle"
                            );
                            bridge.broadcast(WsServerMessage::Status {
                                state: CcState::Idle,
                            });
                            bridge.reset_compaction_state().await;
                        }
                    }
                }
            } else {
                // Non-compaction status clear (e.g. after `requesting`).
                // handle_turn_completed handles the Idle transition.
                debug!(
                    conversation_id = bridge.conversation_id,
                    "CC status cleared"
                );
            }
        }
        Some(other) => {
            // Unknown status string — CC may have added new statuses.
            // Surface it so we know to handle it. Dedup on the status
            // string so repeated identical statuses only page once per
            // process lifetime.
            warn!(
                conversation_id = bridge.conversation_id,
                status = other,
                "unknown CC status — possible CC upgrade"
            );
            alert_dispatcher.alert_once_per_process(
                brenn_lib::obs::alerting::AlertSeverity::Warning,
                "Unknown CC status".into(),
                other,
                format!(
                    "CC sent status {:?} which Brenn doesn't recognize. \
                     This likely means CC was upgraded.",
                    other
                ),
            );
        }
    }
}

pub(super) async fn handle_compact_boundary(
    bridge: &ActiveBridge,
    metadata: Option<&brenn_cc::protocol::incoming::CompactMetadata>,
) {
    info!(
        conversation_id = bridge.conversation_id,
        ?metadata,
        "compact boundary"
    );

    // Insert a compaction marker into the user-visible message history.
    let marker = serde_json::json!({
        "type": "compact_boundary",
        "metadata": metadata,
    });
    // Persist to the diagnostic log. CompactBoundary is not broadcast to the
    // frontend so the returned seq is not forwarded.
    let _compact_boundary_seq =
        persist_incoming_message(bridge, "compact_boundary", None, None, &marker).await;
}

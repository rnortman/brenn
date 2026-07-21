use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::ws_types::WsServerMessage;
use tracing::{info, warn};

use super::super::bridge_io::persist_incoming_message;
use super::super::compaction::update_context_from_assistant;
use super::super::{ActiveBridge, PendingToolUse};

pub(super) fn handle_stream_event(
    bridge: &ActiveBridge,
    evt: &brenn_cc::protocol::incoming::StreamEventMessage,
) {
    if let Some(delta) = evt.event.get("delta") {
        let delta_type = delta.get("type").and_then(|t| t.as_str());
        match delta_type {
            Some("text_delta") => {
                if let Some(text) = delta.get("text").and_then(|t| t.as_str()) {
                    bridge.broadcast(WsServerMessage::StreamToken {
                        token: text.to_string(),
                    });
                }
            }
            Some("thinking_delta") => {
                if let Some(thinking) = delta.get("thinking").and_then(|t| t.as_str()) {
                    bridge.broadcast(WsServerMessage::ThinkingToken {
                        token: thinking.to_string(),
                    });
                }
            }
            _ => {
                // Other delta types (e.g., input_json_delta for tool use) — ignore.
            }
        }
    }
}

/// Parse the `/context` response from a synthetic assistant message.
///
/// Expected format: `**Tokens:** 13.7k / 200k (7%)` or `**Tokens:** 67.2k / 1m (7%)`
/// The `k` suffix means "×1,000" — `13.7k` = 13,700 tokens.
/// The `m` suffix means "×1,000,000" — `1m` = 1,000,000 tokens.
/// Values without a suffix are used as-is (e.g., `500 / 200k`).
pub(in crate::active_bridge) async fn handle_assistant_message(
    bridge: &ActiveBridge,
    msg: &brenn_cc::protocol::incoming::AssistantMessage,
    alert_dispatcher: &AlertDispatcher,
) {
    // Alert on unknown content block types — CC probably added something new.
    use brenn_cc::protocol::incoming::ContentBlock;
    for block in &msg.message.content {
        if matches!(block, ContentBlock::Unknown) {
            warn!("unknown content block type in assistant message — possible CC upgrade");
            alert_dispatcher.alert(
                brenn_lib::obs::alerting::AlertSeverity::Warning,
                "Unknown CC content block type".into(),
                "CC sent an assistant message containing an unrecognized content block type. This likely means CC was upgraded and Brenn needs updating.".into(),
            );
            break; // one alert per message is enough
        }
    }

    // Persist the raw CC message before rendering — the DB stores source JSON,
    // rendering is a presentation concern applied at send-time. Capture db_seq
    // so the live broadcast carries it for frontend dedup against history replay.
    let db_seq = persist_incoming_message(
        bridge,
        "assistant",
        Some(&msg.uuid),
        msg.parent_tool_use_id.as_deref(),
        msg,
    )
    .await;

    // Register pending tool uses from tool_use content blocks. These are consumed
    // by emit_tool_result_summaries when the ToolResult arrives. Every tool use
    // the model invokes passes through here first.
    {
        use brenn_cc::protocol::incoming::ContentBlock;
        let mut pending = bridge.pending_tool_uses.lock().await;
        for block in &msg.message.content {
            if let ContentBlock::ToolUse { id, name, input } = block {
                pending.insert(
                    id.clone(),
                    PendingToolUse {
                        tool_name: name.clone(),
                        tool_input: input.clone(),
                    },
                );
            }
        }
    }

    // Update context fill from stream token counts. Only top-level messages
    // drive the user-facing context pill (subagents are skipped inside).
    // Returns Some(new_slug) on a genuine mid-session model switch — look up
    // the new slug in the cache and re-seed max_tokens accordingly.
    if let Some(new_slug) = update_context_from_assistant(bridge, msg, alert_dispatcher) {
        let conn = bridge.db.lock().await;
        let cached = brenn_lib::model_window_cache::get(&conn, &new_slug);
        drop(conn);
        match cached {
            Some((max, _ver, _updated)) => {
                *bridge.seed_max_tokens.lock().expect("seed_max_tokens lock") = Some(max);
            }
            None => {
                info!(
                    model = %new_slug,
                    "model slug changed mid-session and not in cache \
                     — context usage deferred until result frame"
                );
                // seed_max_tokens already nulled by update_context_from_assistant.
            }
        }
    }

    // Render content blocks to HTML (markdown + thinking details).
    let content = crate::markdown::render_content_blocks(&msg.message.content);

    // seq: Some(db_seq) lets the frontend deduplicate this live broadcast against a
    // concurrent history replay (reconnect-from-idle race fix).
    bridge.broadcast(WsServerMessage::AssistantMessage {
        content,
        seq: Some(db_seq),
    });
    // Thinking state is owned by `set_cc_busy`, not broadcast per message.
}

//! History replay: convert stored conversation messages to WsServerMessages.
//!
//! Reuses the same message types the frontend handles during live conversations.
//! No separate "history message" type — one code path for live and replayed messages.

use brenn_cc::protocol::incoming::ContentBlock;
use brenn_lib::conversation::{self, ConversationStatus, MessageDirection};
use brenn_lib::rusqlite::Connection;
use brenn_lib::ws_types::{
    ConversationListStatus, ConversationSummary, HistoryPageMessage, SystemMessageCategory,
    WsServerMessage,
};

/// Find the replay seam for bounded history replay.
///
/// Returns `Some(seam_seq)` if the conversation has more than `limit` replayable
/// messages — the seam is a compaction boundary (if one exists nearby) or a
/// hard cutoff. Returns `None` if the full history fits within the limit.
///
/// The seam guarantees at least `limit` messages of full-fidelity replay.
/// It snaps to the nearest `compact_boundary` at or before the cutoff seq,
/// giving a clean semantic break at a point where CC had already forgotten
/// everything before it.
pub fn find_replay_seam(conn: &Connection, conversation_id: i64, limit: usize) -> Option<i64> {
    let total = conversation::count_replayable_messages(conn, conversation_id);
    if total as usize <= limit {
        return None; // Full history fits — no seam needed.
    }

    // The cutoff seq is the last replayable message we want to EXCLUDE.
    // build_history replays `seq > seam_seq`, so the seam seq itself is excluded.
    // Position (total - limit - 1) is the last message before the replay window.
    let cutoff_offset = total as usize - limit - 1;
    let cutoff_seq = conversation::nth_replayable_seq(conn, conversation_id, cutoff_offset)
        .unwrap_or_else(|| {
            panic!(
                "cutoff offset {cutoff_offset} out of range: total={total} limit={limit} \
                 (invariant: total > limit implies offset is valid)"
            )
        });

    // Snap to a compaction boundary if one exists at or before the cutoff.
    // Boundaries are never replayable, so `seq > boundary` naturally excludes
    // the boundary itself — no off-by-one concern.
    let seam = conversation::latest_compact_boundary_before(conn, conversation_id, cutoff_seq)
        .unwrap_or(cutoff_seq);

    Some(seam)
}

/// Parse `system_category` from a JSON payload, panicking on data-integrity
/// violations (missing or unrecognized value when `rendered_html` is present).
///
/// Extracted to eliminate duplication between `build_history` and
/// `build_simplified_page`, which must enforce identical invariants.
fn parse_system_category(payload: &serde_json::Value, msg_id: i64) -> SystemMessageCategory {
    if payload["system_category"].is_null() {
        panic!(
            "message id {} has rendered_html but no system_category — \
             data integrity violation",
            msg_id
        )
    } else {
        serde_json::from_value(payload["system_category"].clone()).unwrap_or_else(|e| {
            panic!(
                "stored system_category {:?} on message id {} is not a \
                 recognized SystemMessageCategory variant: {e}",
                payload["system_category"], msg_id
            )
        })
    }
}

/// Build the history replay for a conversation as a Vec of WsServerMessages.
///
/// Only includes user, assistant, tool_summary, and artifact_display messages.
/// Control messages, stream events, rate limits, etc. are skipped — they're
/// transient protocol noise. Artifact (content) messages are cached but not
/// emitted directly — they back artifact_display messages.
///
/// `artifact_version_counts` maps file_path → total version count. Must be
/// pre-computed upfront because an `artifact_display` for v1 may appear in
/// the stream before the `artifact` message for v2. The caller computes this
/// from `get_artifact_index()` via `version_counts_from_index()`.
///
/// `from_seq` controls incremental replay: `None` replays everything,
/// `Some(n)` replays only messages with `seq > n`. All emitted messages
/// carry their DB `seq` for frontend tracking.
///
/// `seam_seq` is the lower bound for bounded history replay. When set,
/// only messages with `seq > seam_seq` are replayed. The effective lower
/// bound is `max(from_seq, seam_seq)`.
// ALLOW: each argument here is a distinct, unrelated piece of context
// pulled from a different place at the call site (DB conn, conversation
// metadata, app config, request range). Bundling them into a context
// struct would just push the same arg count into the struct ctor at the
// caller without consolidating anything.
#[allow(clippy::too_many_arguments)]
pub fn build_history(
    conn: &Connection,
    conversation_id: i64,
    cwd: Option<&str>,
    working_dir: &std::path::Path,
    slug: &str,
    mounts: &[crate::artifact::MountRoot],
    total_versions: &std::collections::HashMap<String, i32>,
    from_seq: Option<i64>,
    seam_seq: Option<i64>,
    frontmatter_cfg: &brenn_lib::config::FrontmatterRenderConfig,
) -> Vec<WsServerMessage> {
    use std::collections::HashMap;

    // Validate from_seq: if higher than the conversation's max seq, fall back
    // to full replay. Protects against stale seq from a different conversation.
    let from_seq = from_seq.and_then(|seq| {
        let max_seq = conversation::get_max_seq(conn, conversation_id);
        if seq > max_seq.unwrap_or(0) {
            None // Stale — fall back to full replay.
        } else {
            Some(seq)
        }
    });

    // Effective lower bound: the more recent of from_seq and seam_seq.
    // When from_seq < seam_seq, send_history (routes/ws/history.rs) detects the
    // gap and sends ConversationSwitched{reload:true} before the batch, so the
    // build_history function itself just uses max(from_seq, seam_seq) correctly.
    let effective_from = match (from_seq, seam_seq) {
        (Some(f), Some(s)) => Some(f.max(s)),
        (a, b) => a.or(b),
    };

    // Load only artifact-type messages for the cache. An artifact_display in
    // the post-seam range may reference an artifact from before the seam.
    let artifact_messages = conversation::get_artifact_messages(conn, conversation_id);

    // Pre-fetch all attachments for this conversation to avoid N+1 queries.
    let attachments_by_msg = conversation::get_attachments_for_conversation(conn, conversation_id);

    // Cache artifact content from the artifact messages.
    // Maps artifact message id → (file_path, content, version, seq).
    let mut artifact_cache: HashMap<i64, (String, String, i32, i64)> = HashMap::new();
    for msg in &artifact_messages {
        let payload: serde_json::Value =
            serde_json::from_str(&msg.payload).expect("stored message payload must be valid JSON");
        let file_path = payload["file_path"]
            .as_str()
            .expect("artifact must have file_path")
            .to_string();
        let content = payload["content"]
            .as_str()
            .expect("artifact must have content")
            .to_string();
        let version = payload["version"]
            .as_i64()
            .expect("artifact must have version") as i32;
        artifact_cache.insert(msg.id, (file_path, content, version, msg.seq));
    }

    // Select messages to emit: all (full replay) or from effective_from+1 (bounded).
    let messages = match effective_from {
        Some(seq) => conversation::get_messages_from(conn, conversation_id, seq + 1),
        None => conversation::get_messages(conn, conversation_id),
    };

    messages
        .iter()
        .filter_map(|msg| {
            // Panic on malformed payload — DB contains raw JSON we wrote.
            // If it's corrupt, that's a data integrity violation.
            let payload: serde_json::Value = serde_json::from_str(&msg.payload)
                .expect("stored message payload must be valid JSON");

            match msg.msg_type.as_str() {
                "user" if msg.direction == MessageDirection::Outgoing => {
                    // Dispatch to the correct wire variant based on whether
                    // `rendered_html` is present in the persisted payload.
                    // - Present → system row (written by `send_system_message`) →
                    //   emit `SystemMessageBroadcast`.
                    // - Absent → chat-input row or legacy `send_internal_user_message`
                    //   row → emit `UserMessageEcho` (plain bubble).
                    //
                    // Read `rendered_html` FIRST so that a system row whose
                    // `message.content` is missing is not silently dropped by the
                    // `?` filter that lives in the chat-row branch below.
                    //
                    // This is the existing DB-shape distinction since commit
                    // `1e8bb90`; no new persistence-level discriminator is added.
                    if let Some(rendered_html) = payload["rendered_html"].as_str() {
                        // System row: `system_category` must be present and valid.
                        // Panic on missing or unrecognized — data-integrity violation.
                        // Also panic if `rendered_html` is present but `system_category`
                        // is absent, which would mean we wrote a malformed row.
                        let category =
                            parse_system_category(&payload, msg.id);

                        // Build timestamp from `created_at`; system rows have no
                        // `sender_tz` so we pass None.
                        let timestamp =
                            build_history_timestamp(&msg.created_at, None);

                        Some(WsServerMessage::SystemMessageBroadcast {
                            rendered_html: rendered_html.to_string(),
                            category,
                            timestamp,
                            seq: Some(msg.seq),
                        })
                    } else {
                        // Chat-input row (or legacy `send_internal_user_message` row).
                        // Only replay rows whose payload carries a content string.
                        let text = payload["message"]["content"].as_str()?;

                        // Resolve username from sender_user_id.
                        let username = msg.sender_user_id
                            .and_then(|uid| conversation::get_username(conn, uid))
                            .unwrap_or_default();

                        // Build timestamp in sender's local timezone.
                        let timestamp =
                            build_history_timestamp(&msg.created_at, msg.sender_tz.as_deref());

                        // Include any attachments for this message.
                        let attachments = attachments_by_msg
                            .get(&msg.id)
                            .cloned()
                            .unwrap_or_default();

                        Some(WsServerMessage::UserMessageEcho {
                            text: text.to_string(),
                            username,
                            timestamp,
                            attachments,
                            // Selected tasks are ephemeral context (not persisted in DB).
                            // Replayed messages won't show task chips. See select-and-chat design.
                            selected_tasks: vec![],
                            seq: Some(msg.seq),
                        })
                    }
                }
                "assistant" => {
                    // Parse content blocks from raw JSON, render through
                    // the Phase 8 markdown pipeline.
                    let content_blocks = parse_content_blocks(&payload);
                    let rendered = crate::markdown::render_content_blocks(&content_blocks);
                    Some(WsServerMessage::AssistantMessage {
                        content: rendered,
                        seq: Some(msg.seq),
                    })
                }
                "tool_summary" => {
                    let tool_name = payload["tool_name"].as_str()?;
                    let rendered_summary = payload["rendered_summary"].as_str()?;
                    let detail_html = payload["detail_html"]
                        .as_str()
                        .map(|s| s.to_string());
                    Some(WsServerMessage::ToolUseSummary {
                        tool_name: tool_name.to_string(),
                        rendered_summary: rendered_summary.to_string(),
                        detail_html,
                        seq: Some(msg.seq),
                    })
                }
                "artifact" => {
                    // Already cached in the pre-pass above. Don't emit.
                    None
                }
                "artifact_display" => {
                    let artifact_message_id = payload["artifact_message_id"]
                        .as_i64()
                        .expect("artifact_display must have artifact_message_id");

                    let (file_path, content, version, artifact_seq) =
                        artifact_cache.get(&artifact_message_id).unwrap_or_else(|| {
                            panic!(
                                "artifact_display references artifact message {artifact_message_id} \
                                 which was not seen — data integrity violation"
                            )
                        });

                    let rendered_html = crate::frontmatter::render_markdown_with_frontmatter(
                        content,
                        frontmatter_cfg,
                    );
                    let file_total = total_versions.get(file_path.as_str()).copied().unwrap_or(0);

                    let stable_url = cwd.and_then(|cwd| {
                        crate::artifact::compute_stable_url(
                            file_path,
                            std::path::Path::new(cwd),
                            working_dir,
                            mounts,
                            slug,
                        )
                    });

                    Some(WsServerMessage::ArtifactContent {
                        file_path: file_path.clone(),
                        rendered_html,
                        raw_content: content.clone(),
                        snapshot: Some(brenn_lib::ws_types::SnapshotMetadata {
                            message_id: artifact_message_id,
                            version: *version,
                            total_versions: file_total,
                            seq: *artifact_seq,
                            stable_url,
                        }),
                        seq: Some(msg.seq),
                    })
                }
                "target_result" => {
                    let target = payload["target"].as_str()?.to_string();
                    let success = payload["success"].as_bool()?;
                    let summary = payload["summary"].as_str()?.to_string();
                    let detail = payload["detail"].as_str().map(|s| s.to_string());
                    let files: Vec<String> = payload["files"]
                        .as_array()?
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    Some(WsServerMessage::TargetResult {
                        target,
                        success,
                        summary,
                        detail,
                        files,
                        seq: Some(msg.seq),
                    })
                }
                // Approval history, tool results, rate limits, etc. — not replayed.
                _ => None,
            }
        })
        .collect()
}

/// Build an ISO 8601 timestamp from a stored UTC `created_at` and optional sender timezone.
///
/// If `sender_tz` is a valid IANA timezone, converts the UTC time to that timezone.
/// Otherwise, returns the UTC timestamp as-is.
fn build_history_timestamp(created_at: &str, sender_tz: Option<&str>) -> String {
    let Ok(utc) = chrono::DateTime::parse_from_rfc3339(created_at) else {
        return created_at.to_string();
    };
    let utc = utc.with_timezone(&chrono::Utc);

    if let Some(tz_str) = sender_tz
        && let Ok(tz) = tz_str.parse::<chrono_tz::Tz>()
    {
        return utc.with_timezone(&tz).to_rfc3339();
    }

    utc.to_rfc3339()
}

/// Page size for backward pagination.
const SIMPLIFIED_PAGE_SIZE: usize = 100;

/// Build a page of simplified messages for backward pagination.
///
/// Returns `(messages, has_more)`. Messages are in chronological order.
/// Only includes user text and assistant text — no tool summaries, artifacts,
/// or other side-effect-bearing message types.
///
/// Note: assistant messages with only `ToolUse` blocks (no text) are filtered
/// out. This means a page can return fewer than `page_size` messages even when
/// `has_more` is true. In extreme cases (many tool-only turns), a page could
/// be empty. The frontend handles this gracefully — the sentinel triggers
/// another page load on the next scroll.
pub fn build_simplified_page(
    conn: &Connection,
    conversation_id: i64,
    before_seq: i64,
) -> (Vec<HistoryPageMessage>, bool) {
    let rows =
        conversation::get_simplified_page(conn, conversation_id, before_seq, SIMPLIFIED_PAGE_SIZE);

    let has_more = rows.len() > SIMPLIFIED_PAGE_SIZE;
    let rows = if has_more {
        &rows[rows.len() - SIMPLIFIED_PAGE_SIZE..]
    } else {
        &rows
    };

    // Pre-fetch usernames to avoid N+1 queries (one per chat-user message).
    let mut username_cache: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    for msg in rows {
        if let Some(uid) = msg.sender_user_id {
            username_cache
                .entry(uid)
                .or_insert_with(|| conversation::get_username(conn, uid).unwrap_or_default());
        }
    }

    let message_ids: Vec<i64> = rows.iter().map(|m| m.id).collect();
    let attachments_by_msg = conversation::get_attachments_for_messages(conn, &message_ids);

    let messages = rows
        .iter()
        .filter_map(|msg| {
            let payload: serde_json::Value = serde_json::from_str(&msg.payload)
                .expect("stored message payload must be valid JSON");

            match msg.msg_type.as_str() {
                "user" if msg.direction == MessageDirection::Outgoing => {
                    // Check for system-origin row first (rendered_html present
                    // iff written by `send_system_message`).
                    if let Some(rendered_html) = payload["rendered_html"].as_str() {
                        // System row: `system_category` must be present and valid.
                        // Panic on missing or unrecognized — data-integrity violation.
                        let category = parse_system_category(&payload, msg.id);

                        let timestamp = build_history_timestamp(&msg.created_at, None);

                        Some(HistoryPageMessage {
                            seq: msg.seq,
                            role: "user".to_string(),
                            rendered_html: rendered_html.to_string(),
                            timestamp,
                            username: None,
                            category: Some(category),
                            attachments: vec![],
                        })
                    } else {
                        // Chat-input row (or pre-1e8bb906 legacy system row):
                        // fall through to the plain-bubble path.
                        let text = payload["message"]["content"].as_str()?;
                        let username = msg
                            .sender_user_id
                            .and_then(|uid| username_cache.get(&uid).cloned());
                        let timestamp =
                            build_history_timestamp(&msg.created_at, msg.sender_tz.as_deref());

                        // Escape user text for safe innerHTML.
                        let escaped = brenn_lib::util::html_escape(text);
                        let rendered_html = format!("<div class=\"msg-text\">{escaped}</div>");

                        let attachments =
                            attachments_by_msg.get(&msg.id).cloned().unwrap_or_default();

                        Some(HistoryPageMessage {
                            seq: msg.seq,
                            role: "user".to_string(),
                            rendered_html,
                            timestamp,
                            username,
                            category: None,
                            attachments,
                        })
                    }
                }
                "assistant" => {
                    let content_blocks = parse_content_blocks(&payload);
                    // Filter to only Text blocks — skip ToolUse blocks.
                    let text_blocks: Vec<_> = content_blocks
                        .into_iter()
                        .filter(|b| matches!(b, ContentBlock::Text { .. }))
                        .collect();
                    if text_blocks.is_empty() {
                        return None; // Tool-only turn — nothing to show.
                    }
                    let rendered = crate::markdown::render_content_blocks(&text_blocks);
                    let timestamp =
                        build_history_timestamp(&msg.created_at, msg.sender_tz.as_deref());

                    Some(HistoryPageMessage {
                        seq: msg.seq,
                        role: "assistant".to_string(),
                        rendered_html: rendered,
                        timestamp,
                        username: None,
                        category: None,
                        attachments: vec![],
                    })
                }
                _ => None,
            }
        })
        .collect();

    (messages, has_more)
}

/// Parse ContentBlock array from a stored assistant message payload.
///
/// The payload is the full serialized `AssistantMessage` from brenn-cc.
/// We extract `message.content` and deserialize each element as a ContentBlock.
fn parse_content_blocks(payload: &serde_json::Value) -> Vec<ContentBlock> {
    let content = match payload.get("message").and_then(|m| m.get("content")) {
        Some(c) => c,
        None => return Vec::new(),
    };

    // Panic on malformed content blocks — the DB contains CC output we stored.
    // If it's corrupt, that's a data integrity violation.
    serde_json::from_value(content.clone()).expect("stored assistant content blocks must be valid")
}

/// Build conversation summaries for the sidebar list, scoped to an app.
///
/// When `multiuser` is true, includes shared conversations from other users.
/// Uses pre-joined queries to avoid N+1 per-row lookups.
pub fn build_conversation_list(
    conn: &Connection,
    user_id: i64,
    app_slug: &str,
    multiuser: bool,
) -> Vec<ConversationSummary> {
    let rows = if multiuser {
        conversation::list_conversation_summaries_multiuser(conn, user_id, app_slug)
    } else {
        conversation::list_conversation_summaries(conn, user_id, app_slug)
    };
    rows.into_iter()
        .map(|row| {
            let owner = if row.user_id != user_id {
                row.owner_username
            } else {
                None
            };
            ConversationSummary {
                id: row.id,
                title: row.title,
                status: match row.status {
                    ConversationStatus::Active => ConversationListStatus::Active,
                    ConversationStatus::Completed => ConversationListStatus::Completed,
                    ConversationStatus::Error => ConversationListStatus::Error,
                },
                model: row.model,
                updated_at: row.updated_at,
                message_count: row.message_count,
                shared: row.shared,
                owner,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::auth::user::create_user;
    use brenn_lib::conversation::MessageDirection;
    use brenn_lib::db::init_db_memory;

    /// Test helper: build history with auto-computed version counts.
    fn test_build_history(
        conn: &brenn_lib::rusqlite::Connection,
        conv_id: i64,
    ) -> Vec<WsServerMessage> {
        let index = crate::artifact_snapshot::get_artifact_index(conn, conv_id);
        let counts = crate::artifact_snapshot::version_counts_from_index(&index);
        build_history(
            conn,
            conv_id,
            None,
            std::path::Path::new("."),
            "test",
            &[],
            &counts,
            None,
            None,
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        )
    }

    #[test]
    fn build_history_user_messages() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello world"}}"#,
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::UserMessageEcho { text, .. } => {
                assert_eq!(text, "hello world");
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    #[test]
    fn build_history_user_message_with_sender_attribution() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "alice", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Store a user message with sender info.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            Some(user_id),
            Some("Asia/Tokyo"),
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::UserMessageEcho {
                text,
                username,
                timestamp,
                ..
            } => {
                assert_eq!(text, "hello");
                assert_eq!(username, "alice");
                // Timestamp should be in Asia/Tokyo timezone.
                assert!(
                    timestamp.contains("+09:00"),
                    "expected JST offset in timestamp, got: {timestamp}"
                );
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    #[test]
    fn build_history_user_message_with_attachments() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "alice", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert a user message with attachments.
        let (msg_id, _seq) = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"check this"}}"#,
            Some(user_id),
            None,
            None,
        );

        // Insert attachment metadata.
        conversation::insert_attachments(
            &conn,
            &[conversation::StoredAttachment {
                upload_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                message_id: msg_id,
                filename: "receipt.jpg".to_string(),
                media_type: "image/jpeg".to_string(),
                size: 12345,
                disk_filename: "550e8400-e29b-41d4-a716-446655440000_receipt.jpg".to_string(),
            }],
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::UserMessageEcho {
                text, attachments, ..
            } => {
                assert_eq!(text, "check this");
                assert_eq!(attachments.len(), 1);
                assert_eq!(
                    attachments[0].upload_id,
                    "550e8400-e29b-41d4-a716-446655440000"
                );
                assert_eq!(attachments[0].filename, "receipt.jpg");
                assert_eq!(attachments[0].media_type, "image/jpeg");
                assert_eq!(attachments[0].size, 12345);
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    #[test]
    fn build_history_user_message_without_attachments_has_empty_vec() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "alice", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"no attachments"}}"#,
            Some(user_id),
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::UserMessageEcho { attachments, .. } => {
                assert!(attachments.is_empty());
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    #[test]
    fn build_history_user_message_without_sender_has_empty_username() {
        // Legacy messages (or messages without sender info) should have empty username.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "alice", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Store a user message WITHOUT sender info (sender_user_id = None).
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::UserMessageEcho { username, .. } => {
                assert!(
                    username.is_empty(),
                    "expected empty username, got: {username}"
                );
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    // System-message replay parity (system-message-collapse design)
    // ──────────────────────────────────────────────────────────────────────

    /// System rows (written by `send_system_message`) must replay as
    /// `SystemMessageBroadcast`, not `UserMessageEcho`. Renamed from
    /// `replay_system_message_carries_rendered_html_and_category`.
    #[test]
    fn replay_row_with_rendered_html_emits_system_broadcast() {
        // Hand-craft a row that mirrors what `send_system_message` writes:
        // payload contains rendered_html + system_category alongside the
        // standard {type, message} fields.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "owner", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": "Context is at 75%"},
            "rendered_html": "<details class=\"brenn-system\">\
                              <summary>Compaction reminder (context 75%)</summary>\
                              <div class=\"brenn-system-body\"><p>body</p></div>\
                              </details>",
            "system_category": "CompactionReminder",
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            &payload.to_string(),
            Some(user_id),
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::SystemMessageBroadcast {
                rendered_html,
                category,
                seq,
                ..
            } => {
                assert!(
                    rendered_html.contains("brenn-system"),
                    "rendered_html round-trips: {rendered_html:?}"
                );
                assert_eq!(
                    *category,
                    brenn_lib::ws_types::SystemMessageCategory::CompactionReminder,
                );
                // Replay rows must carry the DB seq for incremental reconnect.
                assert!(seq.is_some(), "replayed row carries seq");
            }
            other => panic!("expected SystemMessageBroadcast, got {other:?}"),
        }
    }

    /// Chat-input rows (no rendered_html) must replay as `UserMessageEcho`
    /// (plain bubble). This also covers legacy `send_internal_user_message`
    /// rows, which have the same DB shape — they render as plain bubbles per R6.
    #[test]
    fn replay_row_without_rendered_html_emits_user_message_echo() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "alice", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            Some(user_id),
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::UserMessageEcho { username, text, .. } => {
                // Flat-bubble path: the persisted user's name is replayed.
                assert_eq!(username, "alice");
                assert_eq!(text, "hello");
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    /// Cat-7 rows (attributed to a real human user, but with system_category set)
    /// must replay as `SystemMessageBroadcast` — the variant has no `username`
    /// field, so there is nothing to override and no risk of surfacing the
    /// requesting human's name as the system's voice.
    #[test]
    fn replay_system_row_attributed_to_human_emits_system_broadcast() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let owner_id = create_user(&conn, "owner", "$argon2id$fake");
        let other_id = create_user(&conn, "bob", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, owner_id, "test", false);

        let payload = serde_json::json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": "The user has requested compaction. ..."
            },
            "rendered_html": "<details class=\"brenn-system\"><summary>Compaction requested by user</summary></details>",
            "system_category": "CompactionUserRequest",
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            &payload.to_string(),
            Some(other_id), // Attributed to a non-owner human user.
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::SystemMessageBroadcast { category, .. } => {
                // SystemMessageBroadcast has no username field — attribution
                // in the DB is unaffected, but the wire never shows a name.
                assert_eq!(
                    *category,
                    brenn_lib::ws_types::SystemMessageCategory::CompactionUserRequest,
                );
            }
            other => panic!("expected SystemMessageBroadcast, got {other:?}"),
        }
    }

    /// A row with `rendered_html` present but `system_category` absent is a
    /// data-integrity violation and must panic.
    #[test]
    #[should_panic(expected = "has rendered_html but no system_category")]
    fn replay_panics_on_rendered_html_without_system_category() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "owner", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": "x"},
            "rendered_html": "<details></details>",
            // Deliberately omit system_category.
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            &payload.to_string(),
            Some(user_id),
            None,
            None,
        );

        let _ = test_build_history(&conn, conv_id);
    }

    #[test]
    #[should_panic(expected = "is not a recognized SystemMessageCategory variant")]
    fn replay_panics_on_unknown_system_category() {
        // Per CLAUDE.md "fail fast": an unrecognized system_category in the
        // DB is a data-integrity violation (we wrote it ourselves; a
        // string we can't deserialize means corruption or schema drift).
        // The replay path must panic, not silently downgrade to None.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "owner", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": "x"},
            "rendered_html": "<details></details>",
            "system_category": "GarbageVariant",
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            &payload.to_string(),
            Some(user_id),
            None,
            None,
        );

        // Should panic with the message we wrote in `build_messages`.
        let _ = test_build_history(&conn, conv_id);
    }

    /// `UiError` category round-trips through the DB → replay path without
    /// corruption. This pins the `serde` serialization contract for the new
    /// variant: if a future `#[serde(rename_all = ...)]` attribute changes
    /// the serialized string from `"UiError"` to something else, this test
    /// will catch it before a deploy.
    #[test]
    fn replay_ui_error_row_round_trips_category() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "owner", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Persist a literal `"UiError"` string — exactly as `serde_json::json!`
        // produces from a `SystemMessageCategory::UiError` unit variant today.
        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": "[System] User attempted: graf_todo_done(...)"},
            "rendered_html": "<details class=\"brenn-system brenn-system-ui-error\" open>\
                              <summary>UI tool error: graf_todo_done</summary>\
                              <div class=\"brenn-system-body\"><p>error detail</p></div>\
                              </details>",
            "system_category": "UiError",
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            &payload.to_string(),
            Some(user_id),
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::SystemMessageBroadcast {
                rendered_html,
                category,
                seq,
                ..
            } => {
                assert_eq!(
                    *category,
                    brenn_lib::ws_types::SystemMessageCategory::UiError,
                    "UiError category round-trips through serde serialization"
                );
                assert!(
                    rendered_html.contains("brenn-system-ui-error"),
                    "rendered_html round-trips: {rendered_html:?}"
                );
                assert!(seq.is_some(), "replayed row carries seq");
            }
            other => panic!("expected SystemMessageBroadcast, got {other:?}"),
        }
    }

    #[test]
    fn build_history_assistant_messages() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let payload = serde_json::json!({
            "uuid": "msg-1",
            "parent_tool_use_id": null,
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Hello!"}
                ]
            }
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            Some("msg-1"),
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::AssistantMessage { content, .. } => {
                assert!(content.contains("Hello!"), "should contain rendered text");
            }
            other => panic!("expected AssistantMessage, got {other:?}"),
        }
    }

    #[test]
    fn build_history_skips_control_messages() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // User message
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hi"}}"#,
            None,
            None,
            None,
        );
        // Control message (should be skipped)
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "control_request",
            None,
            None,
            r#"{"type":"control_request"}"#,
            None,
            None,
            None,
        );
        // Rate limit (should be skipped)
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "rate_limit_event",
            None,
            None,
            r#"{"type":"rate_limit_event"}"#,
            None,
            None,
            None,
        );
        // Assistant message
        let payload = serde_json::json!({
            "uuid": "msg-2",
            "parent_tool_use_id": null,
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "bye"}]
            }
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            Some("msg-2"),
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 2, "should only have user + assistant");
    }

    #[test]
    fn build_conversation_list_basic() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");

        let conv1 = conversation::create_conversation(&conn, user_id, "test", false);
        conversation::set_title(&conn, conv1, "First");
        let _ = conversation::append_message(
            &conn,
            conv1,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            "{}",
            None,
            None,
            None,
        );

        let conv2 = conversation::create_conversation(&conn, user_id, "test", false);
        conversation::set_title(&conn, conv2, "Second");
        conversation::complete_conversation(&conn, conv2, Some(0.05));

        let list = build_conversation_list(&conn, user_id, "test", false);
        assert_eq!(list.len(), 2);
        // Newest first.
        assert_eq!(list[0].id, conv2);
        assert_eq!(list[0].title.as_deref(), Some("Second"));
        assert_eq!(list[0].status, ConversationListStatus::Completed);
        assert_eq!(list[0].message_count, 0);

        assert_eq!(list[1].id, conv1);
        assert_eq!(list[1].title.as_deref(), Some("First"));
        assert_eq!(list[1].status, ConversationListStatus::Active);
        assert_eq!(list[1].message_count, 1);
    }

    /// Store an artifact via the real `store_artifact_snapshot` path.
    /// Returns the SnapshotResult.
    fn store_artifact(
        conn: &Connection,
        conv_id: i64,
        file_path: &str,
        content: &str,
    ) -> crate::artifact_snapshot::SnapshotResult {
        crate::artifact_snapshot::store_artifact_snapshot(conn, conv_id, file_path, content, "t1")
    }

    #[test]
    fn build_history_replays_artifact_display() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // User message, then artifact display, then assistant message.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"show file"}}"#,
            None,
            None,
            None,
        );
        store_artifact(&conn, conv_id, "docs/plan.md", "# Plan");
        let payload = serde_json::json!({
            "uuid": "msg-1",
            "parent_tool_use_id": null,
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "Done."}]
            }
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            Some("msg-1"),
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        // user, artifact_display (as ArtifactContent), assistant = 3 messages
        assert_eq!(history.len(), 3);

        // First is user echo.
        assert!(matches!(
            &history[0],
            WsServerMessage::UserMessageEcho { .. }
        ));

        // Second is artifact content.
        match &history[1] {
            WsServerMessage::ArtifactContent {
                file_path,
                rendered_html,
                snapshot,
                ..
            } => {
                assert_eq!(file_path, "docs/plan.md");
                assert!(rendered_html.contains("Plan"));
                let snap = snapshot.as_ref().expect("should have snapshot");
                assert_eq!(snap.version, 1);
                assert_eq!(snap.total_versions, 1);
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }

        // Third is assistant message.
        assert!(matches!(
            &history[2],
            WsServerMessage::AssistantMessage { .. }
        ));
    }

    #[test]
    fn build_history_replays_artifact_with_frontmatter() {
        // Regression: bulk-history replay must run stored artifact
        // content through `split_and_render_frontmatter` just like the
        // live DisplayFile and load-by-id paths. Without this, dropping
        // the wrapper inside `build_history`'s `artifact_display` arm
        // would silently re-introduce the run-on-paragraph bug for
        // history replays.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let content = "---\nstatus: in_progress\npriority: 2\n---\n# Plan\n";
        store_artifact(&conn, conv_id, "docs/plan.md", content);

        let history = test_build_history(&conn, conv_id);
        match history
            .iter()
            .find(|m| matches!(m, WsServerMessage::ArtifactContent { .. }))
            .expect("artifact content emitted")
        {
            WsServerMessage::ArtifactContent { rendered_html, .. } => {
                assert!(
                    rendered_html.contains("class=\"fm-block\""),
                    "frontmatter rendered: {rendered_html}"
                );
                assert!(
                    rendered_html.contains("<h1>Plan</h1>"),
                    "body still rendered: {rendered_html}"
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn build_history_replays_mount_artifact_with_mount_stable_url() {
        // Regression: artifact with a mount-slug-prefixed display path must
        // come back on history replay with the mount-form stable URL. Pins
        // the mount_roots threading through `build_history`.
        let db = init_db_memory();
        let cwd = tempfile::tempdir().unwrap();
        let mount = tempfile::tempdir().unwrap();
        std::fs::write(mount.path().join("plan.md"), "# Plan").unwrap();

        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        store_artifact(&conn, conv_id, "life/plan.md", "# Plan");

        let mounts = vec![crate::artifact::MountRoot {
            host_path: mount.path().to_path_buf(),
            slug: "life".into(),
        }];
        let index = crate::artifact_snapshot::get_artifact_index(&conn, conv_id);
        let counts = crate::artifact_snapshot::version_counts_from_index(&index);
        let history = build_history(
            &conn,
            conv_id,
            Some(cwd.path().to_str().unwrap()),
            cwd.path(),
            "test",
            &mounts,
            &counts,
            None,
            None,
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        );

        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::ArtifactContent {
                file_path,
                snapshot,
                ..
            } => {
                assert_eq!(file_path, "life/plan.md");
                let snap = snapshot.as_ref().expect("snapshot metadata");
                assert_eq!(
                    snap.stable_url.as_deref(),
                    Some("/app/test/mount/life/file/plan.md"),
                );
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }
    }

    #[test]
    fn build_history_dedup_multiple_displays_same_artifact() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Same content twice — dedup produces 1 artifact + 2 displays.
        store_artifact(&conn, conv_id, "f.md", "# F");
        store_artifact(&conn, conv_id, "f.md", "# F");

        let history = test_build_history(&conn, conv_id);
        // Both displays produce ArtifactContent.
        assert_eq!(history.len(), 2);
        for msg in &history {
            assert!(matches!(msg, WsServerMessage::ArtifactContent { .. }));
        }
    }

    #[test]
    fn build_history_total_versions_correct_across_versions() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Two different versions of the same file.
        // Message order: artifact_v1, display_v1, artifact_v2, display_v2.
        // The display_v1 appears BEFORE artifact_v2 in the stream,
        // so incremental counting would give total_versions=1 there.
        // The upfront query correctly gives 2 for both.
        store_artifact(&conn, conv_id, "f.md", "v1");
        store_artifact(&conn, conv_id, "f.md", "v2");

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 2);

        // Both displays should show total_versions = 2.
        for msg in &history {
            match msg {
                WsServerMessage::ArtifactContent { snapshot, .. } => {
                    let snap = snapshot.as_ref().unwrap();
                    assert_eq!(snap.total_versions, 2);
                }
                other => panic!("expected ArtifactContent, got {other:?}"),
            }
        }
    }

    #[test]
    fn build_history_orphan_artifact_silently_skipped() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Artifact with no display — manually inserted since
        // store_artifact_snapshot always creates a display too.
        // This is a pathological case (shouldn't happen in production),
        // but we verify it doesn't crash or emit anything.
        let payload = serde_json::json!({
            "file_path": "f.md",
            "content": "# Orphan",
            "content_hash": "sha256:orphan",
            "version": 1,
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "artifact",
            None,
            Some("t1"),
            &payload.to_string(),
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(
            history.len(),
            0,
            "orphan artifact should not produce output"
        );
    }

    #[test]
    #[should_panic(expected = "data integrity violation")]
    fn build_history_panics_on_dangling_artifact_display() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert an artifact_display referencing a nonexistent artifact.
        let display_payload = serde_json::json!({
            "file_path": "f.md",
            "artifact_message_id": 99999,
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "artifact_display",
            None,
            Some("t1"),
            &display_payload.to_string(),
            None,
            None,
            None,
        );

        // This should panic.
        test_build_history(&conn, conv_id);
    }

    #[test]
    fn build_conversation_list_user_isolation() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user1 = create_user(&conn, "alice", "$argon2id$fake");
        let user2 = create_user(&conn, "bob", "$argon2id$fake");

        conversation::create_conversation(&conn, user1, "test", false);
        conversation::create_conversation(&conn, user2, "test", false);

        let list1 = build_conversation_list(&conn, user1, "test", false);
        let list2 = build_conversation_list(&conn, user2, "test", false);
        assert_eq!(list1.len(), 1);
        assert_eq!(list2.len(), 1);
        assert_ne!(list1[0].id, list2[0].id);
    }

    // -----------------------------------------------------------------------
    // build_history_timestamp
    // -----------------------------------------------------------------------

    #[test]
    fn history_timestamp_with_timezone() {
        // UTC time, sender was in Asia/Tokyo (+09:00).
        let result = build_history_timestamp("2026-03-26T05:32:00+00:00", Some("Asia/Tokyo"));
        assert!(
            result.contains("+09:00"),
            "expected JST offset, got: {result}"
        );
        assert!(result.starts_with("2026-03-26T14:32:00"));
    }

    #[test]
    fn history_timestamp_without_timezone() {
        let result = build_history_timestamp("2026-03-26T05:32:00+00:00", None);
        assert!(result.contains("+00:00"), "expected UTC, got: {result}");
    }

    #[test]
    fn history_timestamp_invalid_timezone_falls_back() {
        let result = build_history_timestamp("2026-03-26T05:32:00+00:00", Some("Invalid/Zone"));
        assert!(
            result.contains("+00:00"),
            "expected UTC fallback, got: {result}"
        );
    }

    #[test]
    fn history_timestamp_invalid_datetime_returns_as_is() {
        let result = build_history_timestamp("not-a-date", Some("Asia/Tokyo"));
        assert_eq!(result, "not-a-date");
    }

    // -----------------------------------------------------------------------
    // build_conversation_list with multiuser
    // -----------------------------------------------------------------------

    #[test]
    fn conversation_list_multiuser_includes_shared() {
        let db = brenn_lib::db::init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake");
        let bob = create_user(&conn, "bob", "$argon2id$fake");

        // Alice: shared conv, Bob: shared conv
        conversation::create_conversation(&conn, alice, "test", true);
        conversation::create_conversation(&conn, bob, "test", true);

        // Bob sees both with multiuser=true.
        let list = build_conversation_list(&conn, bob, "test", true);
        assert_eq!(list.len(), 2);

        // One should have owner set (Alice's conv).
        let owned_by_alice: Vec<_> = list.iter().filter(|c| c.owner.is_some()).collect();
        assert_eq!(owned_by_alice.len(), 1);
        assert_eq!(owned_by_alice[0].owner.as_deref(), Some("alice"));
    }

    #[test]
    fn conversation_list_non_multiuser_excludes_others() {
        let db = brenn_lib::db::init_db_memory();
        let conn = db.blocking_lock();
        let alice = create_user(&conn, "alice", "$argon2id$fake");
        let bob = create_user(&conn, "bob", "$argon2id$fake");

        conversation::create_conversation(&conn, alice, "test", true);
        conversation::create_conversation(&conn, bob, "test", true);

        // With multiuser=false, Bob only sees his own.
        let list = build_conversation_list(&conn, bob, "test", false);
        assert_eq!(list.len(), 1);
        assert!(list[0].owner.is_none());
    }

    // -----------------------------------------------------------------------
    // Incremental replay (from_seq)
    // -----------------------------------------------------------------------

    /// Test helper: build history with a from_seq cutoff.
    fn test_build_history_from(
        conn: &brenn_lib::rusqlite::Connection,
        conv_id: i64,
        from_seq: i64,
    ) -> Vec<WsServerMessage> {
        let index = crate::artifact_snapshot::get_artifact_index(conn, conv_id);
        let counts = crate::artifact_snapshot::version_counts_from_index(&index);
        build_history(
            conn,
            conv_id,
            None,
            std::path::Path::new("."),
            "test",
            &[],
            &counts,
            Some(from_seq),
            None,
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        )
    }

    #[test]
    fn incremental_replay_returns_only_new_messages() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Message 1: user
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            None,
            None,
            None,
        );
        // Message 2: assistant
        let payload = serde_json::json!({
            "uuid": "msg-1",
            "parent_tool_use_id": null,
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "Hi there!"}]
            }
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            Some("msg-1"),
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );
        // Message 3: user (the one after disconnect)
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"new message"}}"#,
            None,
            None,
            None,
        );

        // Full replay returns all 3.
        let full = test_build_history(&conn, conv_id);
        assert_eq!(full.len(), 3);

        // Get the seq of message 2 (assistant).
        let seq_2 = match &full[1] {
            WsServerMessage::AssistantMessage { seq, .. } => seq.expect("should have seq"),
            other => panic!("expected AssistantMessage, got {other:?}"),
        };

        // Incremental from seq_2 should return only message 3.
        let incremental = test_build_history_from(&conn, conv_id, seq_2);
        assert_eq!(incremental.len(), 1);
        match &incremental[0] {
            WsServerMessage::UserMessageEcho { text, seq, .. } => {
                assert_eq!(text, "new message");
                assert!(seq.is_some(), "incremental messages should carry seq");
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    #[test]
    fn incremental_replay_nothing_new_returns_empty() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            None,
            None,
            None,
        );

        let full = test_build_history(&conn, conv_id);
        let last_seq = match &full[0] {
            WsServerMessage::UserMessageEcho { seq, .. } => seq.expect("should have seq"),
            other => panic!("expected UserMessageEcho, got {other:?}"),
        };

        // Nothing new since last_seq — should return empty.
        let incremental = test_build_history_from(&conn, conv_id, last_seq);
        assert!(
            incremental.is_empty(),
            "should return no messages when nothing is new"
        );
    }

    #[test]
    fn incremental_replay_stale_seq_falls_back_to_full() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            None,
            None,
            None,
        );

        // Stale seq (way beyond max) — should fall back to full replay.
        let history = test_build_history_from(&conn, conv_id, 99999);
        assert_eq!(
            history.len(),
            1,
            "stale seq should fall back to full replay"
        );
    }

    #[test]
    fn incremental_replay_artifact_cache_spans_cutoff() {
        // The hard case: the artifact content row (type="artifact") is BEFORE
        // the from_seq cutoff, but the artifact_display row that references it
        // is AFTER the cutoff. The display won't render unless build_history
        // pre-caches all artifact rows from the full conversation.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // store_artifact creates artifact (seq=0) + artifact_display (seq=1).
        store_artifact(&conn, conv_id, "docs/plan.md", "# Plan");

        // Set cutoff between artifact and artifact_display.
        // artifact is at seq=0, display is at seq=1.
        // from_seq=0 means "give me seq > 0" → only the display row.
        // The display references the artifact row, which is NOT in the
        // incremental query result — it must come from the pre-cache.
        let incremental = test_build_history_from(&conn, conv_id, 0);
        assert_eq!(incremental.len(), 1);
        match &incremental[0] {
            WsServerMessage::ArtifactContent {
                file_path,
                rendered_html,
                ..
            } => {
                assert_eq!(file_path, "docs/plan.md");
                assert!(rendered_html.contains("Plan"));
            }
            other => panic!("expected ArtifactContent, got {other:?}"),
        }
    }

    #[test]
    fn full_replay_messages_carry_seq() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::UserMessageEcho { seq, .. } => {
                assert!(seq.is_some(), "full replay messages should carry seq");
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    #[test]
    fn build_history_skips_incoming_user_messages() {
        // Incoming "user" messages are CC internal (e.g., set_model acks, tool
        // results). They should NOT appear in the replay as UserMessageEcho.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Outgoing user message (human-sent) — should appear.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            None,
            None,
            None,
        );

        // Incoming user message (CC set_model ack) — should be filtered out.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "user",
            None,
            None,
            r#"{"message":{"role":"user","content":"<local-command-stdout>Set model to sonnet</local-command-stdout>"}}"#,
            None,
            None,
            None,
        );

        // Another incoming user message (tool result) — should be filtered out.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "user",
            None,
            None,
            r#"{"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}}"#,
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(
            history.len(),
            1,
            "should only replay the outgoing user message, not incoming CC messages"
        );
        match &history[0] {
            WsServerMessage::UserMessageEcho { text, .. } => {
                assert_eq!(text, "hello");
            }
            other => panic!("expected UserMessageEcho, got {other:?}"),
        }
    }

    #[test]
    fn build_history_replays_target_result() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let payload = serde_json::json!({
            "target": "import",
            "success": true,
            "summary": "Imported 5 transactions",
            "detail": "stdout:\n{\"count\":5}\nstderr:\n",
            "files": ["bank.ofx"],
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "target_result",
            None,
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1, "should replay the target_result message");
        match &history[0] {
            WsServerMessage::TargetResult {
                target,
                success,
                summary,
                detail,
                files,
                seq,
            } => {
                assert_eq!(target, "import");
                assert!(success);
                assert_eq!(summary, "Imported 5 transactions");
                assert!(detail.as_ref().unwrap().contains("count"));
                assert_eq!(files, &["bank.ofx"]);
                assert!(seq.is_some(), "replayed message should have seq");
            }
            other => panic!("expected TargetResult, got {other:?}"),
        }
    }

    #[test]
    fn build_history_target_result_without_detail() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // No detail field (null in JSON).
        let payload = serde_json::json!({
            "target": "import",
            "success": false,
            "summary": "Command failed",
            "detail": null,
            "files": [],
        });
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "target_result",
            None,
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        assert_eq!(history.len(), 1);
        match &history[0] {
            WsServerMessage::TargetResult {
                success, detail, ..
            } => {
                assert!(!success);
                assert!(detail.is_none());
            }
            other => panic!("expected TargetResult, got {other:?}"),
        }
    }

    // --- find_replay_seam tests ---

    /// Helper: insert N user messages and return the conversation id.
    fn create_conv_with_messages(conn: &Connection, user_id: i64, count: usize) -> i64 {
        let conv_id = conversation::create_conversation(conn, user_id, "test", false);
        for i in 0..count {
            insert_user_outgoing(
                conn,
                conv_id,
                &format!(r#"{{"type":"user","message":{{"role":"user","content":"msg {i}"}}}}"#),
                None,
            );
        }
        conv_id
    }

    /// Helper: insert a compact_boundary marker.
    fn insert_compact_boundary(conn: &Connection, conv_id: i64) {
        let _ = conversation::append_message(
            conn,
            conv_id,
            MessageDirection::Incoming,
            "compact_boundary",
            None,
            None,
            r#"{"type":"compact_boundary","metadata":{"trigger":"test"}}"#,
            None,
            None,
            None,
        );
    }

    #[test]
    fn find_replay_seam_no_seam_when_under_limit() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = create_conv_with_messages(&conn, user_id, 10);

        // 10 messages, limit 100 → no seam needed.
        assert_eq!(find_replay_seam(&conn, conv_id, 100), None);
    }

    #[test]
    fn find_replay_seam_hard_cut_when_no_boundary() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = create_conv_with_messages(&conn, user_id, 20);

        // 20 messages, limit 5, no compact boundary → hard cut.
        let seam = find_replay_seam(&conn, conv_id, 5);
        assert!(seam.is_some(), "should have a seam");
        // Post-seam messages should be >= 5.
        let seam_seq = seam.unwrap();
        let post_seam = conversation::get_messages_from(&conn, conv_id, seam_seq + 1);
        let replayable: Vec<_> = post_seam
            .iter()
            .filter(|m| m.msg_type == "user" && m.direction == MessageDirection::Outgoing)
            .collect();
        assert!(
            replayable.len() >= 5,
            "expected at least 5 replayable messages after seam, got {}",
            replayable.len()
        );
    }

    #[test]
    fn find_replay_seam_snaps_to_compact_boundary() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert 10 messages, then a compact boundary, then 10 more.
        for i in 0..10 {
            let _ = conversation::append_message(
                &conn,
                conv_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                &format!(r#"{{"type":"user","message":{{"role":"user","content":"before {i}"}}}}"#),
                None,
                None,
                None,
            );
        }
        insert_compact_boundary(&conn, conv_id);
        for i in 0..10 {
            let _ = conversation::append_message(
                &conn,
                conv_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                &format!(r#"{{"type":"user","message":{{"role":"user","content":"after {i}"}}}}"#),
                None,
                None,
                None,
            );
        }

        // 20 replayable messages, limit 5. Cutoff is 15th replayable from start.
        // The compact boundary at seq 10 is before seq 15, so seam snaps to it.
        let seam = find_replay_seam(&conn, conv_id, 5);
        assert!(seam.is_some());
        let seam_seq = seam.unwrap();

        // The seam should be the compact_boundary seq.
        let boundary_msg = conversation::get_messages(&conn, conv_id)
            .into_iter()
            .find(|m| m.msg_type == "compact_boundary")
            .expect("should have a compact_boundary");
        assert_eq!(
            seam_seq, boundary_msg.seq,
            "seam should snap to compact_boundary"
        );
    }

    // --- build_simplified_page tests ---

    #[test]
    fn build_simplified_page_returns_user_and_assistant() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert user message.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            Some(user_id),
            None,
            None,
        );

        // Insert assistant message with text.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            None,
            None,
            r#"{"message":{"content":[{"type":"text","text":"world"}]}}"#,
            None,
            None,
            None,
        );

        // Insert tool_summary (should NOT appear in simplified page).
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "tool_summary",
            None,
            None,
            r#"{"tool_name":"Read","rendered_summary":"read foo","detail_html":null}"#,
            None,
            None,
            None,
        );

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, has_more) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert!(!has_more);
        assert_eq!(
            page.len(),
            2,
            "should have user + assistant, not tool_summary"
        );
        assert_eq!(page[0].role, "user");
        assert_eq!(page[1].role, "assistant");
        // User message should be HTML-escaped.
        assert!(page[0].rendered_html.contains("hello"));
        // Assistant message should be rendered markdown.
        assert!(page[1].rendered_html.contains("world"));
    }

    #[test]
    fn build_simplified_page_has_more_and_correct_slice() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert more than SIMPLIFIED_PAGE_SIZE messages.
        for i in 0..(SIMPLIFIED_PAGE_SIZE + 5) {
            let _ = conversation::append_message(
                &conn,
                conv_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                &format!(r#"{{"type":"user","message":{{"role":"user","content":"msg {i}"}}}}"#),
                None,
                None,
                None,
            );
        }

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, has_more) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert!(has_more, "should have more pages");
        assert_eq!(page.len(), SIMPLIFIED_PAGE_SIZE);

        // The page should contain the NEWEST messages (closest to before_seq),
        // not the oldest. Verify the last message has the highest seq.
        let last_seq = page.last().unwrap().seq;
        let first_seq = page.first().unwrap().seq;
        assert!(
            last_seq > first_seq,
            "page should be in ascending seq order"
        );

        // The last message in the page should be the one just before max_seq.
        // (We have max_seq + 5 messages; the page is the newest 100.)
        assert_eq!(last_seq, max_seq);
    }

    #[test]
    fn build_simplified_page_skips_tool_only_assistant() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert an assistant message with only a ToolUse block (no text).
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            None,
            None,
            r#"{"message":{"content":[{"type":"tool_use","id":"tu_1","name":"Read","input":{"path":"foo"}}]}}"#,
            None,
            None,
            None,
        );

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page.len(), 0, "tool-only assistant should be filtered out");
    }

    #[test]
    fn build_simplified_page_cursor_pagination() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert 5 user messages.
        for i in 0..5 {
            let _ = conversation::append_message(
                &conn,
                conv_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                &format!(r#"{{"type":"user","message":{{"role":"user","content":"msg {i}"}}}}"#),
                None,
                None,
                None,
            );
        }

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();

        // First page: get all 5 with before_seq = max+1.
        let (page1, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page1.len(), 5);

        // Second page: use the first message's seq as cursor.
        let cursor = page1[0].seq;
        let (page2, has_more) = build_simplified_page(&conn, conv_id, cursor);
        assert_eq!(page2.len(), 0, "no messages before the first one");
        assert!(!has_more);

        // Page from the middle: cursor = seq of msg 3 → should get msgs 0, 1, 2.
        let mid_cursor = page1[3].seq;
        let (page3, _) = build_simplified_page(&conn, conv_id, mid_cursor);
        assert_eq!(page3.len(), 3);
        assert!(
            page3.last().unwrap().seq < mid_cursor,
            "all messages should be before the cursor"
        );
    }

    #[test]
    fn build_simplified_page_escapes_html_in_user_text() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"<script>alert('xss')</script>"}}"#,
            None,
            None,
            None,
        );

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page.len(), 1);
        assert!(
            !page[0].rendered_html.contains("<script>"),
            "script tag should be escaped: {}",
            page[0].rendered_html
        );
        assert!(page[0].rendered_html.contains("&lt;script&gt;"));
    }

    // --- history-simplified-attachments: new tests ---

    #[test]
    fn build_simplified_page_includes_attachments() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let (msg_id, _) = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"see attached"}}"#,
            Some(user_id),
            None,
            None,
        );

        conversation::insert_attachments(
            &conn,
            &[
                conversation::StoredAttachment {
                    upload_id: "test-upload-id-001".to_string(),
                    message_id: msg_id,
                    filename: "photo.png".to_string(),
                    media_type: "image/png".to_string(),
                    size: 2048,
                    disk_filename: "test-upload-id-001_photo.png".to_string(),
                },
                conversation::StoredAttachment {
                    upload_id: "test-upload-id-002".to_string(),
                    message_id: msg_id,
                    filename: "doc.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    size: 4096,
                    disk_filename: "test-upload-id-002_doc.pdf".to_string(),
                },
            ],
        );

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page.len(), 1);
        assert_eq!(
            page[0].attachments.len(),
            2,
            "user message should carry all its attachments"
        );
        // Verify both attachments are present (order not guaranteed by DB).
        let upload_ids: Vec<&str> = page[0]
            .attachments
            .iter()
            .map(|a| a.upload_id.as_str())
            .collect();
        assert!(
            upload_ids.contains(&"test-upload-id-001"),
            "first attachment missing"
        );
        assert!(
            upload_ids.contains(&"test-upload-id-002"),
            "second attachment missing"
        );
        let att1 = page[0]
            .attachments
            .iter()
            .find(|a| a.upload_id == "test-upload-id-001")
            .unwrap();
        assert_eq!(att1.filename, "photo.png");
        assert_eq!(att1.media_type, "image/png");
        assert_eq!(att1.size, 2048);
    }

    #[test]
    fn build_simplified_page_no_attachments_empty_vec() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"no files"}}"#,
            Some(user_id),
            None,
            None,
        );

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page.len(), 1);
        assert!(
            page[0].attachments.is_empty(),
            "user message with no attachments should have empty vec"
        );
    }

    #[test]
    fn build_simplified_page_attachments_not_on_assistant() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert a user message with an attachment.
        let (user_msg_id, _) = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"user msg"}}"#,
            Some(user_id),
            None,
            None,
        );

        conversation::insert_attachments(
            &conn,
            &[conversation::StoredAttachment {
                upload_id: "test-upload-id-002".to_string(),
                message_id: user_msg_id,
                filename: "doc.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size: 5000,
                disk_filename: "test-upload-id-002_doc.pdf".to_string(),
            }],
        );

        // Insert an assistant message.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            None,
            None,
            r#"{"message":{"content":[{"type":"text","text":"response"}]}}"#,
            None,
            None,
            None,
        );

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page.len(), 2);

        let assistant_msg = page.iter().find(|m| m.role == "assistant").unwrap();
        assert!(
            assistant_msg.attachments.is_empty(),
            "assistant message must have empty attachments"
        );

        let user_msg = page.iter().find(|m| m.role == "user").unwrap();
        assert_eq!(
            user_msg.attachments.len(),
            1,
            "user message should carry its attachment"
        );
    }

    // --- system-msg-backfill-gap: new tests ---

    /// Helper: insert one outgoing user message with the given payload.
    fn insert_user_outgoing(
        conn: &Connection,
        conv_id: i64,
        payload: &str,
        sender_user_id: Option<i64>,
    ) -> (i64, i64) {
        conversation::append_message(
            conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            payload,
            sender_user_id,
            None,
            None,
        )
    }

    /// System-origin rows must emit a `HistoryPageMessage` with
    /// `category: Some(...)` and verbatim `rendered_html` (no re-wrapping).
    #[test]
    fn simplified_page_emits_system_row_with_category() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "owner", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let system_html = "<details class=\"brenn-system brenn-system-compaction-reminder\">\
                           <summary>Compaction reminder (context 75%)</summary>\
                           <div class=\"brenn-system-body\"><p>body</p></div>\
                           </details>";

        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": "Context is at 75%"},
            "rendered_html": system_html,
            "system_category": "CompactionReminder",
        });
        insert_user_outgoing(&conn, conv_id, &payload.to_string(), Some(user_id));

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page.len(), 1);
        let msg = &page[0];
        // category must be present and correct.
        assert_eq!(
            msg.category,
            Some(brenn_lib::ws_types::SystemMessageCategory::CompactionReminder),
        );
        // rendered_html is verbatim — no wrapping, no escaping.
        assert_eq!(
            msg.rendered_html, system_html,
            "rendered_html must be the persisted block byte-for-byte"
        );
        // role is preserved as "user" (DB row type, not load-bearing but documented).
        assert_eq!(msg.role, "user");
        // username is None — system rows carry no user attribution.
        assert_eq!(
            msg.username, None,
            "system rows must not leak the sender's username"
        );
        // System-origin rows must never carry attachments.
        assert!(
            msg.attachments.is_empty(),
            "system-origin rows must have empty attachments"
        );
    }

    /// A row with `rendered_html` present but `system_category` absent is a
    /// data-integrity violation in the simplified-page path too — must panic.
    #[test]
    #[should_panic(expected = "has rendered_html but no system_category")]
    fn simplified_page_panics_on_rendered_html_without_system_category() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "owner", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": "x"},
            "rendered_html": "<details></details>",
            // Deliberately omit system_category.
        });
        insert_user_outgoing(&conn, conv_id, &payload.to_string(), Some(user_id));

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let _ = build_simplified_page(&conn, conv_id, max_seq + 1);
    }

    /// An unrecognized `system_category` value in the simplified-page path
    /// is a data-integrity violation — must panic.
    #[test]
    #[should_panic(expected = "is not a recognized SystemMessageCategory variant")]
    fn simplified_page_panics_on_unknown_system_category() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "owner", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": "x"},
            "rendered_html": "<details></details>",
            "system_category": "GarbageVariant",
        });
        insert_user_outgoing(&conn, conv_id, &payload.to_string(), Some(user_id));

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let _ = build_simplified_page(&conn, conv_id, max_seq + 1);
    }

    /// Plain chat-input rows emit `category: None` with HTML-escaped bubble.
    #[test]
    fn simplified_page_chat_user_row_unchanged() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "alice", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        insert_user_outgoing(
            &conn,
            conv_id,
            r#"{"type":"user","message":{"role":"user","content":"hello world"}}"#,
            Some(user_id),
        );

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page.len(), 1);
        let msg = &page[0];
        assert_eq!(msg.category, None);
        assert_eq!(msg.role, "user");
        assert_eq!(
            msg.rendered_html,
            "<div class=\"msg-text\">hello world</div>"
        );
    }

    /// Pre-1e8bb906 legacy system rows (no `rendered_html`/`system_category`)
    /// degrade to a plain user bubble — unchanged from today, same as
    /// `build_history` degrade behavior.
    #[test]
    fn simplified_page_legacy_pre_1e8bb906_system_row_degrades() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "owner", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Old-shape payload: only {type, message}, no rendered_html or system_category.
        insert_user_outgoing(
            &conn,
            conv_id,
            r#"{"type":"user","message":{"role":"user","content":"Context is at 75%"}}"#,
            None,
        );

        let max_seq = conversation::get_max_seq(&conn, conv_id).unwrap();
        let (page, _) = build_simplified_page(&conn, conv_id, max_seq + 1);
        assert_eq!(page.len(), 1);
        let msg = &page[0];
        // Degrades to plain bubble — same as chat user row.
        assert_eq!(msg.category, None);
        assert_eq!(msg.role, "user");
        assert_eq!(
            msg.rendered_html,
            "<div class=\"msg-text\">Context is at 75%</div>"
        );
    }

    #[test]
    fn find_replay_seam_hard_cut_gives_exact_limit() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = create_conv_with_messages(&conn, user_id, 20);

        let seam = find_replay_seam(&conn, conv_id, 5).unwrap();

        // build_history with this seam should produce exactly 5 messages.
        let index = crate::artifact_snapshot::get_artifact_index(&conn, conv_id);
        let counts = crate::artifact_snapshot::version_counts_from_index(&index);
        let history = build_history(
            &conn,
            conv_id,
            None,
            std::path::Path::new("."),
            "test",
            &[],
            &counts,
            None,
            Some(seam),
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        );
        assert_eq!(
            history.len(),
            5,
            "seam should give exactly `limit` messages in the replay"
        );
    }

    #[test]
    fn build_history_with_seam_limits_output() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = create_conv_with_messages(&conn, user_id, 50);

        // Full history should have 50 messages.
        let full = test_build_history(&conn, conv_id);
        assert_eq!(full.len(), 50);

        // With a seam at the 10th message's seq, we should get only the
        // messages after that seq.
        let all_msgs = conversation::get_messages(&conn, conv_id);
        let seam_seq = all_msgs[9].seq; // 10th message (0-indexed)

        let index = crate::artifact_snapshot::get_artifact_index(&conn, conv_id);
        let counts = crate::artifact_snapshot::version_counts_from_index(&index);
        let bounded = build_history(
            &conn,
            conv_id,
            None,
            std::path::Path::new("."),
            "test",
            &[],
            &counts,
            None,
            Some(seam_seq),
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        );
        assert_eq!(bounded.len(), 40, "should replay only messages after seam");
    }

    // --- REPLAYABLE_FILTER sync test ---

    #[test]
    fn replayable_filter_matches_build_history_types() {
        // Verify that count_replayable_messages counts exactly the same
        // message types that build_history emits.
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert one of each replayed type.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            Some(user_id),
            None,
            None,
        );
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "assistant",
            None,
            None,
            r#"{"message":{"content":[{"type":"text","text":"hi"}]}}"#,
            None,
            None,
            None,
        );
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "tool_summary",
            None,
            None,
            r#"{"tool_name":"Read","rendered_summary":"read foo","detail_html":null}"#,
            None,
            None,
            None,
        );
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "target_result",
            None,
            None,
            r#"{"target":"import","success":true,"summary":"ok","detail":null,"files":[]}"#,
            None,
            None,
            None,
        );
        // Insert non-replayed types.
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "result",
            None,
            None,
            r#"{"type":"result"}"#,
            None,
            None,
            None,
        );
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "compact_boundary",
            None,
            None,
            r#"{"type":"compact_boundary"}"#,
            None,
            None,
            None,
        );
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Incoming,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"tool result"}}"#,
            None,
            None,
            None,
        );

        let history = test_build_history(&conn, conv_id);
        let replayable_count = conversation::count_replayable_messages(&conn, conv_id) as usize;

        assert_eq!(
            history.len(),
            replayable_count,
            "REPLAYABLE_FILTER count ({replayable_count}) must match \
             build_history output count ({})",
            history.len(),
        );
    }

    /// Integration test for the wake-spawn drain race (design §Part B, B.3).
    ///
    /// Simulates: bridge spawns → drain fires and persists a system-message row
    /// → a WS handler attaches and calls `build_history`. Asserts that the
    /// system card appears in the history output, proving the live broadcast
    /// being missed is not data loss — the persisted row delivers via history.
    ///
    /// With `seq: Some(n)` on the live broadcast (B.2) and the frontend dedup
    /// rule (B.3), double-rendering is also prevented when the live broadcast
    /// does reach an already-attached tab.
    #[test]
    fn wake_spawn_drain_visible_via_history_on_attach() {
        use brenn_lib::ws_types::SystemMessageCategory;

        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Simulate the drain firing before any WS tab is attached: insert the
        // system-message row directly (bypassing the broadcast that would be
        // dropped on an empty broadcast_tx). This is exactly what the race
        // looks like in production — the row is in the DB, the broadcast went
        // nowhere because no receiver existed yet.
        let rendered =
            crate::system_message::render_event_drain(&[brenn_lib::messaging::IngressEvent {
                id: 1,
                conversation_id: conv_id,
                source: "cron:test".to_string(),
                summary: "test event".to_string(),
                payload: r#"{"x":1}"#.to_string(),
                created_at: chrono::Utc::now(),
            }])
            .expect("non-empty events must produce a render");

        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": rendered.text},
            "rendered_html": rendered.rendered_html,
            "system_category": rendered.category,
        });
        let (_id, inserted_seq) = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            &payload.to_string(),
            Some(user_id),
            None,
            None,
        );

        // Now the WS handler attaches and calls build_history (from_seq=None).
        let history = test_build_history(&conn, conv_id);

        // The system card must be present — this is the fix for the bug.
        let system_msgs: Vec<_> = history
            .iter()
            .filter(|m| matches!(m, WsServerMessage::SystemMessageBroadcast { .. }))
            .collect();
        assert_eq!(
            system_msgs.len(),
            1,
            "system card must appear via history on attach; got history: {history:?}"
        );
        match &system_msgs[0] {
            WsServerMessage::SystemMessageBroadcast {
                category,
                seq,
                rendered_html,
                ..
            } => {
                assert_eq!(*category, SystemMessageCategory::EventDrain);
                assert_eq!(
                    *seq,
                    Some(inserted_seq),
                    "history-replayed seq must match the DB row's seq"
                );
                assert!(
                    rendered_html.contains("brenn-system-event-drain"),
                    "rendered_html must carry event-drain class"
                );
            }
            _ => unreachable!(),
        }
    }

    /// Pins the existing `build_history` graceful fallback: a stale `from_seq`
    /// greater than the conversation's max seq falls back to full replay rather
    /// than panicking or returning an empty slice.
    ///
    /// This behaviour is intentional (see design §B.5): a tab reconnecting
    /// after a dev DB reset sends a `?seq` that is in the future relative to
    /// the fresh DB. Full replay is the right recovery.
    #[test]
    fn stale_from_seq_falls_back_to_full_replay() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let user_id = create_user(&conn, "test", "$argon2id$fake");
        let conv_id = conversation::create_conversation(&conn, user_id, "test", false);

        // Insert one message (seq = 0).
        let _ = conversation::append_message(
            &conn,
            conv_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            Some(user_id),
            None,
            None,
        );

        // Build history with a stale from_seq (999 > 0, the max seq).
        let index = crate::artifact_snapshot::get_artifact_index(&conn, conv_id);
        let counts = crate::artifact_snapshot::version_counts_from_index(&index);
        let history_stale = build_history(
            &conn,
            conv_id,
            None,
            std::path::Path::new("."),
            "test",
            &[],
            &counts,
            Some(999), // stale seq — future relative to DB
            None,
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        );

        // Full replay from from_seq=None produces same result.
        let history_full = build_history(
            &conn,
            conv_id,
            None,
            std::path::Path::new("."),
            "test",
            &[],
            &counts,
            None,
            None,
            &brenn_lib::config::FrontmatterRenderConfig::default(),
        );

        assert_eq!(
            history_stale.len(),
            history_full.len(),
            "stale from_seq must produce full replay (same length as no from_seq)"
        );
        assert!(
            !history_stale.is_empty(),
            "full replay must include the one persisted message"
        );
    }
}

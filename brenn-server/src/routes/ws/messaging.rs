//! `resolve_bridge`, `handle_send_message`, `handle_stop_request`,
//! `handle_request_compaction`, `persist_and_send`, `truncate_title`.

use std::sync::Arc;

use brenn_lib::conversation::{self, ConversationStatus};
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::{CcState, DebugViewportSnapshotData, WsServerMessage};
use tracing::{error, info, warn};

use super::connection::WsConnection;
use crate::active_bridge::ActiveBridge;

/// User-facing error message sent when a message is persisted but CC delivery fails.
/// Used as a typed constant so tests can match without brittle substring coupling.
pub(super) const CC_SEND_FAILURE_MSG: &str =
    "Message was saved but failed to reach the assistant. Please try sending again.";

// impl WsConnection — message send, stop, compaction, persist-and-send
impl WsConnection {
    /// Handle SendMessage: spawn/lookup bridge and send the message.
    ///
    /// Returns `true` if the message was dispatched to CC, `false` on any
    /// rejection path (viewer-only, validation failure, SSRF, bridge error).
    pub(super) async fn handle_send_message(
        &mut self,
        text: &str,
        attachments: Vec<brenn_lib::ws_types::AttachmentRef>,
        model: Option<&str>,
        selected_tasks: Vec<brenn_lib::ws_types::SelectedTask>,
    ) -> bool {
        if self.viewer_only {
            let _ = self.send_ws(WsServerMessage::AppBusy {
                message: "This app has an active session from another user. \
                          You can force-close it to start your own."
                    .to_string(),
            });
            return false;
        }

        // Validate model against cached available models (if we have them).
        if let Some(m) = model {
            let models = self.state.cached_models.read().await;
            let is_valid = models.get(&self.app_slug).is_none_or(|app_models| {
                app_models.is_empty() || app_models.iter().any(|mi| mi.value == m)
            });
            drop(models);
            if !is_valid {
                log_and_alert_security_event(
                    &self.state.alert_dispatcher,
                    SecurityEventType::SchemaViolation,
                    self.client_ip,
                    &format!("user {} sent invalid model {:?}", self.user_id, m),
                );
                let _ = self.send_ws(WsServerMessage::Error {
                    message: format!("Invalid model: {m}"),
                });
                return false;
            }
        }

        // Validate selected_tasks.
        if selected_tasks.len() > 20 {
            log_and_alert_security_event(
                &self.state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                self.client_ip,
                &format!(
                    "user {} sent {} selected_tasks (max 20)",
                    self.user_id,
                    selected_tasks.len()
                ),
            );
            let _ = self.send_ws(WsServerMessage::Error {
                message: "Too many selected tasks (max 20)".to_string(),
            });
            return false;
        }
        for task in &selected_tasks {
            if task.task_ref.len() > 256
                || !task.task_ref.ends_with(".md")
                || task.task_ref.split('/').any(|seg| seg == "..")
            {
                log_and_alert_security_event(
                    &self.state.alert_dispatcher,
                    SecurityEventType::SchemaViolation,
                    self.client_ip,
                    &format!(
                        "user {} sent invalid selected task (ref={:?})",
                        self.user_id, task.task_ref,
                    ),
                );
                let _ = self.send_ws(WsServerMessage::Error {
                    message: format!("Invalid task reference: {}", task.task_ref),
                });
                return false;
            }
        }

        let Some(bridge) = self.resolve_bridge(Some(text)).await else {
            return false;
        };
        if let Some(m) = model
            && let Err(e) = bridge.set_model(m).await
        {
            warn!(
                conversation_id = self.current_conversation_id,
                model = m,
                error = %e,
                "set_model failed (continuing anyway)"
            );
        }
        self.persist_and_send(bridge, text, &attachments, &selected_tasks)
            .await
    }

    /// Resolve (or create) the bridge for the current WS connection.
    ///
    /// Three cases, in order:
    /// 1. **Active**: a live bridge exists for `current_conversation_id` → return it immediately.
    /// 2. **Resume**: a dead bridge + DB-resident conversation → validate ownership, reactivate,
    ///    wake CC, attach this WS connection, send `ConversationSwitched` + sidebar refresh.
    /// 3. **Create**: no current conversation → single-instance check, create a new conversation,
    ///    wake CC, attach, send `ConversationSwitched` + `HistoryComplete` + `ArtifactIndex` +
    ///    sidebar refresh.
    ///
    /// Returns `None` only when an error has already been sent to the client.
    ///
    /// `title_text`: pass `None` when no user message text is available.
    pub(super) async fn resolve_bridge(
        &mut self,
        title_text: Option<&str>,
    ) -> Option<Arc<ActiveBridge>> {
        let multiuser = self.app_config().multiuser;

        // --- Case 1: active bridge ---
        if let Some(conv_id) = self.current_conversation_id {
            if let Some(bridge) = self.state.active_bridges.get(conv_id).await {
                return Some(bridge);
            }

            // --- Case 2: resume dead bridge ---
            let conv = {
                let conn = self.state.db.lock().await;
                conversation::get_conversation_opt(&conn, conv_id)
            };
            if let Some(conv) = conv {
                if conv.app_slug != self.app_slug {
                    // Wrong app — don't allow cross-app access
                    // (would use wrong AppConfig for CC spawning).
                    let _ = self.send_ws(WsServerMessage::Error {
                        message: "Conversation not found".to_string(),
                    });
                    return None;
                }
                if !conversation::can_access_conversation(self.user_id, &conv, multiuser) {
                    log_and_alert_security_event(
                        &self.state.alert_dispatcher,
                        SecurityEventType::SchemaViolation,
                        self.client_ip,
                        &format!(
                            "user {} tried to access conversation {} owned by user {}",
                            self.user_id, conv_id, conv.user_id
                        ),
                    );
                    let _ = self.send_ws(WsServerMessage::Error {
                        message: "Not authorized".to_string(),
                    });
                    return None;
                }
                // Reactivate and/or auto-title in a single lock scope.
                let needs_reactivate = conv.status != ConversationStatus::Active;
                let needs_title = title_text.is_some() && conv.title.is_none();
                if needs_reactivate || needs_title {
                    let conn = self.state.db.lock().await;
                    if needs_reactivate {
                        conversation::reactivate_conversation(&conn, conv_id);
                    }
                    if needs_title {
                        conversation::set_title(
                            &conn,
                            conv_id,
                            &truncate_title(title_text.unwrap(), 80),
                        );
                    }
                }

                // Single-instance enforcement.
                if self.check_single_instance_blocked().await {
                    return None;
                }

                // Show "Starting assistant..." while we wait for CC to spawn.
                // If the eager spawn already completed, wake_conversation returns
                // instantly and the user barely sees this.
                let _ = self.send_ws(WsServerMessage::Status {
                    state: CcState::Connecting,
                });
                // Use wake_with_conv to pass the already-loaded conv row and
                // skip the redundant DB fetch inside wake_conversation.
                let bridge = match self.state.wake_with_conv(&conv, self.timezone).await {
                    Ok(b) => b,
                    Err(e) => {
                        error!("failed to resume CC session: {e}");
                        let _ = self.send_ws(WsServerMessage::Error {
                            message: format!("Failed to start CC: {e}"),
                        });
                        let _ = self.send_ws(WsServerMessage::Status {
                            state: CcState::Error,
                        });
                        return None;
                    }
                };
                self.attach_to_bridge(&bridge).await;
                let _ = self.send_ws(self.conversation_switched(Some(&conv), CcState::Thinking));
                self.send_conversation_list().await;
                return Some(bridge);
            }
        }

        // --- Case 3: create new conversation ---
        // Single-instance enforcement.
        if self.check_single_instance_blocked().await {
            return None;
        }
        let shared = multiuser;
        let conv_id = {
            let conn = self.state.db.lock().await;
            let cid =
                conversation::create_conversation(&conn, self.user_id, &self.app_slug, shared);
            if let Some(t) = title_text {
                conversation::set_title(&conn, cid, &truncate_title(t, 80));
            }
            cid
        };
        self.current_conversation_id = Some(conv_id);

        // Show "Starting assistant..." while we wait for CC to spawn.
        let _ = self.send_ws(WsServerMessage::Status {
            state: CcState::Connecting,
        });
        match self.state.wake_conversation(conv_id, self.timezone).await {
            Ok(bridge) => {
                self.attach_to_bridge(&bridge).await;
                // We just created this conversation — we're the owner.
                let _ = self.send_ws(WsServerMessage::ConversationSwitched {
                    conversation_id: Some(conv_id),
                    state: CcState::Thinking,
                    is_owner: true,
                    shared,
                    reload: false,
                });
                // No history for a new conversation — signal completion immediately.
                let _ = self.send_ws(WsServerMessage::HistoryComplete {
                    oldest_loaded_seq: None,
                });
                let _ = self.send_ws(WsServerMessage::ArtifactIndex { files: vec![] });
                // Refresh sidebar to show the new conversation.
                self.send_conversation_list().await;
                Some(bridge)
            }
            Err(e) => {
                error!("failed to spawn CC session: {e}");
                let _ = self.send_ws(WsServerMessage::Error {
                    message: format!("Failed to start CC: {e}"),
                });
                let _ = self.send_ws(WsServerMessage::Status {
                    state: CcState::Error,
                });
                None
            }
        }
    }

    /// Handle StopRequest: interrupt the CC subprocess for the current conversation.
    pub(super) async fn handle_stop_request(&mut self) {
        if let Some(conv_id) = self.current_conversation_id
            && let Some(bridge) = self.state.active_bridges.get(conv_id).await
            && let Err(e) = bridge.interrupt().await
        {
            warn!("interrupt failed: {e}");
        }
    }

    /// Handle user-initiated compaction request (click-to-compact UI).
    ///
    /// Sends a message to CC asking it to persist state and call RequestCompaction.
    /// Uses the normal message flow (persist to DB, echo to browsers, send to CC).
    ///
    /// Note: there's a benign TOCTOU between `can_start_compaction()` and the
    /// actual `send_message` inside `persist_and_send`. If the phase changes in
    /// between (e.g., a hard trigger fires), the message gets queued and delivered
    /// after compaction — slightly confusing UX but harmless.
    pub(super) async fn handle_request_compaction(&mut self) {
        let Some(conv_id) = self.current_conversation_id else {
            return;
        };
        let Some(bridge) = self.state.active_bridges.get(conv_id).await else {
            let _ = self.send_ws(WsServerMessage::Error {
                message: "No active session".to_string(),
            });
            return;
        };

        // Check compaction state — reject if already in progress.
        if !bridge.can_start_compaction().await {
            let _ = self.send_ws(WsServerMessage::Error {
                message: "Compaction already in progress".to_string(),
            });
            return;
        }

        let app_config = self.app_config();
        let local_now = chrono::Utc::now().with_timezone(&self.effective_timezone().await);
        let device_slug_owned = self.fetch_device_slug().await;
        let rendered = crate::system_message::render_user_compaction_request(
            &self.username,
            Some(&device_slug_owned),
            &local_now,
            app_config.prefix_username,
            app_config.prefix_timestamp,
            app_config.prefix_device,
        );
        // attribute_to_user_id = Some(self.user_id) keeps the DB row attributed
        // to the requesting human (for multi-user joins), while the broadcast
        // username is "[system]".
        if let Err(e) = bridge
            .send_system_message(rendered, Some(self.user_id))
            .await
        {
            let _ = self.send_ws(WsServerMessage::Error {
                message: format!("Failed to send compaction request: {e}"),
            });
        }
    }

    /// Persist user message and send to CC.
    ///
    /// Two distinct text paths:
    /// - **DB + echo:** Raw text + sender metadata (username, timestamp, user_id).
    /// - **CC:** Optionally prefixed with `[username YYYY-MM-DD HH:MM TZ]` based on app config,
    ///   with attachment notification lines appended.
    ///
    /// Returns `true` if the message was sent to CC, `false` if aborted (e.g. attachment
    /// resolve failure). Callers use the return value to gate `record_usage`.
    pub(super) async fn persist_and_send(
        &mut self,
        bridge: Arc<ActiveBridge>,
        text: &str,
        attachment_refs: &[brenn_lib::ws_types::AttachmentRef],
        selected_tasks: &[brenn_lib::ws_types::SelectedTask],
    ) -> bool {
        // Resolve attachments from the pending uploads registry.
        let app_config = self.app_config();
        let resolved = match crate::routes::upload::resolve_attachments(
            attachment_refs,
            &self.app_slug,
            self.user_id,
            &app_config.working_dir,
            &self.state.pending_uploads,
        )
        .await
        {
            Ok(r) => r,
            Err(reason) => {
                // Could be a bad UUID (fail2ban) or just an expired upload (user error).
                // Log as schema violation — the fail2ban threshold handles frequency.
                brenn_lib::obs::security::log_and_alert_security_event(
                    &self.state.alert_dispatcher,
                    brenn_lib::obs::security::SecurityEventType::SchemaViolation,
                    self.client_ip,
                    &format!("SendMessage attachment resolve failed: {reason}"),
                );
                let _ = self.send_ws(WsServerMessage::Error {
                    message: format!("Attachment error: {reason}"),
                });
                return false;
            }
        };

        let now = chrono::Utc::now();

        // Pre-compute echo metadata and CC text before persist consumes resolved.
        let attachment_metas: Vec<brenn_lib::ws_types::AttachmentMeta> =
            resolved.iter().map(|r| r.to_meta()).collect();

        // Load device + device_user in one lock scope; also check reminder trigger.
        // `local_now` is computed here from the device_user row so that the
        // tz_override is honoured — one lock, one load (design §2.2).
        let (device_slug_owned, slug_reminder, prefix_device, local_now) = {
            let conn = self.state.db.lock().await;
            let device = brenn_lib::auth::device::load_device(&conn, self.device_id);
            let du = brenn_lib::auth::device::load_device_user(&conn, self.device_id, self.user_id);
            let slug = du.display_slug(&device).to_string();
            let pc = self.app_config().prefix_device;
            let eff_tz = brenn_lib::auth::device::effective_timezone(&du, self.timezone, now);
            let local_now = now.with_timezone(&eff_tz);

            // Unassigned-slug reminder: fire when assigned_slug is null and the
            // 24h rate-limit window has passed (or slug_prompted_at is null).
            let reminder_needed = du.assigned_slug.is_none() && {
                match du.slug_prompted_at {
                    None => true,
                    Some(last) => now.signed_duration_since(last).num_hours() >= 24,
                }
            };
            let reminder = if reminder_needed {
                brenn_lib::auth::device::touch_slug_prompted_at(
                    &conn,
                    self.device_id,
                    self.user_id,
                );
                tracing::debug!(device_id = self.device_id, "device slug reminder injected");
                Some(crate::system_message::render_device_slug_reminder(
                    &device.guessed_slug,
                    device.platform.as_deref(),
                    device.screen_width,
                    device.screen_height,
                    device.user_agent.as_deref(),
                ))
            } else {
                None
            };
            (slug, reminder, pc, local_now)
        };
        let timestamp = local_now.to_rfc3339();

        let cc_text = {
            let base = crate::cc_message_prefix::build_cc_message_text(
                text,
                &self.username,
                Some(&device_slug_owned),
                &local_now,
                app_config.prefix_username,
                app_config.prefix_timestamp,
                prefix_device,
            );
            if resolved.is_empty() {
                base
            } else {
                let attachment_lines: Vec<String> =
                    resolved.iter().map(|r| r.cc_notification()).collect();
                format!("{}\n{}", base, attachment_lines.join("\n"))
            }
        };

        // Persist raw text + attachment metadata in a single DB lock scope.
        // Capture db_seq so the UserMessageEcho broadcast can carry it for
        // frontend deduplication against history replay.
        let (_msg_id, echo_db_seq) = bridge
            .persist_user_message_with_attachments(
                text,
                self.user_id,
                Some(self.timezone_str()),
                Some(self.device_id),
                |msg_id| {
                    resolved
                        .into_iter()
                        .map(|r| brenn_lib::conversation::StoredAttachment {
                            upload_id: r.upload_id.to_string(),
                            message_id: msg_id,
                            filename: r.filename,
                            media_type: r.media_type,
                            size: r.size,
                            disk_filename: r.disk_filename,
                        })
                        .collect()
                },
            )
            .await;

        // Broadcast echo with attribution, attachments, and selected tasks.
        // seq: Some(echo_db_seq) lets the frontend deduplicate this live broadcast
        // against a concurrent history replay (reconnect-from-idle race fix).
        bridge.broadcast_user_echo(WsServerMessage::UserMessageEcho {
            text: text.to_string(),
            username: self.username.clone(),
            timestamp: timestamp.clone(),
            attachments: attachment_metas,
            selected_tasks: selected_tasks.to_vec(),
            seq: Some(echo_db_seq),
        });

        // Messaging budget reset: a user chat message is the load-bearing
        // signal that bounds runaway agent-to-agent loops. See design §7.7.
        // Tool approvals, event injection, and idle hooks do NOT reset.
        // `AppConfig::messaging_send_budget()` honors per-app override →
        // global `[messaging].default_send_budget` → 100.
        if self.state.messenger.is_some() {
            let budget = self.app_config().messaging_send_budget();
            let conn = self.state.db.lock().await;
            brenn_lib::messaging::db::reset_send_budget(&conn, bridge.conversation_id, budget);
        }

        // Inject unassigned-slug reminder if needed: persist+broadcast to DB/UI now,
        // then include the reminder text as an extra block in the CC send below so
        // that the reminder and user message arrive in the same NDJSON envelope.
        // This closes the partial-failure window (CC never sees a dangling reminder
        // without its accompanying user message).
        let slug_reminder_text = if let Some(reminder) = slug_reminder {
            let text = reminder.text.clone();
            bridge
                .persist_and_broadcast_system_message(reminder, Some(self.user_id))
                .await;
            Some(text)
        } else {
            None
        };

        // Build the CC message: plain text, multi-block with context, and/or reminder.
        // Collect extra blocks: reminder (if any) + selected-task context (if any).
        let mut extra_blocks: Vec<String> = Vec::new();
        if let Some(ref reminder_text) = slug_reminder_text {
            extra_blocks.push(reminder_text.clone());
        }
        if !selected_tasks.is_empty() {
            // Build compact JSON context block for selected tasks.
            // SelectedTask already has #[serde(rename = "ref")] so serializes correctly.
            #[derive(serde::Serialize)]
            struct ContextBlock<'a> {
                context: &'static str,
                tasks: &'a [brenn_lib::ws_types::SelectedTask],
            }
            let context_block = serde_json::to_string(&ContextBlock {
                context: "selected_tasks",
                tasks: selected_tasks,
            })
            .expect("selected_tasks context serialization cannot fail");
            extra_blocks.push(context_block);
        }

        let cc_send_err: Option<String> = if extra_blocks.is_empty() {
            bridge.send_message(&cc_text).await.err()
        } else {
            let msg = brenn_cc::protocol::user_message_with_context(&cc_text, &extra_blocks);
            bridge.send_outgoing(msg).await.err()
        };
        if let Some(e) = cc_send_err {
            warn!(
                conversation_id = self.current_conversation_id,
                user_id = self.user_id,
                app_slug = %self.app_slug,
                error = %e,
                "CC send failed after message persisted"
            );
            let send_result = self.send_ws(WsServerMessage::Error {
                message: CC_SEND_FAILURE_MSG.to_string(),
            });
            if send_result != super::connection::SendResult::Ok {
                warn!(
                    conversation_id = self.current_conversation_id,
                    user_id = self.user_id,
                    app_slug = %self.app_slug,
                    cc_error = %e,
                    ws_send_result = ?send_result,
                    "CC send failed and WS error frame could not be delivered (double failure)"
                );
            }
        }
        true
    }
}

/// Truncate text to a title. Tries to break at a word boundary.
/// Safe for multi-byte UTF-8 — uses char boundaries, not byte offsets.
fn truncate_title(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    // Count characters, not bytes.
    let char_count = text.chars().count();
    if char_count <= max_chars {
        return text.to_string();
    }
    // Find the byte offset of the max_chars-th character.
    let byte_end = text
        .char_indices()
        .nth(max_chars)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    let truncated = &text[..byte_end];
    // Try to break at a word boundary (space) in the second half.
    let half_byte = text
        .char_indices()
        .nth(max_chars / 2)
        .map(|(i, _)| i)
        .unwrap_or(0);
    if let Some(space_pos) = truncated.rfind(' ')
        && space_pos > half_byte
    {
        return format!("{}…", &text[..space_pos]);
    }
    format!("{}…", truncated)
}

// impl WsConnection — debug snapshot handler
impl WsConnection {
    /// Handle `WsClientMessage::DebugViewportSnapshot`.
    ///
    /// Steps (in order):
    /// 1. Write a structured INFO log with a greppable tag (`debug_viewport_snapshot`)
    ///    containing full user/conversation context and the serialized payload.
    ///    This log succeeds unconditionally — the bridge resolution below is best-effort.
    /// 2. Inject the snapshot into the user's current CC conversation via the
    ///    system-message dual-delivery path (persisted + broadcast as a neutral
    ///    collapsed card, sent to CC as a `<brenn-debug-snapshot>`-tagged host message).
    ///
    /// This is a diagnostic, not a usage-billable user action — no `record_usage` or
    /// `EventType`. A well-formed message never panics the connection (AC7).
    pub(super) async fn handle_debug_viewport_snapshot(
        &mut self,
        mut snapshot: Box<DebugViewportSnapshotData>,
    ) {
        // Truncate all string fields before logging to bound log size.
        // A hostile or buggy client can send multi-MB strings; truncation keeps
        // them from bloating the INFO log or (via render_debug_snapshot) the CC
        // context. Truncation (not rejection) preserves AC8 — a well-formed
        // message is not a security event. 512 chars is generous for any
        // legitimate UA/style/ID value.
        const STR_MAX: usize = 512;
        fn trunc(s: &mut String) {
            if s.len() > STR_MAX {
                s.truncate(STR_MAX);
                s.push_str("…[truncated]");
            }
        }
        fn trunc_opt(s: &mut Option<String>) {
            if let Some(v) = s {
                trunc(v);
            }
        }
        fn trunc_vec(v: &mut Option<Vec<String>>) {
            if let Some(vec) = v {
                vec.truncate(16); // cap element count
                for s in vec.iter_mut() {
                    trunc(s);
                }
            }
        }
        trunc(&mut snapshot.user_agent);
        trunc_vec(&mut snapshot.ua_brands);
        trunc_opt(&mut snapshot.active_element_tag);
        trunc_opt(&mut snapshot.active_element_id);
        trunc(&mut snapshot.visibility_state);
        trunc(&mut snapshot.client_timestamp);
        trunc(&mut snapshot.build_id);
        trunc_opt(&mut snapshot.screen_orientation_type);
        trunc_opt(&mut snapshot.html_height);
        trunc_opt(&mut snapshot.body_height);
        trunc_opt(&mut snapshot.body_overflow);
        trunc_opt(&mut snapshot.input_bar_position);
        trunc_opt(&mut snapshot.input_bar_flex_shrink);
        trunc_opt(&mut snapshot.app_main_min_height);
        trunc_opt(&mut snapshot.pane_layout_min_height);
        trunc_opt(&mut snapshot.pane_layout_height);
        trunc_opt(&mut snapshot.message_list_min_height);
        trunc_opt(&mut snapshot.message_list_height);
        trunc_opt(&mut snapshot.mobile_slot_content_min_height);
        trunc_opt(&mut snapshot.app_main_height);
        trunc_opt(&mut snapshot.safe_area_inset_top);
        trunc_opt(&mut snapshot.safe_area_inset_right);
        trunc_opt(&mut snapshot.safe_area_inset_bottom);
        trunc_opt(&mut snapshot.safe_area_inset_left);

        // Step 1: structured INFO log with the full raw payload (all fields
        // including free-text strings, now truncated). The INFO log is for
        // human triage only; string fields must NOT reach CC (prompt-injection
        // risk — see render_debug_snapshot for the mitigation).
        let full_json = serde_json::to_string(&*snapshot)
            .expect("serializing an already-deserialized owned struct cannot fail");
        info!(
            debug_viewport_snapshot = true,
            user_id = self.user_id,
            app_slug = %self.app_slug,
            conversation_id = ?self.current_conversation_id,
            device_id = self.device_id,
            payload = %full_json,
            "debug viewport snapshot received",
        );

        // Step 2: fire an alert so the snapshot reaches the operator out-of-band
        // (the alert pipeline emails it). This is the easiest way to pull the
        // diagnostic off the device. Info severity — it's a diagnostic, not an
        // incident. full_json is the same already-truncated payload as the INFO
        // log; it goes to the operator's own inbox, so the prompt-injection
        // concern that gates render_debug_snapshot does not apply here.
        self.state.alert_dispatcher.alert(
            brenn_lib::obs::alerting::AlertSeverity::Info,
            "Debug viewport snapshot".to_string(),
            full_json,
        );

        // Step 3: inject as a system message. resolve_bridge creates a fresh
        // conversation if there is none (Case 3). On None it has already sent an
        // Error frame; log a warn and return — the INFO log above already captured
        // the snapshot, so no diagnostic data is lost.
        let Some(bridge) = self.resolve_bridge(None).await else {
            warn!(
                user_id = self.user_id,
                "debug snapshot: no bridge available; skipping CC injection"
            );
            return;
        };

        // render_debug_snapshot injects only numeric/boolean geometry into CC;
        // free-text fields are excluded (prompt-injection mitigation).
        let rendered = crate::system_message::render_debug_snapshot(&snapshot);
        if let Err(e) = bridge
            .send_system_message(rendered, Some(self.user_id))
            .await
        {
            // CC delivery failed after persist+broadcast. The UI card is already
            // visible and the snapshot log succeeded; no rollback is possible or
            // necessary (async-only delivery: CC receives the next inject on reconnect).
            // Log and return.
            warn!(user_id = self.user_id, error = %e, "debug snapshot: CC send failed after persist/broadcast");
        }
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::auth::user::create_user;
    use brenn_lib::conversation;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::ws_types::{CcState, WsServerMessage};
    use tokio::sync::{broadcast, mpsc};

    use uuid::Uuid;

    use super::super::testing::*;
    use super::truncate_title;
    use crate::active_bridge::ActiveBridge;
    use crate::state::AppState;

    #[tokio::test]
    async fn viewer_only_rejects_send_message_without_security_event() {
        let (mut conn, mut ws_rx, db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        // Create another user's bridge and simulate auto-attach.
        let other_user_id = {
            let c = db.lock().await;
            create_user(&c, "otheruser", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, other_user_id, "test", false)
        };
        let (broadcast_tx, _) = broadcast::channel(64);
        let other_bridge = ActiveBridge::inject_for_test(
            other_user_id,
            other_conv_id,
            "test",
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(other_conv_id, other_bridge.clone())
            .await;

        // Simulate auto-attach as viewer.
        conn.attach_to_bridge(&other_bridge).await;
        conn.viewer_only = true;

        // Try to send a message.
        conn.handle_send_message("hello", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        // Should get AppBusy (which shows the steal button), not a raw Error.
        let has_app_busy = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::AppBusy { .. }));
        assert!(has_app_busy, "expected AppBusy, got: {msgs:?}");
        // Should NOT get a spawn attempt or security event.
        let has_switched = msgs
            .iter()
            .any(|m| matches!(m, WsServerMessage::ConversationSwitched { .. }));
        assert!(!has_switched, "should not spawn, got: {msgs:?}");
    }

    #[test]
    fn truncate_title_short_text() {
        assert_eq!(truncate_title("hello world", 80), "hello world");
    }

    #[test]
    fn truncate_title_at_word_boundary() {
        let text = "This is a long title that should be truncated at a word boundary somewhere in the middle of the text";
        let title = truncate_title(text, 60);
        assert!(title.len() <= 62); // 60 + "…"
        assert!(title.ends_with('…'));
        assert!(!title.contains("text")); // should have been cut before this
    }

    #[test]
    fn truncate_title_no_spaces() {
        let text = "a".repeat(100);
        let title = truncate_title(&text, 80);
        assert!(title.chars().count() <= 81); // 80 chars + "…"
        assert!(title.ends_with('…'));
    }

    #[test]
    fn truncate_title_multibyte_utf8() {
        // Emoji and CJK characters are multi-byte — must not panic.
        let text = "🐧".repeat(50); // 50 penguin emoji (4 bytes each)
        let title = truncate_title(&text, 10);
        assert!(title.chars().count() <= 11);
        assert!(title.ends_with('…'));

        // Mixed ASCII and multi-byte.
        let text = "café résumé naïve über straße 日本語テスト more text here to exceed the limit";
        let title = truncate_title(text, 30);
        assert!(title.chars().count() <= 31);
    }

    #[test]
    fn truncate_title_trims_whitespace() {
        assert_eq!(truncate_title("  hello  ", 80), "hello");
    }

    #[tokio::test]
    async fn stop_request_without_conversation_is_silent_noop() {
        // StopRequest with no current conversation should not panic or error.
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);

        let (ws_tx, mut ws_rx) = mpsc::channel(256);
        let (user_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            (uid, did)
        };

        let mut conn = WsConnBuilder::with_defaults(
            user_id,
            TEST_USERNAME.to_string(),
            TEST_APP_SLUG.to_string(),
            ws_tx,
            state,
            device_id,
        )
        .build();

        // Should complete without panicking.
        conn.handle_stop_request().await;

        // No messages sent to the client.
        let msgs = collect_messages(&mut ws_rx).await;
        assert!(
            msgs.is_empty(),
            "stop_request should not send any WS messages"
        );
    }

    #[tokio::test]
    async fn send_message_with_attachment_persists_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let (mut conn, mut ws_rx, db, user_id, conv_id) =
            test_ws_conn_with_working_dir(dir.path().to_path_buf()).await;

        let upload_id = Uuid::new_v4();
        inject_pending_upload(&conn.state, upload_id, user_id, "test", "receipt.jpg").await;
        create_fake_attachment_file(dir.path(), &upload_id, "receipt.jpg");

        conn.handle_send_message(
            "check this receipt",
            vec![brenn_lib::ws_types::AttachmentRef {
                upload_id: upload_id.to_string(),
            }],
            None,
            vec![],
        )
        .await;

        // Verify attachment was persisted in DB.
        {
            let db_conn = db.lock().await;
            let attachments = conversation::get_attachments_for_conversation(&db_conn, conv_id);
            assert!(
                !attachments.is_empty(),
                "expected attachments in DB, got none"
            );
            let all_metas: Vec<_> = attachments.values().flatten().collect();
            let meta = all_metas
                .iter()
                .find(|m| m.upload_id == upload_id.to_string())
                .expect("expected attachment with matching upload_id");
            assert_eq!(meta.filename, "receipt.jpg");
            assert_eq!(meta.media_type, "image/jpeg");
            assert_eq!(meta.size, 12345);
        }

        // Verify no error was sent on the persistence/auth path.
        // A CC-send-failure error IS expected (injected bridge has no CC session).
        // Any other Error variant is unexpected.
        let msgs = collect_messages(&mut ws_rx).await;
        let cc_send_error = msgs.iter().find(|m| is_cc_send_failure_error(m));
        assert!(
            cc_send_error.is_some(),
            "expected CC-send-failure error frame (injected bridge has no session), got: {msgs:?}"
        );
        let unexpected_error = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::Error { .. }) && !is_cc_send_failure_error(m));
        assert!(
            unexpected_error.is_none(),
            "expected no auth/routing error, got: {unexpected_error:?}"
        );

        // Upload should be removed from pending.
        assert!(conn.state.pending_uploads.lock().await.is_empty());
    }

    #[tokio::test]
    async fn send_message_with_invalid_upload_id_returns_error() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_send_message(
            "look at this",
            vec![brenn_lib::ws_types::AttachmentRef {
                upload_id: Uuid::new_v4().to_string(),
            }],
            None,
            vec![],
        )
        .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let error = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            error.is_some(),
            "expected Error for missing upload_id, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_with_bad_uuid_returns_error() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_send_message(
            "look at this",
            vec![brenn_lib::ws_types::AttachmentRef {
                upload_id: "not-a-uuid".to_string(),
            }],
            None,
            vec![],
        )
        .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let error = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            error.is_some(),
            "expected Error for bad UUID, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_with_wrong_app_upload_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let (mut conn, mut ws_rx, _db, user_id, _conv_id) =
            test_ws_conn_with_working_dir(dir.path().to_path_buf()).await;

        let upload_id = Uuid::new_v4();
        inject_pending_upload(&conn.state, upload_id, user_id, "other-app", "file.txt").await;
        create_fake_attachment_file(dir.path(), &upload_id, "file.txt");

        conn.handle_send_message(
            "look",
            vec![brenn_lib::ws_types::AttachmentRef {
                upload_id: upload_id.to_string(),
            }],
            None,
            vec![],
        )
        .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let error = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            error.is_some(),
            "expected Error for wrong app, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_with_wrong_user_upload_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) =
            test_ws_conn_with_working_dir(dir.path().to_path_buf()).await;

        let upload_id = Uuid::new_v4();
        inject_pending_upload(&conn.state, upload_id, 9999, "test", "file.txt").await;
        create_fake_attachment_file(dir.path(), &upload_id, "file.txt");

        conn.handle_send_message(
            "look",
            vec![brenn_lib::ws_types::AttachmentRef {
                upload_id: upload_id.to_string(),
            }],
            None,
            vec![],
        )
        .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let error = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::Error { .. }));
        assert!(
            error.is_some(),
            "expected Error for wrong user, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_without_attachments_still_works() {
        let (mut conn, mut ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_send_message("plain message", vec![], None, vec![])
            .await;

        // A CC-send-failure error IS expected (injected bridge has no CC session).
        // Any other Error variant is unexpected.
        let msgs = collect_messages(&mut ws_rx).await;
        let cc_send_error = msgs.iter().find(|m| is_cc_send_failure_error(m));
        assert!(
            cc_send_error.is_some(),
            "expected CC-send-failure error frame (injected bridge has no session), got: {msgs:?}"
        );
        let unexpected_error = msgs
            .iter()
            .find(|m| matches!(m, WsServerMessage::Error { .. }) && !is_cc_send_failure_error(m));
        assert!(
            unexpected_error.is_none(),
            "expected no auth/routing error, got: {unexpected_error:?}"
        );

        // Message should be persisted.
        let db_conn = db.lock().await;
        let messages = conversation::get_messages(&db_conn, conv_id);
        let user_msg = messages.iter().find(|m| m.msg_type == "user");
        assert!(user_msg.is_some(), "expected user message in DB");

        // No attachments should be persisted.
        let attachments = conversation::get_attachments_for_conversation(&db_conn, conv_id);
        assert!(attachments.is_empty(), "expected no attachments in DB");
    }

    // --- Model validation tests ---

    #[tokio::test]
    async fn send_message_with_invalid_model_returns_error() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Populate cached models for the app.
        conn.state
            .cached_models
            .write()
            .await
            .insert("test".to_string(), test_model_infos());

        // Send with an invalid model.
        conn.handle_send_message("hello", vec![], Some("gpt-4"), vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let error = msgs.iter().find(|m| match m {
            WsServerMessage::Error { message } => message.contains("Invalid model"),
            _ => false,
        });
        assert!(
            error.is_some(),
            "expected Error with 'Invalid model', got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_with_valid_model_succeeds() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Populate cached models for the app.
        conn.state.cached_models.write().await.insert(
            "test".to_string(),
            vec![brenn_lib::ws_types::ModelInfo {
                value: "sonnet".into(),
                display_name: "Sonnet".into(),
                description: "Fast".into(),
            }],
        );

        // Send with a valid model — should not get "Invalid model" error.
        conn.handle_send_message("hello", vec![], Some("sonnet"), vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let invalid_model_error = msgs.iter().any(|m| match m {
            WsServerMessage::Error { message } => message.contains("Invalid model"),
            _ => false,
        });
        assert!(
            !invalid_model_error,
            "unexpected 'Invalid model' error: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_with_no_cached_models_allows_any_model() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // No cached models — validation should pass for any model value.
        conn.handle_send_message("hello", vec![], Some("anything-goes"), vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        let invalid_model_error = msgs.iter().any(|m| match m {
            WsServerMessage::Error { message } => message.contains("Invalid model"),
            _ => false,
        });
        assert!(
            !invalid_model_error,
            "should not reject model when cache is empty: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_with_model_on_existing_bridge_still_sends() {
        // When a bridge already exists and set_model fails (test bridges have no
        // real CC session), the user message should still be sent — set_model
        // failure is warn-and-continue, not a blocking error.
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);
        let (ws_tx, mut ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        let (user_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            (uid, did)
        };
        let conv_id = {
            let conn = db.lock().await;
            conversation::create_conversation(&conn, user_id, TEST_APP_SLUG, false)
        };

        let bridge = ActiveBridge::inject_for_test(
            user_id,
            conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );
        state.active_bridges.insert(conv_id, bridge.clone()).await;

        let mut conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            ..WsConnBuilder::with_defaults(
                user_id,
                TEST_USERNAME.to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        // Send with a model — set_model will fail (test bridge has no session),
        // but the user message should still be persisted.
        conn.handle_send_message("hello with model", vec![], Some("opus"), vec![])
            .await;

        // The message should be persisted in the DB despite set_model failure.
        let db_conn = db.lock().await;
        let messages = conversation::get_messages(&db_conn, conv_id);
        let user_msg = messages
            .iter()
            .find(|m| m.msg_type == "user" && m.payload.contains("hello with model"));
        assert!(
            user_msg.is_some(),
            "user message should be persisted even when set_model fails"
        );

        // Should not have gotten an "Invalid model" error (no cached models).
        let msgs = collect_messages(&mut ws_rx).await;
        let has_invalid_model_error = msgs.iter().any(|m| match m {
            WsServerMessage::Error { message } => message.contains("Invalid model"),
            _ => false,
        });
        assert!(
            !has_invalid_model_error,
            "should not get model validation error: {msgs:?}"
        );
    }

    // === selected_tasks validation tests ===

    #[tokio::test]
    async fn send_message_rejects_too_many_selected_tasks() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        let tasks: Vec<brenn_lib::ws_types::SelectedTask> = (0..21)
            .map(|i| brenn_lib::ws_types::SelectedTask {
                task_ref: format!("todo/task-{i}.md"),
            })
            .collect();

        conn.handle_send_message("hello", vec![], None, tasks).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message.contains("Too many")),
        );
        assert!(has_error, "expected error for >20 selected tasks: {msgs:?}");
    }

    #[tokio::test]
    async fn send_message_rejects_path_traversal_in_task_ref() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        let tasks = vec![brenn_lib::ws_types::SelectedTask {
            task_ref: "todo/../secrets.md".to_string(),
        }];

        conn.handle_send_message("hello", vec![], None, tasks).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message.contains("Invalid task")),
        );
        assert!(
            has_error,
            "expected error for path traversal in task_ref: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_rejects_non_md_task_ref() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        let tasks = vec![brenn_lib::ws_types::SelectedTask {
            task_ref: "todo/secrets.txt".to_string(),
        }];

        conn.handle_send_message("hello", vec![], None, tasks).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message.contains("Invalid task")),
        );
        assert!(has_error, "expected error for non-.md task_ref: {msgs:?}");
    }

    #[tokio::test]
    async fn send_message_accepts_valid_selected_tasks() {
        let (mut conn, mut ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        let tasks = vec![brenn_lib::ws_types::SelectedTask {
            task_ref: "life:todo/buy-groceries.md".to_string(),
        }];

        conn.handle_send_message("Which first?", vec![], None, tasks)
            .await;

        let msgs = collect_messages(&mut ws_rx).await;
        // Should not have a validation error.
        let has_validation_error = msgs.iter().any(|m| {
            matches!(m, WsServerMessage::Error { message } if message.contains("Invalid task") || message.contains("Too many"))
        });
        assert!(
            !has_validation_error,
            "valid selected tasks should not produce errors: {msgs:?}"
        );

        // Message should be persisted.
        let db_conn = db.lock().await;
        let messages = conversation::get_messages(&db_conn, conv_id);
        let user_msg = messages.iter().find(|m| m.msg_type == "user");
        assert!(
            user_msg.is_some(),
            "expected user message in DB after valid selected_tasks send"
        );
    }

    #[tokio::test]
    async fn send_message_accepts_exactly_20_selected_tasks() {
        let (mut conn, mut ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Exactly 20 is the limit — should pass.
        let tasks: Vec<brenn_lib::ws_types::SelectedTask> = (0..20)
            .map(|i| brenn_lib::ws_types::SelectedTask {
                task_ref: format!("todo/task-{i}.md"),
            })
            .collect();

        conn.handle_send_message("hello", vec![], None, tasks).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_validation_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message.contains("Too many")),
        );
        assert!(
            !has_validation_error,
            "exactly 20 selected tasks should be accepted: {msgs:?}"
        );

        // Message should be persisted at the boundary.
        let db_conn = db.lock().await;
        let messages = conversation::get_messages(&db_conn, conv_id);
        let user_msg = messages.iter().find(|m| m.msg_type == "user");
        assert!(
            user_msg.is_some(),
            "expected user message in DB after exactly-20 selected_tasks send"
        );
    }

    #[tokio::test]
    async fn send_message_rejects_task_ref_over_256_chars() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // 257 chars — just over the limit.
        let long_ref = format!("todo/{}.md", "x".repeat(249)); // "todo/" + 249 + ".md" = 257
        let tasks = vec![brenn_lib::ws_types::SelectedTask { task_ref: long_ref }];

        conn.handle_send_message("hello", vec![], None, tasks).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message.contains("Invalid task")),
        );
        assert!(
            has_error,
            "expected error for task_ref > 256 chars: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn send_message_accepts_task_ref_at_256_chars() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Exactly 256 chars — should pass.
        let ref_256 = format!("todo/{}.md", "x".repeat(248)); // "todo/" + 248 + ".md" = 256
        assert_eq!(ref_256.len(), 256);
        let tasks = vec![brenn_lib::ws_types::SelectedTask { task_ref: ref_256 }];

        conn.handle_send_message("hello", vec![], None, tasks).await;

        let msgs = collect_messages(&mut ws_rx).await;
        let has_validation_error = msgs.iter().any(
            |m| matches!(m, WsServerMessage::Error { message } if message.contains("Invalid task")),
        );
        assert!(
            !has_validation_error,
            "task_ref at exactly 256 chars should be accepted: {msgs:?}"
        );
    }

    /// Resume path in handle_send_message emits CcState::Connecting before spawning.
    #[tokio::test]
    async fn resume_send_message_emits_connecting_then_thinking() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.handle_send_message("hello", vec![], None, vec![])
            .await;

        let msgs = collect_messages(&mut ws_rx).await;

        // Should see Status { Connecting } before ConversationSwitched { Thinking }.
        let connecting_idx = msgs.iter().position(|m| {
            matches!(
                m,
                WsServerMessage::Status {
                    state: CcState::Connecting
                }
            )
        });
        let switched_idx = msgs.iter().position(|m| {
            matches!(
                m,
                WsServerMessage::ConversationSwitched {
                    state: CcState::Thinking,
                    ..
                }
            )
        });
        assert!(
            connecting_idx.is_some(),
            "expected Status {{ Connecting }} in messages, got: {msgs:?}"
        );
        assert!(
            switched_idx.is_some(),
            "expected ConversationSwitched {{ Thinking }} in messages, got: {msgs:?}"
        );
        assert!(
            connecting_idx.unwrap() < switched_idx.unwrap(),
            "Connecting should come before Thinking, got: {msgs:?}"
        );
    }
    /// `build_cc_message_text` includes the device slug when `prefix_device=true`.
    /// Tests the prefix-building function directly with a device slug from the DB.
    #[tokio::test]
    async fn persist_and_send_includes_device_slug_in_prefix() {
        let (conn, _ws_rx, db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Load device slug as persist_and_send would.
        let (device_slug, guessed_slug) = {
            let db_conn = db.lock().await;
            let device = brenn_lib::auth::device::load_device(&db_conn, conn.device_id);
            let du =
                brenn_lib::auth::device::load_device_user(&db_conn, conn.device_id, conn.user_id);
            let slug = du.display_slug(&device).to_string();
            let guessed = device.guessed_slug.clone();
            (slug, guessed)
        };

        // Build prefix text as the real code path does.
        let now: chrono::DateTime<chrono_tz::Tz> =
            chrono::Utc::now().with_timezone(&chrono_tz::UTC);
        let prefixed = crate::cc_message_prefix::build_cc_message_text(
            "hello world",
            TEST_USERNAME,
            Some(&device_slug),
            &now,
            false,
            false,
            true, // prefix_device=true
        );
        // The guessed slug from test UA "Mozilla/5.0 (X11; Linux x86_64) Chrome/125.0"
        // should be "chrome-linux".
        assert!(
            prefixed.contains(&guessed_slug),
            "prefix must contain device slug {guessed_slug}: {prefixed}"
        );
    }

    /// After `assign_device_slug` renames a device, `build_cc_message_text` picks
    /// up the new name on the next load (per-call `load_device_user` wiring).
    #[tokio::test]
    async fn persist_and_send_picks_up_renamed_slug() {
        let (conn, _ws_rx, db, uid, _conv_id) = test_ws_conn_with_resume_conv().await;
        let device_id = conn.device_id;

        // Assign a slug.
        {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::assign_device_slug(&db_conn, device_id, "renamed-dev", uid);
        }

        // Load slug as persist_and_send would after the rename.
        let device_slug = conn.fetch_device_slug().await;

        let now: chrono::DateTime<chrono_tz::Tz> =
            chrono::Utc::now().with_timezone(&chrono_tz::UTC);
        let prefixed = crate::cc_message_prefix::build_cc_message_text(
            "message after rename",
            TEST_USERNAME,
            Some(&device_slug),
            &now,
            false,
            false,
            true,
        );
        assert!(
            prefixed.contains("renamed-dev"),
            "prefix must reflect renamed slug: {prefixed}"
        );
    }

    /// Unassigned device: the first user message emits a slug reminder. The
    /// reminder is persisted to DB (observable by querying the messages table)
    /// and the user message itself is also persisted.
    #[tokio::test]
    async fn unassigned_device_first_message_emits_reminder() {
        let (mut conn, _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // Verify device has no assigned slug initially.
        {
            let db_conn = db.lock().await;
            let du =
                brenn_lib::auth::device::load_device_user(&db_conn, conn.device_id, conn.user_id);
            assert!(
                du.assigned_slug.is_none(),
                "device must start with no assigned slug"
            );
        }

        conn.handle_send_message("first message", vec![], None, vec![])
            .await;

        // Verify slug_prompted_at was updated.
        {
            let db_conn = db.lock().await;
            let du =
                brenn_lib::auth::device::load_device_user(&db_conn, conn.device_id, conn.user_id);
            assert!(
                du.slug_prompted_at.is_some(),
                "slug_prompted_at must be set after first reminder"
            );
        }

        // The reminder should be persisted as a separate system message row.
        let db_conn = db.lock().await;
        let reminder_count: i64 = db_conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1 \
                 AND direction = 'outgoing' AND msg_type = 'user' \
                 AND payload LIKE '%DeviceSlugReminder%'",
                rusqlite::params![conv_id],
                |row| row.get(0),
            )
            .expect("count query");
        assert_eq!(
            reminder_count, 1,
            "one DeviceSlugReminder message must be persisted"
        );
    }

    /// Unassigned device: the second message within 24h must NOT emit another reminder.
    #[tokio::test]
    async fn unassigned_device_second_message_within_24h_no_reminder() {
        let (mut conn, _ws_rx, db, _uid, conv_id) = test_ws_conn_with_resume_conv().await;

        // First message — emits reminder, sets slug_prompted_at.
        conn.handle_send_message("first", vec![], None, vec![])
            .await;

        // Second message within the same second — should not emit reminder.
        conn.handle_send_message("second", vec![], None, vec![])
            .await;

        let db_conn = db.lock().await;
        let reminder_count: i64 = db_conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1 \
                 AND direction = 'outgoing' AND msg_type = 'user' \
                 AND payload LIKE '%DeviceSlugReminder%'",
                rusqlite::params![conv_id],
                |row| row.get(0),
            )
            .expect("count query");
        assert_eq!(
            reminder_count, 1,
            "24h rate-limit must suppress second reminder"
        );
    }

    /// After `DeviceAssignSlug` assigns a slug, subsequent messages must not
    /// emit a reminder.
    #[tokio::test]
    async fn assigned_device_no_reminder_after_rename() {
        let (mut conn, _ws_rx, db, uid, conv_id) = test_ws_conn_with_resume_conv().await;
        let device_id = conn.device_id;

        // Assign a slug directly.
        {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::assign_device_slug(&db_conn, device_id, "my-device", uid);
        }

        conn.handle_send_message("message with assigned device", vec![], None, vec![])
            .await;

        let db_conn = db.lock().await;
        let reminder_count: i64 = db_conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1 \
                 AND direction = 'outgoing' AND msg_type = 'user' \
                 AND payload LIKE '%DeviceSlugReminder%'",
                rusqlite::params![conv_id],
                |row| row.get(0),
            )
            .expect("count query");
        assert_eq!(
            reminder_count, 0,
            "assigned device must not trigger reminder"
        );
    }

    /// `render_ui_error` with a real device slug from the connection's device shows
    /// the device slug on the `[System] Device:` line.
    #[tokio::test]
    async fn ui_error_includes_device_slug() {
        let (conn, _ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        let device_slug = conn.fetch_device_slug().await;

        let render = crate::system_message::render_ui_error(
            "some_tool",
            "path/to/thing",
            &[],
            r#"{"x":1}"#,
            Some(&device_slug),
        );
        assert!(
            render.text.contains("[System] Device:"),
            "UI error must have Device: line: {}",
            render.text
        );
        assert!(
            render.text.contains(&device_slug),
            "UI error must contain device slug {device_slug}: {}",
            render.text
        );
    }

    /// `render_ui_error` reflects renamed device slug after `assign_device_slug`.
    #[tokio::test]
    async fn ui_error_picks_up_renamed_slug() {
        let (conn, _ws_rx, db, uid, _conv_id) = test_ws_conn_with_resume_conv().await;
        let device_id = conn.device_id;

        {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::assign_device_slug(
                &db_conn,
                device_id,
                "renamed-for-error",
                uid,
            );
        }

        let device_slug = conn.fetch_device_slug().await;

        let render = crate::system_message::render_ui_error(
            "some_tool",
            "path/to/thing",
            &[],
            r#"{"x":1}"#,
            Some(&device_slug),
        );
        assert!(
            render.text.contains("renamed-for-error"),
            "UI error must reflect renamed slug: {}",
            render.text
        );
    }

    // --- send-message-reject-guard: handle_send_message returns false on rejection ---

    /// Viewer-only rejection returns false (message not dispatched).
    #[tokio::test]
    async fn handle_send_message_returns_false_on_viewer_only_rejection() {
        let (mut conn, _ws_rx, db, _user_id) =
            test_ws_conn_for_app(test_apps_single_instance()).await;

        let other_user_id = {
            let c = db.lock().await;
            create_user(&c, "otheruser2", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, other_user_id, "test", false)
        };
        let (broadcast_tx, _) = broadcast::channel(64);
        let other_bridge = ActiveBridge::inject_for_test(
            other_user_id,
            other_conv_id,
            "test",
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(other_conv_id, other_bridge.clone())
            .await;
        conn.attach_to_bridge(&other_bridge).await;
        conn.viewer_only = true;

        let dispatched = conn
            .handle_send_message("hello", vec![], None, vec![])
            .await;
        assert!(
            !dispatched,
            "viewer-only rejection must return false (not dispatched)"
        );
    }

    /// Invalid model rejection returns false (message not dispatched).
    #[tokio::test]
    async fn handle_send_message_returns_false_on_invalid_model_rejection() {
        let (mut conn, _ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        conn.state.cached_models.write().await.insert(
            "test".to_string(),
            vec![brenn_lib::ws_types::ModelInfo {
                value: "sonnet".into(),
                display_name: "Sonnet".into(),
                description: "Fast".into(),
            }],
        );

        let dispatched = conn
            .handle_send_message("hello", vec![], Some("bad-model"), vec![])
            .await;
        assert!(
            !dispatched,
            "invalid model rejection must return false (not dispatched)"
        );
    }

    /// Too-many-tasks rejection returns false (message not dispatched).
    #[tokio::test]
    async fn handle_send_message_returns_false_on_too_many_tasks() {
        let (mut conn, _ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        let tasks: Vec<brenn_lib::ws_types::SelectedTask> = (0..21)
            .map(|i| brenn_lib::ws_types::SelectedTask {
                task_ref: format!("todo/task-{i}.md"),
            })
            .collect();

        let dispatched = conn.handle_send_message("hello", vec![], None, tasks).await;
        assert!(
            !dispatched,
            "too-many-tasks rejection must return false (not dispatched)"
        );
    }

    /// Path-traversal task_ref rejection returns false (message not dispatched).
    #[tokio::test]
    async fn handle_send_message_returns_false_on_invalid_task_ref() {
        let (mut conn, _ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        let tasks = vec![brenn_lib::ws_types::SelectedTask {
            task_ref: "../etc/passwd".to_string(),
        }];

        let dispatched = conn.handle_send_message("hello", vec![], None, tasks).await;
        assert!(
            !dispatched,
            "path-traversal task_ref must return false (not dispatched)"
        );
    }

    /// Successful dispatch returns true.
    #[tokio::test]
    async fn handle_send_message_returns_true_on_dispatch() {
        let (mut conn, _ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        let dispatched = conn
            .handle_send_message("hello", vec![], None, vec![])
            .await;
        assert!(dispatched, "successful dispatch must return true");
    }

    // --- usage-obs-send-message-attr-guard: sender_device_id persisted before return ---

    /// `messages.sender_device_id` must be persisted before `handle_send_message`
    /// returns so that `handle_turn_completed` → `resolve_sender_for_conversation`
    /// can attribute the `llm_turn` event via the primary path.
    ///
    /// Regression canary: if `persist_user_message_with_attachments` is ever deferred
    /// (e.g. moved to a spawned task), this test will fail and the attribution path
    /// will silently degrade.
    ///
    /// Uses the same active-bridge fixture as `send_message_with_model_on_existing_bridge_still_sends`
    /// to take the fast path in `resolve_bridge` (Case 1) and go straight to `persist_and_send`.
    #[tokio::test]
    async fn handle_send_message_persists_sender_device_id_before_return() {
        let (mut conn, _ws_rx, db, _user_id, conv_id) = test_ws_conn_with_active_bridge().await;
        let device_id = conn.device_id;
        // Verify Case 1 dispatch path: active_bridges must hold the bridge for
        // conv_id before handle_send_message is called.
        assert!(
            conn.state.active_bridges.get(conv_id).await.is_some(),
            "test_ws_conn_with_active_bridge must insert bridge into active_bridges (Case 1)"
        );

        conn.handle_send_message("test attribution", vec![], None, vec![])
            .await;

        // Query messages.sender_device_id synchronously after return.
        // Use ORDER BY id ASC LIMIT 1: the user message (with sender_device_id) is written
        // first by persist_user_message_with_attachments; any slug reminder (sender_device_id
        // IS NULL) is written after, so it has a higher id. Taking the lowest id row gives
        // the actual user message, not the system reminder.
        // If sender_device_id is NULL, the primary attribution path cannot work.
        let db_conn = db.lock().await;
        let sender_device_id: Option<i64> = db_conn
            .query_row(
                "SELECT sender_device_id FROM messages \
                 WHERE conversation_id = ?1 AND direction = 'outgoing' AND msg_type = 'user' \
                 ORDER BY id ASC LIMIT 1",
                rusqlite::params![conv_id],
                |row| row.get(0),
            )
            .expect("message row must exist after handle_send_message returns");
        assert_eq!(
            sender_device_id,
            Some(device_id),
            "sender_device_id must be persisted before handle_send_message returns \
             (required for llm_turn primary attribution)"
        );
    }

    // ── TZ override read-site tests: persist_and_send and request_compaction ──

    /// Build a test apps map with `prefix_timestamp = true` so TZ override is
    /// observable in the message prefix text.
    fn test_apps_with_timestamp_prefix()
    -> std::sync::Arc<indexmap::IndexMap<String, brenn_lib::config::AppConfig>> {
        let mut apps = indexmap::IndexMap::new();
        let mut cfg = crate::test_support::app_config::default_test_app_config("test", "Test App");
        cfg.prefix_timestamp = true;
        apps.insert("test".to_string(), cfg);
        std::sync::Arc::new(apps)
    }

    /// `persist_and_send` honours the TZ override: the `UserMessageEcho.timestamp`
    /// carries the RFC3339 offset of the override zone (Asia/Tokyo = +09:00).
    #[tokio::test]
    async fn persist_and_send_honours_tz_override() {
        let db = init_db_memory();
        let apps = test_apps_with_timestamp_prefix();
        let state = AppState::for_test(db.clone(), Some(apps));

        let (ws_tx, _ws_rx) = mpsc::channel(256);
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<WsServerMessage>(64);

        let (user_id, conv_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            conversation::complete_conversation(&conn, cid, None);
            (uid, cid, did)
        };

        // Set TZ override to Asia/Tokyo.
        {
            let conn = db.lock().await;
            brenn_lib::auth::device::set_tz_override(
                &conn,
                device_id,
                user_id,
                Some("Asia/Tokyo"),
                None,
            );
        }

        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        state.active_bridges.insert(conv_id, bridge.clone()).await;

        let mut conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            ..WsConnBuilder::with_defaults(
                user_id,
                TEST_USERNAME.to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        conn.handle_send_message("hello", vec![], None, vec![])
            .await;

        // Collect from broadcast channel until we find a UserMessageEcho.
        let echo_timestamp = {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                match tokio::time::timeout_at(deadline, broadcast_rx.recv()).await {
                    Ok(Ok(WsServerMessage::UserMessageEcho { timestamp, .. })) => break timestamp,
                    Ok(Ok(_)) => continue,
                    _ => panic!("no UserMessageEcho received within 2s"),
                }
            }
        };

        // Asia/Tokyo is UTC+9; RFC3339 must contain "+09:00".
        assert!(
            echo_timestamp.contains("+09:00"),
            "UserMessageEcho.timestamp must carry the override TZ offset (+09:00); got: {echo_timestamp}"
        );
        // NOTE: `UserMessageEcho.timestamp` is `local_now.to_rfc3339()`, and
        // `build_cc_message_text` (the CC-prefix path) receives the same `local_now`
        // value at messaging.rs:433 — they cannot disagree unless a future refactor
        // splits `local_now`. The echo-timestamp assertion above is therefore
        // sufficient to verify both the echo path and the CC-prefix path.
    }

    /// `persist_and_send` reverts to browser TZ (UTC, +00:00) after clearing the override.
    #[tokio::test]
    async fn persist_and_send_reverts_after_tz_override_clear() {
        let db = init_db_memory();
        let apps = test_apps_with_timestamp_prefix();
        let state = AppState::for_test(db.clone(), Some(apps));

        let (ws_tx, _ws_rx) = mpsc::channel(256);
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<WsServerMessage>(64);

        let (user_id, conv_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, TEST_USERNAME, "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            conversation::complete_conversation(&conn, cid, None);
            (uid, cid, did)
        };

        // Set override then immediately clear it.
        {
            let conn = db.lock().await;
            brenn_lib::auth::device::set_tz_override(
                &conn,
                device_id,
                user_id,
                Some("Asia/Tokyo"),
                None,
            );
            brenn_lib::auth::device::set_tz_override(&conn, device_id, user_id, None, None);
        }

        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        state.active_bridges.insert(conv_id, bridge.clone()).await;

        let mut conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            ..WsConnBuilder::with_defaults(
                user_id,
                TEST_USERNAME.to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        conn.handle_send_message("hello", vec![], None, vec![])
            .await;

        let echo_timestamp = {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                match tokio::time::timeout_at(deadline, broadcast_rx.recv()).await {
                    Ok(Ok(WsServerMessage::UserMessageEcho { timestamp, .. })) => break timestamp,
                    Ok(Ok(_)) => continue,
                    _ => panic!("no UserMessageEcho received within 2s"),
                }
            }
        };

        // After clear, browser TZ = UTC → RFC3339 offset must be +00:00.
        assert!(
            echo_timestamp.contains("+00:00"),
            "UserMessageEcho.timestamp must revert to UTC offset (+00:00) after clear; got: {echo_timestamp}"
        );
        // NOTE: same `local_now` shared with build_cc_message_text (messaging.rs:433);
        // echo-timestamp assertion covers both paths. See comment in persist_and_send_honours_tz_override.
    }

    /// `request_compaction` honours the TZ override: the persisted system-message
    /// text contains the override zone name when `prefix_timestamp = true`.
    #[tokio::test]
    async fn request_compaction_honours_tz_override() {
        let (mut conn, _ws_rx, db, user_id, conv_id) =
            test_ws_conn_with_active_bridge_and_apps(test_apps_with_timestamp_prefix()).await;

        // Set override to Asia/Tokyo.
        {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::set_tz_override(
                &db_conn,
                conn.device_id,
                user_id,
                Some("Asia/Tokyo"),
                None,
            );
        }

        conn.handle_request_compaction().await;

        // The system message text is persisted in the messages table.
        // Look for the Asia/Tokyo zone name in the payload content.
        let db_conn = db.lock().await;
        // Filter to the compaction user-request system message specifically rather
        // than relying on row ordering — a future test adding another outgoing message
        // after compaction would silently check the wrong row.
        // system_category lives inside the payload JSON, not as a DB column.
        let payload: String = db_conn
            .query_row(
                "SELECT payload FROM messages \
                 WHERE conversation_id = ?1 \
                   AND direction = 'outgoing' \
                   AND msg_type = 'user' \
                   AND json_extract(payload, '$.system_category') IS NOT NULL \
                 ORDER BY id DESC LIMIT 1",
                rusqlite::params![conv_id],
                |row| row.get(0),
            )
            .expect("compaction system message must be persisted after handle_request_compaction");

        assert!(
            payload.contains("Asia/Tokyo"),
            "compaction system-message text must include the override zone 'Asia/Tokyo'; payload: {payload}"
        );
    }

    // -----------------------------------------------------------------------
    // handle_debug_viewport_snapshot handler tests (design §4)
    //
    // Tests the handler directly (not via dispatch) to verify:
    //   (a) With an active bridge: system message persisted with
    //       `DebugSnapshot` category, text begins with the mandatory prefix
    //       (AC5), attributed to the user (sender_user_id column).
    //   (b) No current conversation (no bridge): INFO log fires and no
    //       message is persisted; handler does not panic.
    // -----------------------------------------------------------------------

    // Minimal DebugViewportSnapshotData is shared with dispatch.rs via testing.rs.
    // Use the canonical `minimal_debug_snapshot_data()` from testing.rs (reuse-1 fix).
    use super::super::testing::minimal_debug_snapshot_data as minimal_snapshot;

    /// With an active bridge, `handle_debug_viewport_snapshot` must persist a
    /// system message with:
    ///   - `system_category = "DebugSnapshot"` in the payload (AC5);
    ///   - text beginning with the mandatory human-readable prefix (AC5);
    ///   - `sender_user_id` equal to the connection's `user_id` (attribution).
    #[tokio::test]
    async fn handle_debug_viewport_snapshot_persists_system_message_with_debug_snapshot_category() {
        let (mut conn, _ws_rx, db, user_id, conv_id) = test_ws_conn_with_active_bridge().await;

        conn.handle_debug_viewport_snapshot(minimal_snapshot())
            .await;

        // `persist_broadcast_send` persists the row synchronously before the CC
        // send attempt, so the row is in the DB as soon as the handler returns.
        let (payload, sender_user_id): (String, Option<i64>) = {
            let db_conn = db.lock().await;
            db_conn
                .query_row(
                    "SELECT payload, sender_user_id FROM messages \
                     WHERE conversation_id = ?1 \
                       AND direction = 'outgoing' \
                       AND msg_type = 'user' \
                       AND json_extract(payload, '$.system_category') = 'DebugSnapshot' \
                     ORDER BY id DESC LIMIT 1",
                    rusqlite::params![conv_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("DebugSnapshot system message must be persisted")
        };

        // Category is already confirmed by the query; assert on the CC-facing text prefix.
        // The `system_rendered_text` field in the payload holds the `<brenn-*>`-wrapped text.
        assert!(
            payload.contains("The user clicked the Debug UI button:"),
            "persisted payload must contain the mandatory prefix (AC5): {payload}"
        );

        // Attribution: sender_user_id must be the requesting user.
        assert_eq!(
            sender_user_id,
            Some(user_id),
            "system message must be attributed to the requesting user_id"
        );
    }

    /// With no current conversation (no bridge), `handle_debug_viewport_snapshot`
    /// must NOT panic and must NOT persist any system message. The INFO log is
    /// asserted via `tracing_test::traced_test`.
    ///
    /// `test_ws_conn_for_app` returns a connection with no `current_conversation_id`,
    /// no test_wake_bridge, and no active bridges — so `resolve_bridge` returns `None`.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn handle_debug_viewport_snapshot_no_conversation_logs_info_and_no_persist() {
        let (mut conn, _ws_rx, db, _uid) = test_ws_conn_for_app(test_apps()).await;

        // Handler must complete without panic.
        conn.handle_debug_viewport_snapshot(minimal_snapshot())
            .await;

        // INFO log with the greppable tag must have fired (AC4).
        assert!(
            logs_contain("debug_viewport_snapshot"),
            "INFO log with 'debug_viewport_snapshot' tag must fire even when no bridge (AC4)"
        );

        // No system message must be persisted when bridge resolution fails.
        let count: i64 = db
            .lock()
            .await
            .query_row(
                "SELECT COUNT(*) FROM messages \
                 WHERE direction = 'outgoing' \
                   AND msg_type = 'user' \
                   AND json_extract(payload, '$.system_category') = 'DebugSnapshot'",
                [],
                |row| row.get(0),
            )
            .expect("count query");
        assert_eq!(
            count, 0,
            "no DebugSnapshot system message must be persisted when bridge resolution returns None"
        );
    }
}

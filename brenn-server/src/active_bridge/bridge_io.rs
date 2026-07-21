//! Bridge I/O: CC subprocess send paths, broadcast channel, system-message persist+broadcast, user-message persist with attachments, and incoming-message persistence.

use std::sync::atomic::Ordering;

use brenn_lib::conversation::{self, MessageDirection};
use brenn_lib::ws_types::{SystemMessageCategory, ViewportClass, WsServerMessage};
use tokio::sync::broadcast;
use tracing::info;

use super::ActiveBridge;
use super::compaction::CompactionPhase;

impl ActiveBridge {
    pub async fn set_model(&self, model: &str) -> Result<(), String> {
        // Skip if we already sent this model to CC.
        {
            let last = self.last_set_model.lock().await;
            if last.as_deref() == Some(model) {
                return Ok(());
            }
        }
        let session = self.session.lock().await;
        let session = session
            .as_ref()
            .ok_or_else(|| "CC session is not running".to_string())?;
        session
            .set_model(model)
            .await
            .map_err(|e| format!("failed to send set_model to CC: {e}"))?;
        // Update tracking after successful send.
        *self.last_set_model.lock().await = Some(model.to_string());
        Ok(())
    }

    pub async fn send_message(&self, text: &str) -> Result<(), String> {
        // Forward to send_outgoing; all compaction, session-alive, and busy-flag logic lives there.
        self.send_outgoing(brenn_cc::protocol::builders::user_message(text))
            .await
    }

    /// Send text to CC as a user turn without persisting to DB or broadcasting
    /// to browsers. Used for ephemeral context (lint errors) that the LLM should
    /// see but that should not re-appear on conversation replay.
    ///
    /// Sets `cc_busy` on success — CC will respond to any `CcOutgoing::User`
    /// message; tracking must stay consistent. If the session is dead or the
    /// send fails, returns `Err` and the caller logs and continues.
    ///
    /// Called directly during any compaction phase; lint context is ephemeral
    /// so message loss on a dead/compacting session is acceptable — the
    /// caller's dedup logic will suppress a repeat inject on the next query.
    pub async fn send_cc_only_system_text(&self, text: &str) -> Result<(), String> {
        let session = self.session.lock().await;
        let session = session
            .as_ref()
            .ok_or_else(|| "CC session is not running".to_string())?;

        if !session.is_alive() {
            return Err("CC session has died".to_string());
        }

        session
            .send_message(text)
            .await
            .map_err(|e| format!("failed to send cc-only system text to CC: {e}"))
            .inspect(|_| self.set_cc_busy("send_cc_only_system_text"))
    }

    /// Send a pre-built outgoing message to CC.
    ///
    /// Side effect: if the current compaction phase is `WaitingForIdle`, this
    /// call cancels the idle timer and resets phase to `Normal` before sending.
    /// Any caller that must send without cancelling the timer should avoid this
    /// path and acquire the session directly.
    pub async fn send_outgoing(&self, msg: brenn_cc::protocol::CcOutgoing) -> Result<(), String> {
        {
            let mut state = self.compaction.lock().await;
            match &state.phase {
                CompactionPhase::Normal => {} // proceed normally
                CompactionPhase::WaitingForIdle => {
                    // User message cancels the soft-trigger idle timer.
                    // Not yet committed (no visible persist message sent).
                    state.cancel_idle_timer();
                    state.phase = CompactionPhase::Normal;
                    info!("compaction idle timer cancelled by user message");
                    // Fall through to send normally.
                }
                phase => {
                    // PendingTurnCompletion, PersistingState, Compacting:
                    // send directly into the NDJSON stream — CC picks up
                    // messages written during compaction after it completes.
                    info!("sending message during compaction ({:?})", phase);
                }
            }
        }

        let session = self.session.lock().await;
        let session = session
            .as_ref()
            .ok_or_else(|| "CC session is not running".to_string())?;

        if !session.is_alive() {
            return Err("CC session has died".to_string());
        }

        // Set busy only after the send succeeds — a failed send must not leave
        // cc_idle=false with no turn in flight (UI stuck on "Thinking").
        session
            .send_outgoing(msg)
            .await
            .map_err(|e| format!("failed to send outgoing to CC: {e}"))
            .inspect(|_| self.set_cc_busy("send_outgoing"))
    }

    /// Subscribe to this bridge's broadcast channel.
    pub fn subscribe(&self) -> broadcast::Receiver<WsServerMessage> {
        self.event_tx.subscribe()
    }

    /// Update the shared flag (privacy toggle). Mirrors a DB update.
    pub fn set_shared(&self, shared: bool) {
        self.shared.store(shared, Ordering::Relaxed);
    }

    /// Update the viewport class. Called by WS connections on `SetViewportClass`.
    pub fn set_viewport_class(&self, vc: ViewportClass) {
        *self
            .viewport_class
            .lock()
            .expect("viewport_class lock poisoned") = vc;
    }

    /// Get the current viewport class (last reported by any client).
    pub(super) fn get_viewport_class(&self) -> ViewportClass {
        *self
            .viewport_class
            .lock()
            .expect("viewport_class lock poisoned")
    }

    /// Broadcast a message to all subscribed WS connections.
    pub(crate) fn broadcast(&self, msg: WsServerMessage) {
        // send() fails only when there are zero receivers. This is expected —
        // the bridge stays alive even with no tabs connected (CC keeps running).
        if self.event_tx.send(msg).is_err() {
            tracing::debug!("broadcast with no receivers (expected when no tabs connected)");
        }
    }

    /// Broadcast a `UserMessageEcho` (chat-input origin) to all tabs for
    /// multi-tab sync. Called by `routes/ws.rs` after persisting a human
    /// chat message.
    pub fn broadcast_user_echo(&self, echo: WsServerMessage) {
        self.broadcast(echo);
    }

    /// Send a pre-rendered system message: persist to DB (with `rendered_html`
    /// and `system_category`), broadcast to browsers as a `SystemMessageBroadcast`
    /// carrying the collapsed-card HTML, and send the LLM-facing `text` to CC.
    ///
    /// This is the **only** sender for non-chat-input rows. The variant tag on
    /// `SystemMessageBroadcast` is the discriminator; it cannot be produced
    /// without `rendered_html` and `category` because `SystemMessageRender`
    /// carries them as required fields.
    ///
    /// `attribute_to_user_id`:
    /// - `None` → attribute to the conversation owner (default for cats 1–7).
    /// - `Some(uid)` → attribute to that user in `messages.sender_user_id`
    ///   (used by cat 7 so DB joins on the requesting human remain correct).
    pub async fn send_system_message(
        &self,
        rendered: crate::system_message::SystemMessageRender,
        attribute_to_user_id: Option<i64>,
    ) -> Result<(), String> {
        let user_id_for_db = attribute_to_user_id.unwrap_or(self.user_id);
        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": rendered.text},
            "rendered_html": rendered.rendered_html,
            "system_category": rendered.category,
        });
        self.persist_broadcast_send(
            &payload.to_string(),
            &rendered.text,
            user_id_for_db,
            rendered.rendered_html,
            rendered.category,
            "send_system_message",
            "failed to send system message to CC",
        )
        .await
    }

    /// Persist a system message row, broadcast a `SystemMessageBroadcast` to
    /// all attached browsers, and send `cc_text` to the CC subprocess.
    ///
    /// The delivered-marking point is post-flush (design §2.2, D1 fix): the
    /// session lock is released after enqueue so interactive ops on this bridge
    /// (chat, approvals, compaction) are never blocked by the flush await, while
    /// FIFO stdin order is fixed at enqueue time by the single writer task.
    #[allow(clippy::too_many_arguments)]
    async fn persist_broadcast_send(
        &self,
        payload_json: &str,
        cc_text: &str,
        sender_user_id: i64,
        rendered_html: String,
        category: SystemMessageCategory,
        cc_busy_reason: &'static str,
        cc_send_err_prefix: &'static str,
    ) -> Result<(), String> {
        // 1. Persist and capture the DB seq for live-broadcast dedup.
        let db_seq = {
            let conn = self.db.lock().await;
            let (_id, seq) = conversation::append_message(
                &conn,
                self.conversation_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                payload_json,
                Some(sender_user_id),
                None, // no timezone for system-generated messages
                None, // no device for system-generated messages
            );
            seq
        };

        // 2. Broadcast with the DB seq so attached tabs can deduplicate
        //    against history replay (B.2).
        let timestamp = brenn_lib::db::format_ts_for_db(chrono::Utc::now());
        self.broadcast(WsServerMessage::SystemMessageBroadcast {
            rendered_html,
            category,
            timestamp,
            seq: Some(db_seq),
        });

        // 3. Enqueue to CC and acquire the flush-ack receiver.
        //    Acquire session lock, enqueue (fixing FIFO position in the writer),
        //    then DROP the lock before awaiting the ack. This ensures concurrent
        //    callers (user chat, approvals, compaction) on this bridge are not
        //    blocked while CC processes the stdin bytes (design §2.6).
        let ack_rx = {
            let session = self.session.lock().await;
            let session = session
                .as_ref()
                .ok_or_else(|| "CC session is not running".to_string())?;
            if !session.is_alive() {
                return Err("CC session has died".to_string());
            }
            // Enqueue and capture the ack receiver before releasing the lock.
            // Set busy on successful enqueue: the message is FIFO-positioned in the
            // writer queue and CC will process it (or the session will die, resetting
            // cc_idle via the session-death path). If the flush later fails, the push
            // row stays delivered_at IS NULL (R3) — a redelivery may come in a future
            // session. Setting busy here is safe: cc_idle is always reset by session
            // death before a new session starts, so a flush failure does not leave
            // cc_idle permanently stuck.
            let rx = session
                .send_message_acked(cc_text)
                .await
                .map_err(|e| format!("{cc_send_err_prefix}: {e}"))?;
            self.set_cc_busy(cc_busy_reason);
            rx
            // session guard drops here — lock released before the flush await.
        };

        // 4. Await the flush ack with no timeout (design §2.6, Q1 resolution).
        //    CC crash/kill → broken pipe → writer errors → ack resolves Err →
        //    caller leaves the row delivered_at IS NULL → redelivered on restart.
        //    A permanently-hung-but-alive CC parks this await harmlessly: the
        //    lock is free, other bridges are isolated, and the row stays parked.
        ack_rx
            .await
            .map_err(|_| format!("{cc_send_err_prefix}: writer task exited before flush"))?
            .map_err(|e| format!("{cc_send_err_prefix}: flush failed: {e}"))
    }

    /// Persist a system message to DB and broadcast to attached browsers, but do
    /// NOT send to CC. Used when the caller will fold the CC text into a combined
    /// multi-block message (e.g., device slug reminder batched with a user message).
    pub async fn persist_and_broadcast_system_message(
        &self,
        rendered: crate::system_message::SystemMessageRender,
        attribute_to_user_id: Option<i64>,
    ) {
        let user_id_for_db = attribute_to_user_id.unwrap_or(self.user_id);
        let payload = serde_json::json!({
            "type": "user",
            "message": {"role": "user", "content": rendered.text},
            "rendered_html": rendered.rendered_html,
            "system_category": rendered.category,
        });
        let payload_json = payload.to_string();
        let db_seq = {
            let conn = self.db.lock().await;
            let (_id, seq) = conversation::append_message(
                &conn,
                self.conversation_id,
                MessageDirection::Outgoing,
                "user",
                None,
                None,
                &payload_json,
                Some(user_id_for_db),
                None, // no timezone for system-generated messages
                None, // no device for system-generated messages
            );
            seq
        };
        let timestamp = brenn_lib::db::format_ts_for_db(chrono::Utc::now());
        self.broadcast(WsServerMessage::SystemMessageBroadcast {
            rendered_html: rendered.rendered_html,
            category: rendered.category,
            timestamp,
            seq: Some(db_seq),
        });
    }

    /// Send an interrupt to CC (stop current generation).
    /// CC will finish gracefully and emit a `result` message.
    pub async fn interrupt(&self) -> Result<(), String> {
        let session = self.session.lock().await;
        let session = session
            .as_ref()
            .ok_or_else(|| "CC session is not running".to_string())?;

        if !session.is_alive() {
            return Err("CC session has died".to_string());
        }

        session
            .interrupt()
            .await
            .map_err(|e| format!("failed to interrupt CC: {e}"))
    }

    /// Check if the CC session is still alive.
    pub async fn is_alive(&self) -> bool {
        let session = self.session.lock().await;
        session.as_ref().is_some_and(|s| s.is_alive())
    }

    /// Persist a user-sent message with optional attachments.
    ///
    /// Returns `(msg_id, db_seq)`. The `db_seq` is needed by the caller to stamp
    /// the `UserMessageEcho` broadcast with `seq: Some(db_seq)` for frontend dedup.
    pub async fn persist_user_message_with_attachments(
        &self,
        text: &str,
        sender_user_id: i64,
        sender_tz: Option<&str>,
        sender_device_id: Option<i64>,
        make_attachments: impl FnOnce(i64) -> Vec<conversation::StoredAttachment>,
    ) -> (i64, i64) {
        let payload =
            serde_json::json!({"type": "user", "message": {"role": "user", "content": text}});
        let conn = self.db.lock().await;
        let (msg_id, seq) = conversation::append_message(
            &conn,
            self.conversation_id,
            MessageDirection::Outgoing,
            "user",
            None,
            None,
            &payload.to_string(),
            Some(sender_user_id),
            sender_tz,
            sender_device_id,
        );
        let attachments = make_attachments(msg_id);
        if !attachments.is_empty() {
            conversation::insert_attachments(&conn, &attachments);
        }
        (msg_id, seq)
    }
}

/// Persist an incoming CC message against a borrowed connection and return its DB seq.
///
/// Sync inner used by `handle_turn_completed` inside the unified DB lock scope.
/// The seq is required so callers can stamp live broadcasts with `seq: Some(db_seq)`,
/// enabling frontend deduplication against history replay for the reconnect-from-idle
/// race fix (see docs/adr/2026/05/06-system-message-race/).
pub(super) fn persist_incoming_message_sync<T: serde::Serialize>(
    conn: &rusqlite::Connection,
    conversation_id: i64,
    msg_type: &str,
    uuid: Option<&str>,
    parent_tool_use_id: Option<&str>,
    payload: &T,
) -> i64 {
    let payload_json = serde_json::to_string(payload).expect("CC message payload must serialize");
    let (_id, seq) = conversation::append_message(
        conn,
        conversation_id,
        MessageDirection::Incoming,
        msg_type,
        uuid,
        parent_tool_use_id,
        &payload_json,
        None,
        None,
        None,
    );
    seq
}

/// Persist an incoming CC message and return its DB seq.
///
/// Async wrapper for non-turn-end callers (cc_event_loop). The turn-end path
/// uses `persist_incoming_message_sync` directly inside the unified DB scope.
pub(super) async fn persist_incoming_message<T: serde::Serialize>(
    bridge: &ActiveBridge,
    msg_type: &str,
    uuid: Option<&str>,
    parent_tool_use_id: Option<&str>,
    payload: &T,
) -> i64 {
    let conn = bridge.db.lock().await;
    persist_incoming_message_sync(
        &conn,
        bridge.conversation_id,
        msg_type,
        uuid,
        parent_tool_use_id,
        payload,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    use super::super::test_support::{
        create_test_device_for_user, test_bridge, test_bridge_singleton,
        test_bridge_with_failing_session,
    };

    #[tokio::test]
    async fn set_model_dedup_skips_redundant_calls() {
        // When last_set_model matches the requested model, set_model should
        // return Ok without touching the CC session.
        let (_bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // First call fails because test bridge has no CC session.
        let result = _bridge.set_model("sonnet").await;
        assert!(result.is_err(), "should fail with no CC session");

        // Manually set last_set_model to simulate a previous successful call.
        *_bridge.last_set_model.lock().await = Some("sonnet".to_string());

        // Now the same model should return Ok (skipped, no session access).
        let result = _bridge.set_model("sonnet").await;
        assert!(result.is_ok(), "should skip redundant set_model");

        // A different model should try to call CC and fail (no session).
        let result = _bridge.set_model("opus").await;
        assert!(result.is_err(), "different model should attempt CC call");

        // last_set_model should still be "sonnet" since the opus call failed.
        assert_eq!(
            _bridge.last_set_model.lock().await.as_deref(),
            Some("sonnet"),
            "failed set_model should not update last_set_model"
        );
    }

    #[tokio::test]
    async fn send_message_does_not_set_busy_when_send_fails() {
        // Covers the `.inspect(|_| self.set_cc_busy("send_outgoing"))` guard at
        // bridge_io.rs:99 — the closure must not fire when send_outgoing returns Err.
        // The dummy session has is_alive()=true but a closed channel, so it hits the
        // exact race the guard protects against.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_failing_session().await;
        bridge.cc_idle.store(true, Ordering::SeqCst);
        let result = bridge.send_message("hello").await;
        assert!(result.is_err(), "send must fail on dummy-session bridge");
        assert!(
            bridge.cc_idle.load(Ordering::SeqCst),
            "cc_idle must remain true; set_cc_busy must not fire on Err from send_outgoing"
        );
    }

    #[tokio::test]
    async fn send_message_does_not_set_busy_when_session_dead() {
        // send_message must check session aliveness before flipping cc_idle.
        // If `set_cc_busy` ran on a dead-session path, cc_idle would stick at
        // false with no turn ever completing to restore it. Start from `true`
        // (the only direction that actually discriminates: a stray
        // `set_cc_busy` would flip it to false).
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        bridge.cc_idle.store(true, Ordering::SeqCst);
        let err = bridge.send_message("hello").await;
        assert!(err.is_err(), "send should fail on test bridge (no session)");
        assert!(
            bridge.cc_idle.load(Ordering::SeqCst),
            "cc_idle must remain true; set_cc_busy must not run on dead-session path"
        );
    }

    #[tokio::test]
    async fn send_system_message_persists_rendered_html_and_category() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;

        let render = crate::system_message::render_compaction_reminder(75);
        // Test bridge has no real CC session so the send to CC fails, but
        // persistence + broadcast happen first.
        let _ = bridge.send_system_message(render, None).await;

        // ── Persistence assertions ───────────────────────────────────────────
        let conn = bridge.db.lock().await;
        let messages = conversation::get_messages(&conn, bridge.conversation_id);
        let row = messages.last().expect("at least one row");
        assert_eq!(row.msg_type, "user");
        let payload: serde_json::Value =
            serde_json::from_str(&row.payload).expect("payload is JSON");
        assert!(
            payload["rendered_html"].is_string(),
            "rendered_html present: {payload}"
        );
        assert_eq!(
            payload["system_category"],
            serde_json::Value::String("CompactionReminder".to_string()),
        );
        assert!(
            payload["message"]["content"]
                .as_str()
                .unwrap()
                .contains("75%"),
            "text contains usage_pct: {payload}"
        );
        drop(conn);

        // ── Broadcast assertions ─────────────────────────────────────────────
        // The broadcast must be `SystemMessageBroadcast` (not `UserMessageEcho`)
        // — this pins the wire-variant split that is the core structural fix.
        let mut found = false;
        while let Ok(msg) = broadcast_rx.try_recv() {
            if let WsServerMessage::SystemMessageBroadcast {
                rendered_html,
                category,
                ..
            } = msg
            {
                assert!(
                    !rendered_html.is_empty(),
                    "broadcast carries non-empty rendered_html"
                );
                assert_eq!(
                    category,
                    SystemMessageCategory::CompactionReminder,
                    "broadcast carries correct category"
                );
                found = true;
                break;
            }
        }
        assert!(
            found,
            "expected SystemMessageBroadcast from send_system_message, \
             not UserMessageEcho or missing broadcast"
        );
    }

    #[tokio::test]
    async fn send_system_message_attributes_to_specified_user() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;

        // Create a second, non-owner user.
        let other_uid = {
            let conn = bridge.db.lock().await;
            brenn_lib::auth::user::create_user(&conn, "other", "$argon2id$fake")
        };

        let render = crate::system_message::render_compaction_reminder(80);
        let _ = bridge.send_system_message(render, Some(other_uid)).await;

        // Persisted row attributed to the specified user, NOT the owner.
        let conn = bridge.db.lock().await;
        let messages = conversation::get_messages(&conn, bridge.conversation_id);
        let row = messages.last().unwrap();
        assert_eq!(
            row.sender_user_id,
            Some(other_uid),
            "row attributed to specified user"
        );
        assert_ne!(
            row.sender_user_id,
            Some(bridge.user_id),
            "not attributed to conversation owner"
        );
        drop(conn);

        // Broadcast is a SystemMessageBroadcast (variant tag discriminates
        // system-origin messages from chat-input echoes). It carries
        // `rendered_html` + `category`; there is no `username` field on
        // this variant — system origin is implicit in the type.
        let mut found = false;
        while let Ok(msg) = broadcast_rx.try_recv() {
            if let WsServerMessage::SystemMessageBroadcast {
                rendered_html,
                category,
                ..
            } = msg
            {
                assert!(
                    !rendered_html.is_empty(),
                    "broadcast carries non-empty rendered_html"
                );
                assert_eq!(
                    category,
                    SystemMessageCategory::CompactionReminder,
                    "broadcast carries correct category"
                );
                found = true;
                break;
            }
        }
        assert!(found, "expected SystemMessageBroadcast broadcast");
    }

    /// Live `SystemMessageBroadcast` broadcasts carry `seq: Some(_)` so that
    /// attached tabs can deduplicate against history replay (B.2).
    #[tokio::test]
    async fn persist_broadcast_send_emits_some_seq() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;

        let render = crate::system_message::render_compaction_reminder(75);
        let _ = bridge.send_system_message(render, None).await;

        let mut found_seq: Option<Option<i64>> = None;
        while let Ok(msg) = broadcast_rx.try_recv() {
            if let WsServerMessage::SystemMessageBroadcast { seq, .. } = msg {
                found_seq = Some(seq);
                break;
            }
        }
        let seq_val = found_seq.expect("expected a SystemMessageBroadcast broadcast");
        assert!(
            seq_val.is_some(),
            "live SystemMessageBroadcast must carry seq: Some(_), got None"
        );

        // The seq returned in the broadcast must match the DB row.
        let conn = bridge.db.lock().await;
        let messages = conversation::get_messages(&conn, bridge.conversation_id);
        let row = messages.last().unwrap();
        assert_eq!(
            Some(row.seq),
            seq_val,
            "broadcast seq must match the persisted DB row seq"
        );
    }

    #[tokio::test]
    async fn send_system_message_attributes_to_owner_when_none() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;

        let render = crate::system_message::render_compaction_reminder(82);
        let _ = bridge.send_system_message(render, None).await;

        let conn = bridge.db.lock().await;
        let messages = conversation::get_messages(&conn, bridge.conversation_id);
        let row = messages.last().unwrap();
        assert_eq!(
            row.sender_user_id,
            Some(bridge.user_id),
            "row attributed to conversation owner when attribute_to_user_id=None"
        );
    }

    #[tokio::test]
    async fn send_cc_only_system_text_sends_to_session() {
        // Success path: text must reach the CC session and cc_idle must flip to false.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let mut rx = super::super::test_support::install_recording_session(&bridge).await;
        bridge.cc_idle.store(true, Ordering::SeqCst);

        let result = bridge.send_cc_only_system_text("lint error text").await;
        assert!(result.is_ok(), "must succeed with live recording session");
        assert!(
            !bridge.cc_idle.load(Ordering::SeqCst),
            "cc_idle must flip to false after successful send"
        );

        // The outgoing channel must have received a User message containing our text.
        let msg = rx.try_recv().expect("outgoing channel must have a message");
        assert_eq!(
            super::super::test_support::user_text(&msg),
            "lint error text",
            "text sent to CC must match the input"
        );
    }

    #[tokio::test]
    async fn send_cc_only_system_text_dead_session_returns_err() {
        // With no CC session (`test_bridge` has no real session), the method
        // must return Err and must NOT set cc_busy.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        bridge.cc_idle.store(true, Ordering::SeqCst);

        let result = bridge.send_cc_only_system_text("test text").await;
        assert!(result.is_err(), "must fail with no CC session");
        assert!(
            bridge.cc_idle.load(Ordering::SeqCst),
            "cc_idle must stay true when session is absent"
        );
    }

    #[tokio::test]
    async fn send_cc_only_system_text_failing_session_returns_err() {
        // With a session whose channel is closed (failing session), the method
        // must return Err and must NOT flip cc_idle.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_failing_session().await;
        bridge.cc_idle.store(true, Ordering::SeqCst);

        let result = bridge.send_cc_only_system_text("test text").await;
        assert!(result.is_err(), "must fail on dummy-session bridge");
        assert!(
            bridge.cc_idle.load(Ordering::SeqCst),
            "cc_idle must remain true; set_cc_busy must not fire on Err from send"
        );
    }

    #[tokio::test]
    async fn send_cc_only_system_text_dead_alive_flag_returns_err() {
        // Covers the `is_alive() == false` guard at bridge_io.rs:58-60.
        // Install a live recording session, then mark it dead via the alive flag
        // (simulating a session that died but whose Option has not yet been cleared).
        // The method must return Err without sending and must NOT flip cc_idle.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let mut rx = super::super::test_support::install_recording_session(&bridge).await;
        bridge.cc_idle.store(true, Ordering::SeqCst);

        // Mark the session dead via the test-only helper (simulates a session
        // that exited without the Option wrapper being cleared yet).
        {
            let guard = bridge.session.lock().await;
            if let Some(session) = guard.as_ref() {
                session.mark_dead_for_test();
            }
        }

        let result = bridge.send_cc_only_system_text("must not reach CC").await;
        assert!(result.is_err(), "must fail when is_alive() == false");
        assert!(
            bridge.cc_idle.load(Ordering::SeqCst),
            "cc_idle must stay true when is_alive() guard fires"
        );
        assert!(
            rx.try_recv().is_err(),
            "must not send to CC when session is dead"
        );
    }

    #[tokio::test]
    async fn send_outgoing_cancels_waiting_for_idle() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        super::super::test_support::set_waiting_for_idle(&bridge).await;

        // Call send_outgoing directly (not via send_message) to pin the postcondition
        // at the send_outgoing API boundary. It will fail (no CC session) but the
        // WaitingForIdle branch at bridge_io.rs:82-88 executes before the session check.
        let _ = bridge
            .send_outgoing(brenn_cc::protocol::builders::user_message("direct send"))
            .await;

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "send_outgoing must reset phase to Normal when entry phase is WaitingForIdle"
        );
        assert!(
            state.idle_timer.is_none(),
            "send_outgoing must clear idle_timer when entry phase is WaitingForIdle"
        );
    }

    #[tokio::test]
    async fn persist_user_message_writes_sender_device_id() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let device_id =
            create_test_device_for_user(&bridge.db, bridge.user_id, "Mozilla/5.0 Chrome/125").await;

        bridge
            .persist_user_message_with_attachments(
                "hello",
                bridge.user_id,
                None,
                Some(device_id),
                |_msg_id| vec![],
            )
            .await;

        let conn = bridge.db.lock().await;
        let sender_device_id: Option<i64> = conn
            .query_row(
                "SELECT sender_device_id FROM messages WHERE conversation_id = ?1 ORDER BY id DESC LIMIT 1",
                rusqlite::params![bridge.conversation_id],
                |row| row.get(0),
            )
            .expect("should find message row");
        assert_eq!(
            sender_device_id,
            Some(device_id),
            "messages.sender_device_id must match the device that sent the message"
        );
    }

    // -------------------------------------------------------------------------
    // Acceptance 8 — flush-stall isolation (R11, design §2.6)
    // -------------------------------------------------------------------------

    /// Acceptance 8 — flush-stall isolation (design §4 item 8, R11).
    ///
    /// While `persist_broadcast_send` is awaiting a held-back flush ack (simulating
    /// an alive-but-stalled CC writer), the `bridge.session` lock must be FREE so a
    /// concurrent `send_cc_only_system_text` on the same bridge is not serialized
    /// behind the stalled dispatch.
    ///
    /// This validates the "enqueue under lock, release lock, then await ack" guarantee
    /// from design §2.6 and R11(a).
    ///
    /// Test structure:
    ///   1. Install a stalling session (sends succeed; ack is not auto-fired).
    ///   2. Spawn task A: call `send_system_message` — blocks awaiting the ack.
    ///   3. Let task A enter `persist_broadcast_send` and release the session lock.
    ///   4. From the main task, call `send_cc_only_system_text` with a short timeout.
    ///      If the lock had been held, this would time out. Assert it succeeds promptly.
    ///   5. Release the ack sender → task A completes with Ok(()).
    ///   6. Assert task A's result is Ok(()).
    #[tokio::test]
    async fn persist_broadcast_send_releases_lock_before_ack_await() {
        use std::time::Duration;
        use tokio::time::timeout;

        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        let mut rx = super::super::test_support::install_stalling_session(&bridge).await;
        bridge.cc_idle.store(true, Ordering::SeqCst);

        // Spawn task A: send_system_message calls persist_broadcast_send which
        // enqueues, drops the lock, then awaits the ack (will block until we fire it).
        let bridge_clone = bridge.clone();
        let task_a = tokio::spawn(async move {
            let render = crate::system_message::render_compaction_reminder(50);
            bridge_clone.send_system_message(render, None).await
        });

        // Receive the envelope from the stalling session channel.
        // This confirms task A has enqueued the message and released the session lock.
        let envelope = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for task A to enqueue its message")
            .expect("session channel closed unexpectedly");

        // The envelope must carry an ack sender (stalling session, auto_ack=false).
        let ack_tx = envelope
            .ack
            .expect("stalling session must place ack:Some in the envelope");

        // Task A is now blocked awaiting ack_rx. The session lock must be FREE.
        // Send a second message on the same bridge — must not block behind task A.
        let second_result = timeout(
            Duration::from_millis(500),
            bridge.send_cc_only_system_text("concurrent send while task A awaits ack"),
        )
        .await
        .expect(
            "send_cc_only_system_text timed out — session lock was NOT released before the ack \
             await; design §2.6 lock-release guarantee is violated",
        );
        // The second send must succeed (session is alive, channel has capacity).
        assert!(
            second_result.is_ok(),
            "concurrent send_cc_only_system_text must succeed while task A awaits its ack: \
             {second_result:?}"
        );

        // Release ack: task A unblocks and returns Ok(()).
        ack_tx
            .send(Ok(()))
            .expect("task A must still be awaiting the ack");
        let task_a_result = timeout(Duration::from_secs(2), task_a)
            .await
            .expect("task A timed out after ack was released")
            .expect("task A panicked");
        assert!(
            task_a_result.is_ok(),
            "send_system_message must return Ok(()) after ack resolves Ok: {task_a_result:?}"
        );
        // NOTE: this test validates same-bridge lock release but does not test cross-bridge
        // fan-out isolation (a stall on bridge A must not delay bridge B's dispatch task).
        // The cross-bridge isolation comes from per-bridge tokio::spawn in dispatcher_loop.
        // That property is tested by `dispatcher_loop_cross_bridge_isolation` below.
    }

    /// Cross-bridge fan-out isolation (R11b): a delivery stall on bridge A must not
    /// delay delivery to bridge B.
    ///
    /// The isolation mechanism is the per-group `tokio::spawn` in `dispatcher_loop`
    /// (dispatcher.rs:300–335). Two bridges = two distinct subscriber keys = two
    /// independent spawned tasks. This test drives the real `dispatcher_loop` via
    /// `spawn_dispatcher_task` to exercise that structure directly.
    ///
    /// Setup:
    ///   - One shared `ActiveBridges` registry.
    ///   - Bridge A: stalling session (ack withheld; delivery parks at the ack await).
    ///   - Bridge B: recording session (auto-acks; captures every `OutgoingEnvelope`).
    ///
    /// Regression-sensitivity (deterministic): rows are inserted in two scans, not one.
    /// Scan 1 contains only A's row; once A's fan-out task is confirmed to have started
    /// (rx_a receives A's envelope), B's row is inserted and kick fires scan 2. Under
    /// the inline-join regression, scan 1 blocks forever awaiting A's handle, scan 2
    /// never starts, rx_b never returns — deterministic hang regardless of HashMap
    /// iteration order. A single-scan approach with both rows would be only
    /// probabilistically sensitive (if B's group iterates first, B delivers before A
    /// blocks the loop).
    ///
    /// Pass condition: `rx_b.recv().await` returns (no timeout). Returns iff
    /// `dispatcher_loop` spawned B's fan-out task independently of A's.
    #[tokio::test]
    async fn dispatcher_loop_cross_bridge_isolation() {
        use std::sync::Arc;

        use brenn_lib::messaging::config::{
            Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig,
            ResolvedSubscription, Sink,
        };
        use brenn_lib::messaging::db::upsert_channels;
        use brenn_lib::messaging::db::{PendingPushInsert, insert_message_with_pushes, utc_to_ns};
        use brenn_lib::messaging::dispatcher::spawn_dispatcher_task;
        use brenn_lib::messaging::query::NoopWakeRouter;
        use brenn_lib::messaging::{
            ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, SubscriberEntry,
            SubscriberEntryKind, Urgency, WakeMin, canonical_address,
        };
        use brenn_lib::messaging::{ParticipantId, WakeRouter};
        use chrono::Utc;
        use indexmap::IndexMap;
        use uuid::Uuid;

        use crate::active_bridge::test_fixtures::TestBridgeConfig;
        use crate::active_bridge::{ActiveBridge, ActiveBridges};
        use crate::messaging_router::WakeRouterImpl;
        use crate::test_support::app_config::default_test_app_config;

        // ------------------------------------------------------------------
        // 1. Shared DB + channel setup.
        // ------------------------------------------------------------------
        let db = brenn_lib::db::init_db_memory();

        // Two conversations (A and B) with distinct app slugs.
        let (conv_id_a, conv_id_b) = {
            let conn = db.lock().await;
            // Insert a user.
            let uid = brenn_lib::auth::user::create_user(&conn, "testuser", "$argon2id$fake");
            let cid_a = brenn_lib::conversation::create_conversation(&conn, uid, "app-a", false);
            let cid_b = brenn_lib::conversation::create_conversation(&conn, uid, "app-b", false);
            (cid_a, cid_b)
        };

        // Channel A: subscribed by "app-a".
        let channel_uuid_a = Uuid::new_v4();
        let channel_addr_a = canonical_address("xb-ch-a");
        // Channel B: subscribed by "app-b".
        let channel_uuid_b = Uuid::new_v4();
        let channel_addr_b = canonical_address("xb-ch-b");

        // TODO(wasm-messenger-test-helper): mk_entry is the 4th inline construction of a
        // ChannelEntry with this shape (cc_event_loop.rs:3241, messaging_router.rs:560,
        // bridge_io.rs here). Extract to a shared test helper when consolidating
        // the Messenger setup pattern.
        let mk_entry = |uuid: Uuid, addr: &str, app: &str| ChannelEntry {
            uuid,
            address: addr.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App(app.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };

        {
            let conn = db.lock().await;
            let entry_a = mk_entry(channel_uuid_a, &channel_addr_a, "app-a");
            let entry_b = mk_entry(channel_uuid_b, &channel_addr_b, "app-b");
            upsert_channels(&conn, &[entry_a.clone(), entry_b.clone()]);
        }

        // ------------------------------------------------------------------
        // 2. Build the shared ActiveBridges registry + two bridges.
        //    make_bridge_no_loop allocates a fresh in-memory DB per call, so
        //    we use inject_for_test_full directly with the shared DB.
        // ------------------------------------------------------------------
        let shared_registry = ActiveBridges::new();

        let (broadcast_tx_a, _broadcast_rx_a) = tokio::sync::broadcast::channel(16);
        let bridge_a = ActiveBridge::inject_for_test_full(
            1,
            conv_id_a,
            "app-a",
            db.clone(),
            broadcast_tx_a,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(shared_registry.clone()),
                ..Default::default()
            },
        );

        let (broadcast_tx_b, _broadcast_rx_b) = tokio::sync::broadcast::channel(16);
        let bridge_b = ActiveBridge::inject_for_test_full(
            1,
            conv_id_b,
            "app-b",
            db.clone(),
            broadcast_tx_b,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(shared_registry.clone()),
                ..Default::default()
            },
        );

        // ------------------------------------------------------------------
        // 3. Install sessions: A stalls (ack withheld), B records and auto-acks.
        // ------------------------------------------------------------------
        let mut rx_a = super::super::test_support::install_stalling_session(&bridge_a).await;
        let mut rx_b = super::super::test_support::install_recording_session(&bridge_b).await;

        // Insert both bridges into the shared registry.
        shared_registry.insert(conv_id_a, bridge_a).await;
        shared_registry.insert(conv_id_b, bridge_b).await;

        // ------------------------------------------------------------------
        // 4. Build WakeRouterImpl over the shared registry and a minimal Messenger.
        //    The Messenger is only needed for register_released_pushes (deliver-after
        //    rows); our Immediate rows never trigger that path. Use a NoopWakeRouter
        //    as the Messenger's internal router to avoid circular dependency.
        // ------------------------------------------------------------------
        let dir = MessagingDirectory::with_entries(vec![
            mk_entry(channel_uuid_a, &channel_addr_a, "app-a"),
            mk_entry(channel_uuid_b, &channel_addr_b, "app-b"),
        ]);

        let mk_app_config = |slug: &str, ch_uuid: Uuid, ch_addr: &str| {
            let mut cfg = default_test_app_config(slug, slug);
            cfg.messaging = Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid: ch_uuid,
                    channel_address: ch_addr.to_string(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                }],
            });
            // The dispatcher's delivery-time ACL floor (design §2.2 Point B) now
            // re-authorizes every parked push against the app's policy; without a
            // covering matcher the row would be denied and never delivered. Stamp
            // the covering delivery policy for this app's channel.
            cfg.policy = crate::test_support::app_config::delivery_policy_for_addresses([ch_addr]);
            cfg
        };
        let mut apps: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
        apps.insert(
            "app-a".to_string(),
            mk_app_config("app-a", channel_uuid_a, &channel_addr_a),
        );
        apps.insert(
            "app-b".to_string(),
            mk_app_config("app-b", channel_uuid_b, &channel_addr_b),
        );

        let internal_router: Arc<dyn WakeRouter> = Arc::new(NoopWakeRouter);
        let messenger = Messenger::new(
            db.clone(),
            Arc::new(dir),
            Arc::from("test"),
            Arc::new(apps),
            internal_router,
            MessagingGlobalConfig::default(),
        );

        let router = Arc::new(WakeRouterImpl::new(shared_registry));
        for slug in ["app-a", "app-b"] {
            router.register_delivery_binding(
                brenn_lib::messaging::SubscriberEntryKind::App(slug.to_string()),
                crate::messaging_router::DeliveryBinding::ConversationBridge,
            );
        }
        let kick = messenger.dispatch_kick_notify();

        // Spawn the real dispatcher loop (the code under test).
        let _dispatcher_handle = spawn_dispatcher_task(
            db.clone(),
            router as Arc<dyn WakeRouter>,
            kick.clone(),
            messenger,
        );

        // ------------------------------------------------------------------
        // 5. Scan 1: insert only A's row and kick. Wait for A's fan-out task to start
        //    (confirmed by rx_a receiving A's envelope). This ensures A's task is
        //    genuinely parked at the ack await before B's row is introduced.
        //
        //    Deterministic regression sensitivity: under the inline-join regression,
        //    scan 1 processes A's group and then blocks awaiting A's join handle. A's
        //    fan-out task still enqueues A's envelope in rx_a (the stalling session
        //    enqueues before awaiting the ack), so rx_a.recv() returns even under the
        //    regression. But the loop is now stuck — it never reaches scan 2.
        // ------------------------------------------------------------------
        {
            let conn = db.lock().await;
            let sub_a = ParticipantId::for_conversation(conv_id_a);
            let ts = utc_to_ns(Utc::now());
            insert_message_with_pushes(
                &conn,
                channel_uuid_a,
                "host",
                "sender",
                "msg-for-a",
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None, // no release_after → immediately dispatchable
                ts,
                &[PendingPushInsert {
                    target_subscriber: sub_a,
                    target_app_slug: "app-a".to_string(),
                    eager_wake: true,
                    release_after: None,
                    delivery_deadline: None,
                }],
            );
        }
        kick.notify_one();

        // Confirm A's fan-out task started and is parked at the ack await.
        // 2s timeout is a hung-guard (not a correctness margin): A's envelope arrives
        // almost immediately once the dispatcher scans and spawns A's fan-out task.
        let envelope_a = tokio::time::timeout(std::time::Duration::from_secs(2), rx_a.recv())
            .await
            .expect("bridge A must receive its message within 2s (hung-guard)")
            .expect("stalling session channel closed");

        // ------------------------------------------------------------------
        // 6. Scan 2: insert B's row and kick. A's fan-out task is parked (ack withheld).
        //    Under the inline-join regression, the loop is stuck waiting for A's handle
        //    and never processes scan 2 — rx_b never returns → harness-killed (FAIL).
        //    With the correct independent-spawn behavior, B's fan-out task spawns from
        //    scan 2, B's recording session auto-acks, and rx_b returns (PASS).
        // ------------------------------------------------------------------
        {
            let conn = db.lock().await;
            let sub_b = ParticipantId::for_conversation(conv_id_b);
            let ts = utc_to_ns(Utc::now());
            insert_message_with_pushes(
                &conn,
                channel_uuid_b,
                "host",
                "sender",
                "msg-for-b",
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None, // no release_after
                ts,
                &[PendingPushInsert {
                    target_subscriber: sub_b,
                    target_app_slug: "app-b".to_string(),
                    eager_wake: true,
                    release_after: None,
                    delivery_deadline: None,
                }],
            );
        }
        kick.notify_one();

        // Pass condition (no timeout): returns iff the loop spawned B's fan-out task
        // independently of A's. Under the inline-join regression this never returns.
        let _outgoing_b: brenn_cc::session::OutgoingEnvelope = rx_b
            .recv()
            .await
            .expect("bridge B's recording session must deliver the message");

        // Release A's withheld ack, confirming A was genuinely parked (not absent)
        // during B's delivery.
        let ack_tx = envelope_a
            .ack
            .expect("stalling session must place ack:Some in the envelope");
        ack_tx
            .send(Ok(()))
            .expect("dispatcher fan-out task must still be awaiting the ack");
    }

    #[tokio::test]
    async fn persist_incoming_message_sync_pins_argument_wiring() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let conn = bridge.db.lock().await;

        let seq = persist_incoming_message_sync(
            &conn,
            bridge.conversation_id,
            "test-msg-type",
            Some("test-uuid-123"),
            Some("test-parent-tool-use-456"),
            &serde_json::json!({"key": "value"}),
        );

        // test_bridge() inserts no messages, so the first inserted row gets seq 0.
        assert_eq!(seq, 0, "first inserted message must get seq 0");

        let msgs = conversation::get_messages_from(&conn, bridge.conversation_id, seq);
        let row = msgs
            .first()
            .expect("message row must exist at returned seq");

        assert_eq!(
            row.direction,
            conversation::MessageDirection::Incoming,
            "direction must be incoming"
        );
        assert_eq!(
            row.msg_type, "test-msg-type",
            "msg_type must match argument"
        );
        assert_eq!(
            row.cc_uuid,
            Some("test-uuid-123".to_string()),
            "cc_uuid column must receive the uuid argument, not parent_tool_use_id"
        );
        assert_eq!(
            row.parent_tool_use_id,
            Some("test-parent-tool-use-456".to_string()),
            "parent_tool_use_id column must receive the parent_tool_use_id argument, not uuid"
        );
    }
}

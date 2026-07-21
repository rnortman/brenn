//! `handle_client_message` — the `match msg` dispatch on `WsClientMessage`.

use brenn_lib::obs::security::{
    SecurityEventType, log_and_alert_security_event, log_and_alert_ssrf_attempt,
};
use brenn_lib::usage::EventType;
use brenn_lib::ws_types::{PushClickTraceEvent, WsClientMessage, WsServerMessage};
use tracing::{debug, error, info, warn};

use super::connection::{QueuedResponse, WsConnection};

pub(super) async fn handle_client_message(
    text: &str,
    conn: &mut WsConnection,
    client_ip: std::net::IpAddr,
) {
    // Device unenrollment for already-open WS sessions: the middleware
    // rejects unenrolled devices on new connections (the sentinel-token
    // overwrite at the auth layer invalidates the cookie before any new WS
    // connection can be established). For sessions established before the
    // unenroll, the operator restarts the server to tear them down. No
    // per-message DB check is needed.
    //
    // TODO(unenroll-live-session-teardown): two gaps remain. (1) Already-open
    // WS sessions from an unenrolled device continue to dispatch all message
    // variants until server restart. (2) `resolve_or_create_device` creates a
    // new device row for the same authenticated user when the sentinel-token
    // blocks the old one, so a new WS connection can still be established.
    // Closing both gaps requires §13 live-session registry (to drop the
    // broadcast channel for the affected WS task on unenroll) and/or
    // `sessions`-table revocation in `unenroll_device`. See also
    // `unenroll-cc-bridge-gate` for the parallel CC-tool-call gap.

    let msg: WsClientMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            // Discriminant check: if the raw JSON parses as a Value and looks
            // like a PushClickTrace outer shape, this is a schema-drift failure
            // (stale SW emitting a PushClickTrace variant with wrong/missing
            // inner fields post-deploy), not attacker traffic. Log without
            // security_event = true so fail2ban ignores it.
            //
            // We require both the top-level `"type": "PushClickTrace"` AND
            // the presence of an `"event"` key whose value is a JSON object
            // with a `"type"` string field. Requiring the inner shape raises
            // the bar for an attacker trying to abuse the carve-out: a bare
            // `{"type":"PushClickTrace","event":"garbage"}` still triggers
            // the security path, limiting bypass to payloads that structurally
            // resemble genuine drift.
            const PUSH_CLICK_TRACE_TYPE: &str = "PushClickTrace";
            let is_push_trace_drift = serde_json::from_str::<serde_json::Value>(text)
                .ok()
                .is_some_and(|v| {
                    v.get("type").and_then(|t| t.as_str()) == Some(PUSH_CLICK_TRACE_TYPE)
                        && v.get("event")
                            .and_then(|e| e.as_object())
                            .and_then(|o| o.get("type"))
                            .and_then(|t| t.as_str())
                            .is_some()
                });

            if is_push_trace_drift {
                warn!(
                    push_trace_schema_drift = true,
                    error = %e,
                    "PushClickTrace deserialization failed — likely stale SW schema drift"
                );
            } else {
                log_and_alert_security_event(
                    &conn.state.alert_dispatcher,
                    SecurityEventType::MalformedMessage,
                    client_ip,
                    &format!("malformed WS message: {e}"),
                );
            }
            let _ = conn.send_ws(WsServerMessage::Error {
                message: "Invalid message format".to_string(),
            });
            return;
        }
    };

    match msg {
        WsClientMessage::SendMessage {
            text,
            attachments,
            model,
            selected_tasks,
        } => {
            // record_usage after handle_send_message so current_conversation_id is set
            // (handle_send_message → resolve_bridge → wake_conversation sets it).
            // Guard on the return value: on validation/SSRF rejection paths the message
            // was never dispatched and recording a send_message event would inflate counts.
            let dispatched = conn
                .handle_send_message(&text, attachments, model.as_deref(), selected_tasks)
                .await;
            if dispatched {
                conn.record_usage(EventType::SendMessage).await;
            }
        }
        WsClientMessage::PermissionResponse {
            request_id,
            decision,
        } => {
            if let Some(conv_id) = conn.current_conversation_id {
                if let Some(bridge) = conn.state.active_bridges.get(conv_id).await {
                    bridge
                        .handle_permission_response(&request_id, decision)
                        .await;
                } else {
                    tracing::info!(
                        request_id = %request_id,
                        "no active bridge, queuing permission response"
                    );
                    conn.queued_responses.push(QueuedResponse::Permission {
                        request_id,
                        decision,
                    });
                    conn.ensure_bridge_starting(conv_id).await;
                }
            }
        }
        WsClientMessage::ToolCardResponse {
            request_id,
            decision,
        } => {
            if let Some(conv_id) = conn.current_conversation_id {
                if let Some(bridge) = conn.state.active_bridges.get(conv_id).await {
                    bridge
                        .handle_tool_card_response(&request_id, decision)
                        .await;
                } else {
                    tracing::info!(
                        request_id = %request_id,
                        "no active bridge, queuing tool card response"
                    );
                    conn.queued_responses.push(QueuedResponse::ToolCard {
                        request_id,
                        decision,
                    });
                    conn.ensure_bridge_starting(conv_id).await;
                }
            }
        }
        WsClientMessage::ClientError { message } => {
            // Cap the logged payload: any legitimate error (stack trace + message)
            // fits in 4 KiB. Oversize payloads are a fail2ban signal independent
            // of rate-limiting, so they are always logged with client_ip before the
            // bucket check — an attacker who exhausts the bucket then sends large
            // payloads must not be able to suppress the IP-tagged warning.
            const MAX_CLIENT_ERROR_BYTES: usize = 4 * 1024;
            let oversized = message.len() > MAX_CLIENT_ERROR_BYTES;
            if oversized {
                warn!(
                    client_ip = %client_ip,
                    len = message.len(),
                    "ClientError message exceeds size limit; dropping"
                );
            }
            // All messages — oversized or not — pass through the rate bucket so
            // the suppressed count accurately reflects total suppressed volume.
            if conn.client_error_bucket.try_consume(client_ip) && !oversized {
                error!(client_error = ?message, "frontend reported protocol error");
            }
        }
        WsClientMessage::PushClickTrace { user_id, event } => match event {
            PushClickTraceEvent::HandlerEntry {
                ref target_user_id,
                ref target_path,
                ref redirector_url,
                ref payload_keys,
            } => {
                info!(
                    event_type = "HandlerEntry",
                    trace_user_id = user_id,
                    target_user_id = ?target_user_id,
                    target_path = %target_path,
                    redirector_url = %redirector_url,
                    payload_keys = ?payload_keys,
                    "service worker push-click trace"
                );
            }
            PushClickTraceEvent::MatchAllResult { ref clients } => {
                info!(
                    event_type = "MatchAllResult",
                    trace_user_id = user_id,
                    client_count = clients.len(),
                    clients = ?clients,
                    "service worker push-click trace"
                );
            }
            PushClickTraceEvent::BrennClientsFilter {
                ref kept,
                ref dropped_with_reason,
            } => {
                info!(
                    event_type = "BrennClientsFilter",
                    trace_user_id = user_id,
                    kept = ?kept,
                    dropped_with_reason = ?dropped_with_reason,
                    "service worker push-click trace"
                );
            }
            PushClickTraceEvent::T1Chosen {
                ref client_id,
                ref target_path,
                focus_rejected,
            } => {
                info!(
                    event_type = "T1Chosen",
                    trace_user_id = user_id,
                    client_id = %client_id,
                    target_path = %target_path,
                    focus_rejected,
                    "service worker push-click trace"
                );
            }
            PushClickTraceEvent::T1Skipped { ref reason } => {
                info!(
                    event_type = "T1Skipped",
                    trace_user_id = user_id,
                    reason = %reason,
                    "service worker push-click trace"
                );
            }
            PushClickTraceEvent::OpenWindowCalled { ref url } => {
                info!(
                    event_type = "OpenWindowCalled",
                    trace_user_id = user_id,
                    url = %url,
                    "service worker push-click trace"
                );
            }
            PushClickTraceEvent::OpenWindowResult { ref opened_url } => {
                info!(
                    event_type = "OpenWindowResult",
                    trace_user_id = user_id,
                    opened_url = ?opened_url,
                    "service worker push-click trace"
                );
            }
            PushClickTraceEvent::FenixCascadeSkipped => {
                info!(
                    event_type = "FenixCascadeSkipped",
                    trace_user_id = user_id,
                    "Fenix detected — skipping focus-existing cascade"
                );
            }
            PushClickTraceEvent::Terminal { branch } => {
                info!(
                    event_type = "Terminal",
                    trace_user_id = user_id,
                    branch = ?branch,
                    "service worker push-click trace"
                );
            }
        },
        WsClientMessage::ListConversations => {
            if !conn.app_config().singleton {
                conn.handle_list_conversations().await;
            }
            // Singleton apps: no-op — there's no conversation list.
        }
        WsClientMessage::SwitchConversation { conversation_id } => {
            if conn.app_config().singleton {
                warn!(
                    app = %conn.app_slug,
                    conversation_id,
                    "SwitchConversation rejected — singleton app"
                );
                let _ = conn.send_ws(WsServerMessage::Error {
                    message: "Cannot switch conversations in a singleton app".to_string(),
                });
            } else {
                conn.record_usage(EventType::SwitchConversation).await;
                conn.handle_switch_conversation(conversation_id).await;
            }
        }
        WsClientMessage::NewConversation => {
            if conn.app_config().singleton {
                warn!(app = %conn.app_slug, "NewConversation rejected — singleton app");
                let _ = conn.send_ws(WsServerMessage::Error {
                    message: "Cannot create new conversations in a singleton app".to_string(),
                });
            } else {
                conn.record_usage(EventType::NewConversation).await;
                conn.handle_new_conversation().await;
            }
        }
        WsClientMessage::Reconnect {
            conversation_id,
            last_seq,
        } => {
            if conn.app_config().singleton && conn.current_conversation_id != Some(conversation_id)
            {
                warn!(
                    app = %conn.app_slug,
                    conversation_id,
                    "Reconnect to wrong conversation rejected — singleton app"
                );
                let _ = conn.send_ws(WsServerMessage::Error {
                    message: "Cannot switch conversations in a singleton app".to_string(),
                });
            } else {
                conn.handle_reconnect(conversation_id, last_seq).await;
            }
        }
        WsClientMessage::ReopenArtifact {
            file_path,
            message_id,
        } => {
            conn.handle_reopen_artifact(&file_path, message_id).await;
        }
        WsClientMessage::LoadArtifactSnapshot { message_id } => {
            conn.handle_load_artifact_snapshot(message_id).await;
        }
        WsClientMessage::StealApp => {
            conn.handle_steal_app().await;
        }
        WsClientMessage::SetTimezone { timezone } => match timezone.parse::<chrono_tz::Tz>() {
            Ok(tz) => conn.timezone = tz,
            Err(_) => {
                warn!(timezone = %timezone, "invalid IANA timezone from client, keeping UTC");
            }
        },
        WsClientMessage::SetDeviceInfo {
            user_agent,
            platform,
            screen_width,
            screen_height,
        } => {
            let conn_db = conn.state.db.lock().await;
            brenn_lib::auth::device::update_device_info(
                &conn_db,
                conn.device_id,
                &user_agent,
                &platform,
                screen_width,
                screen_height,
            );
        }
        WsClientMessage::StopRequest => {
            conn.record_usage(EventType::StopRequest).await;
            conn.handle_stop_request().await;
        }
        WsClientMessage::SetViewportClass { viewport_class } => {
            debug!(user = %conn.username, ?viewport_class, "viewport class updated");
            conn.viewport_class = viewport_class;
            conn.send_layout().await;
            // Update the bridge's viewport class so approval rendering uses it.
            if let Some(conv_id) = conn.current_conversation_id
                && let Some(bridge) = conn.state.active_bridges.get(conv_id).await
            {
                bridge.set_viewport_class(viewport_class);
            }
        }
        WsClientMessage::SetConversationPrivacy {
            conversation_id,
            shared,
        } => {
            conn.record_usage(EventType::SetConversationPrivacy).await;
            conn.handle_set_conversation_privacy(conversation_id, shared)
                .await;
        }
        WsClientMessage::RunTarget { target, upload_ids } => {
            conn.record_usage(EventType::RunTarget).await;
            if let Some(handle) = conn.handle_run_target(&target, &upload_ids).await {
                let user_id = conn.user_id;
                let app_slug = conn.app_slug.clone();
                let conversation_id = conn.current_conversation_id;
                tokio::spawn(async move {
                    if let Err(e) = handle.await {
                        error!(
                            user_id,
                            app_slug = %app_slug,
                            conversation_id = ?conversation_id,
                            "run_target_task panicked: {:?}", e
                        );
                    }
                });
            }
        }
        WsClientMessage::RequestCompaction => {
            conn.record_usage(EventType::RequestCompaction).await;
            conn.handle_request_compaction().await;
        }

        WsClientMessage::TodoRefresh => {
            conn.record_usage(EventType::TodoRefresh).await;
            conn.handle_todo_refresh().await;
        }
        WsClientMessage::TodoDone {
            path,
            repo,
            completion_date,
        } => {
            conn.record_usage(EventType::TodoDone).await;
            conn.handle_todo_done(&path, repo.as_deref(), completion_date)
                .await;
        }
        WsClientMessage::TodoSchedule { path, repo, date } => {
            conn.record_usage(EventType::TodoSchedule).await;
            conn.handle_todo_schedule(&path, repo.as_deref(), date)
                .await;
        }
        WsClientMessage::TodoReorder {
            path,
            repo,
            after,
            before,
        } => {
            conn.record_usage(EventType::TodoReorder).await;
            conn.handle_todo_reorder(
                &path,
                repo.as_deref(),
                after.as_ref().map(|a| (a.path.as_str(), a.repo.as_deref())),
                before
                    .as_ref()
                    .map(|a| (a.path.as_str(), a.repo.as_deref())),
            )
            .await;
        }
        WsClientMessage::LoadMoreHistory { before_seq } => {
            conn.handle_load_more_history(before_seq).await;
        }

        // PWA Push subscription lifecycle handlers.
        //
        // Precondition for all three: the active app must have
        // `pwa_push.enabled = true`. Messages from non-gated apps are rejected
        // with a security event (logged at fail2ban-feed level).
        WsClientMessage::PushVapidKeyRequest => {
            let app_config = conn.app_config();
            if !app_config.pwa_push_enabled() {
                log_and_alert_security_event(
                    &conn.state.alert_dispatcher,
                    SecurityEventType::SchemaViolation,
                    client_ip,
                    &format!(
                        "PushVapidKeyRequest from non-gated app {:?} (user {})",
                        conn.app_slug, conn.user_id
                    ),
                );
                return;
            }
            // pwa_push is enabled → the service must be present.
            let pwa_push = conn.state.pwa_push.as_ref().expect(
                "app has the PwaPush policy grant but AppState.pwa_push is None — startup bug",
            );
            let _ = conn.send_ws(WsServerMessage::PushVapidKey {
                public_key_b64url: pwa_push.public_key_b64url().to_string(),
            });
        }
        WsClientMessage::PushSubscribe {
            endpoint,
            p256dh,
            auth,
        } => {
            let app_config = conn.app_config();
            if !app_config.pwa_push_enabled() {
                log_and_alert_security_event(
                    &conn.state.alert_dispatcher,
                    SecurityEventType::SchemaViolation,
                    client_ip,
                    &format!(
                        "PushSubscribe from non-gated app {:?} (user {})",
                        conn.app_slug, conn.user_id
                    ),
                );
                return;
            }
            // pwa_push is enabled → the service must be present.
            let pwa_push = conn.state.pwa_push.as_ref().expect(
                "app has the PwaPush policy grant but AppState.pwa_push is None — startup bug",
            );
            // Validate the wire fields (p256dh, auth, endpoint).
            // Endpoint rejections (SSRF) are a distinct threat — emit SsrfAttempt at Critical.
            // Key-format rejections (p256dh/auth) continue as MalformedMessage at Warning.
            // validate_push_subscribe_fields returns a ValidatedEndpoint wrapping
            // the url::Url-normalized URL; pass that directly to upsert_subscription.
            let normalized_endpoint = match brenn_lib::pwa_push::db::validate_push_subscribe_fields(
                &endpoint,
                &p256dh,
                &auth,
                pwa_push.endpoint_policy(),
            ) {
                Ok(normalized) => normalized,
                Err(brenn_lib::pwa_push::db::SubscribeValidationError::Endpoint(ref reason)) => {
                    // Extract host prefix for triage: parse the raw endpoint to get the host;
                    // fall back to an endpoint prefix if parse fails.
                    let host_hint = url::Url::parse(&endpoint)
                        .ok()
                        .and_then(|u| u.host_str().map(|h| h.chars().take(32).collect::<String>()))
                        .unwrap_or_else(|| brenn_lib::pwa_push::endpoint_preview(&endpoint));
                    let detail = format!(
                        "PushSubscribe endpoint reject: reason={} host_hint={} user_id={} device_id={}",
                        reason.code(),
                        host_hint,
                        conn.user_id,
                        conn.device_id,
                    );
                    log_and_alert_ssrf_attempt(&conn.state.alert_dispatcher, client_ip, &detail);
                    return;
                }
                Err(e) => {
                    log_and_alert_security_event(
                        &conn.state.alert_dispatcher,
                        SecurityEventType::MalformedMessage,
                        client_ip,
                        &format!(
                            "PushSubscribe malformed fields from user {}: {e}",
                            conn.user_id
                        ),
                    );
                    return;
                }
            };
            // Upsert the subscription row using the normalized endpoint.
            let db = conn.state.db.lock().await;
            brenn_lib::pwa_push::db::upsert_subscription(
                &db,
                conn.device_id,
                conn.user_id,
                &normalized_endpoint,
                &p256dh,
                &auth,
            );
            drop(db);
            // Confirm current subscription state to the client.
            let _ = conn.send_ws(WsServerMessage::PushEnabled { enabled: true });
            debug!(
                user = %conn.username,
                device_id = conn.device_id,
                "PushSubscribe: subscription upserted"
            );
        }
        WsClientMessage::PushUnsubscribe => {
            let app_config = conn.app_config();
            if !app_config.pwa_push_enabled() {
                log_and_alert_security_event(
                    &conn.state.alert_dispatcher,
                    SecurityEventType::SchemaViolation,
                    client_ip,
                    &format!(
                        "PushUnsubscribe from non-gated app {:?} (user {})",
                        conn.app_slug, conn.user_id
                    ),
                );
                return;
            }
            let db = conn.state.db.lock().await;
            brenn_lib::pwa_push::db::delete_subscription(&db, conn.device_id, conn.user_id);
            drop(db);
            let _ = conn.send_ws(WsServerMessage::PushEnabled { enabled: false });
            debug!(
                user = %conn.username,
                device_id = conn.device_id,
                "PushUnsubscribe: subscription deleted"
            );
        }
        WsClientMessage::DebugViewportSnapshot { snapshot } => {
            // Diagnostic, not usage-billable — no `record_usage`/`EventType`
            // on purpose (the design explicitly excludes usage accounting for
            // debug snapshots).
            conn.handle_debug_viewport_snapshot(snapshot).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering as AtomicOrdering;

    use brenn_lib::auth::user::create_user;
    use brenn_lib::conversation;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::ws_types::{
        DebugViewportSnapshotData, ViewportClass, WsClientMessage, WsServerMessage,
    };
    use tokio::sync::{broadcast, mpsc};

    use super::super::connection::WsConnection;
    use super::super::testing::*;
    use super::handle_client_message;
    use crate::active_bridge::ActiveBridge;
    use crate::state::AppState;

    // -----------------------------------------------------------------------
    // Idle-hook timer plumbing through the WS dispatch
    //
    // Verifies the design's "Defining 'idle'" / "Changes to routes/ws.rs"
    // contract: UI-channel handlers cancel-and-rearm the bridge's idle-hook
    // timer; non-UI messages don't touch it; ToolCardResponse routes via
    // `inject_tool_result_to_cc` (CC-channel), no separate UI touch.
    //
    // Approach: arm the bridge's hook timer, snapshot the JoinHandle id,
    // dispatch a message, then snapshot again. Cancel-and-rearm produces a
    // *different* JoinHandle id; "no touch" leaves the id unchanged.
    // -----------------------------------------------------------------------

    /// Test-only IdleHook: always pending, returns `None` from `check`.
    /// Sufficient for the "is the timer armed?" plumbing tests below.
    struct StubHook {
        pending: AtomicBool,
    }

    impl crate::idle_hooks::IdleHook for StubHook {
        fn name(&self) -> &str {
            "stub"
        }
        fn check<'a>(
            &'a self,
            _bridge: &'a ActiveBridge,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Option<serde_json::Value>> + Send + 'a>,
        > {
            Box::pin(async { None })
        }
        fn on_delivered(&self) {}
        fn on_resolved(&self) {}
        fn has_pending_work(&self) -> bool {
            self.pending.load(AtomicOrdering::SeqCst)
        }
    }

    /// Build a `WsConnection` with a live test bridge wired up via
    /// `state.active_bridges`, idle-hook timer enabled, and a stub hook
    /// registered. Returns `(conn, ws_rx, bridge)` so tests can read
    /// `bridge.idle_hook_timer_handle_id_for_test()` on either side of a
    /// dispatch.
    async fn ws_conn_with_idle_hook_bridge() -> (
        WsConnection,
        mpsc::Receiver<WsServerMessage>,
        Arc<ActiveBridge>,
    ) {
        let db = init_db_memory();
        let state = AppState::for_test(db.clone(), None);

        let (ws_tx, ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(64);

        let (user_id, conv_id, device_id) = {
            let conn = db.lock().await;
            let uid = create_user(&conn, "ihtest", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "ih", false);
            let did = create_test_device(&conn, uid);
            (uid, cid, did)
        };

        // Big enough to never fire during the test; small enough that
        // arming succeeds (gate is `> 0`).
        let bridge = ActiveBridge::inject_for_test_with_idle_hook_secs(
            user_id,
            conv_id,
            "test",
            db,
            broadcast_tx,
            3600,
            vec![],
        );
        bridge.register_idle_hook(Arc::new(StubHook {
            pending: AtomicBool::new(true),
        }));
        bridge.maybe_arm_idle_hook_timer().await;
        assert!(
            bridge.idle_hook_timer_handle_id_for_test().is_some(),
            "precondition: timer should be armed before dispatch",
        );

        state.active_bridges.insert(conv_id, bridge.clone()).await;

        let conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            ..WsConnBuilder::with_defaults(
                user_id,
                "ihtest".to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        (conn, ws_rx, bridge)
    }

    /// Assert: dispatching `msg_json` cancels-and-rearms the timer.
    /// Cancel-and-rearm produces a different `JoinHandle` id.
    async fn assert_cancel_and_rearm(msg_json: &str) {
        let (mut conn, _ws_rx, bridge) = ws_conn_with_idle_hook_bridge().await;
        let before = bridge
            .idle_hook_timer_handle_id_for_test()
            .expect("timer armed at start");
        handle_client_message(msg_json, &mut conn, TEST_CLIENT_IP).await;
        let after = bridge.idle_hook_timer_handle_id_for_test();
        assert!(
            after.is_some(),
            "{msg_json}: timer must be re-armed (CC was idle)"
        );
        assert_ne!(
            after.unwrap(),
            before,
            "{msg_json}: cancel-and-rearm must produce a fresh handle"
        );
    }

    /// Assert: dispatching `msg_json` does NOT touch the timer (same
    /// handle id before and after).
    async fn assert_does_not_touch_timer(msg_json: &str) {
        let (mut conn, _ws_rx, bridge) = ws_conn_with_idle_hook_bridge().await;
        let before = bridge
            .idle_hook_timer_handle_id_for_test()
            .expect("timer armed at start");
        handle_client_message(msg_json, &mut conn, TEST_CLIENT_IP).await;
        let after = bridge
            .idle_hook_timer_handle_id_for_test()
            .expect("timer must remain armed");
        assert_eq!(
            after, before,
            "{msg_json}: handler must not cancel-and-rearm the timer"
        );
    }

    #[tokio::test]
    async fn ui_handler_todo_refresh_cancels_and_rearms() {
        assert_cancel_and_rearm(r#"{"type":"TodoRefresh"}"#).await;
    }

    #[tokio::test]
    async fn ui_handler_todo_done_cancels_and_rearms() {
        assert_cancel_and_rearm(
            r#"{"type":"TodoDone","path":"todo/x.md","completion_date":"2026-04-22"}"#,
        )
        .await;
    }

    #[tokio::test]
    async fn ui_handler_todo_schedule_cancels_and_rearms() {
        assert_cancel_and_rearm(
            r#"{"type":"TodoSchedule","path":"todo/x.md","date":"2026-04-22"}"#,
        )
        .await;
    }

    #[tokio::test]
    async fn ui_handler_todo_reorder_cancels_and_rearms() {
        // `before` anchor satisfies the at-least-one-anchor rule.
        assert_cancel_and_rearm(
            r#"{"type":"TodoReorder","path":"todo/x.md","before":{"path":"todo/y.md"}}"#,
        )
        .await;
    }

    #[tokio::test]
    async fn ui_handler_reopen_artifact_cancels_and_rearms() {
        assert_cancel_and_rearm(r#"{"type":"ReopenArtifact","file_path":"foo.md"}"#).await;
    }

    #[tokio::test]
    async fn ui_handler_load_artifact_snapshot_cancels_and_rearms() {
        assert_cancel_and_rearm(r#"{"type":"LoadArtifactSnapshot","message_id":1}"#).await;
    }

    #[tokio::test]
    async fn non_ui_set_viewport_class_does_not_touch_timer() {
        assert_does_not_touch_timer(r#"{"type":"SetViewportClass","viewport_class":"Compact"}"#)
            .await;
    }

    #[tokio::test]
    async fn non_ui_set_timezone_does_not_touch_timer() {
        assert_does_not_touch_timer(r#"{"type":"SetTimezone","timezone":"Asia/Tokyo"}"#).await;
    }

    #[tokio::test]
    async fn non_ui_list_conversations_does_not_touch_timer() {
        assert_does_not_touch_timer(r#"{"type":"ListConversations"}"#).await;
    }

    #[tokio::test]
    async fn non_ui_reconnect_does_not_touch_timer_when_same_conversation() {
        // Reconnect to the existing current conversation. The handler
        // re-resolves history but does not call `touch_ui_activity`. We
        // can't easily cover the "different conversation" path without
        // a richer fixture, but the same-conversation case proves the
        // dispatch arm doesn't carry a UI-touch.
        let (mut conn, _ws_rx, bridge) = ws_conn_with_idle_hook_bridge().await;
        let conv_id = conn.current_conversation_id.unwrap();
        let before = bridge
            .idle_hook_timer_handle_id_for_test()
            .expect("timer armed at start");
        let msg = format!(r#"{{"type":"Reconnect","conversation_id":{conv_id},"last_seq":null}}"#);
        handle_client_message(&msg, &mut conn, TEST_CLIENT_IP).await;
        let after = bridge
            .idle_hook_timer_handle_id_for_test()
            .expect("timer remains armed");
        assert_eq!(
            after, before,
            "Reconnect dispatch must not cancel-and-rearm the hook timer"
        );
    }

    #[tokio::test]
    async fn tool_card_response_unknown_request_does_not_touch_timer() {
        // `ToolCardResponse` reaches the bridge via `inject_tool_result_to_cc`
        // (CC-channel) and trips `set_cc_busy` only when there is a real
        // pending request. With an unknown request_id, the bridge early-
        // returns without `set_cc_busy`. The dispatch arm itself does not
        // call `touch_ui_activity` (this is the contract under test):
        // observed by the timer state being unchanged.
        assert_does_not_touch_timer(
            r#"{"type":"ToolCardResponse","request_id":"unknown","decision":{"type":"Allow"}}"#,
        )
        .await;
    }

    // -----------------------------------------------------------------------
    // Device infrastructure tests (design §4, §2.4–2.9)
    // -----------------------------------------------------------------------

    /// `SetDeviceInfo` updates the device row's platform, user_agent, screen
    /// dimensions, and last_seen_at.
    #[tokio::test]
    async fn set_device_info_updates_row() {
        let (conn, _ws_rx, db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::update_device_info(
                &db_conn,
                conn.device_id,
                "Mozilla/5.0 (X11; Linux x86_64) Chrome/125",
                "Linux x86_64",
                1920,
                1080,
            );
        }

        let row = {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::load_device(&db_conn, conn.device_id)
        };
        assert_eq!(
            row.user_agent.as_deref(),
            Some("Mozilla/5.0 (X11; Linux x86_64) Chrome/125")
        );
        assert_eq!(row.platform.as_deref(), Some("Linux x86_64"));
        assert_eq!(row.screen_width, Some(1920));
        assert_eq!(row.screen_height, Some(1080));
    }

    /// `update_device_info` drops screen dimensions outside `1..=100000`.
    #[tokio::test]
    async fn set_device_info_drops_absurd_screen() {
        let (conn, _ws_rx, db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        // Store a valid value first.
        {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::update_device_info(
                &db_conn,
                conn.device_id,
                "ua",
                "Linux",
                1920,
                1080,
            );
        }

        // Now send an absurd width — previously stored value should be retained.
        {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::update_device_info(
                &db_conn,
                conn.device_id,
                "ua",
                "Linux",
                1_000_000,
                1080,
            );
        }

        let row = {
            let db_conn = db.lock().await;
            brenn_lib::auth::device::load_device(&db_conn, conn.device_id)
        };
        assert_eq!(
            row.screen_width,
            Some(1920),
            "absurd width must not overwrite valid stored width"
        );
    }

    // -----------------------------------------------------------------------
    // PWA Push WS handler tests (design §2.6.2, §4.2)
    // -----------------------------------------------------------------------

    /// `PushVapidKeyRequest` from a pwa_push-enabled app returns the VAPID public key.
    #[tokio::test]
    async fn push_vapid_key_request_returns_public_key() {
        let (mut conn, mut ws_rx, _db, _uid, pwa_push) = test_ws_conn_with_pwa_push().await;

        handle_client_message(
            r#"{"type":"PushVapidKeyRequest"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        let msg = ws_rx.try_recv().expect("PushVapidKey must be sent");
        match msg {
            WsServerMessage::PushVapidKey { public_key_b64url } => {
                assert_eq!(
                    public_key_b64url,
                    pwa_push.public_key_b64url(),
                    "returned public key must match stored VAPID keypair"
                );
            }
            other => panic!("expected PushVapidKey, got {other:?}"),
        }
    }

    /// `PushVapidKeyRequest` from an app with `pwa_push = false` is rejected
    /// with a security event (no response sent).
    #[tokio::test]
    async fn push_vapid_key_request_from_non_gated_app_is_rejected() {
        // Default test_apps() has pwa_push: None (disabled).
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        handle_client_message(
            r#"{"type":"PushVapidKeyRequest"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        // No message should have been sent.
        assert!(
            ws_rx.try_recv().is_err(),
            "no WS message must be sent from non-gated app"
        );
    }

    /// `PushSubscribe` with valid fields upserts the subscription row and
    /// sends `PushEnabled { enabled: true }`.
    #[tokio::test]
    async fn push_subscribe_upserts_row_and_sends_enabled() {
        let (mut conn, mut ws_rx, db, _uid, _pwa_push) = test_ws_conn_with_pwa_push().await;

        let msg = format!(
            r#"{{"type":"PushSubscribe","endpoint":"https://push.example.com/sub","p256dh":"{}","auth":"{}"}}"#,
            fake_p256dh(),
            fake_auth(),
        );
        handle_client_message(&msg, &mut conn, TEST_CLIENT_IP).await;

        // Subscription row must exist.
        let exists = {
            let db_conn = db.lock().await;
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
        };
        assert!(
            exists,
            "subscription row must be created after PushSubscribe"
        );

        // PushEnabled { enabled: true } must be sent.
        let msg = ws_rx.try_recv().expect("PushEnabled must be sent");
        assert!(
            matches!(msg, WsServerMessage::PushEnabled { enabled: true }),
            "expected PushEnabled(true), got {msg:?}"
        );
    }

    /// `PushSubscribe` with a bad `p256dh` (wrong byte length) is rejected
    /// with a security event and no subscription row is created.
    #[tokio::test]
    async fn push_subscribe_validates_p256dh_byte_length() {
        let (mut conn, mut ws_rx, db, _uid, _pwa_push) = test_ws_conn_with_pwa_push().await;

        use base64ct::{Base64UrlUnpadded, Encoding as _};
        // Only 32 bytes (wrong length — must be 65 for uncompressed P-256).
        let short_p256dh = Base64UrlUnpadded::encode_string(&[0u8; 32]);

        let msg = format!(
            r#"{{"type":"PushSubscribe","endpoint":"https://push.example.com/sub","p256dh":"{short_p256dh}","auth":"{}"}}"#,
            fake_auth(),
        );
        handle_client_message(&msg, &mut conn, TEST_CLIENT_IP).await;

        // No subscription row should be created.
        let exists = {
            let db_conn = db.lock().await;
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
        };
        assert!(
            !exists,
            "no subscription row must be created on validation failure"
        );

        // No WS message should be sent.
        assert!(
            ws_rx.try_recv().is_err(),
            "no WS message on validation failure"
        );
    }

    /// `PushSubscribe` with a bad `auth` (wrong byte length) is rejected.
    #[tokio::test]
    async fn push_subscribe_validates_auth_byte_length() {
        let (mut conn, mut ws_rx, db, _uid, _pwa_push) = test_ws_conn_with_pwa_push().await;

        use base64ct::{Base64UrlUnpadded, Encoding as _};
        // Only 8 bytes (wrong length — must be 16).
        let short_auth = Base64UrlUnpadded::encode_string(&[0u8; 8]);

        let msg = format!(
            r#"{{"type":"PushSubscribe","endpoint":"https://push.example.com/sub","p256dh":"{}","auth":"{short_auth}"}}"#,
            fake_p256dh(),
        );
        handle_client_message(&msg, &mut conn, TEST_CLIENT_IP).await;

        let exists = {
            let db_conn = db.lock().await;
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
        };
        assert!(!exists, "no subscription row on bad auth length");
        assert!(ws_rx.try_recv().is_err(), "no WS message on bad auth");
    }

    /// `PushSubscribe` from an app with `pwa_push = false` is rejected.
    #[tokio::test]
    async fn push_subscribe_with_app_pwa_push_off_logs_security_event_and_drops() {
        let (mut conn, mut ws_rx, db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        let msg = format!(
            r#"{{"type":"PushSubscribe","endpoint":"https://push.example.com/sub","p256dh":"{}","auth":"{}"}}"#,
            fake_p256dh(),
            fake_auth(),
        );
        handle_client_message(&msg, &mut conn, TEST_CLIENT_IP).await;

        let exists = {
            let db_conn = db.lock().await;
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
        };
        assert!(!exists, "no subscription from non-gated app");
        assert!(
            ws_rx.try_recv().is_err(),
            "no WS message from non-gated app"
        );
    }

    /// `PushSubscribe` with a private-IP endpoint is rejected as `SsrfAttempt`:
    /// no subscription row is written and no WS message is sent.
    ///
    /// The fixture uses unenforced empty policy, which still blocks private IPs
    /// via the IP-block rules — no fixture override needed.
    #[tokio::test]
    async fn push_subscribe_ssrf_endpoint_rejected_no_row_written() {
        let (mut conn, mut ws_rx, db, _uid, _pwa_push) = test_ws_conn_with_pwa_push().await;

        let msg = format!(
            r#"{{"type":"PushSubscribe","endpoint":"https://127.0.0.1/push","p256dh":"{}","auth":"{}"}}"#,
            fake_p256dh(),
            fake_auth(),
        );
        handle_client_message(&msg, &mut conn, TEST_CLIENT_IP).await;

        // No subscription row must be created.
        let exists = {
            let db_conn = db.lock().await;
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
        };
        assert!(
            !exists,
            "SSRF endpoint must not result in a subscription row"
        );

        // No WS message must be sent (silent drop; we never confirm to the attacker).
        assert!(
            ws_rx.try_recv().is_err(),
            "no WS message must be sent for SSRF endpoint rejection"
        );
    }

    /// `PushUnsubscribe` deletes the subscription row and sends `PushEnabled { enabled: false }`.
    #[tokio::test]
    async fn push_unsubscribe_deletes_row() {
        let (mut conn, mut ws_rx, db, _uid, _pwa_push) = test_ws_conn_with_pwa_push().await;

        // Insert a subscription first.
        {
            let db_conn = db.lock().await;
            brenn_lib::pwa_push::db::upsert_subscription(
                &db_conn,
                conn.device_id,
                conn.user_id,
                &brenn_lib::pwa_push::endpoint_validator::ValidatedEndpoint::for_testing(
                    "https://push.example.com/sub",
                ),
                &fake_p256dh(),
                &fake_auth(),
            );
        }

        handle_client_message(r#"{"type":"PushUnsubscribe"}"#, &mut conn, TEST_CLIENT_IP).await;

        let exists = {
            let db_conn = db.lock().await;
            brenn_lib::pwa_push::db::subscription_exists(&db_conn, conn.device_id, conn.user_id)
        };
        assert!(
            !exists,
            "subscription row must be deleted after PushUnsubscribe"
        );

        let msg = ws_rx.try_recv().expect("PushEnabled must be sent");
        assert!(
            matches!(msg, WsServerMessage::PushEnabled { enabled: false }),
            "expected PushEnabled(false), got {msg:?}"
        );
    }

    // -----------------------------------------------------------------------
    // PushClickTrace dispatch — coverage gate for the trace arm
    //
    // PushClickTrace is a logging-only dispatch arm (no WS response). These
    // tests verify the dispatch arm is wired correctly and does not panic on
    // valid payloads. If the arm is accidentally deleted, these tests fail.
    // -----------------------------------------------------------------------

    /// `PushClickTrace { T1Chosen, focus_rejected: false }` dispatches without
    /// panic and emits no WS response (trace is logging-only).
    #[tokio::test]
    async fn push_click_trace_t1chosen_focus_not_rejected_no_panic_no_response() {
        let (mut conn, mut ws_rx, _db, uid, _pwa_push) = test_ws_conn_with_pwa_push().await;
        let msg = format!(
            r#"{{"type":"PushClickTrace","user_id":{uid},"event":{{"type":"T1Chosen","client_id":"abc","target_path":"/app/graf/","focus_rejected":false}}}}"#
        );
        handle_client_message(&msg, &mut conn, TEST_CLIENT_IP).await;
        assert!(
            ws_rx.try_recv().is_err(),
            "PushClickTrace must not send a WS response"
        );
    }

    /// `PushClickTrace { T1Chosen, focus_rejected: true }` dispatches without
    /// panic and emits no WS response.
    #[tokio::test]
    async fn push_click_trace_t1chosen_focus_rejected_no_panic_no_response() {
        let (mut conn, mut ws_rx, _db, uid, _pwa_push) = test_ws_conn_with_pwa_push().await;
        let msg = format!(
            r#"{{"type":"PushClickTrace","user_id":{uid},"event":{{"type":"T1Chosen","client_id":"abc","target_path":"/app/graf/","focus_rejected":true}}}}"#
        );
        handle_client_message(&msg, &mut conn, TEST_CLIENT_IP).await;
        assert!(
            ws_rx.try_recv().is_err(),
            "PushClickTrace must not send a WS response"
        );
    }

    // -----------------------------------------------------------------------
    // Schema-drift signal — PushClickTrace vs. attacker traffic classification
    //
    // Verifies that a PushClickTrace-shaped message with invalid inner fields
    // does NOT trigger a security event (fail2ban-feed), while genuinely
    // malformed messages still do.
    //
    // The "schema-drift" carve-out requires the outer shape to look like a
    // genuine PushClickTrace: top-level "type" == "PushClickTrace" AND an
    // "event" key whose value is a JSON object with a "type" string field.
    // Payloads lacking the inner event-object shape fall through to the
    // security path (see production code comment for rationale).
    //
    // -----------------------------------------------------------------------
    //
    /// Build a WsConnection whose AppState uses a capturing alert dispatcher.
    /// Returns `(conn, ws_rx, captured_alerts, alert_handle)`.
    ///
    /// The broadcast receiver is intentionally dropped (consistent with
    /// test_ws_conn_with_resume_conv_and_apps). These tests never reach a
    /// broadcast-send path; the dropped receiver is safe.
    async fn ws_conn_with_capturing_alerter() -> (
        WsConnection,
        mpsc::Receiver<WsServerMessage>,
        std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        use brenn_lib::obs::alerting::make_capturing_alerter;

        let db = brenn_lib::db::init_db_memory();
        let (alert_dispatcher, captured, alert_handle) = make_capturing_alerter();
        let mut state = AppState::for_test(db.clone(), None);
        state.alert_dispatcher = alert_dispatcher;

        let (ws_tx, ws_rx) = mpsc::channel(256);
        let (broadcast_tx, _) = broadcast::channel::<WsServerMessage>(64);

        let (user_id, device_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "drifttest", "$argon2id$fake");
            let did = create_test_device(&conn, uid);
            let cid =
                brenn_lib::conversation::create_conversation(&conn, uid, TEST_APP_SLUG, false);
            brenn_lib::conversation::complete_conversation(&conn, cid, None);
            (uid, did, cid)
        };
        let test_bridge = crate::active_bridge::ActiveBridge::inject_for_test(
            user_id,
            conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );
        *state.test_wake_bridge.lock().await = Some(test_bridge.clone());

        let conn = WsConnBuilder {
            current_conversation_id: Some(conv_id),
            test_bridge: Some(test_bridge),
            ..WsConnBuilder::with_defaults(
                user_id,
                "drifttest".to_string(),
                TEST_APP_SLUG.to_string(),
                ws_tx,
                state,
                device_id,
            )
        }
        .build();

        (conn, ws_rx, captured, alert_handle)
    }

    /// Sanity check: the drift payload used by the schema-drift tests must
    /// actually fail WsClientMessage deserialization (T1Chosen missing
    /// required fields). If serde ever gains defaults for those fields, the
    /// test payload must be updated.
    #[test]
    fn drift_payload_fails_ws_client_message_deserialization() {
        let payload = r#"{"type":"PushClickTrace","user_id":1,"event":{"type":"T1Chosen"}}"#;
        assert!(
            serde_json::from_str::<WsClientMessage>(payload).is_err(),
            "drift payload must fail WsClientMessage deserialization; \
             if T1Chosen gained serde defaults, update the test payloads"
        );
    }

    /// A PushClickTrace-typed message with invalid inner fields (schema drift) must
    /// NOT dispatch a security event / phone alert.  The connection still gets the
    /// standard Error response.
    #[tokio::test]
    async fn malformed_push_click_trace_does_not_trigger_security_event() {
        let (mut conn, mut ws_rx, captured, alert_handle) = ws_conn_with_capturing_alerter().await;

        // T1Chosen missing client_id, target_path, focus_rejected — valid outer
        // shape (type + event object with type string), invalid inner fields.
        handle_client_message(
            r#"{"type":"PushClickTrace","user_id":1,"event":{"type":"T1Chosen"}}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        // Error response still sent — verify it is the Error variant specifically.
        let sent = ws_rx
            .try_recv()
            .expect("Error WS response must be sent even for schema-drift messages");
        assert!(
            matches!(sent, WsServerMessage::Error { .. }),
            "schema-drift path must send WsServerMessage::Error; got {sent:?}"
        );

        // Drain the alerter and assert no alert was dispatched.
        drop(conn); // drop the dispatcher clone held by AppState
        alert_handle.await.expect("alert handle must not panic");
        let alerts = captured.lock().unwrap();
        assert!(
            alerts.is_empty(),
            "schema-drift PushClickTrace must not dispatch a security alert; got {alerts:?}"
        );
    }

    /// A PushClickTrace with a completely absent `event` field (realistic older-SW
    /// drift shape) must also be classified as schema drift, not a security event.
    #[tokio::test]
    async fn push_click_trace_missing_event_field_is_schema_drift() {
        let (mut conn, mut ws_rx, captured, alert_handle) = ws_conn_with_capturing_alerter().await;

        // Older SW that omitted the `event` key entirely.
        handle_client_message(
            r#"{"type":"PushClickTrace","user_id":1}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        // This payload lacks the inner event-object shape required by the
        // discriminant check, so it falls through to the security path.
        // Verify that behavior is intentional and locked in.
        drop(conn);
        alert_handle.await.expect("alert handle must not panic");
        let alerts = captured.lock().unwrap();
        // A payload missing the event key entirely does NOT satisfy the inner-shape
        // check (requires event.type string), so it is treated as a security event.
        // This is the conservative behavior: if a stale SW omits `event` entirely
        // we prefer a false positive (fail2ban) over a bypass surface.
        assert!(
            !alerts.is_empty(),
            "PushClickTrace missing event field must be treated as security event \
             (no inner-shape match); got {alerts:?}"
        );
        // Verify the response was still sent.
        assert!(
            ws_rx.try_recv().is_ok(),
            "Error WS response must still be sent"
        );
    }

    /// A PushClickTrace where `event` is a scalar (not an object) must NOT bypass
    /// the security path — this is a potential attacker probe, not genuine drift.
    #[tokio::test]
    async fn push_click_trace_event_scalar_triggers_security_event() {
        let (mut conn, _ws_rx, captured, alert_handle) = ws_conn_with_capturing_alerter().await;

        // Attacker probing with scalar `event`.
        handle_client_message(
            r#"{"type":"PushClickTrace","event":"notanobject"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        drop(conn);
        alert_handle.await.expect("alert handle must not panic");
        let alerts = captured.lock().unwrap();
        assert!(
            !alerts.is_empty(),
            "PushClickTrace with scalar event must dispatch a security alert; got {alerts:?}"
        );
        assert!(
            alerts[0].0.contains("malformed_message"),
            "alert title must contain 'malformed_message'; got {:?}",
            alerts[0].0
        );
    }

    /// An unknown message type (not PushClickTrace) must still trigger a security event.
    #[tokio::test]
    async fn unknown_message_type_still_triggers_security_event() {
        let (mut conn, _ws_rx, captured, alert_handle) = ws_conn_with_capturing_alerter().await;

        handle_client_message(r#"{"type":"TotallyBogus"}"#, &mut conn, TEST_CLIENT_IP).await;

        drop(conn);
        alert_handle.await.expect("alert handle must not panic");
        let alerts = captured.lock().unwrap();
        assert!(
            !alerts.is_empty(),
            "unknown message type must dispatch a security alert"
        );
        assert!(
            alerts[0].0.contains("malformed_message"),
            "alert title must contain 'malformed_message'; got {:?}",
            alerts[0].0
        );
    }

    /// Unparseable (non-JSON) input must still trigger a security event.
    #[tokio::test]
    async fn unparseable_json_still_triggers_security_event() {
        let (mut conn, _ws_rx, captured, alert_handle) = ws_conn_with_capturing_alerter().await;

        handle_client_message("not json at all", &mut conn, TEST_CLIENT_IP).await;

        drop(conn);
        alert_handle.await.expect("alert handle must not panic");
        let alerts = captured.lock().unwrap();
        assert!(
            !alerts.is_empty(),
            "unparseable input must dispatch a security alert"
        );
        assert!(
            alerts[0].0.contains("malformed_message"),
            "alert title must contain 'malformed_message'; got {:?}",
            alerts[0].0
        );
    }

    // -----------------------------------------------------------------------
    // SetViewportClass — layout frame delivery
    //
    // Verifies that dispatching SetViewportClass sends the correct SetLayout
    // frame to the client. This tests the send_layout call path at the
    // dispatch level (complement to the connect-time test in event_loop.rs).
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // SendMessage dispatch — record_usage guard
    //
    // Verifies that the `if dispatched` guard in the SendMessage arm of
    // handle_client_message prevents writing a usage_events row when
    // handle_send_message returns false. This tests the wiring in dispatch.rs,
    // complementing the per-rejection tests in messaging.rs that only call
    // handle_send_message directly.
    // -----------------------------------------------------------------------

    /// A viewer-only `SendMessage` dispatched through `handle_client_message` must
    /// NOT write a `usage_events` row (the `if dispatched` guard in dispatch.rs).
    ///
    /// Paired with the viewer-only test in messaging.rs (which asserts `!dispatched`)
    /// to pin the full call chain: rejection → false → guard → no DB write.
    #[tokio::test]
    async fn send_message_rejection_does_not_write_usage_event() {
        let (mut conn, _ws_rx, db, _uid) = test_ws_conn_for_app(test_apps_single_instance()).await;

        // Make the connection a viewer (another user owns a live bridge).
        let other_user_id = {
            let c = db.lock().await;
            create_user(&c, "otherviewer", "$argon2id$fake")
        };
        let other_conv_id = {
            let c = db.lock().await;
            conversation::create_conversation(&c, other_user_id, TEST_APP_SLUG, false)
        };
        let (broadcast_tx, _) = broadcast::channel(64);
        let other_bridge = ActiveBridge::inject_for_test(
            other_user_id,
            other_conv_id,
            TEST_APP_SLUG,
            db.clone(),
            broadcast_tx,
        );
        conn.state
            .active_bridges
            .insert(other_conv_id, other_bridge.clone())
            .await;
        conn.attach_to_bridge(&other_bridge).await;
        conn.viewer_only = true;

        handle_client_message(
            r#"{"type":"SendMessage","text":"hello","attachments":[],"selected_tasks":[]}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        // No usage_events row must be written for a rejected send.
        let count: i64 = {
            let db_conn = db.lock().await;
            db_conn
                .query_row(
                    "SELECT COUNT(*) FROM usage_events WHERE event_type = 'send_message'",
                    [],
                    |row| row.get(0),
                )
                .expect("count query must succeed")
        };
        assert_eq!(
            count, 0,
            "viewer-only rejection must not write a usage_events row (if dispatched guard broken)"
        );
    }

    /// A valid `SendMessage` dispatched through `handle_client_message` writes
    /// exactly one `usage_events` row — the positive side of the guard.
    #[tokio::test]
    async fn send_message_dispatch_writes_usage_event() {
        let (mut conn, _ws_rx, db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;

        handle_client_message(
            r#"{"type":"SendMessage","text":"hello","attachments":[],"selected_tasks":[]}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        let count: i64 = {
            let db_conn = db.lock().await;
            db_conn
                .query_row(
                    "SELECT COUNT(*) FROM usage_events WHERE event_type = 'send_message'",
                    [],
                    |row| row.get(0),
                )
                .expect("count query must succeed")
        };
        assert_eq!(
            count, 1,
            "valid SendMessage dispatch must write exactly one usage_events row"
        );
    }

    /// `TodoDone` dispatched through `handle_client_message` writes exactly one
    /// `usage_events` row with `event_type = 'todo_done'` and the connection's
    /// `device_id`.  The test asserts only the usage record — not the todo
    /// outcome — the todo outcome is separately tested.
    #[tokio::test]
    async fn todo_done_records_ui_event_with_device_id() {
        let (mut conn, _ws_rx, db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;
        let expected_device_id = conn.device_id;

        handle_client_message(
            r#"{"type":"TodoDone","path":"todo/x.md","completion_date":"2026-05-27"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        let (count, device_id): (i64, i64) = {
            let db_conn = db.lock().await;
            db_conn
                .query_row(
                    "SELECT COUNT(*), device_id FROM usage_events WHERE event_type = 'todo_done'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("query must succeed")
        };
        assert_eq!(count, 1, "TodoDone must write exactly one usage_events row");
        assert_eq!(
            device_id, expected_device_id,
            "usage_events.device_id must match conn.device_id"
        );
    }

    /// `SetViewportClass{Compact}` sends `SetLayout{SinglePane}`.
    #[tokio::test]
    async fn set_viewport_class_sends_set_layout_frame() {
        let (mut conn, mut ws_rx, _db, _uid, _conv_id) = test_ws_conn_with_resume_conv().await;
        // Fixture starts Wide; dispatch Compact to force a layout change.
        assert_eq!(conn.viewport_class, ViewportClass::Wide);

        handle_client_message(
            r#"{"type":"SetViewportClass","viewport_class":"Compact"}"#,
            &mut conn,
            TEST_CLIENT_IP,
        )
        .await;

        assert_eq!(conn.viewport_class, ViewportClass::Compact);
        let msg = ws_rx
            .try_recv()
            .expect("SetLayout frame must be sent after SetViewportClass");
        assert!(
            matches!(
                msg,
                WsServerMessage::SetLayout {
                    layout: brenn_lib::ws_types::PaneLayout::SinglePane
                }
            ),
            "expected SetLayout(SinglePane), got {msg:?}"
        );
    }

    // -----------------------------------------------------------------------
    // DebugViewportSnapshot dispatch tests (design §2.3, §4)
    //
    // A minimal valid JSON payload for DebugViewportSnapshot: all required
    // (non-Option) fields set; all Option fields set to null. Tests verify:
    //   (a) does not touch the idle-hook timer (not a UI action);
    //   (b) does not dispatch a security event (AC8);
    //   (c) emits the INFO log with `debug_viewport_snapshot=true` (AC4).
    // -----------------------------------------------------------------------

    /// Minimal well-formed DebugViewportSnapshot payload as a JSON string.
    ///
    /// Serialized from `DebugViewportSnapshotData::default()` so that adding
    /// struct fields never requires updating a hand-maintained JSON literal.
    /// `DebugViewportSnapshotData` has no `skip_serializing_if` on its fields,
    /// so serde emits explicit `null` for every `Option`; the deserializer
    /// accepts that back as `None`, making this a valid minimal payload.
    fn minimal_debug_snapshot_json() -> String {
        serde_json::to_string(&WsClientMessage::DebugViewportSnapshot {
            snapshot: Box::new(DebugViewportSnapshotData::default()),
        })
        .expect("WsClientMessage serialization is infallible")
    }

    /// A well-formed `DebugViewportSnapshot` must NOT cancel-and-rearm the
    /// idle-hook timer. It is a diagnostic, not a UI action.
    #[tokio::test]
    async fn debug_viewport_snapshot_does_not_touch_timer() {
        assert_does_not_touch_timer(&minimal_debug_snapshot_json()).await;
    }

    /// A well-formed `DebugViewportSnapshot` must NOT dispatch a security
    /// alert — it is a legitimate expected message, not attacker traffic (AC8).
    #[tokio::test]
    async fn debug_viewport_snapshot_does_not_trigger_security_event() {
        let (mut conn, _ws_rx, captured, alert_handle) = ws_conn_with_capturing_alerter().await;

        handle_client_message(&minimal_debug_snapshot_json(), &mut conn, TEST_CLIENT_IP).await;

        drop(conn);
        alert_handle.await.expect("alert handle must not panic");
        let alerts = captured.lock().unwrap();
        // A well-formed snapshot does intentionally dispatch one Info diagnostic
        // alert ("Debug viewport snapshot") — that is the email destination. What
        // AC8 forbids is a *security* alert, whose title is `Security: {event}`.
        let security_alerts: Vec<_> = alerts
            .iter()
            .filter(|(title, _)| title.starts_with("Security:"))
            .collect();
        assert!(
            security_alerts.is_empty(),
            "well-formed DebugViewportSnapshot must not dispatch a security alert (AC8); got {security_alerts:?}"
        );
    }

    /// A well-formed `DebugViewportSnapshot` must emit an INFO log with the
    /// greppable `debug_viewport_snapshot` tag (AC4) — even when there is no
    /// current conversation (bridge resolution returns None). Uses the no-bridge
    /// fixture to exercise the path described in design §3 and AC4.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn debug_viewport_snapshot_logs_info_tag() {
        // Use test_ws_conn_for_app (no conversation, no bridge) to verify that
        // the INFO log fires unconditionally before bridge resolution, per AC4.
        let (mut conn, _ws_rx, _db, _uid) = test_ws_conn_for_app(test_apps()).await;

        handle_client_message(&minimal_debug_snapshot_json(), &mut conn, TEST_CLIENT_IP).await;

        assert!(
            logs_contain("debug_viewport_snapshot"),
            "DebugViewportSnapshot handler must emit an INFO log with the \
             'debug_viewport_snapshot' tag (AC4), even with no active bridge"
        );
    }
}

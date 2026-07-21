//! Sync permission machinery: pending permission slots, replay-on-attach snapshots, AlwaysAllow rule creation, and browser permission response handling.

use std::net::{IpAddr, Ipv4Addr};

use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
use brenn_lib::approval_rules::{ApprovalMatch, CompiledRule};
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::{CcState, PermissionDecision, RuleScope, WsServerMessage};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use super::ActiveBridge;
use super::tool_summary::ApprovalOutcome;

/// Stored per pending synchronous permission approval (Bash, Edit, etc.).
///
/// Interactive tool requests (ProposeReconciliation, BatchReconcile) are NOT
/// stored here — they go to the `pending_tool_requests` DB table. This struct
/// is ONLY for CC Permission prompts that block until the user allows/denies.
pub(super) struct PendingPermission {
    pub(super) tx: oneshot::Sender<CcApprovalDecision>,
    /// The original tool input from CC's request. Echoed back on Allow.
    pub(super) original_input: serde_json::Value,
    /// CC's tool_use_id, used to track approval outcomes for summary detail.
    pub(super) tool_use_id: String,
    /// Tool name, used at replay time to look up the right formatter.
    pub(super) tool_name: String,
    /// The (possibly enriched) input passed to `format_tool_display` for the
    /// live broadcast. Stored so a fresh attach can re-render byte-identical
    /// output. For `mcp__pfin__reconcile` this is the pfin-enriched value;
    /// for other tools it matches `original_input`.
    pub(super) display_input: serde_json::Value,
}

/// Snapshot of a pending permission for replay on attach. Produced by
/// [`ActiveBridge::pending_permission_snapshots`].
#[derive(Debug, Clone)]
pub struct PendingPermissionSnapshot {
    pub request_id: String,
    pub tool_name: String,
    /// The un-enriched tool input; matches the live broadcast's `tool_input`.
    pub tool_input: serde_json::Value,
    /// The pfin-enriched value fed to `format_tool_display` on replay.
    pub display_input: serde_json::Value,
}

impl ActiveBridge {
    /// Snapshot the currently-pending synchronous permissions for replay on
    /// attach. Takes the mutex, clones, drops — no locks escape. The returned
    /// value carries only what the caller needs to re-render the dialog; the
    /// `oneshot::Sender` stays on the bridge.
    pub async fn pending_permission_snapshots(&self) -> Vec<PendingPermissionSnapshot> {
        let permissions = self.pending_permissions.lock().await;
        permissions
            .iter()
            .map(|(request_id, pending)| PendingPermissionSnapshot {
                request_id: request_id.clone(),
                tool_name: pending.tool_name.clone(),
                tool_input: pending.original_input.clone(),
                display_input: pending.display_input.clone(),
            })
            .collect()
    }

    /// Cheap check used by the attach-site `cc_state` calc: returns true iff
    /// there is at least one pending synchronous permission. Avoids the
    /// full snapshot clone that the state decision doesn't need.
    pub async fn has_pending_permissions(&self) -> bool {
        !self.pending_permissions.lock().await.is_empty()
    }

    /// Drain all pending synchronous permissions and broadcast
    /// `PermissionCancelled` for each cleared entry. Returns the request_ids
    /// that were cancelled (useful for logging).
    ///
    /// Used on bridge-teardown paths where the oneshot senders are about to
    /// be dropped (turn-completion clear, `cc_event_loop` post-exit). Empty
    /// maps result in zero broadcasts.
    pub async fn drain_and_cancel_pending_permissions(&self) -> Vec<String> {
        let cleared_ids: Vec<String> = {
            let mut permissions = self.pending_permissions.lock().await;
            if permissions.is_empty() {
                return Vec::new();
            }
            let ids = permissions.keys().cloned().collect::<Vec<_>>();
            permissions.clear();
            ids
        };
        for request_id in &cleared_ids {
            self.broadcast(WsServerMessage::PermissionCancelled {
                request_id: request_id.clone(),
            });
        }
        cleared_ids
    }

    /// Handle a permission response from a browser tab (synchronous — CC is blocking).
    pub async fn handle_permission_response(&self, request_id: &str, decision: PermissionDecision) {
        // For AlwaysAllow: create rules first (regardless of whether the
        // pending approval still exists), then resolve the approval as Allow.
        if let PermissionDecision::AlwaysAllow {
            ref patterns,
            ref scope,
            ref tool_name,
        } = decision
        {
            // Validate all patterns before creating any — no partial rule creation.
            for pattern in patterns {
                if let Err(e) = CompiledRule::compile(tool_name, pattern) {
                    warn!(
                        pattern = %pattern,
                        error = %e,
                        "invalid pattern in AlwaysAllow — sending error to browser",
                    );
                    self.broadcast(WsServerMessage::ApprovalRuleError {
                        request_id: request_id.to_string(),
                        error: e,
                    });
                    return;
                }
            }

            // All patterns valid — create rules.
            for pattern in patterns {
                match self.create_approval_rule(tool_name, pattern, scope).await {
                    Ok(()) => {
                        info!(
                            pattern = %pattern,
                            scope = ?scope,
                            "created approval rule from AlwaysAllow",
                        );
                    }
                    Err(e) => {
                        panic!(
                            "BUG: pattern validation passed but rule creation failed: \
                             pattern={pattern:?}, error={e}"
                        );
                    }
                }
            }
        }

        let pending_permission = {
            let mut permissions = self.pending_permissions.lock().await;
            permissions.remove(request_id)
        };

        let Some(pending) = pending_permission else {
            // Unknown request_id: absent from this bridge's in-memory
            // pending_permissions map. A correct browser only ever returns a
            // request_id we issued to this bridge, so this is the same "browser
            // returned an id we never issued" signal as the tool-card unknown
            // case — upgrade to a SchemaViolation security signal for fail2ban
            // coverage symmetry (design Decision 4). This path is NOT the H1355
            // authz fix: the permission map is per-bridge, so a foreign id is
            // simply absent and structurally cannot resolve another
            // conversation's request.
            log_and_alert_security_event(
                &self.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                &format!(
                    "permission response for unknown request_id {request_id} \
                     (bridge attached to conversation {}, user {})",
                    self.conversation_id, self.user_id
                ),
            );
            return;
        };

        // Record the manual approval outcome so the ToolResult handler can
        // show "Approved by user" in the detail view. Only insert for
        // allowed tools — denied tools don't produce a ToolResult.
        if !matches!(decision, PermissionDecision::Deny { .. }) {
            let mut outcomes = self.approval_outcomes.lock().await;
            outcomes.insert(
                pending.tool_use_id,
                ApprovalOutcome {
                    approval_match: ApprovalMatch::NoMatch,
                },
            );
        }

        let cc_decision = match &decision {
            PermissionDecision::Allow { updated_input } => CcApprovalDecision::Allow {
                updated_input: Some(merge_tool_input(
                    &pending.original_input,
                    updated_input.as_ref(),
                )),
            },
            PermissionDecision::Deny { reason } => CcApprovalDecision::Deny {
                reason: reason.clone().unwrap_or_else(|| "User denied".to_string()),
            },
            PermissionDecision::AlwaysAllow { .. } => {
                // Rule already created above. Treat as Allow for CC.
                CcApprovalDecision::Allow {
                    updated_input: Some(pending.original_input.clone()),
                }
            }
        };
        let send_result = pending.tx.send(cc_decision);
        self.broadcast(WsServerMessage::PermissionResolved {
            request_id: request_id.to_string(),
            decision: match decision {
                PermissionDecision::AlwaysAllow { .. } => PermissionDecision::Allow {
                    updated_input: None,
                },
                other => other,
            },
        });
        match send_result {
            Ok(()) => {
                // AwaitingApproval → Thinking. This path bypasses
                // `set_cc_busy` (cc_idle is already false from the original
                // user message), so the broadcast is explicit.
                self.broadcast(WsServerMessage::Status {
                    state: CcState::Thinking,
                });
            }
            Err(_) => {
                // Suppressing the Thinking broadcast: CC already moved on
                // (receiver dropped), so the UI would be stuck in a state
                // CC is no longer in.
                warn!("permission response for {request_id} dropped — CC may have moved on");
            }
        }
    }

    /// Create an approval rule from an AlwaysAllow decision.
    /// Validates the pattern, inserts into DB, and adds to the in-memory cache.
    async fn create_approval_rule(
        &self,
        tool_name: &str,
        pattern: &str,
        scope: &RuleScope,
    ) -> Result<(), String> {
        // Log oversized patterns at error level for fail2ban (browser sent
        // untrusted input exceeding the size limit).
        if pattern.len() > 512 {
            error!(
                pattern_len = pattern.len(),
                "approval rule pattern exceeds size limit — possible abuse"
            );
        }

        let compiled = CompiledRule::compile(tool_name, pattern)?;

        // Insert into DB.
        let conversation_id = match scope {
            RuleScope::Conversation => Some(self.conversation_id),
            RuleScope::Permanent => None,
        };
        {
            let conn = self.db.lock().await;
            brenn_lib::db::insert_approval_rule(
                &conn,
                &self.app_slug,
                conversation_id,
                tool_name,
                pattern,
            );
        }

        // Add to in-memory cache.
        self.approval_rules.add_dynamic(compiled).await;

        Ok(())
    }
}

/// Merge browser's `updated_input` patch into the original tool input from CC.
///
/// - If `patch` is `None`, returns the original unchanged.
/// - If both original and patch are JSON objects, shallow-merges (patch keys win).
/// - Otherwise, returns the patch as-is (backward-compatible with components
///   that send a full replacement payload).
pub(super) fn merge_tool_input(
    original: &serde_json::Value,
    patch: Option<&serde_json::Value>,
) -> serde_json::Value {
    let Some(patch) = patch else {
        return original.clone();
    };

    match (original, patch) {
        (serde_json::Value::Object(orig_map), serde_json::Value::Object(patch_map)) => {
            let mut merged = orig_map.clone();
            for (key, value) in patch_map {
                merged.insert(key.clone(), value.clone());
            }
            serde_json::Value::Object(merged)
        }
        // Patch is not an object (or original isn't) — use patch as the whole value.
        // This preserves backward compatibility with existing components.
        _ => patch.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_fixtures::{TestBridgeConfig, pfin_test_integrations};
    use super::super::test_support::{
        await_fence, drain_broadcast, event_fence, recv_broadcast, test_bridge,
        test_bridge_with_config,
    };
    use super::*;
    use brenn_cc::session::{ApprovalKind, ApprovalRequest, SessionEvent};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn approval_response_routes_to_cc() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_3".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t3".into(),
                input: serde_json::json!({"command": "rm -rf /"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Wait for the approval to be stashed.
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // User denies.
        bridge
            .handle_permission_response(
                "req_3",
                PermissionDecision::Deny {
                    reason: Some("nope".into()),
                },
            )
            .await;

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Deny { reason } => assert_eq!(reason, "nope"),
            other => panic!("expected Deny, got {other:?}"),
        }

        // Should also broadcast PermissionResolved.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::PermissionResolved {
                request_id,
                decision,
            } => {
                assert_eq!(request_id, "req_3");
                assert!(matches!(decision, PermissionDecision::Deny { .. }));
            }
            other => panic!("expected PermissionResolved, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn approval_cancelled_clears_pending() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Send an approval request first.
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_cancel".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t1".into(),
                input: serde_json::json!({}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Wait for approval to be stashed.
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        {
            let approvals = bridge.pending_permissions.lock().await;
            assert_eq!(approvals.len(), 1);
        }

        // Now cancel it.
        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::ApprovalCancelled {
                request_id: "req_cancel".into(),
            })
            .await
            .unwrap();

        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::PermissionCancelled { request_id } => {
                assert_eq!(request_id, "req_cancel");
            }
            other => panic!("expected PermissionCancelled, got {other:?}"),
        }

        await_fence(fence).await;
        {
            let approvals = bridge.pending_permissions.lock().await;
            assert_eq!(approvals.len(), 0);
        }
    }

    // -----------------------------------------------------------------------
    // Pending-permission replay (permission-request-not-replayed-on-reconnect)
    // -----------------------------------------------------------------------

    /// Inserting a Permission approval round-trips through
    /// `pending_permission_snapshots` with matching `tool_name`, `tool_input`,
    /// and `display_input`.
    #[tokio::test]
    async fn pending_permission_snapshot_roundtrip() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let input = serde_json::json!({"command": "echo hi"});
        let req = ApprovalRequest {
            request_id: "req_snap_1".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "tu_snap_1".into(),
                input: input.clone(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        // Drain initial broadcast so nothing leaks into later asserts.
        let _ = recv_broadcast(&mut broadcast_rx).await;
        let _ = recv_broadcast(&mut broadcast_rx).await;

        let snapshots = bridge.pending_permission_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        let snap = &snapshots[0];
        assert_eq!(snap.request_id, "req_snap_1");
        assert_eq!(snap.tool_name, "Bash");
        assert_eq!(snap.tool_input, input);
        // For non-pfin tools display_input == original_input.
        assert_eq!(snap.display_input, input);
    }

    /// Replaying via `format_tool_display` from a snapshot produces the exact
    /// same `formatted_display` string as the live broadcast did.
    #[tokio::test]
    async fn attach_replays_pending_permission() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let input = serde_json::json!({"command": "ls -la"});
        let req = ApprovalRequest {
            request_id: "req_replay".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "tu_replay".into(),
                input: input.clone(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Drain initial broadcast: PermissionRequest + Status(AwaitingApproval).
        let live_req = recv_broadcast(&mut broadcast_rx).await;
        let live_status = recv_broadcast(&mut broadcast_rx).await;
        let live_formatted = match &live_req {
            WsServerMessage::PermissionRequest {
                request_id,
                tool_name,
                formatted_display,
                ..
            } => {
                assert_eq!(request_id, "req_replay");
                assert_eq!(tool_name, "Bash");
                formatted_display.clone()
            }
            other => panic!("expected PermissionRequest, got {other:?}"),
        };
        assert!(matches!(
            live_status,
            WsServerMessage::Status {
                state: CcState::AwaitingApproval
            }
        ));

        // Simulate a fresh attach: re-render via the same formatter.
        let snapshots = bridge.pending_permission_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        let snap = &snapshots[0];
        let replayed = crate::approval_formatter::format_tool_display(
            &bridge.tool_registry,
            &snap.tool_name,
            &snap.display_input,
        );
        assert_eq!(
            replayed, live_formatted,
            "replayed formatted_display must match the live broadcast byte-for-byte"
        );
    }

    /// An empty `pending_permissions` map means the snapshot is empty; the
    /// caller can trivially emit zero frames.
    #[tokio::test]
    async fn attach_with_no_pending_permissions_sends_nothing_extra() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let snapshots = bridge.pending_permission_snapshots().await;
        assert!(
            snapshots.is_empty(),
            "fresh bridge must have no pending permissions, got {snapshots:?}"
        );
    }

    /// `mcp__pfin__reconcile` runs through the enrichment path; on a plain
    /// `inject_for_test` bridge there's no pfin MCP server configured, so
    /// enrichment is a no-op and display_input == original_input. The contract
    /// we pin here: `display_input` on the snapshot is the same value fed to
    /// `format_tool_display` at insert time.
    #[tokio::test]
    async fn pfin_enriched_display_input_survives_replay() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let input = serde_json::json!({
            "import_id": "imp-1",
            "proposals": []
        });
        let req = ApprovalRequest {
            request_id: "req_pfin_replay".into(),
            kind: ApprovalKind::Permission {
                tool_name: "mcp__pfin__reconcile".into(),
                tool_use_id: "tu_pfin".into(),
                input: input.clone(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Drain live broadcast.
        let live_req = recv_broadcast(&mut broadcast_rx).await;
        let _ = recv_broadcast(&mut broadcast_rx).await;
        let live_formatted = match &live_req {
            WsServerMessage::PermissionRequest {
                formatted_display, ..
            } => formatted_display.clone(),
            other => panic!("expected PermissionRequest, got {other:?}"),
        };

        // Snapshot → re-render. Must match live byte-for-byte.
        let snapshots = bridge.pending_permission_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        let snap = &snapshots[0];
        let replayed = crate::approval_formatter::format_tool_display(
            &bridge.tool_registry,
            &snap.tool_name,
            &snap.display_input,
        );
        assert_eq!(replayed, live_formatted);
    }

    /// Reconnect-from-new-tab regression: a second subscriber attaches after
    /// the initial broadcast, sees the replay, resolves the permission, and
    /// the oneshot is driven correctly.
    #[tokio::test]
    async fn reconnect_resolves_from_new_tab() {
        let (bridge, event_tx, mut broadcast_rx_a, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_reconnect".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "tu_r".into(),
                input: serde_json::json!({"command": "echo hi"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Tab A receives the live broadcast.
        let _ = recv_broadcast(&mut broadcast_rx_a).await;
        let _ = recv_broadcast(&mut broadcast_rx_a).await;

        // Tab A disconnects (drop its receiver). Tab B subscribes AFTER the
        // initial broadcast — it would miss the live PermissionRequest.
        drop(broadcast_rx_a);
        let mut broadcast_rx_b = bridge.subscribe();

        // Replay: fetch snapshots, re-render, emit to B.
        let snapshots = bridge.pending_permission_snapshots().await;
        assert_eq!(
            snapshots.len(),
            1,
            "B's replay must see the pending request"
        );

        // B resolves the permission.
        bridge
            .handle_permission_response(
                "req_reconnect",
                PermissionDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // The oneshot must have fired with Allow.
        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Allow { .. }),
            "oneshot should have received Allow, got {decision:?}"
        );

        // And B must receive the PermissionResolved broadcast.
        let resolved = recv_broadcast(&mut broadcast_rx_b).await;
        match &resolved {
            WsServerMessage::PermissionResolved { request_id, .. } => {
                assert_eq!(request_id, "req_reconnect");
            }
            other => panic!("expected PermissionResolved, got {other:?}"),
        }

        // Pending map is now empty.
        assert!(bridge.pending_permission_snapshots().await.is_empty());
    }

    /// Two tabs both see the initial broadcast; when tab A resolves, tab B
    /// gets `PermissionResolved` (existing behavior — pinned here as a
    /// regression test for the replay change).
    #[tokio::test]
    async fn two_tabs_one_resolves_other_dismisses() {
        let (bridge, event_tx, mut broadcast_rx_a, _ab) = test_bridge().await;
        let mut broadcast_rx_b = bridge.subscribe();

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_two_tabs".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "tu_two".into(),
                input: serde_json::json!({"command": "ls"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Both tabs see PermissionRequest + Status(AwaitingApproval).
        let _ = recv_broadcast(&mut broadcast_rx_a).await;
        let _ = recv_broadcast(&mut broadcast_rx_a).await;
        let _ = recv_broadcast(&mut broadcast_rx_b).await;
        let _ = recv_broadcast(&mut broadcast_rx_b).await;

        // Tab A resolves.
        bridge
            .handle_permission_response(
                "req_two_tabs",
                PermissionDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // oneshot fired.
        assert!(matches!(
            resp_rx.await.unwrap(),
            CcApprovalDecision::Allow { .. }
        ));

        // Tab B receives PermissionResolved exactly once.
        let msg_b = recv_broadcast(&mut broadcast_rx_b).await;
        match &msg_b {
            WsServerMessage::PermissionResolved { request_id, .. } => {
                assert_eq!(request_id, "req_two_tabs");
            }
            other => panic!("expected PermissionResolved on tab B, got {other:?}"),
        }

        // No duplicates on tab A (its PermissionResolved is the only trailing message).
        let msg_a = recv_broadcast(&mut broadcast_rx_a).await;
        match &msg_a {
            WsServerMessage::PermissionResolved { request_id, .. } => {
                assert_eq!(request_id, "req_two_tabs");
            }
            other => panic!("expected PermissionResolved on tab A, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn approval_allow_without_updated_input_echoes_original() {
        // When the browser approves without providing updated_input, the
        // handler should echo back the original input from CC's permission
        // request. CC requires updated_input on Permission Allow.
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let original_input = serde_json::json!({"command": "echo hello"});
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_normal".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_normal".into(),
                input: original_input.clone(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Consume the broadcast messages.
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // User allows with no updated_input (the normal case).
        bridge
            .handle_permission_response(
                "req_normal",
                PermissionDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Allow { updated_input } => {
                assert_eq!(
                    updated_input,
                    Some(original_input),
                    "should echo original input when browser doesn't provide updated_input"
                );
            }
            other => panic!("expected Allow, got {other:?}"),
        }
    }

    /// After the user resolves a permission prompt (Allow or Deny), CC keeps
    /// working on the same turn — Deny sends the denial reason as input.
    /// The UI must flip from AwaitingApproval → Thinking immediately instead
    /// of waiting for CC's next assistant message.
    /// `handle_permission_response` is a synchronous path that doesn't go
    /// through `set_cc_busy`, so the broadcast is explicit.
    async fn assert_permission_response_broadcasts_thinking(decision: PermissionDecision) {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_pt".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_pt".into(),
                input: serde_json::json!({"command": "echo hi"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Consume PermissionRequest + Status(AwaitingApproval).
        let _ = recv_broadcast(&mut broadcast_rx).await;
        let _ = recv_broadcast(&mut broadcast_rx).await;

        bridge.handle_permission_response("req_pt", decision).await;

        let resolved = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(&resolved, WsServerMessage::PermissionResolved { .. }),
            "expected PermissionResolved, got {resolved:?}"
        );
        let status = recv_broadcast(&mut broadcast_rx).await;
        match &status {
            WsServerMessage::Status { state } => assert_eq!(*state, CcState::Thinking),
            other => panic!("expected Status(Thinking), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn permission_response_allow_broadcasts_thinking() {
        assert_permission_response_broadcasts_thinking(PermissionDecision::Allow {
            updated_input: None,
        })
        .await;
    }

    /// Regression guard: the Thinking broadcast must not be made conditional
    /// on decision type. CC continues processing after Deny (consuming the
    /// denial reason) just as it does after Allow.
    #[tokio::test]
    async fn permission_response_deny_broadcasts_thinking() {
        assert_permission_response_broadcasts_thinking(PermissionDecision::Deny {
            reason: Some("no way".into()),
        })
        .await;
    }

    /// Dropped oneshot receiver (CC moved on or died) must not trigger a
    /// Thinking broadcast — it would strand the UI in a state CC is no
    /// longer in. PermissionResolved still fires so other tabs clear.
    #[tokio::test]
    async fn permission_response_skips_thinking_when_send_fails() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_drop".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_drop".into(),
                input: serde_json::json!({"command": "echo hi"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Consume PermissionRequest + Status(AwaitingApproval).
        let _ = recv_broadcast(&mut broadcast_rx).await;
        let _ = recv_broadcast(&mut broadcast_rx).await;

        // Drop the receiver to simulate CC having moved on.
        drop(resp_rx);

        bridge
            .handle_permission_response(
                "req_drop",
                PermissionDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // PermissionResolved still goes out.
        let resolved = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(&resolved, WsServerMessage::PermissionResolved { .. }),
            "expected PermissionResolved, got {resolved:?}"
        );

        // But no Status broadcast. handle_permission_response is awaited directly
        // here (not dispatched through cc_event_loop); by the time `.await` returns
        // all broadcasts are emitted. Drain immediately with no wait.
        let extra = drain_broadcast(&mut broadcast_rx);
        assert!(
            extra.is_empty(),
            "dropped oneshot must not trigger Status(Thinking), got {extra:?}"
        );
    }

    #[tokio::test]
    async fn approval_passes_through_updated_input() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_ui".into(),
            kind: ApprovalKind::Permission {
                tool_name: "AskUserQuestion".into(),
                tool_use_id: "t_ui".into(),
                input: serde_json::json!({"questions": []}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Wait for broadcast.
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // Respond with updated_input (like the AskUserQuestion dialog would).
        let answers = serde_json::json!({
            "questions": [],
            "answers": {"Which lib?": "date-fns"}
        });
        bridge
            .handle_permission_response(
                "req_ui",
                PermissionDecision::Allow {
                    updated_input: Some(answers.clone()),
                },
            )
            .await;

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Allow { updated_input } => {
                let ui = updated_input.expect("should pass through updated_input");
                assert_eq!(ui["answers"]["Which lib?"], "date-fns");
            }
            other => panic!("expected Allow with updated_input, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn approval_merges_partial_updated_input_into_original() {
        // Interactive tool scenario: browser sends { selected: 1 } as
        // updated_input, backend merges it into the original so CC gets
        // the full payload with the patch applied.
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let original_input = serde_json::json!({
            "import_id": "imp-123",
            "proposals": [
                { "label": "Groceries", "transaction": {} },
                { "label": "Restaurant", "transaction": {} }
            ]
        });

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_merge".into(),
            kind: ApprovalKind::Permission {
                tool_name: "mcp__pfin__reconcile".into(),
                tool_use_id: "t_merge".into(),
                input: original_input.clone(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // Browser sends only the selection — not the full payload.
        bridge
            .handle_permission_response(
                "req_merge",
                PermissionDecision::Allow {
                    updated_input: Some(serde_json::json!({ "selected": 1 })),
                },
            )
            .await;

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Allow { updated_input } => {
                let ui = updated_input.expect("should have merged updated_input");
                // Original fields preserved.
                assert_eq!(ui["import_id"], "imp-123", "import_id should be preserved");
                assert!(ui["proposals"].is_array(), "proposals should be preserved");
                // Patch field added.
                assert_eq!(ui["selected"], 1, "selected should be merged in");
            }
            other => panic!("expected Allow with merged input, got {other:?}"),
        }
    }

    #[test]
    fn merge_tool_input_no_patch_returns_original() {
        let original = serde_json::json!({"a": 1, "b": 2});
        let result = merge_tool_input(&original, None);
        assert_eq!(result, original);
    }

    #[test]
    fn merge_tool_input_shallow_merge_patch_wins() {
        let original = serde_json::json!({"a": 1, "b": 2});
        let patch = serde_json::json!({"b": 99, "c": 3});
        let result = merge_tool_input(&original, Some(&patch));
        assert_eq!(result["a"], 1);
        assert_eq!(result["b"], 99);
        assert_eq!(result["c"], 3);
    }

    #[test]
    fn merge_tool_input_non_object_patch_replaces() {
        let original = serde_json::json!({"a": 1});
        let patch = serde_json::json!("just a string");
        let result = merge_tool_input(&original, Some(&patch));
        assert_eq!(result, "just a string");
    }

    #[test]
    fn merge_tool_input_non_object_original_uses_patch() {
        let original = serde_json::json!("string");
        let patch = serde_json::json!({"selected": 1});
        let result = merge_tool_input(&original, Some(&patch));
        assert_eq!(result, serde_json::json!({"selected": 1}));
    }

    // -----------------------------------------------------------------------
    // AlwaysAllow integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn always_allow_creates_rule_and_resolves() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Send a Bash Permission request.
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_aa".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_aa".into(),
                input: serde_json::json!({"command": "git status --porcelain"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Wait for ApprovalRequest + Status broadcasts.
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // User clicks "Always Allow" with a pattern, conversation scope.
        bridge
            .handle_permission_response(
                "req_aa",
                PermissionDecision::AlwaysAllow {
                    patterns: vec!["git status\\b.*".into()],
                    scope: RuleScope::Conversation,
                    tool_name: "Bash".into(),
                },
            )
            .await;

        // CC should receive Allow.
        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Allow { .. }),
            "AlwaysAllow should resolve as Allow to CC, got {decision:?}"
        );

        // Broadcast should be PermissionResolved with Allow (not AlwaysAllow).
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::PermissionResolved { decision, .. } => {
                assert!(
                    matches!(decision, PermissionDecision::Allow { .. }),
                    "broadcast should map AlwaysAllow to Allow, got {decision:?}"
                );
            }
            other => panic!("expected PermissionResolved, got {other:?}"),
        }

        // Followed by Status(Thinking): AwaitingApproval → Thinking.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::Status { state } => assert_eq!(*state, CcState::Thinking),
            other => panic!("expected Status(Thinking), got {other:?}"),
        }

        // The rule should now auto-approve matching commands.
        let (resp_tx2, resp_rx2) = oneshot::channel();
        let req2 = ApprovalRequest {
            request_id: "req_aa2".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_aa2".into(),
                input: serde_json::json!({"command": "git status"}),
            },
            response_tx: resp_tx2,
        };
        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(req2))
            .await
            .unwrap();

        // Should be auto-approved — no broadcast to browser.
        let decision2 = resp_rx2.await.unwrap();
        assert!(
            matches!(decision2, CcApprovalDecision::Allow { .. }),
            "matching command should be auto-approved after AlwaysAllow"
        );
        await_fence(fence).await;
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            msgs.is_empty(),
            "auto-approved command should not broadcast, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn always_allow_non_matching_still_prompts() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Create a rule via AlwaysAllow for git status.
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_nm1".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_nm1".into(),
                input: serde_json::json!({"command": "git status"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        bridge
            .handle_permission_response(
                "req_nm1",
                PermissionDecision::AlwaysAllow {
                    patterns: vec!["git status\\b.*".into()],
                    scope: RuleScope::Conversation,
                    tool_name: "Bash".into(),
                },
            )
            .await;
        let _msg = recv_broadcast(&mut broadcast_rx).await; // PermissionResolved
        let _msg = recv_broadcast(&mut broadcast_rx).await; // Status(Thinking)

        // Now send a non-matching command — should still prompt.
        let (resp_tx2, _resp_rx2) = oneshot::channel();
        let req2 = ApprovalRequest {
            request_id: "req_nm2".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_nm2".into(),
                input: serde_json::json!({"command": "git commit -m 'foo'"}),
            },
            response_tx: resp_tx2,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req2))
            .await
            .unwrap();

        // Should broadcast ApprovalRequest (not auto-approved).
        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(&msg, WsServerMessage::PermissionRequest { tool_name, .. } if tool_name == "Bash"),
            "non-matching command should prompt, got {msg:?}"
        );
    }

    #[tokio::test]
    async fn always_allow_invalid_pattern_sends_error() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_bad".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_bad".into(),
                input: serde_json::json!({"command": "ls"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // Send AlwaysAllow with an invalid regex.
        bridge
            .handle_permission_response(
                "req_bad",
                PermissionDecision::AlwaysAllow {
                    patterns: vec!["(unclosed".into()],
                    scope: RuleScope::Permanent,
                    tool_name: "Bash".into(),
                },
            )
            .await;

        // Should broadcast ApprovalRuleError, NOT ApprovalResolved.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::ApprovalRuleError { request_id, error } => {
                assert_eq!(request_id, "req_bad");
                assert!(
                    error.contains("invalid regex"),
                    "error should mention invalid regex, got: {error}"
                );
            }
            other => panic!("expected ApprovalRuleError, got {other:?}"),
        }

        // The pending approval should still be alive (dialog stays open).
        // Verify by sending a valid Allow — it should still resolve.
        bridge
            .handle_permission_response(
                "req_bad",
                PermissionDecision::Allow {
                    updated_input: None,
                },
            )
            .await;
        let msg2 = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(&msg2, WsServerMessage::PermissionResolved { .. }),
            "pending approval should still be resolvable after rule error, got {msg2:?}"
        );
    }

    #[tokio::test]
    async fn always_allow_permanent_persists_to_db() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_perm".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_perm".into(),
                input: serde_json::json!({"command": "make build"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        bridge
            .handle_permission_response(
                "req_perm",
                PermissionDecision::AlwaysAllow {
                    patterns: vec!["make\\b.*".into()],
                    scope: RuleScope::Permanent,
                    tool_name: "Bash".into(),
                },
            )
            .await;
        let _msg = recv_broadcast(&mut broadcast_rx).await; // ApprovalResolved

        // Verify the rule was persisted to DB.
        let rules = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::load_approval_rules(&conn, "test", bridge.conversation_id)
        };
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].tool_name, "Bash");
        assert_eq!(rules[0].pattern, "make\\b.*");
        assert!(
            rules[0].conversation_id.is_none(),
            "permanent rule should have NULL conversation_id"
        );
    }

    #[tokio::test]
    async fn always_allow_conversation_scoped_persists_with_conversation_id() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_conv".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_conv".into(),
                input: serde_json::json!({"command": "cargo test"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        bridge
            .handle_permission_response(
                "req_conv",
                PermissionDecision::AlwaysAllow {
                    patterns: vec!["cargo test\\b.*".into()],
                    scope: RuleScope::Conversation,
                    tool_name: "Bash".into(),
                },
            )
            .await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        let rules = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::load_approval_rules(&conn, "test", bridge.conversation_id)
        };
        assert_eq!(rules.len(), 1);
        assert_eq!(
            rules[0].conversation_id,
            Some(bridge.conversation_id),
            "conversation-scoped rule should have the conversation_id set"
        );
    }

    #[tokio::test]
    async fn always_allow_on_cancelled_request_still_creates_rule() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_cancel".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_cancel".into(),
                input: serde_json::json!({"command": "git log"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        let _msg = recv_broadcast(&mut broadcast_rx).await;
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // Simulate CC cancelling the request by removing it from pending.
        {
            let mut approvals = bridge.pending_permissions.lock().await;
            approvals.remove("req_cancel");
        }

        // User still clicks AlwaysAllow (they were editing the pattern).
        bridge
            .handle_permission_response(
                "req_cancel",
                PermissionDecision::AlwaysAllow {
                    patterns: vec!["git log\\b.*".into()],
                    scope: RuleScope::Conversation,
                    tool_name: "Bash".into(),
                },
            )
            .await;

        // Rule should still have been created.
        let rules = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::load_approval_rules(&conn, "test", bridge.conversation_id)
        };
        assert_eq!(
            rules.len(),
            1,
            "rule should be created even when pending approval was cancelled"
        );
        assert_eq!(rules[0].pattern, "git log\\b.*");

        // Verify the rule works for future requests.
        let (resp_tx2, resp_rx2) = oneshot::channel();
        let req2 = ApprovalRequest {
            request_id: "req_cancel2".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_cancel2".into(),
                input: serde_json::json!({"command": "git log --oneline"}),
            },
            response_tx: resp_tx2,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req2))
            .await
            .unwrap();

        let decision = resp_rx2.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Allow { .. }),
            "rule from cancelled request should auto-approve future matches"
        );
    }

    #[tokio::test]
    async fn always_allow_multiple_patterns_creates_multiple_rules() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Send a compound command to generate an approval request.
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_multi".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_multi".into(),
                input: serde_json::json!({"command": "git status && cargo test"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Consume the ApprovalRequest and Status broadcasts.
        let _approval_msg = recv_broadcast(&mut broadcast_rx).await;
        let _status_msg = recv_broadcast(&mut broadcast_rx).await;

        // User clicks AlwaysAllow with both patterns.
        bridge
            .handle_permission_response(
                "req_multi",
                PermissionDecision::AlwaysAllow {
                    patterns: vec!["git status\\b.*".into(), "cargo test\\b.*".into()],
                    scope: RuleScope::Permanent,
                    tool_name: "Bash".into(),
                },
            )
            .await;

        // Both rules should exist in DB.
        let rules = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::load_approval_rules(&conn, "test", bridge.conversation_id)
        };
        assert_eq!(rules.len(), 2, "should create two rules");
        let patterns: Vec<&str> = rules.iter().map(|r| r.pattern.as_str()).collect();
        assert!(patterns.contains(&"git status\\b.*"));
        assert!(patterns.contains(&"cargo test\\b.*"));
    }

    #[tokio::test]
    async fn always_allow_invalid_pattern_aborts_all() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_abort".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_abort".into(),
                input: serde_json::json!({"command": "git status && something"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let _approval_msg = recv_broadcast(&mut broadcast_rx).await;
        let _status_msg = recv_broadcast(&mut broadcast_rx).await;

        // First pattern valid, second invalid — should create NO rules.
        bridge
            .handle_permission_response(
                "req_abort",
                PermissionDecision::AlwaysAllow {
                    patterns: vec!["git status\\b.*".into(), "(unclosed".into()],
                    scope: RuleScope::Permanent,
                    tool_name: "Bash".into(),
                },
            )
            .await;

        // No rules should have been created.
        let rules = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::load_approval_rules(&conn, "test", bridge.conversation_id)
        };
        assert_eq!(
            rules.len(),
            0,
            "no rules should be created when any pattern is invalid"
        );

        // Should have received an ApprovalRuleError.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(msg, WsServerMessage::ApprovalRuleError { .. }),
            "expected ApprovalRuleError, got {msg:?}"
        );
    }

    #[tokio::test]
    async fn permission_response_for_unknown_request_id_is_noop() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // No pending permission or DB entry for this request_id.
        bridge
            .handle_permission_response(
                "req_nonexistent",
                PermissionDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // Should not panic — just logs a warning + security signal.
    }

    /// Decision 4 (design test 6): a permission response for a request_id absent
    /// from `pending_permissions` emits the same `SchemaViolation` security
    /// signal as the tool-card unknown case and resolves nothing.
    #[tokio::test]
    async fn unknown_permission_request_id_emits_security_signal() {
        let (dispatcher, captured, handle) =
            brenn_lib::obs::alerting::make_capturing_alerter_with_severity();
        let (bridge, event_tx, mut broadcast_rx, _ab) =
            super::super::test_support::test_bridge_with_dispatcher(dispatcher).await;
        let conv_id = bridge.conversation_id;
        let user_id = bridge.user_id;

        bridge
            .handle_permission_response(
                "req_absent",
                PermissionDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // Nothing resolved → no broadcast.
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(msgs.is_empty(), "unknown id must not broadcast: {msgs:?}");

        super::super::test_support::drop_and_drain_alerts(event_tx, bridge, handle).await;

        let captured = captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "unknown permission request_id must emit exactly one alert, got: {captured:?}"
        );
        let (severity, title, body) = &captured[0];
        assert!(
            matches!(severity, brenn_lib::obs::alerting::AlertSeverity::Warning),
            "alert severity must be Warning, got: {severity:?}"
        );
        assert!(
            title.contains("schema_violation"),
            "alert title must be a schema_violation, got: {title}"
        );
        assert!(
            body.contains("req_absent")
                && body.contains(&conv_id.to_string())
                && body.contains(&user_id.to_string()),
            "alert body must carry the request id, conversation id, and user id, got: {body}"
        );
    }

    #[tokio::test]
    async fn permission_response_for_tool_card_request_id_is_noop() {
        // A tool card request exists in the DB, but a PermissionResponse arrives
        // for it (wrong handler). Should be a no-op — the handler only checks
        // pending_permissions (in-memory), not the DB.
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Insert a tool card request directly into the DB.
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_cross_tc",
                bridge.conversation_id,
                "mcp__brenn__ProposeReconciliation",
                r#"{"proposals":[]}"#,
                None,
            );
        }

        bridge
            .handle_permission_response(
                "req_cross_tc",
                PermissionDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // The DB entry should still be pending (untouched).
        {
            let conn = bridge.db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&conn, "req_cross_tc")
                .expect("DB entry should still exist");
            assert_eq!(req.status, "pending", "should not have been resolved");
        }

        // No broadcast should have occurred (no match in pending_permissions).
        // handle_permission_response is awaited directly here (not dispatched through
        // cc_event_loop); by the time `.await` returns all broadcasts are emitted.
        // Drain immediately with no wait.
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            msgs.is_empty(),
            "should not broadcast anything, got: {msgs:?}"
        );
    }
}

//! Death/teardown-family tests: `SessionEvent::Died` handling
//! (`mark_conversation_error`, alert suppression on intentional shutdown,
//! compact-boundary reset) and event-channel-closed teardown
//! (registry removal, conversation completion, permission cancellation).
//! Peeled out of `tests/mod.rs` per design §2.4.

use super::super::super::test_support::{
    await_fence, await_fence_n, drain_broadcast, event_fence, recv_broadcast, test_bridge,
    test_bridge_with_dispatcher,
};
use super::super::*;
use brenn_lib::ws_types::CcState;

use brenn_cc::session::{ApprovalKind, ApprovalRequest};
use brenn_lib::conversation::ConversationStatus;
use brenn_lib::obs::alerting::{AlertDispatcher, CountingAlerter, RateLimiter};

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;

#[tokio::test]
async fn died_sends_error_and_status() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    event_tx
        .send(SessionEvent::Died(brenn_cc::error::CcError::SendFailed))
        .await
        .unwrap();

    let msg1 = recv_broadcast(&mut broadcast_rx).await;
    let msg2 = recv_broadcast(&mut broadcast_rx).await;

    match &msg1 {
        WsServerMessage::Error { message } => {
            assert!(message.contains("died"), "error should mention CC died");
        }
        other => panic!("expected Error, got {other:?}"),
    }
    match &msg2 {
        WsServerMessage::Status { state } => assert_eq!(*state, CcState::Error),
        other => panic!("expected Status Error, got {other:?}"),
    }
}

#[tokio::test]
async fn died_with_server_shutting_down_suppresses_alerts() {
    // On intentional server shutdown (SIGTERM), SessionEvent::Died
    // must not fire the "CC session died" Warning, must not mark the
    // conversation errored, and must not broadcast Error state. The
    // conversation should remain resumable after restart.
    use std::sync::atomic::AtomicU32;
    let alert_count = Arc::new(AtomicU32::new(0));
    let (dispatcher, _h) = AlertDispatcher::new(
        CountingAlerter(alert_count.clone()),
        RateLimiter::new(10, 60),
    );
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    // Flip the server-shutdown flag before firing Died. This
    // simulates what shutdown_signal does on SIGTERM.
    bridge.server_shutting_down.store(true, Ordering::SeqCst);

    event_tx
        .send(SessionEvent::Died(brenn_cc::error::CcError::SendFailed))
        .await
        .unwrap();

    // No broadcast should arrive — both the Error message and Error
    // status are skipped.
    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(100),
        recv_broadcast(&mut broadcast_rx),
    )
    .await;
    assert!(
        result.is_err(),
        "server_shutting_down should suppress Error/Status broadcasts"
    );

    // And the conversation should still be Active (not errored).
    let conv = {
        let conn = bridge.db.lock().await;
        conversation::get_conversation(&conn, bridge.conversation_id)
    };
    assert_eq!(
        conv.status,
        ConversationStatus::Active,
        "conversation should stay Active for resume-after-restart"
    );

    // Alert side is also silent — this is the test that guards against
    // the #4 warning-storm root cause returning.
    assert_eq!(
        alert_count.load(Ordering::SeqCst),
        0,
        "server_shutting_down must suppress the 'CC session died' alert"
    );
}

#[tokio::test]
async fn died_without_shutdown_flag_still_alerts() {
    // Regression complement: when neither drain_on_idle nor
    // server_shutting_down is set, a real unexpected death still fires
    // the Warning alert + errors the conversation. Ensures fix #4 is
    // suppression-only, not general-purpose alert silencing.
    use std::sync::atomic::AtomicU32;
    let alert_count = Arc::new(AtomicU32::new(0));
    let (dispatcher, _h) = AlertDispatcher::new(
        CountingAlerter(alert_count.clone()),
        RateLimiter::new(10, 60),
    );
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_dispatcher(dispatcher).await;

    assert!(!bridge.server_shutting_down.load(Ordering::SeqCst));
    assert!(!bridge.drain_on_idle.load(Ordering::SeqCst));

    event_tx
        .send(SessionEvent::Died(brenn_cc::error::CcError::SendFailed))
        .await
        .unwrap();

    // Error broadcast arrives.
    let msg = recv_broadcast(&mut broadcast_rx).await;
    assert!(matches!(msg, WsServerMessage::Error { .. }));

    // Eventually Status=Error.
    let msg = recv_broadcast(&mut broadcast_rx).await;
    assert!(matches!(
        msg,
        WsServerMessage::Status {
            state: CcState::Error
        }
    ));

    // And the conversation is marked Errored.
    let conv = {
        let conn = bridge.db.lock().await;
        conversation::get_conversation(&conn, bridge.conversation_id)
    };
    assert_eq!(conv.status, ConversationStatus::Error);

    // Alert fires.
    assert_eq!(
        alert_count.load(Ordering::SeqCst),
        1,
        "unexpected Died must still alert"
    );
}

#[tokio::test]
async fn died_with_drain_on_idle_skips_error() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    // Simulate intentional drain shutdown.
    bridge.drain_on_idle.store(true, Ordering::SeqCst);

    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::Died(brenn_cc::error::CcError::ProcessDied {
            exit_status: None,
        }))
        .await
        .unwrap();

    await_fence(fence).await;

    // Should NOT have broadcast Error or Status::Error.
    let msgs = drain_broadcast(&mut broadcast_rx);
    for msg in &msgs {
        match msg {
            WsServerMessage::Error { .. } => {
                panic!("should not broadcast Error on intentional drain shutdown");
            }
            WsServerMessage::Status { state } => {
                assert_ne!(
                    *state,
                    CcState::Error,
                    "should not broadcast Error state on intentional drain shutdown"
                );
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn subprocess_crash_broadcasts_cancel() {
    let (bridge, event_tx, mut broadcast_rx, active_bridges) = test_bridge().await;
    active_bridges
        .insert(bridge.conversation_id, bridge.clone())
        .await;

    // Insert a pending permission first.
    let (resp_tx, _resp_rx) = oneshot::channel();
    let req = ApprovalRequest {
        request_id: "req_crash".into(),
        kind: ApprovalKind::Permission {
            tool_name: "Bash".into(),
            tool_use_id: "tu_crash".into(),
            input: serde_json::json!({"command": "true"}),
        },
        response_tx: resp_tx,
    };
    event_tx
        .send(SessionEvent::ApprovalRequired(req))
        .await
        .unwrap();
    // Drain the live broadcast.
    let _ = recv_broadcast(&mut broadcast_rx).await;
    let _ = recv_broadcast(&mut broadcast_rx).await;

    // Drop the event sender → event loop exits → teardown path runs.
    let fence = event_fence(&bridge);
    drop(event_tx);

    // The teardown must broadcast PermissionCancelled before removing
    // the bridge from the registry.
    let mut saw_cancel = false;
    for _ in 0..4 {
        match tokio::time::timeout(std::time::Duration::from_millis(500), broadcast_rx.recv()).await
        {
            Ok(Ok(WsServerMessage::PermissionCancelled { request_id })) => {
                assert_eq!(request_id, "req_crash");
                saw_cancel = true;
                break;
            }
            Ok(Ok(_other)) => continue,
            _ => break,
        }
    }
    assert!(
        saw_cancel,
        "event-loop teardown must broadcast PermissionCancelled for live entries"
    );

    // And the bridge must be removed from the registry (pre-existing
    // teardown behavior) after the broadcast. Await the teardown epoch
    // to ensure complete_and_kill and drain_and_cancel_pending_permissions ran.
    await_fence(fence).await;
    assert!(
        active_bridges.get(bridge.conversation_id).await.is_none(),
        "bridge should be removed after event loop ends"
    );
}

#[tokio::test]
async fn event_channel_closed_removes_from_registry() {
    let (bridge_ref, event_tx, _broadcast_rx, active_bridges) = test_bridge().await;

    // Subscribe the fence immediately after test_bridge() so the receiver
    // starts at epoch 0 (before the startup-drain increment). The event loop
    // will increment the epoch twice: once for the startup drain (0→1) and
    // once for post-loop teardown after drop(event_tx) (1→2). await_fence_n(2)
    // ensures both increments have fired before asserting on side effects.
    let fence = event_fence(&bridge_ref);

    // Register the bridge in active_bridges.
    active_bridges
        .insert(bridge_ref.conversation_id, bridge_ref.clone())
        .await;

    // Verify it's registered.
    assert!(
        active_bridges
            .get(bridge_ref.conversation_id)
            .await
            .is_some()
    );

    // Drop the sender to close the channel (simulates CC exit).
    drop(event_tx);

    // Await startup-drain epoch + teardown epoch (2 total).
    await_fence_n(fence, 2).await;

    // Bridge should be removed from registry.
    assert!(
        active_bridges
            .get(bridge_ref.conversation_id)
            .await
            .is_none(),
        "bridge should be removed from registry after CC exits"
    );
}

#[tokio::test]
async fn event_channel_closed_completes_conversation() {
    let (bridge, event_tx, _broadcast_rx, active_bridges) = test_bridge().await;

    // Subscribe the fence immediately after test_bridge() at epoch 0 — see
    // event_channel_closed_removes_from_registry for the full explanation.
    // Await 2 epochs: startup-drain (0→1) + teardown after drop(event_tx) (1→2).
    let fence = event_fence(&bridge);

    active_bridges
        .insert(bridge.conversation_id, bridge.clone())
        .await;

    // Verify conversation starts as Active.
    {
        let conn = bridge.db.lock().await;
        let conv = conversation::get_conversation(&conn, bridge.conversation_id);
        assert_eq!(conv.status, ConversationStatus::Active);
    }

    // Drop the sender to close the channel (simulates CC exit).
    drop(event_tx);

    await_fence_n(fence, 2).await;

    // Conversation should be Completed.
    let conn = bridge.db.lock().await;
    let conv = conversation::get_conversation(&conn, bridge.conversation_id);
    assert_eq!(
        conv.status,
        ConversationStatus::Completed,
        "conversation should be Completed after CC exits"
    );
}

/// `Died` resets `compact_boundary_seen` to false even when pre-set to true.
#[tokio::test]
async fn died_resets_compact_boundary_seen() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;
    bridge.compaction.lock().await.compact_boundary_seen = true;

    event_tx
        .send(SessionEvent::Died(brenn_cc::error::CcError::SendFailed))
        .await
        .unwrap();

    let _msg1 = recv_broadcast(&mut broadcast_rx).await;
    let _msg2 = recv_broadcast(&mut broadcast_rx).await;

    let state = bridge.compaction.lock().await;
    assert!(
        !state.compact_boundary_seen,
        "compact_boundary_seen must be false after Died"
    );
}
